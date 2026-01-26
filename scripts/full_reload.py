#!/usr/bin/env python3
"""
Full MAUDE Database Reload Script.

This script performs a complete data reload following the correct order:
1. Backup current database
2. Drop all tables and recreate schema
3. Download any missing files (device files prioritized)
4. Load in correct order (device FIRST, then master, then others)
5. Populate master_events from devices
6. Run all validations
7. Generate final health report

Usage:
    python scripts/full_reload.py
    python scripts/full_reload.py --skip-download
    python scripts/full_reload.py --years 2020 2021 2022 2023 2024
    python scripts/full_reload.py --checkpoint checkpoint.json

CRITICAL: Device files must be loaded FIRST because master files do NOT
contain manufacturer or product code data - only device files have it.
"""

import argparse
import fnmatch
import json
import re
import shutil
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional
import time

PROJECT_ROOT = Path(__file__).parent.parent


def glob_case_insensitive(directory: Path, pattern: str) -> List[Path]:
    """
    Case-insensitive glob matching for file patterns.

    Handles both uppercase (DEVICE2020.txt) and lowercase (device2020.txt) filenames.

    Args:
        directory: Directory to search in.
        pattern: Glob pattern (e.g., "device*.txt").

    Returns:
        List of matching Path objects, sorted alphabetically.
    """
    regex_pattern = fnmatch.translate(pattern)
    regex = re.compile(regex_pattern, re.IGNORECASE)

    matches = []
    try:
        for item in directory.iterdir():
            if item.is_file() and regex.match(item.name):
                matches.append(item)
    except OSError:
        pass

    return sorted(matches)
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from src.database import get_connection, initialize_database
from src.ingestion.download import MAUDEDownloader
from src.ingestion.loader import MAUDELoader
from src.ingestion.validators import FileValidator, validate_all_files

logger = get_logger("full_reload")


@dataclass
class ReloadCheckpoint:
    """Checkpoint for resumable reload."""
    started_at: str
    phase: str = "init"
    completed_phases: List[str] = field(default_factory=list)
    loaded_files: Dict[str, List[str]] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    last_updated: str = ""

    def save(self, filepath: Path) -> None:
        """Save checkpoint to file."""
        self.last_updated = datetime.now().isoformat()
        with open(filepath, "w") as f:
            json.dump(asdict(self), f, indent=2)

    @classmethod
    def load(cls, filepath: Path) -> "ReloadCheckpoint":
        """Load checkpoint from file."""
        with open(filepath, "r") as f:
            data = json.load(f)
        return cls(**data)


@dataclass
class ReloadResult:
    """Result of full reload operation."""
    success: bool = False
    started_at: str = ""
    completed_at: str = ""
    phases_completed: List[str] = field(default_factory=list)
    records_by_type: Dict[str, int] = field(default_factory=dict)
    validation_results: Dict = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


def backup_database(db_path: Path) -> Optional[Path]:
    """
    Create a backup of the current database.

    Args:
        db_path: Path to database file.

    Returns:
        Path to backup file, or None if no backup needed.
    """
    if not db_path.exists():
        logger.info("No existing database to backup")
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.with_suffix(f".backup_{timestamp}.duckdb")

    logger.info(f"Backing up database to: {backup_path}")
    shutil.copy2(db_path, backup_path)

    return backup_path


def reset_database(db_path: Path) -> None:
    """
    Drop all tables and recreate schema.

    Args:
        db_path: Path to database file.
    """
    logger.info("Resetting database schema...")

    with get_connection(db_path) as conn:
        # Drop all tables
        tables = [
            "ingestion_log",
            "patient_problems",
            "device_problems",
            "mdr_text",
            "patients",
            "devices",
            "master_events",
            "asr_patient_problems",
            "asr_reports",
            "den_reports",
            "manufacturer_disclaimers",
            "problem_codes",
            "patient_problem_codes",
            "product_codes",
            "manufacturers",
            "daily_aggregates",
        ]

        for table in tables:
            try:
                conn.execute(f"DROP TABLE IF EXISTS {table}")
            except Exception as e:
                logger.warning(f"Could not drop {table}: {e}")

        # Recreate schema
        initialize_database(conn)

    logger.info("Database schema reset complete")


def download_missing_files(
    data_dir: Path,
    file_types: Optional[List[str]] = None,
    years: Optional[List[int]] = None,
    prioritize_device: bool = True,
) -> Dict[str, List[str]]:
    """
    Download any missing files.

    Args:
        data_dir: Directory for downloads.
        file_types: Types to download (None = all).
        years: Specific years (None = all).
        prioritize_device: Download device files first.

    Returns:
        Dictionary of downloaded files by type.
    """
    logger.info("Checking for missing files...")

    downloader = MAUDEDownloader(output_dir=data_dir, use_extended_files=True)
    missing = downloader.check_for_updates()

    if not missing:
        logger.info("All files already downloaded")
        return {}

    # Prioritize device files if requested
    if prioritize_device and "device" in missing:
        logger.info(f"Downloading {len(missing['device'])} missing device files first...")
        downloader.download_file_type("device", years=years)

        # Update missing
        missing = downloader.check_for_updates()

    # Download remaining
    downloaded = {}
    download_order = ["master", "patient", "text", "problem", "asr", "den"]

    for file_type in download_order:
        if file_types and file_type not in file_types:
            continue
        if file_type in missing:
            logger.info(f"Downloading {len(missing[file_type])} {file_type} files...")
            results = downloader.download_file_type(file_type, years=years)
            downloaded[file_type] = [r.filename for r in results if r.success]

    return downloaded


def validate_files_before_load(data_dir: Path) -> Dict[str, bool]:
    """
    Validate files before loading.

    Args:
        data_dir: Directory containing files.

    Returns:
        Dictionary mapping file type to validation status.
    """
    logger.info("Validating files before load...")

    results = validate_all_files(data_dir, file_types=["master", "device"])
    status = {}

    for file_type, validations in results.items():
        all_valid = all(v.is_valid for v in validations)
        status[file_type] = all_valid

        invalid_count = sum(1 for v in validations if not v.is_valid)
        if invalid_count > 0:
            logger.warning(f"{file_type}: {invalid_count} files failed validation")

    return status


def load_all_data(
    data_dir: Path,
    db_path: Path,
    checkpoint: Optional[ReloadCheckpoint] = None,
) -> Dict[str, int]:
    """
    Load all MAUDE data in correct order.

    CRITICAL: Device files MUST be loaded FIRST.
    Only device files contain MANUFACTURER_D_NAME and DEVICE_REPORT_PRODUCT_CODE.
    Master files do NOT have this data.

    Args:
        data_dir: Directory containing files.
        db_path: Path to database.
        checkpoint: Optional checkpoint for resumption.

    Returns:
        Dictionary mapping file type to record count.
    """
    logger.info("Loading all MAUDE data...")
    logger.info("IMPORTANT: Loading DEVICE files first (they contain manufacturer data)")

    loader = MAUDELoader(db_path=db_path)

    # Loading order is CRITICAL
    # 1. Device first (contains manufacturer and product code)
    # 2. Master (core event data)
    # 3. Patient, Text, Problem (child tables)
    # 4. Lookup tables, ASR, DEN
    load_order = [
        "device",              # MUST BE FIRST - has manufacturer data
        "master",              # Core events
        "patient",             # Patient outcomes
        "text",                # Narratives
        "problem",             # Device problems
        "patient_problem",     # Patient problems
        "problem_lookup",      # Problem code descriptions
        "patient_problem_data", # Patient problem descriptions
        "asr",                 # Alternative Summary Reports
        "asr_ppc",             # ASR patient problems
        "den",                 # Legacy DEN data
        "disclaimer",          # Manufacturer disclaimers
    ]

    # Filter by checkpoint if resuming
    if checkpoint and checkpoint.loaded_files:
        already_loaded = set()
        for files in checkpoint.loaded_files.values():
            already_loaded.update(files)
        logger.info(f"Resuming from checkpoint - {len(already_loaded)} files already loaded")

    records_by_type = {}

    with get_connection(db_path) as conn:
        for file_type in load_order:
            logger.info(f"\n{'='*50}")
            logger.info(f"Loading {file_type.upper()} files...")
            logger.info(f"{'='*50}")

            try:
                pattern = loader._get_file_pattern(file_type)
                # Use case-insensitive glob to handle DEVICE2020.txt vs device2020.txt
                files = glob_case_insensitive(data_dir, pattern)

                # Filter out problem files from device pattern
                if file_type == "device":
                    files = [f for f in files if "problem" not in f.name.lower()]
                    # Also include device{year}.txt files (case-insensitive)
                    device_year_files = glob_case_insensitive(data_dir, "device*.txt")
                    device_year_files = [f for f in device_year_files if "problem" not in f.name.lower()]
                    files = sorted(set(files + device_year_files))

                # For ASR, exclude ppc files
                if file_type == "asr":
                    files = [f for f in files if "ppc" not in f.name.lower()]

                # For DEN, filter to valid year files
                if file_type == "den":
                    valid_files = []
                    for f in files:
                        name = f.name.lower()
                        if name.startswith("mdr") and len(name) <= 9:
                            year_part = name[3:5]
                            if year_part.isdigit():
                                year = int(year_part)
                                if 84 <= year <= 97:
                                    valid_files.append(f)
                        elif name == "disclaim.txt":
                            valid_files.append(f)
                    files = valid_files

                if not files:
                    logger.warning(f"No {file_type} files found")
                    continue

                logger.info(f"Found {len(files)} {file_type} files")

                type_total = 0
                for filepath in files:
                    try:
                        result = loader.load_file(filepath, file_type, conn)
                        type_total += result.records_loaded

                        if checkpoint:
                            if file_type not in checkpoint.loaded_files:
                                checkpoint.loaded_files[file_type] = []
                            checkpoint.loaded_files[file_type].append(filepath.name)

                    except Exception as e:
                        logger.error(f"Error loading {filepath.name}: {e}")
                        if checkpoint:
                            checkpoint.errors.append(f"{filepath.name}: {e}")

                records_by_type[file_type] = type_total
                logger.info(f"Loaded {type_total:,} {file_type} records")

            except Exception as e:
                logger.error(f"Error loading {file_type} files: {e}")
                records_by_type[file_type] = 0

    return records_by_type


def populate_master_from_devices(db_path: Path) -> tuple[int, int]:
    """
    Copy manufacturer and product code from devices to master_events.

    This is the CRITICAL step that populates manufacturer_clean and product_code
    in master_events. These fields come from device files, not master files.

    Args:
        db_path: Path to database.

    Returns:
        Tuple of (manufacturer_records, product_code_records) updated.
    """
    logger.info("\n" + "="*50)
    logger.info("POPULATING MASTER EVENTS FROM DEVICES")
    logger.info("="*50)
    logger.info("This copies manufacturer and product_code from devices to master_events")

    loader = MAUDELoader(db_path=db_path)

    with get_connection(db_path) as conn:
        mfr_updated, product_updated = loader.populate_master_from_devices(conn)

    logger.info(f"Updated {mfr_updated:,} manufacturer records")
    logger.info(f"Updated {product_updated:,} product_code records")

    return mfr_updated, product_updated


def run_validation(db_path: Path) -> Dict:
    """
    Run validation queries and return results.

    Args:
        db_path: Path to database.

    Returns:
        Dictionary with validation results.
    """
    logger.info("\n" + "="*50)
    logger.info("RUNNING VALIDATION")
    logger.info("="*50)

    results = {}

    with get_connection(db_path, read_only=True) as conn:
        # Table counts
        tables = ["master_events", "devices", "patients", "mdr_text", "device_problems"]
        results["table_counts"] = {}
        for table in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            results["table_counts"][table] = count
            logger.info(f"  {table}: {count:,} records")

        # Manufacturer coverage
        mfr_stats = conn.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(manufacturer_clean) as with_mfr,
                ROUND(COUNT(manufacturer_clean) * 100.0 / COUNT(*), 1) as pct
            FROM master_events
        """).fetchone()

        results["manufacturer_coverage"] = {
            "total": mfr_stats[0],
            "with_manufacturer": mfr_stats[1],
            "percent": mfr_stats[2],
        }
        logger.info(f"\nManufacturer coverage: {mfr_stats[2]:.1f}%")

        # Coverage by year
        by_year = conn.execute("""
            SELECT
                EXTRACT(YEAR FROM date_received)::INTEGER as year,
                COUNT(*) as total,
                COUNT(manufacturer_clean) as with_mfr,
                ROUND(COUNT(manufacturer_clean) * 100.0 / COUNT(*), 1) as pct
            FROM master_events
            WHERE date_received IS NOT NULL
            GROUP BY 1
            ORDER BY 1
        """).fetchall()

        results["coverage_by_year"] = []
        logger.info("\nCoverage by year:")
        for row in by_year:
            if row[0]:
                year_data = {
                    "year": int(row[0]),
                    "total": row[1],
                    "with_manufacturer": row[2],
                    "percent": row[3],
                }
                results["coverage_by_year"].append(year_data)

                status = "OK" if row[3] >= 90 else "LOW" if row[3] >= 50 else "CRITICAL"
                logger.info(f"  {row[0]}: {row[3]:.1f}% [{status}]")

        # Success criteria
        results["success"] = mfr_stats[2] >= 90
        if results["success"]:
            logger.info("\n SUCCESS: Manufacturer coverage >= 90%")
        else:
            logger.warning(f"\n BELOW TARGET: Manufacturer coverage {mfr_stats[2]:.1f}% (target: 90%)")

    return results


def run_full_reload(
    data_dir: Path,
    db_path: Path,
    skip_download: bool = False,
    skip_backup: bool = False,
    years: Optional[List[int]] = None,
    checkpoint_path: Optional[Path] = None,
) -> ReloadResult:
    """
    Execute the full reload process.

    Args:
        data_dir: Directory containing/for data files.
        db_path: Path to database.
        skip_download: Skip downloading files.
        skip_backup: Skip database backup.
        years: Specific years to process.
        checkpoint_path: Path to checkpoint file for resumption.

    Returns:
        ReloadResult with complete status.
    """
    result = ReloadResult(started_at=datetime.now().isoformat())

    # Load or create checkpoint
    checkpoint = None
    if checkpoint_path and checkpoint_path.exists():
        checkpoint = ReloadCheckpoint.load(checkpoint_path)
        logger.info(f"Resuming from checkpoint: phase={checkpoint.phase}")
    else:
        checkpoint = ReloadCheckpoint(started_at=result.started_at)
        if checkpoint_path:
            checkpoint.save(checkpoint_path)

    try:
        # Phase 1: Backup
        if "backup" not in checkpoint.completed_phases:
            if not skip_backup:
                logger.info("\n" + "="*60)
                logger.info("PHASE 1: BACKUP")
                logger.info("="*60)
                backup_path = backup_database(db_path)
                if backup_path:
                    logger.info(f"Backup created: {backup_path}")

            checkpoint.completed_phases.append("backup")
            checkpoint.phase = "reset"
            if checkpoint_path:
                checkpoint.save(checkpoint_path)

        # Phase 2: Reset Database
        if "reset" not in checkpoint.completed_phases:
            logger.info("\n" + "="*60)
            logger.info("PHASE 2: RESET DATABASE")
            logger.info("="*60)
            reset_database(db_path)

            checkpoint.completed_phases.append("reset")
            checkpoint.phase = "download"
            if checkpoint_path:
                checkpoint.save(checkpoint_path)

        # Phase 3: Download
        if "download" not in checkpoint.completed_phases:
            if not skip_download:
                logger.info("\n" + "="*60)
                logger.info("PHASE 3: DOWNLOAD MISSING FILES")
                logger.info("="*60)
                download_missing_files(data_dir, years=years, prioritize_device=True)

            checkpoint.completed_phases.append("download")
            checkpoint.phase = "validate"
            if checkpoint_path:
                checkpoint.save(checkpoint_path)

        # Phase 4: Validate
        if "validate" not in checkpoint.completed_phases:
            logger.info("\n" + "="*60)
            logger.info("PHASE 4: VALIDATE FILES")
            logger.info("="*60)
            validation_status = validate_files_before_load(data_dir)

            for file_type, is_valid in validation_status.items():
                if not is_valid:
                    result.warnings.append(f"Some {file_type} files failed validation")

            checkpoint.completed_phases.append("validate")
            checkpoint.phase = "load"
            if checkpoint_path:
                checkpoint.save(checkpoint_path)

        # Phase 5: Load Data
        if "load" not in checkpoint.completed_phases:
            logger.info("\n" + "="*60)
            logger.info("PHASE 5: LOAD DATA")
            logger.info("="*60)
            result.records_by_type = load_all_data(data_dir, db_path, checkpoint)

            checkpoint.completed_phases.append("load")
            checkpoint.phase = "populate"
            if checkpoint_path:
                checkpoint.save(checkpoint_path)

        # Phase 6: Populate Master from Devices
        if "populate" not in checkpoint.completed_phases:
            logger.info("\n" + "="*60)
            logger.info("PHASE 6: POPULATE MASTER FROM DEVICES")
            logger.info("="*60)
            mfr_updated, product_updated = populate_master_from_devices(db_path)

            checkpoint.completed_phases.append("populate")
            checkpoint.phase = "validate_final"
            if checkpoint_path:
                checkpoint.save(checkpoint_path)

        # Phase 7: Final Validation
        logger.info("\n" + "="*60)
        logger.info("PHASE 7: FINAL VALIDATION")
        logger.info("="*60)
        result.validation_results = run_validation(db_path)
        result.success = result.validation_results.get("success", False)

        checkpoint.completed_phases.append("validate_final")
        checkpoint.phase = "complete"
        if checkpoint_path:
            checkpoint.save(checkpoint_path)

        result.phases_completed = checkpoint.completed_phases

    except Exception as e:
        logger.error(f"Reload failed: {e}")
        result.errors.append(str(e))
        result.success = False

    result.completed_at = datetime.now().isoformat()
    return result


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Full MAUDE database reload",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--data-dir",
        type=Path,
        default=config.data.raw_path,
        help="Directory containing/for data files",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=config.database.path,
        help="Path to database file",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading files",
    )
    parser.add_argument(
        "--skip-backup",
        action="store_true",
        help="Skip database backup",
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        help="Specific years to process",
    )
    parser.add_argument(
        "--checkpoint",
        type=Path,
        help="Checkpoint file for resumable reload",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        help="Save results to JSON file",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )

    args = parser.parse_args()

    setup_logging(log_level=args.log_level)

    logger.info("="*60)
    logger.info("MAUDE FULL DATABASE RELOAD")
    logger.info("="*60)
    logger.info(f"Data directory: {args.data_dir}")
    logger.info(f"Database: {args.db}")
    if args.years:
        logger.info(f"Years: {args.years}")

    start_time = time.time()

    # Run reload
    result = run_full_reload(
        data_dir=args.data_dir,
        db_path=args.db,
        skip_download=args.skip_download,
        skip_backup=args.skip_backup,
        years=args.years,
        checkpoint_path=args.checkpoint,
    )

    elapsed = time.time() - start_time

    # Print summary
    print("\n" + "="*60)
    print("RELOAD COMPLETE")
    print("="*60)
    print(f"Status: {'SUCCESS' if result.success else 'FAILED'}")
    print(f"Duration: {elapsed/60:.1f} minutes")
    print(f"Phases completed: {len(result.phases_completed)}")

    if result.records_by_type:
        print("\nRecords loaded:")
        for file_type, count in result.records_by_type.items():
            print(f"  {file_type}: {count:,}")

    if result.validation_results:
        mfr = result.validation_results.get("manufacturer_coverage", {})
        print(f"\nManufacturer coverage: {mfr.get('percent', 0):.1f}%")

    if result.errors:
        print("\nErrors:")
        for error in result.errors:
            print(f"  - {error}")

    if result.warnings:
        print("\nWarnings:")
        for warning in result.warnings:
            print(f"  - {warning}")

    # Save results
    if args.output:
        with open(args.output, "w") as f:
            json.dump(asdict(result), f, indent=2, default=str)
        print(f"\nResults saved to: {args.output}")

    print("="*60)

    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
