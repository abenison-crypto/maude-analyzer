#!/usr/bin/env python3
"""
Full Data Reload Script - Fixes quote-swallowing and embedded newline issues.

This script:
1. Uses QUOTE_NONE parsing to prevent quote-swallowing
2. Handles embedded newlines by rejoining split records
3. Reloads all affected file types in correct order
4. Validates record counts after each load
5. Reports comprehensive statistics

File loading order (important for referential integrity):
1. devices (first - needed to populate manufacturer in master)
2. master_events
3. patients
4. mdr_text
5. device_problems

IMPORTANT: The parser.py has been fixed to use quoting=csv.QUOTE_NONE
instead of quotechar='"'. This script uses the MAUDELoader which
leverages the fixed parser to ensure no quote-swallowing occurs.

Usage:
    python scripts/full_data_reload.py
    python scripts/full_data_reload.py --data-dir data/raw --db data/maude.duckdb
    python scripts/full_data_reload.py --types device master --dry-run
"""

import sys
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger
from src.database import get_connection, initialize_database
from src.ingestion.loader import MAUDELoader
from src.ingestion.parser import count_physical_lines

logger = get_logger("full_data_reload")


def get_file_sort_key(filepath: Path) -> tuple:
    """
    Return a sort key for proper file loading order.

    Order:
    1. Thru/historical files (e.g., foidevthru1997.txt, mdrfoiThru2023.txt)
    2. Annual files by year (e.g., foidev1998.txt, device2020.txt)
    3. Current year base file (e.g., foidev.txt, mdrfoi.txt, patient.txt)
    4. Add files (e.g., foidevAdd.txt, mdrfoiAdd.txt)
    5. Change files (e.g., devicechange.txt, mdrfoiChange.txt)

    This ensures Add files come before Change files.
    """
    name = filepath.name.lower()

    # Priority 1: Thru files (historical aggregates)
    if "thru" in name:
        # Extract year for sorting within thru files
        match = re.search(r'thru(\d{4})', name)
        year = int(match.group(1)) if match else 0
        return (1, year, name)

    # Priority 5: Change files (must be loaded LAST)
    if "change" in name:
        return (5, 0, name)

    # Priority 4: Add files (must be loaded after base, before change)
    if "add" in name:
        return (4, 0, name)

    # Priority 2: Annual files with year
    year_match = re.search(r'(\d{4})', name)
    if year_match:
        year = int(year_match.group(1))
        return (2, year, name)

    # Priority 3: Current year base file (no year in name)
    # e.g., foidev.txt, mdrfoi.txt, patient.txt, foitext.txt
    return (3, 0, name)


def select_files_for_load(files: List[Path], file_type: str) -> List[Path]:
    """
    Select the correct subset of files to load for a given file type.

    CRITICAL: This function fixes the bug where ALL Thru files were loaded
    instead of just the latest one for cumulative file types.

    File Type Behaviors:
    - master, patient: Cumulative Thru files - only load LATEST Thru file
    - device: Incremental Thru + annual files - load ALL (no overlap)
    - text: Incremental annual files - load ALL (no overlap)

    For all types:
    - Load current file (e.g., mdrfoi.txt)
    - Load Add file (e.g., mdrfoiAdd.txt)
    - Load Change file LAST (e.g., mdrfoiChange.txt)

    Args:
        files: List of file paths matching the file type pattern
        file_type: Type of file (master, device, patient, text, problem)

    Returns:
        Filtered and correctly ordered list of files to load
    """
    import re

    if not files:
        return []

    # Categorize files
    thru_files = []
    annual_files = []
    current_files = []
    add_files = []
    change_files = []

    for f in files:
        name = f.name.lower()

        if "thru" in name:
            # Extract year from thru file
            match = re.search(r'thru(\d{4})', name, re.IGNORECASE)
            year = int(match.group(1)) if match else 0
            thru_files.append((year, f))
        elif "change" in name:
            change_files.append(f)
        elif "add" in name:
            add_files.append(f)
        elif re.search(r'\d{4}', name):
            # Annual file with year
            year_match = re.search(r'(\d{4})', name)
            year = int(year_match.group(1)) if year_match else 0
            annual_files.append((year, f))
        else:
            # Current file (no year in name)
            current_files.append(f)

    result = []

    # Handle Thru files based on file type
    if thru_files:
        if file_type in ["master", "patient"]:
            # CUMULATIVE: Only load the LATEST Thru file
            # mdrfoiThru2025 contains ALL data through 2025 (supersedes Thru2023)
            thru_files.sort(key=lambda x: x[0], reverse=True)
            latest_thru = thru_files[0]
            result.append(latest_thru[1])

            # Log if we're skipping older Thru files
            if len(thru_files) > 1:
                skipped = [f[1].name for f in thru_files[1:]]
                logger.info(
                    f"Using latest cumulative file {latest_thru[1].name}, "
                    f"skipping older: {skipped}"
                )
        else:
            # INCREMENTAL (device, text): Load all Thru files in order
            # foidevthru1997 is pre-1998 data, separate from annual files
            thru_files.sort(key=lambda x: x[0])
            result.extend([f[1] for f in thru_files])

    # Add annual files in chronological order
    if annual_files:
        annual_files.sort(key=lambda x: x[0])
        result.extend([f[1] for f in annual_files])

    # Add current file (base file without year)
    result.extend(current_files)

    # Add files come BEFORE Change files (critical ordering)
    result.extend(add_files)
    result.extend(change_files)

    return result


def get_files_by_type(data_dir: Path, file_types: List[str]) -> Dict[str, List[Path]]:
    """
    Collect files to process by type, with correct file selection and ordering.

    CRITICAL FIX: This function now uses select_files_for_load() to:
    1. For cumulative types (master, patient): Select ONLY the latest Thru file
    2. For incremental types (device, text): Load ALL files in chronological order
    3. Always ensure Add files load BEFORE Change files

    Loading order per type:
    1. Latest Thru file (for cumulative) OR all Thru files (for incremental)
    2. Annual files (by year ascending)
    3. Current year base file
    4. Add files (new records)
    5. Change files (modifications) - MUST be last
    """
    file_patterns = {
        "device": ["foidev*.txt", "device*.txt"],
        "master": ["mdrfoi*.txt"],
        "patient": ["patient*.txt"],
        "text": ["foitext*.txt"],
        "problem": ["foidevproblem*.txt"],
    }

    files_by_type = {}
    for ftype in file_types:
        files = []
        for pattern in file_patterns.get(ftype, []):
            files.extend(data_dir.glob(pattern))

        # Filter out problem files from device patterns
        if ftype == "device":
            files = [f for f in files if "problem" not in f.name.lower()]

        # Filter out patient problem files from patient patterns
        if ftype == "patient":
            files = [f for f in files if "problem" not in f.name.lower()]

        # Remove duplicates
        files = list(set(files))

        # CRITICAL: Apply file selection logic to pick correct files
        # This fixes the bug where ALL Thru files were loaded
        files = select_files_for_load(files, ftype)

        if files:
            files_by_type[ftype] = files
            logger.info(f"Selected {len(files)} {ftype} files for loading")

    return files_by_type


def run_full_reload(
    data_dir: Path,
    db_path: Path,
    file_types: Optional[List[str]] = None,
    batch_size: int = 10000,
):
    """
    Run full data reload using MAUDELoader with fixed parsing.

    Args:
        data_dir: Directory containing source files
        db_path: Path to database
        file_types: Types to reload (default: all)
        batch_size: Records per batch
    """
    logger.info("=" * 80)
    logger.info("FULL DATA RELOAD WITH FIXED PARSING")
    logger.info("=" * 80)
    logger.info(f"Data directory: {data_dir}")
    logger.info(f"Database: {db_path}")
    logger.info(f"Batch size: {batch_size}")

    if file_types is None:
        file_types = ["device", "master", "patient", "text", "problem"]

    logger.info(f"File types: {', '.join(file_types)}")

    # Collect files to process
    files_by_type = get_files_by_type(data_dir, file_types)

    for ftype, files in files_by_type.items():
        logger.info(f"Found {len(files)} {ftype} files")

    # Initialize database schema
    logger.info("Initializing database schema...")
    with get_connection(db_path) as conn:
        initialize_database(conn)
    logger.info("Database schema initialized")

    # Initialize loader with fixed parser
    loader = MAUDELoader(
        db_path=db_path,
        batch_size=batch_size,
        enable_transaction_safety=True,
        enable_validation=True,
    )

    # Track results
    all_results = {}
    start_time = datetime.now()

    # Process files in order
    for ftype in file_types:
        files = files_by_type.get(ftype, [])
        if not files:
            logger.warning(f"No {ftype} files found")
            continue

        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {len(files)} {ftype.upper()} files")
        logger.info("=" * 60)

        type_results = []
        for filepath in files:
            logger.info(f"\nLoading {filepath.name}...")

            # Count physical lines first for validation
            try:
                total_lines, valid_data, orphans = count_physical_lines(filepath)
                logger.info(f"  Physical lines: {total_lines:,} (valid: {valid_data:,}, orphans: {orphans})")
            except Exception as e:
                logger.warning(f"  Could not count lines: {e}")

            # Load using MAUDELoader
            try:
                result = loader.load_file(filepath, ftype)
                type_results.append({
                    'filename': filepath.name,
                    'physical_lines': valid_data if 'valid_data' in dir() else 0,
                    'records_loaded': result.records_loaded,
                    'records_skipped': result.records_skipped,
                    'records_errors': result.records_errors,
                    'duration': result.duration_seconds,
                    'status': 'OK' if result.records_errors == 0 else 'WARNING',
                })

                logger.info(
                    f"  Loaded: {result.records_loaded:,}, "
                    f"Skipped: {result.records_skipped:,}, "
                    f"Errors: {result.records_errors:,}, "
                    f"Duration: {result.duration_seconds:.1f}s"
                )

                # Check for validation issues
                if result.stage3_validation and not result.stage3_validation.passed:
                    for issue in result.stage3_validation.issues:
                        if issue.severity == "CRITICAL":
                            logger.error(f"  CRITICAL: {issue.message}")

            except Exception as e:
                logger.error(f"  FAILED: {e}")
                type_results.append({
                    'filename': filepath.name,
                    'physical_lines': 0,
                    'records_loaded': 0,
                    'records_skipped': 0,
                    'records_errors': 1,
                    'duration': 0,
                    'status': 'ERROR',
                    'error': str(e),
                })

        all_results[ftype] = type_results

    # Populate manufacturer data in master from devices
    if "device" in file_types and "master" in file_types:
        logger.info("\nPopulating master_events with manufacturer data from devices...")
        try:
            with get_connection(db_path) as conn:
                loader.populate_master_from_devices(conn)
        except Exception as e:
            logger.error(f"Failed to populate manufacturer data: {e}")

    # Print summary
    total_duration = (datetime.now() - start_time).total_seconds()

    print("\n" + "=" * 80)
    print("RELOAD SUMMARY")
    print("=" * 80)
    print(f"Total duration: {total_duration / 60:.1f} minutes")

    total_loaded = 0
    total_expected = 0
    total_errors = 0

    for ftype in file_types:
        type_results = all_results.get(ftype, [])
        if type_results:
            loaded = sum(r['records_loaded'] for r in type_results)
            expected = sum(r['physical_lines'] for r in type_results)
            errors = sum(r['records_errors'] for r in type_results)

            total_loaded += loaded
            total_expected += expected
            total_errors += errors

            variance = ((expected - loaded) / expected * 100) if expected > 0 else 0

            print(f"\n{ftype.upper()}:")
            print(f"  Files: {len(type_results)}")
            print(f"  Expected: {expected:,}")
            print(f"  Loaded: {loaded:,}")
            print(f"  Errors: {errors:,}")
            print(f"  Variance: {variance:.2f}%")

            # Show problem files
            problems = [r for r in type_results if r['status'] != 'OK']
            if problems:
                print(f"  Problem files:")
                for p in problems[:5]:
                    msg = p.get('error', f"{p['records_errors']} errors")
                    print(f"    - {p['filename']}: {msg}")

    print(f"\nTOTAL:")
    print(f"  Expected records: {total_expected:,}")
    print(f"  Loaded records: {total_loaded:,}")
    print(f"  Errors: {total_errors:,}")
    if total_expected > 0:
        print(f"  Overall variance: {((total_expected - total_loaded) / total_expected * 100):.2f}%")

    # Final database stats
    print("\n" + "-" * 40)
    print("DATABASE TABLE COUNTS:")
    print("-" * 40)
    try:
        with get_connection(db_path, read_only=True) as conn:
            for table in ["master_events", "devices", "patients", "mdr_text", "device_problems"]:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"  {table}: {count:,}")
    except Exception as e:
        print(f"  Error getting counts: {e}")

    return all_results


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Full data reload with fixed parsing")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory containing source files"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=config.database.path,
        help="Database path"
    )
    parser.add_argument(
        "--types",
        nargs="+",
        choices=["device", "master", "patient", "text", "problem"],
        help="File types to reload (default: all)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Records per batch (default: 10000)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be reloaded without doing it"
    )

    args = parser.parse_args()

    if args.dry_run:
        logger.info("DRY RUN MODE - no changes will be made")
        files_by_type = get_files_by_type(args.data_dir, args.types or ["device", "master", "patient", "text", "problem"])

        for ftype, files in files_by_type.items():
            print(f"\n{ftype.upper()} ({len(files)} files):")
            for f in files:
                try:
                    total, valid, orphans = count_physical_lines(f)
                    print(f"  {f.name}: {valid:,} valid lines ({orphans} orphans)")
                except Exception as e:
                    print(f"  {f.name}: ERROR - {e}")
    else:
        run_full_reload(
            args.data_dir,
            args.db,
            args.types,
            args.batch_size,
        )


if __name__ == "__main__":
    main()
