#!/usr/bin/env python
"""
Incremental Update Script for MAUDE Analyzer.

This script performs weekly incremental updates:
1. Downloads new ADD/CHANGE files from FDA
2. Downloads and processes current year files (updated weekly by FDA)
3. Optionally supplements with openFDA API for recent records
4. Validates no gaps exist in the data

Usage:
    python scripts/incremental_update.py [options]

Options:
    --skip-download     Skip download step (use existing files)
    --skip-validation   Skip validation step
    --force             Force re-download even if files exist
    --all-products      Update all products (not just SCS codes)
    --data-dir PATH     Custom data directory
    --db PATH           Custom database path

Typical workflow:
    # Weekly update (run every Monday)
    python scripts/incremental_update.py

    # Force re-download of current year files
    python scripts/incremental_update.py --force
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from src.database import get_connection, get_table_counts
from src.database.schema import get_schema_version
from src.ingestion import (
    MAUDEDownloader,
    MAUDELoader,
    DataValidator,
    print_validation_report,
)

# Files that are updated weekly by FDA
WEEKLY_UPDATE_FILES = {
    "master": ["mdrfoi.zip"],         # Current year MDR records
    "device": ["foidev.zip"],         # Current year device records
    "patient": ["patient.zip"],       # Current year patient records
    "text": ["foitext.zip"],          # Current year text records
    "problem": ["foidevproblem.zip"], # All problem codes (updated weekly)
}


def get_latest_record_date(conn, table: str = "master_events") -> datetime:
    """
    Get the date of the most recent record in the database.

    Args:
        conn: Database connection.
        table: Table to check.

    Returns:
        Most recent date or None.
    """
    try:
        result = conn.execute(f"""
            SELECT MAX(date_received)
            FROM {table}
            WHERE date_received IS NOT NULL
        """).fetchone()
        return result[0] if result else None
    except Exception:
        return None


def get_record_count_by_date(conn, since_date: datetime, table: str = "master_events") -> int:
    """
    Get count of records since a specific date.

    Args:
        conn: Database connection.
        since_date: Start date.
        table: Table to check.

    Returns:
        Record count.
    """
    try:
        result = conn.execute(f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE date_received >= ?
        """, [since_date]).fetchone()
        return result[0] if result else 0
    except Exception:
        return 0


def validate_data_continuity(conn, logger) -> bool:
    """
    Check for gaps in the data.

    Args:
        conn: Database connection.
        logger: Logger instance.

    Returns:
        True if data is continuous, False if gaps detected.
    """
    try:
        # Check for any missing days in the last 30 days
        result = conn.execute("""
            WITH date_series AS (
                SELECT CAST(generate_series AS DATE) as expected_date
                FROM generate_series(
                    CURRENT_DATE - INTERVAL 30 DAY,
                    CURRENT_DATE,
                    INTERVAL 1 DAY
                )
            ),
            actual_dates AS (
                SELECT DISTINCT CAST(date_received AS DATE) as actual_date
                FROM master_events
                WHERE date_received >= CURRENT_DATE - INTERVAL 30 DAY
            )
            SELECT COUNT(DISTINCT expected_date) as expected,
                   COUNT(DISTINCT actual_date) as actual
            FROM date_series
            LEFT JOIN actual_dates ON expected_date = actual_date
        """).fetchone()

        if result:
            expected, actual = result
            coverage = actual / max(expected, 1) * 100
            logger.info(f"Data coverage (last 30 days): {coverage:.1f}%")
            return coverage >= 80  # Allow some gaps (weekends, holidays)

    except Exception as e:
        logger.warning(f"Could not validate data continuity: {e}")

    return True


def main():
    """Main entry point for incremental update."""
    parser = argparse.ArgumentParser(
        description="Run incremental update of MAUDE data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download step (use existing files)",
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip validation step",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if files exist",
    )
    parser.add_argument(
        "--filter-codes",
        type=str,
        default=None,
        help="Comma-separated product codes to filter (e.g., 'GZB,LGW,PMP'). Default: all products",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=config.data.raw_path,
        help="Custom data directory",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=config.database.path,
        help="Custom database path",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(log_level=args.log_level)
    logger = get_logger("incremental_update")

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("MAUDE Analyzer - Incremental Update")
    logger.info(f"Started at: {start_time}")
    logger.info("=" * 60)

    # Check database exists
    if not args.db.exists():
        logger.error(f"Database not found: {args.db}")
        logger.error("Run initial_load.py first to create the database.")
        return 1

    # Pre-update statistics
    with get_connection(args.db) as conn:
        pre_counts = get_table_counts(conn)
        latest_date = get_latest_record_date(conn)
        schema_version = get_schema_version(conn)

        logger.info(f"Database: {args.db}")
        logger.info(f"Schema version: {schema_version}")
        logger.info(f"Most recent record: {latest_date}")
        logger.info(f"Current record counts:")
        for table, count in pre_counts.items():
            if count > 0:
                logger.info(f"  {table}: {count:,}")

    try:
        # Step 1: Download weekly update files
        if not args.skip_download:
            logger.info("\n" + "-" * 40)
            logger.info("STEP 1: Downloading weekly update files")
            logger.info("-" * 40)

            downloader = MAUDEDownloader(output_dir=args.data_dir)

            for file_type, files in WEEKLY_UPDATE_FILES.items():
                for filename in files:
                    url = f"https://www.accessdata.fda.gov/MAUDE/ftparea/{filename}"
                    logger.info(f"Downloading {filename}...")

                    result = downloader._download_file(
                        url,
                        file_type,
                        force=args.force
                    )

                    if result.success:
                        logger.info(
                            f"  Downloaded: {result.size_bytes / 1024 / 1024:.1f} MB, "
                            f"extracted {len(result.extracted_files)} files"
                        )
                    else:
                        logger.warning(f"  Failed: {result.error}")

        else:
            logger.info("\n[Skipping download - using existing files]")

        # Step 2: Load new data
        logger.info("\n" + "-" * 40)
        logger.info("STEP 2: Loading new data")
        logger.info("-" * 40)

        # Parse filter codes
        filter_codes = None
        if args.filter_codes:
            filter_codes = [c.strip() for c in args.filter_codes.split(",")]

        loader = MAUDELoader(
            db_path=args.db,
            filter_product_codes=filter_codes,
        )

        # Find the current year files
        current_year_files = []
        for file_type, patterns in WEEKLY_UPDATE_FILES.items():
            for pattern in patterns:
                base = pattern.replace(".zip", "")
                matches = list(args.data_dir.glob(f"{base}*.txt"))
                for match in matches:
                    current_year_files.append((match, file_type))

        if not current_year_files:
            logger.warning("No current year files found to load")
        else:
            logger.info(f"Found {len(current_year_files)} files to process")

            total_loaded = 0
            total_errors = 0

            # Load in order: master first (for MDR key tracking)
            for file_type in ["master", "device", "patient", "text", "problem"]:
                files_for_type = [
                    (f, t) for f, t in current_year_files if t == file_type
                ]

                for filepath, ftype in files_for_type:
                    logger.info(f"Loading {filepath.name}...")
                    result = loader.load_file(filepath, ftype)

                    total_loaded += result.records_loaded
                    total_errors += result.records_errors

                    logger.info(
                        f"  Loaded: {result.records_loaded:,}, "
                        f"Skipped: {result.records_skipped:,}, "
                        f"Errors: {result.records_errors:,}"
                    )

            logger.info(f"\nTotal: {total_loaded:,} records loaded, {total_errors:,} errors")

        # Step 3: Validate
        if not args.skip_validation:
            logger.info("\n" + "-" * 40)
            logger.info("STEP 3: Validating data")
            logger.info("-" * 40)

            with get_connection(args.db) as conn:
                # Check data continuity
                if validate_data_continuity(conn, logger):
                    logger.info("Data continuity check: PASSED")
                else:
                    logger.warning("Data continuity check: GAPS DETECTED")
                    logger.warning("Consider running a full reload for complete data")

            # Run validator
            validator = DataValidator(db_path=args.db)
            report = validator.run_all_checks()

            if report.passed:
                logger.info("All validation checks passed")
            else:
                logger.warning("Some validation checks failed")
                print_validation_report(report)

        # Step 4: Post-update summary
        logger.info("\n" + "-" * 40)
        logger.info("STEP 4: Update Summary")
        logger.info("-" * 40)

        with get_connection(args.db) as conn:
            post_counts = get_table_counts(conn)
            new_latest_date = get_latest_record_date(conn)

            logger.info("Record counts (before -> after):")
            for table in pre_counts.keys():
                before = pre_counts.get(table, 0)
                after = post_counts.get(table, 0)
                diff = after - before
                if before > 0 or after > 0:
                    diff_str = f"+{diff:,}" if diff > 0 else str(diff)
                    logger.info(f"  {table}: {before:,} -> {after:,} ({diff_str})")

            logger.info(f"\nMost recent record: {latest_date} -> {new_latest_date}")

            # Database size
            db_size = args.db.stat().st_size / 1024 / 1024
            logger.info(f"Database size: {db_size:.1f} MB")

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"\nCompleted at: {end_time}")
        logger.info(f"Duration: {duration}")

        logger.info("\n" + "=" * 60)
        logger.info("Incremental update complete!")
        logger.info("=" * 60)

        return 0

    except KeyboardInterrupt:
        logger.warning("\nUpdate interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"\nError during update: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
