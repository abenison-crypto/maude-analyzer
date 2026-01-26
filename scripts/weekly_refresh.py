#!/usr/bin/env python
"""
Weekly Data Refresh Script for MAUDE Analyzer.

This script performs the complete weekly refresh cycle:
1. Check for new FDA files (using HTTP HEAD for Last-Modified)
2. Download new/updated files
3. Process CHANGE files first (updates to existing records)
4. Process ADD files (new records)
5. Re-load current year files (handles corrections)
6. Validate data integrity
7. Generate summary report

Run this script on Fridays after FDA's Thursday release.

Recommended cron schedule:
    # Weekly refresh - Fridays at 2 AM (after FDA Thursday release)
    0 2 * * 5 cd /path/to/maude-analyzer && ./venv/bin/python scripts/weekly_refresh.py

Usage:
    python scripts/weekly_refresh.py [options]

Options:
    --skip-download     Skip download step (use existing files)
    --skip-validation   Skip validation step
    --force             Force re-download even if files unchanged
    --dry-run           Check for updates without downloading
    --all-products      Load all products (not just SCS codes)
    --notify            Send notification on completion (requires config)
    --data-dir PATH     Custom data directory
    --db PATH           Custom database path
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

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
from src.ingestion.fda_discovery import FDADiscovery, DiscoveryResult
from src.ingestion.change_processor import ChangeProcessor, process_all_change_files


def check_for_updates(discovery: FDADiscovery, logger) -> DiscoveryResult:
    """
    Check FDA servers for new or updated files.

    Args:
        discovery: FDADiscovery instance.
        logger: Logger instance.

    Returns:
        DiscoveryResult with files needing download.
    """
    logger.info("Checking FDA servers for updates...")
    result = discovery.check_for_updates(
        check_annual=True,
        check_add_change=True,
    )

    logger.info(f"Files checked: {result.files_checked}")
    logger.info(f"New files: {len(result.new_files)}")
    logger.info(f"Updated files: {len(result.updated_files)}")

    if result.files_needing_download:
        logger.info("Files needing download:")
        for f in result.files_needing_download:
            size_mb = f.size_bytes / 1024 / 1024 if f.size_bytes else 0
            status = "NEW" if f.is_new else "UPDATED"
            logger.info(f"  [{status}] {f.filename} ({f.file_category}) - {size_mb:.1f} MB")
    else:
        logger.info("All files are up to date")

    return result


def download_files(
    discovery: FDADiscovery,
    discovery_result: DiscoveryResult,
    downloader: MAUDEDownloader,
    force: bool,
    logger,
) -> Dict[str, List[Any]]:
    """
    Download new and updated files from FDA.

    Args:
        discovery: FDADiscovery instance.
        discovery_result: Result from check_for_updates.
        downloader: MAUDEDownloader instance.
        force: Force re-download even if unchanged.
        logger: Logger instance.

    Returns:
        Dictionary of download results by category.
    """
    results = {"current": [], "add": [], "change": [], "annual": []}

    files_by_category = discovery_result.by_category()

    for category, files in files_by_category.items():
        if not files:
            continue

        logger.info(f"\nDownloading {category.upper()} files...")

        for file_info in files:
            logger.info(f"  Downloading {file_info.filename}...")

            result = downloader._download_file(
                file_info.url,
                file_info.file_type,
                force=force,
            )

            results[category].append(result)

            if result.success:
                # Mark file as downloaded in discovery state
                discovery.mark_downloaded(
                    file_info.filename,
                    file_info.last_modified_remote,
                    file_info.size_bytes,
                )
                logger.info(
                    f"    Downloaded: {result.size_bytes / 1024 / 1024:.1f} MB, "
                    f"extracted {len(result.extracted_files)} files"
                )
            else:
                logger.warning(f"    Failed: {result.error}")

    return results


def process_change_files(
    data_dir: Path,
    db_path: Path,
    logger,
) -> List[Any]:
    """
    Process CHANGE files (updates to existing records).

    Args:
        data_dir: Directory containing data files.
        db_path: Path to database.
        logger: Logger instance.

    Returns:
        List of ChangeResult objects.
    """
    logger.info("\nProcessing CHANGE files...")

    with get_connection(db_path) as conn:
        results = process_all_change_files(data_dir, conn)

    if results:
        total_updated = sum(r.records_updated for r in results)
        total_not_found = sum(r.records_not_found for r in results)
        logger.info(f"CHANGE files processed: {len(results)}")
        logger.info(f"  Records updated: {total_updated:,}")
        logger.info(f"  Records not found: {total_not_found:,}")
    else:
        logger.info("No CHANGE files to process")

    return results


def load_data_files(
    data_dir: Path,
    db_path: Path,
    filter_codes: Optional[List[str]],
    logger,
) -> Dict[str, Any]:
    """
    Load data files (ADD files and current year files).

    Args:
        data_dir: Directory containing data files.
        db_path: Path to database.
        filter_codes: Optional list of product codes to filter by (None = all).
        logger: Logger instance.

    Returns:
        Dictionary with load statistics.
    """
    logger.info("\nLoading data files...")

    loader = MAUDELoader(
        db_path=db_path,
        filter_product_codes=filter_codes,
    )

    # File patterns to load (ADD files and current year files)
    patterns = {
        "master": ["mdrfoi*.txt", "mdrfoiAdd*.txt"],
        "device": ["foidev*.txt", "foidevAdd*.txt"],
        "patient": ["patient*.txt", "patientAdd*.txt"],
        "text": ["foitext*.txt", "foitextAdd*.txt"],
        "problem": ["foidevproblem*.txt"],
    }

    # Exclude CHANGE files (handled separately)
    exclude_patterns = ["*Change*"]

    total_loaded = 0
    total_skipped = 0
    total_errors = 0

    with get_connection(db_path) as conn:
        # Load in order: device first (for MDR key tracking), then master, then others
        for file_type in ["device", "master", "patient", "text", "problem"]:
            type_patterns = patterns.get(file_type, [])

            files = []
            for pattern in type_patterns:
                for f in data_dir.glob(pattern):
                    # Exclude CHANGE files
                    if any(excl.replace("*", "") in f.name for excl in exclude_patterns):
                        continue
                    # Exclude problem files from device pattern
                    if file_type == "device" and "problem" in f.name.lower():
                        continue
                    files.append(f)

            if not files:
                continue

            logger.info(f"  Loading {len(files)} {file_type} files...")

            for filepath in sorted(files):
                result = loader.load_file(filepath, file_type, conn)
                total_loaded += result.records_loaded
                total_skipped += result.records_skipped
                total_errors += result.records_errors

                if result.records_loaded > 0:
                    logger.info(
                        f"    {filepath.name}: {result.records_loaded:,} loaded, "
                        f"{result.records_skipped:,} skipped"
                    )

    logger.info(f"\nTotal: {total_loaded:,} records loaded")
    logger.info(f"  Skipped: {total_skipped:,}")
    logger.info(f"  Errors: {total_errors:,}")

    return {
        "records_loaded": total_loaded,
        "records_skipped": total_skipped,
        "records_errors": total_errors,
    }


def validate_data(
    db_path: Path,
    logger,
) -> bool:
    """
    Run validation checks on the database.

    Args:
        db_path: Path to database.
        logger: Logger instance.

    Returns:
        True if validation passed.
    """
    logger.info("\nRunning validation checks...")

    validator = DataValidator(db_path=db_path)
    report = validator.run_all_checks()

    if report.passed:
        logger.info("All validation checks passed")
    else:
        logger.warning("Some validation checks failed")
        print_validation_report(report)

    return report.passed


def update_data_freshness(
    db_path: Path,
    logger,
) -> None:
    """
    Update the data_freshness table with current statistics.

    Args:
        db_path: Path to database.
        logger: Logger instance.
    """
    logger.info("Updating data freshness tracking...")

    tables_to_track = [
        ("master_events", "date_received"),
        ("devices", "date_received"),
        ("patients", "date_received"),
        ("mdr_text", "date_report"),
        ("device_problems", "date_added"),
        ("patient_problems", "date_added"),
    ]

    from datetime import date
    today = date.today()

    with get_connection(db_path) as conn:
        for table_name, date_column in tables_to_track:
            try:
                # Get latest record date and count
                result = conn.execute(f"""
                    SELECT
                        MAX({date_column}) as latest_date,
                        COUNT(*) as record_count
                    FROM {table_name}
                    WHERE {date_column} IS NOT NULL
                """).fetchone()

                latest_date = result[0]
                record_count = result[1]

                # Calculate days since update
                if latest_date:
                    if hasattr(latest_date, 'date'):
                        latest_date = latest_date.date()
                    days_old = (today - latest_date).days if isinstance(latest_date, date) else None
                else:
                    days_old = None

                # Determine status
                if days_old is None:
                    status = "UNKNOWN"
                elif days_old <= 7:
                    status = "CURRENT"
                elif days_old <= 14:
                    status = "STALE"
                else:
                    status = "VERY_STALE"

                # Upsert into data_freshness table
                conn.execute("""
                    INSERT INTO data_freshness (
                        table_name, last_successful_load, latest_record_date,
                        days_since_update, record_count, status, updated_at
                    ) VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT (table_name) DO UPDATE SET
                        last_successful_load = CURRENT_TIMESTAMP,
                        latest_record_date = excluded.latest_record_date,
                        days_since_update = excluded.days_since_update,
                        record_count = excluded.record_count,
                        status = excluded.status,
                        updated_at = CURRENT_TIMESTAMP
                """, [table_name, latest_date, days_old, record_count, status])

                logger.debug(f"  {table_name}: {record_count:,} records, latest: {latest_date}, status: {status}")

            except Exception as e:
                logger.warning(f"  Error updating freshness for {table_name}: {e}")

    logger.info("Data freshness tracking updated")


def generate_summary(
    db_path: Path,
    start_time: datetime,
    pre_counts: Dict[str, int],
    logger,
) -> Dict[str, Any]:
    """
    Generate and log summary of the refresh operation.

    Args:
        db_path: Path to database.
        start_time: When the refresh started.
        pre_counts: Table counts before refresh.
        logger: Logger instance.

    Returns:
        Summary dictionary.
    """
    logger.info("\n" + "=" * 60)
    logger.info("REFRESH SUMMARY")
    logger.info("=" * 60)

    with get_connection(db_path) as conn:
        post_counts = get_table_counts(conn)

        # Get date range
        date_range = conn.execute("""
            SELECT MIN(date_received), MAX(date_received)
            FROM master_events
            WHERE date_received IS NOT NULL
        """).fetchone()

    # Record changes
    logger.info("\nRecord counts (before -> after):")
    for table in sorted(pre_counts.keys()):
        before = pre_counts.get(table, 0)
        after = post_counts.get(table, 0)
        diff = after - before
        if before > 0 or after > 0:
            diff_str = f"+{diff:,}" if diff >= 0 else str(diff)
            logger.info(f"  {table}: {before:,} -> {after:,} ({diff_str})")

    # Date range
    if date_range[0]:
        logger.info(f"\nDate range: {date_range[0]} to {date_range[1]}")

    # Database size
    db_size = db_path.stat().st_size / 1024 / 1024
    logger.info(f"Database size: {db_size:.1f} MB")

    # Duration
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"\nCompleted at: {end_time}")
    logger.info(f"Duration: {duration}")

    return {
        "pre_counts": pre_counts,
        "post_counts": post_counts,
        "date_range": (date_range[0], date_range[1]),
        "database_size_mb": db_size,
        "duration": str(duration),
    }


def main():
    """Main entry point for weekly refresh."""
    parser = argparse.ArgumentParser(
        description="Run weekly data refresh",
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
        help="Force re-download even if files unchanged",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check for updates without downloading",
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
    logger = get_logger("weekly_refresh")

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("MAUDE Analyzer - Weekly Data Refresh")
    logger.info(f"Started at: {start_time}")
    logger.info("=" * 60)

    # Check database exists
    if not args.db.exists():
        logger.error(f"Database not found: {args.db}")
        logger.error("Run initial_load.py first to create the database.")
        return 1

    # Get pre-refresh statistics
    with get_connection(args.db) as conn:
        pre_counts = get_table_counts(conn)
        schema_version = get_schema_version(conn)

        logger.info(f"Database: {args.db}")
        logger.info(f"Schema version: {schema_version}")
        logger.info(f"Current record counts:")
        for table, count in sorted(pre_counts.items()):
            if count > 0:
                logger.info(f"  {table}: {count:,}")

    try:
        # Initialize components
        discovery = FDADiscovery(state_file=args.data_dir / ".fda_discovery_state.json")
        downloader = MAUDEDownloader(output_dir=args.data_dir)

        # Step 1: Check for updates
        logger.info("\n" + "-" * 40)
        logger.info("STEP 1: Checking for FDA updates")
        logger.info("-" * 40)

        discovery_result = check_for_updates(discovery, logger)

        if args.dry_run:
            logger.info("\n[Dry run - exiting without downloading]")
            return 0

        # Step 2: Download files
        if not args.skip_download and discovery_result.files_needing_download:
            logger.info("\n" + "-" * 40)
            logger.info("STEP 2: Downloading new/updated files")
            logger.info("-" * 40)

            download_results = download_files(
                discovery,
                discovery_result,
                downloader,
                args.force,
                logger,
            )
        elif args.skip_download:
            logger.info("\n[Skipping download - using existing files]")
        else:
            logger.info("\n[No files need downloading]")

        # Step 3: Process CHANGE files (updates to existing records)
        logger.info("\n" + "-" * 40)
        logger.info("STEP 3: Processing CHANGE files")
        logger.info("-" * 40)

        change_results = process_change_files(args.data_dir, args.db, logger)

        # Step 4: Load ADD and current year files
        logger.info("\n" + "-" * 40)
        logger.info("STEP 4: Loading data files")
        logger.info("-" * 40)

        # Parse filter codes
        filter_codes = None
        if args.filter_codes:
            filter_codes = [c.strip() for c in args.filter_codes.split(",")]

        load_results = load_data_files(
            args.data_dir,
            args.db,
            filter_codes=filter_codes,
            logger=logger,
        )

        # Step 5: Validate
        if not args.skip_validation:
            logger.info("\n" + "-" * 40)
            logger.info("STEP 5: Validating data")
            logger.info("-" * 40)

            validation_passed = validate_data(args.db, logger)
        else:
            logger.info("\n[Skipping validation]")
            validation_passed = True

        # Step 5b: Update data freshness tracking
        logger.info("\n" + "-" * 40)
        logger.info("STEP 5b: Updating data freshness")
        logger.info("-" * 40)

        update_data_freshness(args.db, logger)

        # Step 6: Summary
        logger.info("\n" + "-" * 40)
        logger.info("STEP 6: Summary")
        logger.info("-" * 40)

        summary = generate_summary(args.db, start_time, pre_counts, logger)

        logger.info("\n" + "=" * 60)
        logger.info("Weekly refresh complete!")
        logger.info("=" * 60)

        return 0 if validation_passed else 1

    except KeyboardInterrupt:
        logger.warning("\nRefresh interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"\nError during refresh: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
