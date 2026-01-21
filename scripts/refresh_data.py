#!/usr/bin/env python3
"""
Refresh MAUDE data from FDA sources.

This script can be run manually or scheduled via cron/launchd.

Usage:
    python scripts/refresh_data.py              # Default: API update (last 30 days)
    python scripts/refresh_data.py --full       # Full update (download + API)
    python scripts/refresh_data.py --api-only   # Only fetch from openFDA API
    python scripts/refresh_data.py --days 90    # Fetch last 90 days from API
    python scripts/refresh_data.py --maintenance # Run maintenance after update

Example cron job (run daily at 2 AM):
    0 2 * * * cd /path/to/maude-analyzer && /path/to/venv/bin/python scripts/refresh_data.py >> logs/refresh.log 2>&1
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger
from src.database import initialize_database, run_full_maintenance
from src.ingestion import DataUpdater, get_update_status

logger = get_logger("refresh")


def main():
    parser = argparse.ArgumentParser(
        description="Refresh MAUDE data from FDA sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run full update (download new files + API fetch)"
    )
    parser.add_argument(
        "--api-only",
        action="store_true",
        help="Only fetch from openFDA API (no file downloads)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to fetch from API (default: 30)"
    )
    parser.add_argument(
        "--maintenance",
        action="store_true",
        help="Run database maintenance after update"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show current data status and exit"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Minimal output (only errors)"
    )

    args = parser.parse_args()

    # Check if database exists
    if not config.database.path.exists() and not args.status:
        logger.error("Database not found. Run initial_load.py first.")
        print("Error: Database not found. Run initial_load.py first.")
        sys.exit(1)

    # Show status
    if args.status:
        show_status()
        return

    # Run update
    start_time = datetime.now()

    if not args.quiet:
        print(f"MAUDE Data Refresh - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)

    updater = DataUpdater()

    try:
        if args.full:
            # Full update: download + load + API
            if not args.quiet:
                print("\nRunning full update...")
                print("  - Checking for new FDA files...")

            results = updater.run_full_update(
                download_new=True,
                load_new_files=True,
                fetch_from_api=True,
                api_days=args.days,
            )

            if not args.quiet:
                print("\nResults:")
                for source, status in results.items():
                    print(f"  {source}:")
                    print(f"    Added: {status.records_added}")
                    print(f"    Updated: {status.records_updated}")
                    print(f"    Success: {status.success}")
                    if status.errors:
                        print(f"    Errors: {status.errors[:3]}")

        elif args.api_only:
            # API only
            if not args.quiet:
                print(f"\nFetching from openFDA API (last {args.days} days)...")

            result = updater.update_from_openfda(days=args.days)

            if not args.quiet:
                print(f"\nResults:")
                print(f"  Records added: {result.records_added}")
                print(f"  Records updated: {result.records_updated}")
                print(f"  Duration: {result.duration_seconds:.1f}s")
                if result.errors:
                    print(f"  Errors: {result.errors[:3]}")

        else:
            # Default: API update only (most common use case)
            if not args.quiet:
                print(f"\nFetching recent updates from openFDA (last {args.days} days)...")

            result = updater.update_from_openfda(days=args.days)

            if not args.quiet:
                print(f"  Added: {result.records_added}, Updated: {result.records_updated}")

        # Run maintenance if requested
        if args.maintenance:
            if not args.quiet:
                print("\nRunning database maintenance...")

            maint_results = run_full_maintenance()

            if not args.quiet:
                for op, result in maint_results.items():
                    status = "OK" if result.success else "FAILED"
                    print(f"  {op}: {status}")

    except Exception as e:
        logger.error(f"Refresh failed: {e}")
        print(f"\nError: {e}")
        sys.exit(1)

    # Summary
    duration = (datetime.now() - start_time).total_seconds()

    if not args.quiet:
        print("\n" + "=" * 50)
        print(f"Refresh completed in {duration:.1f} seconds")

        # Show new status
        new_status = get_update_status()
        print(f"Total MDRs: {new_status.total_mdrs:,}")
        print(f"Latest data: {new_status.latest_date_received}")

    logger.info(f"Refresh completed in {duration:.1f}s")


def show_status():
    """Show current data status."""
    status = get_update_status()

    print("MAUDE Database Status")
    print("=" * 50)
    print(f"Database: {config.database.path}")
    print(f"Size: {status.database_size_mb:.1f} MB")
    print(f"Total MDRs: {status.total_mdrs:,}")
    print(f"Date range: {status.earliest_date_received} to {status.latest_date_received}")
    print(f"Last update: {status.last_update} ({status.last_update_source})")

    print("\nTable counts:")
    for table, count in status.table_counts.items():
        print(f"  {table}: {count:,}")


if __name__ == "__main__":
    main()
