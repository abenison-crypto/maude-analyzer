#!/usr/bin/env python
"""
FDA File Freshness Checker for MAUDE Analyzer.

A lightweight script that checks if new FDA files are available without
downloading them. Uses HTTP HEAD requests to check Last-Modified headers.

Use this script for:
- Daily monitoring of FDA file availability
- Pre-refresh checks to see what would be downloaded
- Automated alerting when new data is available

Recommended cron schedule:
    # Daily freshness check - 6 AM (notify only)
    0 6 * * * cd /path/to/maude-analyzer && ./venv/bin/python scripts/check_freshness.py --notify-only

Usage:
    python scripts/check_freshness.py [options]

Options:
    --notify-only       Only print/notify, don't update state
    --json              Output in JSON format
    --include-add       Include ADD files in check
    --include-change    Include CHANGE files in check
    --quiet             Only output if updates are available
"""

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from src.database import get_connection, get_table_counts
from src.ingestion.fda_discovery import FDADiscovery, DiscoveryResult


def check_database_freshness(db_path: Path) -> Dict[str, Any]:
    """
    Check freshness of the local database.

    Args:
        db_path: Path to database.

    Returns:
        Dictionary with freshness information.
    """
    result = {
        "database_exists": db_path.exists(),
        "latest_record_date": None,
        "days_since_latest": None,
        "total_records": 0,
        "database_size_mb": 0,
    }

    if not db_path.exists():
        return result

    try:
        with get_connection(db_path) as conn:
            # Get latest record date
            latest = conn.execute("""
                SELECT MAX(date_received)
                FROM master_events
                WHERE date_received IS NOT NULL
            """).fetchone()

            if latest and latest[0]:
                result["latest_record_date"] = str(latest[0])
                days_diff = (datetime.now().date() - latest[0]).days
                result["days_since_latest"] = days_diff

            # Get total records
            count = conn.execute("SELECT COUNT(*) FROM master_events").fetchone()
            result["total_records"] = count[0] if count else 0

        # Get database size
        result["database_size_mb"] = round(db_path.stat().st_size / 1024 / 1024, 1)

    except Exception as e:
        result["error"] = str(e)

    return result


def check_fda_freshness(
    include_add_change: bool = False,
    state_file: Path = None,
) -> DiscoveryResult:
    """
    Check FDA servers for file freshness.

    Args:
        include_add_change: Whether to check ADD/CHANGE files.
        state_file: Path to discovery state file.

    Returns:
        DiscoveryResult with freshness information.
    """
    discovery = FDADiscovery(state_file=state_file)
    return discovery.check_for_updates(
        check_annual=True,
        check_add_change=include_add_change,
    )


def format_human_readable(
    db_freshness: Dict[str, Any],
    fda_result: DiscoveryResult,
) -> str:
    """
    Format results for human-readable output.

    Args:
        db_freshness: Database freshness info.
        fda_result: FDA discovery result.

    Returns:
        Formatted string.
    """
    lines = []
    lines.append("=" * 60)
    lines.append("FDA MAUDE Data Freshness Report")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 60)

    # Database status
    lines.append("\nLOCAL DATABASE STATUS:")
    lines.append("-" * 30)

    if db_freshness["database_exists"]:
        lines.append(f"  Total records: {db_freshness['total_records']:,}")
        lines.append(f"  Database size: {db_freshness['database_size_mb']:.1f} MB")

        if db_freshness["latest_record_date"]:
            lines.append(f"  Latest record: {db_freshness['latest_record_date']}")
            days = db_freshness["days_since_latest"]
            if days is not None:
                if days == 0:
                    freshness = "TODAY"
                elif days == 1:
                    freshness = "1 day ago"
                else:
                    freshness = f"{days} days ago"
                lines.append(f"  Data age: {freshness}")

                # Warning if data is stale
                if days > 14:
                    lines.append(f"  WARNING: Data is more than 2 weeks old!")
    else:
        lines.append("  Database not found - run initial_load.py first")

    # FDA server status
    lines.append("\nFDA SERVER STATUS:")
    lines.append("-" * 30)
    lines.append(f"  Files checked: {fda_result.files_checked}")

    if fda_result.files_needing_download:
        lines.append(f"  Files needing download: {len(fda_result.files_needing_download)}")
        lines.append("")

        # Group by category
        by_category = fda_result.by_category()

        for category, files in by_category.items():
            if files:
                lines.append(f"  {category.upper()} files:")
                for f in files:
                    size_mb = f.size_bytes / 1024 / 1024 if f.size_bytes else 0
                    status = "NEW" if f.is_new else "UPDATED"
                    lines.append(f"    [{status}] {f.filename} - {size_mb:.1f} MB")
    else:
        lines.append("  All files are up to date!")

    # Recommendations
    lines.append("\nRECOMMENDATIONS:")
    lines.append("-" * 30)

    if fda_result.files_needing_download:
        lines.append("  Run: python scripts/weekly_refresh.py")
    else:
        lines.append("  No action needed - data is current")

    lines.append("")
    return "\n".join(lines)


def format_json(
    db_freshness: Dict[str, Any],
    fda_result: DiscoveryResult,
) -> str:
    """
    Format results as JSON.

    Args:
        db_freshness: Database freshness info.
        fda_result: FDA discovery result.

    Returns:
        JSON string.
    """
    data = {
        "timestamp": datetime.now().isoformat(),
        "database": db_freshness,
        "fda_server": {
            "files_checked": fda_result.files_checked,
            "new_files": [
                {
                    "filename": f.filename,
                    "type": f.file_type,
                    "category": f.file_category,
                    "size_bytes": f.size_bytes,
                    "last_modified": f.last_modified_remote.isoformat() if f.last_modified_remote else None,
                }
                for f in fda_result.new_files
            ],
            "updated_files": [
                {
                    "filename": f.filename,
                    "type": f.file_type,
                    "category": f.file_category,
                    "size_bytes": f.size_bytes,
                    "last_modified": f.last_modified_remote.isoformat() if f.last_modified_remote else None,
                }
                for f in fda_result.updated_files
            ],
        },
        "updates_available": len(fda_result.files_needing_download) > 0,
    }

    return json.dumps(data, indent=2)


def main():
    """Main entry point for freshness check."""
    parser = argparse.ArgumentParser(
        description="Check FDA MAUDE data freshness",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--notify-only",
        action="store_true",
        help="Only print/notify, don't update local state",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output in JSON format",
    )
    parser.add_argument(
        "--include-add",
        action="store_true",
        help="Include ADD files in check",
    )
    parser.add_argument(
        "--include-change",
        action="store_true",
        help="Include CHANGE files in check",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Only output if updates are available",
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

    args = parser.parse_args()

    # Check database freshness
    db_freshness = check_database_freshness(args.db)

    # Determine state file path
    if args.notify_only:
        # Use temporary state file to avoid modifying production state
        state_file = args.data_dir / ".fda_discovery_state_check.json"
    else:
        state_file = args.data_dir / ".fda_discovery_state.json"

    # Check FDA server freshness
    include_add_change = args.include_add or args.include_change
    fda_result = check_fda_freshness(
        include_add_change=include_add_change,
        state_file=state_file,
    )

    # Quiet mode - only output if updates available
    if args.quiet and not fda_result.files_needing_download:
        return 0

    # Format output
    if args.json:
        output = format_json(db_freshness, fda_result)
    else:
        output = format_human_readable(db_freshness, fda_result)

    print(output)

    # Return exit code based on whether updates are available
    # 0 = up to date, 1 = updates available
    return 0 if not fda_result.files_needing_download else 1


if __name__ == "__main__":
    sys.exit(main())
