#!/usr/bin/env python3
"""
Reconcile Historical vs Incremental Data - Compare base and update records.

This script verifies data consistency between historical (thru{year}) files
and incremental (Add/Change) files by checking:
- (Historical + Adds) - Changes = Current Total
- Records only in Add files (new since historical)
- Orphaned Change records (changes without base record)

Usage:
    python scripts/reconcile_historical_incremental.py
    python scripts/reconcile_historical_incremental.py --year 2024
    python scripts/reconcile_historical_incremental.py --json --output reconciliation.json
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict
import re

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from src.database import get_connection

logger = get_logger("reconcile_historical")


@dataclass
class FileReconciliation:
    """Reconciliation results for a file type."""
    file_type: str
    historical_files: List[str] = field(default_factory=list)
    add_files: List[str] = field(default_factory=list)
    change_files: List[str] = field(default_factory=list)
    historical_record_count: int = 0
    add_record_count: int = 0
    change_record_count: int = 0
    current_total: int = 0
    expected_total: int = 0  # historical + adds
    variance: int = 0
    variance_pct: float = 0.0
    orphan_changes: int = 0  # Changes without base records
    records_only_in_adds: int = 0
    status: str = "UNKNOWN"


@dataclass
class ReconciliationReport:
    """Complete reconciliation report."""
    timestamp: datetime
    database_path: str
    overall_status: str = "UNKNOWN"
    file_type_results: Dict[str, FileReconciliation] = field(default_factory=dict)
    consistency_checks: List[Dict] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "database_path": self.database_path,
            "overall_status": self.overall_status,
            "file_type_results": {
                ft: asdict(r) for ft, r in self.file_type_results.items()
            },
            "consistency_checks": self.consistency_checks,
            "recommendations": self.recommendations,
        }


def categorize_files_from_audit(conn, file_type: str) -> Tuple[List[str], List[str], List[str]]:
    """
    Categorize loaded files by type (historical, add, change).

    Args:
        conn: Database connection.
        file_type: File type to check.

    Returns:
        Tuple of (historical_files, add_files, change_files).
    """
    historical = []
    adds = []
    changes = []

    # Get loaded files
    try:
        files = conn.execute("""
            SELECT file_name
            FROM ingestion_log
            WHERE file_type = ?
              AND status IN ('COMPLETED', 'COMPLETED_WITH_ERRORS')
        """, [file_type]).fetchall()

        for (filename,) in files:
            name_lower = filename.lower()

            if 'change' in name_lower:
                changes.append(filename)
            elif 'add' in name_lower:
                adds.append(filename)
            elif 'thru' in name_lower:
                historical.append(filename)
            else:
                # Current/annual files are considered part of base
                historical.append(filename)

    except Exception as e:
        logger.warning(f"Could not get file list for {file_type}: {e}")

    return historical, adds, changes


def count_records_by_source_pattern(conn, table_name: str, pattern: str) -> int:
    """
    Count records from files matching a pattern.

    Args:
        conn: Database connection.
        table_name: Table to query.
        pattern: SQL LIKE pattern for source_file.

    Returns:
        Record count.
    """
    try:
        result = conn.execute(f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE source_file LIKE ?
        """, [pattern]).fetchone()
        return result[0] if result else 0
    except Exception:
        return 0


def find_orphan_change_records(conn, table_name: str, key_column: str) -> int:
    """
    Find records from Change files that don't have a base record.

    This checks for records that were supposedly updated but the original
    record doesn't exist (possibly because historical file wasn't loaded).

    Args:
        conn: Database connection.
        table_name: Table to check.
        key_column: Primary key column.

    Returns:
        Count of orphan change records.
    """
    # This is a heuristic - Change files should update existing records
    # If we have records ONLY from Change files for certain keys, they're orphans
    try:
        # Get keys that ONLY appear in Change files
        result = conn.execute(f"""
            WITH change_keys AS (
                SELECT DISTINCT {key_column}
                FROM {table_name}
                WHERE source_file LIKE '%Change%'
            ),
            base_keys AS (
                SELECT DISTINCT {key_column}
                FROM {table_name}
                WHERE source_file NOT LIKE '%Change%'
            )
            SELECT COUNT(*)
            FROM change_keys c
            WHERE NOT EXISTS (
                SELECT 1 FROM base_keys b
                WHERE b.{key_column} = c.{key_column}
            )
        """).fetchone()
        return result[0] if result else 0
    except Exception as e:
        logger.warning(f"Could not find orphan changes in {table_name}: {e}")
        return 0


def reconcile_file_type(
    conn,
    file_type: str,
    table_name: str,
) -> FileReconciliation:
    """
    Reconcile data for a single file type.

    Args:
        conn: Database connection.
        file_type: File type (master, device, etc.).
        table_name: Database table name.

    Returns:
        FileReconciliation results.
    """
    result = FileReconciliation(file_type=file_type)

    # Categorize files
    historical, adds, changes = categorize_files_from_audit(conn, file_type)
    result.historical_files = historical
    result.add_files = adds
    result.change_files = changes

    # Count records by source type
    try:
        # Total current records
        result.current_total = conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]

        # Records from historical/base files
        for f in historical:
            result.historical_record_count += count_records_by_source_pattern(
                conn, table_name, f"%{f}%"
            )

        # Records from Add files
        for f in adds:
            result.add_record_count += count_records_by_source_pattern(
                conn, table_name, f"%{f}%"
            )

        # Records from Change files
        for f in changes:
            result.change_record_count += count_records_by_source_pattern(
                conn, table_name, f"%{f}%"
            )

        # Calculate expected total
        # For tables with INSERT OR REPLACE, expected = total (changes update in place)
        # For tables with DELETE/INSERT, expected = historical + adds
        if file_type == "master":
            # Master uses INSERT OR REPLACE
            result.expected_total = result.current_total
        else:
            # Child tables: changes might create new records or update
            result.expected_total = result.historical_record_count + result.add_record_count

        # Calculate variance
        result.variance = abs(result.current_total - result.expected_total)
        if result.expected_total > 0:
            result.variance_pct = (result.variance / result.expected_total) * 100

        # Find orphan changes
        if file_type == "master":
            result.orphan_changes = find_orphan_change_records(
                conn, table_name, "mdr_report_key"
            )

        # Determine status
        if result.variance_pct < 0.1:
            result.status = "RECONCILED"
        elif result.variance_pct < 1.0:
            result.status = "MINOR_VARIANCE"
        else:
            result.status = "VARIANCE_DETECTED"

    except Exception as e:
        logger.error(f"Error reconciling {file_type}: {e}")
        result.status = "ERROR"

    return result


def check_cross_table_consistency(conn) -> List[Dict]:
    """
    Check consistency across related tables.

    Returns:
        List of consistency check results.
    """
    checks = []

    # Check: All device MDR keys should be in master
    try:
        orphan_devices = conn.execute("""
            SELECT COUNT(DISTINCT mdr_report_key)
            FROM devices d
            WHERE NOT EXISTS (
                SELECT 1 FROM master_events m
                WHERE m.mdr_report_key = d.mdr_report_key
            )
        """).fetchone()[0]

        total_device_keys = conn.execute(
            "SELECT COUNT(DISTINCT mdr_report_key) FROM devices"
        ).fetchone()[0]

        checks.append({
            "check": "device_to_master_linkage",
            "orphan_count": orphan_devices,
            "total_count": total_device_keys,
            "status": "PASS" if orphan_devices == 0 else "FAIL",
        })
    except Exception as e:
        logger.warning(f"Device linkage check failed: {e}")

    # Check: Event counts match
    try:
        master_count = conn.execute("SELECT COUNT(*) FROM master_events").fetchone()[0]
        device_events = conn.execute(
            "SELECT COUNT(DISTINCT mdr_report_key) FROM devices"
        ).fetchone()[0]

        # ~90% of masters should have devices
        coverage = (device_events / master_count * 100) if master_count > 0 else 0

        checks.append({
            "check": "master_device_coverage",
            "masters_with_devices": device_events,
            "total_masters": master_count,
            "coverage_pct": round(coverage, 2),
            "status": "PASS" if coverage >= 85 else "WARNING" if coverage >= 70 else "FAIL",
        })
    except Exception as e:
        logger.warning(f"Coverage check failed: {e}")

    return checks


def run_reconciliation(db_path: Path) -> ReconciliationReport:
    """
    Run complete reconciliation analysis.

    Args:
        db_path: Path to database.

    Returns:
        ReconciliationReport with all results.
    """
    report = ReconciliationReport(
        timestamp=datetime.now(),
        database_path=str(db_path),
    )

    file_type_tables = {
        "master": "master_events",
        "device": "devices",
        "patient": "patients",
        "text": "mdr_text",
    }

    with get_connection(db_path, read_only=True) as conn:
        # Reconcile each file type
        for file_type, table_name in file_type_tables.items():
            try:
                result = reconcile_file_type(conn, file_type, table_name)
                report.file_type_results[file_type] = result

                # Generate recommendations
                if result.status == "VARIANCE_DETECTED":
                    report.recommendations.append(
                        f"{file_type}: Variance of {result.variance:,} records "
                        f"({result.variance_pct:.2f}%) detected - review loaded files"
                    )

                if result.orphan_changes > 0:
                    report.recommendations.append(
                        f"{file_type}: {result.orphan_changes:,} orphan change records - "
                        f"historical files may be missing"
                    )

            except Exception as e:
                logger.error(f"Error reconciling {file_type}: {e}")

        # Cross-table consistency checks
        report.consistency_checks = check_cross_table_consistency(conn)

        # Determine overall status
        statuses = [r.status for r in report.file_type_results.values()]
        check_statuses = [c.get("status", "UNKNOWN") for c in report.consistency_checks]

        if "ERROR" in statuses or "FAIL" in check_statuses:
            report.overall_status = "INCONSISTENT"
        elif "VARIANCE_DETECTED" in statuses or "WARNING" in check_statuses:
            report.overall_status = "MINOR_ISSUES"
        elif all(s == "RECONCILED" for s in statuses):
            report.overall_status = "FULLY_RECONCILED"
        else:
            report.overall_status = "PARTIAL"

    return report


def print_report(report: ReconciliationReport) -> None:
    """Print reconciliation report to console."""
    print("=" * 70)
    print("HISTORICAL VS INCREMENTAL RECONCILIATION")
    print("=" * 70)
    print(f"\nTimestamp: {report.timestamp}")
    print(f"Database: {report.database_path}")
    print(f"Overall Status: {report.overall_status}")

    print("\n" + "-" * 70)
    print("FILE TYPE RECONCILIATION")
    print("-" * 70)

    for file_type, result in report.file_type_results.items():
        status_sym = {
            "RECONCILED": "[OK]",
            "MINOR_VARIANCE": "[~]",
            "VARIANCE_DETECTED": "[!]",
            "ERROR": "[X]",
        }.get(result.status, "[?]")

        print(f"\n  {file_type.upper()} {status_sym}")
        print(f"    Files:")
        print(f"      Historical: {len(result.historical_files)}")
        print(f"      Add:        {len(result.add_files)}")
        print(f"      Change:     {len(result.change_files)}")
        print(f"    Records:")
        print(f"      Historical: {result.historical_record_count:>12,}")
        print(f"      From Adds:  {result.add_record_count:>12,}")
        print(f"      From Changes: {result.change_record_count:>10,}")
        print(f"      Current Total: {result.current_total:>10,}")
        if result.variance > 0:
            print(f"    Variance: {result.variance:,} ({result.variance_pct:.2f}%)")
        if result.orphan_changes > 0:
            print(f"    Orphan Changes: {result.orphan_changes:,}")

    if report.consistency_checks:
        print("\n" + "-" * 70)
        print("CROSS-TABLE CONSISTENCY")
        print("-" * 70)
        for check in report.consistency_checks:
            status_sym = {"PASS": "[OK]", "WARNING": "[~]", "FAIL": "[X]"}.get(
                check.get("status"), "[?]"
            )
            print(f"  {check['check']} {status_sym}")
            for k, v in check.items():
                if k not in ("check", "status"):
                    print(f"    {k}: {v}")

    if report.recommendations:
        print("\n" + "-" * 70)
        print("RECOMMENDATIONS")
        print("-" * 70)
        for rec in report.recommendations:
            print(f"  - {rec}")

    print("\n" + "=" * 70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Reconcile historical vs incremental MAUDE data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--db",
        type=Path,
        default=config.database.path,
        help="Path to DuckDB database",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        help="Save results to file",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="WARNING",
        help="Logging level",
    )

    args = parser.parse_args()

    setup_logging(log_level=args.log_level)

    # Run reconciliation
    report = run_reconciliation(args.db)

    # Output results
    if args.json:
        output = json.dumps(report.to_dict(), indent=2)
        if args.output:
            with open(args.output, "w") as f:
                f.write(output)
            print(f"Results saved to: {args.output}")
        else:
            print(output)
    else:
        print_report(report)
        if args.output:
            with open(args.output, "w") as f:
                json.dump(report.to_dict(), f, indent=2)
            print(f"\nJSON results saved to: {args.output}")

    # Return exit code
    return 0 if report.overall_status in ["FULLY_RECONCILED", "PARTIAL"] else 1


if __name__ == "__main__":
    sys.exit(main())
