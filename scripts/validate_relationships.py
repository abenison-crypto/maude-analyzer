#!/usr/bin/env python3
"""
Validate data relationships in the MAUDE database.

This script verifies referential integrity between tables:
1. Every device record should have a matching master event
2. Every text record should have a matching master event
3. Reports orphan counts by year
4. Reports master events without devices (expected ~10% by FDA design)

Usage:
    python scripts/validate_relationships.py
    python scripts/validate_relationships.py --fix-orphans
    python scripts/validate_relationships.py --json --output validation.json
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from src.database import get_connection


@dataclass
class OrphanSummary:
    """Summary of orphan records for a table."""
    table_name: str
    total_records: int = 0
    orphan_count: int = 0
    orphan_pct: float = 0.0
    orphans_by_year: Dict[int, int] = field(default_factory=dict)
    sample_mdr_keys: List[str] = field(default_factory=list)


@dataclass
class CoverageSummary:
    """Summary of coverage for a relationship."""
    description: str
    total_parent: int = 0
    matched: int = 0
    unmatched: int = 0
    coverage_pct: float = 0.0
    unmatched_by_year: Dict[int, int] = field(default_factory=dict)


@dataclass
class ValidationReport:
    """Complete validation report."""
    timestamp: datetime
    database_path: str
    orphan_summaries: Dict[str, OrphanSummary] = field(default_factory=dict)
    coverage_summaries: Dict[str, CoverageSummary] = field(default_factory=dict)
    issues: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    passed: bool = True

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON export."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "database_path": self.database_path,
            "passed": self.passed,
            "issues": self.issues,
            "warnings": self.warnings,
            "orphan_summaries": {
                k: asdict(v) for k, v in self.orphan_summaries.items()
            },
            "coverage_summaries": {
                k: asdict(v) for k, v in self.coverage_summaries.items()
            },
        }


def check_orphan_records(conn, child_table: str, child_fk: str = "mdr_report_key") -> OrphanSummary:
    """
    Check for orphan records in a child table (records without matching master).

    Args:
        conn: Database connection.
        child_table: Name of child table.
        child_fk: Foreign key column name.

    Returns:
        OrphanSummary with results.
    """
    summary = OrphanSummary(table_name=child_table)

    # Get total count
    total = conn.execute(f"SELECT COUNT(*) FROM {child_table}").fetchone()[0]
    summary.total_records = total

    if total == 0:
        return summary

    # Count orphans
    orphan_count = conn.execute(f"""
        SELECT COUNT(*) FROM {child_table} c
        WHERE NOT EXISTS (
            SELECT 1 FROM master_events m
            WHERE m.mdr_report_key = c.{child_fk}
        )
    """).fetchone()[0]

    summary.orphan_count = orphan_count
    summary.orphan_pct = round((orphan_count / total) * 100, 2) if total > 0 else 0

    # Get orphans by year if available
    try:
        # Check if date_received exists
        year_query = f"""
            SELECT
                EXTRACT(YEAR FROM c.date_received) as year,
                COUNT(*) as cnt
            FROM {child_table} c
            WHERE NOT EXISTS (
                SELECT 1 FROM master_events m
                WHERE m.mdr_report_key = c.{child_fk}
            )
            AND c.date_received IS NOT NULL
            GROUP BY 1
            ORDER BY 1
        """
        by_year = conn.execute(year_query).fetchall()
        summary.orphans_by_year = {int(row[0]): row[1] for row in by_year if row[0]}
    except Exception:
        # date_received may not exist in all tables
        pass

    # Get sample orphan keys
    sample_query = f"""
        SELECT c.{child_fk}
        FROM {child_table} c
        WHERE NOT EXISTS (
            SELECT 1 FROM master_events m
            WHERE m.mdr_report_key = c.{child_fk}
        )
        LIMIT 10
    """
    samples = conn.execute(sample_query).fetchall()
    summary.sample_mdr_keys = [str(row[0]) for row in samples]

    return summary


def check_master_device_coverage(conn) -> CoverageSummary:
    """
    Check how many master events have device records.

    Note: ~10% of master events intentionally have no device records
    per FDA design (some report types don't require device info).

    Args:
        conn: Database connection.

    Returns:
        CoverageSummary with results.
    """
    summary = CoverageSummary(
        description="Master events with device records"
    )

    # Get total master events
    total = conn.execute("SELECT COUNT(*) FROM master_events").fetchone()[0]
    summary.total_parent = total

    if total == 0:
        return summary

    # Count master events WITH device records
    with_device = conn.execute("""
        SELECT COUNT(*) FROM master_events m
        WHERE EXISTS (
            SELECT 1 FROM devices d
            WHERE d.mdr_report_key = m.mdr_report_key
        )
    """).fetchone()[0]

    summary.matched = with_device
    summary.unmatched = total - with_device
    summary.coverage_pct = round((with_device / total) * 100, 2) if total > 0 else 0

    # Get unmatched by year
    by_year = conn.execute("""
        SELECT
            EXTRACT(YEAR FROM m.date_received) as year,
            COUNT(*) as cnt
        FROM master_events m
        WHERE NOT EXISTS (
            SELECT 1 FROM devices d
            WHERE d.mdr_report_key = m.mdr_report_key
        )
        AND m.date_received IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """).fetchall()

    summary.unmatched_by_year = {int(row[0]): row[1] for row in by_year if row[0]}

    return summary


def check_manufacturer_population(conn) -> CoverageSummary:
    """
    Check manufacturer_clean population rate in master_events.

    This is the critical metric - should be >90% after proper loading.

    Args:
        conn: Database connection.

    Returns:
        CoverageSummary with results.
    """
    summary = CoverageSummary(
        description="Master events with manufacturer_clean populated"
    )

    stats = conn.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(manufacturer_clean) as with_mfr
        FROM master_events
    """).fetchone()

    summary.total_parent = stats[0]
    summary.matched = stats[1]
    summary.unmatched = stats[0] - stats[1]
    summary.coverage_pct = round((stats[1] / stats[0]) * 100, 2) if stats[0] > 0 else 0

    # Get by year
    by_year = conn.execute("""
        SELECT
            EXTRACT(YEAR FROM date_received) as year,
            COUNT(*) as total,
            COUNT(manufacturer_clean) as with_mfr
        FROM master_events
        WHERE date_received IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """).fetchall()

    # Store years with low coverage
    summary.unmatched_by_year = {}
    for row in by_year:
        if row[0]:
            year = int(row[0])
            total = row[1]
            with_mfr = row[2]
            if total > 0:
                pct = (with_mfr / total) * 100
                if pct < 90:  # Flag years with low coverage
                    summary.unmatched_by_year[year] = total - with_mfr

    return summary


def check_product_code_population(conn) -> CoverageSummary:
    """
    Check product_code population rate in master_events.

    Args:
        conn: Database connection.

    Returns:
        CoverageSummary with results.
    """
    summary = CoverageSummary(
        description="Master events with product_code populated"
    )

    stats = conn.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(product_code) as with_code
        FROM master_events
    """).fetchone()

    summary.total_parent = stats[0]
    summary.matched = stats[1]
    summary.unmatched = stats[0] - stats[1]
    summary.coverage_pct = round((stats[1] / stats[0]) * 100, 2) if stats[0] > 0 else 0

    # Get by year
    by_year = conn.execute("""
        SELECT
            EXTRACT(YEAR FROM date_received) as year,
            COUNT(*) - COUNT(product_code) as missing
        FROM master_events
        WHERE date_received IS NOT NULL
        GROUP BY 1
        HAVING COUNT(*) - COUNT(product_code) > 0
        ORDER BY 1
    """).fetchall()

    summary.unmatched_by_year = {int(row[0]): row[1] for row in by_year if row[0]}

    return summary


def run_validation(db_path: Path) -> ValidationReport:
    """
    Run complete validation checks.

    Args:
        db_path: Path to database.

    Returns:
        ValidationReport with all results.
    """
    report = ValidationReport(
        timestamp=datetime.now(),
        database_path=str(db_path),
    )

    with get_connection(db_path, read_only=True) as conn:
        # Check orphan records in child tables
        child_tables = [
            ("devices", "mdr_report_key"),
            ("patients", "mdr_report_key"),
            ("mdr_text", "mdr_report_key"),
            ("device_problems", "mdr_report_key"),
            ("patient_problems", "mdr_report_key"),
        ]

        for table, fk in child_tables:
            try:
                summary = check_orphan_records(conn, table, fk)
                report.orphan_summaries[table] = summary

                # Check thresholds
                if summary.orphan_pct > 5:
                    report.warnings.append(
                        f"{table}: {summary.orphan_pct:.1f}% orphan records"
                    )
                if summary.orphan_pct > 20:
                    report.issues.append(
                        f"HIGH orphan rate in {table}: {summary.orphan_pct:.1f}%"
                    )
                    report.passed = False
            except Exception as e:
                report.warnings.append(f"Could not check {table}: {e}")

        # Check coverage metrics
        try:
            # Master-Device coverage
            device_coverage = check_master_device_coverage(conn)
            report.coverage_summaries["master_device"] = device_coverage

            # Expected: ~90% of masters should have devices
            if device_coverage.coverage_pct < 80:
                report.issues.append(
                    f"Low device coverage: {device_coverage.coverage_pct:.1f}% "
                    "(expected >80%)"
                )
                report.passed = False
            elif device_coverage.coverage_pct < 90:
                report.warnings.append(
                    f"Device coverage below target: {device_coverage.coverage_pct:.1f}%"
                )

        except Exception as e:
            report.warnings.append(f"Could not check master-device coverage: {e}")

        try:
            # Manufacturer population (CRITICAL)
            mfr_coverage = check_manufacturer_population(conn)
            report.coverage_summaries["manufacturer_clean"] = mfr_coverage

            if mfr_coverage.coverage_pct < 50:
                report.issues.append(
                    f"CRITICAL: Only {mfr_coverage.coverage_pct:.1f}% have manufacturer_clean. "
                    "Device files may be missing. Run populate_master_from_devices()."
                )
                report.passed = False
            elif mfr_coverage.coverage_pct < 90:
                report.issues.append(
                    f"Manufacturer coverage below target: {mfr_coverage.coverage_pct:.1f}% "
                    "(target >90%)"
                )
                report.passed = False

        except Exception as e:
            report.warnings.append(f"Could not check manufacturer coverage: {e}")

        try:
            # Product code population
            product_coverage = check_product_code_population(conn)
            report.coverage_summaries["product_code"] = product_coverage

            if product_coverage.coverage_pct < 90:
                report.warnings.append(
                    f"Product code coverage: {product_coverage.coverage_pct:.1f}% "
                    "(target >90%)"
                )

        except Exception as e:
            report.warnings.append(f"Could not check product code coverage: {e}")

    return report


def print_report(report: ValidationReport) -> None:
    """Print validation report to console."""
    print("=" * 70)
    print("MAUDE DATA RELATIONSHIP VALIDATION")
    print("=" * 70)
    print(f"\nTimestamp: {report.timestamp}")
    print(f"Database: {report.database_path}")
    print(f"Status: {'PASS' if report.passed else 'FAIL'}")

    # Orphan Records
    print("\n" + "-" * 70)
    print("ORPHAN RECORDS (Child records without matching master)")
    print("-" * 70)

    for table, summary in report.orphan_summaries.items():
        status = "OK" if summary.orphan_pct < 5 else "WARNING" if summary.orphan_pct < 20 else "CRITICAL"
        print(f"\n  {table}:")
        print(f"    Total:   {summary.total_records:>12,}")
        print(f"    Orphans: {summary.orphan_count:>12,} ({summary.orphan_pct:.1f}%) [{status}]")

        if summary.orphans_by_year:
            top_years = sorted(summary.orphans_by_year.items(), key=lambda x: -x[1])[:5]
            print("    Top years with orphans:")
            for year, count in top_years:
                print(f"      {year}: {count:,}")

    # Coverage Metrics
    print("\n" + "-" * 70)
    print("COVERAGE METRICS")
    print("-" * 70)

    for key, summary in report.coverage_summaries.items():
        status = "OK" if summary.coverage_pct >= 90 else "WARNING" if summary.coverage_pct >= 50 else "CRITICAL"
        print(f"\n  {summary.description}:")
        print(f"    Total:     {summary.total_parent:>12,}")
        print(f"    Matched:   {summary.matched:>12,}")
        print(f"    Unmatched: {summary.unmatched:>12,}")
        print(f"    Coverage:  {summary.coverage_pct:>11.1f}% [{status}]")

        if summary.unmatched_by_year and len(summary.unmatched_by_year) > 0:
            print("    Years with gaps:")
            for year, count in sorted(summary.unmatched_by_year.items())[:5]:
                print(f"      {year}: {count:,} missing")

    # Issues and Warnings
    if report.issues:
        print("\n" + "-" * 70)
        print("CRITICAL ISSUES")
        print("-" * 70)
        for issue in report.issues:
            print(f"  - {issue}")

    if report.warnings:
        print("\n" + "-" * 70)
        print("WARNINGS")
        print("-" * 70)
        for warning in report.warnings:
            print(f"  - {warning}")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    if report.passed:
        print("All critical validation checks PASSED")
    else:
        print(f"FAILED - {len(report.issues)} critical issues found")

    print("=" * 70)


def fix_orphans(db_path: Path, table: str, dry_run: bool = True) -> int:
    """
    Delete orphan records from a table.

    Args:
        db_path: Path to database.
        table: Table to clean.
        dry_run: If True, just count without deleting.

    Returns:
        Number of records deleted (or would be deleted).
    """
    with get_connection(db_path, read_only=dry_run) as conn:
        # Count orphans
        count = conn.execute(f"""
            SELECT COUNT(*) FROM {table} c
            WHERE NOT EXISTS (
                SELECT 1 FROM master_events m
                WHERE m.mdr_report_key = c.mdr_report_key
            )
        """).fetchone()[0]

        if not dry_run and count > 0:
            conn.execute(f"""
                DELETE FROM {table}
                WHERE mdr_report_key IN (
                    SELECT c.mdr_report_key FROM {table} c
                    WHERE NOT EXISTS (
                        SELECT 1 FROM master_events m
                        WHERE m.mdr_report_key = c.mdr_report_key
                    )
                )
            """)
            print(f"Deleted {count:,} orphan records from {table}")

        return count


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Validate MAUDE data relationships",
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
        "--fix-orphans",
        action="store_true",
        help="Delete orphan records (DESTRUCTIVE)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without deleting",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="WARNING",
        help="Logging level",
    )

    args = parser.parse_args()

    setup_logging(log_level=args.log_level)

    # Run validation
    report = run_validation(args.db)

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

    # Fix orphans if requested
    if args.fix_orphans or args.dry_run:
        tables = ["devices", "patients", "mdr_text", "device_problems", "patient_problems"]

        print("\n" + "=" * 70)
        print(f"{'ORPHAN CLEANUP' if not args.dry_run else 'ORPHAN CLEANUP (DRY RUN)'}")
        print("=" * 70)

        for table in tables:
            summary = report.orphan_summaries.get(table)
            if summary and summary.orphan_count > 0:
                if args.dry_run:
                    print(f"  Would delete {summary.orphan_count:,} orphans from {table}")
                else:
                    fix_orphans(args.db, table, dry_run=False)

    # Return exit code
    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
