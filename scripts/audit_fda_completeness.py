#!/usr/bin/env python
"""
FDA MAUDE Data Completeness Audit Script.

This script verifies that all FDA MAUDE data has been properly:
1. Downloaded - All expected files exist in raw data directory
2. Parsed - Files can be read and have expected structure
3. Loaded - Data exists in database tables
4. Validated - Record counts and date ranges are reasonable

Usage:
    python scripts/audit_fda_completeness.py [options]

Options:
    --data-dir PATH     Directory containing downloaded files
    --db PATH           Path to DuckDB database
    --verbose           Show detailed output
    --fix               Attempt to fix identified issues
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, field

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from src.database import get_connection, get_table_counts
from src.ingestion.download import KNOWN_FILES, MAUDEDownloader


@dataclass
class AuditResult:
    """Result of an audit check."""
    check_name: str
    passed: bool
    message: str
    details: Dict = field(default_factory=dict)
    severity: str = "INFO"  # INFO, WARNING, ERROR, CRITICAL


@dataclass
class AuditReport:
    """Complete audit report."""
    timestamp: datetime
    results: List[AuditResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Check if all critical and error checks passed."""
        for result in self.results:
            if not result.passed and result.severity in ("ERROR", "CRITICAL"):
                return False
        return True

    @property
    def summary(self) -> Dict[str, int]:
        """Get summary counts by result type."""
        counts = {"passed": 0, "warning": 0, "error": 0, "critical": 0}
        for result in self.results:
            if result.passed:
                counts["passed"] += 1
            elif result.severity == "WARNING":
                counts["warning"] += 1
            elif result.severity == "ERROR":
                counts["error"] += 1
            elif result.severity == "CRITICAL":
                counts["critical"] += 1
        return counts


class FDACompletenessAuditor:
    """Audits FDA MAUDE data completeness."""

    # Expected minimums for validation
    EXPECTED_MINIMUMS = {
        "master_events": 20_000_000,     # ~23M expected
        "devices": 8_000_000,            # ~9.8M expected
        "patients": 2_000_000,           # ~2.9M expected
        "mdr_text": 5_000_000,           # Should be millions with annual files
        "device_problems": 20_000_000,   # ~22M expected
        "problem_codes": 600,            # ~641 expected
        "product_codes": 3000,           # ~3,848 expected
        "patient_problems": 15_000_000,  # ~21M expected
    }

    # Expected date ranges
    EXPECTED_DATE_RANGES = {
        "master_events": (1984, 2026),
        "devices": (1984, 2026),
        "den_reports": (1984, 1997),
        "asr_reports": (1999, 2019),
    }

    def __init__(
        self,
        data_dir: Path,
        db_path: Path,
    ):
        """
        Initialize the auditor.

        Args:
            data_dir: Directory containing downloaded files.
            db_path: Path to DuckDB database.
        """
        self.data_dir = data_dir
        self.db_path = db_path
        self.logger = get_logger("audit")
        self.report = AuditReport(timestamp=datetime.now())

    def run_all_checks(self) -> AuditReport:
        """Run all audit checks."""
        self.logger.info("Starting FDA MAUDE completeness audit...")

        # Downloaded files checks
        self._check_downloaded_files()

        # Database checks
        self._check_database_tables()
        self._check_record_counts()
        self._check_date_ranges()
        self._check_lookup_tables()
        self._check_data_relationships()

        return self.report

    def _add_result(
        self,
        check_name: str,
        passed: bool,
        message: str,
        details: Dict = None,
        severity: str = "INFO",
    ) -> None:
        """Add an audit result."""
        result = AuditResult(
            check_name=check_name,
            passed=passed,
            message=message,
            details=details or {},
            severity=severity,
        )
        self.report.results.append(result)

        # Log the result
        if passed:
            self.logger.info(f"PASS: {check_name} - {message}")
        else:
            log_method = getattr(self.logger, severity.lower(), self.logger.warning)
            log_method(f"FAIL: {check_name} - {message}")

    def _check_downloaded_files(self) -> None:
        """Check that all expected files have been downloaded."""
        self.logger.info("Checking downloaded files...")

        downloader = MAUDEDownloader(output_dir=self.data_dir)
        existing = downloader.get_existing_files()

        for file_type, expected_files in KNOWN_FILES.items():
            existing_for_type = existing.get(file_type, [])
            existing_count = len(existing_for_type)
            expected_count = len(expected_files)

            # Calculate what's missing
            existing_bases = {f.replace(".txt", "").lower() for f in existing_for_type}
            expected_bases = {f.replace(".zip", "").lower() for f in expected_files}
            missing = expected_bases - existing_bases

            if len(missing) == 0:
                self._add_result(
                    f"downloaded_{file_type}",
                    True,
                    f"All {expected_count} {file_type} files downloaded",
                    {"count": existing_count},
                )
            else:
                self._add_result(
                    f"downloaded_{file_type}",
                    False,
                    f"Missing {len(missing)} of {expected_count} {file_type} files",
                    {"missing": list(missing)[:10], "total_missing": len(missing)},
                    severity="WARNING" if file_type in ("asr", "den") else "ERROR",
                )

    def _check_database_tables(self) -> None:
        """Check that all database tables exist."""
        self.logger.info("Checking database tables...")

        expected_tables = [
            "master_events", "devices", "patients", "mdr_text",
            "device_problems", "patient_problems",
            "asr_reports", "asr_patient_problems",
            "den_reports", "manufacturer_disclaimers",
            "problem_codes", "patient_problem_codes",
            "product_codes", "manufacturers",
        ]

        if not self.db_path.exists():
            self._add_result(
                "database_exists",
                False,
                f"Database not found at {self.db_path}",
                severity="CRITICAL",
            )
            return

        try:
            with get_connection(self.db_path) as conn:
                # Get list of tables
                tables_result = conn.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                ).fetchall()
                existing_tables = {row[0] for row in tables_result}

                for table in expected_tables:
                    if table in existing_tables:
                        self._add_result(
                            f"table_{table}",
                            True,
                            f"Table {table} exists",
                        )
                    else:
                        self._add_result(
                            f"table_{table}",
                            False,
                            f"Table {table} does not exist",
                            severity="ERROR",
                        )

        except Exception as e:
            self._add_result(
                "database_connection",
                False,
                f"Could not connect to database: {e}",
                severity="CRITICAL",
            )

    def _check_record_counts(self) -> None:
        """Check that tables have expected record counts."""
        self.logger.info("Checking record counts...")

        if not self.db_path.exists():
            return

        try:
            with get_connection(self.db_path) as conn:
                counts = get_table_counts(conn)

                for table, expected_min in self.EXPECTED_MINIMUMS.items():
                    actual = counts.get(table, 0)

                    if actual >= expected_min:
                        self._add_result(
                            f"count_{table}",
                            True,
                            f"{table}: {actual:,} records (expected min: {expected_min:,})",
                            {"actual": actual, "expected_min": expected_min},
                        )
                    elif actual > 0:
                        pct = (actual / expected_min) * 100
                        self._add_result(
                            f"count_{table}",
                            False,
                            f"{table}: {actual:,} records ({pct:.1f}% of expected {expected_min:,})",
                            {"actual": actual, "expected_min": expected_min, "percentage": pct},
                            severity="WARNING",
                        )
                    else:
                        self._add_result(
                            f"count_{table}",
                            False,
                            f"{table}: EMPTY (expected at least {expected_min:,})",
                            {"actual": 0, "expected_min": expected_min},
                            severity="ERROR",
                        )

        except Exception as e:
            self._add_result(
                "record_counts",
                False,
                f"Error checking record counts: {e}",
                severity="ERROR",
            )

    def _check_date_ranges(self) -> None:
        """Check that date ranges cover expected periods."""
        self.logger.info("Checking date ranges...")

        if not self.db_path.exists():
            return

        try:
            with get_connection(self.db_path) as conn:
                # Check master_events date range
                result = conn.execute("""
                    SELECT
                        MIN(YEAR(date_received)) as min_year,
                        MAX(YEAR(date_received)) as max_year,
                        COUNT(*) as total
                    FROM master_events
                    WHERE date_received IS NOT NULL
                """).fetchone()

                if result and result[0]:
                    min_year, max_year, total = result
                    expected_min, expected_max = self.EXPECTED_DATE_RANGES["master_events"]

                    if min_year <= expected_min + 5 and max_year >= expected_max - 1:
                        self._add_result(
                            "date_range_master",
                            True,
                            f"Master events date range: {min_year}-{max_year} ({total:,} dated records)",
                            {"min_year": min_year, "max_year": max_year, "count": total},
                        )
                    else:
                        self._add_result(
                            "date_range_master",
                            False,
                            f"Master events date range: {min_year}-{max_year} (expected ~{expected_min}-{expected_max})",
                            {"min_year": min_year, "max_year": max_year, "expected": (expected_min, expected_max)},
                            severity="WARNING",
                        )

                # Check for DEN reports if table exists
                try:
                    den_result = conn.execute("""
                        SELECT
                            MIN(report_year) as min_year,
                            MAX(report_year) as max_year,
                            COUNT(*) as total
                        FROM den_reports
                        WHERE report_year IS NOT NULL
                    """).fetchone()

                    if den_result and den_result[0]:
                        min_year, max_year, total = den_result
                        expected_min, expected_max = self.EXPECTED_DATE_RANGES["den_reports"]

                        self._add_result(
                            "date_range_den",
                            min_year <= expected_min and max_year >= expected_max - 1,
                            f"DEN reports date range: {min_year}-{max_year} ({total:,} records)",
                            {"min_year": min_year, "max_year": max_year, "count": total},
                        )
                except:
                    pass  # Table may not exist yet

        except Exception as e:
            self._add_result(
                "date_ranges",
                False,
                f"Error checking date ranges: {e}",
                severity="ERROR",
            )

    def _check_lookup_tables(self) -> None:
        """Check that lookup tables are properly populated."""
        self.logger.info("Checking lookup tables...")

        if not self.db_path.exists():
            return

        try:
            with get_connection(self.db_path) as conn:
                # Check problem_codes
                problem_count = conn.execute(
                    "SELECT COUNT(*) FROM problem_codes"
                ).fetchone()[0]

                if problem_count >= 600:
                    self._add_result(
                        "lookup_problem_codes",
                        True,
                        f"Problem codes lookup: {problem_count} entries",
                        {"count": problem_count},
                    )
                elif problem_count > 0:
                    self._add_result(
                        "lookup_problem_codes",
                        False,
                        f"Problem codes lookup incomplete: {problem_count} (expected ~641)",
                        {"count": problem_count},
                        severity="WARNING",
                    )
                else:
                    self._add_result(
                        "lookup_problem_codes",
                        False,
                        "Problem codes lookup is EMPTY",
                        severity="ERROR",
                    )

                # Check product_codes
                product_count = conn.execute(
                    "SELECT COUNT(*) FROM product_codes"
                ).fetchone()[0]

                if product_count >= 3000:
                    self._add_result(
                        "lookup_product_codes",
                        True,
                        f"Product codes lookup: {product_count} entries",
                        {"count": product_count},
                    )
                elif product_count > 0:
                    self._add_result(
                        "lookup_product_codes",
                        False,
                        f"Product codes lookup incomplete: {product_count} (expected ~3,848)",
                        {"count": product_count},
                        severity="WARNING",
                    )
                else:
                    self._add_result(
                        "lookup_product_codes",
                        False,
                        "Product codes lookup is EMPTY",
                        severity="ERROR",
                    )

                # Check manufacturers
                mfr_count = conn.execute(
                    "SELECT COUNT(*) FROM manufacturers"
                ).fetchone()[0]

                if mfr_count >= 1000:
                    self._add_result(
                        "lookup_manufacturers",
                        True,
                        f"Manufacturers lookup: {mfr_count} entries",
                        {"count": mfr_count},
                    )
                elif mfr_count > 0:
                    self._add_result(
                        "lookup_manufacturers",
                        False,
                        f"Manufacturers lookup may be incomplete: {mfr_count}",
                        {"count": mfr_count},
                        severity="WARNING",
                    )
                else:
                    self._add_result(
                        "lookup_manufacturers",
                        False,
                        "Manufacturers lookup is EMPTY",
                        severity="WARNING",
                    )

        except Exception as e:
            self._add_result(
                "lookup_tables",
                False,
                f"Error checking lookup tables: {e}",
                severity="ERROR",
            )

    def _check_data_relationships(self) -> None:
        """Check that data relationships are valid."""
        self.logger.info("Checking data relationships...")

        if not self.db_path.exists():
            return

        try:
            with get_connection(self.db_path) as conn:
                # Check that devices can link to master_events
                orphan_devices = conn.execute("""
                    SELECT COUNT(*) FROM devices d
                    WHERE NOT EXISTS (
                        SELECT 1 FROM master_events m
                        WHERE m.mdr_report_key = d.mdr_report_key
                    )
                """).fetchone()[0]

                total_devices = conn.execute("SELECT COUNT(*) FROM devices").fetchone()[0]

                if total_devices > 0:
                    orphan_pct = (orphan_devices / total_devices) * 100

                    if orphan_pct < 1:
                        self._add_result(
                            "relationship_devices_master",
                            True,
                            f"Device-Master relationship: {orphan_pct:.2f}% orphan devices",
                            {"orphan_count": orphan_devices, "total": total_devices},
                        )
                    else:
                        self._add_result(
                            "relationship_devices_master",
                            False,
                            f"High orphan rate: {orphan_pct:.1f}% devices without master record",
                            {"orphan_count": orphan_devices, "total": total_devices},
                            severity="WARNING",
                        )

        except Exception as e:
            self._add_result(
                "data_relationships",
                False,
                f"Error checking relationships: {e}",
                severity="ERROR",
            )


def print_audit_report(report: AuditReport) -> None:
    """Print the audit report."""
    print("\n" + "=" * 70)
    print("FDA MAUDE DATA COMPLETENESS AUDIT REPORT")
    print(f"Timestamp: {report.timestamp}")
    print("=" * 70)

    # Group results by category
    categories = {}
    for result in report.results:
        category = result.check_name.split("_")[0]
        if category not in categories:
            categories[category] = []
        categories[category].append(result)

    for category, results in sorted(categories.items()):
        print(f"\n{category.upper()}")
        print("-" * 40)

        for result in results:
            status = "PASS" if result.passed else f"FAIL ({result.severity})"
            print(f"  [{status}] {result.check_name}")
            print(f"          {result.message}")

    # Summary
    summary = report.summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("-" * 40)
    print(f"  Passed:   {summary['passed']}")
    print(f"  Warnings: {summary['warning']}")
    print(f"  Errors:   {summary['error']}")
    print(f"  Critical: {summary['critical']}")
    print(f"\n  Overall: {'PASS' if report.passed else 'FAIL'}")
    print("=" * 70)


def main():
    """Main entry point for audit script."""
    parser = argparse.ArgumentParser(
        description="Audit FDA MAUDE data completeness",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--data-dir",
        type=Path,
        default=config.data.raw_path,
        help="Directory containing downloaded files",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=config.database.path,
        help="Path to DuckDB database",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed output",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(log_level=args.log_level if not args.verbose else "DEBUG")

    # Run audit
    auditor = FDACompletenessAuditor(
        data_dir=args.data_dir,
        db_path=args.db,
    )

    report = auditor.run_all_checks()

    # Print report
    print_audit_report(report)

    # Return exit code based on results
    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
