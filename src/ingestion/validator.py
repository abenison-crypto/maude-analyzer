"""Validate MAUDE data quality."""

import duckdb
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config, SCS_PRODUCT_CODES, SCS_MANUFACTURERS
from config.logging_config import get_logger
from src.database import get_connection, get_table_counts

logger = get_logger("validator")


@dataclass
class ValidationResult:
    """Result of a validation check."""

    check_name: str
    passed: bool
    message: str
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationReport:
    """Complete validation report."""

    timestamp: datetime = field(default_factory=datetime.now)
    checks: List[ValidationResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Check if all validations passed."""
        return all(c.passed for c in self.checks)

    @property
    def summary(self) -> str:
        """Get summary string."""
        passed = sum(1 for c in self.checks if c.passed)
        total = len(self.checks)
        return f"{passed}/{total} checks passed"


class DataValidator:
    """Validate MAUDE data in the database."""

    def __init__(self, db_path: Optional[Path] = None):
        """
        Initialize the validator.

        Args:
            db_path: Path to database file.
        """
        self.db_path = db_path or config.database.path

    def run_all_checks(self) -> ValidationReport:
        """
        Run all validation checks.

        Returns:
            ValidationReport with all results.
        """
        report = ValidationReport()

        with get_connection(self.db_path) as conn:
            # Basic checks
            report.checks.append(self.check_tables_exist(conn))
            report.checks.append(self.check_record_counts(conn))

            # Data quality checks
            report.checks.append(self.check_primary_keys(conn))
            report.checks.append(self.check_date_ranges(conn))
            report.checks.append(self.check_product_codes(conn))
            report.checks.append(self.check_manufacturers(conn))
            report.checks.append(self.check_event_types(conn))

            # Referential integrity
            report.checks.append(self.check_device_references(conn))
            report.checks.append(self.check_patient_references(conn))

        return report

    def check_tables_exist(self, conn: duckdb.DuckDBPyConnection) -> ValidationResult:
        """Check that all required tables exist."""
        required_tables = [
            "master_events",
            "devices",
            "patients",
            "mdr_text",
            "device_problems",
        ]

        existing_tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        existing = {t[0] for t in existing_tables}

        missing = [t for t in required_tables if t not in existing]

        return ValidationResult(
            check_name="tables_exist",
            passed=len(missing) == 0,
            message=f"Missing tables: {missing}" if missing else "All required tables exist",
            details={"missing": missing, "existing": list(existing)},
        )

    def check_record_counts(self, conn: duckdb.DuckDBPyConnection) -> ValidationResult:
        """Check that tables have records."""
        counts = get_table_counts(conn)

        empty_tables = [t for t, c in counts.items() if c == 0]

        # master_events should have records; others might be empty depending on data
        critical_empty = "master_events" in empty_tables

        return ValidationResult(
            check_name="record_counts",
            passed=not critical_empty,
            message=(
                "No records in master_events table"
                if critical_empty
                else f"Tables have records. Empty: {empty_tables or 'None'}"
            ),
            details=counts,
        )

    def check_primary_keys(self, conn: duckdb.DuckDBPyConnection) -> ValidationResult:
        """Check for duplicate primary keys."""
        duplicates = {}

        # Check master_events
        result = conn.execute("""
            SELECT mdr_report_key, COUNT(*) as cnt
            FROM master_events
            GROUP BY mdr_report_key
            HAVING COUNT(*) > 1
            LIMIT 10
        """).fetchall()

        if result:
            duplicates["master_events"] = len(result)

        return ValidationResult(
            check_name="primary_keys",
            passed=len(duplicates) == 0,
            message=(
                f"Duplicate keys found in: {list(duplicates.keys())}"
                if duplicates
                else "No duplicate primary keys"
            ),
            details=duplicates,
        )

    def check_date_ranges(self, conn: duckdb.DuckDBPyConnection) -> ValidationResult:
        """Check that dates are within reasonable ranges."""
        issues = []

        # Check date_received range
        result = conn.execute("""
            SELECT
                MIN(date_received) as min_date,
                MAX(date_received) as max_date,
                COUNT(*) FILTER (WHERE date_received < '1990-01-01') as too_old,
                COUNT(*) FILTER (WHERE date_received > CURRENT_DATE + INTERVAL '1 year') as future
            FROM master_events
            WHERE date_received IS NOT NULL
        """).fetchone()

        if result:
            min_date, max_date, too_old, future = result

            if too_old and too_old > 0:
                issues.append(f"{too_old} records with date_received before 1990")

            if future and future > 0:
                issues.append(f"{future} records with future date_received")

        return ValidationResult(
            check_name="date_ranges",
            passed=len(issues) == 0,
            message="; ".join(issues) if issues else "Date ranges are valid",
            details={
                "min_date_received": str(min_date) if min_date else None,
                "max_date_received": str(max_date) if max_date else None,
            },
        )

    def check_product_codes(self, conn: duckdb.DuckDBPyConnection) -> ValidationResult:
        """Check that SCS product codes are present."""
        result = conn.execute("""
            SELECT product_code, COUNT(*) as cnt
            FROM master_events
            WHERE product_code IN (SELECT UNNEST(?))
            GROUP BY product_code
            ORDER BY cnt DESC
        """, [SCS_PRODUCT_CODES]).fetchall()

        found_codes = {r[0]: r[1] for r in result}
        missing_codes = [c for c in SCS_PRODUCT_CODES if c not in found_codes]

        return ValidationResult(
            check_name="product_codes",
            passed=len(found_codes) > 0,
            message=(
                f"Found SCS product codes: {list(found_codes.keys())}"
                if found_codes
                else "No SCS product codes found"
            ),
            details={"found": found_codes, "missing": missing_codes},
        )

    def check_manufacturers(self, conn: duckdb.DuckDBPyConnection) -> ValidationResult:
        """Check manufacturer distribution."""
        result = conn.execute("""
            SELECT manufacturer_clean, COUNT(*) as cnt
            FROM master_events
            WHERE manufacturer_clean IS NOT NULL
            GROUP BY manufacturer_clean
            ORDER BY cnt DESC
            LIMIT 20
        """).fetchall()

        top_manufacturers = {r[0]: r[1] for r in result}

        # Check if known SCS manufacturers are present
        scs_found = [m for m in SCS_MANUFACTURERS if m in top_manufacturers]

        return ValidationResult(
            check_name="manufacturers",
            passed=len(scs_found) > 0,
            message=f"Found {len(scs_found)} known SCS manufacturers",
            details={
                "top_manufacturers": top_manufacturers,
                "scs_manufacturers_found": scs_found,
            },
        )

    def check_event_types(self, conn: duckdb.DuckDBPyConnection) -> ValidationResult:
        """Check event type distribution."""
        result = conn.execute("""
            SELECT event_type, COUNT(*) as cnt
            FROM master_events
            WHERE event_type IS NOT NULL
            GROUP BY event_type
            ORDER BY cnt DESC
        """).fetchall()

        event_types = {r[0]: r[1] for r in result}

        valid_types = {"D", "IN", "M", "O", "*"}
        unknown_types = [t for t in event_types if t not in valid_types]

        return ValidationResult(
            check_name="event_types",
            passed=len(event_types) > 0,
            message=(
                f"Event types: {list(event_types.keys())}"
                + (f" (unknown: {unknown_types})" if unknown_types else "")
            ),
            details=event_types,
        )

    def check_device_references(
        self, conn: duckdb.DuckDBPyConnection
    ) -> ValidationResult:
        """Check referential integrity between devices and master_events."""
        result = conn.execute("""
            SELECT COUNT(*) as orphan_count
            FROM devices d
            LEFT JOIN master_events m ON d.mdr_report_key = m.mdr_report_key
            WHERE m.mdr_report_key IS NULL
        """).fetchone()

        orphan_count = result[0] if result else 0

        return ValidationResult(
            check_name="device_references",
            passed=orphan_count == 0,
            message=(
                f"{orphan_count} orphan device records"
                if orphan_count > 0
                else "All device records have matching master events"
            ),
            details={"orphan_count": orphan_count},
        )

    def check_patient_references(
        self, conn: duckdb.DuckDBPyConnection
    ) -> ValidationResult:
        """Check referential integrity between patients and master_events."""
        result = conn.execute("""
            SELECT COUNT(*) as orphan_count
            FROM patients p
            LEFT JOIN master_events m ON p.mdr_report_key = m.mdr_report_key
            WHERE m.mdr_report_key IS NULL
        """).fetchone()

        orphan_count = result[0] if result else 0

        return ValidationResult(
            check_name="patient_references",
            passed=orphan_count == 0,
            message=(
                f"{orphan_count} orphan patient records"
                if orphan_count > 0
                else "All patient records have matching master events"
            ),
            details={"orphan_count": orphan_count},
        )

    def get_data_summary(self, conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
        """
        Get comprehensive data summary.

        Args:
            conn: Database connection.

        Returns:
            Dictionary with summary statistics.
        """
        summary = {}

        # Record counts
        summary["record_counts"] = get_table_counts(conn)

        # Date range
        result = conn.execute("""
            SELECT
                MIN(date_received) as min_date,
                MAX(date_received) as max_date
            FROM master_events
        """).fetchone()
        summary["date_range"] = {
            "min": str(result[0]) if result[0] else None,
            "max": str(result[1]) if result[1] else None,
        }

        # Event type breakdown
        result = conn.execute("""
            SELECT event_type, COUNT(*) as cnt
            FROM master_events
            GROUP BY event_type
        """).fetchall()
        summary["event_types"] = {r[0]: r[1] for r in result}

        # Top manufacturers
        result = conn.execute("""
            SELECT manufacturer_clean, COUNT(*) as cnt
            FROM master_events
            GROUP BY manufacturer_clean
            ORDER BY cnt DESC
            LIMIT 10
        """).fetchall()
        summary["top_manufacturers"] = {r[0]: r[1] for r in result}

        # Product codes
        result = conn.execute("""
            SELECT product_code, COUNT(*) as cnt
            FROM master_events
            GROUP BY product_code
            ORDER BY cnt DESC
            LIMIT 10
        """).fetchall()
        summary["top_product_codes"] = {r[0]: r[1] for r in result}

        return summary


def print_validation_report(report: ValidationReport) -> None:
    """Print a formatted validation report."""
    print("\n" + "=" * 60)
    print("MAUDE DATA VALIDATION REPORT")
    print(f"Timestamp: {report.timestamp}")
    print(f"Status: {report.summary}")
    print("=" * 60)

    for check in report.checks:
        status = "PASS" if check.passed else "FAIL"
        print(f"\n[{status}] {check.check_name}")
        print(f"      {check.message}")

        if check.details and not check.passed:
            for key, value in check.details.items():
                if value:
                    print(f"      - {key}: {value}")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    validator = DataValidator()
    report = validator.run_all_checks()
    print_validation_report(report)

    # Print data summary
    with get_connection() as conn:
        summary = validator.get_data_summary(conn)
        print("\nData Summary:")
        for key, value in summary.items():
            print(f"  {key}: {value}")
