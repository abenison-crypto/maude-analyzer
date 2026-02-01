#!/usr/bin/env python3
"""
Audit Schema Compliance - Compare database schema vs FDA specification.

This script validates that the database schema matches the official FDA MAUDE
column specifications, identifying missing, extra, or mismatched columns.

Usage:
    python scripts/audit_schema_compliance.py
    python scripts/audit_schema_compliance.py --json --output compliance_report.json
    python scripts/audit_schema_compliance.py --table master_events
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set
import yaml

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from config.schema_registry import (
    MASTER_COLUMNS_FDA_86,
    MASTER_COLUMNS_FDA_84,
    DEVICE_COLUMNS_FDA_28,
    DEVICE_COLUMNS_FDA_34,
    PATIENT_COLUMNS_FDA,
    TEXT_COLUMNS_FDA,
    PROBLEM_COLUMNS_FDA,
    PATIENT_PROBLEM_COLUMNS_FDA,
)
from config.column_mappings import COLUMN_MAPPINGS
from src.database import get_connection
from src.database.schema import get_table_columns

logger = get_logger("audit_schema_compliance")


@dataclass
class ColumnComplianceResult:
    """Result of column compliance check."""
    column_name: str
    fda_name: str
    status: str  # 'OK', 'MISSING', 'EXTRA', 'TYPE_MISMATCH'
    expected_type: Optional[str] = None
    actual_type: Optional[str] = None
    notes: str = ""


@dataclass
class TableComplianceResult:
    """Result of table compliance check."""
    table_name: str
    fda_file_type: str
    total_columns: int = 0
    expected_columns: int = 0
    matched_columns: int = 0
    missing_columns: List[str] = field(default_factory=list)
    extra_columns: List[str] = field(default_factory=list)
    type_mismatches: List[Dict] = field(default_factory=list)
    compliance_pct: float = 0.0
    status: str = "UNKNOWN"
    column_details: List[ColumnComplianceResult] = field(default_factory=list)


@dataclass
class SchemaComplianceReport:
    """Complete schema compliance report."""
    timestamp: datetime
    database_path: str
    fda_spec_version: str
    overall_status: str = "UNKNOWN"
    tables_checked: int = 0
    tables_compliant: int = 0
    tables_non_compliant: int = 0
    table_results: Dict[str, TableComplianceResult] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON export."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "database_path": self.database_path,
            "fda_spec_version": self.fda_spec_version,
            "overall_status": self.overall_status,
            "summary": {
                "tables_checked": self.tables_checked,
                "tables_compliant": self.tables_compliant,
                "tables_non_compliant": self.tables_non_compliant,
            },
            "table_results": {
                name: asdict(result)
                for name, result in self.table_results.items()
            },
            "recommendations": self.recommendations,
        }


# FDA columns mapped to DB table names
FDA_TO_DB_TABLE_MAPPING = {
    "master": "master_events",
    "device": "devices",
    "patient": "patients",
    "text": "mdr_text",
    "problem": "device_problems",
    "patient_problem": "patient_problems",
}

# Expected FDA columns by file type (using 86-col master and 34-col device as current)
FDA_EXPECTED_COLUMNS = {
    "master": MASTER_COLUMNS_FDA_86,
    "device": DEVICE_COLUMNS_FDA_34,
    "patient": PATIENT_COLUMNS_FDA,
    "text": TEXT_COLUMNS_FDA,
    "problem": PROBLEM_COLUMNS_FDA,
    "patient_problem": PATIENT_PROBLEM_COLUMNS_FDA,
}


def load_fda_specification() -> Dict:
    """Load FDA specification from YAML file."""
    spec_path = PROJECT_ROOT / "config" / "fda_specification.yaml"
    if spec_path.exists():
        with open(spec_path, 'r') as f:
            return yaml.safe_load(f)
    return {}


def get_db_column_types(conn, table_name: str) -> Dict[str, str]:
    """Get column names and types from database table."""
    try:
        result = conn.execute(f"DESCRIBE {table_name}").fetchall()
        return {row[0]: row[1] for row in result}
    except Exception as e:
        logger.error(f"Error describing table {table_name}: {e}")
        return {}


def check_table_compliance(
    conn,
    file_type: str,
    table_name: str,
    fda_columns: List[str],
    column_mapping: Dict[str, str]
) -> TableComplianceResult:
    """
    Check compliance of a database table against FDA column specification.

    Args:
        conn: Database connection.
        file_type: FDA file type (master, device, etc.).
        table_name: Database table name.
        fda_columns: Expected FDA column names.
        column_mapping: Mapping from FDA to DB column names.

    Returns:
        TableComplianceResult with compliance details.
    """
    result = TableComplianceResult(
        table_name=table_name,
        fda_file_type=file_type,
        expected_columns=len(fda_columns),
    )

    # Get actual database columns
    db_columns = get_db_column_types(conn, table_name)
    result.total_columns = len(db_columns)

    # Expected DB column names (mapped from FDA)
    expected_db_columns = set()
    for fda_col in fda_columns:
        db_col = column_mapping.get(fda_col.upper())
        if db_col:
            expected_db_columns.add(db_col)
        else:
            # If no mapping, use lowercase version
            expected_db_columns.add(fda_col.lower())

    actual_db_columns = set(db_columns.keys())

    # Find missing columns (in FDA spec but not in DB)
    for fda_col in fda_columns:
        db_col = column_mapping.get(fda_col.upper(), fda_col.lower())
        if db_col not in actual_db_columns:
            result.missing_columns.append(fda_col)
            result.column_details.append(ColumnComplianceResult(
                column_name=db_col,
                fda_name=fda_col,
                status="MISSING",
                notes=f"FDA column {fda_col} not found in database"
            ))
        else:
            result.matched_columns += 1
            result.column_details.append(ColumnComplianceResult(
                column_name=db_col,
                fda_name=fda_col,
                status="OK",
                actual_type=db_columns.get(db_col)
            ))

    # Find extra columns (in DB but not in FDA spec)
    # Exclude known derived/metadata columns
    derived_columns = {
        "id", "created_at", "updated_at", "source_file",
        "manufacturer_clean", "manufacturer_d_clean",
        "event_year", "event_month", "received_year", "received_month",
        "patient_age_numeric", "patient_age_unit",
        "outcome_codes_raw", "treatment_codes_raw",
        "outcome_death", "outcome_life_threatening", "outcome_hospitalization",
        "outcome_disability", "outcome_congenital_anomaly",
        "outcome_required_intervention", "outcome_other",
        "treatment_drug", "treatment_device", "treatment_surgery",
        "treatment_other", "treatment_unknown", "treatment_no_information",
        "treatment_blood_products", "treatment_hospitalization", "treatment_physical_therapy",
        "baseline_report_number", "schema_version",
    }

    for db_col in actual_db_columns:
        if db_col not in expected_db_columns and db_col not in derived_columns:
            result.extra_columns.append(db_col)
            result.column_details.append(ColumnComplianceResult(
                column_name=db_col,
                fda_name="N/A",
                status="EXTRA",
                actual_type=db_columns.get(db_col),
                notes="Column not in FDA specification"
            ))

    # Calculate compliance percentage
    if result.expected_columns > 0:
        result.compliance_pct = round(
            (result.matched_columns / result.expected_columns) * 100, 2
        )

    # Determine status
    if result.compliance_pct == 100 and not result.extra_columns:
        result.status = "COMPLIANT"
    elif result.compliance_pct >= 95:
        result.status = "MOSTLY_COMPLIANT"
    elif result.compliance_pct >= 80:
        result.status = "PARTIAL"
    else:
        result.status = "NON_COMPLIANT"

    return result


def run_schema_compliance_audit(
    db_path: Path,
    tables: Optional[List[str]] = None
) -> SchemaComplianceReport:
    """
    Run complete schema compliance audit.

    Args:
        db_path: Path to database.
        tables: Optional list of specific tables to check.

    Returns:
        SchemaComplianceReport with all results.
    """
    fda_spec = load_fda_specification()

    report = SchemaComplianceReport(
        timestamp=datetime.now(),
        database_path=str(db_path),
        fda_spec_version=fda_spec.get("schema_version", "unknown"),
    )

    # Determine which tables to check
    if tables:
        tables_to_check = {
            ft: FDA_TO_DB_TABLE_MAPPING[ft]
            for ft in FDA_TO_DB_TABLE_MAPPING
            if FDA_TO_DB_TABLE_MAPPING[ft] in tables
        }
    else:
        tables_to_check = FDA_TO_DB_TABLE_MAPPING

    with get_connection(db_path, read_only=True) as conn:
        for file_type, table_name in tables_to_check.items():
            try:
                fda_columns = FDA_EXPECTED_COLUMNS.get(file_type, [])
                column_mapping = COLUMN_MAPPINGS.get(file_type, {})

                result = check_table_compliance(
                    conn, file_type, table_name, fda_columns, column_mapping
                )
                report.table_results[table_name] = result
                report.tables_checked += 1

                if result.status == "COMPLIANT":
                    report.tables_compliant += 1
                else:
                    report.tables_non_compliant += 1

                # Add recommendations for issues
                if result.missing_columns:
                    report.recommendations.append(
                        f"{table_name}: Add missing FDA columns: {', '.join(result.missing_columns[:5])}"
                        + (f" (+{len(result.missing_columns)-5} more)" if len(result.missing_columns) > 5 else "")
                    )

            except Exception as e:
                logger.error(f"Error checking {table_name}: {e}")
                report.recommendations.append(f"Could not check {table_name}: {e}")

    # Determine overall status
    if report.tables_checked == 0:
        report.overall_status = "NO_TABLES_CHECKED"
    elif report.tables_compliant == report.tables_checked:
        report.overall_status = "FULLY_COMPLIANT"
    elif report.tables_compliant > 0:
        report.overall_status = "PARTIALLY_COMPLIANT"
    else:
        report.overall_status = "NON_COMPLIANT"

    return report


def print_report(report: SchemaComplianceReport) -> None:
    """Print compliance report to console."""
    print("=" * 70)
    print("FDA MAUDE SCHEMA COMPLIANCE AUDIT")
    print("=" * 70)
    print(f"\nTimestamp: {report.timestamp}")
    print(f"Database: {report.database_path}")
    print(f"FDA Spec Version: {report.fda_spec_version}")
    print(f"Overall Status: {report.overall_status}")

    print("\n" + "-" * 70)
    print("SUMMARY")
    print("-" * 70)
    print(f"  Tables Checked:      {report.tables_checked}")
    print(f"  Tables Compliant:    {report.tables_compliant}")
    print(f"  Tables Non-Compliant: {report.tables_non_compliant}")

    print("\n" + "-" * 70)
    print("TABLE DETAILS")
    print("-" * 70)

    for table_name, result in report.table_results.items():
        status_symbol = {
            "COMPLIANT": "[OK]",
            "MOSTLY_COMPLIANT": "[~]",
            "PARTIAL": "[!]",
            "NON_COMPLIANT": "[X]",
        }.get(result.status, "[?]")

        print(f"\n  {table_name} {status_symbol}")
        print(f"    FDA File Type: {result.fda_file_type}")
        print(f"    Expected Columns: {result.expected_columns}")
        print(f"    Matched Columns:  {result.matched_columns}")
        print(f"    Compliance:       {result.compliance_pct}%")

        if result.missing_columns:
            print(f"    Missing ({len(result.missing_columns)}):")
            for col in result.missing_columns[:5]:
                print(f"      - {col}")
            if len(result.missing_columns) > 5:
                print(f"      ... and {len(result.missing_columns) - 5} more")

        if result.extra_columns:
            print(f"    Extra (non-FDA) columns ({len(result.extra_columns)}):")
            for col in result.extra_columns[:5]:
                print(f"      - {col}")
            if len(result.extra_columns) > 5:
                print(f"      ... and {len(result.extra_columns) - 5} more")

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
        description="Audit database schema compliance with FDA MAUDE specification",
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
        "--table",
        action="append",
        dest="tables",
        help="Specific table(s) to check (can be specified multiple times)",
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

    # Run audit
    report = run_schema_compliance_audit(args.db, args.tables)

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
    return 0 if report.overall_status in ["FULLY_COMPLIANT", "PARTIALLY_COMPLIANT"] else 1


if __name__ == "__main__":
    sys.exit(main())
