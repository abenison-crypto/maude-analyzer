#!/usr/bin/env python3
"""
Schema Validation Script for MAUDE Analyzer.

Validates that the database schema matches the expected configuration defined
in config/schema_config.yaml. Reports missing columns, type mismatches, and
data coverage issues.

Usage:
    python scripts/validate_schema.py --db data/maude.duckdb
    python scripts/validate_schema.py --db data/maude.duckdb --strict
    python scripts/validate_schema.py --db data/maude.duckdb --coverage
"""

import argparse
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import yaml

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import get_logger

logger = get_logger("validate_schema")


@dataclass
class ColumnValidation:
    """Result of validating a single column."""
    column_name: str
    exists: bool
    expected_type: Optional[str] = None
    actual_type: Optional[str] = None
    type_match: bool = True
    coverage: Optional[float] = None
    coverage_threshold: Optional[float] = None
    coverage_ok: bool = True
    is_sparse: bool = False
    issues: List[str] = field(default_factory=list)


@dataclass
class TableValidation:
    """Result of validating a single table."""
    table_name: str
    exists: bool
    row_count: int = 0
    expected_min_rows: int = 0
    columns: Dict[str, ColumnValidation] = field(default_factory=dict)
    missing_columns: List[str] = field(default_factory=list)
    extra_columns: List[str] = field(default_factory=list)
    issues: List[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """Check if table validation passed."""
        if not self.exists:
            return False
        if self.missing_columns:
            return False
        return all(col.exists and col.type_match for col in self.columns.values())


@dataclass
class SchemaValidationResult:
    """Complete schema validation result."""
    timestamp: datetime
    db_path: str
    config_version: str
    tables: Dict[str, TableValidation] = field(default_factory=dict)
    global_issues: List[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """Check if overall validation passed."""
        return all(t.is_valid for t in self.tables.values()) and not self.global_issues

    @property
    def total_issues(self) -> int:
        """Count total issues across all tables."""
        count = len(self.global_issues)
        for table in self.tables.values():
            count += len(table.issues)
            for col in table.columns.values():
                count += len(col.issues)
        return count


class SchemaValidator:
    """Validates database schema against YAML configuration."""

    def __init__(self, db_path: str, config_path: Optional[str] = None):
        """
        Initialize the validator.

        Args:
            db_path: Path to DuckDB database file
            config_path: Path to schema config YAML (defaults to config/schema_config.yaml)
        """
        self.db_path = Path(db_path)
        self.config_path = Path(config_path) if config_path else PROJECT_ROOT / "config" / "schema_config.yaml"
        self.config: Dict[str, Any] = {}
        self.conn: Optional[duckdb.DuckDBPyConnection] = None

    def load_config(self) -> None:
        """Load schema configuration from YAML."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Schema config not found: {self.config_path}")

        with open(self.config_path) as f:
            self.config = yaml.safe_load(f)

        logger.info(f"Loaded schema config version {self.config.get('schema_version', 'unknown')}")

    def connect(self) -> None:
        """Connect to the database."""
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")

        self.conn = duckdb.connect(str(self.db_path), read_only=True)
        logger.info(f"Connected to database: {self.db_path}")

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def get_table_columns(self, table_name: str) -> Dict[str, str]:
        """
        Get column names and types for a table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary mapping column names to their types
        """
        try:
            result = self.conn.execute(f"DESCRIBE {table_name}").fetchall()
            return {row[0]: row[1] for row in result}
        except Exception as e:
            logger.warning(f"Could not describe table {table_name}: {e}")
            return {}

    def get_table_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        try:
            result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            return result[0] if result else 0
        except Exception:
            return 0

    def get_column_coverage(self, table_name: str, column_name: str, row_count: int) -> float:
        """
        Calculate data coverage for a column.

        Args:
            table_name: Name of the table
            column_name: Name of the column
            row_count: Total row count in table

        Returns:
            Coverage percentage (0.0 to 1.0)
        """
        if row_count == 0:
            return 0.0

        try:
            result = self.conn.execute(f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE {column_name} IS NOT NULL
                  AND CAST({column_name} AS VARCHAR) != ''
            """).fetchone()
            non_null_count = result[0] if result else 0
            return non_null_count / row_count
        except Exception as e:
            logger.debug(f"Could not calculate coverage for {table_name}.{column_name}: {e}")
            return 0.0

    def validate_table(
        self,
        table_name: str,
        table_config: Dict[str, Any],
        check_coverage: bool = False
    ) -> TableValidation:
        """
        Validate a single table against its configuration.

        Args:
            table_name: Name of the table
            table_config: Configuration dictionary for the table
            check_coverage: Whether to check data coverage thresholds

        Returns:
            TableValidation result
        """
        result = TableValidation(table_name=table_name, exists=False)

        # Check if table exists
        actual_columns = self.get_table_columns(table_name)
        if not actual_columns:
            result.issues.append(f"Table '{table_name}' does not exist or is empty")
            return result

        result.exists = True
        result.row_count = self.get_table_row_count(table_name)
        result.expected_min_rows = table_config.get("row_count_threshold", 0)

        # Check row count threshold
        if result.row_count < result.expected_min_rows:
            result.issues.append(
                f"Row count ({result.row_count:,}) below threshold ({result.expected_min_rows:,})"
            )

        # Get expected columns from config
        expected_columns = table_config.get("columns", {})

        # Find missing and extra columns
        expected_names = set(expected_columns.keys())
        actual_names = set(actual_columns.keys())

        result.missing_columns = list(expected_names - actual_names)
        result.extra_columns = list(actual_names - expected_names)

        for missing in result.missing_columns:
            col_config = expected_columns.get(missing, {})
            if col_config.get("required", False):
                result.issues.append(f"Missing required column: {missing}")

        # Validate each expected column
        for col_name, col_config in expected_columns.items():
            col_validation = ColumnValidation(
                column_name=col_name,
                exists=col_name in actual_columns,
                expected_type=col_config.get("type")
            )

            if col_validation.exists:
                col_validation.actual_type = actual_columns[col_name]

                # Check type compatibility (simplified check)
                if col_validation.expected_type:
                    expected_upper = col_validation.expected_type.upper()
                    actual_upper = col_validation.actual_type.upper()

                    # Normalize common type variations
                    type_mappings = {
                        "VARCHAR": ["VARCHAR", "STRING", "TEXT"],
                        "INTEGER": ["INTEGER", "INT", "BIGINT", "SMALLINT", "INT32", "INT64"],
                        "BOOLEAN": ["BOOLEAN", "BOOL"],
                        "DATE": ["DATE"],
                        "TIMESTAMP": ["TIMESTAMP", "DATETIME", "TIMESTAMP WITH TIME ZONE"],
                        "DECIMAL": ["DECIMAL", "DOUBLE", "FLOAT", "NUMERIC"],
                        "TEXT": ["TEXT", "VARCHAR", "STRING"],
                    }

                    col_validation.type_match = False
                    for canonical, variants in type_mappings.items():
                        if expected_upper in variants or expected_upper == canonical:
                            if any(v in actual_upper for v in variants) or actual_upper == canonical:
                                col_validation.type_match = True
                                break

                    if not col_validation.type_match:
                        col_validation.issues.append(
                            f"Type mismatch: expected {col_validation.expected_type}, got {col_validation.actual_type}"
                        )

                # Check coverage if requested
                if check_coverage:
                    coverage_threshold = col_config.get("coverage_threshold")
                    if coverage_threshold is not None:
                        col_validation.coverage = self.get_column_coverage(
                            table_name, col_name, result.row_count
                        )
                        col_validation.coverage_threshold = coverage_threshold
                        col_validation.coverage_ok = col_validation.coverage >= coverage_threshold

                        if not col_validation.coverage_ok:
                            col_validation.issues.append(
                                f"Coverage ({col_validation.coverage:.1%}) below threshold ({coverage_threshold:.1%})"
                            )

                    # Check if column is known sparse
                    if col_config.get("sparse", False):
                        col_validation.is_sparse = True

            else:
                if col_config.get("required", False):
                    col_validation.issues.append("Required column is missing")

            result.columns[col_name] = col_validation

        return result

    def validate(self, check_coverage: bool = False, strict: bool = False) -> SchemaValidationResult:
        """
        Run full schema validation.

        Args:
            check_coverage: Whether to check data coverage thresholds
            strict: Whether to fail on warnings (extra columns, etc.)

        Returns:
            SchemaValidationResult with all findings
        """
        result = SchemaValidationResult(
            timestamp=datetime.now(),
            db_path=str(self.db_path),
            config_version=self.config.get("schema_version", "unknown")
        )

        # Validate main tables
        tables_config = self.config.get("tables", {})
        for table_name, table_config in tables_config.items():
            table_result = self.validate_table(table_name, table_config, check_coverage)
            result.tables[table_name] = table_result

            if strict and table_result.extra_columns:
                table_result.issues.append(
                    f"Unexpected columns found: {', '.join(table_result.extra_columns)}"
                )

        # Validate lookup tables
        lookup_config = self.config.get("lookup_tables", {})
        for table_name, table_config in lookup_config.items():
            table_result = self.validate_table(table_name, table_config, check_coverage=False)
            result.tables[table_name] = table_result

        return result


def format_validation_report(result: SchemaValidationResult, verbose: bool = False) -> str:
    """
    Format validation result as a human-readable report.

    Args:
        result: SchemaValidationResult to format
        verbose: Include detailed column information

    Returns:
        Formatted report string
    """
    lines = []
    lines.append("=" * 70)
    lines.append("MAUDE ANALYZER SCHEMA VALIDATION REPORT")
    lines.append("=" * 70)
    lines.append(f"Timestamp: {result.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Database: {result.db_path}")
    lines.append(f"Config Version: {result.config_version}")
    lines.append(f"Overall Status: {'PASS' if result.is_valid else 'FAIL'}")
    lines.append(f"Total Issues: {result.total_issues}")
    lines.append("")

    # Global issues
    if result.global_issues:
        lines.append("GLOBAL ISSUES:")
        for issue in result.global_issues:
            lines.append(f"  - {issue}")
        lines.append("")

    # Per-table results
    for table_name, table_result in sorted(result.tables.items()):
        status = "PASS" if table_result.is_valid else "FAIL"
        exists_str = "exists" if table_result.exists else "MISSING"

        lines.append("-" * 70)
        lines.append(f"Table: {table_name} [{status}] ({exists_str})")

        if table_result.exists:
            lines.append(f"  Rows: {table_result.row_count:,}")
            if table_result.expected_min_rows > 0:
                lines.append(f"  Expected minimum: {table_result.expected_min_rows:,}")

        if table_result.missing_columns:
            lines.append(f"  Missing columns: {', '.join(sorted(table_result.missing_columns))}")

        if table_result.issues:
            lines.append("  Issues:")
            for issue in table_result.issues:
                lines.append(f"    - {issue}")

        if verbose:
            # Show column details
            coverage_issues = []
            type_issues = []

            for col_name, col_result in sorted(table_result.columns.items()):
                if col_result.issues:
                    for issue in col_result.issues:
                        if "Coverage" in issue:
                            coverage_issues.append(f"    {col_name}: {issue}")
                        elif "Type" in issue:
                            type_issues.append(f"    {col_name}: {issue}")

            if type_issues:
                lines.append("  Type issues:")
                lines.extend(type_issues)

            if coverage_issues:
                lines.append("  Coverage issues:")
                lines.extend(coverage_issues)

        lines.append("")

    lines.append("=" * 70)
    lines.append(f"VALIDATION {'PASSED' if result.is_valid else 'FAILED'}")
    lines.append("=" * 70)

    return "\n".join(lines)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Validate MAUDE database schema against configuration"
    )
    parser.add_argument(
        "--db",
        type=str,
        default="data/maude.duckdb",
        help="Path to DuckDB database file"
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Path to schema config YAML (default: config/schema_config.yaml)"
    )
    parser.add_argument(
        "--coverage",
        action="store_true",
        help="Check data coverage thresholds"
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail on warnings (extra columns, etc.)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed column information"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON"
    )

    args = parser.parse_args()

    # Resolve paths relative to project root
    db_path = PROJECT_ROOT / args.db if not Path(args.db).is_absolute() else Path(args.db)

    try:
        validator = SchemaValidator(str(db_path), args.config)
        validator.load_config()
        validator.connect()

        result = validator.validate(
            check_coverage=args.coverage,
            strict=args.strict
        )

        validator.close()

        if args.json:
            import json
            # Convert to JSON-serializable dict
            output = {
                "timestamp": result.timestamp.isoformat(),
                "db_path": result.db_path,
                "config_version": result.config_version,
                "is_valid": result.is_valid,
                "total_issues": result.total_issues,
                "tables": {}
            }
            for table_name, table_result in result.tables.items():
                output["tables"][table_name] = {
                    "exists": table_result.exists,
                    "is_valid": table_result.is_valid,
                    "row_count": table_result.row_count,
                    "missing_columns": table_result.missing_columns,
                    "issues": table_result.issues
                }
            print(json.dumps(output, indent=2))
        else:
            report = format_validation_report(result, verbose=args.verbose)
            print(report)

        # Exit with appropriate code
        sys.exit(0 if result.is_valid else 1)

    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(2)
    except Exception as e:
        logger.exception(f"Validation failed: {e}")
        sys.exit(2)


if __name__ == "__main__":
    main()
