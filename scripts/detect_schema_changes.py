#!/usr/bin/env python3
"""
Schema Change Detection Script for MAUDE Analyzer.

This script compares the current database schema against the Unified Schema Registry
and reports any differences. Useful for:
- Detecting when FDA adds new columns
- Validating schema after data ingestion
- Generating registry updates when schema changes

Usage:
    python scripts/detect_schema_changes.py [--db-path PATH] [--suggest-updates]

Examples:
    # Check current database against registry
    python scripts/detect_schema_changes.py

    # Check specific database file
    python scripts/detect_schema_changes.py --db-path data/maude.duckdb

    # Generate suggested registry updates
    python scripts/detect_schema_changes.py --suggest-updates
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb

from config.unified_schema import (
    get_schema_registry,
    SCHEMA_VERSION,
    SchemaEvolution,
    ColumnDefinition,
)


@dataclass
class SchemaDiff:
    """Represents differences between database and registry schema."""
    table: str
    missing_in_db: List[str] = field(default_factory=list)
    missing_in_registry: List[str] = field(default_factory=list)
    type_mismatches: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class SchemaChangeReport:
    """Complete schema change detection report."""
    registry_version: str = SCHEMA_VERSION
    database_path: str = ""
    tables_in_db: List[str] = field(default_factory=list)
    tables_in_registry: List[str] = field(default_factory=list)
    missing_tables_in_db: List[str] = field(default_factory=list)
    extra_tables_in_db: List[str] = field(default_factory=list)
    table_diffs: Dict[str, SchemaDiff] = field(default_factory=dict)
    suggested_updates: List[str] = field(default_factory=list)

    def has_differences(self) -> bool:
        """Check if there are any schema differences."""
        if self.missing_tables_in_db or self.extra_tables_in_db:
            return True
        for diff in self.table_diffs.values():
            if diff.missing_in_db or diff.missing_in_registry or diff.type_mismatches:
                return True
        return False

    def print_report(self, verbose: bool = False):
        """Print the schema change report."""
        print("=" * 70)
        print("SCHEMA CHANGE DETECTION REPORT")
        print("=" * 70)
        print(f"Registry Version: {self.registry_version}")
        print(f"Database: {self.database_path}")
        print()

        if not self.has_differences():
            print("No schema differences detected.")
            print()
            return

        # Missing tables
        if self.missing_tables_in_db:
            print("MISSING TABLES IN DATABASE:")
            for table in self.missing_tables_in_db:
                print(f"  - {table}")
            print()

        # Extra tables
        if self.extra_tables_in_db:
            print("EXTRA TABLES IN DATABASE (not in registry):")
            for table in self.extra_tables_in_db:
                print(f"  + {table}")
            print()

        # Column differences
        for table, diff in self.table_diffs.items():
            if not (diff.missing_in_db or diff.missing_in_registry or diff.type_mismatches):
                continue

            print(f"TABLE: {table}")
            print("-" * 40)

            if diff.missing_in_db:
                print("  Columns missing in database (expected by registry):")
                for col in diff.missing_in_db:
                    # Check if it's an optional column
                    optional_marker = " (optional)" if col in SchemaEvolution.get_optional_columns(table) else ""
                    print(f"    - {col}{optional_marker}")

            if diff.missing_in_registry:
                print("  Columns in database but not in registry:")
                for col in diff.missing_in_registry:
                    print(f"    + {col}")

            if diff.type_mismatches:
                print("  Type mismatches:")
                for mismatch in diff.type_mismatches:
                    print(f"    ! {mismatch['column']}: registry={mismatch['expected']}, db={mismatch['actual']}")

            print()

        # Suggested updates
        if self.suggested_updates:
            print("SUGGESTED REGISTRY UPDATES:")
            print("-" * 40)
            for update in self.suggested_updates:
                print(update)
            print()


def detect_schema_changes(db_path: Optional[str] = None) -> SchemaChangeReport:
    """
    Detect differences between database schema and registry.

    Args:
        db_path: Path to DuckDB database file

    Returns:
        SchemaChangeReport with all detected differences
    """
    registry = get_schema_registry()
    report = SchemaChangeReport()

    # Determine database path
    if db_path:
        report.database_path = db_path
    else:
        # Try to find default database
        project_root = Path(__file__).parent.parent
        possible_paths = [
            project_root / "data" / "maude.duckdb",
            project_root / "maude.duckdb",
        ]
        for path in possible_paths:
            if path.exists():
                report.database_path = str(path)
                break

    if not report.database_path:
        print("Error: No database found. Specify path with --db-path")
        sys.exit(1)

    # Connect to database
    try:
        conn = duckdb.connect(report.database_path, read_only=True)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

    try:
        # Get tables from database
        tables_result = conn.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
        """).fetchall()
        report.tables_in_db = [row[0] for row in tables_result]

        # Get tables from registry
        report.tables_in_registry = list(registry.tables.keys())

        # Find missing/extra tables
        db_table_set = set(report.tables_in_db)
        registry_table_set = set(report.tables_in_registry)

        report.missing_tables_in_db = list(registry_table_set - db_table_set)
        report.extra_tables_in_db = list(db_table_set - registry_table_set)

        # Compare columns for each table in both
        common_tables = db_table_set & registry_table_set

        for table in common_tables:
            diff = compare_table_columns(conn, table, registry)
            if diff.missing_in_db or diff.missing_in_registry or diff.type_mismatches:
                report.table_diffs[table] = diff

    finally:
        conn.close()

    return report


def compare_table_columns(conn, table: str, registry) -> SchemaDiff:
    """Compare columns in a single table."""
    diff = SchemaDiff(table=table)

    # Get columns from database
    try:
        describe_result = conn.execute(f"DESCRIBE {table}").fetchall()
        db_columns = {row[0]: row[1] for row in describe_result}
    except Exception as e:
        print(f"Warning: Could not describe table {table}: {e}")
        return diff

    # Get columns from registry
    table_def = registry.get_table(table)
    if not table_def:
        return diff

    registry_columns = {col.db_name: col for col in table_def.columns}

    # Find missing columns
    db_col_set = set(db_columns.keys())
    registry_col_set = set(registry_columns.keys())

    # Columns in registry but not in database
    for col in registry_col_set - db_col_set:
        col_def = registry_columns[col]
        # Only report if not optional
        if not col_def.is_optional:
            diff.missing_in_db.append(col)

    # Columns in database but not in registry
    diff.missing_in_registry = list(db_col_set - registry_col_set)

    return diff


def generate_suggested_updates(report: SchemaChangeReport) -> List[str]:
    """Generate Python code suggestions for registry updates."""
    suggestions = []

    for table, diff in report.table_diffs.items():
        if diff.missing_in_registry:
            suggestions.append(f"\n# New columns found in {table}:")
            for col in diff.missing_in_registry:
                # Generate a column definition template
                suggestions.append(f'''ColumnDefinition("{col}", "{col.upper()}", "{table}", "VARCHAR",
    is_optional=True, description="New column - verify type"),''')

    return suggestions


def main():
    parser = argparse.ArgumentParser(
        description="Detect schema differences between database and registry"
    )
    parser.add_argument(
        "--db-path", "-d",
        help="Path to DuckDB database file",
        default=None
    )
    parser.add_argument(
        "--suggest-updates", "-s",
        help="Generate suggested registry updates",
        action="store_true"
    )
    parser.add_argument(
        "--verbose", "-v",
        help="Show verbose output",
        action="store_true"
    )
    parser.add_argument(
        "--json",
        help="Output as JSON",
        action="store_true"
    )

    args = parser.parse_args()

    # Detect schema changes
    report = detect_schema_changes(args.db_path)

    # Generate suggestions if requested
    if args.suggest_updates:
        report.suggested_updates = generate_suggested_updates(report)

    # Output results
    if args.json:
        import json
        output = {
            "registry_version": report.registry_version,
            "database_path": report.database_path,
            "has_differences": report.has_differences(),
            "missing_tables_in_db": report.missing_tables_in_db,
            "extra_tables_in_db": report.extra_tables_in_db,
            "table_diffs": {
                table: {
                    "missing_in_db": diff.missing_in_db,
                    "missing_in_registry": diff.missing_in_registry,
                    "type_mismatches": diff.type_mismatches,
                }
                for table, diff in report.table_diffs.items()
            },
        }
        print(json.dumps(output, indent=2))
    else:
        report.print_report(verbose=args.verbose)

    # Exit with error code if differences found
    if report.has_differences():
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
