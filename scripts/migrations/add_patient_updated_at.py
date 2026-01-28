#!/usr/bin/env python3
"""
Migration: Add updated_at column to patients table.

This migration adds an `updated_at` timestamp column to the patients table
to enable tracking when patient records are modified (e.g., by CHANGE files).

Usage:
    python scripts/migrations/add_patient_updated_at.py --db data/maude.duckdb
    python scripts/migrations/add_patient_updated_at.py --db data/maude.duckdb --dry-run
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import duckdb

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import get_logger

logger = get_logger("migration_patient_updated_at")

MIGRATION_NAME = "add_patient_updated_at"
MIGRATION_VERSION = "2.1.1"


def check_column_exists(conn: duckdb.DuckDBPyConnection, table: str, column: str) -> bool:
    """Check if a column exists in a table."""
    try:
        result = conn.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table}'
              AND column_name = '{column}'
        """).fetchone()
        return result is not None
    except Exception as e:
        logger.warning(f"Could not check column existence: {e}")
        return False


def get_row_count(conn: duckdb.DuckDBPyConnection, table: str) -> int:
    """Get row count for a table."""
    try:
        result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        return result[0] if result else 0
    except Exception:
        return 0


def run_migration(db_path: str, dry_run: bool = False) -> bool:
    """
    Run the migration to add updated_at column to patients table.

    Args:
        db_path: Path to DuckDB database
        dry_run: If True, only show what would be done

    Returns:
        True if migration succeeded, False otherwise
    """
    logger.info(f"Starting migration: {MIGRATION_NAME}")
    logger.info(f"Database: {db_path}")

    if dry_run:
        logger.info("DRY RUN MODE - No changes will be made")

    conn = None
    try:
        conn = duckdb.connect(db_path, read_only=dry_run)

        # Check if column already exists
        if check_column_exists(conn, "patients", "updated_at"):
            logger.info("Column 'updated_at' already exists in patients table - skipping migration")
            return True

        row_count = get_row_count(conn, "patients")
        logger.info(f"Patients table has {row_count:,} rows")

        if dry_run:
            logger.info("Would execute: ALTER TABLE patients ADD COLUMN updated_at TIMESTAMP")
            logger.info("Would execute: UPDATE patients SET updated_at = created_at WHERE updated_at IS NULL")
            logger.info(f"Would update {row_count:,} rows")
            return True

        # Start transaction
        logger.info("Adding updated_at column to patients table...")

        # Add the column
        conn.execute("""
            ALTER TABLE patients ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """)
        logger.info("Column added successfully")

        # Backfill with created_at values
        logger.info("Backfilling updated_at with created_at values...")

        # Count rows that need updating first
        needs_update = conn.execute("""
            SELECT COUNT(*) FROM patients WHERE updated_at IS NULL
        """).fetchone()[0]

        conn.execute("""
            UPDATE patients
            SET updated_at = COALESCE(created_at, CURRENT_TIMESTAMP)
            WHERE updated_at IS NULL
        """)

        logger.info(f"Backfilled {needs_update:,} rows")

        # Record migration in app_settings
        logger.info("Recording migration in app_settings...")
        conn.execute("""
            INSERT INTO app_settings (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
        """, [f"migration_{MIGRATION_NAME}", f"completed:{datetime.now().isoformat()}"])

        # Update schema version
        conn.execute("""
            INSERT INTO app_settings (key, value, updated_at)
            VALUES ('schema_version', ?, CURRENT_TIMESTAMP)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP
        """, [MIGRATION_VERSION])

        logger.info(f"Migration {MIGRATION_NAME} completed successfully")
        logger.info(f"Schema version updated to {MIGRATION_VERSION}")

        return True

    except Exception as e:
        logger.exception(f"Migration failed: {e}")
        return False

    finally:
        if conn:
            conn.close()


def verify_migration(db_path: str) -> bool:
    """
    Verify the migration was applied correctly.

    Args:
        db_path: Path to DuckDB database

    Returns:
        True if migration is verified, False otherwise
    """
    logger.info("Verifying migration...")

    conn = None
    try:
        conn = duckdb.connect(db_path, read_only=True)

        # Check column exists
        if not check_column_exists(conn, "patients", "updated_at"):
            logger.error("Verification failed: updated_at column does not exist")
            return False

        # Check column has data
        result = conn.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(updated_at) as with_timestamp
            FROM patients
        """).fetchone()

        total, with_timestamp = result
        coverage = with_timestamp / total if total > 0 else 0

        logger.info(f"Total rows: {total:,}")
        logger.info(f"Rows with updated_at: {with_timestamp:,} ({coverage:.1%})")

        if coverage < 0.99:
            logger.warning(f"Coverage is below 99%: {coverage:.1%}")

        # Check migration record
        result = conn.execute("""
            SELECT value FROM app_settings WHERE key = ?
        """, [f"migration_{MIGRATION_NAME}"]).fetchone()

        if result:
            logger.info(f"Migration record found: {result[0]}")
        else:
            logger.warning("Migration record not found in app_settings")

        logger.info("Verification complete")
        return True

    except Exception as e:
        logger.exception(f"Verification failed: {e}")
        return False

    finally:
        if conn:
            conn.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Add updated_at column to patients table"
    )
    parser.add_argument(
        "--db",
        type=str,
        default="data/maude.duckdb",
        help="Path to DuckDB database file"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify migration was applied correctly"
    )

    args = parser.parse_args()

    # Resolve path relative to project root
    db_path = PROJECT_ROOT / args.db if not Path(args.db).is_absolute() else Path(args.db)

    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        sys.exit(2)

    if args.verify:
        success = verify_migration(str(db_path))
    else:
        success = run_migration(str(db_path), dry_run=args.dry_run)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
