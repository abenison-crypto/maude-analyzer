#!/usr/bin/env python
"""
Database Schema Migration Script - Migrate to Schema v2.0

This script migrates existing MAUDE databases to the new schema that supports
all FDA fields.

Changes in v2.0:
- Master Events: Added ~30 new columns for complete FDA field coverage
- Devices: Added device_event_key column
- Patients: Added 5 patient demographic fields + derived age fields
- MDR Text: Added mdr_text_key column
- Added download_state table for tracking downloads

Usage:
    python scripts/migrate_schema_v2.py [options]

Options:
    --db PATH           Path to database file (default: config path)
    --backup            Create backup before migration
    --dry-run           Show what would be done without making changes
    --force             Skip confirmation prompt
"""

import argparse
import shutil
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from src.database import get_connection
from src.database.schema import (
    SCHEMA_VERSION,
    get_schema_version,
    get_table_columns,
)

# Column additions for each table
MASTER_NEW_COLUMNS = [
    ("adverse_event_flag", "VARCHAR"),
    ("product_problem_flag", "VARCHAR"),
    ("reprocessed_and_reused_flag", "VARCHAR"),
    ("manufacturer_contact_title", "VARCHAR"),
    ("manufacturer_contact_first_name", "VARCHAR"),
    ("manufacturer_contact_last_name", "VARCHAR"),
    ("manufacturer_g1_name", "VARCHAR"),
    ("manufacturer_g1_street_1", "VARCHAR"),
    ("manufacturer_g1_street_2", "VARCHAR"),
    ("manufacturer_g1_city", "VARCHAR"),
    ("manufacturer_g1_state", "VARCHAR"),
    ("manufacturer_g1_zip", "VARCHAR"),
    ("manufacturer_g1_zip_ext", "VARCHAR"),
    ("manufacturer_g1_country", "VARCHAR"),
    ("manufacturer_g1_postal", "VARCHAR"),
    ("device_date_of_manufacture", "DATE"),
    ("mfr_report_type", "VARCHAR"),
    ("source_type", "VARCHAR"),
    ("date_added", "DATE"),
    ("date_changed", "DATE"),
    ("reporter_state_code", "VARCHAR"),
    ("reporter_country_code", "VARCHAR"),
    ("noe_summarized", "VARCHAR"),
    ("supplemental_dates_fda_received", "VARCHAR"),
    ("supplemental_dates_mfr_received", "VARCHAR"),
    ("previous_use_code", "VARCHAR"),
    ("baseline_report_number", "VARCHAR"),
    ("schema_version", "VARCHAR"),
    ("manufacturer_link_flag_old", "VARCHAR"),
]

DEVICE_NEW_COLUMNS = [
    ("device_event_key", "VARCHAR"),
    ("implant_flag", "VARCHAR"),
    ("date_removed_flag", "VARCHAR"),
    ("manufacturer_d_country", "VARCHAR"),
    ("manufacturer_d_postal", "VARCHAR"),
]

PATIENT_NEW_COLUMNS = [
    ("patient_age", "VARCHAR"),
    ("patient_sex", "VARCHAR"),
    ("patient_weight", "VARCHAR"),
    ("patient_ethnicity", "VARCHAR"),
    ("patient_race", "VARCHAR"),
    ("patient_age_numeric", "DECIMAL"),
    ("patient_age_unit", "VARCHAR"),
    ("treatment_codes_raw", "VARCHAR"),
]

TEXT_NEW_COLUMNS = [
    ("mdr_text_key", "VARCHAR"),
    ("date_report", "DATE"),
]


def get_missing_columns(conn, table_name: str, new_columns: list) -> list:
    """Get list of columns that need to be added."""
    existing = set(get_table_columns(conn, table_name))
    missing = []
    for col_name, col_type in new_columns:
        if col_name.lower() not in [c.lower() for c in existing]:
            missing.append((col_name, col_type))
    return missing


def generate_alter_statements(conn) -> list:
    """Generate ALTER TABLE statements for migration."""
    statements = []

    # Master table
    missing_master = get_missing_columns(conn, "master_events", MASTER_NEW_COLUMNS)
    for col_name, col_type in missing_master:
        statements.append(
            f"ALTER TABLE master_events ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
        )

    # Devices table
    missing_device = get_missing_columns(conn, "devices", DEVICE_NEW_COLUMNS)
    for col_name, col_type in missing_device:
        statements.append(
            f"ALTER TABLE devices ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
        )

    # Patients table
    missing_patient = get_missing_columns(conn, "patients", PATIENT_NEW_COLUMNS)
    for col_name, col_type in missing_patient:
        statements.append(
            f"ALTER TABLE patients ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
        )

    # Text table
    missing_text = get_missing_columns(conn, "mdr_text", TEXT_NEW_COLUMNS)
    for col_name, col_type in missing_text:
        statements.append(
            f"ALTER TABLE mdr_text ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
        )

    # Create download_state table if not exists
    statements.append("""
        CREATE TABLE IF NOT EXISTS download_state (
            filename VARCHAR PRIMARY KEY,
            file_type VARCHAR,
            url VARCHAR,
            size_bytes BIGINT,
            checksum VARCHAR,
            download_started TIMESTAMP,
            download_completed TIMESTAMP,
            status VARCHAR,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # New indexes
    new_indexes = [
        "CREATE INDEX IF NOT EXISTS idx_master_adverse_flag ON master_events(adverse_event_flag)",
        "CREATE INDEX IF NOT EXISTS idx_master_product_flag ON master_events(product_problem_flag)",
        "CREATE INDEX IF NOT EXISTS idx_master_report_number ON master_events(report_number)",
        "CREATE INDEX IF NOT EXISTS idx_master_date_added ON master_events(date_added)",
        "CREATE INDEX IF NOT EXISTS idx_devices_event_key ON devices(device_event_key)",
        "CREATE INDEX IF NOT EXISTS idx_devices_model ON devices(model_number)",
        "CREATE INDEX IF NOT EXISTS idx_patients_sex ON patients(patient_sex)",
        "CREATE INDEX IF NOT EXISTS idx_patients_age ON patients(patient_age_numeric)",
        "CREATE INDEX IF NOT EXISTS idx_text_key ON mdr_text(mdr_text_key)",
    ]
    statements.extend(new_indexes)

    return statements


def main():
    """Main entry point for migration."""
    parser = argparse.ArgumentParser(
        description="Migrate MAUDE database to schema v2.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--db",
        type=Path,
        default=config.database.path,
        help="Path to database file",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Create backup before migration",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(log_level=args.log_level)
    logger = get_logger("migrate")

    logger.info("=" * 60)
    logger.info("MAUDE Database Schema Migration - v2.0")
    logger.info("=" * 60)

    # Check if database exists
    if not args.db.exists():
        logger.error(f"Database not found: {args.db}")
        logger.error("Nothing to migrate. Run initial_load.py first.")
        return 1

    logger.info(f"Database: {args.db}")
    logger.info(f"Database size: {args.db.stat().st_size / 1024 / 1024:.1f} MB")

    try:
        with get_connection(args.db) as conn:
            # Check current schema version
            current_version = get_schema_version(conn)
            logger.info(f"Current schema version: {current_version or 'unknown'}")
            logger.info(f"Target schema version: {SCHEMA_VERSION}")

            if current_version == SCHEMA_VERSION:
                logger.info("Database is already at the target schema version.")
                return 0

            # Generate migration statements
            statements = generate_alter_statements(conn)

            if not statements:
                logger.info("No migration needed - schema is up to date.")
                return 0

            logger.info(f"\nMigration will execute {len(statements)} statements:")
            for i, stmt in enumerate(statements[:10], 1):
                # Truncate long statements for display
                display = stmt.strip()[:80]
                if len(stmt) > 80:
                    display += "..."
                logger.info(f"  {i}. {display}")
            if len(statements) > 10:
                logger.info(f"  ... and {len(statements) - 10} more")

            if args.dry_run:
                logger.info("\n[DRY RUN] No changes were made.")
                logger.info("\nFull migration SQL:")
                for stmt in statements:
                    print(stmt.strip())
                    print(";")
                return 0

            # Confirmation
            if not args.force:
                print("\nProceed with migration? [y/N] ", end="")
                response = input().strip().lower()
                if response != "y":
                    logger.info("Migration cancelled.")
                    return 0

        # Create backup if requested
        if args.backup:
            backup_path = args.db.with_suffix(
                f".backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}.duckdb"
            )
            logger.info(f"Creating backup: {backup_path}")
            shutil.copy(args.db, backup_path)
            logger.info("Backup created successfully.")

        # Execute migration
        with get_connection(args.db) as conn:
            logger.info("\nExecuting migration...")
            success_count = 0
            error_count = 0

            for stmt in statements:
                try:
                    conn.execute(stmt)
                    success_count += 1
                except Exception as e:
                    error_count += 1
                    # Some ALTER TABLE statements may fail if column exists
                    # This is expected and can be ignored
                    if "already exists" in str(e).lower():
                        logger.debug(f"Column already exists (OK): {e}")
                    else:
                        logger.warning(f"Statement failed: {e}")

            # Update schema version
            conn.execute(
                "INSERT OR REPLACE INTO app_settings (key, value) VALUES ('schema_version', ?)",
                [SCHEMA_VERSION]
            )

            logger.info(f"\nMigration complete:")
            logger.info(f"  Statements executed: {success_count}")
            logger.info(f"  Statements skipped: {error_count}")

            # Verify migration
            new_version = get_schema_version(conn)
            logger.info(f"  New schema version: {new_version}")

        logger.info("\n" + "=" * 60)
        logger.info("Migration completed successfully!")
        logger.info("=" * 60)
        return 0

    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
