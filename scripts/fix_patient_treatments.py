#!/usr/bin/env python3
"""Migration script to parse existing patient treatment codes into boolean flags.

This script backfills the treatment boolean columns for existing patient records
that have treatment_codes_raw populated but no boolean flags set.

Usage:
    python scripts/fix_patient_treatments.py [options]

Options:
    --dry-run       Show what would be changed without making changes
    --batch-size    Number of records to process per batch (default: 10000)
    --limit         Maximum number of records to process (default: all)
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config, TREATMENT_CODES
from config.logging_config import get_logger
from src.database import get_connection

logger = get_logger("fix_patient_treatments")


def parse_treatment_codes(codes_str: str) -> dict:
    """
    Parse treatment codes string into boolean flags.

    Args:
        codes_str: Semicolon-separated treatment codes (e.g., "1;3;8").

    Returns:
        Dictionary mapping treatment field names to boolean values.
    """
    treatments = {
        "treatment_drug": False,
        "treatment_device": False,
        "treatment_surgery": False,
        "treatment_other": False,
        "treatment_unknown": False,
        "treatment_no_information": False,
        "treatment_blood_products": False,
        "treatment_hospitalization": False,
        "treatment_physical_therapy": False,
    }

    if not codes_str:
        return treatments

    # Map code to field name
    code_to_field = {
        "1": "treatment_drug",
        "2": "treatment_device",
        "3": "treatment_surgery",
        "4": "treatment_other",
        "5": "treatment_unknown",
        "6": "treatment_no_information",
        "7": "treatment_blood_products",
        "8": "treatment_hospitalization",
        "9": "treatment_physical_therapy",
    }

    codes = str(codes_str).split(";")
    for code in codes:
        code = code.strip()
        if code in code_to_field:
            treatments[code_to_field[code]] = True

    return treatments


def get_records_to_fix(conn, limit: int = None) -> int:
    """
    Count records that need treatment codes parsed.

    Args:
        conn: Database connection.
        limit: Maximum records to count.

    Returns:
        Count of records needing update.
    """
    query = """
        SELECT COUNT(*) FROM patients
        WHERE treatment_codes_raw IS NOT NULL
          AND treatment_codes_raw != ''
          AND treatment_drug = FALSE
          AND treatment_device = FALSE
          AND treatment_surgery = FALSE
          AND treatment_other = FALSE
          AND treatment_unknown = FALSE
          AND treatment_no_information = FALSE
          AND treatment_blood_products = FALSE
          AND treatment_hospitalization = FALSE
          AND treatment_physical_therapy = FALSE
    """

    result = conn.execute(query).fetchone()
    return result[0] if result else 0


def fix_treatment_codes(
    db_path: Path,
    batch_size: int = 10000,
    limit: int = None,
    dry_run: bool = False,
) -> dict:
    """
    Parse treatment codes for existing patient records.

    Args:
        db_path: Path to database.
        batch_size: Number of records per batch.
        limit: Maximum records to process.
        dry_run: If True, don't make changes.

    Returns:
        Dictionary with processing statistics.
    """
    stats = {
        "records_found": 0,
        "records_updated": 0,
        "records_skipped": 0,
        "records_errors": 0,
        "dry_run": dry_run,
    }

    with get_connection(db_path) as conn:
        # First check if columns exist
        try:
            conn.execute("SELECT treatment_drug FROM patients LIMIT 1")
        except Exception as e:
            logger.error(f"Treatment columns don't exist. Run schema migration first: {e}")
            return stats

        # Count records to fix
        stats["records_found"] = get_records_to_fix(conn, limit)
        logger.info(f"Found {stats['records_found']:,} records to fix")

        if stats["records_found"] == 0:
            logger.info("No records need treatment code parsing")
            return stats

        if dry_run:
            logger.info("[DRY RUN] Would update these records - no changes made")

            # Show sample of what would be updated
            sample = conn.execute("""
                SELECT id, treatment_codes_raw
                FROM patients
                WHERE treatment_codes_raw IS NOT NULL
                  AND treatment_codes_raw != ''
                  AND treatment_drug = FALSE
                LIMIT 10
            """).fetchall()

            logger.info("Sample records that would be updated:")
            for row in sample:
                patient_id, codes_raw = row
                parsed = parse_treatment_codes(codes_raw)
                active = [k for k, v in parsed.items() if v]
                logger.info(f"  ID {patient_id}: '{codes_raw}' -> {active}")

            return stats

        # Process in batches
        offset = 0
        processed = 0
        max_records = limit or float('inf')

        while processed < max_records:
            # Fetch batch of records to update
            query = f"""
                SELECT id, treatment_codes_raw
                FROM patients
                WHERE treatment_codes_raw IS NOT NULL
                  AND treatment_codes_raw != ''
                  AND treatment_drug = FALSE
                  AND treatment_device = FALSE
                  AND treatment_surgery = FALSE
                  AND treatment_other = FALSE
                  AND treatment_unknown = FALSE
                  AND treatment_no_information = FALSE
                  AND treatment_blood_products = FALSE
                  AND treatment_hospitalization = FALSE
                  AND treatment_physical_therapy = FALSE
                LIMIT {batch_size}
            """

            batch = conn.execute(query).fetchall()

            if not batch:
                break

            # Update each record
            for row in batch:
                patient_id, codes_raw = row

                try:
                    parsed = parse_treatment_codes(codes_raw)

                    conn.execute("""
                        UPDATE patients SET
                            treatment_drug = ?,
                            treatment_device = ?,
                            treatment_surgery = ?,
                            treatment_other = ?,
                            treatment_unknown = ?,
                            treatment_no_information = ?,
                            treatment_blood_products = ?,
                            treatment_hospitalization = ?,
                            treatment_physical_therapy = ?
                        WHERE id = ?
                    """, [
                        parsed["treatment_drug"],
                        parsed["treatment_device"],
                        parsed["treatment_surgery"],
                        parsed["treatment_other"],
                        parsed["treatment_unknown"],
                        parsed["treatment_no_information"],
                        parsed["treatment_blood_products"],
                        parsed["treatment_hospitalization"],
                        parsed["treatment_physical_therapy"],
                        patient_id,
                    ])

                    stats["records_updated"] += 1

                except Exception as e:
                    stats["records_errors"] += 1
                    if stats["records_errors"] <= 10:
                        logger.warning(f"Error updating patient {patient_id}: {e}")

                processed += 1
                if processed >= max_records:
                    break

            logger.info(f"Processed {processed:,} records ({stats['records_updated']:,} updated)")

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Parse existing patient treatment codes into boolean flags",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be changed without making changes",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Number of records to process per batch",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of records to process",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=config.database.path,
        help="Database path",
    )

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Patient Treatment Code Migration")
    logger.info("=" * 60)
    logger.info(f"Database: {args.db}")
    logger.info(f"Batch size: {args.batch_size}")
    logger.info(f"Dry run: {args.dry_run}")

    start_time = datetime.now()

    stats = fix_treatment_codes(
        db_path=args.db,
        batch_size=args.batch_size,
        limit=args.limit,
        dry_run=args.dry_run,
    )

    duration = datetime.now() - start_time

    logger.info("\n" + "=" * 60)
    logger.info("MIGRATION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Records found: {stats['records_found']:,}")
    logger.info(f"Records updated: {stats['records_updated']:,}")
    logger.info(f"Records errors: {stats['records_errors']:,}")
    logger.info(f"Duration: {duration}")

    if args.dry_run:
        logger.info("\n[DRY RUN] No changes were made. Run without --dry-run to apply changes.")

    return 0 if stats["records_errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
