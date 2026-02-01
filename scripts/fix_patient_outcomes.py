#!/usr/bin/env python3
"""
Migration script to populate patient outcome boolean fields.

The patients table has outcome boolean fields (outcome_death, outcome_hospitalization, etc.)
that are currently 0% populated. This script parses the raw outcome codes from
sequence_number_outcome or outcome_codes_raw and populates the boolean fields.

Usage:
    python scripts/fix_patient_outcomes.py [--dry-run] [--batch-size N]

Options:
    --dry-run       Show what would be done without making changes
    --batch-size N  Process N records at a time (default: 100000)
    --validate      Only run validation, don't update
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
import time

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger
from src.database import get_connection
from src.ingestion.outcome_parser import (
    analyze_outcome_distribution,
    get_outcome_coverage,
    detect_outcome_source_column,
    validate_outcome_parsing,
    generate_update_sql,
)

logger = get_logger("fix_patient_outcomes")


def print_section(title: str) -> None:
    """Print a section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def analyze_current_state(conn) -> dict:
    """Analyze the current state of patient outcome data."""
    print_section("Analyzing Current State")

    # Get total patient count
    total = conn.execute("SELECT COUNT(*) FROM patients").fetchone()[0]
    print(f"Total patient records: {total:,}")

    # Get current coverage
    print("\nCurrent outcome field coverage:")
    coverage = get_outcome_coverage(conn)
    for field, pct in coverage.items():
        status = "OK" if pct > 0 else "EMPTY"
        print(f"  {field}: {pct:.2f}% [{status}]")

    # Analyze raw outcome distribution
    print("\nAnalyzing raw outcome values...")
    source_col = detect_outcome_source_column(conn)
    if source_col:
        print(f"Source column detected: {source_col}")

        distribution = analyze_outcome_distribution(conn)
        print(f"\nTop 20 raw outcome values:")
        for i, (value, count) in enumerate(list(distribution.items())[:20]):
            print(f"  {i+1}. '{value}': {count:,}")
    else:
        print("WARNING: No source column with outcome data found!")

    return {
        "total": total,
        "coverage": coverage,
        "source_column": source_col,
    }


def validate_parsing(conn) -> dict:
    """Validate the outcome parsing logic."""
    print_section("Validating Parsing Logic")

    result = validate_outcome_parsing(conn, sample_size=5000)

    if "error" in result:
        print(f"ERROR: {result['error']}")
        return result

    print(f"Source column: {result['source_column']}")
    print(f"Total sampled: {result['total_sampled']:,}")
    print(f"Successfully parsed: {result['successfully_parsed']:,}")
    print(f"With errors: {result['with_errors']:,}")
    print(f"Parse success rate: {result['parse_success_rate']:.1f}%")

    print("\nCode distribution in sample:")
    for code, count in sorted(result['code_distribution'].items(), key=lambda x: -x[1]):
        if count > 0:
            print(f"  {code}: {count:,}")

    if result['sample_unrecognized']:
        print("\nSample unrecognized values:")
        for val in result['sample_unrecognized'][:5]:
            print(f"  '{val}'")

    return result


def run_migration(conn, source_column: str, batch_size: int, dry_run: bool) -> dict:
    """Run the migration to populate outcome fields."""
    print_section("Running Migration")

    if dry_run:
        print("DRY RUN MODE - No changes will be made")

    # Get count of records to update
    total = conn.execute(f"""
        SELECT COUNT(*) FROM patients
        WHERE {source_column} IS NOT NULL
    """).fetchone()[0]

    print(f"Records to process: {total:,}")
    print(f"Batch size: {batch_size:,}")

    if total == 0:
        print("No records to update!")
        return {"updated": 0}

    if dry_run:
        # Show sample of what would be updated
        print("\nSample of records that would be updated:")
        sample = conn.execute(f"""
            SELECT id, mdr_report_key, {source_column}
            FROM patients
            WHERE {source_column} IS NOT NULL
            LIMIT 5
        """).fetchdf()
        print(sample.to_string())
        return {"updated": 0, "would_update": total}

    # Generate and execute update SQL
    update_sql = generate_update_sql(source_column)
    print("\nUpdate SQL:")
    print(update_sql[:500] + "..." if len(update_sql) > 500 else update_sql)

    start_time = time.time()

    try:
        # Execute the update
        print("\nExecuting update...")
        conn.execute(update_sql)
        conn.execute("COMMIT")

        elapsed = time.time() - start_time
        print(f"\nUpdate completed in {elapsed:.1f} seconds")

        # Verify the update
        print("\nVerifying update...")
        new_coverage = get_outcome_coverage(conn)
        print("\nNew outcome field coverage:")
        for field, pct in new_coverage.items():
            print(f"  {field}: {pct:.2f}%")

        return {
            "updated": total,
            "elapsed_seconds": elapsed,
            "new_coverage": new_coverage,
        }

    except Exception as e:
        logger.error(f"Error during migration: {e}")
        print(f"\nERROR: {e}")
        print("Rolling back...")
        conn.execute("ROLLBACK")
        return {"error": str(e)}


def create_backup_table(conn) -> bool:
    """Create a backup of the patients table before migration."""
    print_section("Creating Backup")

    backup_name = f"patients_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    try:
        conn.execute(f"""
            CREATE TABLE {backup_name} AS
            SELECT * FROM patients
        """)
        print(f"Backup created: {backup_name}")

        count = conn.execute(f"SELECT COUNT(*) FROM {backup_name}").fetchone()[0]
        print(f"Backup contains {count:,} records")

        return True

    except Exception as e:
        logger.error(f"Error creating backup: {e}")
        print(f"ERROR: Could not create backup: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Populate patient outcome boolean fields from raw outcome codes"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100000,
        help="Process N records at a time (default: 100000)"
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Only run validation, don't update"
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip creating a backup table"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("  MAUDE Patient Outcomes Migration Script")
    print("=" * 60)
    print(f"\nDatabase: {config.database.path}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    with get_connection() as conn:
        # Analyze current state
        state = analyze_current_state(conn)

        if not state.get("source_column"):
            print("\nERROR: No source column found. Cannot proceed.")
            sys.exit(1)

        # Validate parsing
        validation = validate_parsing(conn)

        if "error" in validation:
            print("\nERROR: Validation failed. Cannot proceed.")
            sys.exit(1)

        if validation["parse_success_rate"] < 50:
            print("\nWARNING: Parse success rate is low!")
            if not args.dry_run:
                response = input("Continue anyway? (y/N): ")
                if response.lower() != 'y':
                    print("Aborted.")
                    sys.exit(0)

        if args.validate:
            print("\nValidation complete. Use --dry-run or remove --validate to proceed.")
            sys.exit(0)

        # Create backup unless skipped
        if not args.no_backup and not args.dry_run:
            if not create_backup_table(conn):
                print("\nERROR: Backup failed. Aborting.")
                sys.exit(1)

        # Run migration
        result = run_migration(
            conn,
            source_column=state["source_column"],
            batch_size=args.batch_size,
            dry_run=args.dry_run,
        )

        print_section("Summary")

        if args.dry_run:
            print(f"DRY RUN: Would update {result.get('would_update', 0):,} records")
        elif "error" in result:
            print(f"FAILED: {result['error']}")
            sys.exit(1)
        else:
            print(f"Successfully updated {result.get('updated', 0):,} records")
            print(f"Time elapsed: {result.get('elapsed_seconds', 0):.1f} seconds")

    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
