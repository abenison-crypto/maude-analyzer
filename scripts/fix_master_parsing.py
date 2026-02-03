#!/usr/bin/env python3
"""
Fix the master file parsing issue caused by unmatched quotes.

The FDA MAUDE data contains literal quote characters (e.g., O'REILLY becomes O"REILLY,
or manufacturer names like "ROCHE DIABETES CARE, INC. with unmatched quotes).

Python's csv.reader treats " as a quote character by default, which causes it to
consume millions of lines when an unmatched quote is encountered.

This script reloads the mdrfoiThru2025.txt file with quotechar disabled.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger
from src.database import get_connection, initialize_database

logger = get_logger("fix_master_parsing")


def count_lines_and_records(filepath: Path) -> tuple[int, int]:
    """
    Count physical lines and valid data records in file.

    Returns:
        Tuple of (total_lines, valid_data_lines)
    """
    total = 0
    valid = 0

    with open(filepath, 'r', encoding='latin-1', errors='replace') as f:
        for line in f:
            total += 1
            # Valid data lines start with a digit (MDR_REPORT_KEY is numeric)
            if line and line[0].isdigit():
                valid += 1

    return total, valid


def parse_master_file_fixed(filepath: Path, batch_size: int = 10000):
    """
    Parse master file without quote character to avoid swallowing records.

    Uses simple line-by-line parsing with pipe delimiter instead of csv.reader.

    Yields:
        Record dictionaries ready for database insertion.
    """
    # Get column names from header
    with open(filepath, 'r', encoding='latin-1', errors='replace') as f:
        header_line = f.readline().strip().replace('\r', '')
        columns = header_line.split('|')
        # Normalize column names to lowercase for database
        columns = [c.strip().lower().replace(' ', '_') for c in columns]

    logger.info(f"Header has {len(columns)} columns")

    # Column name mapping from FDA to our schema
    column_map = {
        'manufacturer_link_flag_': 'manufacturer_link_flag_old',
        'manufacturer_g1_state_code': 'manufacturer_g1_state',
        'manufacturer_g1_zip_code': 'manufacturer_g1_zip',
        'manufacturer_g1_zip_code_ext': 'manufacturer_g1_zip_ext',
        'manufacturer_g1_country_code': 'manufacturer_g1_country',
        'manufacturer_g1_postal_code': 'manufacturer_g1_postal',
        'manufacturer_state_code': 'manufacturer_state',
        'manufacturer_zip_code': 'manufacturer_zip',
        'manufacturer_zip_code_ext': 'manufacturer_zip_ext',
        'manufacturer_country_code': 'manufacturer_country',
        'manufacturer_postal_code': 'manufacturer_postal',
        'distributor_state_code': 'distributor_state',
        'distributor_zip_code': 'distributor_zip',
        'distributor_zip_code_ext': 'distributor_zip_ext',
        'manufacturer_contact_t_name': 'manufacturer_contact_title',
        'manufacturer_contact_f_name': 'manufacturer_contact_first_name',
        'manufacturer_contact_l_name': 'manufacturer_contact_last_name',
        'manufacturer_contact_street_1': 'manufacturer_contact_address_1',
        'manufacturer_contact_street_2': 'manufacturer_contact_address_2',
        'manufacturer_contact_zip_code': 'manufacturer_contact_zip',
        'pma_pmn_num': 'pma_pmn_number',
        'summary_report': 'summary_report_flag',
        'suppl_dates_fda_received': 'supplemental_dates_fda_received',
        'suppl_dates_mfr_received': 'supplemental_dates_mfr_received',
    }

    # Apply column mapping
    columns = [column_map.get(c, c) for c in columns]

    # Read data lines
    processed = 0
    skipped = 0

    with open(filepath, 'r', encoding='latin-1', errors='replace') as f:
        # Skip header
        f.readline()

        for line_num, line in enumerate(f, 2):
            line = line.strip().replace('\r', '')
            if not line:
                continue

            fields = line.split('|')

            # Validate MDR_REPORT_KEY is numeric
            mdr_key = fields[0] if fields else ''
            if not mdr_key or not mdr_key.isdigit():
                skipped += 1
                continue

            # Build record dict
            record = {}
            for i, col in enumerate(columns):
                if i < len(fields):
                    val = fields[i].strip()
                    record[col] = val if val else None
                else:
                    record[col] = None

            # Add source file tracking
            record['source_file'] = filepath.name

            # Add derived fields (received_year, received_month)
            if record.get('date_received'):
                try:
                    date_str = record['date_received']
                    if '/' in date_str:
                        parts = date_str.split('/')
                        if len(parts) == 3:
                            month, day, year = parts
                            if len(year) == 2:
                                year = '20' + year if int(year) < 50 else '19' + year
                            record['received_year'] = int(year)
                            record['received_month'] = int(month)
                except:
                    pass

            processed += 1
            yield record

            if processed % 1000000 == 0:
                logger.info(f"Processed {processed:,} records ({skipped:,} skipped)...")

    logger.info(f"Final: {processed:,} records parsed, {skipped:,} skipped")


def reload_master_file(filepath: Path, db_path: Path = None):
    """
    Reload master file with fixed parsing.

    This will:
    1. Delete existing records from this source file
    2. Re-parse with fixed parser (no quotechar)
    3. Insert all records
    """
    if db_path is None:
        db_path = config.database.path

    start_time = datetime.now()

    logger.info(f"=== Reloading {filepath.name} with fixed parsing ===")

    # Count expected records
    logger.info("Counting records in source file...")
    total_lines, valid_lines = count_lines_and_records(filepath)
    logger.info(f"Source file: {total_lines:,} lines, {valid_lines:,} valid data lines")

    # Connect to database
    with get_connection(db_path) as conn:
        # Check current record count from this file
        before_count = conn.execute(
            "SELECT COUNT(*) FROM master_events WHERE source_file = ?",
            [filepath.name]
        ).fetchone()[0]
        logger.info(f"Current records from {filepath.name}: {before_count:,}")

        # Delete existing records from this file
        logger.info(f"Deleting existing records from {filepath.name}...")
        conn.execute(
            "DELETE FROM master_events WHERE source_file = ?",
            [filepath.name]
        )
        logger.info("Deleted.")

        # Insert in batches
        batch = []
        batch_size = 10000
        total_inserted = 0

        # Define columns for insert (subset that we actually use)
        insert_columns = [
            'mdr_report_key', 'event_key', 'report_number', 'report_source_code',
            'manufacturer_link_flag_old', 'number_devices_in_event', 'number_patients_in_event',
            'date_received', 'adverse_event_flag', 'product_problem_flag',
            'date_report', 'date_of_event', 'reprocessed_and_reused_flag',
            'reporter_occupation_code', 'health_professional', 'initial_report_to_fda',
            'date_facility_aware', 'report_date', 'report_to_fda', 'date_report_to_fda',
            'event_location', 'date_report_to_manufacturer',
            'manufacturer_contact_title', 'manufacturer_contact_first_name',
            'manufacturer_contact_last_name', 'manufacturer_contact_address_1',
            'manufacturer_contact_address_2', 'manufacturer_contact_city',
            'manufacturer_contact_state', 'manufacturer_contact_zip',
            'manufacturer_contact_zip_ext', 'manufacturer_contact_country',
            'manufacturer_contact_postal',
            'manufacturer_contact_area_code', 'manufacturer_contact_exchange',
            'manufacturer_contact_phone_no', 'manufacturer_contact_extension',
            'manufacturer_contact_pcountry', 'manufacturer_contact_pcity',
            'manufacturer_contact_plocal',
            'manufacturer_g1_name', 'manufacturer_g1_street_1', 'manufacturer_g1_street_2',
            'manufacturer_g1_city', 'manufacturer_g1_state', 'manufacturer_g1_zip',
            'manufacturer_g1_zip_ext', 'manufacturer_g1_country', 'manufacturer_g1_postal',
            'date_manufacturer_received', 'mfr_report_type', 'device_date_of_manufacture',
            'single_use_flag', 'remedial_action', 'previous_use_code',
            'removal_correction_number', 'event_type',
            'distributor_name', 'distributor_address_1', 'distributor_address_2',
            'distributor_city', 'distributor_state', 'distributor_zip', 'distributor_zip_ext',
            'report_to_manufacturer',
            'manufacturer_name', 'manufacturer_address_1', 'manufacturer_address_2',
            'manufacturer_city', 'manufacturer_state', 'manufacturer_zip',
            'manufacturer_zip_ext', 'manufacturer_country', 'manufacturer_postal',
            'type_of_report', 'source_type', 'date_added', 'date_changed',
            'reporter_state_code', 'reporter_country_code',
            'pma_pmn_number', 'exemption_number', 'summary_report_flag',
            'noe_summarized', 'supplemental_dates_fda_received', 'supplemental_dates_mfr_received',
            'received_year', 'received_month', 'source_file',
        ]

        import pandas as pd

        for record in parse_master_file_fixed(filepath):
            # Build row with only insert columns
            row = {col: record.get(col) for col in insert_columns}
            batch.append(row)

            if len(batch) >= batch_size:
                df = pd.DataFrame(batch, columns=insert_columns)
                col_names = ", ".join(insert_columns)
                select_cols = ", ".join([f'"{c}"' for c in insert_columns])
                conn.execute(f"INSERT OR REPLACE INTO master_events ({col_names}) SELECT {select_cols} FROM df")
                total_inserted += len(batch)
                batch = []

                if total_inserted % 100000 == 0:
                    logger.info(f"Inserted {total_inserted:,} records...")

        # Insert remaining
        if batch:
            df = pd.DataFrame(batch, columns=insert_columns)
            col_names = ", ".join(insert_columns)
            select_cols = ", ".join([f'"{c}"' for c in insert_columns])
            conn.execute(f"INSERT OR REPLACE INTO master_events ({col_names}) SELECT {select_cols} FROM df")
            total_inserted += len(batch)

        # Verify
        after_count = conn.execute(
            "SELECT COUNT(*) FROM master_events WHERE source_file = ?",
            [filepath.name]
        ).fetchone()[0]

        # Check year distribution
        logger.info("\n=== Records by received_year ===")
        results = conn.execute("""
            SELECT received_year, COUNT(*) as cnt
            FROM master_events
            WHERE source_file = ?
            GROUP BY received_year
            ORDER BY received_year
        """, [filepath.name]).fetchall()

        for year, cnt in results:
            print(f"  {year}: {cnt:,}")

    duration = (datetime.now() - start_time).total_seconds()

    logger.info(f"\n=== Summary ===")
    logger.info(f"Source file valid lines: {valid_lines:,}")
    logger.info(f"Records before reload: {before_count:,}")
    logger.info(f"Records inserted: {total_inserted:,}")
    logger.info(f"Records after reload: {after_count:,}")
    logger.info(f"Duration: {duration:.1f} seconds")

    # Calculate recovery
    if before_count > 0:
        recovery_pct = (after_count - before_count) / before_count * 100
        logger.info(f"Recovery: {after_count - before_count:,} records ({recovery_pct:.1f}%)")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Fix master file parsing")
    parser.add_argument(
        "--file",
        type=Path,
        default=Path("data/raw/mdrfoiThru2025.txt"),
        help="Master file to reload"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=config.database.path,
        help="Database path"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Just count records, don't reload"
    )

    args = parser.parse_args()

    if not args.file.exists():
        logger.error(f"File not found: {args.file}")
        sys.exit(1)

    if args.dry_run:
        logger.info("DRY RUN: Counting records only")
        total_lines, valid_lines = count_lines_and_records(args.file)
        logger.info(f"Source file: {total_lines:,} lines, {valid_lines:,} valid data lines")

        # Test parsing
        count = 0
        for record in parse_master_file_fixed(args.file):
            count += 1
            if count >= 100:
                break
        logger.info(f"Sample parsed OK: {count} records")
    else:
        reload_master_file(args.file, args.db)


if __name__ == "__main__":
    main()
