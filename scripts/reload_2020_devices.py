#!/usr/bin/env python3
"""
Reload 2020+ Device Files with Corrected Schema.

This script reloads device files from 2020 onwards using the new 34-column
schema that includes DATE_RECEIVED and other fields that were previously
not being captured.

The 2020+ device files have a different schema than pre-2020 files:
- 34 columns (vs 28 for pre-2020)
- New columns: IMPLANT_DATE_YEAR, DATE_REMOVED_YEAR, SERVICED_BY_3RD_PARTY_FLAG,
               COMBINATION_PRODUCT_FLAG, UDI-DI, UDI-PUBLIC

Usage:
    python scripts/reload_2020_devices.py
    python scripts/reload_2020_devices.py --file device2024.txt
    python scripts/reload_2020_devices.py --dry-run
"""

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path
import time

# Setup path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import duckdb


def parse_date(date_str: str) -> datetime.date:
    """Parse date in YYYY/MM/DD format."""
    if not date_str or date_str.strip() == '':
        return None
    try:
        return datetime.strptime(date_str.strip(), '%Y/%m/%d').date()
    except:
        return None


def parse_int(val: str) -> int:
    """Parse integer."""
    if not val or val.strip() == '':
        return None
    try:
        return int(val.strip())
    except:
        return None


MAX_FIELD_LENGTH = 10000  # Truncate fields longer than this to avoid ART key size errors


def safe_str(val: str, max_len: int = MAX_FIELD_LENGTH) -> str:
    """Safely truncate string fields to avoid index key size errors."""
    if not val:
        return None
    if len(val) > max_len:
        return val[:max_len]
    return val


def load_device_file(conn, filepath: Path, start_row: int = 0) -> int:
    """
    Load a 2020+ device file into the database.

    Args:
        conn: DuckDB connection
        filepath: Path to device file

    Returns:
        Number of records loaded
    """
    filename = filepath.name
    print(f"Loading {filename}...")

    # Increase CSV field size limit
    csv.field_size_limit(sys.maxsize)

    records = []
    batch_size = 10000
    total_loaded = 0
    start = time.time()

    with open(filepath, 'r', encoding='latin-1') as f:
        reader = csv.reader(f, delimiter='|')
        header = [h.upper().strip() for h in next(reader)]

        # Verify this is a 34-column file
        if len(header) != 34:
            print(f"  WARNING: Expected 34 columns, got {len(header)}. Skipping.")
            return 0

        for row_num, row in enumerate(reader):
            if row_num < start_row:
                continue  # Skip already-loaded rows
            if len(row) < 34:
                continue

            record = (
                safe_str(row[0]) if len(row) > 0 else None,  # mdr_report_key
                safe_str(row[1]) if len(row) > 1 else None,  # device_event_key
                safe_str(row[2]) if len(row) > 2 else None,  # implant_flag
                safe_str(row[3]) if len(row) > 3 else None,  # date_removed_flag
                parse_int(row[4]) if len(row) > 4 else None,  # device_sequence_number
                safe_str(row[5]) if len(row) > 5 else None,  # implant_date_year
                safe_str(row[6]) if len(row) > 6 else None,  # date_removed_year
                safe_str(row[7]) if len(row) > 7 else None,  # serviced_by_3rd_party_flag
                parse_date(row[8]) if len(row) > 8 else None,  # date_received
                safe_str(row[9]) if len(row) > 9 else None,  # brand_name
                safe_str(row[10]) if len(row) > 10 else None,  # generic_name
                safe_str(row[11]) if len(row) > 11 else None,  # manufacturer_d_name
                safe_str(row[12]) if len(row) > 12 else None,  # manufacturer_d_address_1
                safe_str(row[13]) if len(row) > 13 else None,  # manufacturer_d_address_2
                safe_str(row[14]) if len(row) > 14 else None,  # manufacturer_d_city
                safe_str(row[15]) if len(row) > 15 else None,  # manufacturer_d_state
                safe_str(row[16]) if len(row) > 16 else None,  # manufacturer_d_zip
                safe_str(row[17]) if len(row) > 17 else None,  # manufacturer_d_zip_ext
                safe_str(row[18]) if len(row) > 18 else None,  # manufacturer_d_country
                safe_str(row[19]) if len(row) > 19 else None,  # manufacturer_d_postal
                safe_str(row[20]) if len(row) > 20 else None,  # device_operator
                parse_date(row[21]) if len(row) > 21 else None,  # expiration_date_of_device
                safe_str(row[22]) if len(row) > 22 else None,  # model_number
                safe_str(row[23]) if len(row) > 23 else None,  # catalog_number
                safe_str(row[24]) if len(row) > 24 else None,  # lot_number
                safe_str(row[25]) if len(row) > 25 else None,  # other_id_number
                safe_str(row[26]) if len(row) > 26 else None,  # device_availability
                parse_date(row[27]) if len(row) > 27 else None,  # date_returned_to_manufacturer
                safe_str(row[28]) if len(row) > 28 else None,  # device_report_product_code
                safe_str(row[29]) if len(row) > 29 else None,  # device_age_text
                safe_str(row[30]) if len(row) > 30 else None,  # device_evaluated_by_manufacturer
                safe_str(row[31]) if len(row) > 31 else None,  # combination_product_flag
                safe_str(row[32]) if len(row) > 32 else None,  # udi_di
                safe_str(row[33]) if len(row) > 33 else None,  # udi_public
                filename,  # source_file
            )
            records.append(record)

            if len(records) >= batch_size:
                conn.executemany('''
                    INSERT INTO devices (
                        mdr_report_key, device_event_key, implant_flag, date_removed_flag,
                        device_sequence_number, implant_date_year, date_removed_year,
                        serviced_by_3rd_party_flag, date_received, brand_name, generic_name,
                        manufacturer_d_name, manufacturer_d_address_1, manufacturer_d_address_2,
                        manufacturer_d_city, manufacturer_d_state, manufacturer_d_zip,
                        manufacturer_d_zip_ext, manufacturer_d_country, manufacturer_d_postal,
                        device_operator, expiration_date_of_device, model_number, catalog_number,
                        lot_number, other_id_number, device_availability, date_returned_to_manufacturer,
                        device_report_product_code, device_age_text, device_evaluated_by_manufacturer,
                        combination_product_flag, udi_di, udi_public, source_file
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', records)
                total_loaded += len(records)
                records = []
                print(f"  Loaded {total_loaded:,} records...", end='\r')

    # Insert remaining records
    if records:
        conn.executemany('''
            INSERT INTO devices (
                mdr_report_key, device_event_key, implant_flag, date_removed_flag,
                device_sequence_number, implant_date_year, date_removed_year,
                serviced_by_3rd_party_flag, date_received, brand_name, generic_name,
                manufacturer_d_name, manufacturer_d_address_1, manufacturer_d_address_2,
                manufacturer_d_city, manufacturer_d_state, manufacturer_d_zip,
                manufacturer_d_zip_ext, manufacturer_d_country, manufacturer_d_postal,
                device_operator, expiration_date_of_device, model_number, catalog_number,
                lot_number, other_id_number, device_availability, date_returned_to_manufacturer,
                device_report_product_code, device_age_text, device_evaluated_by_manufacturer,
                combination_product_flag, udi_di, udi_public, source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', records)
        total_loaded += len(records)

    elapsed = time.time() - start
    rate = total_loaded / elapsed if elapsed > 0 else 0
    print(f"  Loaded {total_loaded:,} records in {elapsed:.1f}s ({rate:,.0f} rec/s)")

    return total_loaded


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', type=Path, default=PROJECT_ROOT / 'data' / 'maude.duckdb',
                       help='Path to database')
    parser.add_argument('--data-dir', type=Path, default=PROJECT_ROOT / 'data' / 'raw',
                       help='Path to raw data directory')
    parser.add_argument('--file', type=str, help='Specific file to reload (e.g., device2024.txt)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done')
    parser.add_argument('--skip-delete', action='store_true', help='Skip deletion of existing records')
    parser.add_argument('--start-row', type=int, default=0, help='Row to start from (for resuming)')

    args = parser.parse_args()

    # Files to process
    if args.file:
        files = [args.file]
    else:
        files = ['device2020.txt', 'device2021.txt', 'device2022.txt',
                 'device2023.txt', 'device2024.txt', 'device2025.txt']

    print("="*60)
    print("RELOAD 2020+ DEVICE FILES")
    print("="*60)
    print(f"Database: {args.db}")
    print(f"Data directory: {args.data_dir}")
    print(f"Files to process: {files}")

    if args.dry_run:
        print("\nDRY RUN - no changes will be made")
        return 0

    conn = duckdb.connect(str(args.db))

    # Delete existing records
    if not args.skip_delete:
        print("\nDeleting existing records...")
        for f in files:
            pattern = f.replace('.txt', '')
            conn.execute(f"DELETE FROM devices WHERE source_file LIKE '{pattern}%'")
            print(f"  Deleted records from {f}")
        conn.execute("CHECKPOINT")

    # Load files
    print("\nLoading files...")
    total = 0
    for filename in files:
        filepath = args.data_dir / filename
        if not filepath.exists():
            print(f"  SKIP: {filename} not found")
            continue

        loaded = load_device_file(conn, filepath, args.start_row)
        total += loaded
        args.start_row = 0  # Only skip rows for first file

    conn.execute("CHECKPOINT")

    # Verify
    print("\n" + "="*60)
    print("VERIFICATION")
    print("="*60)
    for f in files:
        stats = conn.execute(f'''
            SELECT COUNT(*),
                   SUM(CASE WHEN date_received IS NULL THEN 1 ELSE 0 END),
                   MIN(date_received), MAX(date_received)
            FROM devices WHERE source_file = '{f}'
        ''').fetchone()
        if stats[0] > 0:
            null_pct = stats[1] * 100 / stats[0]
            print(f"  {f}: {stats[0]:,} records, {null_pct:.1f}% null dates, range: {stats[2]} to {stats[3]}")

    conn.close()

    print(f"\n{'='*60}")
    print(f"COMPLETE: {total:,} total records loaded")
    print("="*60)

    return 0


if __name__ == '__main__':
    sys.exit(main())
