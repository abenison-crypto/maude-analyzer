#!/usr/bin/env python
"""
Populate FDA MAUDE lookup tables from FDA data files and derived data.

This script populates the following lookup tables:
1. problem_codes - Device problem code descriptions (from deviceproblemcodes.txt)
2. patient_problem_codes - Patient problem code descriptions (from patientproblemdata.txt)
3. product_codes - FDA product code definitions (from FDA device classification database)
4. manufacturers - Manufacturer name mappings (derived from loaded data)

Usage:
    python scripts/populate_lookup_tables.py [options]

Options:
    --data-dir PATH     Directory containing FDA files
    --db PATH           Path to DuckDB database
    --rebuild           Drop and rebuild all lookup tables
    --table TABLE       Only populate specific table
"""

import argparse
import sys
import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config, MANUFACTURER_MAPPINGS
from config.logging_config import setup_logging, get_logger
from src.database import get_connection, initialize_database


def populate_problem_codes(
    conn,
    data_dir: Path,
    logger,
) -> int:
    """
    Populate problem_codes table from deviceproblemcodes.txt or .csv.

    Args:
        conn: DuckDB connection
        data_dir: Directory containing FDA files
        logger: Logger instance

    Returns:
        Number of records loaded
    """
    # Look for the file (try .csv first, then .txt)
    problem_codes_file = data_dir / "deviceproblemcodes.csv"
    delimiter = ","

    if not problem_codes_file.exists():
        problem_codes_file = data_dir / "deviceproblemcodes.txt"
        delimiter = "|"

    if not problem_codes_file.exists():
        logger.warning(f"Problem codes file not found: {problem_codes_file}")
        return 0

    logger.info(f"Loading problem codes from {problem_codes_file}")

    # Clear existing data
    conn.execute("DELETE FROM problem_codes")

    count = 0
    errors = 0

    try:
        with open(problem_codes_file, "r", encoding="utf-8-sig", errors="replace") as f:
            reader = csv.reader(f, delimiter=delimiter)

            # Skip header row (FDA_CODE,TERM,...)
            first_row = next(reader, None)
            if first_row:
                # Check if it's a header by looking for non-numeric first field
                first_val = first_row[0].strip().lstrip('\ufeff')
                if first_val.upper() in ('FDA_CODE', 'CODE', 'PROBLEM_CODE') or not first_val.replace('-', '').isdigit():
                    # Header row, skip it
                    logger.debug(f"Skipping header: {first_row}")
                else:
                    # First row is data, process it
                    if len(first_row) >= 2:
                        try:
                            code = first_val
                            desc = first_row[1].strip() if len(first_row) > 1 else None
                            conn.execute(
                                "INSERT INTO problem_codes (problem_code, description) VALUES (?, ?)",
                                [code, desc]
                            )
                            count += 1
                        except Exception as e:
                            errors += 1

            # Process remaining rows
            for row in reader:
                if len(row) >= 1:
                    try:
                        code = row[0].strip()
                        desc = row[1].strip() if len(row) > 1 else None

                        if code and code.replace('-', '').isdigit():
                            conn.execute(
                                "INSERT OR REPLACE INTO problem_codes (problem_code, description) VALUES (?, ?)",
                                [code, desc]
                            )
                            count += 1
                    except Exception as e:
                        errors += 1
                        if errors < 10:
                            logger.debug(f"Error loading problem code: {e}")

        logger.info(f"Loaded {count} problem codes ({errors} errors)")

    except Exception as e:
        logger.error(f"Error reading problem codes file: {e}")

    return count


def populate_patient_problem_codes(
    conn,
    data_dir: Path,
    logger,
) -> int:
    """
    Populate patient_problem_codes table from patientproblemcode.csv or patientproblemdata.txt.

    Args:
        conn: DuckDB connection
        data_dir: Directory containing FDA files
        logger: Logger instance

    Returns:
        Number of records loaded
    """
    # Look for the file (try .csv first, then .txt variants)
    patient_problem_file = data_dir / "patientproblemcode.csv"
    delimiter = ","

    if not patient_problem_file.exists():
        patient_problem_file = data_dir / "patientproblemdata.csv"

    if not patient_problem_file.exists():
        patient_problem_file = data_dir / "patientproblemdata.txt"
        delimiter = "|"

    if not patient_problem_file.exists():
        logger.warning(f"Patient problem data file not found: {patient_problem_file}")
        return 0

    logger.info(f"Loading patient problem codes from {patient_problem_file}")

    # Clear existing data
    conn.execute("DELETE FROM patient_problem_codes")

    count = 0
    errors = 0

    try:
        with open(patient_problem_file, "r", encoding="utf-8-sig", errors="replace") as f:
            reader = csv.reader(f, delimiter=delimiter)

            # Skip header if present
            first_row = next(reader, None)
            if first_row:
                first_val = first_row[0].strip().upper().lstrip('\ufeff') if first_row[0] else ""
                if "CODE" in first_val or "PROBLEM" in first_val or "FDA" in first_val:
                    # Header row, skip
                    logger.debug(f"Skipping header: {first_row}")
                else:
                    # Data row, process
                    if len(first_row) >= 1:
                        try:
                            code = first_row[0].strip()
                            desc = first_row[1].strip() if len(first_row) > 1 else None
                            if code and code.replace('-', '').isdigit():
                                conn.execute(
                                    "INSERT INTO patient_problem_codes (problem_code, description) VALUES (?, ?)",
                                    [code, desc]
                                )
                                count += 1
                        except Exception:
                            errors += 1

            # Process remaining rows
            for row in reader:
                if len(row) >= 1:
                    try:
                        code = row[0].strip()
                        desc = row[1].strip() if len(row) > 1 else None

                        if code and code.replace('-', '').isdigit():
                            conn.execute(
                                "INSERT OR REPLACE INTO patient_problem_codes (problem_code, description) VALUES (?, ?)",
                                [code, desc]
                            )
                            count += 1
                    except Exception as e:
                        errors += 1
                        if errors < 10:
                            logger.debug(f"Error loading patient problem code: {e}")

        logger.info(f"Loaded {count} patient problem codes ({errors} errors)")

    except Exception as e:
        logger.error(f"Error reading patient problem data file: {e}")

    return count


def populate_product_codes_from_data(
    conn,
    logger,
) -> int:
    """
    Populate product_codes table by extracting unique codes from loaded data.

    This is a fallback when the FDA product classification file isn't available.

    Args:
        conn: DuckDB connection
        logger: Logger instance

    Returns:
        Number of records loaded
    """
    logger.info("Extracting unique product codes from loaded data...")

    # Get unique product codes from devices table
    try:
        result = conn.execute("""
            SELECT DISTINCT device_report_product_code
            FROM devices
            WHERE device_report_product_code IS NOT NULL
              AND device_report_product_code != ''
        """).fetchall()

        codes = [row[0] for row in result]
        logger.info(f"Found {len(codes)} unique product codes in devices table")

        # Insert into product_codes (if not already present)
        count = 0
        for code in codes:
            try:
                conn.execute("""
                    INSERT INTO product_codes (product_code, device_name)
                    SELECT ?, 'Unknown - derived from data'
                    WHERE NOT EXISTS (
                        SELECT 1 FROM product_codes WHERE product_code = ?
                    )
                """, [code, code])
                count += 1
            except:
                pass

        # Also get from master_events if available
        try:
            master_result = conn.execute("""
                SELECT DISTINCT product_code
                FROM master_events
                WHERE product_code IS NOT NULL
                  AND product_code != ''
            """).fetchall()

            for row in master_result:
                code = row[0]
                try:
                    conn.execute("""
                        INSERT INTO product_codes (product_code, device_name)
                        SELECT ?, 'Unknown - derived from data'
                        WHERE NOT EXISTS (
                            SELECT 1 FROM product_codes WHERE product_code = ?
                        )
                    """, [code, code])
                    count += 1
                except:
                    pass
        except:
            pass

        logger.info(f"Added {count} product codes from data")
        return count

    except Exception as e:
        logger.error(f"Error extracting product codes: {e}")
        return 0


def populate_manufacturers_from_data(
    conn,
    logger,
) -> int:
    """
    Populate manufacturers table by extracting from loaded data and applying mappings.

    Args:
        conn: DuckDB connection
        logger: Logger instance

    Returns:
        Number of records loaded
    """
    logger.info("Extracting and mapping manufacturer names from loaded data...")

    # Clear existing
    conn.execute("DELETE FROM manufacturers")

    try:
        # Get unique manufacturer names from devices
        result = conn.execute("""
            SELECT
                manufacturer_d_name as raw_name,
                manufacturer_d_clean as clean_name,
                COUNT(*) as count
            FROM devices
            WHERE manufacturer_d_name IS NOT NULL
            GROUP BY manufacturer_d_name, manufacturer_d_clean
            ORDER BY count DESC
            LIMIT 50000
        """).fetchall()

        logger.info(f"Found {len(result)} unique manufacturer entries")

        # Build mapping using predefined mappings
        count = 0
        seen_raw = set()

        for row in result:
            raw_name, clean_name, _ = row
            if raw_name in seen_raw:
                continue
            seen_raw.add(raw_name)

            # Try to find a standard name from predefined mappings
            raw_upper = raw_name.upper().strip() if raw_name else ""
            parent_company = MANUFACTURER_MAPPINGS.get(raw_upper)

            # Determine if this is a known SCS manufacturer
            is_scs = parent_company in [
                "Abbott", "Medtronic", "Boston Scientific", "Nevro",
                "Stimwave", "Nalu Medical", "Saluda Medical"
            ]

            try:
                conn.execute("""
                    INSERT INTO manufacturers (id, raw_name, clean_name, parent_company, is_scs_manufacturer)
                    VALUES (?, ?, ?, ?, ?)
                """, [count + 1, raw_name, clean_name or raw_name, parent_company, is_scs])
                count += 1
            except Exception as e:
                logger.debug(f"Error inserting manufacturer: {e}")

        logger.info(f"Loaded {count} manufacturer mappings")
        return count

    except Exception as e:
        logger.error(f"Error populating manufacturers: {e}")
        return 0


def update_master_events_from_devices(
    conn,
    logger,
) -> Tuple[int, int]:
    """
    Update master_events with manufacturer and product_code from devices table.

    The FDA stores these fields in device files, not master files.

    Args:
        conn: DuckDB connection
        logger: Logger instance

    Returns:
        Tuple of (manufacturer_updated, product_code_updated)
    """
    logger.info("Updating master_events from devices table...")

    # Check current state
    before = conn.execute("""
        SELECT
            COUNT(manufacturer_clean) as has_mfr,
            COUNT(product_code) as has_product
        FROM master_events
    """).fetchone()

    # Update with data from devices
    conn.execute("""
        UPDATE master_events
        SET
            manufacturer_clean = COALESCE(master_events.manufacturer_clean, sub.manufacturer_d_clean),
            product_code = COALESCE(master_events.product_code, sub.device_report_product_code)
        FROM (
            SELECT
                mdr_report_key,
                FIRST(manufacturer_d_clean) as manufacturer_d_clean,
                FIRST(device_report_product_code) as device_report_product_code
            FROM devices
            WHERE manufacturer_d_clean IS NOT NULL
               OR device_report_product_code IS NOT NULL
            GROUP BY mdr_report_key
        ) sub
        WHERE master_events.mdr_report_key = sub.mdr_report_key
          AND (master_events.manufacturer_clean IS NULL OR master_events.product_code IS NULL)
    """)

    # Check results
    after = conn.execute("""
        SELECT
            COUNT(manufacturer_clean) as has_mfr,
            COUNT(product_code) as has_product
        FROM master_events
    """).fetchone()

    mfr_added = after[0] - before[0]
    product_added = after[1] - before[1]

    logger.info(f"Updated master_events: {mfr_added:,} manufacturers, {product_added:,} product codes")

    return mfr_added, product_added


def main():
    """Main entry point for lookup table population."""
    parser = argparse.ArgumentParser(
        description="Populate FDA MAUDE lookup tables",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--data-dir",
        type=Path,
        default=config.data.raw_path,
        help="Directory containing FDA files",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=config.database.path,
        help="Path to DuckDB database",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Drop and rebuild all lookup tables",
    )
    parser.add_argument(
        "--table",
        choices=["problem_codes", "patient_problem_codes", "product_codes", "manufacturers", "all"],
        default="all",
        help="Only populate specific table",
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
    logger = get_logger("populate_lookups")

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("FDA MAUDE Lookup Table Population")
    logger.info(f"Started at: {start_time}")
    logger.info("=" * 60)

    logger.info(f"Data directory: {args.data_dir}")
    logger.info(f"Database: {args.db}")

    try:
        with get_connection(args.db) as conn:
            # Initialize schema if needed
            initialize_database(conn)

            results = {}

            # Populate problem codes
            if args.table in ("all", "problem_codes"):
                results["problem_codes"] = populate_problem_codes(conn, args.data_dir, logger)

            # Populate patient problem codes
            if args.table in ("all", "patient_problem_codes"):
                results["patient_problem_codes"] = populate_patient_problem_codes(conn, args.data_dir, logger)

            # Populate product codes
            if args.table in ("all", "product_codes"):
                results["product_codes"] = populate_product_codes_from_data(conn, logger)

            # Populate manufacturers
            if args.table in ("all", "manufacturers"):
                results["manufacturers"] = populate_manufacturers_from_data(conn, logger)

            # Update master_events with device data
            if args.table == "all":
                mfr_updated, product_updated = update_master_events_from_devices(conn, logger)
                results["master_mfr_updated"] = mfr_updated
                results["master_product_updated"] = product_updated

            # Print summary
            print("\n" + "=" * 60)
            print("SUMMARY")
            print("-" * 40)
            for table, count in results.items():
                print(f"  {table}: {count:,}")

            # Show final counts
            print("\n" + "-" * 40)
            print("Final table counts:")

            for table in ["problem_codes", "patient_problem_codes", "product_codes", "manufacturers"]:
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    print(f"  {table}: {count:,}")
                except:
                    print(f"  {table}: (error)")

            print("=" * 60)

        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"\nCompleted at: {end_time}")
        logger.info(f"Duration: {duration}")

        return 0

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
