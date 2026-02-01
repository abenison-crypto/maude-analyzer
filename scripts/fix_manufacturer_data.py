#!/usr/bin/env python
"""
One-time fix to populate manufacturer_clean and product_code in master_events from devices table.

The FDA MAUDE data architecture stores manufacturer and product code information in the
device file (foidev.txt), NOT in the master file (mdrfoi.txt). The master file's
MANUFACTURER_NAME field is 99.99% empty by design.

This script:
1. Verifies the devices table has manufacturer data
2. Updates master_events.manufacturer_clean from devices.manufacturer_d_clean
3. Updates master_events.product_code from devices.device_report_product_code
4. Reports the results

Usage:
    python scripts/fix_manufacturer_data.py [--dry-run]
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from src.database import get_connection


def main():
    """Main entry point for manufacturer data fix."""
    parser = argparse.ArgumentParser(
        description="Populate master_events manufacturer/product_code from devices table"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=config.database.path,
        help="Database path",
    )

    args = parser.parse_args()

    setup_logging(log_level="INFO")
    logger = get_logger("fix_manufacturer_data")

    logger.info("=" * 60)
    logger.info("MAUDE Data Fix: Populate Master Events from Devices")
    logger.info(f"Database: {args.db}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info("=" * 60)

    if not args.db.exists():
        logger.error(f"Database not found: {args.db}")
        return 1

    with get_connection(args.db) as conn:
        # Step 1: Check devices table has data
        logger.info("\nStep 1: Checking devices table...")
        result = conn.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(manufacturer_d_clean) as has_mfr,
                COUNT(device_report_product_code) as has_product
            FROM devices
        """).fetchone()

        total_devices = result[0]
        devices_with_mfr = result[1]
        devices_with_product = result[2]

        logger.info(f"  Total devices: {total_devices:,}")
        logger.info(f"  With manufacturer_d_clean: {devices_with_mfr:,} ({100*devices_with_mfr/total_devices:.1f}%)")
        logger.info(f"  With product_code: {devices_with_product:,} ({100*devices_with_product/total_devices:.1f}%)")

        if devices_with_mfr == 0:
            logger.error("ERROR: devices table has no manufacturer data!")
            logger.error("Please ensure devices are loaded first with: python scripts/initial_load.py")
            return 1

        # Step 2: Check current master_events state
        logger.info("\nStep 2: Checking current master_events state...")
        result = conn.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(manufacturer_clean) as has_mfr,
                COUNT(product_code) as has_product
            FROM master_events
        """).fetchone()

        total_master = result[0]
        master_with_mfr = result[1]
        master_with_product = result[2]

        logger.info(f"  Total master_events: {total_master:,}")
        logger.info(f"  With manufacturer_clean: {master_with_mfr:,} ({100*master_with_mfr/total_master:.1f}%)")
        logger.info(f"  With product_code: {master_with_product:,} ({100*master_with_product/total_master:.1f}%)")

        # Step 3: Check how many can be populated
        logger.info("\nStep 3: Checking join potential...")
        result = conn.execute("""
            SELECT COUNT(DISTINCT m.mdr_report_key)
            FROM master_events m
            JOIN devices d ON m.mdr_report_key = d.mdr_report_key
            WHERE (m.manufacturer_clean IS NULL OR m.product_code IS NULL)
              AND (d.manufacturer_d_clean IS NOT NULL OR d.device_report_product_code IS NOT NULL)
        """).fetchone()

        can_populate = result[0]
        logger.info(f"  Master events that can be populated from devices: {can_populate:,}")

        if can_populate == 0:
            logger.info("\nNo updates needed - data already populated or no matching devices.")
            return 0

        # Step 4: Perform the update
        if args.dry_run:
            logger.info("\nStep 4: DRY RUN - No changes will be made")
            logger.info(f"  Would update {can_populate:,} master_events records")
        else:
            logger.info("\nStep 4: Updating master_events from devices...")
            start_time = datetime.now()

            # DuckDB uses different syntax than SQLite for UPDATE FROM
            # Using a subquery approach that works in DuckDB
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

            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"  Update completed in {duration:.1f}s")

        # Step 5: Verify results
        logger.info("\nStep 5: Verifying results...")
        result = conn.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(manufacturer_clean) as has_mfr,
                COUNT(product_code) as has_product
            FROM master_events
        """).fetchone()

        final_total = result[0]
        final_with_mfr = result[1]
        final_with_product = result[2]

        logger.info(f"  Total master_events: {final_total:,}")
        logger.info(f"  With manufacturer_clean: {final_with_mfr:,} ({100*final_with_mfr/final_total:.1f}%)")
        logger.info(f"  With product_code: {final_with_product:,} ({100*final_with_product/final_total:.1f}%)")

        # Show improvement
        mfr_improvement = final_with_mfr - master_with_mfr
        product_improvement = final_with_product - master_with_product

        if not args.dry_run:
            logger.info(f"\n  Manufacturer records added: {mfr_improvement:,}")
            logger.info(f"  Product code records added: {product_improvement:,}")

        # Step 6: Show top manufacturers
        logger.info("\nStep 6: Top manufacturers in master_events:")
        top_mfrs = conn.execute("""
            SELECT manufacturer_clean, COUNT(*) as cnt
            FROM master_events
            WHERE manufacturer_clean IS NOT NULL
            GROUP BY manufacturer_clean
            ORDER BY cnt DESC
            LIMIT 10
        """).fetchall()

        for mfr, cnt in top_mfrs:
            logger.info(f"  {mfr}: {cnt:,}")

        # Step 7: Show top product codes
        logger.info("\nTop product codes in master_events:")
        top_products = conn.execute("""
            SELECT product_code, COUNT(*) as cnt
            FROM master_events
            WHERE product_code IS NOT NULL
            GROUP BY product_code
            ORDER BY cnt DESC
            LIMIT 10
        """).fetchall()

        for code, cnt in top_products:
            logger.info(f"  {code}: {cnt:,}")

    logger.info("\n" + "=" * 60)
    if args.dry_run:
        logger.info("DRY RUN COMPLETE - No changes made")
        logger.info("Run without --dry-run to apply changes")
    else:
        logger.info("FIX COMPLETE!")
        logger.info("Dashboard should now show manufacturer and product data")
    logger.info("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
