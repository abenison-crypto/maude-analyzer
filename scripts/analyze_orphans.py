#!/usr/bin/env python3
"""Analyze orphaned events and data quality issues.

This script investigates events without device records and provides
insights into data completeness and potential solutions.

Usage:
    python scripts/analyze_orphans.py
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.services.database import get_db


def analyze_orphan_summary(db):
    """Get high-level orphan statistics."""
    print("\n" + "=" * 60)
    print("ORPHAN EVENT ANALYSIS")
    print("=" * 60)

    # Total events without devices
    query = """
        SELECT
            COUNT(*) as total_events,
            COUNT(DISTINCT m.mdr_report_key) - COUNT(DISTINCT d.mdr_report_key) as orphaned_events
        FROM master_events m
        LEFT JOIN devices d ON m.mdr_report_key = d.mdr_report_key
    """
    result = db.fetch_one("""
        SELECT
            (SELECT COUNT(*) FROM master_events) as total_events,
            (SELECT COUNT(*) FROM master_events m
             WHERE NOT EXISTS (SELECT 1 FROM devices d WHERE d.mdr_report_key = m.mdr_report_key)) as orphaned_events
    """)

    total, orphaned = result
    print(f"\nTotal events: {total:,}")
    print(f"Events without device records: {orphaned:,} ({orphaned * 100 / total:.1f}%)")


def analyze_orphans_by_year(db):
    """Analyze orphaned events by year."""
    print("\n" + "-" * 60)
    print("ORPHANED EVENTS BY YEAR")
    print("-" * 60)

    query = """
        SELECT
            EXTRACT(YEAR FROM m.date_received)::INTEGER as year,
            COUNT(*) as total_events,
            SUM(CASE WHEN d.mdr_report_key IS NULL THEN 1 ELSE 0 END) as orphaned_events
        FROM master_events m
        LEFT JOIN (SELECT DISTINCT mdr_report_key FROM devices) d ON m.mdr_report_key = d.mdr_report_key
        WHERE m.date_received IS NOT NULL
        GROUP BY EXTRACT(YEAR FROM m.date_received)
        ORDER BY year DESC
        LIMIT 15
    """

    results = db.fetch_all(query)
    print(f"\n{'Year':<6} {'Total':>12} {'Orphaned':>12} {'%':>8}")
    print("-" * 40)

    for year, total, orphaned in results:
        pct = orphaned * 100 / total if total > 0 else 0
        print(f"{int(year):<6} {total:>12,} {orphaned:>12,} {pct:>7.1f}%")


def analyze_manufacturer_coverage_by_year(db):
    """Analyze manufacturer coverage by year."""
    print("\n" + "-" * 60)
    print("MANUFACTURER COVERAGE BY YEAR")
    print("-" * 60)

    query = """
        SELECT
            EXTRACT(YEAR FROM date_received)::INTEGER as year,
            COUNT(*) as total,
            COUNT(manufacturer_clean) as with_mfr
        FROM master_events
        WHERE date_received IS NOT NULL
        GROUP BY EXTRACT(YEAR FROM date_received)
        ORDER BY year DESC
        LIMIT 15
    """

    results = db.fetch_all(query)
    print(f"\n{'Year':<6} {'Total':>12} {'With Mfr':>12} {'%':>8}")
    print("-" * 40)

    for year, total, with_mfr in results:
        pct = with_mfr * 100 / total if total > 0 else 0
        print(f"{int(year):<6} {total:>12,} {with_mfr:>12,} {pct:>7.1f}%")


def analyze_event_types_for_orphans(db):
    """Check if certain event types are more likely to be orphaned."""
    print("\n" + "-" * 60)
    print("ORPHAN RATE BY EVENT TYPE")
    print("-" * 60)

    query = """
        SELECT
            CASE m.event_type
                WHEN 'D' THEN 'Death'
                WHEN 'IN' THEN 'Injury'
                WHEN 'M' THEN 'Malfunction'
                WHEN 'O' THEN 'Other'
                ELSE 'Unknown'
            END as event_type,
            COUNT(*) as total,
            SUM(CASE WHEN d.mdr_report_key IS NULL THEN 1 ELSE 0 END) as orphaned
        FROM master_events m
        LEFT JOIN (SELECT DISTINCT mdr_report_key FROM devices) d ON m.mdr_report_key = d.mdr_report_key
        GROUP BY m.event_type
        ORDER BY total DESC
    """

    results = db.fetch_all(query)
    print(f"\n{'Event Type':<15} {'Total':>12} {'Orphaned':>12} {'%':>8}")
    print("-" * 50)

    for event_type, total, orphaned in results:
        pct = orphaned * 100 / total if total > 0 else 0
        print(f"{event_type:<15} {total:>12,} {orphaned:>12,} {pct:>7.1f}%")


def identify_patterns(db):
    """Identify patterns in orphaned events."""
    print("\n" + "-" * 60)
    print("DATA PATTERN ANALYSIS")
    print("-" * 60)

    # Check if there's a correlation with date_received patterns
    query = """
        SELECT
            CASE
                WHEN date_received IS NULL THEN 'No date'
                WHEN date_received < DATE '2010-01-01' THEN 'Before 2010'
                WHEN date_received < DATE '2015-01-01' THEN '2010-2014'
                WHEN date_received < DATE '2020-01-01' THEN '2015-2019'
                ELSE '2020+'
            END as period,
            COUNT(*) as total,
            COUNT(manufacturer_clean) as with_mfr,
            COUNT(product_code) as with_product
        FROM master_events
        GROUP BY
            CASE
                WHEN date_received IS NULL THEN 'No date'
                WHEN date_received < DATE '2010-01-01' THEN 'Before 2010'
                WHEN date_received < DATE '2015-01-01' THEN '2010-2014'
                WHEN date_received < DATE '2020-01-01' THEN '2015-2019'
                ELSE '2020+'
            END
        ORDER BY total DESC
    """

    results = db.fetch_all(query)
    print(f"\n{'Period':<15} {'Total':>12} {'With Mfr':>10} {'With Prod':>10}")
    print("-" * 50)

    for period, total, with_mfr, with_product in results:
        mfr_pct = with_mfr * 100 / total if total > 0 else 0
        prod_pct = with_product * 100 / total if total > 0 else 0
        print(f"{period:<15} {total:>12,} {mfr_pct:>9.1f}% {prod_pct:>9.1f}%")


def generate_recommendations(db):
    """Generate recommendations based on analysis."""
    print("\n" + "=" * 60)
    print("RECOMMENDATIONS")
    print("=" * 60)

    # Check recent data quality
    result = db.fetch_one("""
        SELECT
            COUNT(*) as total,
            COUNT(manufacturer_clean) as with_mfr
        FROM master_events
        WHERE EXTRACT(YEAR FROM date_received) >= 2020
    """)

    recent_total, recent_with_mfr = result
    recent_pct = recent_with_mfr * 100 / recent_total if recent_total > 0 else 0

    print(f"""
1. DATA COMPLETENESS ISSUE
   - Recent data (2020+) has {recent_pct:.1f}% manufacturer coverage
   - This is likely due to device data not being loaded for recent years

2. RECOMMENDED ACTIONS
   - Download and process recent DEVICE*.txt files from FDA
   - Run scripts/fix_manufacturer_data.py to populate manufacturer_clean
   - Run scripts/populate_lookup_tables.py to update product codes

3. HISTORICAL DATA
   - Older data (2010-2019) has better manufacturer coverage (~96%)
   - For analysis, consider focusing on date ranges with complete data

4. FOR SIGNAL DETECTION
   - The /api/analytics/signals endpoint automatically uses dates
     with available manufacturer data
   - This ensures reliable signal detection even with incomplete recent data
""")


def main():
    """Run complete orphan analysis."""
    print(f"MAUDE Data Quality - Orphan Analysis")
    print(f"Timestamp: {datetime.now().isoformat()}")

    db = get_db()

    try:
        analyze_orphan_summary(db)
        analyze_orphans_by_year(db)
        analyze_manufacturer_coverage_by_year(db)
        analyze_event_types_for_orphans(db)
        identify_patterns(db)
        generate_recommendations(db)

        print("\n" + "=" * 60)
        print("Analysis complete")

    except Exception as e:
        print(f"Error during analysis: {e}")
        raise


if __name__ == "__main__":
    main()
