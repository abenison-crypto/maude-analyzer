#!/usr/bin/env python3
"""Database health check script for MAUDE Analyzer.

Generates a comprehensive data quality report including:
- NULL manufacturer_clean counts
- Orphaned devices/events
- Field completeness statistics
"""

import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from src.database import get_connection


def run_health_check(verbose: bool = True) -> dict:
    """Run comprehensive database health check.

    Args:
        verbose: Print results as they are collected.

    Returns:
        Dictionary containing all health check results.
    """
    results = {
        "timestamp": datetime.now().isoformat(),
        "database_path": str(config.database.path),
        "issues": [],
        "warnings": [],
    }

    with get_connection(read_only=True) as conn:
        # 1. Basic table counts
        if verbose:
            print("=" * 60)
            print("MAUDE DATABASE HEALTH CHECK")
            print("=" * 60)
            print(f"\nDatabase: {config.database.path}")
            print(f"Timestamp: {results['timestamp']}")
            print("\n" + "-" * 60)
            print("TABLE COUNTS")
            print("-" * 60)

        tables = ["master_events", "devices", "patients", "mdr_text", "device_problems"]
        results["table_counts"] = {}

        for table in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                results["table_counts"][table] = count
                if verbose:
                    print(f"{table:20s}: {count:>15,}")
            except Exception as e:
                results["table_counts"][table] = f"ERROR: {e}"
                if verbose:
                    print(f"{table:20s}: ERROR - {e}")

        # 2. Manufacturer data quality (critical issue)
        if verbose:
            print("\n" + "-" * 60)
            print("MANUFACTURER DATA QUALITY (Critical)")
            print("-" * 60)

        mfr_stats = conn.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(manufacturer_clean) as with_mfr,
                COUNT(*) - COUNT(manufacturer_clean) as null_mfr,
                ROUND(COUNT(manufacturer_clean) * 100.0 / COUNT(*), 2) as pct_with_mfr
            FROM master_events
        """).fetchone()

        results["manufacturer"] = {
            "total_events": mfr_stats[0],
            "with_manufacturer": mfr_stats[1],
            "null_manufacturer": mfr_stats[2],
            "percent_with_manufacturer": mfr_stats[3],
        }

        if verbose:
            print(f"Total events:           {mfr_stats[0]:>12,}")
            print(f"With manufacturer:      {mfr_stats[1]:>12,} ({mfr_stats[3]:.1f}%)")
            print(f"NULL manufacturer:      {mfr_stats[2]:>12,} ({100 - mfr_stats[3]:.1f}%)")

        if mfr_stats[3] < 90:
            issue = f"Only {mfr_stats[3]:.1f}% of events have manufacturer_clean - run populate_master_from_devices()"
            results["issues"].append(issue)
            if verbose:
                print(f"\n⚠️  ISSUE: {issue}")

        # 3. Product code quality
        if verbose:
            print("\n" + "-" * 60)
            print("PRODUCT CODE DATA QUALITY")
            print("-" * 60)

        product_stats = conn.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(product_code) as with_code,
                ROUND(COUNT(product_code) * 100.0 / COUNT(*), 2) as pct_with_code
            FROM master_events
        """).fetchone()

        results["product_code"] = {
            "total_events": product_stats[0],
            "with_product_code": product_stats[1],
            "percent_with_product_code": product_stats[2],
        }

        if verbose:
            print(f"With product_code:      {product_stats[1]:>12,} ({product_stats[2]:.1f}%)")
            print(f"NULL product_code:      {product_stats[0] - product_stats[1]:>12,} ({100 - product_stats[2]:.1f}%)")

        # 4. Orphaned records analysis
        if verbose:
            print("\n" + "-" * 60)
            print("ORPHANED RECORDS ANALYSIS")
            print("-" * 60)

        # Orphaned devices (devices without master events)
        orphaned_devices = conn.execute("""
            SELECT COUNT(*) FROM devices d
            WHERE NOT EXISTS (
                SELECT 1 FROM master_events m WHERE m.mdr_report_key = d.mdr_report_key
            )
        """).fetchone()[0]

        total_devices = results["table_counts"].get("devices", 0)
        if isinstance(total_devices, int) and total_devices > 0:
            orphan_pct = round(orphaned_devices * 100.0 / total_devices, 2)
        else:
            orphan_pct = 0

        results["orphaned_devices"] = {
            "count": orphaned_devices,
            "percent": orphan_pct,
        }

        if verbose:
            print(f"Orphaned devices:       {orphaned_devices:>12,} ({orphan_pct:.1f}%)")

        if orphan_pct > 10:
            warning = f"{orphan_pct:.1f}% of devices are orphaned (not linked to master_events)"
            results["warnings"].append(warning)

        # Events without devices
        events_no_device = conn.execute("""
            SELECT COUNT(*) FROM master_events m
            WHERE NOT EXISTS (
                SELECT 1 FROM devices d WHERE d.mdr_report_key = m.mdr_report_key
            )
        """).fetchone()[0]

        total_events = results["table_counts"].get("master_events", 0)
        if isinstance(total_events, int) and total_events > 0:
            no_device_pct = round(events_no_device * 100.0 / total_events, 2)
        else:
            no_device_pct = 0

        results["events_without_devices"] = {
            "count": events_no_device,
            "percent": no_device_pct,
        }

        if verbose:
            print(f"Events without devices: {events_no_device:>12,} ({no_device_pct:.1f}%)")

        # 5. Field completeness for key fields
        if verbose:
            print("\n" + "-" * 60)
            print("FIELD COMPLETENESS (master_events)")
            print("-" * 60)

        key_fields = [
            "date_received", "date_of_event", "event_type",
            "date_facility_aware", "report_to_manufacturer",
            "manufacturer_name", "manufacturer_clean", "product_code"
        ]

        results["field_completeness"] = {}

        for field in key_fields:
            try:
                stats = conn.execute(f"""
                    SELECT
                        COUNT({field}) as non_null,
                        ROUND(COUNT({field}) * 100.0 / COUNT(*), 2) as pct
                    FROM master_events
                """).fetchone()

                results["field_completeness"][field] = {
                    "non_null_count": stats[0],
                    "percent": stats[1],
                }

                if verbose:
                    status = "✓" if stats[1] > 90 else ("⚠" if stats[1] > 50 else "✗")
                    print(f"{status} {field:30s}: {stats[1]:>6.1f}%")

            except Exception as e:
                results["field_completeness"][field] = {"error": str(e)}
                if verbose:
                    print(f"✗ {field:30s}: ERROR - {e}")

        # 6. Date range
        if verbose:
            print("\n" + "-" * 60)
            print("DATE RANGE")
            print("-" * 60)

        date_range = conn.execute("""
            SELECT
                MIN(date_received) as min_date,
                MAX(date_received) as max_date
            FROM master_events
            WHERE date_received IS NOT NULL
        """).fetchone()

        results["date_range"] = {
            "earliest": str(date_range[0]) if date_range[0] else None,
            "latest": str(date_range[1]) if date_range[1] else None,
        }

        if verbose:
            print(f"Earliest event:         {date_range[0]}")
            print(f"Latest event:           {date_range[1]}")

        # 7. Event type distribution
        if verbose:
            print("\n" + "-" * 60)
            print("EVENT TYPE DISTRIBUTION")
            print("-" * 60)

        event_types = conn.execute("""
            SELECT
                event_type,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct
            FROM master_events
            GROUP BY event_type
            ORDER BY count DESC
        """).fetchall()

        results["event_types"] = {}
        for row in event_types:
            event_type = row[0] if row[0] else "(NULL)"
            results["event_types"][event_type] = {
                "count": row[1],
                "percent": row[2],
            }
            if verbose:
                type_label = {
                    "D": "D (Death)",
                    "I": "I (Injury)",
                    "M": "M (Malfunction)",
                    "O": "O (Other)",
                    "*": "* (Unknown)",
                    None: "(NULL)",
                }.get(row[0], row[0])
                print(f"{type_label or '(NULL)':20s}: {row[1]:>12,} ({row[2]:>5.1f}%)")

        # 8. Manufacturer coverage by year
        if verbose:
            print("\n" + "-" * 60)
            print("MANUFACTURER COVERAGE BY YEAR")
            print("-" * 60)

        mfr_by_year = conn.execute("""
            SELECT
                EXTRACT(YEAR FROM date_received)::INTEGER as year,
                COUNT(*) as total,
                COUNT(manufacturer_clean) as with_mfr,
                ROUND(COUNT(manufacturer_clean) * 100.0 / COUNT(*), 1) as pct
            FROM master_events
            WHERE date_received IS NOT NULL
            GROUP BY 1
            ORDER BY 1
        """).fetchall()

        results["manufacturer_by_year"] = []
        years_with_gaps = []
        for row in mfr_by_year:
            if row[0]:
                year_data = {
                    "year": int(row[0]),
                    "total": row[1],
                    "with_manufacturer": row[2],
                    "percent": row[3],
                }
                results["manufacturer_by_year"].append(year_data)

                if row[3] < 50:
                    years_with_gaps.append((int(row[0]), row[3]))

                if verbose:
                    status = "✓" if row[3] >= 90 else ("⚠" if row[3] >= 50 else "✗")
                    print(f"{status} {row[0]}: {row[3]:>5.1f}% ({row[2]:,} of {row[1]:,})")

        if years_with_gaps:
            gap_msg = f"Low manufacturer coverage in years: {', '.join(f'{y[0]} ({y[1]:.0f}%)' for y in years_with_gaps[:5])}"
            results["warnings"].append(gap_msg)

        # 9. Device match rate by year
        if verbose:
            print("\n" + "-" * 60)
            print("DEVICE MATCH RATE BY YEAR")
            print("-" * 60)

        device_by_year = conn.execute("""
            SELECT
                EXTRACT(YEAR FROM m.date_received)::INTEGER as year,
                COUNT(*) as total,
                COUNT(DISTINCT CASE WHEN d.mdr_report_key IS NOT NULL THEN m.mdr_report_key END) as with_device,
                ROUND(COUNT(DISTINCT CASE WHEN d.mdr_report_key IS NOT NULL THEN m.mdr_report_key END) * 100.0 / COUNT(*), 1) as pct
            FROM master_events m
            LEFT JOIN devices d ON m.mdr_report_key = d.mdr_report_key
            WHERE m.date_received IS NOT NULL
            GROUP BY 1
            ORDER BY 1
        """).fetchall()

        results["device_match_by_year"] = []
        years_no_devices = []
        for row in device_by_year:
            if row[0]:
                year_data = {
                    "year": int(row[0]),
                    "total": row[1],
                    "with_device": row[2],
                    "percent": row[3],
                }
                results["device_match_by_year"].append(year_data)

                if row[3] < 10:
                    years_no_devices.append(int(row[0]))

                if verbose:
                    status = "✓" if row[3] >= 80 else ("⚠" if row[3] >= 50 else "✗")
                    print(f"{status} {row[0]}: {row[3]:>5.1f}% ({row[2]:,} of {row[1]:,})")

        if years_no_devices:
            device_msg = f"Missing device files for years: {', '.join(str(y) for y in years_no_devices[:10])}"
            results["issues"].append(device_msg)

        # 10. Top manufacturers (if data exists)
        if results["manufacturer"]["with_manufacturer"] > 0:
            if verbose:
                print("\n" + "-" * 60)
                print("TOP 10 MANUFACTURERS")
                print("-" * 60)

            top_mfrs = conn.execute("""
                SELECT
                    manufacturer_clean,
                    COUNT(*) as count
                FROM master_events
                WHERE manufacturer_clean IS NOT NULL
                GROUP BY manufacturer_clean
                ORDER BY count DESC
                LIMIT 10
            """).fetchall()

            results["top_manufacturers"] = []
            for row in top_mfrs:
                results["top_manufacturers"].append({
                    "name": row[0],
                    "count": row[1],
                })
                if verbose:
                    print(f"{row[0][:40]:40s}: {row[1]:>12,}")

        # Summary
        if verbose:
            print("\n" + "=" * 60)
            print("SUMMARY")
            print("=" * 60)

            if results["issues"]:
                print("\n🔴 CRITICAL ISSUES:")
                for issue in results["issues"]:
                    print(f"   • {issue}")

            if results["warnings"]:
                print("\n🟡 WARNINGS:")
                for warning in results["warnings"]:
                    print(f"   • {warning}")

            if not results["issues"] and not results["warnings"]:
                print("\n🟢 No critical issues found.")

            print("\n" + "=" * 60)

    return results


def main():
    """Main entry point."""
    import argparse
    import json

    parser = argparse.ArgumentParser(description="MAUDE Database Health Check")
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    parser.add_argument("--output", "-o", type=Path, help="Save results to file")
    args = parser.parse_args()

    results = run_health_check(verbose=not args.json)

    if args.json:
        print(json.dumps(results, indent=2, default=str))

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nResults saved to: {args.output}")

    # Return exit code based on issues
    return 1 if results["issues"] else 0


if __name__ == "__main__":
    sys.exit(main())
