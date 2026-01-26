#!/usr/bin/env python3
"""Comprehensive Data Audit Script for MAUDE Analyzer.

This script performs a full audit of all downloaded files and database state:
1. All expected files downloaded (compare KNOWN_FILES vs data/raw/)
2. All files loaded (check ingestion_log)
3. Patient problem codes loaded (~21M expected)
4. Referential integrity (orphan rates)
5. Data freshness (days since latest record)
6. Duplicate detection
7. Statistical anomalies (month-over-month count anomalies)

Usage:
    python scripts/data_audit.py [options]

Options:
    --json          Output results as JSON
    --output PATH   Save results to file
    --verbose       Show detailed output
    --fix           Attempt to fix detected issues (re-download/re-load)
"""

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Any, Optional

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger
from src.database import get_connection
from src.ingestion.download import KNOWN_FILES, INCREMENTAL_FILES

logger = get_logger("data_audit")


# Expected record counts (approximate - for sanity checks)
EXPECTED_COUNTS = {
    "master_events": 10_000_000,      # ~10M+ master events
    "devices": 15_000_000,            # ~15M+ device records
    "patients": 20_000_000,           # ~20M+ patient records
    "mdr_text": 25_000_000,           # ~25M+ text records
    "device_problems": 20_000_000,    # ~20M+ device problem links
    "patient_problems": 20_000_000,   # ~21M expected patient problem codes
}

# Minimum expected counts (below this triggers a warning)
MIN_EXPECTED_COUNTS = {
    "master_events": 5_000_000,
    "devices": 5_000_000,
    "patients": 5_000_000,
    "mdr_text": 10_000_000,
    "device_problems": 10_000_000,
    "patient_problems": 15_000_000,  # Plan says ~21M expected
}


def check_downloaded_files(data_dir: Path, verbose: bool = False) -> Dict[str, Any]:
    """
    Check if all expected files have been downloaded.

    Args:
        data_dir: Directory containing downloaded files.
        verbose: Print detailed output.

    Returns:
        Dictionary with download audit results.
    """
    results = {
        "total_expected": 0,
        "total_found": 0,
        "missing_files": [],
        "extra_files": [],
        "found_by_category": {},
        "status": "PASS",
    }

    # Collect all expected files
    expected_files = set()
    for category, files in KNOWN_FILES.items():
        results["found_by_category"][category] = {"expected": [], "found": [], "missing": []}
        for f in files:
            expected_files.add(f)
            results["found_by_category"][category]["expected"].append(f)
            results["total_expected"] += 1

    # Also include incremental files
    for category, types in INCREMENTAL_FILES.items():
        for file_type, files in types.items():
            for f in files:
                if f:  # Skip empty entries
                    expected_files.add(f)
                    if category in results["found_by_category"]:
                        results["found_by_category"][category]["expected"].append(f)
                    results["total_expected"] += 1

    # Check which files exist
    found_files = set()
    for f in data_dir.glob("*.zip"):
        found_files.add(f.name)

    # Also check for extracted .txt files (in case zips were cleaned up)
    for f in data_dir.glob("*.txt"):
        # Map txt back to potential zip name
        base = f.stem
        potential_zip = f"{base}.zip"
        if potential_zip in expected_files:
            found_files.add(potential_zip)

    results["total_found"] = len(found_files & expected_files)

    # Find missing files
    missing = expected_files - found_files
    results["missing_files"] = sorted(list(missing))

    # Find extra files (downloaded but not in expected list)
    extra = found_files - expected_files
    # Filter out non-FDA files
    extra = {f for f in extra if not f.startswith(".")}
    results["extra_files"] = sorted(list(extra))

    # Update category-level results
    for category in results["found_by_category"]:
        expected_set = set(results["found_by_category"][category]["expected"])
        found_set = found_files & expected_set
        missing_set = expected_set - found_files
        results["found_by_category"][category]["found"] = sorted(list(found_set))
        results["found_by_category"][category]["missing"] = sorted(list(missing_set))

    # Determine status
    if results["missing_files"]:
        results["status"] = "FAIL"

    if verbose:
        print("\n" + "=" * 60)
        print("FILE DOWNLOAD AUDIT")
        print("=" * 60)
        print(f"Expected files: {results['total_expected']}")
        print(f"Found files: {results['total_found']}")

        if results["missing_files"]:
            print(f"\nMISSING FILES ({len(results['missing_files'])}):")
            for f in results["missing_files"][:20]:  # Limit to first 20
                print(f"  - {f}")
            if len(results["missing_files"]) > 20:
                print(f"  ... and {len(results['missing_files']) - 20} more")
        else:
            print("\nAll expected files present")

    return results


def check_ingestion_log(conn, verbose: bool = False) -> Dict[str, Any]:
    """
    Check ingestion log for loaded files and any failures.

    Args:
        conn: Database connection.
        verbose: Print detailed output.

    Returns:
        Dictionary with ingestion log audit results.
    """
    results = {
        "total_loaded": 0,
        "total_failed": 0,
        "failed_files": [],
        "successful_files": [],
        "by_file_type": {},
        "last_load": None,
        "status": "PASS",
    }

    try:
        # Get all ingestion records
        records = conn.execute("""
            SELECT file_name, file_type, status, records_loaded, records_errors,
                   completed_at, error_message
            FROM ingestion_log
            ORDER BY completed_at DESC
        """).fetchall()

        for row in records:
            file_name, file_type, status, records_loaded, records_errors, completed_at, error_msg = row

            if status == "completed":
                results["total_loaded"] += 1
                results["successful_files"].append(file_name)
            else:
                results["total_failed"] += 1
                results["failed_files"].append({
                    "file": file_name,
                    "status": status,
                    "error": error_msg,
                })

            # Track by file type
            if file_type not in results["by_file_type"]:
                results["by_file_type"][file_type] = {
                    "loaded": 0, "failed": 0, "total_records": 0
                }

            if status == "completed":
                results["by_file_type"][file_type]["loaded"] += 1
                results["by_file_type"][file_type]["total_records"] += records_loaded or 0

        # Get last successful load time
        last_load = conn.execute("""
            SELECT MAX(completed_at) FROM ingestion_log WHERE status = 'completed'
        """).fetchone()[0]
        results["last_load"] = str(last_load) if last_load else None

    except Exception as e:
        results["status"] = "ERROR"
        results["error"] = str(e)

    if results["total_failed"] > 0:
        results["status"] = "WARNING"

    if verbose:
        print("\n" + "=" * 60)
        print("INGESTION LOG AUDIT")
        print("=" * 60)
        print(f"Files loaded: {results['total_loaded']}")
        print(f"Files failed: {results['total_failed']}")
        print(f"Last load: {results['last_load']}")

        if results["failed_files"]:
            print("\nFailed files:")
            for f in results["failed_files"][:10]:
                err = f['error'][:50] if f['error'] else 'Unknown error'
                print(f"  - {f['file']}: {err}...")

    return results


def check_table_counts(conn, verbose: bool = False) -> Dict[str, Any]:
    """
    Check record counts for all tables and compare to expected counts.

    Args:
        conn: Database connection.
        verbose: Print detailed output.

    Returns:
        Dictionary with table count audit results.
    """
    results = {
        "counts": {},
        "warnings": [],
        "status": "PASS",
    }

    tables = [
        "master_events", "devices", "patients", "mdr_text",
        "device_problems", "patient_problems", "product_codes",
        "problem_codes", "patient_problem_codes",
    ]

    if verbose:
        print("\n" + "=" * 60)
        print("TABLE COUNTS")
        print("=" * 60)

    for table in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            results["counts"][table] = count

            # Check against minimum expected
            min_expected = MIN_EXPECTED_COUNTS.get(table, 0)
            expected = EXPECTED_COUNTS.get(table, 0)

            status = "OK"
            if min_expected > 0 and count < min_expected:
                status = "LOW"
                results["warnings"].append(
                    f"{table}: {count:,} records (expected >={min_expected:,})"
                )
                results["status"] = "WARNING"
            elif expected > 0 and count < expected * 0.5:
                status = "CHECK"

            if verbose:
                expected_str = f"(expected ~{expected:,})" if expected > 0 else ""
                status_icon = {"OK": "", "LOW": "", "CHECK": ""}[status]
                print(f"{status_icon} {table:25s}: {count:>15,} {expected_str}")

        except Exception as e:
            results["counts"][table] = f"ERROR: {e}"
            if verbose:
                print(f" {table:25s}: ERROR - {e}")

    return results


def check_patient_problems(conn, verbose: bool = False) -> Dict[str, Any]:
    """
    Verify patient problem codes are loaded correctly.

    Args:
        conn: Database connection.
        verbose: Print detailed output.

    Returns:
        Dictionary with patient problem audit results.
    """
    results = {
        "patient_problems_count": 0,
        "patient_problem_codes_count": 0,
        "unique_problem_codes": 0,
        "records_with_problems": 0,
        "status": "PASS",
    }

    try:
        # Count patient problems
        results["patient_problems_count"] = conn.execute(
            "SELECT COUNT(*) FROM patient_problems"
        ).fetchone()[0]

        # Count patient problem code definitions
        results["patient_problem_codes_count"] = conn.execute(
            "SELECT COUNT(*) FROM patient_problem_codes"
        ).fetchone()[0]

        # Unique codes used
        results["unique_problem_codes"] = conn.execute(
            "SELECT COUNT(DISTINCT patient_problem_code) FROM patient_problems"
        ).fetchone()[0]

        # Patients with problems
        results["records_with_problems"] = conn.execute(
            "SELECT COUNT(DISTINCT mdr_report_key) FROM patient_problems"
        ).fetchone()[0]

        # Check if count is below expected (~21M)
        if results["patient_problems_count"] < 15_000_000:
            results["status"] = "WARNING"
            results["warning"] = f"Patient problems count ({results['patient_problems_count']:,}) below expected ~21M"

    except Exception as e:
        results["status"] = "ERROR"
        results["error"] = str(e)

    if verbose:
        print("\n" + "=" * 60)
        print("PATIENT PROBLEMS AUDIT")
        print("=" * 60)
        print(f"Patient problem records: {results['patient_problems_count']:,}")
        print(f"Problem code definitions: {results['patient_problem_codes_count']:,}")
        print(f"Unique codes in use: {results['unique_problem_codes']:,}")
        print(f"MDR keys with problems: {results['records_with_problems']:,}")

        if results.get("warning"):
            print(f"\n {results['warning']}")

    return results


def check_referential_integrity(conn, verbose: bool = False) -> Dict[str, Any]:
    """
    Check referential integrity (orphan records).

    Args:
        conn: Database connection.
        verbose: Print detailed output.

    Returns:
        Dictionary with referential integrity audit results.
    """
    results = {
        "orphan_rates": {},
        "warnings": [],
        "status": "PASS",
    }

    checks = [
        ("devices", "orphaned_devices", """
            SELECT COUNT(*) FROM devices d
            WHERE NOT EXISTS (
                SELECT 1 FROM master_events m WHERE m.mdr_report_key = d.mdr_report_key
            )
        """),
        ("patients", "orphaned_patients", """
            SELECT COUNT(*) FROM patients p
            WHERE NOT EXISTS (
                SELECT 1 FROM master_events m WHERE m.mdr_report_key = p.mdr_report_key
            )
        """),
        ("mdr_text", "orphaned_text", """
            SELECT COUNT(*) FROM mdr_text t
            WHERE NOT EXISTS (
                SELECT 1 FROM master_events m WHERE m.mdr_report_key = t.mdr_report_key
            )
        """),
        ("device_problems", "orphaned_device_problems", """
            SELECT COUNT(*) FROM device_problems dp
            WHERE NOT EXISTS (
                SELECT 1 FROM master_events m WHERE m.mdr_report_key = dp.mdr_report_key
            )
        """),
    ]

    if verbose:
        print("\n" + "=" * 60)
        print("REFERENTIAL INTEGRITY CHECK")
        print("=" * 60)

    for table, check_name, query in checks:
        try:
            # Get orphan count
            orphan_count = conn.execute(query).fetchone()[0]

            # Get total count
            total_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

            if total_count > 0:
                orphan_rate = round(orphan_count * 100.0 / total_count, 2)
            else:
                orphan_rate = 0

            results["orphan_rates"][check_name] = {
                "orphan_count": orphan_count,
                "total_count": total_count,
                "orphan_rate_pct": orphan_rate,
            }

            # Warn if orphan rate is high (>10%)
            if orphan_rate > 10:
                results["warnings"].append(
                    f"{table}: {orphan_rate:.1f}% orphaned ({orphan_count:,} of {total_count:,})"
                )
                results["status"] = "WARNING"

            if verbose:
                status = "" if orphan_rate <= 10 else ""
                print(f"{status} {table:20s}: {orphan_rate:>5.1f}% orphaned ({orphan_count:,} of {total_count:,})")

        except Exception as e:
            results["orphan_rates"][check_name] = {"error": str(e)}
            if verbose:
                print(f" {table:20s}: ERROR - {e}")

    return results


def check_data_freshness(conn, verbose: bool = False) -> Dict[str, Any]:
    """
    Check data freshness (days since latest record).

    Args:
        conn: Database connection.
        verbose: Print detailed output.

    Returns:
        Dictionary with data freshness audit results.
    """
    results = {
        "freshness": {},
        "warnings": [],
        "status": "PASS",
    }

    today = date.today()

    checks = [
        ("master_events", "date_received", 7),   # Should be <7 days old
        ("master_events", "date_added", 7),      # FDA adds weekly
        ("devices", "date_received", 14),
        ("patients", "date_received", 14),
    ]

    if verbose:
        print("\n" + "=" * 60)
        print("DATA FRESHNESS CHECK")
        print("=" * 60)

    for table, date_column, max_days in checks:
        try:
            result = conn.execute(f"""
                SELECT MAX({date_column}) FROM {table}
                WHERE {date_column} IS NOT NULL
            """).fetchone()[0]

            if result:
                latest_date = result if isinstance(result, date) else result.date() if hasattr(result, 'date') else result
                days_old = (today - latest_date).days if isinstance(latest_date, date) else None

                results["freshness"][f"{table}.{date_column}"] = {
                    "latest_date": str(latest_date),
                    "days_old": days_old,
                    "max_acceptable": max_days,
                }

                if days_old and days_old > max_days:
                    results["warnings"].append(
                        f"{table}.{date_column}: {days_old} days old (max: {max_days})"
                    )
                    results["status"] = "STALE"

                if verbose:
                    status = "" if days_old and days_old <= max_days else ""
                    print(f"{status} {table}.{date_column}: {latest_date} ({days_old} days old)")
            else:
                results["freshness"][f"{table}.{date_column}"] = {"latest_date": None}
                if verbose:
                    print(f" {table}.{date_column}: No data")

        except Exception as e:
            results["freshness"][f"{table}.{date_column}"] = {"error": str(e)}
            if verbose:
                print(f" {table}.{date_column}: ERROR - {e}")

    return results


def check_duplicates(conn, verbose: bool = False) -> Dict[str, Any]:
    """
    Check for duplicate records.

    Args:
        conn: Database connection.
        verbose: Print detailed output.

    Returns:
        Dictionary with duplicate detection results.
    """
    results = {
        "duplicates": {},
        "warnings": [],
        "status": "PASS",
    }

    checks = [
        ("master_events", "mdr_report_key", "exact_pk_dups"),
        ("devices", "mdr_report_key, device_sequence_number", "composite_key_dups"),
        ("patients", "mdr_report_key, patient_sequence_number", "composite_key_dups"),
        ("mdr_text", "mdr_report_key, mdr_text_key", "composite_key_dups"),
    ]

    if verbose:
        print("\n" + "=" * 60)
        print("DUPLICATE DETECTION")
        print("=" * 60)

    for table, key_columns, check_type in checks:
        try:
            # Count duplicates
            dup_query = f"""
                SELECT COUNT(*) FROM (
                    SELECT {key_columns}, COUNT(*) as cnt
                    FROM {table}
                    GROUP BY {key_columns}
                    HAVING COUNT(*) > 1
                ) dups
            """
            dup_count = conn.execute(dup_query).fetchone()[0]

            # Count total affected rows
            affected_query = f"""
                SELECT COALESCE(SUM(cnt - 1), 0) FROM (
                    SELECT {key_columns}, COUNT(*) as cnt
                    FROM {table}
                    GROUP BY {key_columns}
                    HAVING COUNT(*) > 1
                ) dups
            """
            affected_rows = conn.execute(affected_query).fetchone()[0]

            results["duplicates"][table] = {
                "duplicate_keys": dup_count,
                "excess_rows": int(affected_rows),
                "key_columns": key_columns,
            }

            if dup_count > 0:
                results["warnings"].append(
                    f"{table}: {dup_count:,} duplicate keys ({affected_rows:,} excess rows)"
                )
                results["status"] = "WARNING"

            if verbose:
                status = "" if dup_count == 0 else ""
                print(f"{status} {table}: {dup_count:,} duplicate keys ({affected_rows:,} excess rows)")

        except Exception as e:
            results["duplicates"][table] = {"error": str(e)}
            if verbose:
                print(f" {table}: ERROR - {e}")

    return results


def detect_statistical_anomalies(conn, verbose: bool = False) -> Dict[str, Any]:
    """
    Detect statistical anomalies (month-over-month count variations).

    Args:
        conn: Database connection.
        verbose: Print detailed output.

    Returns:
        Dictionary with anomaly detection results.
    """
    results = {
        "monthly_trends": [],
        "anomalies": [],
        "status": "PASS",
    }

    try:
        # Get monthly event counts for the last 24 months
        monthly = conn.execute("""
            SELECT
                EXTRACT(YEAR FROM date_received)::INTEGER as year,
                EXTRACT(MONTH FROM date_received)::INTEGER as month,
                COUNT(*) as count
            FROM master_events
            WHERE date_received IS NOT NULL
              AND date_received >= CURRENT_DATE - INTERVAL '24 months'
            GROUP BY 1, 2
            ORDER BY 1, 2
        """).fetchall()

        if monthly:
            counts = [row[2] for row in monthly]
            avg_count = sum(counts) / len(counts)

            for i, row in enumerate(monthly):
                year, month, count = row
                results["monthly_trends"].append({
                    "year": year,
                    "month": month,
                    "count": count,
                })

                # Flag if count is >50% different from average
                deviation = abs(count - avg_count) / avg_count if avg_count > 0 else 0
                if deviation > 0.5:
                    results["anomalies"].append({
                        "period": f"{year}-{month:02d}",
                        "count": count,
                        "average": round(avg_count),
                        "deviation_pct": round(deviation * 100, 1),
                    })

            if results["anomalies"]:
                results["status"] = "WARNING"

    except Exception as e:
        results["status"] = "ERROR"
        results["error"] = str(e)

    if verbose:
        print("\n" + "=" * 60)
        print("STATISTICAL ANOMALY DETECTION")
        print("=" * 60)

        if results["anomalies"]:
            print(f"Found {len(results['anomalies'])} anomalous months:")
            for a in results["anomalies"][:5]:
                print(f"  {a['period']}: {a['count']:,} (avg: {a['average']:,}, deviation: {a['deviation_pct']:.1f}%)")
        else:
            print("No statistical anomalies detected")

    return results


def run_full_audit(data_dir: Path, db_path: Path, verbose: bool = True) -> Dict[str, Any]:
    """
    Run complete data audit.

    Args:
        data_dir: Directory containing data files.
        db_path: Path to database.
        verbose: Print detailed output.

    Returns:
        Complete audit results dictionary.
    """
    results = {
        "timestamp": datetime.now().isoformat(),
        "data_dir": str(data_dir),
        "database_path": str(db_path),
        "overall_status": "PASS",
        "sections": {},
        "summary": {
            "total_checks": 0,
            "passed": 0,
            "warnings": 0,
            "failures": 0,
        },
    }

    if verbose:
        print("\n" + "=" * 60)
        print("MAUDE DATA QUALITY AUDIT")
        print("=" * 60)
        print(f"Timestamp: {results['timestamp']}")
        print(f"Data Directory: {data_dir}")
        print(f"Database: {db_path}")

    # Check downloaded files
    results["sections"]["downloads"] = check_downloaded_files(data_dir, verbose)

    # Database checks
    with get_connection(db_path, read_only=True) as conn:
        results["sections"]["ingestion_log"] = check_ingestion_log(conn, verbose)
        results["sections"]["table_counts"] = check_table_counts(conn, verbose)
        results["sections"]["patient_problems"] = check_patient_problems(conn, verbose)
        results["sections"]["referential_integrity"] = check_referential_integrity(conn, verbose)
        results["sections"]["data_freshness"] = check_data_freshness(conn, verbose)
        results["sections"]["duplicates"] = check_duplicates(conn, verbose)
        results["sections"]["statistical_anomalies"] = detect_statistical_anomalies(conn, verbose)

    # Compute summary
    for section_name, section_data in results["sections"].items():
        results["summary"]["total_checks"] += 1
        status = section_data.get("status", "UNKNOWN")

        if status == "PASS":
            results["summary"]["passed"] += 1
        elif status in ["WARNING", "STALE", "CHECK"]:
            results["summary"]["warnings"] += 1
            if results["overall_status"] == "PASS":
                results["overall_status"] = "WARNING"
        else:  # FAIL, ERROR
            results["summary"]["failures"] += 1
            results["overall_status"] = "FAIL"

    # Print summary
    if verbose:
        print("\n" + "=" * 60)
        print("AUDIT SUMMARY")
        print("=" * 60)
        print(f"Total checks: {results['summary']['total_checks']}")
        print(f"  Passed: {results['summary']['passed']}")
        print(f"  Warnings: {results['summary']['warnings']}")
        print(f"  Failures: {results['summary']['failures']}")
        print(f"\nOverall Status: {results['overall_status']}")

        # Collect all warnings
        all_warnings = []
        for section in results["sections"].values():
            if "warnings" in section:
                all_warnings.extend(section["warnings"])

        if all_warnings:
            print("\n WARNINGS:")
            for w in all_warnings[:20]:
                print(f"  - {w}")

        print("\n" + "=" * 60)

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="MAUDE Data Quality Audit",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--json", action="store_true", help="Output results as JSON")
    parser.add_argument("--output", "-o", type=Path, help="Save results to file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--data-dir", type=Path, default=config.data.raw_path)
    parser.add_argument("--db", type=Path, default=config.database.path)

    args = parser.parse_args()

    # Run audit
    results = run_full_audit(
        data_dir=args.data_dir,
        db_path=args.db,
        verbose=not args.json or args.verbose,
    )

    # Output results
    if args.json:
        print(json.dumps(results, indent=2, default=str))

    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nResults saved to: {args.output}")

    # Return exit code based on status
    if results["overall_status"] == "FAIL":
        return 1
    elif results["overall_status"] == "WARNING":
        return 0  # Warnings don't cause failure
    return 0


if __name__ == "__main__":
    sys.exit(main())
