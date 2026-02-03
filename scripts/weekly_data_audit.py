#!/usr/bin/env python3
"""
Weekly Data Audit Script

This script compares physical line counts in source files against database record counts
to detect data integrity issues such as:
- Quote-swallowing during parsing
- Records lost during transformation
- Load failures not properly logged

Run this script weekly as part of the data refresh pipeline to ensure
complete data loading.

Usage:
    python scripts/weekly_data_audit.py
    python scripts/weekly_data_audit.py --data-dir /path/to/raw --db /path/to/maude.duckdb
    python scripts/weekly_data_audit.py --alert-threshold 0.5  # Alert if >0.5% variance
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger
from src.database import get_connection
from src.ingestion.parser import count_physical_lines, MAUDEParser

logger = get_logger("weekly_data_audit")


@dataclass
class FileAuditResult:
    """Result of auditing a single file."""
    filename: str
    file_type: str
    physical_lines: int  # Total lines in file
    valid_data_lines: int  # Lines starting with digit
    orphan_lines: int  # Lines not starting with digit (embedded newlines)
    db_record_count: int  # Records in database for this file
    variance: int  # Difference between expected and actual
    variance_pct: float  # Variance as percentage
    status: str  # OK, WARNING, CRITICAL, ERROR


@dataclass
class AuditSummary:
    """Summary of the entire audit."""
    timestamp: datetime
    total_files: int
    files_ok: int
    files_warning: int
    files_critical: int
    files_error: int
    total_expected_records: int
    total_db_records: int
    overall_variance_pct: float
    critical_issues: List[str]


def get_file_type_mapping() -> Dict[str, Tuple[str, List[str]]]:
    """
    Get mapping of file types to (table_name, file_patterns).

    Returns:
        Dictionary mapping file_type to (table_name, [file_patterns]).
    """
    return {
        "master": ("master_events", ["mdrfoi*.txt"]),
        "device": ("devices", ["foidev*.txt", "device*.txt"]),
        "patient": ("patients", ["patient*.txt"]),
        "text": ("mdr_text", ["foitext*.txt"]),
        "problem": ("device_problems", ["foidevproblem*.txt"]),
    }


def get_db_count_for_file(conn, table_name: str, filename: str) -> int:
    """
    Get the count of records in the database from a specific source file.

    Args:
        conn: Database connection.
        table_name: Name of the table.
        filename: Source filename to match.

    Returns:
        Number of records from this source file.
    """
    try:
        result = conn.execute(f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE source_file LIKE ?
        """, [f"%{filename}%"]).fetchone()
        return result[0] if result else 0
    except Exception as e:
        logger.warning(f"Could not get count for {filename} in {table_name}: {e}")
        return 0


def audit_file(
    filepath: Path,
    file_type: str,
    table_name: str,
    conn,
) -> FileAuditResult:
    """
    Audit a single file for data integrity.

    Args:
        filepath: Path to the source file.
        file_type: Type of file (master, device, etc.).
        table_name: Database table name.
        conn: Database connection.

    Returns:
        FileAuditResult with audit findings.
    """
    # Count physical lines in file
    try:
        physical_lines, valid_data_lines, orphan_lines = count_physical_lines(filepath)
    except Exception as e:
        return FileAuditResult(
            filename=filepath.name,
            file_type=file_type,
            physical_lines=0,
            valid_data_lines=0,
            orphan_lines=0,
            db_record_count=0,
            variance=0,
            variance_pct=0.0,
            status=f"ERROR: {e}",
        )

    # Get database count
    db_count = get_db_count_for_file(conn, table_name, filepath.name)

    # Calculate variance
    # Use valid_data_lines as expected (excludes header and orphan lines)
    expected = valid_data_lines
    variance = abs(db_count - expected)
    variance_pct = (variance / expected * 100) if expected > 0 else 0.0

    # Determine status
    if variance_pct == 0:
        status = "OK"
    elif variance_pct <= 0.1:
        status = "OK"  # Within acceptable threshold
    elif variance_pct <= 1.0:
        status = "WARNING"
    elif variance_pct <= 10.0:
        status = "CRITICAL"
    else:
        status = "CRITICAL - MAJOR DATA LOSS"

    return FileAuditResult(
        filename=filepath.name,
        file_type=file_type,
        physical_lines=physical_lines,
        valid_data_lines=valid_data_lines,
        orphan_lines=orphan_lines,
        db_record_count=db_count,
        variance=variance,
        variance_pct=variance_pct,
        status=status,
    )


def run_audit(
    data_dir: Path,
    db_path: Path,
    alert_threshold_pct: float = 0.1,
) -> AuditSummary:
    """
    Run a complete audit of all source files against database.

    Args:
        data_dir: Directory containing source files.
        db_path: Path to the database.
        alert_threshold_pct: Threshold for alerting (default 0.1%).

    Returns:
        AuditSummary with findings.
    """
    logger.info("=" * 70)
    logger.info("WEEKLY DATA AUDIT")
    logger.info(f"Data directory: {data_dir}")
    logger.info(f"Database: {db_path}")
    logger.info(f"Alert threshold: {alert_threshold_pct}%")
    logger.info("=" * 70)

    file_type_mapping = get_file_type_mapping()
    results: List[FileAuditResult] = []
    critical_issues: List[str] = []

    with get_connection(db_path, read_only=True) as conn:
        for file_type, (table_name, patterns) in file_type_mapping.items():
            logger.info(f"\nAuditing {file_type} files ({table_name})...")

            # Find all matching files
            files = []
            for pattern in patterns:
                files.extend(data_dir.glob(pattern))

            # Exclude problem files from device pattern
            if file_type == "device":
                files = [f for f in files if "problem" not in f.name.lower()]

            files = sorted(set(files))

            if not files:
                logger.warning(f"  No {file_type} files found")
                continue

            for filepath in files:
                result = audit_file(filepath, file_type, table_name, conn)
                results.append(result)

                # Log result
                status_indicator = "✓" if result.status == "OK" else "✗"
                logger.info(
                    f"  {status_indicator} {result.filename}: "
                    f"physical={result.valid_data_lines:,}, "
                    f"db={result.db_record_count:,}, "
                    f"variance={result.variance_pct:.2f}% [{result.status}]"
                )

                # Track critical issues
                if "CRITICAL" in result.status:
                    critical_issues.append(
                        f"{result.filename}: {result.variance:,} records missing "
                        f"({result.variance_pct:.1f}%)"
                    )

    # Calculate summary
    total_expected = sum(r.valid_data_lines for r in results)
    total_db = sum(r.db_record_count for r in results)
    overall_variance_pct = (
        abs(total_db - total_expected) / total_expected * 100
        if total_expected > 0 else 0.0
    )

    summary = AuditSummary(
        timestamp=datetime.now(),
        total_files=len(results),
        files_ok=sum(1 for r in results if r.status == "OK"),
        files_warning=sum(1 for r in results if r.status == "WARNING"),
        files_critical=sum(1 for r in results if "CRITICAL" in r.status),
        files_error=sum(1 for r in results if "ERROR" in r.status),
        total_expected_records=total_expected,
        total_db_records=total_db,
        overall_variance_pct=overall_variance_pct,
        critical_issues=critical_issues,
    )

    # Print summary
    logger.info("\n" + "=" * 70)
    logger.info("AUDIT SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Timestamp: {summary.timestamp}")
    logger.info(f"Total files audited: {summary.total_files}")
    logger.info(f"  OK: {summary.files_ok}")
    logger.info(f"  Warning: {summary.files_warning}")
    logger.info(f"  Critical: {summary.files_critical}")
    logger.info(f"  Error: {summary.files_error}")
    logger.info(f"\nTotal expected records: {summary.total_expected_records:,}")
    logger.info(f"Total database records: {summary.total_db_records:,}")
    logger.info(f"Overall variance: {summary.overall_variance_pct:.2f}%")

    if critical_issues:
        logger.error("\nCRITICAL ISSUES DETECTED:")
        for issue in critical_issues:
            logger.error(f"  - {issue}")

    # Alert if threshold exceeded
    if summary.overall_variance_pct > alert_threshold_pct:
        logger.error(
            f"\n⚠️  ALERT: Overall variance {summary.overall_variance_pct:.2f}% "
            f"exceeds threshold {alert_threshold_pct}%"
        )

    return summary


def save_audit_to_db(conn, summary: AuditSummary) -> None:
    """
    Save audit results to quality_metrics_history table.

    Args:
        conn: Database connection.
        summary: Audit summary to save.
    """
    import json

    try:
        # Save overall variance metric
        conn.execute("""
            INSERT INTO quality_metrics_history
            (metric_date, metric_name, metric_value, threshold, status, details)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (metric_date, metric_name) DO UPDATE SET
                metric_value = EXCLUDED.metric_value,
                status = EXCLUDED.status,
                details = EXCLUDED.details
        """, [
            summary.timestamp.date(),
            "data_completeness_variance_pct",
            summary.overall_variance_pct,
            0.1,  # threshold
            "PASS" if summary.overall_variance_pct <= 0.1 else (
                "WARNING" if summary.overall_variance_pct <= 1.0 else "FAIL"
            ),
            json.dumps({
                "total_files": summary.total_files,
                "files_ok": summary.files_ok,
                "files_critical": summary.files_critical,
                "expected_records": summary.total_expected_records,
                "actual_records": summary.total_db_records,
            }),
        ])

        # Save quote-swallowing check
        quote_swallowing_detected = summary.files_critical > 0
        conn.execute("""
            INSERT INTO quality_metrics_history
            (metric_date, metric_name, metric_value, threshold, status, details)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (metric_date, metric_name) DO UPDATE SET
                metric_value = EXCLUDED.metric_value,
                status = EXCLUDED.status,
                details = EXCLUDED.details
        """, [
            summary.timestamp.date(),
            "quote_swallowing_incidents",
            summary.files_critical,
            0,
            "FAIL" if quote_swallowing_detected else "PASS",
            json.dumps({"critical_files": summary.critical_issues}),
        ])

        logger.info("Audit results saved to quality_metrics_history table")

    except Exception as e:
        logger.warning(f"Could not save audit to database: {e}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Weekly data audit - compare source files to database"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=config.data.raw_path,
        help="Directory containing raw FDA data files",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=config.database.path,
        help="Path to DuckDB database",
    )
    parser.add_argument(
        "--alert-threshold",
        type=float,
        default=0.1,
        help="Alert threshold percentage (default: 0.1)",
    )
    parser.add_argument(
        "--save-to-db",
        action="store_true",
        help="Save audit results to quality_metrics_history table",
    )

    args = parser.parse_args()

    # Verify paths exist
    if not args.data_dir.exists():
        logger.error(f"Data directory not found: {args.data_dir}")
        sys.exit(1)

    if not args.db.exists():
        logger.error(f"Database not found: {args.db}")
        sys.exit(1)

    # Run audit
    summary = run_audit(
        data_dir=args.data_dir,
        db_path=args.db,
        alert_threshold_pct=args.alert_threshold,
    )

    # Save to database if requested
    if args.save_to_db:
        with get_connection(args.db) as conn:
            save_audit_to_db(conn, summary)

    # Exit with error code if critical issues
    if summary.files_critical > 0:
        sys.exit(1)
    elif summary.overall_variance_pct > args.alert_threshold:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
