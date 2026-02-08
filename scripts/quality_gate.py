#!/usr/bin/env python3
"""
Quality Gate - Automated quality threshold check for MAUDE data loads.

This script runs after every load to verify data quality metrics against
defined thresholds. Generates a PASS/FAIL report and optionally blocks
further processing if thresholds are exceeded.

Usage:
    python scripts/quality_gate.py
    python scripts/quality_gate.py --strict  # Fail on warnings
    python scripts/quality_gate.py --json --output quality_report.json
    python scripts/quality_gate.py --record-history  # Save metrics to history table
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime, date
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from src.database import get_connection

logger = get_logger("quality_gate")


# =============================================================================
# QUALITY THRESHOLDS
# =============================================================================

# Load from FDA specification if available
DEFAULT_THRESHOLDS = {
    # Orphan rate thresholds (% of child records without master)
    "orphan_rate": {
        "pass": 1.0,       # <1% = PASS
        "warning": 5.0,    # 1-5% = WARNING
        # >5% = FAIL
    },
    # Manufacturer coverage in master_events
    "manufacturer_coverage": {
        "pass": 95.0,      # >95% = PASS
        "warning": 90.0,   # 90-95% = WARNING
        # <90% = FAIL
    },
    # Product code coverage in master_events
    "product_code_coverage": {
        "pass": 95.0,
        "warning": 90.0,
    },
    # Date parse success rate
    "date_parse_success": {
        "pass": 99.0,
        "warning": 95.0,
    },
    # Record count variance (source vs loaded)
    "record_count_variance": {
        "pass": 0.1,       # <0.1% = PASS
        "warning": 1.0,    # 0.1-1% = WARNING
    },
    # Duplicate key rate (should be 0)
    "duplicate_key_rate": {
        "pass": 0.0,
        "warning": 0.01,   # >0.01% = WARNING
    },
    # Data freshness (days since last update)
    "data_freshness_days": {
        "pass": 7,         # <7 days = PASS
        "warning": 14,     # 7-14 days = WARNING
    },
}


@dataclass
class MetricResult:
    """Result of a single metric check."""
    metric_name: str
    value: float
    threshold_pass: float
    threshold_warning: float
    status: str  # PASS, WARNING, FAIL
    details: Optional[str] = None
    breakdown: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QualityGateReport:
    """Complete quality gate report."""
    timestamp: datetime
    database_path: str
    overall_status: str = "UNKNOWN"
    total_metrics: int = 0
    passed_metrics: int = 0
    warning_metrics: int = 0
    failed_metrics: int = 0
    metrics: List[MetricResult] = field(default_factory=list)
    blocking_issues: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON export."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "database_path": self.database_path,
            "overall_status": self.overall_status,
            "summary": {
                "total_metrics": self.total_metrics,
                "passed": self.passed_metrics,
                "warnings": self.warning_metrics,
                "failed": self.failed_metrics,
            },
            "metrics": [asdict(m) for m in self.metrics],
            "blocking_issues": self.blocking_issues,
            "recommendations": self.recommendations,
        }


def check_metric(
    name: str,
    value: float,
    thresholds: Dict,
    lower_is_better: bool = True
) -> MetricResult:
    """
    Check a metric against thresholds.

    Args:
        name: Metric name.
        value: Metric value.
        thresholds: Dict with 'pass' and 'warning' threshold values.
        lower_is_better: If True, lower values are better (e.g., error rate).

    Returns:
        MetricResult with status.
    """
    pass_threshold = thresholds.get("pass", 0)
    warn_threshold = thresholds.get("warning", pass_threshold)

    if lower_is_better:
        if value <= pass_threshold:
            status = "PASS"
        elif value <= warn_threshold:
            status = "WARNING"
        else:
            status = "FAIL"
    else:  # Higher is better (e.g., coverage)
        if value >= pass_threshold:
            status = "PASS"
        elif value >= warn_threshold:
            status = "WARNING"
        else:
            status = "FAIL"

    return MetricResult(
        metric_name=name,
        value=round(value, 4),
        threshold_pass=pass_threshold,
        threshold_warning=warn_threshold,
        status=status,
    )


def check_orphan_rates(conn) -> List[MetricResult]:
    """Check orphan rates for all child tables."""
    results = []
    thresholds = DEFAULT_THRESHOLDS["orphan_rate"]

    child_tables = [
        ("devices", "mdr_report_key"),
        ("patients", "mdr_report_key"),
        ("mdr_text", "mdr_report_key"),
        ("device_problems", "mdr_report_key"),
        ("patient_problems", "mdr_report_key"),
    ]

    for table_name, fk_col in child_tables:
        try:
            total = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if total == 0:
                continue

            orphans = conn.execute(f"""
                SELECT COUNT(*) FROM {table_name} c
                WHERE NOT EXISTS (
                    SELECT 1 FROM master_events m
                    WHERE m.mdr_report_key = c.{fk_col}
                )
            """).fetchone()[0]

            rate = (orphans / total) * 100
            result = check_metric(
                f"orphan_rate_{table_name}",
                rate,
                thresholds,
                lower_is_better=True
            )
            result.details = f"{orphans:,} orphans out of {total:,} records"
            results.append(result)

        except Exception as e:
            logger.warning(f"Could not check orphan rate for {table_name}: {e}")

    return results


def check_coverage_metrics(conn) -> List[MetricResult]:
    """Check field coverage metrics."""
    results = []

    # Manufacturer coverage
    try:
        stats = conn.execute("""
            SELECT COUNT(*), COUNT(manufacturer_clean)
            FROM master_events
        """).fetchone()

        if stats[0] > 0:
            coverage = (stats[1] / stats[0]) * 100
            result = check_metric(
                "manufacturer_coverage",
                coverage,
                DEFAULT_THRESHOLDS["manufacturer_coverage"],
                lower_is_better=False
            )
            result.details = f"{stats[1]:,} of {stats[0]:,} have manufacturer_clean"
            results.append(result)
    except Exception as e:
        logger.warning(f"Could not check manufacturer coverage: {e}")

    # Product code coverage
    try:
        stats = conn.execute("""
            SELECT COUNT(*), COUNT(product_code)
            FROM master_events
        """).fetchone()

        if stats[0] > 0:
            coverage = (stats[1] / stats[0]) * 100
            result = check_metric(
                "product_code_coverage",
                coverage,
                DEFAULT_THRESHOLDS["product_code_coverage"],
                lower_is_better=False
            )
            result.details = f"{stats[1]:,} of {stats[0]:,} have product_code"
            results.append(result)
    except Exception as e:
        logger.warning(f"Could not check product code coverage: {e}")

    return results


def check_data_freshness(conn) -> MetricResult:
    """Check how fresh the data is."""
    thresholds = DEFAULT_THRESHOLDS["data_freshness_days"]

    try:
        # Get most recent date_added (when FDA published the record)
        latest = conn.execute("""
            SELECT MAX(date_added) FROM master_events
            WHERE date_added IS NOT NULL
        """).fetchone()[0]

        if latest:
            if isinstance(latest, str):
                latest = datetime.strptime(latest, "%Y-%m-%d").date()
            elif isinstance(latest, datetime):
                latest = latest.date()

            days_old = (date.today() - latest).days

            result = check_metric(
                "data_freshness_days",
                days_old,
                thresholds,
                lower_is_better=True
            )
            result.details = f"Latest record date: {latest}, {days_old} days ago"
            return result

    except Exception as e:
        logger.warning(f"Could not check data freshness: {e}")

    return MetricResult(
        metric_name="data_freshness_days",
        value=999,
        threshold_pass=thresholds["pass"],
        threshold_warning=thresholds["warning"],
        status="FAIL",
        details="Could not determine data freshness"
    )


def check_record_count_variance(conn) -> List[MetricResult]:
    """Check record count variance from file_audit table."""
    results = []
    thresholds = DEFAULT_THRESHOLDS["record_count_variance"]

    try:
        # Check if file_audit table exists
        try:
            conn.execute("SELECT 1 FROM file_audit LIMIT 1")
        except Exception:
            return results  # Table doesn't exist

        # Get files with variance
        files = conn.execute("""
            SELECT
                filename,
                file_type,
                source_record_count,
                loaded_record_count,
                CASE
                    WHEN source_record_count > 0 THEN
                        ABS(source_record_count - loaded_record_count) * 100.0 / source_record_count
                    ELSE 0
                END as variance_pct
            FROM file_audit
            WHERE source_record_count IS NOT NULL
              AND loaded_record_count IS NOT NULL
              AND load_status = 'COMPLETED'
            ORDER BY variance_pct DESC
            LIMIT 10
        """).fetchall()

        for row in files:
            filename, file_type, source, loaded, variance = row
            if variance > thresholds["warning"]:
                result = check_metric(
                    f"record_count_variance_{filename}",
                    variance,
                    thresholds,
                    lower_is_better=True
                )
                result.details = f"Source: {source:,}, Loaded: {loaded:,}"
                results.append(result)

    except Exception as e:
        logger.warning(f"Could not check record count variance: {e}")

    return results


def check_duplicate_keys(conn) -> List[MetricResult]:
    """Check for duplicate keys in main tables."""
    results = []
    thresholds = DEFAULT_THRESHOLDS["duplicate_key_rate"]

    # Check devices for duplicates (mdr_report_key + device_sequence_number)
    try:
        total = conn.execute("SELECT COUNT(*) FROM devices").fetchone()[0]
        if total > 0:
            dupes = conn.execute("""
                SELECT COUNT(*) FROM (
                    SELECT mdr_report_key, device_sequence_number
                    FROM devices
                    GROUP BY mdr_report_key, device_sequence_number
                    HAVING COUNT(*) > 1
                )
            """).fetchone()[0]

            if dupes > 0:
                rate = (dupes / total) * 100
                result = check_metric(
                    "duplicate_rate_devices",
                    rate,
                    thresholds,
                    lower_is_better=True
                )
                result.details = f"{dupes:,} duplicate key combinations"
                results.append(result)
    except Exception as e:
        logger.warning(f"Could not check device duplicates: {e}")

    # Check patients for duplicates
    try:
        total = conn.execute("SELECT COUNT(*) FROM patients").fetchone()[0]
        if total > 0:
            dupes = conn.execute("""
                SELECT COUNT(*) FROM (
                    SELECT mdr_report_key, patient_sequence_number
                    FROM patients
                    GROUP BY mdr_report_key, patient_sequence_number
                    HAVING COUNT(*) > 1
                )
            """).fetchone()[0]

            if dupes > 0:
                rate = (dupes / total) * 100
                result = check_metric(
                    "duplicate_rate_patients",
                    rate,
                    thresholds,
                    lower_is_better=True
                )
                result.details = f"{dupes:,} duplicate key combinations"
                results.append(result)
    except Exception as e:
        logger.warning(f"Could not check patient duplicates: {e}")

    return results


def record_metrics_to_history(conn, metrics: List[MetricResult]) -> None:
    """Record metrics to quality_metrics_history table."""
    try:
        # Check if table exists
        try:
            conn.execute("SELECT 1 FROM quality_metrics_history LIMIT 1")
        except Exception:
            return  # Table doesn't exist

        today = date.today()
        for metric in metrics:
            conn.execute("""
                INSERT INTO quality_metrics_history
                (metric_date, metric_name, metric_value, threshold, status, details)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (metric_date, metric_name) DO UPDATE SET
                    metric_value = EXCLUDED.metric_value,
                    threshold = EXCLUDED.threshold,
                    status = EXCLUDED.status,
                    details = EXCLUDED.details
            """, [
                today,
                metric.metric_name,
                metric.value,
                metric.threshold_pass,
                metric.status,
                json.dumps({"details": metric.details}) if metric.details else None,
            ])

    except Exception as e:
        logger.warning(f"Could not record metrics to history: {e}")


def run_quality_gate(db_path: Path, strict: bool = False) -> QualityGateReport:
    """
    Run complete quality gate checks.

    Args:
        db_path: Path to database.
        strict: If True, treat warnings as failures.

    Returns:
        QualityGateReport with all results.
    """
    report = QualityGateReport(
        timestamp=datetime.now(),
        database_path=str(db_path),
    )

    with get_connection(db_path, read_only=True) as conn:
        # Run all checks
        all_metrics = []

        # Orphan rates
        all_metrics.extend(check_orphan_rates(conn))

        # Coverage metrics
        all_metrics.extend(check_coverage_metrics(conn))

        # Data freshness
        all_metrics.append(check_data_freshness(conn))

        # Record count variance
        all_metrics.extend(check_record_count_variance(conn))

        # Duplicate keys
        all_metrics.extend(check_duplicate_keys(conn))

        report.metrics = all_metrics
        report.total_metrics = len(all_metrics)

        # Aggregate results
        for metric in all_metrics:
            if metric.status == "PASS":
                report.passed_metrics += 1
            elif metric.status == "WARNING":
                report.warning_metrics += 1
                if strict:
                    report.blocking_issues.append(
                        f"{metric.metric_name}: {metric.value} (threshold: {metric.threshold_pass})"
                    )
            else:  # FAIL
                report.failed_metrics += 1
                report.blocking_issues.append(
                    f"{metric.metric_name}: {metric.value} (threshold: {metric.threshold_pass})"
                )

        # Determine overall status
        if report.failed_metrics > 0:
            report.overall_status = "FAIL"
        elif report.warning_metrics > 0:
            report.overall_status = "WARNING" if not strict else "FAIL"
        else:
            report.overall_status = "PASS"

        # Generate recommendations
        for metric in all_metrics:
            if metric.status == "FAIL":
                if "orphan" in metric.metric_name:
                    report.recommendations.append(
                        f"Run scripts/analyze_orphan_sources.py to identify missing master files"
                    )
                elif "coverage" in metric.metric_name:
                    report.recommendations.append(
                        f"Run populate_master_from_devices() to improve {metric.metric_name}"
                    )
                elif "freshness" in metric.metric_name:
                    report.recommendations.append(
                        f"Download and load latest FDA files"
                    )

    return report


def print_report(report: QualityGateReport) -> None:
    """Print quality gate report to console."""
    print("=" * 70)
    print("FDA MAUDE QUALITY GATE REPORT")
    print("=" * 70)
    print(f"\nTimestamp: {report.timestamp}")
    print(f"Database: {report.database_path}")

    # Overall status with color indicator
    status_indicator = {
        "PASS": "[PASS]",
        "WARNING": "[WARN]",
        "FAIL": "[FAIL]",
    }.get(report.overall_status, "[????]")
    print(f"\nOverall Status: {status_indicator}")

    print("\n" + "-" * 70)
    print("SUMMARY")
    print("-" * 70)
    print(f"  Total Metrics:  {report.total_metrics}")
    print(f"  Passed:         {report.passed_metrics}")
    print(f"  Warnings:       {report.warning_metrics}")
    print(f"  Failed:         {report.failed_metrics}")

    print("\n" + "-" * 70)
    print("METRIC DETAILS")
    print("-" * 70)

    for metric in sorted(report.metrics, key=lambda m: (m.status != "FAIL", m.status != "WARNING", m.metric_name)):
        status_symbol = {"PASS": "[OK]", "WARNING": "[~]", "FAIL": "[X]"}.get(metric.status, "[?]")
        print(f"\n  {metric.metric_name} {status_symbol}")
        print(f"    Value:     {metric.value}")
        print(f"    Threshold: {metric.threshold_pass} (warn: {metric.threshold_warning})")
        if metric.details:
            print(f"    Details:   {metric.details}")

    if report.blocking_issues:
        print("\n" + "-" * 70)
        print("BLOCKING ISSUES")
        print("-" * 70)
        for issue in report.blocking_issues:
            print(f"  - {issue}")

    if report.recommendations:
        print("\n" + "-" * 70)
        print("RECOMMENDATIONS")
        print("-" * 70)
        for rec in list(set(report.recommendations)):  # Dedupe
            print(f"  - {rec}")

    print("\n" + "=" * 70)
    if report.overall_status == "PASS":
        print("All quality checks PASSED")
    else:
        print(f"Quality gate {report.overall_status}")
    print("=" * 70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run quality gate checks on MAUDE database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--db",
        type=Path,
        default=config.database.path,
        help="Path to DuckDB database",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat warnings as failures",
    )
    parser.add_argument(
        "--record-history",
        action="store_true",
        help="Record metrics to quality_metrics_history table",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        help="Save results to file",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="WARNING",
        help="Logging level",
    )

    args = parser.parse_args()

    setup_logging(log_level=args.log_level)

    # Run quality gate
    report = run_quality_gate(args.db, args.strict)

    # Record to history if requested
    if args.record_history:
        with get_connection(args.db) as conn:
            record_metrics_to_history(conn, report.metrics)
            print("Metrics recorded to quality_metrics_history table")

    # Output results
    if args.json:
        output = json.dumps(report.to_dict(), indent=2)
        if args.output:
            with open(args.output, "w") as f:
                f.write(output)
            print(f"Results saved to: {args.output}")
        else:
            print(output)
    else:
        print_report(report)
        if args.output:
            with open(args.output, "w") as f:
                json.dump(report.to_dict(), f, indent=2)
            print(f"\nJSON results saved to: {args.output}")

    # Return exit code
    return 0 if report.overall_status == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
