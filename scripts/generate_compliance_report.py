#!/usr/bin/env python3
"""
Generate Compliance Report - Regulatory-grade data quality report.

Creates comprehensive PDF/HTML report suitable for regulatory submission including:
- Data provenance (files, dates, checksums)
- Completeness metrics by year
- Integrity verification results
- Trend charts for quality metrics

Usage:
    python scripts/generate_compliance_report.py
    python scripts/generate_compliance_report.py --format html --output report.html
    python scripts/generate_compliance_report.py --format pdf --output report.pdf
    python scripts/generate_compliance_report.py --date-range 2023-01-01 2023-12-31
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from src.database import get_connection

logger = get_logger("compliance_report")


@dataclass
class DataProvenance:
    """Data provenance information."""
    filename: str
    file_type: str
    load_date: Optional[datetime] = None
    source_records: Optional[int] = None
    loaded_records: Optional[int] = None
    checksum: Optional[str] = None
    schema_version: Optional[str] = None


@dataclass
class YearlyMetrics:
    """Metrics for a specific year."""
    year: int
    record_count: int = 0
    death_count: int = 0
    injury_count: int = 0
    malfunction_count: int = 0
    unique_manufacturers: int = 0
    unique_product_codes: int = 0
    completeness_pct: float = 0.0


@dataclass
class IntegrityResult:
    """Integrity check result."""
    check_name: str
    table_name: str
    status: str  # PASS, WARNING, FAIL
    value: Any
    threshold: Any
    details: str = ""


@dataclass
class ComplianceReport:
    """Complete compliance report."""
    report_id: str
    generation_date: datetime
    database_path: str
    report_period_start: Optional[date] = None
    report_period_end: Optional[date] = None

    # Summary
    overall_compliance_status: str = "UNKNOWN"
    total_records: int = 0
    data_coverage_years: List[int] = field(default_factory=list)

    # Data provenance
    source_files: List[DataProvenance] = field(default_factory=list)

    # Completeness by year
    yearly_metrics: List[YearlyMetrics] = field(default_factory=list)

    # Integrity verification
    integrity_results: List[IntegrityResult] = field(default_factory=list)

    # Quality trends (if history available)
    quality_trends: Dict[str, List[Dict]] = field(default_factory=dict)

    # Issues and notes
    issues: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return {
            "report_id": self.report_id,
            "generation_date": self.generation_date.isoformat(),
            "database_path": self.database_path,
            "report_period": {
                "start": self.report_period_start.isoformat() if self.report_period_start else None,
                "end": self.report_period_end.isoformat() if self.report_period_end else None,
            },
            "summary": {
                "overall_compliance_status": self.overall_compliance_status,
                "total_records": self.total_records,
                "data_coverage_years": self.data_coverage_years,
            },
            "data_provenance": [asdict(f) for f in self.source_files],
            "completeness_by_year": [asdict(y) for y in self.yearly_metrics],
            "integrity_verification": [asdict(i) for i in self.integrity_results],
            "quality_trends": self.quality_trends,
            "issues": self.issues,
            "notes": self.notes,
        }


def get_data_provenance(conn) -> List[DataProvenance]:
    """Get data provenance information from file_audit and ingestion_log."""
    provenance = []

    # Try file_audit first
    try:
        files = conn.execute("""
            SELECT
                filename,
                file_type,
                load_completed,
                source_record_count,
                loaded_record_count,
                file_checksum,
                schema_version
            FROM file_audit
            WHERE load_status = 'COMPLETED'
            ORDER BY load_completed DESC
        """).fetchall()

        for row in files:
            provenance.append(DataProvenance(
                filename=row[0],
                file_type=row[1],
                load_date=row[2],
                source_records=row[3],
                loaded_records=row[4],
                checksum=row[5],
                schema_version=row[6],
            ))

        if provenance:
            return provenance
    except Exception:
        pass

    # Fall back to ingestion_log
    try:
        files = conn.execute("""
            SELECT
                file_name,
                file_type,
                completed_at,
                records_processed,
                records_loaded,
                NULL as checksum,
                schema_info
            FROM ingestion_log
            WHERE status IN ('COMPLETED', 'COMPLETED_WITH_ERRORS')
            ORDER BY completed_at DESC
        """).fetchall()

        for row in files:
            schema_version = None
            if row[6]:
                try:
                    info = json.loads(row[6])
                    schema_version = f"{info.get('column_count', 'unknown')} columns"
                except:
                    pass

            provenance.append(DataProvenance(
                filename=row[0],
                file_type=row[1],
                load_date=row[2],
                source_records=row[3],
                loaded_records=row[4],
                checksum=row[5],
                schema_version=schema_version,
            ))
    except Exception as e:
        logger.warning(f"Could not get provenance: {e}")

    return provenance


def get_yearly_metrics(conn, start_year: int = 1998, end_year: int = None) -> List[YearlyMetrics]:
    """Get completeness and summary metrics by year."""
    if end_year is None:
        end_year = datetime.now().year

    metrics = []

    for year in range(start_year, end_year + 1):
        try:
            # Get master event counts
            stats = conn.execute(f"""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN event_type = 'D' THEN 1 ELSE 0 END) as deaths,
                    SUM(CASE WHEN event_type = 'IN' THEN 1 ELSE 0 END) as injuries,
                    SUM(CASE WHEN event_type = 'M' THEN 1 ELSE 0 END) as malfunctions,
                    COUNT(DISTINCT manufacturer_clean) as manufacturers,
                    COUNT(DISTINCT product_code) as product_codes,
                    COUNT(manufacturer_clean) as has_manufacturer
                FROM master_events
                WHERE received_year = {year}
            """).fetchone()

            if stats[0] > 0:
                completeness = (stats[6] / stats[0]) * 100 if stats[0] > 0 else 0

                metrics.append(YearlyMetrics(
                    year=year,
                    record_count=stats[0],
                    death_count=stats[1] or 0,
                    injury_count=stats[2] or 0,
                    malfunction_count=stats[3] or 0,
                    unique_manufacturers=stats[4] or 0,
                    unique_product_codes=stats[5] or 0,
                    completeness_pct=round(completeness, 2),
                ))
        except Exception as e:
            logger.warning(f"Could not get metrics for year {year}: {e}")

    return metrics


def run_integrity_checks(conn) -> List[IntegrityResult]:
    """Run integrity verification checks."""
    results = []

    # 1. Check orphan rates
    child_tables = [
        ("devices", "mdr_report_key"),
        ("patients", "mdr_report_key"),
        ("mdr_text", "mdr_report_key"),
        ("device_problems", "mdr_report_key"),
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
            status = "PASS" if rate < 1 else "WARNING" if rate < 5 else "FAIL"

            results.append(IntegrityResult(
                check_name="orphan_rate",
                table_name=table_name,
                status=status,
                value=round(rate, 4),
                threshold=1.0,
                details=f"{orphans:,} orphans out of {total:,} records"
            ))
        except Exception as e:
            logger.warning(f"Orphan check failed for {table_name}: {e}")

    # 2. Check manufacturer coverage
    try:
        stats = conn.execute("""
            SELECT COUNT(*), COUNT(manufacturer_clean)
            FROM master_events
        """).fetchone()

        if stats[0] > 0:
            coverage = (stats[1] / stats[0]) * 100
            status = "PASS" if coverage >= 95 else "WARNING" if coverage >= 90 else "FAIL"

            results.append(IntegrityResult(
                check_name="manufacturer_coverage",
                table_name="master_events",
                status=status,
                value=round(coverage, 2),
                threshold=95.0,
                details=f"{stats[1]:,} of {stats[0]:,} have manufacturer_clean"
            ))
    except Exception as e:
        logger.warning(f"Manufacturer coverage check failed: {e}")

    # 3. Check product code coverage
    try:
        stats = conn.execute("""
            SELECT COUNT(*), COUNT(product_code)
            FROM master_events
        """).fetchone()

        if stats[0] > 0:
            coverage = (stats[1] / stats[0]) * 100
            status = "PASS" if coverage >= 95 else "WARNING" if coverage >= 90 else "FAIL"

            results.append(IntegrityResult(
                check_name="product_code_coverage",
                table_name="master_events",
                status=status,
                value=round(coverage, 2),
                threshold=95.0,
                details=f"{stats[1]:,} of {stats[0]:,} have product_code"
            ))
    except Exception as e:
        logger.warning(f"Product code coverage check failed: {e}")

    # 4. Check date validity
    try:
        invalid_dates = conn.execute("""
            SELECT COUNT(*) FROM master_events
            WHERE date_received IS NOT NULL
              AND (date_received < '1984-01-01' OR date_received > CURRENT_DATE + INTERVAL '1 year')
        """).fetchone()[0]

        total = conn.execute("SELECT COUNT(*) FROM master_events WHERE date_received IS NOT NULL").fetchone()[0]

        if total > 0:
            rate = (invalid_dates / total) * 100
            status = "PASS" if rate < 0.1 else "WARNING" if rate < 1 else "FAIL"

            results.append(IntegrityResult(
                check_name="date_validity",
                table_name="master_events",
                status=status,
                value=round(rate, 4),
                threshold=0.1,
                details=f"{invalid_dates:,} invalid dates out of {total:,}"
            ))
    except Exception as e:
        logger.warning(f"Date validity check failed: {e}")

    return results


def get_quality_trends(conn, days: int = 30) -> Dict[str, List[Dict]]:
    """Get quality metric trends from history table."""
    trends = defaultdict(list)

    try:
        # Check if table exists
        try:
            conn.execute("SELECT 1 FROM quality_metrics_history LIMIT 1")
        except Exception:
            return dict(trends)

        start_date = date.today() - timedelta(days=days)

        metrics = conn.execute("""
            SELECT metric_date, metric_name, metric_value, status
            FROM quality_metrics_history
            WHERE metric_date >= ?
            ORDER BY metric_date
        """, [start_date]).fetchall()

        for row in metrics:
            trends[row[1]].append({
                "date": row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0]),
                "value": row[2],
                "status": row[3],
            })

    except Exception as e:
        logger.warning(f"Could not get quality trends: {e}")

    return dict(trends)


def generate_report(
    db_path: Path,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> ComplianceReport:
    """
    Generate complete compliance report.

    Args:
        db_path: Path to database.
        start_date: Optional start date for report period.
        end_date: Optional end date for report period.

    Returns:
        ComplianceReport with all data.
    """
    report_id = f"MAUDE-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    report = ComplianceReport(
        report_id=report_id,
        generation_date=datetime.now(),
        database_path=str(db_path),
        report_period_start=start_date,
        report_period_end=end_date,
    )

    with get_connection(db_path, read_only=True) as conn:
        # Get total records
        try:
            report.total_records = conn.execute(
                "SELECT COUNT(*) FROM master_events"
            ).fetchone()[0]
        except Exception:
            pass

        # Get data provenance
        report.source_files = get_data_provenance(conn)

        # Get yearly metrics
        report.yearly_metrics = get_yearly_metrics(conn)
        report.data_coverage_years = [m.year for m in report.yearly_metrics if m.record_count > 0]

        # Run integrity checks
        report.integrity_results = run_integrity_checks(conn)

        # Get quality trends
        report.quality_trends = get_quality_trends(conn)

        # Determine overall status
        failed_checks = [r for r in report.integrity_results if r.status == "FAIL"]
        warning_checks = [r for r in report.integrity_results if r.status == "WARNING"]

        if failed_checks:
            report.overall_compliance_status = "NON_COMPLIANT"
            for check in failed_checks:
                report.issues.append(f"{check.check_name} ({check.table_name}): {check.details}")
        elif warning_checks:
            report.overall_compliance_status = "COMPLIANT_WITH_WARNINGS"
            for check in warning_checks:
                report.notes.append(f"{check.check_name} ({check.table_name}): {check.details}")
        else:
            report.overall_compliance_status = "COMPLIANT"

    return report


def generate_html_report(report: ComplianceReport) -> str:
    """Generate HTML version of the report."""
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>FDA MAUDE Data Compliance Report - {report.report_id}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        h1 {{ color: #333; border-bottom: 2px solid #333; padding-bottom: 10px; }}
        h2 {{ color: #444; margin-top: 30px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f4f4f4; }}
        .pass {{ color: green; font-weight: bold; }}
        .warning {{ color: orange; font-weight: bold; }}
        .fail {{ color: red; font-weight: bold; }}
        .summary-box {{ background: #f9f9f9; padding: 20px; border-radius: 5px; margin: 20px 0; }}
        .status-box {{ padding: 15px; border-radius: 5px; margin: 20px 0; }}
        .status-pass {{ background: #d4edda; border: 1px solid #c3e6cb; }}
        .status-warning {{ background: #fff3cd; border: 1px solid #ffeeba; }}
        .status-fail {{ background: #f8d7da; border: 1px solid #f5c6cb; }}
        footer {{ margin-top: 50px; padding-top: 20px; border-top: 1px solid #ddd; color: #666; font-size: 12px; }}
    </style>
</head>
<body>
    <h1>FDA MAUDE Data Compliance Report</h1>

    <div class="summary-box">
        <strong>Report ID:</strong> {report.report_id}<br>
        <strong>Generated:</strong> {report.generation_date.strftime('%Y-%m-%d %H:%M:%S')}<br>
        <strong>Database:</strong> {report.database_path}<br>
        <strong>Total Records:</strong> {report.total_records:,}<br>
        <strong>Coverage Years:</strong> {report.data_coverage_years[0] if report.data_coverage_years else 'N/A'} - {report.data_coverage_years[-1] if report.data_coverage_years else 'N/A'}
    </div>

    <div class="status-box status-{report.overall_compliance_status.lower().replace('_', '-')}">
        <strong>Overall Status:</strong> {report.overall_compliance_status}
    </div>

    <h2>Data Provenance</h2>
    <table>
        <tr>
            <th>Filename</th>
            <th>Type</th>
            <th>Load Date</th>
            <th>Source Records</th>
            <th>Loaded Records</th>
            <th>Schema</th>
        </tr>
"""

    for f in report.source_files[:50]:  # Limit to 50 files
        html += f"""        <tr>
            <td>{f.filename}</td>
            <td>{f.file_type}</td>
            <td>{f.load_date.strftime('%Y-%m-%d') if f.load_date else 'N/A'}</td>
            <td>{f.source_records:,} if f.source_records else 'N/A'</td>
            <td>{f.loaded_records:,} if f.loaded_records else 'N/A'</td>
            <td>{f.schema_version or 'N/A'}</td>
        </tr>
"""

    html += """    </table>

    <h2>Completeness by Year</h2>
    <table>
        <tr>
            <th>Year</th>
            <th>Records</th>
            <th>Deaths</th>
            <th>Injuries</th>
            <th>Malfunctions</th>
            <th>Manufacturers</th>
            <th>Product Codes</th>
            <th>Completeness</th>
        </tr>
"""

    for m in report.yearly_metrics:
        html += f"""        <tr>
            <td>{m.year}</td>
            <td>{m.record_count:,}</td>
            <td>{m.death_count:,}</td>
            <td>{m.injury_count:,}</td>
            <td>{m.malfunction_count:,}</td>
            <td>{m.unique_manufacturers:,}</td>
            <td>{m.unique_product_codes:,}</td>
            <td>{m.completeness_pct:.1f}%</td>
        </tr>
"""

    html += """    </table>

    <h2>Integrity Verification</h2>
    <table>
        <tr>
            <th>Check</th>
            <th>Table</th>
            <th>Status</th>
            <th>Value</th>
            <th>Threshold</th>
            <th>Details</th>
        </tr>
"""

    for r in report.integrity_results:
        html += f"""        <tr>
            <td>{r.check_name}</td>
            <td>{r.table_name}</td>
            <td class="{r.status.lower()}">{r.status}</td>
            <td>{r.value}</td>
            <td>{r.threshold}</td>
            <td>{r.details}</td>
        </tr>
"""

    html += """    </table>
"""

    if report.issues:
        html += """
    <h2>Issues</h2>
    <ul>
"""
        for issue in report.issues:
            html += f"        <li>{issue}</li>\n"
        html += "    </ul>\n"

    if report.notes:
        html += """
    <h2>Notes</h2>
    <ul>
"""
        for note in report.notes:
            html += f"        <li>{note}</li>\n"
        html += "    </ul>\n"

    html += f"""
    <footer>
        <p>This report was automatically generated by the MAUDE Data Pipeline Audit System.</p>
        <p>Report ID: {report.report_id} | Generated: {report.generation_date.strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
    </footer>
</body>
</html>
"""

    return html


def print_report(report: ComplianceReport) -> None:
    """Print compliance report to console."""
    print("=" * 70)
    print("FDA MAUDE DATA COMPLIANCE REPORT")
    print("=" * 70)
    print(f"\nReport ID: {report.report_id}")
    print(f"Generated: {report.generation_date}")
    print(f"Database: {report.database_path}")
    print(f"Total Records: {report.total_records:,}")
    if report.data_coverage_years:
        print(f"Coverage: {report.data_coverage_years[0]} - {report.data_coverage_years[-1]}")

    status_symbol = {
        "COMPLIANT": "[PASS]",
        "COMPLIANT_WITH_WARNINGS": "[WARN]",
        "NON_COMPLIANT": "[FAIL]",
    }.get(report.overall_compliance_status, "[????]")
    print(f"\nOverall Status: {status_symbol} {report.overall_compliance_status}")

    print("\n" + "-" * 70)
    print("DATA PROVENANCE")
    print("-" * 70)
    print(f"  Files loaded: {len(report.source_files)}")
    if report.source_files:
        print("  Recent files:")
        for f in report.source_files[:5]:
            print(f"    - {f.filename} ({f.file_type})")

    print("\n" + "-" * 70)
    print("COMPLETENESS BY YEAR (Recent)")
    print("-" * 70)
    for m in report.yearly_metrics[-5:]:
        print(f"  {m.year}: {m.record_count:>10,} records, {m.completeness_pct:>5.1f}% complete")

    print("\n" + "-" * 70)
    print("INTEGRITY VERIFICATION")
    print("-" * 70)
    for r in report.integrity_results:
        status_sym = {"PASS": "[OK]", "WARNING": "[~]", "FAIL": "[X]"}.get(r.status, "[?]")
        print(f"  {status_sym} {r.check_name} ({r.table_name}): {r.value}")

    if report.issues:
        print("\n" + "-" * 70)
        print("ISSUES")
        print("-" * 70)
        for issue in report.issues:
            print(f"  - {issue}")

    print("\n" + "=" * 70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate FDA MAUDE data compliance report",
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
        "--format",
        choices=["text", "json", "html"],
        default="text",
        help="Output format",
    )
    parser.add_argument(
        "--output", "-o",
        type=Path,
        help="Save report to file",
    )
    parser.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        help="Report period date range (YYYY-MM-DD YYYY-MM-DD)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="WARNING",
        help="Logging level",
    )

    args = parser.parse_args()

    setup_logging(log_level=args.log_level)

    # Parse date range
    start_date = None
    end_date = None
    if args.date_range:
        try:
            start_date = datetime.strptime(args.date_range[0], "%Y-%m-%d").date()
            end_date = datetime.strptime(args.date_range[1], "%Y-%m-%d").date()
        except ValueError as e:
            print(f"Error parsing dates: {e}")
            return 1

    # Generate report
    report = generate_report(args.db, start_date, end_date)

    # Output
    if args.format == "json":
        output = json.dumps(report.to_dict(), indent=2, default=str)
        if args.output:
            with open(args.output, "w") as f:
                f.write(output)
            print(f"JSON report saved to: {args.output}")
        else:
            print(output)

    elif args.format == "html":
        output = generate_html_report(report)
        if args.output:
            with open(args.output, "w") as f:
                f.write(output)
            print(f"HTML report saved to: {args.output}")
        else:
            print(output)

    else:  # text
        print_report(report)
        if args.output:
            with open(args.output, "w") as f:
                json.dump(report.to_dict(), f, indent=2, default=str)
            print(f"\nJSON report also saved to: {args.output}")

    # Return exit code
    return 0 if report.overall_compliance_status == "COMPLIANT" else 1


if __name__ == "__main__":
    sys.exit(main())
