#!/usr/bin/env python3
"""
Audit Data Completeness - Track file downloads, loads, and coverage gaps.

This script provides a comprehensive completeness dashboard showing:
- Files downloaded vs loaded
- Record counts by file type and year
- Missing date ranges (gaps in coverage)
- Files needing re-processing

Usage:
    python scripts/audit_completeness.py
    python scripts/audit_completeness.py --json --output completeness_report.json
    python scripts/audit_completeness.py --file-type device --year 2024
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime, date
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict
import re

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from src.database import get_connection

logger = get_logger("audit_completeness")


@dataclass
class FileStatus:
    """Status of a single file."""
    filename: str
    file_type: str
    year: Optional[int]
    status: str  # 'DOWNLOADED', 'LOADED', 'PARTIAL', 'MISSING', 'ERROR'
    source_records: Optional[int] = None
    loaded_records: Optional[int] = None
    variance_pct: Optional[float] = None
    last_loaded: Optional[datetime] = None
    error_message: Optional[str] = None


@dataclass
class FileTypeSummary:
    """Summary for a file type."""
    file_type: str
    total_files: int = 0
    files_loaded: int = 0
    files_missing: int = 0
    files_with_errors: int = 0
    total_source_records: int = 0
    total_loaded_records: int = 0
    records_by_year: Dict[int, int] = field(default_factory=dict)
    coverage_gaps: List[Tuple[int, int]] = field(default_factory=list)  # (start_year, end_year)
    files: List[FileStatus] = field(default_factory=list)


@dataclass
class CompletenessReport:
    """Complete data completeness report."""
    timestamp: datetime
    database_path: str
    data_directory: str
    overall_status: str = "UNKNOWN"
    total_files_expected: int = 0
    total_files_loaded: int = 0
    total_records_loaded: int = 0
    file_type_summaries: Dict[str, FileTypeSummary] = field(default_factory=dict)
    coverage_issues: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON export."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "database_path": self.database_path,
            "data_directory": self.data_directory,
            "overall_status": self.overall_status,
            "summary": {
                "total_files_expected": self.total_files_expected,
                "total_files_loaded": self.total_files_loaded,
                "total_records_loaded": self.total_records_loaded,
            },
            "file_type_summaries": {
                ft: {
                    "file_type": s.file_type,
                    "total_files": s.total_files,
                    "files_loaded": s.files_loaded,
                    "files_missing": s.files_missing,
                    "total_source_records": s.total_source_records,
                    "total_loaded_records": s.total_loaded_records,
                    "records_by_year": s.records_by_year,
                    "coverage_gaps": s.coverage_gaps,
                }
                for ft, s in self.file_type_summaries.items()
            },
            "coverage_issues": self.coverage_issues,
            "recommendations": self.recommendations,
        }


# Expected file patterns by type
EXPECTED_FILE_PATTERNS = {
    "master": [
        ("mdrfoithru{year}.txt", range(1997, 2024)),  # Historical through 2023
        ("mdrfoi{year}.txt", range(2024, 2026)),      # 2024 onwards
        ("mdrfoi.txt", None),                          # Current
        ("mdrfoiAdd.txt", None),                       # Weekly adds
        ("mdrfoiChange.txt", None),                    # Weekly changes
    ],
    "device": [
        ("foidevthru{year}.txt", range(1997, 2020)),  # Historical through 2019
        ("foidev{year}.txt", range(2019, 2020)),      # 2019
        ("device{year}.txt", range(2020, 2026)),      # 2020+ format
        ("foidev.txt", None),                          # Current
    ],
    "patient": [
        ("patientthru{year}.txt", range(1997, 2026)),
        ("patient.txt", None),
    ],
    "text": [
        ("foitextthru{year}.txt", range(1997, 2026)),
        ("foitext.txt", None),
    ],
    "problem": [
        ("foidevproblem.txt", None),
    ],
}


def extract_year_from_filename(filename: str) -> Optional[int]:
    """Extract year from filename."""
    # Pattern: file{year}.txt or filethru{year}.txt
    match = re.search(r'(19|20)\d{2}', filename.lower())
    if match:
        return int(match.group())
    return None


def get_downloaded_files(data_dir: Path) -> Dict[str, List[Path]]:
    """Get list of downloaded files by type."""
    files_by_type = defaultdict(list)

    file_patterns = {
        "master": ["mdrfoi*.txt"],
        "device": ["foidev*.txt", "device*.txt"],
        "patient": ["patient*.txt"],
        "text": ["foitext*.txt"],
        "problem": ["foidevproblem*.txt"],
        "patient_problem": ["patientproblemcode*.txt"],
    }

    for file_type, patterns in file_patterns.items():
        for pattern in patterns:
            for filepath in data_dir.glob(pattern):
                # Exclude problem files from device
                if file_type == "device" and "problem" in filepath.name.lower():
                    continue
                files_by_type[file_type].append(filepath)

    return dict(files_by_type)


def get_loaded_files_from_db(conn) -> Dict[str, Dict]:
    """Get information about loaded files from ingestion log."""
    loaded_files = {}

    try:
        # Check ingestion_log table
        results = conn.execute("""
            SELECT
                file_name,
                file_type,
                records_processed,
                records_loaded,
                completed_at,
                status,
                error_message
            FROM ingestion_log
            WHERE status IN ('COMPLETED', 'COMPLETED_WITH_ERRORS')
            ORDER BY completed_at DESC
        """).fetchall()

        for row in results:
            filename = row[0]
            if filename not in loaded_files:
                loaded_files[filename] = {
                    "file_type": row[1],
                    "records_processed": row[2],
                    "records_loaded": row[3],
                    "completed_at": row[4],
                    "status": row[5],
                    "error_message": row[6],
                }
    except Exception as e:
        logger.warning(f"Could not query ingestion_log: {e}")

    # Also check file_audit table if it exists
    try:
        results = conn.execute("""
            SELECT
                filename,
                file_type,
                source_record_count,
                loaded_record_count,
                load_completed,
                load_status,
                error_message
            FROM file_audit
            WHERE load_status IN ('COMPLETED', 'PARTIAL')
        """).fetchall()

        for row in results:
            filename = row[0]
            if filename not in loaded_files:
                loaded_files[filename] = {
                    "file_type": row[1],
                    "records_processed": row[2],
                    "records_loaded": row[3],
                    "completed_at": row[4],
                    "status": row[5],
                    "error_message": row[6],
                }
    except Exception:
        pass  # file_audit may not exist yet

    return loaded_files


def get_records_by_year_from_db(conn) -> Dict[str, Dict[int, int]]:
    """Get record counts by year from database tables."""
    records_by_type_year = defaultdict(lambda: defaultdict(int))

    table_queries = [
        ("master", "master_events", "received_year"),
        ("device", "devices", "EXTRACT(YEAR FROM date_received)"),
        ("patient", "patients", "EXTRACT(YEAR FROM date_received)"),
        ("text", "mdr_text", "EXTRACT(YEAR FROM date_report)"),
    ]

    for file_type, table_name, year_col in table_queries:
        try:
            results = conn.execute(f"""
                SELECT
                    {year_col} as year,
                    COUNT(*) as cnt
                FROM {table_name}
                WHERE {year_col} IS NOT NULL
                GROUP BY 1
                ORDER BY 1
            """).fetchall()

            for row in results:
                if row[0]:
                    year = int(row[0])
                    records_by_type_year[file_type][year] = row[1]
        except Exception as e:
            logger.warning(f"Could not get year counts for {table_name}: {e}")

    return dict(records_by_type_year)


def find_coverage_gaps(records_by_year: Dict[int, int], min_year: int = 1998, max_year: int = None) -> List[Tuple[int, int]]:
    """Find gaps in year coverage."""
    if max_year is None:
        max_year = datetime.now().year

    gaps = []
    years_with_data = set(records_by_year.keys())

    gap_start = None
    for year in range(min_year, max_year + 1):
        if year not in years_with_data or records_by_year.get(year, 0) == 0:
            if gap_start is None:
                gap_start = year
        else:
            if gap_start is not None:
                gaps.append((gap_start, year - 1))
                gap_start = None

    if gap_start is not None:
        gaps.append((gap_start, max_year))

    return gaps


def run_completeness_audit(
    db_path: Path,
    data_dir: Path,
    file_types: Optional[List[str]] = None,
    year: Optional[int] = None
) -> CompletenessReport:
    """
    Run complete data completeness audit.

    Args:
        db_path: Path to database.
        data_dir: Path to data directory.
        file_types: Optional list of specific file types to check.
        year: Optional specific year to focus on.

    Returns:
        CompletenessReport with all results.
    """
    report = CompletenessReport(
        timestamp=datetime.now(),
        database_path=str(db_path),
        data_directory=str(data_dir),
    )

    # Get downloaded files
    downloaded_files = get_downloaded_files(data_dir)

    with get_connection(db_path, read_only=True) as conn:
        # Get loaded files info
        loaded_files = get_loaded_files_from_db(conn)

        # Get record counts by year
        records_by_year = get_records_by_year_from_db(conn)

        # Process each file type
        types_to_check = file_types if file_types else list(EXPECTED_FILE_PATTERNS.keys())

        for file_type in types_to_check:
            summary = FileTypeSummary(file_type=file_type)

            # Get downloaded files for this type
            downloaded = downloaded_files.get(file_type, [])

            for filepath in downloaded:
                filename = filepath.name
                file_year = extract_year_from_filename(filename)

                # Skip if year filter is set and doesn't match
                if year and file_year and file_year != year:
                    continue

                summary.total_files += 1
                report.total_files_expected += 1

                # Check if file was loaded
                loaded_info = loaded_files.get(filename)

                if loaded_info:
                    status = FileStatus(
                        filename=filename,
                        file_type=file_type,
                        year=file_year,
                        status="LOADED",
                        source_records=loaded_info.get("records_processed"),
                        loaded_records=loaded_info.get("records_loaded"),
                        last_loaded=loaded_info.get("completed_at"),
                    )

                    # Calculate variance
                    if status.source_records and status.loaded_records:
                        if status.source_records > 0:
                            status.variance_pct = abs(
                                status.source_records - status.loaded_records
                            ) / status.source_records * 100

                    summary.files_loaded += 1
                    summary.total_source_records += status.source_records or 0
                    summary.total_loaded_records += status.loaded_records or 0
                    report.total_files_loaded += 1
                else:
                    status = FileStatus(
                        filename=filename,
                        file_type=file_type,
                        year=file_year,
                        status="MISSING",
                    )
                    summary.files_missing += 1
                    report.coverage_issues.append(
                        f"{file_type}: {filename} downloaded but not loaded"
                    )

                summary.files.append(status)

            # Add record counts by year
            if file_type in records_by_year:
                summary.records_by_year = dict(records_by_year[file_type])
                report.total_records_loaded += sum(summary.records_by_year.values())

            # Find coverage gaps
            if summary.records_by_year:
                summary.coverage_gaps = find_coverage_gaps(summary.records_by_year)
                for gap_start, gap_end in summary.coverage_gaps:
                    if gap_start == gap_end:
                        report.coverage_issues.append(
                            f"{file_type}: No data for year {gap_start}"
                        )
                    else:
                        report.coverage_issues.append(
                            f"{file_type}: No data for years {gap_start}-{gap_end}"
                        )

            report.file_type_summaries[file_type] = summary

    # Generate recommendations
    for file_type, summary in report.file_type_summaries.items():
        if summary.files_missing > 0:
            report.recommendations.append(
                f"Load {summary.files_missing} missing {file_type} file(s)"
            )

        if summary.coverage_gaps:
            years_missing = sum(g[1] - g[0] + 1 for g in summary.coverage_gaps)
            report.recommendations.append(
                f"Download historical {file_type} files for {years_missing} missing year(s)"
            )

    # Determine overall status
    if report.total_files_loaded == report.total_files_expected and not report.coverage_issues:
        report.overall_status = "COMPLETE"
    elif report.total_files_loaded >= report.total_files_expected * 0.9:
        report.overall_status = "MOSTLY_COMPLETE"
    elif report.total_files_loaded > 0:
        report.overall_status = "PARTIAL"
    else:
        report.overall_status = "INCOMPLETE"

    return report


def print_report(report: CompletenessReport) -> None:
    """Print completeness report to console."""
    print("=" * 70)
    print("FDA MAUDE DATA COMPLETENESS AUDIT")
    print("=" * 70)
    print(f"\nTimestamp: {report.timestamp}")
    print(f"Database: {report.database_path}")
    print(f"Data Directory: {report.data_directory}")
    print(f"Overall Status: {report.overall_status}")

    print("\n" + "-" * 70)
    print("SUMMARY")
    print("-" * 70)
    print(f"  Total Files Expected:  {report.total_files_expected:>10,}")
    print(f"  Total Files Loaded:    {report.total_files_loaded:>10,}")
    print(f"  Total Records Loaded:  {report.total_records_loaded:>10,}")

    print("\n" + "-" * 70)
    print("FILE TYPE DETAILS")
    print("-" * 70)

    for file_type, summary in report.file_type_summaries.items():
        load_pct = (summary.files_loaded / summary.total_files * 100) if summary.total_files > 0 else 0

        print(f"\n  {file_type.upper()}")
        print(f"    Files: {summary.files_loaded}/{summary.total_files} loaded ({load_pct:.0f}%)")
        print(f"    Records: {summary.total_loaded_records:,}")

        if summary.records_by_year:
            recent_years = sorted(summary.records_by_year.keys())[-5:]
            print("    Recent years:")
            for yr in recent_years:
                print(f"      {yr}: {summary.records_by_year[yr]:,}")

        if summary.coverage_gaps:
            print(f"    Coverage Gaps: {len(summary.coverage_gaps)}")
            for gap_start, gap_end in summary.coverage_gaps[:3]:
                if gap_start == gap_end:
                    print(f"      - Year {gap_start}")
                else:
                    print(f"      - Years {gap_start}-{gap_end}")

        if summary.files_missing > 0:
            print(f"    Missing files: {summary.files_missing}")

    if report.coverage_issues:
        print("\n" + "-" * 70)
        print("COVERAGE ISSUES")
        print("-" * 70)
        for issue in report.coverage_issues[:10]:
            print(f"  - {issue}")
        if len(report.coverage_issues) > 10:
            print(f"  ... and {len(report.coverage_issues) - 10} more")

    if report.recommendations:
        print("\n" + "-" * 70)
        print("RECOMMENDATIONS")
        print("-" * 70)
        for rec in report.recommendations:
            print(f"  - {rec}")

    print("\n" + "=" * 70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Audit data completeness for FDA MAUDE pipeline",
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
        "--data-dir",
        type=Path,
        default=config.data.raw_path,
        help="Path to data directory",
    )
    parser.add_argument(
        "--file-type",
        action="append",
        dest="file_types",
        help="Specific file type(s) to check",
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Specific year to focus on",
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

    # Run audit
    report = run_completeness_audit(
        args.db,
        args.data_dir,
        args.file_types,
        args.year
    )

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
    return 0 if report.overall_status in ["COMPLETE", "MOSTLY_COMPLETE"] else 1


if __name__ == "__main__":
    sys.exit(main())
