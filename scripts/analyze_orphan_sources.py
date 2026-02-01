#!/usr/bin/env python3
"""
Analyze Orphan Sources - Group orphans by source file and identify root causes.

This script extends orphan analysis to:
- Group orphan records by their source_file
- Identify which master event MDR keys are missing
- Recommend specific files to re-load to fix orphans
- Track orphan trends over time

Usage:
    python scripts/analyze_orphan_sources.py
    python scripts/analyze_orphan_sources.py --table devices --limit 100
    python scripts/analyze_orphan_sources.py --json --output orphan_analysis.json
    python scripts/analyze_orphan_sources.py --fix-recommendations
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from src.database import get_connection

logger = get_logger("analyze_orphan_sources")


@dataclass
class OrphanSourceGroup:
    """Group of orphans from a single source file."""
    source_file: str
    orphan_count: int = 0
    sample_mdr_keys: List[str] = field(default_factory=list)
    earliest_date: Optional[str] = None
    latest_date: Optional[str] = None
    years_affected: List[int] = field(default_factory=list)


@dataclass
class TableOrphanAnalysis:
    """Orphan analysis for a single table."""
    table_name: str
    total_records: int = 0
    orphan_count: int = 0
    orphan_pct: float = 0.0
    orphans_by_source: Dict[str, OrphanSourceGroup] = field(default_factory=dict)
    orphans_by_year: Dict[int, int] = field(default_factory=dict)
    missing_mdr_keys_sample: List[str] = field(default_factory=list)
    recommended_files_to_reload: List[str] = field(default_factory=list)


@dataclass
class OrphanAnalysisReport:
    """Complete orphan analysis report."""
    timestamp: datetime
    database_path: str
    overall_orphan_rate: float = 0.0
    total_orphans: int = 0
    table_analyses: Dict[str, TableOrphanAnalysis] = field(default_factory=dict)
    cross_table_patterns: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    fix_commands: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON export."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "database_path": self.database_path,
            "overall_orphan_rate": self.overall_orphan_rate,
            "total_orphans": self.total_orphans,
            "table_analyses": {
                name: {
                    "table_name": analysis.table_name,
                    "total_records": analysis.total_records,
                    "orphan_count": analysis.orphan_count,
                    "orphan_pct": analysis.orphan_pct,
                    "orphans_by_year": analysis.orphans_by_year,
                    "orphans_by_source": {
                        src: asdict(grp)
                        for src, grp in analysis.orphans_by_source.items()
                    },
                    "missing_mdr_keys_sample": analysis.missing_mdr_keys_sample,
                    "recommended_files_to_reload": analysis.recommended_files_to_reload,
                }
                for name, analysis in self.table_analyses.items()
            },
            "cross_table_patterns": self.cross_table_patterns,
            "recommendations": self.recommendations,
            "fix_commands": self.fix_commands,
        }


# Tables to analyze with their FK column and date column
CHILD_TABLES = {
    "devices": ("mdr_report_key", "date_received"),
    "patients": ("mdr_report_key", "date_received"),
    "mdr_text": ("mdr_report_key", "date_report"),
    "device_problems": ("mdr_report_key", None),
    "patient_problems": ("mdr_report_key", None),
}


def analyze_table_orphans(
    conn,
    table_name: str,
    fk_column: str,
    date_column: Optional[str],
    limit_samples: int = 100
) -> TableOrphanAnalysis:
    """
    Analyze orphan records in a table, grouped by source file.

    Args:
        conn: Database connection.
        table_name: Name of child table.
        fk_column: Foreign key column name.
        date_column: Date column for year analysis (optional).
        limit_samples: Max number of sample MDR keys to collect.

    Returns:
        TableOrphanAnalysis with detailed results.
    """
    analysis = TableOrphanAnalysis(table_name=table_name)

    # Get total count
    total = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    analysis.total_records = total

    if total == 0:
        return analysis

    # Count orphans
    orphan_count = conn.execute(f"""
        SELECT COUNT(*) FROM {table_name} c
        WHERE NOT EXISTS (
            SELECT 1 FROM master_events m
            WHERE m.mdr_report_key = c.{fk_column}
        )
    """).fetchone()[0]

    analysis.orphan_count = orphan_count
    analysis.orphan_pct = round((orphan_count / total) * 100, 4) if total > 0 else 0

    if orphan_count == 0:
        return analysis

    # Group orphans by source file
    try:
        source_query = f"""
            SELECT
                COALESCE(c.source_file, 'UNKNOWN') as src,
                COUNT(*) as cnt
            FROM {table_name} c
            WHERE NOT EXISTS (
                SELECT 1 FROM master_events m
                WHERE m.mdr_report_key = c.{fk_column}
            )
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 50
        """
        source_results = conn.execute(source_query).fetchall()

        for row in source_results:
            source_file = row[0]
            count = row[1]

            group = OrphanSourceGroup(
                source_file=source_file,
                orphan_count=count,
            )

            # Get sample MDR keys for this source
            sample_query = f"""
                SELECT DISTINCT c.{fk_column}
                FROM {table_name} c
                WHERE c.source_file = ?
                  AND NOT EXISTS (
                      SELECT 1 FROM master_events m
                      WHERE m.mdr_report_key = c.{fk_column}
                  )
                LIMIT 10
            """
            samples = conn.execute(sample_query, [source_file]).fetchall()
            group.sample_mdr_keys = [str(s[0]) for s in samples]

            # Get date range if date column exists
            if date_column:
                date_query = f"""
                    SELECT
                        MIN({date_column})::VARCHAR,
                        MAX({date_column})::VARCHAR
                    FROM {table_name} c
                    WHERE c.source_file = ?
                      AND NOT EXISTS (
                          SELECT 1 FROM master_events m
                          WHERE m.mdr_report_key = c.{fk_column}
                      )
                      AND {date_column} IS NOT NULL
                """
                date_range = conn.execute(date_query, [source_file]).fetchone()
                if date_range[0]:
                    group.earliest_date = date_range[0]
                    group.latest_date = date_range[1]

            analysis.orphans_by_source[source_file] = group

            # Recommend file for reload if significant orphans
            if count > 100:
                analysis.recommended_files_to_reload.append(source_file)

    except Exception as e:
        logger.warning(f"Could not analyze orphans by source for {table_name}: {e}")

    # Group orphans by year
    if date_column:
        try:
            year_query = f"""
                SELECT
                    EXTRACT(YEAR FROM c.{date_column}) as year,
                    COUNT(*) as cnt
                FROM {table_name} c
                WHERE NOT EXISTS (
                    SELECT 1 FROM master_events m
                    WHERE m.mdr_report_key = c.{fk_column}
                )
                AND c.{date_column} IS NOT NULL
                GROUP BY 1
                ORDER BY 1
            """
            year_results = conn.execute(year_query).fetchall()
            analysis.orphans_by_year = {int(row[0]): row[1] for row in year_results if row[0]}
        except Exception as e:
            logger.warning(f"Could not get orphans by year for {table_name}: {e}")

    # Get sample of missing MDR keys
    sample_query = f"""
        SELECT DISTINCT c.{fk_column}
        FROM {table_name} c
        WHERE NOT EXISTS (
            SELECT 1 FROM master_events m
            WHERE m.mdr_report_key = c.{fk_column}
        )
        LIMIT {limit_samples}
    """
    samples = conn.execute(sample_query).fetchall()
    analysis.missing_mdr_keys_sample = [str(s[0]) for s in samples]

    return analysis


def find_cross_table_patterns(
    table_analyses: Dict[str, TableOrphanAnalysis]
) -> List[str]:
    """
    Find patterns of orphans across multiple tables.

    Args:
        table_analyses: Analysis results for each table.

    Returns:
        List of pattern descriptions.
    """
    patterns = []

    # Find common MDR keys that are orphans in multiple tables
    all_orphan_keys = {}
    for table_name, analysis in table_analyses.items():
        for key in analysis.missing_mdr_keys_sample:
            if key not in all_orphan_keys:
                all_orphan_keys[key] = []
            all_orphan_keys[key].append(table_name)

    # Keys orphaned in multiple tables
    multi_table_orphans = {k: v for k, v in all_orphan_keys.items() if len(v) > 1}
    if multi_table_orphans:
        patterns.append(
            f"Found {len(multi_table_orphans)} MDR keys orphaned in multiple tables"
        )

    # Find common source files with orphans
    all_sources = defaultdict(list)
    for table_name, analysis in table_analyses.items():
        for source_file in analysis.orphans_by_source.keys():
            all_sources[source_file].append(table_name)

    multi_table_sources = {s: t for s, t in all_sources.items() if len(t) > 1}
    if multi_table_sources:
        patterns.append(
            f"Found {len(multi_table_sources)} source files with orphans in multiple tables"
        )
        # List the top sources
        for source, tables in sorted(multi_table_sources.items(),
                                      key=lambda x: len(x[1]), reverse=True)[:5]:
            patterns.append(f"  - {source}: orphans in {', '.join(tables)}")

    # Find years with high orphan rates across tables
    all_years = defaultdict(lambda: defaultdict(int))
    for table_name, analysis in table_analyses.items():
        for year, count in analysis.orphans_by_year.items():
            all_years[year][table_name] = count

    high_orphan_years = {
        y: dict(t) for y, t in all_years.items()
        if sum(t.values()) > 1000
    }
    if high_orphan_years:
        patterns.append(
            f"Years with >1000 total orphans: {sorted(high_orphan_years.keys())}"
        )

    return patterns


def generate_fix_recommendations(
    table_analyses: Dict[str, TableOrphanAnalysis]
) -> Tuple[List[str], List[str]]:
    """
    Generate recommendations and fix commands.

    Args:
        table_analyses: Analysis results for each table.

    Returns:
        Tuple of (recommendations, fix_commands).
    """
    recommendations = []
    fix_commands = []

    # Collect all files to reload
    files_to_reload = set()
    for analysis in table_analyses.values():
        files_to_reload.update(analysis.recommended_files_to_reload)

    if files_to_reload:
        recommendations.append(
            f"Reload {len(files_to_reload)} source file(s) with high orphan counts"
        )

        # Generate reload commands
        for source_file in sorted(files_to_reload):
            if source_file != "UNKNOWN":
                fix_commands.append(
                    f"python scripts/initial_load.py --file data/raw/{source_file}"
                )

    # Check for high orphan rates
    for table_name, analysis in table_analyses.items():
        if analysis.orphan_pct > 5:
            recommendations.append(
                f"{table_name}: {analysis.orphan_pct:.1f}% orphan rate - "
                f"consider reloading master files for affected years"
            )

    # Check for specific year issues
    all_years_with_orphans = set()
    for analysis in table_analyses.values():
        all_years_with_orphans.update(analysis.orphans_by_year.keys())

    if all_years_with_orphans:
        sorted_years = sorted(all_years_with_orphans)
        recommendations.append(
            f"Years with orphans: {sorted_years[0]}-{sorted_years[-1]}"
        )

    return recommendations, fix_commands


def run_orphan_analysis(
    db_path: Path,
    tables: Optional[List[str]] = None,
    limit_samples: int = 100
) -> OrphanAnalysisReport:
    """
    Run complete orphan analysis.

    Args:
        db_path: Path to database.
        tables: Optional list of specific tables to analyze.
        limit_samples: Max number of sample MDR keys per table.

    Returns:
        OrphanAnalysisReport with all results.
    """
    report = OrphanAnalysisReport(
        timestamp=datetime.now(),
        database_path=str(db_path),
    )

    tables_to_check = {t: CHILD_TABLES[t] for t in (tables or CHILD_TABLES.keys())}

    total_records = 0
    total_orphans = 0

    with get_connection(db_path, read_only=True) as conn:
        for table_name, (fk_column, date_column) in tables_to_check.items():
            try:
                analysis = analyze_table_orphans(
                    conn, table_name, fk_column, date_column, limit_samples
                )
                report.table_analyses[table_name] = analysis

                total_records += analysis.total_records
                total_orphans += analysis.orphan_count

            except Exception as e:
                logger.error(f"Error analyzing {table_name}: {e}")

    # Calculate overall orphan rate
    if total_records > 0:
        report.overall_orphan_rate = round((total_orphans / total_records) * 100, 4)
    report.total_orphans = total_orphans

    # Find cross-table patterns
    report.cross_table_patterns = find_cross_table_patterns(report.table_analyses)

    # Generate recommendations
    report.recommendations, report.fix_commands = generate_fix_recommendations(
        report.table_analyses
    )

    return report


def print_report(report: OrphanAnalysisReport, show_fix_commands: bool = False) -> None:
    """Print orphan analysis report to console."""
    print("=" * 70)
    print("FDA MAUDE ORPHAN SOURCE ANALYSIS")
    print("=" * 70)
    print(f"\nTimestamp: {report.timestamp}")
    print(f"Database: {report.database_path}")
    print(f"Overall Orphan Rate: {report.overall_orphan_rate:.2f}%")
    print(f"Total Orphans: {report.total_orphans:,}")

    print("\n" + "-" * 70)
    print("TABLE ANALYSIS")
    print("-" * 70)

    for table_name, analysis in report.table_analyses.items():
        status = "OK" if analysis.orphan_pct < 1 else "WARNING" if analysis.orphan_pct < 5 else "CRITICAL"

        print(f"\n  {table_name} [{status}]")
        print(f"    Total Records: {analysis.total_records:>12,}")
        print(f"    Orphans:       {analysis.orphan_count:>12,} ({analysis.orphan_pct:.2f}%)")

        if analysis.orphans_by_source:
            print(f"    Top Sources with Orphans:")
            top_sources = sorted(
                analysis.orphans_by_source.items(),
                key=lambda x: x[1].orphan_count,
                reverse=True
            )[:5]
            for source_file, group in top_sources:
                print(f"      - {source_file}: {group.orphan_count:,}")
                if group.earliest_date:
                    print(f"        Date range: {group.earliest_date} to {group.latest_date}")

        if analysis.orphans_by_year:
            print(f"    Years with Most Orphans:")
            top_years = sorted(
                analysis.orphans_by_year.items(),
                key=lambda x: x[1],
                reverse=True
            )[:5]
            for year, count in top_years:
                print(f"      - {year}: {count:,}")

        if analysis.recommended_files_to_reload:
            print(f"    Recommended Files to Reload: {len(analysis.recommended_files_to_reload)}")

    if report.cross_table_patterns:
        print("\n" + "-" * 70)
        print("CROSS-TABLE PATTERNS")
        print("-" * 70)
        for pattern in report.cross_table_patterns:
            print(f"  {pattern}")

    if report.recommendations:
        print("\n" + "-" * 70)
        print("RECOMMENDATIONS")
        print("-" * 70)
        for rec in report.recommendations:
            print(f"  - {rec}")

    if show_fix_commands and report.fix_commands:
        print("\n" + "-" * 70)
        print("FIX COMMANDS")
        print("-" * 70)
        for cmd in report.fix_commands:
            print(f"  {cmd}")

    print("\n" + "=" * 70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Analyze orphan records by source file",
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
        "--table",
        action="append",
        dest="tables",
        choices=list(CHILD_TABLES.keys()),
        help="Specific table(s) to analyze",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Max sample MDR keys per table",
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
        "--fix-recommendations",
        action="store_true",
        help="Show fix commands in output",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="WARNING",
        help="Logging level",
    )

    args = parser.parse_args()

    setup_logging(log_level=args.log_level)

    # Run analysis
    report = run_orphan_analysis(args.db, args.tables, args.limit)

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
        print_report(report, args.fix_recommendations)
        if args.output:
            with open(args.output, "w") as f:
                json.dump(report.to_dict(), f, indent=2)
            print(f"\nJSON results saved to: {args.output}")

    # Return exit code
    return 0 if report.overall_orphan_rate < 1 else 1


if __name__ == "__main__":
    sys.exit(main())
