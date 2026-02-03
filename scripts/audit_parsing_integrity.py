#!/usr/bin/env python3
"""
Comprehensive Audit: Verify source file record counts match database counts.

This script detects parsing issues like:
1. Quote-swallowing: CSV reader consuming multiple lines as one quoted field
2. Embedded newlines: Records split across multiple lines
3. Other parsing failures causing record loss

The key insight is to count PHYSICAL LINES in the source file (not CSV-parsed rows)
and compare against database record counts.

For each file type, this script will:
1. Count physical lines in source files (minus header)
2. Count valid data lines (lines starting with a digit for MDR_REPORT_KEY)
3. Count orphan lines (fragments from split records)
4. Count what CSV reader sees (to detect quote-swallowing)
5. Count records in database by source_file
6. Report discrepancies
"""

import csv
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from src.database import get_connection

csv.field_size_limit(sys.maxsize)


@dataclass
class FileAuditResult:
    """Audit result for a single file."""
    filename: str
    file_type: str
    # Physical line counts
    total_lines: int = 0
    valid_data_lines: int = 0  # Lines starting with digit (valid MDR key)
    header_lines: int = 1
    orphan_lines: int = 0  # Fragments from split records
    # CSV reader counts
    csv_reader_rows: int = 0
    csv_oversized_rows: int = 0  # Rows with suspiciously large content
    # Database counts
    db_record_count: int = 0
    # Discrepancies
    physical_vs_csv_diff: int = 0
    physical_vs_db_diff: int = 0
    csv_vs_db_diff: int = 0
    # Status
    has_quote_swallowing: bool = False
    has_orphan_records: bool = False
    status: str = "OK"
    notes: List[str] = field(default_factory=list)


def count_physical_lines(filepath: Path) -> Tuple[int, int, int]:
    """
    Count physical lines, valid data lines, and orphan lines.

    Returns:
        Tuple of (total_lines, valid_data_lines, orphan_lines)
    """
    total = 0
    valid = 0
    orphan = 0

    with open(filepath, 'r', encoding='latin-1', errors='replace') as f:
        for i, line in enumerate(f):
            total += 1
            if i == 0:  # Skip header
                continue
            # Valid data lines start with a digit (MDR_REPORT_KEY is numeric)
            if line and line[0].isdigit():
                valid += 1
            else:
                orphan += 1

    return total, valid, orphan


def count_csv_reader_rows(filepath: Path, use_quoting: bool = True) -> Tuple[int, int]:
    """
    Count rows as seen by CSV reader and detect oversized rows.

    Args:
        filepath: Path to file
        use_quoting: If True, use default CSV quoting (which can cause issues)

    Returns:
        Tuple of (total_rows, oversized_rows)
    """
    total = 0
    oversized = 0

    with open(filepath, 'r', encoding='latin-1', errors='replace') as f:
        if use_quoting:
            reader = csv.reader(f, delimiter='|', quotechar='"')
        else:
            reader = csv.reader(f, delimiter='|', quoting=csv.QUOTE_NONE)

        for row in reader:
            total += 1
            # Check for oversized rows (indication of quote-swallowing)
            row_str = '|'.join(row)
            if len(row_str) > 10000:
                oversized += 1

    return total, oversized


def count_db_records_by_source(db_path: Path) -> Dict[str, int]:
    """
    Count database records grouped by source_file.

    Returns:
        Dictionary mapping source_file to record count
    """
    counts = {}

    with get_connection(db_path, read_only=True) as conn:
        # Master events
        results = conn.execute("""
            SELECT source_file, COUNT(*) as cnt
            FROM master_events
            WHERE source_file IS NOT NULL
            GROUP BY source_file
        """).fetchall()
        for source_file, cnt in results:
            counts[f"master:{source_file}"] = cnt

        # Devices
        results = conn.execute("""
            SELECT source_file, COUNT(*) as cnt
            FROM devices
            WHERE source_file IS NOT NULL
            GROUP BY source_file
        """).fetchall()
        for source_file, cnt in results:
            counts[f"device:{source_file}"] = cnt

        # Patients
        results = conn.execute("""
            SELECT source_file, COUNT(*) as cnt
            FROM patients
            WHERE source_file IS NOT NULL
            GROUP BY source_file
        """).fetchall()
        for source_file, cnt in results:
            counts[f"patient:{source_file}"] = cnt

        # Text
        results = conn.execute("""
            SELECT source_file, COUNT(*) as cnt
            FROM mdr_text
            WHERE source_file IS NOT NULL
            GROUP BY source_file
        """).fetchall()
        for source_file, cnt in results:
            counts[f"text:{source_file}"] = cnt

        # Problems
        results = conn.execute("""
            SELECT source_file, COUNT(*) as cnt
            FROM device_problems
            WHERE source_file IS NOT NULL
            GROUP BY source_file
        """).fetchall()
        for source_file, cnt in results:
            counts[f"problem:{source_file}"] = cnt

    return counts


def detect_file_type(filename: str) -> Optional[str]:
    """Detect file type from filename."""
    name = filename.lower()

    if "mdrfoi" in name and "problem" not in name:
        return "master"
    elif ("foidev" in name or name.startswith("device")) and "problem" not in name:
        return "device"
    elif name.startswith("patient") and "problem" not in name:
        return "patient"
    elif "foitext" in name:
        return "text"
    elif "foidevproblem" in name or "deviceproblem" in name:
        return "problem"

    return None


def audit_file(filepath: Path, db_counts: Dict[str, int]) -> FileAuditResult:
    """
    Audit a single file for parsing integrity.

    Args:
        filepath: Path to source file
        db_counts: Database record counts by source

    Returns:
        FileAuditResult with audit findings
    """
    filename = filepath.name
    file_type = detect_file_type(filename)

    result = FileAuditResult(
        filename=filename,
        file_type=file_type or "unknown",
    )

    if file_type is None:
        result.status = "SKIPPED"
        result.notes.append("Unknown file type")
        return result

    # 1. Count physical lines
    result.total_lines, result.valid_data_lines, result.orphan_lines = count_physical_lines(filepath)

    # 2. Count CSV reader rows (with default quoting that can cause issues)
    result.csv_reader_rows, result.csv_oversized_rows = count_csv_reader_rows(filepath, use_quoting=True)

    # 3. Get database count
    db_key = f"{file_type}:{filename}"
    result.db_record_count = db_counts.get(db_key, 0)

    # 4. Calculate discrepancies
    # Physical vs CSV (detects quote-swallowing)
    result.physical_vs_csv_diff = result.valid_data_lines - (result.csv_reader_rows - 1)  # -1 for header

    # Physical vs DB (detects overall data loss)
    result.physical_vs_db_diff = result.valid_data_lines - result.db_record_count

    # CSV vs DB (should be small if loader worked correctly)
    result.csv_vs_db_diff = (result.csv_reader_rows - 1) - result.db_record_count

    # 5. Detect issues
    # Quote-swallowing: CSV sees far fewer rows than physical lines
    if result.physical_vs_csv_diff > 1000 or result.csv_oversized_rows > 0:
        result.has_quote_swallowing = True
        result.notes.append(f"CRITICAL: Quote-swallowing detected! Physical lines: {result.valid_data_lines:,}, CSV rows: {result.csv_reader_rows - 1:,}")

    # Orphan records from embedded newlines
    if result.orphan_lines > 100:
        result.has_orphan_records = True
        result.notes.append(f"WARNING: {result.orphan_lines:,} orphan lines from embedded newlines")

    # Database has fewer records than expected
    if result.physical_vs_db_diff > 1000:
        loss_pct = (result.physical_vs_db_diff / result.valid_data_lines * 100) if result.valid_data_lines > 0 else 0
        result.notes.append(f"DATA LOSS: {result.physical_vs_db_diff:,} records missing ({loss_pct:.1f}%)")

    # Set status
    if result.has_quote_swallowing:
        result.status = "CRITICAL"
    elif result.has_orphan_records or result.physical_vs_db_diff > 1000:
        result.status = "WARNING"
    elif result.db_record_count == 0:
        result.status = "NOT_LOADED"
    else:
        result.status = "OK"

    return result


def run_full_audit(data_dir: Path, db_path: Path) -> List[FileAuditResult]:
    """
    Run full audit on all files in data directory.

    Args:
        data_dir: Directory containing source files
        db_path: Path to database

    Returns:
        List of audit results
    """
    print(f"=== Parsing Integrity Audit ===")
    print(f"Data directory: {data_dir}")
    print(f"Database: {db_path}")
    print()

    # Get all database counts first
    print("Loading database record counts...")
    db_counts = count_db_records_by_source(db_path)
    print(f"Found {len(db_counts)} source files in database")
    print()

    # Find all files to audit
    file_patterns = [
        "mdrfoi*.txt",
        "foidev*.txt",
        "device*.txt",
        "patient*.txt",
        "foitext*.txt",
        "foidevproblem*.txt",
    ]

    files_to_audit = []
    for pattern in file_patterns:
        files_to_audit.extend(data_dir.glob(pattern))

    # Filter out problem files from device patterns
    files_to_audit = [f for f in files_to_audit if "problem" not in f.name.lower() or "foidevproblem" in f.name.lower()]
    files_to_audit = sorted(set(files_to_audit))

    print(f"Found {len(files_to_audit)} files to audit")
    print()

    # Audit each file
    results = []
    critical_count = 0
    warning_count = 0

    for filepath in files_to_audit:
        print(f"Auditing {filepath.name}...", end=" ", flush=True)
        result = audit_file(filepath, db_counts)
        results.append(result)

        if result.status == "CRITICAL":
            critical_count += 1
            print(f"CRITICAL!")
        elif result.status == "WARNING":
            warning_count += 1
            print(f"WARNING")
        elif result.status == "NOT_LOADED":
            print(f"not loaded")
        else:
            print(f"OK")

    print()
    print("=" * 80)
    print(f"AUDIT SUMMARY: {critical_count} CRITICAL, {warning_count} WARNING, {len(results) - critical_count - warning_count} OK")
    print("=" * 80)

    return results


def print_detailed_results(results: List[FileAuditResult]):
    """Print detailed audit results."""
    print()
    print("=" * 100)
    print("DETAILED RESULTS")
    print("=" * 100)

    # Sort by status severity
    status_order = {"CRITICAL": 0, "WARNING": 1, "NOT_LOADED": 2, "SKIPPED": 3, "OK": 4}
    sorted_results = sorted(results, key=lambda r: status_order.get(r.status, 5))

    for result in sorted_results:
        print()
        print(f"--- {result.filename} ({result.file_type}) ---")
        print(f"  Status: {result.status}")
        print(f"  Physical lines (total/valid/orphan): {result.total_lines:,} / {result.valid_data_lines:,} / {result.orphan_lines:,}")
        print(f"  CSV reader rows (total/oversized): {result.csv_reader_rows:,} / {result.csv_oversized_rows:,}")
        print(f"  Database records: {result.db_record_count:,}")
        print(f"  Discrepancies (phys-csv/phys-db/csv-db): {result.physical_vs_csv_diff:,} / {result.physical_vs_db_diff:,} / {result.csv_vs_db_diff:,}")

        if result.notes:
            print(f"  Notes:")
            for note in result.notes:
                print(f"    - {note}")


def generate_fix_plan(results: List[FileAuditResult]) -> str:
    """Generate a fix plan based on audit results."""
    lines = []
    lines.append("\n" + "=" * 100)
    lines.append("FIX PLAN")
    lines.append("=" * 100)

    critical_files = [r for r in results if r.status == "CRITICAL"]
    warning_files = [r for r in results if r.status == "WARNING"]

    if critical_files:
        lines.append("\n## CRITICAL: Files with Quote-Swallowing (must fix parser)")
        lines.append("These files lost significant data due to CSV reader quote-swallowing:")
        for r in critical_files:
            loss = r.physical_vs_db_diff
            loss_pct = (loss / r.valid_data_lines * 100) if r.valid_data_lines > 0 else 0
            lines.append(f"  - {r.filename}: {loss:,} records lost ({loss_pct:.1f}%)")
        lines.append("\nFix: Reload these files with quoting=csv.QUOTE_NONE")

    if warning_files:
        lines.append("\n## WARNING: Files with Embedded Newlines")
        lines.append("These files have split records that should be rejoined:")
        for r in warning_files:
            if r.orphan_lines > 0:
                lines.append(f"  - {r.filename}: {r.orphan_lines:,} orphan lines")
        lines.append("\nFix: Preprocess to rejoin split records before loading")

    lines.append("\n## Recommended Actions:")
    lines.append("1. Fix parser.py to use quoting=csv.QUOTE_NONE (prevents quote-swallowing)")
    lines.append("2. Create preprocessing script to rejoin split records")
    lines.append("3. Reload all affected files")
    lines.append("4. Re-run this audit to verify fixes")
    lines.append("5. Add physical line count validation to prevent future issues")

    return "\n".join(lines)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Audit parsing integrity of MAUDE files")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/raw"),
        help="Directory containing source files"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=config.database.path,
        help="Database path"
    )
    parser.add_argument(
        "--detailed",
        action="store_true",
        help="Print detailed results for all files"
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write results to JSON file"
    )

    args = parser.parse_args()

    if not args.data_dir.exists():
        print(f"Error: Data directory not found: {args.data_dir}")
        sys.exit(1)

    if not args.db.exists():
        print(f"Error: Database not found: {args.db}")
        sys.exit(1)

    # Run audit
    results = run_full_audit(args.data_dir, args.db)

    # Print detailed results if requested
    if args.detailed:
        print_detailed_results(results)
    else:
        # Print only issues
        issues = [r for r in results if r.status in ("CRITICAL", "WARNING")]
        if issues:
            print_detailed_results(issues)

    # Generate fix plan
    fix_plan = generate_fix_plan(results)
    print(fix_plan)

    # Save to JSON if requested
    if args.output:
        import json
        output_data = {
            "audit_time": datetime.now().isoformat(),
            "data_dir": str(args.data_dir),
            "db_path": str(args.db),
            "results": [
                {
                    "filename": r.filename,
                    "file_type": r.file_type,
                    "status": r.status,
                    "total_lines": r.total_lines,
                    "valid_data_lines": r.valid_data_lines,
                    "orphan_lines": r.orphan_lines,
                    "csv_reader_rows": r.csv_reader_rows,
                    "csv_oversized_rows": r.csv_oversized_rows,
                    "db_record_count": r.db_record_count,
                    "physical_vs_csv_diff": r.physical_vs_csv_diff,
                    "physical_vs_db_diff": r.physical_vs_db_diff,
                    "csv_vs_db_diff": r.csv_vs_db_diff,
                    "notes": r.notes,
                }
                for r in results
            ]
        }
        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"\nResults saved to: {args.output}")

    # Exit with error code if critical issues found
    critical_count = sum(1 for r in results if r.status == "CRITICAL")
    sys.exit(1 if critical_count > 0 else 0)


if __name__ == "__main__":
    main()
