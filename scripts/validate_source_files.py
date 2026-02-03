#!/usr/bin/env python3
"""
Pre-load validation script for MAUDE data files.

This script validates source files BEFORE loading to catch issues early:
1. File existence and naming consistency
2. Schema detection (column counts)
3. Expected vs actual file coverage
4. Loading order verification

Run this BEFORE running full_data_reload.py to catch issues.

Usage:
    python scripts/validate_source_files.py
    python scripts/validate_source_files.py --data-dir data/raw --fix-names
"""

import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime
import re

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config


@dataclass
class FileValidationResult:
    """Result of validating a single file."""
    filename: str
    file_type: str
    exists: bool
    size_bytes: int
    column_count: int
    expected_columns: Optional[int]
    schema_match: bool
    issues: List[str]


@dataclass
class ValidationSummary:
    """Summary of all validations."""
    total_files: int
    valid_files: int
    files_with_issues: int
    missing_files: List[str]
    schema_mismatches: List[str]
    naming_issues: List[str]
    loading_order_issues: List[str]
    all_issues: List[str]


# Expected file patterns by type
EXPECTED_FILES = {
    "device": {
        "historical": ["foidevthru1997.txt"],
        "annual_foidev": [f"foidev{year}.txt" for year in range(1998, 2020)],
        "annual_device": [f"device{year}.txt" for year in range(2020, 2026)],
        "current": ["foidev.txt"],
        "add": ["foidevAdd.txt"],
        "change": ["devicechange.txt"],
    },
    "master": {
        "historical": ["mdrfoiThru2023.txt", "mdrfoiThru2025.txt"],
        "current": ["mdrfoi.txt"],
        "add": ["mdrfoiAdd.txt"],
        "change": ["mdrfoiChange.txt"],
    },
    "patient": {
        "historical": ["patientThru2025.txt"],
        "current": ["patient.txt"],
        "add": ["patientAdd.txt"],
        "change": ["patientChange.txt"],
    },
    "text": {
        "historical": [f"foitext{year}.txt" for year in range(1996, 2026)],
        "legacy": ["foitextthru1995.txt"],
        "current": ["foitext.txt"],
        "add": ["foitextAdd.txt"],
        "change": ["foitextChange.txt"],
    },
    "problem": {
        "current": ["foidevproblem.txt"],
    },
}

# Expected column counts by schema
EXPECTED_COLUMNS = {
    "device": {
        45: "Legacy 1997-2008",
        28: "2009-2019",
        34: "2020+",
    },
    "master": {
        84: "Legacy",
        86: "Current",
        61: "Sample/Test",  # mdrfoi_sample.txt
    },
    "patient": {
        10: "Current",
    },
    "text": {
        6: "Current",
    },
    "problem": {
        2: "Current",
    },
}


def count_columns(filepath: Path) -> int:
    """Count columns in first line of file."""
    try:
        with open(filepath, encoding='latin-1') as f:
            header = f.readline().strip()
            return len(header.split('|'))
    except Exception as e:
        return -1


def get_file_type(filename: str) -> Optional[str]:
    """Determine file type from filename."""
    name = filename.lower()
    if 'foidevproblem' in name:
        return 'problem'
    if 'foidev' in name or name.startswith('device'):
        return 'device'
    if 'mdrfoi' in name:
        return 'master'
    if 'patient' in name:
        return 'patient'
    if 'foitext' in name:
        return 'text'
    return None


def validate_file(filepath: Path) -> FileValidationResult:
    """Validate a single file."""
    issues = []
    filename = filepath.name
    file_type = get_file_type(filename)

    exists = filepath.exists()
    size_bytes = filepath.stat().st_size if exists else 0
    column_count = count_columns(filepath) if exists else 0

    # Check schema match
    expected_cols = None
    schema_match = True

    if file_type and file_type in EXPECTED_COLUMNS:
        valid_counts = list(EXPECTED_COLUMNS[file_type].keys())
        if column_count not in valid_counts:
            schema_match = False
            issues.append(
                f"Unexpected column count {column_count}, "
                f"expected one of {valid_counts}"
            )
        else:
            expected_cols = column_count

    # Check file size
    if exists and size_bytes == 0:
        issues.append("File is empty")

    # Check naming conventions
    name_lower = filename.lower()
    if file_type == 'device':
        # Check for inconsistent naming
        year_match = re.search(r'(\d{4})', filename)
        if year_match:
            year = int(year_match.group(1))
            if year >= 2020 and 'foidev' in name_lower:
                issues.append(f"File uses old naming 'foidev' for year {year}, should be 'device'")
            if year < 2020 and name_lower.startswith('device') and 'change' not in name_lower:
                issues.append(f"File uses new naming 'device' for year {year}, expected 'foidev'")

    return FileValidationResult(
        filename=filename,
        file_type=file_type or "unknown",
        exists=exists,
        size_bytes=size_bytes,
        column_count=column_count,
        expected_columns=expected_cols,
        schema_match=schema_match,
        issues=issues,
    )


def check_loading_order(files: List[Path]) -> List[str]:
    """Check that files are in correct loading order."""
    issues = []

    add_file_idx = None
    change_file_idx = None

    for idx, f in enumerate(files):
        name = f.name.lower()
        if 'add' in name and add_file_idx is None:
            add_file_idx = idx
        if 'change' in name and change_file_idx is None:
            change_file_idx = idx

    if add_file_idx is not None and change_file_idx is not None:
        if change_file_idx < add_file_idx:
            issues.append(
                f"Loading order error: Change file appears before Add file. "
                f"Add file at position {add_file_idx}, Change file at position {change_file_idx}"
            )

    return issues


def validate_all_files(data_dir: Path) -> ValidationSummary:
    """Validate all source files in directory."""
    all_results = []
    missing_files = []
    schema_mismatches = []
    naming_issues = []
    loading_order_issues = []
    all_issues = []

    # Find all .txt files
    txt_files = sorted(data_dir.glob("*.txt"))

    print(f"Found {len(txt_files)} .txt files in {data_dir}")
    print()

    # Validate each file
    for filepath in txt_files:
        result = validate_file(filepath)
        all_results.append(result)

        if result.issues:
            all_issues.extend([f"{result.filename}: {issue}" for issue in result.issues])
        if not result.schema_match:
            schema_mismatches.append(result.filename)

    # Check for expected files that are missing
    for file_type, categories in EXPECTED_FILES.items():
        for category, expected in categories.items():
            for expected_file in expected:
                if not (data_dir / expected_file).exists():
                    # Only report as missing if it's a critical file
                    if category in ['current', 'add', 'change']:
                        missing_files.append(f"{file_type}/{expected_file}")

    # Group files by type and check loading order
    # Import the same sort key used in full_data_reload.py
    def get_file_sort_key(filepath: Path) -> tuple:
        name = filepath.name.lower()
        if "thru" in name:
            match = re.search(r'thru(\d{4})', name)
            year = int(match.group(1)) if match else 0
            return (1, year, name)
        if "change" in name:
            return (5, 0, name)
        if "add" in name:
            return (4, 0, name)
        year_match = re.search(r'(\d{4})', name)
        if year_match:
            return (2, int(year_match.group(1)), name)
        return (3, 0, name)

    files_by_type: Dict[str, List[Path]] = {}
    for filepath in txt_files:
        file_type = get_file_type(filepath.name)
        if file_type:
            if file_type not in files_by_type:
                files_by_type[file_type] = []
            files_by_type[file_type].append(filepath)

    # Sort files by loading order before checking
    for file_type, files in files_by_type.items():
        sorted_files = sorted(files, key=get_file_sort_key)
        order_issues = check_loading_order(sorted_files)
        loading_order_issues.extend(order_issues)
        all_issues.extend(order_issues)

    # Count valid files
    valid_count = sum(1 for r in all_results if not r.issues)

    return ValidationSummary(
        total_files=len(all_results),
        valid_files=valid_count,
        files_with_issues=len(all_results) - valid_count,
        missing_files=missing_files,
        schema_mismatches=schema_mismatches,
        naming_issues=naming_issues,
        loading_order_issues=loading_order_issues,
        all_issues=all_issues,
    )


def print_file_inventory(data_dir: Path):
    """Print inventory of files by type with column counts."""
    print("=" * 70)
    print("FILE INVENTORY BY TYPE")
    print("=" * 70)

    file_types = ["device", "master", "patient", "text", "problem"]

    for file_type in file_types:
        print(f"\n{file_type.upper()}:")
        print("-" * 50)

        # Find files for this type
        files = []
        for f in sorted(data_dir.glob("*.txt")):
            if get_file_type(f.name) == file_type:
                files.append(f)

        if not files:
            print("  (no files found)")
            continue

        # Sort by loading order
        def sort_key(filepath):
            name = filepath.name.lower()
            if "thru" in name:
                match = re.search(r'thru(\d{4})', name)
                return (1, int(match.group(1)) if match else 0, name)
            if "change" in name:
                return (5, 0, name)
            if "add" in name:
                return (4, 0, name)
            year_match = re.search(r'(\d{4})', name)
            if year_match:
                return (2, int(year_match.group(1)), name)
            return (3, 0, name)

        files = sorted(files, key=sort_key)

        for f in files:
            cols = count_columns(f)
            size_mb = f.stat().st_size / (1024 * 1024)

            # Get schema description
            schema_desc = ""
            if file_type in EXPECTED_COLUMNS and cols in EXPECTED_COLUMNS[file_type]:
                schema_desc = f" ({EXPECTED_COLUMNS[file_type][cols]})"

            print(f"  {f.name:40} {cols:3} cols  {size_mb:8.1f} MB{schema_desc}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Validate MAUDE source files before loading")
    parser.add_argument("--data-dir", type=Path, default=config.data.raw_path)
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed output")
    args = parser.parse_args()

    print("=" * 70)
    print("MAUDE SOURCE FILE VALIDATION")
    print(f"Data directory: {args.data_dir}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Print file inventory
    print_file_inventory(args.data_dir)

    # Run validation
    print("\n")
    print("=" * 70)
    print("VALIDATION RESULTS")
    print("=" * 70)

    summary = validate_all_files(args.data_dir)

    print(f"\nTotal files: {summary.total_files}")
    print(f"Valid files: {summary.valid_files}")
    print(f"Files with issues: {summary.files_with_issues}")

    if summary.missing_files:
        print(f"\nMissing critical files ({len(summary.missing_files)}):")
        for f in summary.missing_files[:10]:
            print(f"  - {f}")
        if len(summary.missing_files) > 10:
            print(f"  ... and {len(summary.missing_files) - 10} more")

    if summary.schema_mismatches:
        print(f"\nSchema mismatches ({len(summary.schema_mismatches)}):")
        for f in summary.schema_mismatches[:10]:
            print(f"  - {f}")

    if summary.loading_order_issues:
        print(f"\nLoading order issues ({len(summary.loading_order_issues)}):")
        for issue in summary.loading_order_issues:
            print(f"  - {issue}")

    if args.verbose and summary.all_issues:
        print(f"\nAll issues ({len(summary.all_issues)}):")
        for issue in summary.all_issues:
            print(f"  - {issue}")

    # Final verdict
    print("\n" + "=" * 70)
    if summary.files_with_issues == 0 and not summary.loading_order_issues:
        print("✓ VALIDATION PASSED - Safe to proceed with data load")
        return 0
    else:
        print("⚠ VALIDATION WARNINGS - Review issues before loading")
        return 1


if __name__ == "__main__":
    sys.exit(main())
