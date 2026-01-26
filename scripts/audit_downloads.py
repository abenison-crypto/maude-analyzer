#!/usr/bin/env python3
"""
Audit FDA MAUDE downloaded files and identify gaps.

This script:
1. Scans data/raw directory for all downloaded files
2. Lists files by type and year
3. Compares against expected FDA file manifest
4. Reports missing files with prioritization

Usage:
    python scripts/audit_downloads.py [options]
    python scripts/audit_downloads.py --download-missing
    python scripts/audit_downloads.py --json --output gaps.json
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
import re

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from src.ingestion.download import KNOWN_FILES, MAUDEDownloader, FDA_DOWNLOAD_BASE


@dataclass
class FileInfo:
    """Information about a downloaded file."""
    filename: str
    file_type: str
    year: Optional[int] = None
    size_bytes: int = 0
    exists: bool = False
    is_current: bool = False  # Current year file (updated weekly)


@dataclass
class AuditReport:
    """Complete download audit report."""
    timestamp: datetime
    data_dir: str
    files_by_type: Dict[str, List[FileInfo]] = field(default_factory=dict)
    missing_by_type: Dict[str, List[str]] = field(default_factory=dict)
    total_downloaded: int = 0
    total_expected: int = 0
    total_missing: int = 0
    total_size_mb: float = 0.0

    @property
    def completeness_pct(self) -> float:
        """Calculate overall completeness percentage."""
        if self.total_expected == 0:
            return 100.0
        return (self.total_downloaded / self.total_expected) * 100

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON export."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "data_dir": self.data_dir,
            "summary": {
                "total_downloaded": self.total_downloaded,
                "total_expected": self.total_expected,
                "total_missing": self.total_missing,
                "completeness_pct": round(self.completeness_pct, 1),
                "total_size_mb": round(self.total_size_mb, 1),
            },
            "files_by_type": {
                ftype: [
                    {
                        "filename": f.filename,
                        "year": f.year,
                        "size_mb": round(f.size_bytes / 1024 / 1024, 2),
                        "exists": f.exists,
                        "is_current": f.is_current,
                    }
                    for f in files
                ]
                for ftype, files in self.files_by_type.items()
            },
            "missing_by_type": self.missing_by_type,
        }


def extract_year_from_filename(filename: str) -> Optional[int]:
    """
    Extract year from FDA filename.

    Examples:
        mdrfoithru2023.zip -> 2023
        foidev2019.zip -> 2019
        device2024.zip -> 2024
        mdr84.zip -> 1984
        ASR_2015.zip -> 2015
    """
    filename_lower = filename.lower()

    # Match patterns like thru2023, 2019, etc
    match = re.search(r'(?:thru)?(\d{4})\.', filename_lower)
    if match:
        return int(match.group(1))

    # Match 2-digit DEN years (mdr84 -> 1984)
    match = re.search(r'mdr(\d{2})\.', filename_lower)
    if match:
        year_2d = int(match.group(1))
        if 84 <= year_2d <= 97:
            return 1900 + year_2d

    # Match ASR_YYYY format
    match = re.search(r'asr_(\d{4})\.', filename_lower)
    if match:
        return int(match.group(1))

    return None


def is_current_file(filename: str) -> bool:
    """Check if file is a current-year file (updated weekly)."""
    current_files = {
        "mdrfoi.zip", "foidev.zip", "patient.zip", "foitext.zip",
        "mdrfoiadd.zip", "mdrfoichange.zip", "foidevchange.zip",
        "patientadd.zip", "patientchange.zip", "foitextchange.zip",
    }
    return filename.lower() in current_files


def scan_downloaded_files(data_dir: Path) -> Dict[str, List[FileInfo]]:
    """
    Scan directory for downloaded FDA files.

    Args:
        data_dir: Directory containing downloaded files.

    Returns:
        Dictionary mapping file type to list of FileInfo.
    """
    files_by_type = {
        "master": [],
        "device": [],
        "patient": [],
        "text": [],
        "problem": [],
        "problem_lookup": [],
        "patient_problem": [],
        "asr": [],
        "den": [],
    }

    for filepath in data_dir.glob("*.txt"):
        filename = filepath.name
        filename_lower = filename.lower()
        size_bytes = filepath.stat().st_size

        file_info = FileInfo(
            filename=filename,
            file_type="unknown",
            size_bytes=size_bytes,
            exists=True,
        )

        # Classify by file type
        if "mdrfoi" in filename_lower and "problem" not in filename_lower:
            file_info.file_type = "master"
            files_by_type["master"].append(file_info)
        elif ("foidev" in filename_lower or filename_lower.startswith("device")) and "problem" not in filename_lower:
            file_info.file_type = "device"
            files_by_type["device"].append(file_info)
        elif filename_lower.startswith("patient") and "problem" not in filename_lower:
            file_info.file_type = "patient"
            files_by_type["patient"].append(file_info)
        elif "foitext" in filename_lower:
            file_info.file_type = "text"
            files_by_type["text"].append(file_info)
        elif filename_lower == "deviceproblemcodes.txt":
            file_info.file_type = "problem_lookup"
            files_by_type["problem_lookup"].append(file_info)
        elif "patientproblem" in filename_lower:
            file_info.file_type = "patient_problem"
            files_by_type["patient_problem"].append(file_info)
        elif "foidevproblem" in filename_lower:
            file_info.file_type = "problem"
            files_by_type["problem"].append(file_info)
        elif filename_lower.startswith("asr"):
            file_info.file_type = "asr"
            files_by_type["asr"].append(file_info)
        elif filename_lower.startswith("mdr") and len(filename) <= 9:
            # DEN legacy files
            year_part = filename_lower[3:5]
            if year_part.isdigit():
                year = int(year_part)
                if 84 <= year <= 97:
                    file_info.file_type = "den"
                    file_info.year = 1900 + year
                    files_by_type["den"].append(file_info)
        elif filename_lower == "disclaim.txt":
            file_info.file_type = "den"
            files_by_type["den"].append(file_info)

        # Extract year for non-DEN files
        if file_info.year is None and file_info.file_type != "den":
            # Map .txt back to .zip for year extraction
            zip_name = filename.rsplit(".", 1)[0] + ".zip"
            file_info.year = extract_year_from_filename(zip_name)
            file_info.is_current = is_current_file(zip_name)

    return files_by_type


def get_expected_files() -> Dict[str, Set[str]]:
    """
    Get the complete set of expected files from KNOWN_FILES.

    Returns:
        Dictionary mapping file type to set of expected filenames (base names without .zip).
    """
    expected = {}
    for file_type, files in KNOWN_FILES.items():
        expected[file_type] = set()
        for f in files:
            # Convert .zip to .txt (expected extracted filename)
            base = f.replace(".zip", "").lower()
            expected[file_type].add(base)
    return expected


def find_missing_files(
    downloaded: Dict[str, List[FileInfo]],
    expected: Dict[str, Set[str]]
) -> Dict[str, List[str]]:
    """
    Find missing files by comparing downloaded vs expected.

    Args:
        downloaded: Downloaded files by type.
        expected: Expected files by type.

    Returns:
        Dictionary mapping file type to list of missing filenames.
    """
    missing = {}

    for file_type, expected_files in expected.items():
        downloaded_files = downloaded.get(file_type, [])
        downloaded_bases = {
            f.filename.replace(".txt", "").lower()
            for f in downloaded_files
        }

        missing_files = expected_files - downloaded_bases
        if missing_files:
            # Sort missing files for consistent output
            missing[file_type] = sorted(missing_files)

    return missing


def run_audit(data_dir: Path) -> AuditReport:
    """
    Run complete download audit.

    Args:
        data_dir: Directory containing downloaded files.

    Returns:
        AuditReport with complete results.
    """
    report = AuditReport(
        timestamp=datetime.now(),
        data_dir=str(data_dir),
    )

    # Scan downloaded files
    downloaded = scan_downloaded_files(data_dir)
    report.files_by_type = downloaded

    # Get expected files
    expected = get_expected_files()

    # Find missing
    missing = find_missing_files(downloaded, expected)
    report.missing_by_type = missing

    # Calculate totals
    for files in downloaded.values():
        report.total_downloaded += len(files)
        report.total_size_mb += sum(f.size_bytes for f in files) / 1024 / 1024

    for files in expected.values():
        report.total_expected += len(files)

    report.total_missing = sum(len(files) for files in missing.values())

    return report


def print_report(report: AuditReport) -> None:
    """Print audit report to console."""
    print("=" * 70)
    print("FDA MAUDE DOWNLOAD AUDIT REPORT")
    print("=" * 70)
    print(f"\nTimestamp: {report.timestamp}")
    print(f"Data Directory: {report.data_dir}")

    print("\n" + "-" * 70)
    print("SUMMARY")
    print("-" * 70)
    print(f"  Total downloaded:  {report.total_downloaded:>6} files")
    print(f"  Total expected:    {report.total_expected:>6} files")
    print(f"  Total missing:     {report.total_missing:>6} files")
    print(f"  Completeness:      {report.completeness_pct:>6.1f}%")
    print(f"  Total size:        {report.total_size_mb:>6.1f} MB")

    print("\n" + "-" * 70)
    print("FILES BY TYPE")
    print("-" * 70)

    for file_type in ["master", "device", "patient", "text", "problem", "asr", "den"]:
        files = report.files_by_type.get(file_type, [])
        missing = report.missing_by_type.get(file_type, [])
        expected_count = len(files) + len(missing)

        status = "OK" if not missing else f"MISSING {len(missing)}"
        print(f"\n  {file_type.upper()}: {len(files)} of {expected_count} downloaded [{status}]")

        # Show years if available
        years = sorted([f.year for f in files if f.year])
        if years:
            print(f"    Years covered: {min(years)}-{max(years)}")

        # Show size
        size_mb = sum(f.size_bytes for f in files) / 1024 / 1024
        print(f"    Total size: {size_mb:.1f} MB")

    # Show missing files detail
    if report.missing_by_type:
        print("\n" + "-" * 70)
        print("MISSING FILES (Priority Order)")
        print("-" * 70)

        # Prioritize device files (needed for manufacturer data)
        priority_order = ["device", "master", "patient", "text", "problem", "asr", "den"]

        for file_type in priority_order:
            missing = report.missing_by_type.get(file_type, [])
            if missing:
                print(f"\n  {file_type.upper()} ({len(missing)} missing):")
                for filename in missing[:10]:  # Show first 10
                    print(f"    - {filename}.zip")
                if len(missing) > 10:
                    print(f"    ... and {len(missing) - 10} more")

    print("\n" + "=" * 70)

    # Recommendations
    if report.total_missing > 0:
        print("\nRECOMMENDATIONS:")

        device_missing = len(report.missing_by_type.get("device", []))
        if device_missing > 0:
            print(f"  1. CRITICAL: Download {device_missing} missing device files first.")
            print("     Device files contain manufacturer data needed for analysis.")
            print("     Run: python scripts/audit_downloads.py --download-missing --type device")

        text_missing = len(report.missing_by_type.get("text", []))
        if text_missing > 0:
            print(f"  2. Download {text_missing} missing text files for narrative data.")

        print("\n  To download all missing files:")
        print("     python scripts/audit_downloads.py --download-missing")

    print("=" * 70)


def download_missing(
    report: AuditReport,
    file_types: Optional[List[str]] = None,
    dry_run: bool = False
) -> None:
    """
    Download missing files identified in the audit.

    Args:
        report: Audit report with missing files.
        file_types: Specific file types to download (None = all).
        dry_run: If True, just show what would be downloaded.
    """
    downloader = MAUDEDownloader(output_dir=Path(report.data_dir), use_extended_files=True)

    for file_type, missing_files in report.missing_by_type.items():
        if file_types and file_type not in file_types:
            continue

        if not missing_files:
            continue

        print(f"\n{'[DRY RUN] ' if dry_run else ''}Downloading {len(missing_files)} missing {file_type} files...")

        for filename in missing_files:
            zip_filename = filename + ".zip"
            url = FDA_DOWNLOAD_BASE + zip_filename

            if dry_run:
                print(f"  Would download: {url}")
            else:
                print(f"  Downloading: {zip_filename}")
                result = downloader._download_file(url, file_type)
                if result.success:
                    print(f"    OK - {result.size_bytes / 1024 / 1024:.1f} MB")
                else:
                    print(f"    FAILED: {result.error}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Audit FDA MAUDE downloaded files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--data-dir",
        type=Path,
        default=config.data.raw_path,
        help="Directory containing downloaded files",
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
        "--download-missing",
        action="store_true",
        help="Download missing files after audit",
    )
    parser.add_argument(
        "--type",
        choices=list(KNOWN_FILES.keys()),
        help="Only process specific file type",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be downloaded without downloading",
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
    report = run_audit(args.data_dir)

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

    # Download missing if requested
    if args.download_missing or args.dry_run:
        file_types = [args.type] if args.type else None
        download_missing(report, file_types, dry_run=args.dry_run)

    # Return exit code
    return 0 if report.total_missing == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
