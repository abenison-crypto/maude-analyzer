#!/usr/bin/env python3
"""
Build FDA MAUDE file manifest by discovering available files.

This script:
1. Uses known file patterns from download.py
2. Optionally checks FDA website for file availability
3. Builds a complete manifest JSON for validation
4. Stores checksums when files are downloaded

Usage:
    python scripts/build_fda_manifest.py
    python scripts/build_fda_manifest.py --check-remote
    python scripts/build_fda_manifest.py --output manifest.json
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional
import hashlib

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from src.ingestion.download import (
    KNOWN_FILES,
    KNOWN_FILES_EXTENDED,
    INCREMENTAL_FILES,
    FDA_DOWNLOAD_BASE,
    MAUDEDownloader,
)

logger = get_logger("manifest")


@dataclass
class ManifestFile:
    """Metadata for a single FDA file."""
    filename: str
    file_type: str
    url: str
    size_bytes: Optional[int] = None
    checksum_md5: Optional[str] = None
    last_modified_remote: Optional[str] = None
    last_verified: Optional[str] = None
    is_available: bool = True
    is_current: bool = False  # Updated weekly
    is_incremental: bool = False  # ADD/CHANGE file
    year_start: Optional[int] = None
    year_end: Optional[int] = None
    notes: Optional[str] = None


@dataclass
class FDAManifest:
    """Complete manifest of FDA MAUDE files."""
    version: str = "1.0"
    generated_at: str = ""
    last_remote_check: Optional[str] = None
    base_url: str = FDA_DOWNLOAD_BASE
    files: List[ManifestFile] = field(default_factory=list)
    file_count: int = 0
    total_size_mb: Optional[float] = None

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON export."""
        return {
            "version": self.version,
            "generated_at": self.generated_at,
            "last_remote_check": self.last_remote_check,
            "base_url": self.base_url,
            "summary": {
                "file_count": self.file_count,
                "total_size_mb": self.total_size_mb,
                "by_type": self._count_by_type(),
            },
            "files": [asdict(f) for f in self.files],
        }

    def _count_by_type(self) -> Dict[str, int]:
        """Count files by type."""
        counts = {}
        for f in self.files:
            counts[f.file_type] = counts.get(f.file_type, 0) + 1
        return counts


def extract_year_range(filename: str, file_type: str) -> tuple[Optional[int], Optional[int]]:
    """
    Extract year range from filename.

    Returns:
        Tuple of (year_start, year_end) or (None, None)
    """
    import re
    filename_lower = filename.lower()

    # Handle "thru" files: mdrfoithru2023.zip -> (None, 2023)
    match = re.search(r'thru(\d{4})\.', filename_lower)
    if match:
        return (None, int(match.group(1)))

    # Handle annual files: foidev2019.zip -> (2019, 2019)
    match = re.search(r'(\d{4})\.zip$', filename_lower)
    if match:
        year = int(match.group(1))
        return (year, year)

    # Handle DEN legacy: mdr84.zip -> (1984, 1984)
    match = re.search(r'mdr(\d{2})\.zip$', filename_lower)
    if match:
        year_2d = int(match.group(1))
        if 84 <= year_2d <= 97:
            year = 1900 + year_2d
            return (year, year)

    # Handle ASR: ASR_2015.zip -> (2015, 2015)
    match = re.search(r'asr_(\d{4})\.', filename_lower)
    if match:
        year = int(match.group(1))
        return (year, year)

    # Current year files
    if filename_lower in {"mdrfoi.zip", "foidev.zip", "patient.zip", "foitext.zip"}:
        current_year = datetime.now().year
        return (current_year, current_year)

    return (None, None)


def is_current_file(filename: str) -> bool:
    """Check if file is updated weekly (current year)."""
    current_files = {
        "mdrfoi.zip", "foidev.zip", "patient.zip", "foitext.zip",
    }
    return filename.lower() in current_files


def is_incremental_file(filename: str) -> bool:
    """Check if file is an incremental update file."""
    incremental_patterns = {"add", "change"}
    filename_lower = filename.lower()
    return any(p in filename_lower for p in incremental_patterns)


def build_manifest(
    include_extended: bool = True,
    include_incremental: bool = False,
) -> FDAManifest:
    """
    Build manifest from known file definitions.

    Args:
        include_extended: Include all annual files (vs just core files).
        include_incremental: Include weekly ADD/CHANGE files.

    Returns:
        FDAManifest with all file metadata.
    """
    manifest = FDAManifest(
        generated_at=datetime.now().isoformat(),
    )

    # Choose file list
    known_files = KNOWN_FILES_EXTENDED if include_extended else KNOWN_FILES

    # Add main files
    for file_type, filenames in known_files.items():
        for filename in filenames:
            year_start, year_end = extract_year_range(filename, file_type)

            file_entry = ManifestFile(
                filename=filename,
                file_type=file_type,
                url=FDA_DOWNLOAD_BASE + filename,
                is_current=is_current_file(filename),
                is_incremental=False,
                year_start=year_start,
                year_end=year_end,
            )

            # Add notes for special files
            if "thru" in filename.lower():
                file_entry.notes = f"Historical data through {year_end}"
            elif file_entry.is_current:
                file_entry.notes = "Current year data, updated weekly"

            manifest.files.append(file_entry)

    # Add incremental files if requested
    if include_incremental:
        for file_type, categories in INCREMENTAL_FILES.items():
            for category, filenames in categories.items():
                for filename in filenames:
                    file_entry = ManifestFile(
                        filename=filename,
                        file_type=file_type,
                        url=FDA_DOWNLOAD_BASE + filename,
                        is_current=True,
                        is_incremental=True,
                        notes=f"Weekly {category.upper()} file",
                    )
                    manifest.files.append(file_entry)

    manifest.file_count = len(manifest.files)

    return manifest


def check_remote_availability(
    manifest: FDAManifest,
    timeout: int = 30,
    sample_only: bool = False,
) -> FDAManifest:
    """
    Check remote file availability using HEAD requests.

    Args:
        manifest: Manifest to check.
        timeout: Request timeout in seconds.
        sample_only: Only check a sample of files (faster).

    Returns:
        Updated manifest with availability and size info.
    """
    import requests

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    })

    files_to_check = manifest.files
    if sample_only:
        # Check first file of each type + current files
        checked_types = set()
        files_to_check = []
        for f in manifest.files:
            if f.file_type not in checked_types or f.is_current:
                files_to_check.append(f)
                checked_types.add(f.file_type)

    total_size = 0
    checked_count = 0

    for file_entry in files_to_check:
        try:
            response = session.head(file_entry.url, timeout=timeout, allow_redirects=True)

            if response.status_code == 200:
                file_entry.is_available = True

                # Get size
                content_length = response.headers.get("Content-Length")
                if content_length:
                    file_entry.size_bytes = int(content_length)
                    total_size += file_entry.size_bytes

                # Get last modified
                last_modified = response.headers.get("Last-Modified")
                if last_modified:
                    file_entry.last_modified_remote = last_modified

                file_entry.last_verified = datetime.now().isoformat()

            elif response.status_code == 404:
                file_entry.is_available = False
                logger.warning(f"File not found: {file_entry.filename}")

            else:
                logger.warning(f"Unexpected status {response.status_code} for {file_entry.filename}")

            checked_count += 1
            if checked_count % 10 == 0:
                logger.info(f"Checked {checked_count} files...")

        except Exception as e:
            logger.warning(f"Error checking {file_entry.filename}: {e}")

    manifest.last_remote_check = datetime.now().isoformat()
    manifest.total_size_mb = round(total_size / 1024 / 1024, 1) if total_size > 0 else None

    return manifest


def update_manifest_with_local(
    manifest: FDAManifest,
    data_dir: Path,
) -> FDAManifest:
    """
    Update manifest with local file checksums and sizes.

    Args:
        manifest: Manifest to update.
        data_dir: Directory containing downloaded files.

    Returns:
        Updated manifest with local file info.
    """
    for file_entry in manifest.files:
        # Look for extracted .txt file
        txt_name = file_entry.filename.replace(".zip", ".txt")
        txt_path = data_dir / txt_name

        if txt_path.exists():
            file_entry.size_bytes = txt_path.stat().st_size

            # Calculate MD5 checksum (for smaller files)
            if file_entry.size_bytes < 100 * 1024 * 1024:  # Only for <100MB files
                try:
                    with open(txt_path, "rb") as f:
                        file_entry.checksum_md5 = hashlib.md5(f.read()).hexdigest()
                except Exception as e:
                    logger.warning(f"Error calculating checksum for {txt_name}: {e}")

    return manifest


def load_manifest(filepath: Path) -> Optional[FDAManifest]:
    """
    Load manifest from JSON file.

    Args:
        filepath: Path to manifest JSON.

    Returns:
        FDAManifest or None if file doesn't exist.
    """
    if not filepath.exists():
        return None

    try:
        with open(filepath, "r") as f:
            data = json.load(f)

        manifest = FDAManifest(
            version=data.get("version", "1.0"),
            generated_at=data.get("generated_at", ""),
            last_remote_check=data.get("last_remote_check"),
            base_url=data.get("base_url", FDA_DOWNLOAD_BASE),
        )

        for f_data in data.get("files", []):
            manifest.files.append(ManifestFile(**f_data))

        manifest.file_count = len(manifest.files)
        manifest.total_size_mb = data.get("summary", {}).get("total_size_mb")

        return manifest

    except Exception as e:
        logger.error(f"Error loading manifest: {e}")
        return None


def save_manifest(manifest: FDAManifest, filepath: Path) -> None:
    """
    Save manifest to JSON file.

    Args:
        manifest: Manifest to save.
        filepath: Output path.
    """
    with open(filepath, "w") as f:
        json.dump(manifest.to_dict(), f, indent=2)

    logger.info(f"Manifest saved to: {filepath}")


def print_manifest_summary(manifest: FDAManifest) -> None:
    """Print manifest summary to console."""
    print("=" * 70)
    print("FDA MAUDE FILE MANIFEST")
    print("=" * 70)
    print(f"\nGenerated: {manifest.generated_at}")
    print(f"Last Remote Check: {manifest.last_remote_check or 'Never'}")
    print(f"Base URL: {manifest.base_url}")

    print("\n" + "-" * 70)
    print("SUMMARY")
    print("-" * 70)
    print(f"  Total files: {manifest.file_count}")
    if manifest.total_size_mb:
        print(f"  Total size:  {manifest.total_size_mb:.1f} MB")

    print("\n  By type:")
    by_type = {}
    for f in manifest.files:
        by_type[f.file_type] = by_type.get(f.file_type, 0) + 1

    for ftype, count in sorted(by_type.items()):
        print(f"    {ftype:20s}: {count:>4} files")

    print("\n" + "-" * 70)
    print("FILE TYPES")
    print("-" * 70)

    for file_type in sorted(by_type.keys()):
        files = [f for f in manifest.files if f.file_type == file_type]
        current = [f for f in files if f.is_current]
        available = [f for f in files if f.is_available]

        print(f"\n  {file_type.upper()}:")
        print(f"    Total: {len(files)}, Available: {len(available)}, Current: {len(current)}")

        # Show year range
        years = [f.year_end for f in files if f.year_end]
        if years:
            print(f"    Year range: {min(years)}-{max(years)}")

        # Show a few examples
        sample = files[:3]
        for f in sample:
            status = "current" if f.is_current else ""
            if not f.is_available:
                status = "NOT AVAILABLE"
            print(f"      - {f.filename} {status}")
        if len(files) > 3:
            print(f"      ... and {len(files) - 3} more")

    print("\n" + "=" * 70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Build FDA MAUDE file manifest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--output", "-o",
        type=Path,
        default=config.data.raw_path / "fda_manifest.json",
        help="Output path for manifest JSON",
    )
    parser.add_argument(
        "--check-remote",
        action="store_true",
        help="Check remote file availability (slow)",
    )
    parser.add_argument(
        "--sample-only",
        action="store_true",
        help="Only check a sample of files when checking remote",
    )
    parser.add_argument(
        "--update-local",
        action="store_true",
        help="Update manifest with local file checksums",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=config.data.raw_path,
        help="Directory containing downloaded files",
    )
    parser.add_argument(
        "--include-incremental",
        action="store_true",
        help="Include weekly ADD/CHANGE files in manifest",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output only JSON (no summary)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )

    args = parser.parse_args()

    setup_logging(log_level=args.log_level)

    # Build manifest
    logger.info("Building FDA file manifest...")
    manifest = build_manifest(
        include_extended=True,
        include_incremental=args.include_incremental,
    )

    # Check remote availability if requested
    if args.check_remote:
        logger.info("Checking remote file availability...")
        manifest = check_remote_availability(
            manifest,
            sample_only=args.sample_only,
        )

    # Update with local info if requested
    if args.update_local:
        logger.info("Updating with local file information...")
        manifest = update_manifest_with_local(manifest, args.data_dir)

    # Output
    if args.json:
        print(json.dumps(manifest.to_dict(), indent=2))
    else:
        print_manifest_summary(manifest)

    # Save to file
    save_manifest(manifest, args.output)
    if not args.json:
        print(f"\nManifest saved to: {args.output}")


if __name__ == "__main__":
    main()
