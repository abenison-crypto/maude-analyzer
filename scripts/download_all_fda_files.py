#!/usr/bin/env python
"""
Comprehensive FDA MAUDE file download script.

This script downloads ALL FDA MAUDE data files including:
- Master event files (current + historical through 2025)
- Device files (1997 through current)
- Patient files
- Text/narrative files (1995 through current + annual)
- Problem code files (device and patient)
- ASR (Alternative Summary Reports) 1999-2019
- DEN (Device Experience Network) legacy files 1984-1997
- Lookup tables (deviceproblemcodes, patientproblemdata)

Usage:
    python scripts/download_all_fda_files.py [options]

Options:
    --data-dir PATH     Output directory for downloaded files
    --file-types TYPES  Comma-separated list of file types to download
    --verify-only       Only verify file availability, don't download
    --force             Force re-download even if files exist
    --include-asr       Include ASR files (21 annual files + ASR_PPC)
    --include-den       Include DEN legacy files (1984-1997)
    --include-all       Download all available files (default)
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger
from src.ingestion.download import (
    MAUDEDownloader,
    KNOWN_FILES,
    INCREMENTAL_FILES,
    FDA_DOWNLOAD_BASE,
)


def verify_file_availability(
    file_types: Optional[List[str]] = None,
    timeout: int = 30,
) -> Dict[str, Dict[str, any]]:
    """
    Verify which files are available on the FDA server.

    Args:
        file_types: List of file types to check. None = all types.
        timeout: Request timeout in seconds.

    Returns:
        Dictionary mapping filename to availability info.
    """
    logger = get_logger("download_all")
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
    })

    if file_types is None:
        file_types = list(KNOWN_FILES.keys())

    results = {}

    for file_type in file_types:
        files = KNOWN_FILES.get(file_type, [])
        logger.info(f"Checking {len(files)} {file_type} files...")

        for filename in files:
            url = FDA_DOWNLOAD_BASE + filename

            try:
                response = session.head(url, timeout=timeout, allow_redirects=True)

                if response.status_code == 200:
                    size = response.headers.get("Content-Length")
                    last_modified = response.headers.get("Last-Modified")
                    results[filename] = {
                        "available": True,
                        "size_bytes": int(size) if size else None,
                        "last_modified": last_modified,
                        "url": url,
                        "file_type": file_type,
                    }
                    logger.debug(f"  OK: {filename} ({size} bytes)")
                elif response.status_code == 404:
                    results[filename] = {
                        "available": False,
                        "error": "Not found (404)",
                        "url": url,
                        "file_type": file_type,
                    }
                    logger.warning(f"  NOT FOUND: {filename}")
                else:
                    results[filename] = {
                        "available": False,
                        "error": f"HTTP {response.status_code}",
                        "url": url,
                        "file_type": file_type,
                    }
                    logger.warning(f"  ERROR: {filename} - HTTP {response.status_code}")

            except Exception as e:
                results[filename] = {
                    "available": False,
                    "error": str(e),
                    "url": url,
                    "file_type": file_type,
                }
                logger.warning(f"  ERROR: {filename} - {e}")

    return results


def print_verification_summary(results: Dict[str, Dict]) -> None:
    """Print a summary of file availability verification."""
    logger = get_logger("download_all")

    # Group by file type
    by_type = {}
    for filename, info in results.items():
        file_type = info.get("file_type", "unknown")
        if file_type not in by_type:
            by_type[file_type] = {"available": [], "missing": []}

        if info.get("available"):
            by_type[file_type]["available"].append(filename)
        else:
            by_type[file_type]["missing"].append(filename)

    print("\n" + "=" * 60)
    print("FDA File Availability Summary")
    print("=" * 60)

    total_available = 0
    total_missing = 0
    total_size = 0

    for file_type, data in by_type.items():
        available = len(data["available"])
        missing = len(data["missing"])
        total_available += available
        total_missing += missing

        # Calculate total size for this type
        type_size = sum(
            results[f].get("size_bytes", 0) or 0
            for f in data["available"]
        )
        total_size += type_size

        print(f"\n{file_type.upper()}:")
        print(f"  Available: {available}")
        print(f"  Missing: {missing}")
        print(f"  Size: {type_size / 1024 / 1024:.1f} MB")

        if missing > 0:
            print(f"  Missing files:")
            for f in data["missing"][:5]:
                print(f"    - {f}")
            if missing > 5:
                print(f"    ... and {missing - 5} more")

    print("\n" + "-" * 60)
    print(f"TOTAL: {total_available} available, {total_missing} missing")
    print(f"TOTAL SIZE: {total_size / 1024 / 1024 / 1024:.2f} GB")
    print("=" * 60)


def main():
    """Main entry point for comprehensive FDA download."""
    parser = argparse.ArgumentParser(
        description="Download all FDA MAUDE data files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--data-dir",
        type=Path,
        default=config.data.raw_path,
        help="Output directory for downloaded files",
    )
    parser.add_argument(
        "--file-types",
        type=str,
        help="Comma-separated list of file types (master,device,patient,text,problem,asr,den)",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify file availability, don't download",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if files exist",
    )
    parser.add_argument(
        "--include-asr",
        action="store_true",
        help="Include ASR files (21 annual files + ASR_PPC)",
    )
    parser.add_argument(
        "--include-den",
        action="store_true",
        help="Include DEN legacy files (1984-1997)",
    )
    parser.add_argument(
        "--include-all",
        action="store_true",
        default=True,
        help="Download all available files (default)",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(log_level=args.log_level)
    logger = get_logger("download_all")

    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info("FDA MAUDE Comprehensive Download")
    logger.info(f"Started at: {start_time}")
    logger.info("=" * 60)

    # Determine file types to process
    if args.file_types:
        file_types = [t.strip() for t in args.file_types.split(",")]
    elif args.include_all:
        file_types = list(KNOWN_FILES.keys())
    else:
        # Base types (always included)
        file_types = ["master", "device", "patient", "text", "problem", "problem_lookup", "patient_problem"]
        if args.include_asr:
            file_types.append("asr")
        if args.include_den:
            file_types.append("den")

    logger.info(f"File types to process: {', '.join(file_types)}")
    logger.info(f"Output directory: {args.data_dir}")

    # Ensure output directory exists
    args.data_dir.mkdir(parents=True, exist_ok=True)

    # Verify file availability first
    logger.info("\nVerifying file availability on FDA server...")
    availability = verify_file_availability(file_types)
    print_verification_summary(availability)

    if args.verify_only:
        logger.info("\nVerification only mode - skipping download.")
        return 0

    # Count available files
    available_files = [f for f, info in availability.items() if info.get("available")]
    logger.info(f"\nReady to download {len(available_files)} files")

    # Estimate total download size
    total_size = sum(
        availability[f].get("size_bytes", 0) or 0
        for f in available_files
    )
    logger.info(f"Estimated total download size: {total_size / 1024 / 1024 / 1024:.2f} GB")

    # Proceed with download
    try:
        downloader = MAUDEDownloader(
            output_dir=args.data_dir,
            use_extended_files=True,
        )

        results = downloader.download_all(
            file_types=file_types,
            force=args.force,
        )

        # Print summary
        print("\n" + "=" * 60)
        print("Download Summary")
        print("=" * 60)

        total_downloaded = 0
        total_failed = 0
        total_bytes = 0

        for file_type, res_list in results.items():
            success = sum(1 for r in res_list if r.success)
            failed = sum(1 for r in res_list if not r.success)
            size = sum(r.size_bytes for r in res_list if r.success)

            total_downloaded += success
            total_failed += failed
            total_bytes += size

            print(f"\n{file_type.upper()}:")
            print(f"  Downloaded: {success}")
            print(f"  Failed: {failed}")
            print(f"  Size: {size / 1024 / 1024:.1f} MB")

            # Show failures
            failures = [r for r in res_list if not r.success]
            if failures:
                print(f"  Failed files:")
                for r in failures[:5]:
                    print(f"    - {r.filename}: {r.error}")
                if len(failures) > 5:
                    print(f"    ... and {len(failures) - 5} more")

        print("\n" + "-" * 60)
        print(f"TOTAL: {total_downloaded} downloaded, {total_failed} failed")
        print(f"TOTAL SIZE: {total_bytes / 1024 / 1024 / 1024:.2f} GB")

        # Also download incremental files
        logger.info("\nDownloading incremental (weekly) files...")
        incremental_results = downloader.download_incremental_files(
            file_types=file_types,
            force=args.force,
        )

        for file_type, res_list in incremental_results.items():
            success = sum(1 for r in res_list if r.success)
            if success > 0:
                logger.info(f"  {file_type}: {success} incremental files downloaded")

        end_time = datetime.now()
        duration = end_time - start_time
        print("\n" + "=" * 60)
        print(f"Completed at: {end_time}")
        print(f"Total duration: {duration}")
        print("=" * 60)

        return 0 if total_failed == 0 else 1

    except KeyboardInterrupt:
        logger.warning("\nDownload interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"\nError during download: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
