#!/usr/bin/env python
"""
Download historical device files (foidev1998.zip - foidev2019.zip) from FDA MAUDE.

The FDA stores device data in annual files, not a single "thru" file like master events.
This script downloads all historical device files that are missing.

Usage:
    python scripts/download_historical_devices.py [--dry-run]
"""

import argparse
import sys
from pathlib import Path
import requests
import zipfile
import io
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import setup_logging, get_logger

FDA_DOWNLOAD_BASE = "https://www.accessdata.fda.gov/MAUDE/ftparea/"
DEVICE_YEARS = list(range(1998, 2020))  # 1998-2019 (2020+ is in foidev.zip)


def download_and_extract(url: str, output_dir: Path, logger) -> bool:
    """Download a zip file and extract its contents."""
    try:
        logger.info(f"  Downloading {url}...")
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        # Extract zip contents
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            for name in zf.namelist():
                if name.endswith('.txt'):
                    output_path = output_dir / name
                    logger.info(f"    Extracting {name}...")
                    with zf.open(name) as src, open(output_path, 'wb') as dst:
                        dst.write(src.read())
                    logger.info(f"    Saved to {output_path}")
        return True
    except Exception as e:
        logger.error(f"  Error downloading {url}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Download historical FDA device files")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be downloaded")
    parser.add_argument("--data-dir", type=Path, default=config.data.raw_path, help="Output directory")
    parser.add_argument("--years", type=str, help="Comma-separated years to download (e.g., '1998,1999,2000')")

    args = parser.parse_args()

    setup_logging(log_level="INFO")
    logger = get_logger("download_historical_devices")

    logger.info("=" * 60)
    logger.info("Download Historical FDA Device Files")
    logger.info(f"Output directory: {args.data_dir}")
    logger.info("=" * 60)

    args.data_dir.mkdir(parents=True, exist_ok=True)

    # Determine which years to download
    if args.years:
        years = [int(y.strip()) for y in args.years.split(",")]
    else:
        years = DEVICE_YEARS

    # Check which files are missing
    missing_years = []
    for year in years:
        expected_file = args.data_dir / f"foidev{year}.txt"
        if not expected_file.exists():
            missing_years.append(year)
        else:
            logger.info(f"  foidev{year}.txt already exists, skipping")

    if not missing_years:
        logger.info("\nAll historical device files already exist!")
        return 0

    logger.info(f"\nNeed to download {len(missing_years)} files: {missing_years}")

    if args.dry_run:
        logger.info("\n[DRY RUN - No files will be downloaded]")
        for year in missing_years:
            url = f"{FDA_DOWNLOAD_BASE}foidev{year}.zip"
            logger.info(f"  Would download: {url}")
        return 0

    # Download missing files
    success_count = 0
    for i, year in enumerate(missing_years, 1):
        url = f"{FDA_DOWNLOAD_BASE}foidev{year}.zip"
        logger.info(f"\n[{i}/{len(missing_years)}] Downloading foidev{year}.zip...")
        if download_and_extract(url, args.data_dir, logger):
            success_count += 1

    logger.info("\n" + "=" * 60)
    logger.info(f"Download complete: {success_count}/{len(missing_years)} files")
    logger.info("=" * 60)

    if success_count < len(missing_years):
        logger.warning("Some files failed to download. Re-run to retry.")
        return 1

    logger.info("\nNext steps:")
    logger.info("  1. Load the new device files:")
    logger.info("     python scripts/initial_load.py --skip-download --type device")
    logger.info("  2. Populate master_events from devices:")
    logger.info("     python scripts/fix_manufacturer_data.py")

    return 0


if __name__ == "__main__":
    sys.exit(main())
