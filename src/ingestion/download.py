"""Download FDA MAUDE data files."""

import re
import requests
from pathlib import Path
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass
from urllib.parse import urljoin
import zipfile
import io
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_exponential
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger

logger = get_logger("download")


# FDA MAUDE download page URL
FDA_MAUDE_URL = "https://www.fda.gov/medical-devices/maude-database/download-maude-data"

# Base URL for file downloads
FDA_DOWNLOAD_BASE = "https://www.accessdata.fda.gov/MAUDE/ftparea/"

# File type patterns
FILE_PATTERNS = {
    "master": r"mdrfoi(ADD|CHANGE)\d+\.zip",
    "device": r"foidev(ADD|CHANGE)\d+\.zip",
    "patient": r"patient(ADD|CHANGE)\d+\.zip",
    "text": r"foitext(ADD|CHANGE)\d+\.zip",
    "problem": r"foidevproblem(ADD|CHANGE)\d+\.zip",
}

# Known file lists (FDA provides these)
# FDA MAUDE files use format: {type}thru{year}.zip for historical, {type}.zip for current
# See: https://www.fda.gov/medical-devices/maude-database/download-maude-data
KNOWN_FILES = {
    "master": [
        "mdrfoithru2023.zip",  # All historical records through 2023
        "mdrfoi.zip",          # Current year records (updated weekly)
    ],
    "device": [
        "foidevthru2023.zip",  # All historical device records through 2023
        "foidev.zip",          # Current year device records
    ],
    "patient": [
        "patientthru2023.zip",  # All historical patient records through 2023
        "patient.zip",          # Current year patient records
    ],
    "text": [
        "foitextthru2023.zip",  # All historical text records through 2023
        "foitext.zip",          # Current year text records
    ],
    "problem": [
        "foidevproblem.zip",    # Device problem codes (all years)
    ],
}


@dataclass
class DownloadResult:
    """Result of a file download operation."""

    filename: str
    success: bool
    size_bytes: int = 0
    extracted_files: List[str] = None
    error: Optional[str] = None

    def __post_init__(self):
        if self.extracted_files is None:
            self.extracted_files = []


class MAUDEDownloader:
    """Downloads and extracts FDA MAUDE data files."""

    def __init__(self, output_dir: Optional[Path] = None):
        """
        Initialize the downloader.

        Args:
            output_dir: Directory to save downloaded files.
        """
        self.output_dir = output_dir or config.data.raw_path
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        })

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60),
    )
    def _download_file(
        self, url: str, extract: bool = True
    ) -> DownloadResult:
        """
        Download a single file with retry logic.

        Args:
            url: URL to download.
            extract: Whether to extract ZIP files.

        Returns:
            DownloadResult with status and details.
        """
        filename = url.split("/")[-1]
        logger.info(f"Downloading: {filename}")

        try:
            response = self.session.get(url, stream=True, timeout=300)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))

            # Download with progress bar
            content = io.BytesIO()
            with tqdm(
                total=total_size,
                unit="iB",
                unit_scale=True,
                desc=filename,
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    content.write(chunk)
                    pbar.update(len(chunk))

            content.seek(0)
            size_bytes = content.getbuffer().nbytes

            extracted_files = []

            if extract and filename.endswith(".zip"):
                # Extract ZIP file
                extracted_files = self._extract_zip(content, filename)
            else:
                # Save raw file
                output_path = self.output_dir / filename
                with open(output_path, "wb") as f:
                    f.write(content.read())
                extracted_files = [filename]

            return DownloadResult(
                filename=filename,
                success=True,
                size_bytes=size_bytes,
                extracted_files=extracted_files,
            )

        except Exception as e:
            logger.error(f"Error downloading {filename}: {e}")
            return DownloadResult(
                filename=filename,
                success=False,
                error=str(e),
            )

    def _extract_zip(self, content: io.BytesIO, zip_filename: str) -> List[str]:
        """
        Extract a ZIP file to the output directory.

        Args:
            content: ZIP file content as BytesIO.
            zip_filename: Name of the ZIP file (for logging).

        Returns:
            List of extracted filenames.
        """
        extracted = []
        try:
            with zipfile.ZipFile(content) as zf:
                for member in zf.namelist():
                    # Skip directories
                    if member.endswith("/"):
                        continue

                    # Extract file
                    output_path = self.output_dir / member
                    with zf.open(member) as source:
                        with open(output_path, "wb") as target:
                            target.write(source.read())
                    extracted.append(member)
                    logger.debug(f"  Extracted: {member}")

            logger.info(f"Extracted {len(extracted)} files from {zip_filename}")
        except zipfile.BadZipFile as e:
            logger.error(f"Bad ZIP file {zip_filename}: {e}")

        return extracted

    def download_file_type(
        self,
        file_type: str,
        years: Optional[List[int]] = None,
    ) -> List[DownloadResult]:
        """
        Download all files of a specific type.

        Args:
            file_type: One of 'master', 'device', 'patient', 'text', 'problem'.
            years: Optional list of years to download. None = all years.

        Returns:
            List of DownloadResult objects.
        """
        if file_type not in KNOWN_FILES:
            raise ValueError(f"Unknown file type: {file_type}")

        results = []
        files_to_download = KNOWN_FILES[file_type]

        # Filter by years if specified
        if years:
            filtered_files = []
            for f in files_to_download:
                # Check if file matches any year
                for year in years:
                    if str(year) in f or (f.endswith("ADD.zip") and 1991 in years):
                        filtered_files.append(f)
                        break
            files_to_download = filtered_files

        for filename in files_to_download:
            url = FDA_DOWNLOAD_BASE + filename
            result = self._download_file(url)
            results.append(result)

        return results

    def download_all(
        self,
        file_types: Optional[List[str]] = None,
        years: Optional[List[int]] = None,
    ) -> Dict[str, List[DownloadResult]]:
        """
        Download all MAUDE data files.

        Args:
            file_types: List of file types to download. None = all types.
            years: Optional list of years. None = all years.

        Returns:
            Dictionary mapping file type to list of results.
        """
        if file_types is None:
            file_types = list(KNOWN_FILES.keys())

        all_results = {}
        for file_type in file_types:
            logger.info(f"Downloading {file_type} files...")
            results = self.download_file_type(file_type, years)
            all_results[file_type] = results

            # Summary
            success = sum(1 for r in results if r.success)
            failed = sum(1 for r in results if not r.success)
            logger.info(f"  {file_type}: {success} succeeded, {failed} failed")

        return all_results

    def get_existing_files(self) -> Dict[str, List[str]]:
        """
        Get list of already downloaded files.

        Returns:
            Dictionary mapping file type to list of filenames.
        """
        existing = {
            "master": [],
            "device": [],
            "patient": [],
            "text": [],
            "problem": [],
        }

        for filepath in self.output_dir.glob("*.txt"):
            filename = filepath.name.lower()
            if filename.startswith("mdrfoi"):
                existing["master"].append(filepath.name)
            elif filename.startswith("foidev") and "problem" not in filename:
                existing["device"].append(filepath.name)
            elif filename.startswith("patient"):
                existing["patient"].append(filepath.name)
            elif filename.startswith("foitext"):
                existing["text"].append(filepath.name)
            elif "problem" in filename:
                existing["problem"].append(filepath.name)

        return existing

    def check_for_updates(self) -> Dict[str, List[str]]:
        """
        Check for new files not yet downloaded.

        Returns:
            Dictionary mapping file type to list of missing filenames.
        """
        existing = self.get_existing_files()
        missing = {}

        for file_type, known in KNOWN_FILES.items():
            existing_base = {f.replace(".txt", "").lower() for f in existing.get(file_type, [])}

            missing_files = []
            for filename in known:
                base = filename.replace(".zip", "").lower()
                if base not in existing_base:
                    missing_files.append(filename)

            if missing_files:
                missing[file_type] = missing_files

        return missing


def download_sample_data(output_dir: Optional[Path] = None) -> Dict[str, List[DownloadResult]]:
    """
    Download a small sample of MAUDE data for testing.

    Downloads only the most recent year's CHANGE files.

    Args:
        output_dir: Output directory.

    Returns:
        Download results.
    """
    downloader = MAUDEDownloader(output_dir)

    # Just download current year change files for testing
    sample_files = {
        "master": ["mdrfoiCHANGE2026.zip"],
        "device": ["foidevCHANGE2026.zip"],
    }

    results = {}
    for file_type, files in sample_files.items():
        results[file_type] = []
        for filename in files:
            url = FDA_DOWNLOAD_BASE + filename
            result = downloader._download_file(url)
            results[file_type].append(result)

    return results


if __name__ == "__main__":
    # Test download
    import argparse

    parser = argparse.ArgumentParser(description="Download FDA MAUDE files")
    parser.add_argument("--type", choices=list(KNOWN_FILES.keys()), help="File type to download")
    parser.add_argument("--year", type=int, help="Specific year to download")
    parser.add_argument("--sample", action="store_true", help="Download sample data only")
    parser.add_argument("--check", action="store_true", help="Check for missing files")

    args = parser.parse_args()

    downloader = MAUDEDownloader()

    if args.check:
        missing = downloader.check_for_updates()
        print("Missing files:")
        for ftype, files in missing.items():
            print(f"  {ftype}: {len(files)} files")
            for f in files[:5]:
                print(f"    - {f}")
            if len(files) > 5:
                print(f"    ... and {len(files) - 5} more")
    elif args.sample:
        results = download_sample_data()
        print("Sample download results:")
        for ftype, res in results.items():
            for r in res:
                status = "OK" if r.success else f"FAILED: {r.error}"
                print(f"  {r.filename}: {status}")
    elif args.type:
        years = [args.year] if args.year else None
        results = downloader.download_file_type(args.type, years)
        for r in results:
            status = "OK" if r.success else f"FAILED: {r.error}"
            print(f"{r.filename}: {status}")
    else:
        print("Use --help for options")
