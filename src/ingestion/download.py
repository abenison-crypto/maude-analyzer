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
KNOWN_FILES = {
    "master": [
        "mdrfoiADD.zip",  # 1991-1996
        "mdrfoiADD1997.zip",
        "mdrfoiADD1998.zip",
        "mdrfoiADD1999.zip",
        "mdrfoiADD2000.zip",
        "mdrfoiADD2001.zip",
        "mdrfoiADD2002.zip",
        "mdrfoiADD2003.zip",
        "mdrfoiADD2004.zip",
        "mdrfoiADD2005.zip",
        "mdrfoiADD2006.zip",
        "mdrfoiADD2007.zip",
        "mdrfoiADD2008.zip",
        "mdrfoiADD2009.zip",
        "mdrfoiADD2010.zip",
        "mdrfoiADD2011.zip",
        "mdrfoiADD2012.zip",
        "mdrfoiADD2013.zip",
        "mdrfoiADD2014.zip",
        "mdrfoiADD2015.zip",
        "mdrfoiADD2016.zip",
        "mdrfoiADD2017.zip",
        "mdrfoiADD2018.zip",
        "mdrfoiADD2019.zip",
        "mdrfoiADD2020.zip",
        "mdrfoiADD2021.zip",
        "mdrfoiADD2022.zip",
        "mdrfoiADD2023.zip",
        "mdrfoiADD2024.zip",
        "mdrfoiADD2025.zip",
        "mdrfoiCHANGE2026.zip",  # Current year changes
    ],
    "device": [
        "foidevADD.zip",
        "foidevADD1997.zip",
        "foidevADD1998.zip",
        "foidevADD1999.zip",
        "foidevADD2000.zip",
        "foidevADD2001.zip",
        "foidevADD2002.zip",
        "foidevADD2003.zip",
        "foidevADD2004.zip",
        "foidevADD2005.zip",
        "foidevADD2006.zip",
        "foidevADD2007.zip",
        "foidevADD2008.zip",
        "foidevADD2009.zip",
        "foidevADD2010.zip",
        "foidevADD2011.zip",
        "foidevADD2012.zip",
        "foidevADD2013.zip",
        "foidevADD2014.zip",
        "foidevADD2015.zip",
        "foidevADD2016.zip",
        "foidevADD2017.zip",
        "foidevADD2018.zip",
        "foidevADD2019.zip",
        "foidevADD2020.zip",
        "foidevADD2021.zip",
        "foidevADD2022.zip",
        "foidevADD2023.zip",
        "foidevADD2024.zip",
        "foidevADD2025.zip",
        "foidevCHANGE2026.zip",
    ],
    "patient": [
        "patientADD.zip",
        "patientADD1997.zip",
        "patientADD1998.zip",
        "patientADD1999.zip",
        "patientADD2000.zip",
        "patientADD2001.zip",
        "patientADD2002.zip",
        "patientADD2003.zip",
        "patientADD2004.zip",
        "patientADD2005.zip",
        "patientADD2006.zip",
        "patientADD2007.zip",
        "patientADD2008.zip",
        "patientADD2009.zip",
        "patientADD2010.zip",
        "patientADD2011.zip",
        "patientADD2012.zip",
        "patientADD2013.zip",
        "patientADD2014.zip",
        "patientADD2015.zip",
        "patientADD2016.zip",
        "patientADD2017.zip",
        "patientADD2018.zip",
        "patientADD2019.zip",
        "patientADD2020.zip",
        "patientADD2021.zip",
        "patientADD2022.zip",
        "patientADD2023.zip",
        "patientADD2024.zip",
        "patientADD2025.zip",
        "patientCHANGE2026.zip",
    ],
    "text": [
        "foitextADD.zip",
        "foitextADD1997.zip",
        "foitextADD1998.zip",
        "foitextADD1999.zip",
        "foitextADD2000.zip",
        "foitextADD2001.zip",
        "foitextADD2002.zip",
        "foitextADD2003.zip",
        "foitextADD2004.zip",
        "foitextADD2005.zip",
        "foitextADD2006.zip",
        "foitextADD2007.zip",
        "foitextADD2008.zip",
        "foitextADD2009.zip",
        "foitextADD2010.zip",
        "foitextADD2011.zip",
        "foitextADD2012.zip",
        "foitextADD2013.zip",
        "foitextADD2014.zip",
        "foitextADD2015.zip",
        "foitextADD2016.zip",
        "foitextADD2017.zip",
        "foitextADD2018.zip",
        "foitextADD2019.zip",
        "foitextADD2020.zip",
        "foitextADD2021.zip",
        "foitextADD2022.zip",
        "foitextADD2023.zip",
        "foitextADD2024.zip",
        "foitextADD2025.zip",
        "foitextCHANGE2026.zip",
    ],
    "problem": [
        "foidevproblem.zip",  # All years in one file typically
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
