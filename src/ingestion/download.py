"""Download FDA MAUDE data files with progress tracking and resumability."""

import hashlib
import json
import re
import requests
from pathlib import Path
from typing import List, Optional, Dict, Tuple
from dataclasses import dataclass, field
from datetime import datetime
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

# =============================================================================
# Complete FDA MAUDE File Lists
# =============================================================================

# Historical files (thru{year}) - updated annually
# Current year files - updated weekly
# Annual device/text files - for specific years

# Generate year ranges for annual files
DEVICE_YEARS = list(range(1998, 2026))  # 1998-2025
TEXT_YEARS = list(range(1996, 2026))    # 1996-2025

KNOWN_FILES = {
    "master": [
        # Historical through 2024 (all records before 2025)
        "mdrfoithru2024.zip",
        # Current year (2025) - updated weekly
        "mdrfoi.zip",
    ],
    "device": [
        # Historical through 2024
        "foidevthru2024.zip",
        # Current year
        "foidev.zip",
    ],
    "patient": [
        # Historical through 2024
        "patientthru2024.zip",
        # Current year
        "patient.zip",
    ],
    "text": [
        # Historical through 2024
        "foitextthru2024.zip",
        # Current year
        "foitext.zip",
    ],
    "problem": [
        # Device problem codes (all years in one file)
        "foidevproblem.zip",
    ],
}

# Weekly incremental update files (ADD = new records, CHANGE = updates to existing)
# These files are released weekly on Thursdays and contain only delta changes
# ADD files contain new MDR records submitted during the week
# CHANGE files contain corrections/updates to existing records
INCREMENTAL_FILES = {
    "master": {
        "add": ["mdrfoiAdd.zip"],
        "change": ["mdrfoiChange.zip"],
    },
    "device": {
        "add": ["foidevAdd.zip"],
        "change": [],  # Device doesn't have change files
    },
    "patient": {
        "add": ["patientAdd.zip"],
        "change": ["patientChange.zip"],
    },
    "text": {
        "add": ["foitextAdd.zip"],
        "change": [],  # Text doesn't have change files
    },
    "problem": {
        "add": [],
        "change": [],
    },
}

# Extended file list including annual files (for full historical load)
KNOWN_FILES_EXTENDED = {
    "master": KNOWN_FILES["master"].copy(),
    "device": KNOWN_FILES["device"] + [f"foidev{year}.zip" for year in DEVICE_YEARS],
    "patient": KNOWN_FILES["patient"].copy(),
    "text": KNOWN_FILES["text"] + [f"foitext{year}.zip" for year in TEXT_YEARS],
    "problem": KNOWN_FILES["problem"].copy(),
}

# Lookup files (problem code descriptions)
LOOKUP_FILES = {
    "problem_codes": "deviceproblemcodes.txt",
}


@dataclass
class DownloadResult:
    """Result of a file download operation."""

    filename: str
    success: bool
    size_bytes: int = 0
    extracted_files: List[str] = None
    error: Optional[str] = None
    checksum: Optional[str] = None
    duration_seconds: float = 0

    def __post_init__(self):
        if self.extracted_files is None:
            self.extracted_files = []


@dataclass
class DownloadState:
    """Persistent state for tracking downloads."""

    filename: str
    file_type: str
    url: str
    status: str = "pending"  # pending, downloading, completed, failed
    size_bytes: int = 0
    checksum: Optional[str] = None
    download_started: Optional[datetime] = None
    download_completed: Optional[datetime] = None
    error_message: Optional[str] = None
    extracted_files: List[str] = field(default_factory=list)


class DownloadTracker:
    """Track download progress for resumability."""

    def __init__(self, state_file: Optional[Path] = None):
        """
        Initialize the download tracker.

        Args:
            state_file: Path to JSON state file.
        """
        self.state_file = state_file or (config.data.raw_path / ".download_state.json")
        self._state: Dict[str, Dict] = {}
        self._load_state()

    def _load_state(self) -> None:
        """Load state from file."""
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    self._state = json.load(f)
            except Exception as e:
                logger.warning(f"Could not load download state: {e}")
                self._state = {}

    def _save_state(self) -> None:
        """Save state to file."""
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, "w") as f:
                json.dump(self._state, f, indent=2, default=str)
        except Exception as e:
            logger.warning(f"Could not save download state: {e}")

    def mark_started(self, filename: str, file_type: str, url: str) -> None:
        """Mark a download as started."""
        self._state[filename] = {
            "file_type": file_type,
            "url": url,
            "status": "downloading",
            "started": datetime.now().isoformat(),
        }
        self._save_state()

    def mark_complete(
        self,
        filename: str,
        checksum: str,
        size_bytes: int,
        extracted_files: List[str],
        last_modified_remote: Optional[datetime] = None,
    ) -> None:
        """Mark a download as complete."""
        if filename in self._state:
            self._state[filename].update({
                "status": "completed",
                "checksum": checksum,
                "size_bytes": size_bytes,
                "completed": datetime.now().isoformat(),
                "extracted_files": extracted_files,
                "last_modified_remote": last_modified_remote.isoformat() if last_modified_remote else None,
            })
            self._save_state()

    def mark_failed(self, filename: str, error: str) -> None:
        """Mark a download as failed."""
        if filename in self._state:
            self._state[filename].update({
                "status": "failed",
                "error": error,
                "failed_at": datetime.now().isoformat(),
            })
            self._save_state()

    def is_downloaded(self, filename: str) -> bool:
        """Check if a file has been successfully downloaded."""
        state = self._state.get(filename, {})
        return state.get("status") == "completed"

    def get_incomplete(self) -> List[str]:
        """Get list of incomplete downloads."""
        incomplete = []
        for filename, state in self._state.items():
            if state.get("status") not in ["completed"]:
                incomplete.append(filename)
        return incomplete

    def get_state(self, filename: str) -> Optional[Dict]:
        """Get state for a specific file."""
        return self._state.get(filename)

    def get_last_modified_remote(self, filename: str) -> Optional[datetime]:
        """Get the remote Last-Modified time for a file."""
        state = self._state.get(filename, {})
        last_mod = state.get("last_modified_remote")
        if last_mod:
            return datetime.fromisoformat(last_mod)
        return None

    def needs_refresh(self, filename: str, remote_last_modified: datetime) -> bool:
        """
        Check if a file needs to be re-downloaded based on remote Last-Modified.

        Args:
            filename: Name of the file.
            remote_last_modified: Last-Modified time from server.

        Returns:
            True if file should be re-downloaded.
        """
        local_modified = self.get_last_modified_remote(filename)
        if local_modified is None:
            return True
        return remote_last_modified > local_modified

    def clear_state(self, filename: Optional[str] = None) -> None:
        """Clear state for a file or all files."""
        if filename:
            self._state.pop(filename, None)
        else:
            self._state = {}
        self._save_state()


class MAUDEDownloader:
    """Downloads and extracts FDA MAUDE data files with progress tracking."""

    def __init__(
        self,
        output_dir: Optional[Path] = None,
        use_extended_files: bool = False
    ):
        """
        Initialize the downloader.

        Args:
            output_dir: Directory to save downloaded files.
            use_extended_files: If True, include all annual files.
        """
        self.output_dir = output_dir or config.data.raw_path
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        })
        self.tracker = DownloadTracker(self.output_dir / ".download_state.json")
        self.known_files = KNOWN_FILES_EXTENDED if use_extended_files else KNOWN_FILES

    def _compute_checksum(self, content: bytes) -> str:
        """Compute MD5 checksum of content."""
        return hashlib.md5(content).hexdigest()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60),
    )
    def _download_file(
        self,
        url: str,
        file_type: str,
        extract: bool = True,
        force: bool = False,
    ) -> DownloadResult:
        """
        Download a single file with retry logic and progress tracking.

        Args:
            url: URL to download.
            file_type: Type of MAUDE file.
            extract: Whether to extract ZIP files.
            force: Force re-download even if already downloaded.

        Returns:
            DownloadResult with status and details.
        """
        filename = url.split("/")[-1]
        start_time = datetime.now()

        # Check if already downloaded
        if not force and self.tracker.is_downloaded(filename):
            state = self.tracker.get_state(filename)
            logger.info(f"Skipping {filename} (already downloaded)")
            return DownloadResult(
                filename=filename,
                success=True,
                size_bytes=state.get("size_bytes", 0),
                extracted_files=state.get("extracted_files", []),
                checksum=state.get("checksum"),
            )

        logger.info(f"Downloading: {filename}")
        self.tracker.mark_started(filename, file_type, url)

        try:
            response = self.session.get(url, stream=True, timeout=300)
            response.raise_for_status()

            total_size = int(response.headers.get("content-length", 0))

            # Capture Last-Modified header for freshness tracking
            last_modified_remote = None
            if "Last-Modified" in response.headers:
                try:
                    last_modified_remote = datetime.strptime(
                        response.headers["Last-Modified"],
                        "%a, %d %b %Y %H:%M:%S %Z"
                    )
                except ValueError:
                    pass

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
            raw_content = content.read()
            size_bytes = len(raw_content)
            checksum = self._compute_checksum(raw_content)

            content.seek(0)
            extracted_files = []

            if extract and filename.endswith(".zip"):
                # Extract ZIP file
                extracted_files = self._extract_zip(content, filename)
            else:
                # Save raw file
                output_path = self.output_dir / filename
                with open(output_path, "wb") as f:
                    f.write(raw_content)
                extracted_files = [filename]

            duration = (datetime.now() - start_time).total_seconds()

            # Mark as complete with remote timestamp
            self.tracker.mark_complete(
                filename, checksum, size_bytes, extracted_files, last_modified_remote
            )

            return DownloadResult(
                filename=filename,
                success=True,
                size_bytes=size_bytes,
                extracted_files=extracted_files,
                checksum=checksum,
                duration_seconds=duration,
            )

        except Exception as e:
            logger.error(f"Error downloading {filename}: {e}")
            self.tracker.mark_failed(filename, str(e))
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
        force: bool = False,
    ) -> List[DownloadResult]:
        """
        Download all files of a specific type.

        Args:
            file_type: One of 'master', 'device', 'patient', 'text', 'problem'.
            years: Optional list of years to download. None = all available.
            force: Force re-download even if already downloaded.

        Returns:
            List of DownloadResult objects.
        """
        if file_type not in self.known_files:
            raise ValueError(f"Unknown file type: {file_type}")

        results = []
        files_to_download = self.known_files[file_type].copy()

        # Filter by years if specified
        if years:
            filtered_files = []
            for f in files_to_download:
                # Match files containing the year
                for year in years:
                    if str(year) in f:
                        filtered_files.append(f)
                        break
                    # Also include "thru" files for the year
                    if "thru" in f and int(f.split("thru")[-1].split(".")[0]) >= year:
                        if f not in filtered_files:
                            filtered_files.append(f)
                        break
            files_to_download = filtered_files

        for filename in files_to_download:
            url = FDA_DOWNLOAD_BASE + filename
            result = self._download_file(url, file_type, force=force)
            results.append(result)

        return results

    def download_all(
        self,
        file_types: Optional[List[str]] = None,
        years: Optional[List[int]] = None,
        force: bool = False,
    ) -> Dict[str, List[DownloadResult]]:
        """
        Download all MAUDE data files.

        Args:
            file_types: List of file types to download. None = all types.
            years: Optional list of years. None = all available.
            force: Force re-download even if already downloaded.

        Returns:
            Dictionary mapping file type to list of results.
        """
        if file_types is None:
            file_types = list(self.known_files.keys())

        all_results = {}
        for file_type in file_types:
            logger.info(f"\nDownloading {file_type} files...")
            results = self.download_file_type(file_type, years, force=force)
            all_results[file_type] = results

            # Summary
            success = sum(1 for r in results if r.success)
            failed = sum(1 for r in results if not r.success)
            total_size = sum(r.size_bytes for r in results if r.success)
            logger.info(
                f"  {file_type}: {success} succeeded, {failed} failed, "
                f"{total_size / 1024 / 1024:.1f} MB"
            )

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
            if "mdrfoi" in filename:
                existing["master"].append(filepath.name)
            elif "foidev" in filename and "problem" not in filename:
                existing["device"].append(filepath.name)
            elif filename.startswith("patient"):
                existing["patient"].append(filepath.name)
            elif "foitext" in filename:
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

        for file_type, known in self.known_files.items():
            existing_base = {
                f.replace(".txt", "").lower()
                for f in existing.get(file_type, [])
            }

            missing_files = []
            for filename in known:
                base = filename.replace(".zip", "").lower()
                # Check if the extracted file exists
                if base not in existing_base:
                    # Also check if download was completed
                    if not self.tracker.is_downloaded(filename):
                        missing_files.append(filename)

            if missing_files:
                missing[file_type] = missing_files

        return missing

    def check_remote_freshness(
        self,
        filenames: Optional[List[str]] = None,
        timeout: int = 30,
    ) -> Dict[str, Dict]:
        """
        Check freshness of files by comparing local state with remote Last-Modified headers.

        Uses HTTP HEAD requests to check without downloading.

        Args:
            filenames: List of filenames to check. If None, checks all known files.
            timeout: HTTP request timeout in seconds.

        Returns:
            Dictionary mapping filename to freshness info:
                - needs_update: bool
                - local_modified: datetime or None
                - remote_modified: datetime or None
                - size_bytes: int or None
        """
        result = {}

        if filenames is None:
            filenames = []
            for files in self.known_files.values():
                filenames.extend(files)

        for filename in filenames:
            url = FDA_DOWNLOAD_BASE + filename
            info = {
                "needs_update": False,
                "local_modified": self.tracker.get_last_modified_remote(filename),
                "remote_modified": None,
                "size_bytes": None,
            }

            try:
                response = self.session.head(url, timeout=timeout, allow_redirects=True)

                if response.status_code == 404:
                    logger.debug(f"File not found on server: {filename}")
                    continue

                response.raise_for_status()

                # Parse Last-Modified header
                if "Last-Modified" in response.headers:
                    info["remote_modified"] = datetime.strptime(
                        response.headers["Last-Modified"],
                        "%a, %d %b %Y %H:%M:%S %Z"
                    )

                # Get content length
                content_length = response.headers.get("Content-Length")
                info["size_bytes"] = int(content_length) if content_length else None

                # Check if update needed
                if info["remote_modified"]:
                    if info["local_modified"] is None:
                        info["needs_update"] = True
                    elif info["remote_modified"] > info["local_modified"]:
                        info["needs_update"] = True

                result[filename] = info

            except Exception as e:
                logger.warning(f"Could not check freshness for {filename}: {e}")
                result[filename] = info

        return result

    def get_incremental_files(self, file_type: Optional[str] = None) -> Dict[str, Dict[str, List[str]]]:
        """
        Get incremental (ADD/CHANGE) file patterns.

        Args:
            file_type: Specific file type, or None for all.

        Returns:
            Dictionary of incremental files by type and category.
        """
        if file_type:
            return {file_type: INCREMENTAL_FILES.get(file_type, {"add": [], "change": []})}
        return INCREMENTAL_FILES

    def download_incremental_files(
        self,
        file_types: Optional[List[str]] = None,
        categories: Optional[List[str]] = None,
        force: bool = False,
    ) -> Dict[str, List[DownloadResult]]:
        """
        Download weekly incremental (ADD/CHANGE) files.

        Note: These files may not always exist on the FDA server.
        ADD files contain new records from the week.
        CHANGE files contain updates to existing records.

        Args:
            file_types: List of file types to download. None = all types.
            categories: List of categories ('add', 'change'). None = both.
            force: Force re-download even if already downloaded.

        Returns:
            Dictionary mapping file type to list of results.
        """
        if file_types is None:
            file_types = list(INCREMENTAL_FILES.keys())

        if categories is None:
            categories = ["add", "change"]

        all_results = {}

        for file_type in file_types:
            type_results = []
            type_files = INCREMENTAL_FILES.get(file_type, {})

            for category in categories:
                for filename in type_files.get(category, []):
                    url = FDA_DOWNLOAD_BASE + filename
                    result = self._download_file(url, file_type, force=force)

                    # Don't treat 404 as failure for incremental files
                    # (they may not exist every week)
                    if result.error and "404" in str(result.error):
                        logger.debug(f"Incremental file not available: {filename}")
                    else:
                        type_results.append(result)

            if type_results:
                all_results[file_type] = type_results

        return all_results

    def get_download_summary(self) -> Dict[str, any]:
        """
        Get summary of download status.

        Returns:
            Summary dictionary.
        """
        existing = self.get_existing_files()
        total_files = sum(len(files) for files in existing.values())
        total_size = sum(
            f.stat().st_size
            for f in self.output_dir.glob("*.txt")
        )

        return {
            "output_dir": str(self.output_dir),
            "files_by_type": {k: len(v) for k, v in existing.items()},
            "total_files": total_files,
            "total_size_mb": total_size / 1024 / 1024,
            "incomplete_downloads": self.tracker.get_incomplete(),
        }


def download_sample_data(output_dir: Optional[Path] = None) -> Dict[str, List[DownloadResult]]:
    """
    Download a small sample of MAUDE data for testing.

    Downloads only the most recent year's files.

    Args:
        output_dir: Output directory.

    Returns:
        Download results.
    """
    downloader = MAUDEDownloader(output_dir)

    # Just download current year files for testing
    sample_files = {
        "master": ["mdrfoi.zip"],
        "device": ["foidev.zip"],
        "patient": ["patient.zip"],
    }

    results = {}
    for file_type, files in sample_files.items():
        results[file_type] = []
        for filename in files:
            url = FDA_DOWNLOAD_BASE + filename
            result = downloader._download_file(url, file_type)
            results[file_type].append(result)

    return results


def download_historical_data(
    output_dir: Optional[Path] = None,
    years: Optional[List[int]] = None,
) -> Dict[str, List[DownloadResult]]:
    """
    Download historical MAUDE data.

    Args:
        output_dir: Output directory.
        years: Optional list of years to download.

    Returns:
        Download results.
    """
    downloader = MAUDEDownloader(output_dir, use_extended_files=True)
    return downloader.download_all(years=years)


if __name__ == "__main__":
    # Command line interface
    import argparse

    parser = argparse.ArgumentParser(description="Download FDA MAUDE files")
    parser.add_argument(
        "--type",
        choices=list(KNOWN_FILES.keys()),
        help="File type to download"
    )
    parser.add_argument("--year", type=int, help="Specific year to download")
    parser.add_argument("--sample", action="store_true", help="Download sample data only")
    parser.add_argument("--check", action="store_true", help="Check for missing files")
    parser.add_argument("--status", action="store_true", help="Show download status")
    parser.add_argument("--force", action="store_true", help="Force re-download")
    parser.add_argument("--extended", action="store_true", help="Include annual files")
    parser.add_argument("--output", type=Path, help="Output directory")

    args = parser.parse_args()

    downloader = MAUDEDownloader(
        output_dir=args.output,
        use_extended_files=args.extended
    )

    if args.status:
        summary = downloader.get_download_summary()
        print("\nDownload Status:")
        print(f"  Output directory: {summary['output_dir']}")
        print(f"  Total files: {summary['total_files']}")
        print(f"  Total size: {summary['total_size_mb']:.1f} MB")
        print("\n  Files by type:")
        for ftype, count in summary["files_by_type"].items():
            print(f"    {ftype}: {count}")
        if summary["incomplete_downloads"]:
            print(f"\n  Incomplete downloads: {len(summary['incomplete_downloads'])}")
            for f in summary["incomplete_downloads"][:5]:
                print(f"    - {f}")

    elif args.check:
        missing = downloader.check_for_updates()
        if missing:
            print("\nMissing files:")
            for ftype, files in missing.items():
                print(f"  {ftype}: {len(files)} files")
                for f in files[:5]:
                    print(f"    - {f}")
                if len(files) > 5:
                    print(f"    ... and {len(files) - 5} more")
        else:
            print("\nAll files are downloaded!")

    elif args.sample:
        results = download_sample_data(args.output)
        print("\nSample download results:")
        for ftype, res in results.items():
            for r in res:
                status = "OK" if r.success else f"FAILED: {r.error}"
                print(f"  {r.filename}: {status}")

    elif args.type:
        years = [args.year] if args.year else None
        results = downloader.download_file_type(args.type, years, force=args.force)
        print(f"\n{args.type} download results:")
        for r in results:
            status = "OK" if r.success else f"FAILED: {r.error}"
            size = f"{r.size_bytes / 1024 / 1024:.1f} MB" if r.success else ""
            print(f"  {r.filename}: {status} {size}")

    else:
        print("Use --help for options")
