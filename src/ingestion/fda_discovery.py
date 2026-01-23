"""FDA File Discovery Module.

Detects new FDA MAUDE files automatically using HTTP HEAD requests
to check Last-Modified headers without downloading entire files.

FDA File Patterns:
- Weekly current: mdrfoi.zip, foidev.zip, patient.zip, foitext.zip
- Weekly ADD:     mdrfoiAdd.zip, foidevAdd.zip (new records)
- Weekly CHANGE:  mdrfoiChange.zip (updates to existing records)
- Annual:         mdrfoithru{year}.zip (released each January)
"""

import json
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger

logger = get_logger("fda_discovery")

# FDA MAUDE FTP area base URL
FDA_FTP_BASE = "https://www.accessdata.fda.gov/MAUDE/ftparea/"

# Current year for dynamic file detection
CURRENT_YEAR = datetime.now().year


@dataclass
class FileInfo:
    """Information about an FDA file."""

    filename: str
    file_type: str  # master, device, patient, text, problem
    file_category: str  # current, add, change, annual
    url: str
    last_modified_remote: Optional[datetime] = None
    last_modified_local: Optional[datetime] = None
    size_bytes: Optional[int] = None
    needs_update: bool = False
    is_new: bool = False


@dataclass
class DiscoveryResult:
    """Result of FDA file discovery."""

    files_checked: int = 0
    new_files: List[FileInfo] = field(default_factory=list)
    updated_files: List[FileInfo] = field(default_factory=list)
    unchanged_files: List[FileInfo] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    discovered_at: datetime = field(default_factory=datetime.now)

    @property
    def files_needing_download(self) -> List[FileInfo]:
        """Get all files that need to be downloaded."""
        return self.new_files + self.updated_files

    def by_category(self) -> Dict[str, List[FileInfo]]:
        """Group files needing download by category."""
        result = {"current": [], "add": [], "change": [], "annual": []}
        for f in self.files_needing_download:
            result[f.file_category].append(f)
        return result


class FDADiscovery:
    """Discovers new and updated FDA MAUDE files."""

    # Files that may exist on FDA servers
    # ADD and CHANGE files are released weekly with current year data
    DISCOVERABLE_FILES = {
        "master": {
            "current": ["mdrfoi.zip"],
            "add": ["mdrfoiAdd.zip"],
            "change": ["mdrfoiChange.zip"],
            "annual": [],  # Populated dynamically
        },
        "device": {
            "current": ["foidev.zip"],
            "add": ["foidevAdd.zip"],
            "change": [],  # Device doesn't have change files
            "annual": [],
        },
        "patient": {
            "current": ["patient.zip"],
            "add": ["patientAdd.zip"],
            "change": ["patientChange.zip"],
            "annual": [],
        },
        "text": {
            "current": ["foitext.zip"],
            "add": ["foitextAdd.zip"],
            "change": [],  # Text doesn't have change files
            "annual": [],
        },
        "problem": {
            "current": ["foidevproblem.zip"],
            "add": [],
            "change": [],
            "annual": [],
        },
    }

    def __init__(
        self,
        state_file: Optional[Path] = None,
        timeout: int = 30,
    ):
        """
        Initialize the FDA discovery module.

        Args:
            state_file: Path to JSON file tracking file states.
            timeout: HTTP request timeout in seconds.
        """
        self.state_file = state_file or (config.data.raw_path / ".fda_discovery_state.json")
        self.timeout = timeout
        self._state: Dict[str, Dict] = {}
        self._load_state()

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        })

    def _load_state(self) -> None:
        """Load discovery state from file."""
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    self._state = json.load(f)
            except Exception as e:
                logger.warning(f"Could not load discovery state: {e}")
                self._state = {}

    def _save_state(self) -> None:
        """Save discovery state to file."""
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.state_file, "w") as f:
                json.dump(self._state, f, indent=2, default=str)
        except Exception as e:
            logger.warning(f"Could not save discovery state: {e}")

    def _get_remote_info(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Get file info from FDA server using HTTP HEAD request.

        Args:
            url: URL to check.

        Returns:
            Dict with last_modified and content_length, or None if not found.
        """
        try:
            response = self.session.head(url, timeout=self.timeout, allow_redirects=True)

            if response.status_code == 404:
                return None

            response.raise_for_status()

            last_modified = None
            if "Last-Modified" in response.headers:
                last_modified = datetime.strptime(
                    response.headers["Last-Modified"],
                    "%a, %d %b %Y %H:%M:%S %Z"
                )

            content_length = response.headers.get("Content-Length")
            size_bytes = int(content_length) if content_length else None

            return {
                "last_modified": last_modified,
                "size_bytes": size_bytes,
            }

        except requests.exceptions.RequestException as e:
            logger.debug(f"Could not fetch info for {url}: {e}")
            return None

    def _get_annual_files(self) -> Dict[str, List[str]]:
        """
        Get list of annual archive files that should exist.

        Returns:
            Dict mapping file_type to list of annual filenames.
        """
        # Annual files are released in January containing all data through the prior year
        # E.g., mdrfoithru2024.zip contains 1991-2024 data
        latest_archive_year = CURRENT_YEAR - 1

        return {
            "master": [f"mdrfoithru{latest_archive_year}.zip"],
            "device": [f"foidevthru{latest_archive_year}.zip"],
            "patient": [f"patientthru{latest_archive_year}.zip"],
            "text": [f"foitextthru{latest_archive_year}.zip"],
            "problem": [],
        }

    def check_for_updates(
        self,
        check_annual: bool = True,
        check_add_change: bool = True,
    ) -> DiscoveryResult:
        """
        Check FDA servers for new or updated files.

        Args:
            check_annual: Whether to check for new annual archive files.
            check_add_change: Whether to check ADD/CHANGE files.

        Returns:
            DiscoveryResult with files needing download.
        """
        result = DiscoveryResult()
        annual_files = self._get_annual_files() if check_annual else {}

        for file_type, categories in self.DISCOVERABLE_FILES.items():
            for category, filenames in categories.items():
                # Skip ADD/CHANGE if not requested
                if not check_add_change and category in ("add", "change"):
                    continue

                # Add annual files
                if category == "annual" and check_annual:
                    filenames = annual_files.get(file_type, [])

                for filename in filenames:
                    url = FDA_FTP_BASE + filename
                    result.files_checked += 1

                    logger.debug(f"Checking {filename}...")

                    remote_info = self._get_remote_info(url)

                    if remote_info is None:
                        # File doesn't exist on server (normal for ADD/CHANGE files
                        # which only exist during certain weeks)
                        continue

                    file_info = FileInfo(
                        filename=filename,
                        file_type=file_type,
                        file_category=category,
                        url=url,
                        last_modified_remote=remote_info["last_modified"],
                        size_bytes=remote_info["size_bytes"],
                    )

                    # Check local state
                    local_state = self._state.get(filename, {})
                    if local_state:
                        local_modified_str = local_state.get("last_modified_remote")
                        if local_modified_str:
                            file_info.last_modified_local = datetime.fromisoformat(
                                local_modified_str
                            )

                    # Determine if file needs download
                    if file_info.last_modified_local is None:
                        file_info.is_new = True
                        file_info.needs_update = True
                        result.new_files.append(file_info)
                        logger.info(f"New file found: {filename}")

                    elif (
                        file_info.last_modified_remote
                        and file_info.last_modified_remote > file_info.last_modified_local
                    ):
                        file_info.needs_update = True
                        result.updated_files.append(file_info)
                        logger.info(
                            f"Updated file found: {filename} "
                            f"(remote: {file_info.last_modified_remote}, "
                            f"local: {file_info.last_modified_local})"
                        )

                    else:
                        result.unchanged_files.append(file_info)
                        logger.debug(f"File unchanged: {filename}")

        return result

    def mark_downloaded(
        self,
        filename: str,
        last_modified: Optional[datetime] = None,
        size_bytes: Optional[int] = None,
    ) -> None:
        """
        Mark a file as downloaded, updating local state.

        Args:
            filename: Name of the downloaded file.
            last_modified: Last-Modified timestamp from server.
            size_bytes: File size in bytes.
        """
        self._state[filename] = {
            "last_modified_remote": last_modified.isoformat() if last_modified else None,
            "size_bytes": size_bytes,
            "downloaded_at": datetime.now().isoformat(),
        }
        self._save_state()

    def detect_new_annual_file(self) -> Optional[FileInfo]:
        """
        Detect if a new annual file has been released (typically in January).

        Returns:
            FileInfo for new annual file, or None if none found.
        """
        # Check if there's a new annual file we haven't seen
        for file_type, files in self._get_annual_files().items():
            for filename in files:
                if filename not in self._state:
                    url = FDA_FTP_BASE + filename
                    remote_info = self._get_remote_info(url)

                    if remote_info:
                        return FileInfo(
                            filename=filename,
                            file_type=file_type,
                            file_category="annual",
                            url=url,
                            last_modified_remote=remote_info["last_modified"],
                            size_bytes=remote_info["size_bytes"],
                            is_new=True,
                            needs_update=True,
                        )

        return None

    def get_freshness_summary(self) -> Dict[str, Any]:
        """
        Get summary of file freshness without making HTTP requests.

        Returns:
            Dictionary with freshness information.
        """
        summary = {
            "state_file": str(self.state_file),
            "files_tracked": len(self._state),
            "by_type": {},
            "oldest_update": None,
            "newest_update": None,
        }

        oldest = None
        newest = None

        for filename, state in self._state.items():
            downloaded_at = state.get("downloaded_at")
            if downloaded_at:
                dt = datetime.fromisoformat(downloaded_at)
                if oldest is None or dt < oldest:
                    oldest = dt
                if newest is None or dt > newest:
                    newest = dt

            # Categorize by file type
            file_type = self._detect_file_type(filename)
            if file_type not in summary["by_type"]:
                summary["by_type"][file_type] = []
            summary["by_type"][file_type].append(filename)

        summary["oldest_update"] = oldest.isoformat() if oldest else None
        summary["newest_update"] = newest.isoformat() if newest else None

        return summary

    def _detect_file_type(self, filename: str) -> str:
        """Detect file type from filename."""
        lower = filename.lower()
        if "mdrfoi" in lower:
            return "master"
        elif "foidev" in lower and "problem" not in lower:
            return "device"
        elif "patient" in lower:
            return "patient"
        elif "foitext" in lower:
            return "text"
        elif "problem" in lower:
            return "problem"
        return "unknown"

    def clear_state(self, filename: Optional[str] = None) -> None:
        """
        Clear discovery state.

        Args:
            filename: Specific file to clear, or None to clear all.
        """
        if filename:
            self._state.pop(filename, None)
        else:
            self._state = {}
        self._save_state()


def check_fda_updates(verbose: bool = True) -> DiscoveryResult:
    """
    Convenience function to check for FDA updates.

    Args:
        verbose: Whether to log findings.

    Returns:
        DiscoveryResult with files needing download.
    """
    discovery = FDADiscovery()
    result = discovery.check_for_updates()

    if verbose:
        if result.files_needing_download:
            logger.info(f"Found {len(result.files_needing_download)} files needing download:")
            for f in result.files_needing_download:
                size_mb = f.size_bytes / 1024 / 1024 if f.size_bytes else 0
                logger.info(f"  {f.filename} ({f.file_category}) - {size_mb:.1f} MB")
        else:
            logger.info("All files are up to date")

    return result


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Check for FDA MAUDE file updates")
    parser.add_argument("--check", action="store_true", help="Check for updates")
    parser.add_argument("--status", action="store_true", help="Show current state")
    parser.add_argument("--clear", action="store_true", help="Clear state")
    parser.add_argument(
        "--skip-add-change",
        action="store_true",
        help="Skip checking ADD/CHANGE files"
    )

    args = parser.parse_args()

    discovery = FDADiscovery()

    if args.status:
        summary = discovery.get_freshness_summary()
        print(f"\nFDA Discovery State:")
        print(f"  State file: {summary['state_file']}")
        print(f"  Files tracked: {summary['files_tracked']}")
        print(f"  Oldest update: {summary['oldest_update']}")
        print(f"  Newest update: {summary['newest_update']}")
        print(f"\n  Files by type:")
        for ftype, files in summary["by_type"].items():
            print(f"    {ftype}: {len(files)}")

    elif args.clear:
        discovery.clear_state()
        print("State cleared")

    elif args.check or True:  # Default action
        print("Checking FDA servers for updates...")
        result = discovery.check_for_updates(
            check_add_change=not args.skip_add_change
        )

        print(f"\nFiles checked: {result.files_checked}")
        print(f"New files: {len(result.new_files)}")
        print(f"Updated files: {len(result.updated_files)}")
        print(f"Unchanged files: {len(result.unchanged_files)}")

        if result.files_needing_download:
            print("\nFiles needing download:")
            for f in result.files_needing_download:
                size_mb = f.size_bytes / 1024 / 1024 if f.size_bytes else 0
                status = "NEW" if f.is_new else "UPDATED"
                print(f"  [{status}] {f.filename} ({f.file_type}/{f.file_category}) - {size_mb:.1f} MB")
