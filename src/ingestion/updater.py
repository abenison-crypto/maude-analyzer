"""Incremental update module for MAUDE data."""

import duckdb
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config, SCS_PRODUCT_CODES, MANUFACTURER_MAPPINGS
from config.logging_config import get_logger
from src.database import get_connection, get_table_counts
from src.ingestion.download import MAUDEDownloader, FDA_DOWNLOAD_BASE
from src.ingestion.loader import MAUDELoader, LoadResult
from src.ingestion.openfda import OpenFDAClient, OpenFDAResult

logger = get_logger("updater")


class UpdateSource(Enum):
    """Source of update data."""
    FDA_DOWNLOAD = "fda_download"
    OPENFDA_API = "openfda_api"
    MANUAL = "manual"


@dataclass
class UpdateStatus:
    """Status of an update operation."""

    source: UpdateSource
    started_at: datetime
    completed_at: Optional[datetime] = None
    records_added: int = 0
    records_updated: int = 0
    records_skipped: int = 0
    errors: List[str] = field(default_factory=list)
    success: bool = False

    @property
    def duration_seconds(self) -> float:
        if self.completed_at and self.started_at:
            return (self.completed_at - self.started_at).total_seconds()
        return 0.0


@dataclass
class DataStatus:
    """Current status of the database."""

    total_mdrs: int = 0
    latest_date_received: Optional[date] = None
    earliest_date_received: Optional[date] = None
    last_update: Optional[datetime] = None
    last_update_source: Optional[str] = None
    database_size_mb: float = 0.0
    table_counts: Dict[str, int] = field(default_factory=dict)


class DataUpdater:
    """Manages incremental updates to the MAUDE database."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        data_dir: Optional[Path] = None,
    ):
        """
        Initialize the updater.

        Args:
            db_path: Path to database file.
            data_dir: Path to raw data directory.
        """
        self.db_path = db_path or config.database.path
        self.data_dir = data_dir or config.data.raw_path
        self.downloader = MAUDEDownloader(output_dir=self.data_dir)
        self.loader = MAUDELoader(db_path=self.db_path)
        self.openfda_client = OpenFDAClient()

    def get_data_status(self) -> DataStatus:
        """
        Get current status of the database.

        Returns:
            DataStatus with database information.
        """
        status = DataStatus()

        if not self.db_path.exists():
            return status

        try:
            with get_connection(self.db_path) as conn:
                # Get table counts
                status.table_counts = get_table_counts(conn)
                status.total_mdrs = status.table_counts.get("master_events", 0)

                # Get date range
                date_range = conn.execute("""
                    SELECT MIN(date_received), MAX(date_received)
                    FROM master_events
                """).fetchone()

                if date_range[0]:
                    status.earliest_date_received = date_range[0]
                    status.latest_date_received = date_range[1]

                # Get last ingestion
                last_ingestion = conn.execute("""
                    SELECT completed_at, source
                    FROM ingestion_log
                    ORDER BY completed_at DESC
                    LIMIT 1
                """).fetchone()

                if last_ingestion:
                    status.last_update = last_ingestion[0]
                    status.last_update_source = last_ingestion[1]

            # Get database file size
            status.database_size_mb = self.db_path.stat().st_size / (1024 * 1024)

        except Exception as e:
            logger.error(f"Error getting data status: {e}")

        return status

    def check_for_fda_updates(self) -> Dict[str, List[str]]:
        """
        Check for new FDA files not yet downloaded.

        Returns:
            Dictionary of missing files by type.
        """
        return self.downloader.check_for_updates()

    def download_fda_updates(
        self,
        file_types: Optional[List[str]] = None,
    ) -> Dict[str, List]:
        """
        Download any new FDA files.

        Args:
            file_types: Types to download (default: all).

        Returns:
            Download results by file type.
        """
        missing = self.check_for_fda_updates()

        if not missing:
            logger.info("No new FDA files to download")
            return {}

        results = {}
        for file_type, files in missing.items():
            if file_types and file_type not in file_types:
                continue

            logger.info(f"Downloading {len(files)} new {file_type} files...")
            type_results = []

            for filename in files:
                url = FDA_DOWNLOAD_BASE + filename
                result = self.downloader._download_file(url)
                type_results.append(result)

            results[file_type] = type_results

        return results

    def load_new_files(
        self,
        file_types: Optional[List[str]] = None,
    ) -> UpdateStatus:
        """
        Load any new FDA files that haven't been ingested.

        Args:
            file_types: Types to load (default: all).

        Returns:
            UpdateStatus with results.
        """
        status = UpdateStatus(
            source=UpdateSource.FDA_DOWNLOAD,
            started_at=datetime.now(),
        )

        try:
            with get_connection(self.db_path) as conn:
                # Get already loaded files
                loaded_files = set(
                    row[0] for row in conn.execute(
                        "SELECT DISTINCT file_name FROM ingestion_log"
                    ).fetchall()
                )

                # Find new files
                for file_type in file_types or ["master", "device", "patient", "text", "problem"]:
                    pattern = self.loader._get_file_pattern(file_type)
                    files = sorted(self.data_dir.glob(pattern))

                    # Exclude problem files from device pattern
                    if file_type == "device":
                        files = [f for f in files if "problem" not in f.name.lower()]

                    for filepath in files:
                        if filepath.name in loaded_files:
                            continue

                        logger.info(f"Loading new file: {filepath.name}")
                        try:
                            result = self.loader.load_file(filepath, file_type, conn)
                            status.records_added += result.records_loaded
                            status.records_skipped += result.records_skipped

                            # Log to database
                            self.loader._log_ingestion(conn, result)

                        except Exception as e:
                            status.errors.append(f"{filepath.name}: {str(e)}")
                            logger.error(f"Error loading {filepath.name}: {e}")

            status.success = len(status.errors) == 0
            status.completed_at = datetime.now()

        except Exception as e:
            status.errors.append(str(e))
            status.completed_at = datetime.now()
            logger.error(f"Update failed: {e}")

        return status

    def update_from_openfda(
        self,
        days: int = 30,
        max_records: int = 5000,
    ) -> UpdateStatus:
        """
        Fetch and load recent data from openFDA API.

        .. deprecated::
            This method is DEPRECATED. The openFDA API returns partial data
            (~20-25 fields) compared to FDA download files (86+ fields).
            Mixing API data with download data creates inconsistent records.

            Use weekly FDA file downloads instead for data ingestion.
            Keep API for: real-time alerts, quick counts, ad-hoc queries.

        Args:
            days: Number of days back to fetch.
            max_records: Maximum records to fetch.

        Returns:
            UpdateStatus with results.
        """
        import warnings
        warnings.warn(
            "update_from_openfda() is deprecated. openFDA API returns partial data "
            "(~20-25 fields vs 86+ in download files). Use FDA file downloads for "
            "data ingestion. Keep API for alerts and ad-hoc queries only.",
            DeprecationWarning,
            stacklevel=2
        )
        status = UpdateStatus(
            source=UpdateSource.OPENFDA_API,
            started_at=datetime.now(),
        )

        try:
            # Fetch from API
            logger.info(f"Fetching records from openFDA (last {days} days)...")
            api_result = self.openfda_client.get_recent_scs_events(
                days=days,
                max_records=max_records,
            )

            if api_result.error:
                status.errors.append(api_result.error)
                status.completed_at = datetime.now()
                return status

            logger.info(f"Fetched {api_result.records_fetched} records from API")

            if not api_result.records:
                status.success = True
                status.completed_at = datetime.now()
                return status

            # Load into database
            with get_connection(self.db_path) as conn:
                for record in api_result.records:
                    try:
                        transformed = self.openfda_client.transform_to_maude_format(record)

                        # Check if record exists
                        mdr_key = transformed["master"].get("mdr_report_key")
                        if not mdr_key:
                            status.records_skipped += 1
                            continue

                        existing = conn.execute(
                            "SELECT 1 FROM master_events WHERE mdr_report_key = ?",
                            [mdr_key]
                        ).fetchone()

                        if existing:
                            # Update existing record
                            self._update_record(conn, transformed)
                            status.records_updated += 1
                        else:
                            # Insert new record
                            self._insert_record(conn, transformed)
                            status.records_added += 1

                    except Exception as e:
                        status.records_skipped += 1
                        if len(status.errors) < 10:
                            status.errors.append(str(e))

                # Log ingestion
                self._log_api_ingestion(conn, status, days)

            status.success = True

        except Exception as e:
            status.errors.append(str(e))
            logger.error(f"openFDA update failed: {e}")

        status.completed_at = datetime.now()
        return status

    def _insert_record(
        self,
        conn: duckdb.DuckDBPyConnection,
        transformed: Dict[str, Any],
    ) -> None:
        """Insert a new record from openFDA."""
        master = transformed["master"]

        # Clean manufacturer name
        manufacturer_clean = self._clean_manufacturer(master.get("manufacturer_name"))

        # Insert master record
        conn.execute("""
            INSERT INTO master_events (
                mdr_report_key, event_key, report_number, date_received, date_of_event,
                manufacturer_name, manufacturer_clean, product_code, event_type,
                type_of_report, product_problem_flag, adverse_event_flag,
                report_source_code, event_location, pma_pmn_number,
                received_year, received_month, source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            master.get("mdr_report_key"),
            master.get("event_key"),
            master.get("report_number"),
            master.get("date_received"),
            master.get("date_of_event"),
            master.get("manufacturer_name"),
            manufacturer_clean,
            master.get("product_code"),
            master.get("event_type"),
            master.get("type_of_report"),
            master.get("product_problem_flag"),
            master.get("adverse_event_flag"),
            master.get("report_source_code"),
            master.get("event_location"),
            master.get("pma_pmn_number"),
            master.get("date_received").year if master.get("date_received") else None,
            master.get("date_received").month if master.get("date_received") else None,
            "openfda_api",
        ))

        # Insert device record
        device = transformed["device"]
        if device.get("mdr_report_key"):
            device_clean = self._clean_manufacturer(device.get("manufacturer_d_name"))
            conn.execute("""
                INSERT INTO devices (
                    mdr_report_key, device_event_key, device_sequence_number,
                    brand_name, generic_name, manufacturer_d_name, manufacturer_d_clean,
                    manufacturer_d_city, manufacturer_d_state, manufacturer_d_country,
                    device_report_product_code, model_number, catalog_number, lot_number,
                    device_availability, device_age_text, source_file
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                device.get("mdr_report_key"),
                device.get("device_event_key"),
                device.get("device_sequence_number"),
                device.get("brand_name"),
                device.get("generic_name"),
                device.get("manufacturer_d_name"),
                device_clean,
                device.get("manufacturer_d_city"),
                device.get("manufacturer_d_state"),
                device.get("manufacturer_d_country"),
                device.get("device_report_product_code"),
                device.get("model_number"),
                device.get("catalog_number"),
                device.get("lot_number"),
                device.get("device_availability"),
                device.get("device_age_text"),
                "openfda_api",
            ))

        # Insert text records
        for text in transformed.get("text", []):
            if text.get("mdr_report_key") and text.get("text_content"):
                conn.execute("""
                    INSERT INTO mdr_text (mdr_report_key, text_type_code, text_content, source_file)
                    VALUES (?, ?, ?, ?)
                """, (
                    text.get("mdr_report_key"),
                    text.get("text_type_code"),
                    text.get("text_content"),
                    "openfda_api",
                ))

    def _update_record(
        self,
        conn: duckdb.DuckDBPyConnection,
        transformed: Dict[str, Any],
    ) -> None:
        """Update an existing record from openFDA."""
        # For now, just update the text if new narratives exist
        mdr_key = transformed["master"].get("mdr_report_key")

        for text in transformed.get("text", []):
            if text.get("text_content"):
                # Check if text exists
                existing = conn.execute("""
                    SELECT 1 FROM mdr_text
                    WHERE mdr_report_key = ? AND text_type_code = ?
                """, [mdr_key, text.get("text_type_code")]).fetchone()

                if not existing:
                    conn.execute("""
                        INSERT INTO mdr_text (mdr_report_key, text_type_code, text_content, source_file)
                        VALUES (?, ?, ?, ?)
                    """, (
                        mdr_key,
                        text.get("text_type_code"),
                        text.get("text_content"),
                        "openfda_api",
                    ))

    def _clean_manufacturer(self, name: Optional[str]) -> Optional[str]:
        """Clean and standardize manufacturer name."""
        if not name:
            return None

        name_upper = name.upper().strip()

        # Check manufacturer mappings
        for pattern, clean_name in MANUFACTURER_MAPPINGS.items():
            if pattern.upper() in name_upper:
                return clean_name

        return name

    def _log_api_ingestion(
        self,
        conn: duckdb.DuckDBPyConnection,
        status: UpdateStatus,
        days: int,
    ) -> None:
        """Log API ingestion to database."""
        try:
            next_id = conn.execute(
                "SELECT COALESCE(MAX(id), 0) + 1 FROM ingestion_log"
            ).fetchone()[0]

            conn.execute("""
                INSERT INTO ingestion_log
                (id, file_name, file_type, source, records_processed, records_loaded,
                 records_errors, started_at, completed_at, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                next_id,
                f"openfda_last_{days}_days",
                "api",
                "OPENFDA_API",
                status.records_added + status.records_updated + status.records_skipped,
                status.records_added + status.records_updated,
                len(status.errors),
                status.started_at,
                status.completed_at,
                "COMPLETED" if status.success else "FAILED",
                "; ".join(status.errors[:5]) if status.errors else None,
            ))
        except Exception as e:
            logger.warning(f"Could not log API ingestion: {e}")

    def run_full_update(
        self,
        download_new: bool = True,
        load_new_files: bool = True,
        fetch_from_api: bool = False,  # Changed default to False - API deprecated
        api_days: int = 30,
    ) -> Dict[str, UpdateStatus]:
        """
        Run a full update from all sources.

        Note: API fetching is disabled by default. The openFDA API returns
        partial data (~20-25 fields vs 86+ fields in download files).
        Use FDA file downloads for complete, authoritative data.

        Args:
            download_new: Download new FDA files.
            load_new_files: Load new downloaded files.
            fetch_from_api: Fetch recent data from openFDA (DEPRECATED, default False).
            api_days: Days of data to fetch from API.

        Returns:
            Dictionary of update statuses by source.
        """
        results = {}

        if download_new:
            logger.info("Checking for new FDA downloads...")
            self.download_fda_updates()

        if load_new_files:
            logger.info("Loading new files...")
            results["fda_files"] = self.load_new_files()

        if fetch_from_api:
            logger.warning(
                "API fetching is deprecated. openFDA returns partial data. "
                "Consider using FDA file downloads only."
            )
            logger.info("Fetching from openFDA API...")
            results["openfda"] = self.update_from_openfda(days=api_days)

        return results


def get_update_status() -> DataStatus:
    """Get current data status."""
    updater = DataUpdater()
    return updater.get_data_status()


def run_incremental_update(
    api_only: bool = False,
    days: int = 30,
    include_api: bool = False,
) -> Dict[str, UpdateStatus]:
    """
    Run an incremental update.

    Recommended: Use FDA file downloads only (include_api=False).
    The openFDA API returns partial data (~20-25 fields vs 86+ in files).

    Args:
        api_only: Only fetch from API, don't download files (DEPRECATED).
        days: Days of API data to fetch.
        include_api: Whether to also fetch from API (default False, deprecated).

    Returns:
        Update results.
    """
    if api_only:
        import warnings
        warnings.warn(
            "api_only mode is deprecated. openFDA API returns partial data. "
            "Use FDA file downloads for complete data.",
            DeprecationWarning,
            stacklevel=2
        )

    updater = DataUpdater()
    return updater.run_full_update(
        download_new=not api_only,
        load_new_files=not api_only,
        fetch_from_api=include_api or api_only,
        api_days=days,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Update MAUDE database")
    parser.add_argument("--status", action="store_true", help="Show data status")
    parser.add_argument("--check", action="store_true", help="Check for new FDA files")
    parser.add_argument("--download", action="store_true", help="Download new FDA files")
    parser.add_argument("--load", action="store_true", help="Load new files")
    parser.add_argument("--api", action="store_true", help="Fetch from openFDA API")
    parser.add_argument("--days", type=int, default=30, help="Days for API fetch")
    parser.add_argument("--full", action="store_true", help="Run full update")

    args = parser.parse_args()

    updater = DataUpdater()

    if args.status:
        status = updater.get_data_status()
        print(f"Database: {updater.db_path}")
        print(f"Size: {status.database_size_mb:.1f} MB")
        print(f"Total MDRs: {status.total_mdrs:,}")
        print(f"Date range: {status.earliest_date_received} to {status.latest_date_received}")
        print(f"Last update: {status.last_update} ({status.last_update_source})")
        print(f"Table counts: {status.table_counts}")

    elif args.check:
        missing = updater.check_for_fda_updates()
        if missing:
            print("Missing files:")
            for ftype, files in missing.items():
                print(f"  {ftype}: {len(files)} files")
        else:
            print("All files up to date")

    elif args.download:
        results = updater.download_fda_updates()
        for ftype, res in results.items():
            success = sum(1 for r in res if r.success)
            print(f"{ftype}: {success}/{len(res)} downloaded")

    elif args.load:
        status = updater.load_new_files()
        print(f"Added: {status.records_added}, Skipped: {status.records_skipped}")
        if status.errors:
            print(f"Errors: {status.errors[:5]}")

    elif args.api:
        status = updater.update_from_openfda(days=args.days)
        print(f"Added: {status.records_added}, Updated: {status.records_updated}")
        print(f"Duration: {status.duration_seconds:.1f}s")
        if status.errors:
            print(f"Errors: {status.errors[:5]}")

    elif args.full:
        results = updater.run_full_update(api_days=args.days)
        for source, status in results.items():
            print(f"\n{source}:")
            print(f"  Added: {status.records_added}")
            print(f"  Updated: {status.records_updated}")
            print(f"  Success: {status.success}")

    else:
        parser.print_help()
