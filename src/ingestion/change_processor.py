"""Change File Processor for FDA MAUDE CHANGE files.

CHANGE files contain updates to existing records (corrections, amendments).
Unlike ADD files which are new records, CHANGE files require UPDATE operations
rather than INSERT operations.

FDA releases CHANGE files weekly (Thursdays) containing modifications to
previously reported adverse events.
"""

import duckdb
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger
from src.database import get_connection
from src.ingestion.parser import MAUDEParser
from src.ingestion.transformer import DataTransformer, transform_record

logger = get_logger("change_processor")


@dataclass
class ChangeResult:
    """Result of processing a CHANGE file."""

    filename: str
    file_type: str
    records_processed: int = 0
    records_updated: int = 0
    records_not_found: int = 0
    records_skipped: int = 0
    records_errors: int = 0
    duration_seconds: float = 0
    error_messages: List[str] = field(default_factory=list)


# Columns to update for each file type (excludes primary keys and immutable fields)
# These are the fields that FDA might update in a CHANGE file
UPDATEABLE_COLUMNS = {
    "master": [
        # Event details that may be corrected
        "date_of_event", "event_type", "event_location",
        "adverse_event_flag", "product_problem_flag",
        # Manufacturer info that may be corrected
        "manufacturer_name", "manufacturer_address_1", "manufacturer_address_2",
        "manufacturer_city", "manufacturer_state", "manufacturer_zip",
        "manufacturer_country",
        # Report details
        "reporter_occupation_code", "health_professional",
        "date_report", "type_of_report",
        # Derived fields to recalculate
        "manufacturer_clean",
        # Metadata
        "date_changed",
    ],
    "patient": [
        # Patient demographics that may be corrected
        "patient_age", "patient_sex", "patient_weight",
        "patient_ethnicity", "patient_race",
        # Outcomes that may be updated
        "outcome_codes_raw", "treatment_codes_raw",
        "outcome_death", "outcome_life_threatening", "outcome_hospitalization",
        "outcome_disability", "outcome_congenital_anomaly",
        "outcome_required_intervention", "outcome_other",
        # Derived
        "patient_age_numeric", "patient_age_unit",
    ],
}


class ChangeProcessor:
    """Process FDA CHANGE files to update existing records."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        batch_size: int = 1000,
    ):
        """
        Initialize the change processor.

        Args:
            db_path: Path to database file.
            batch_size: Number of records to process per batch.
        """
        self.db_path = db_path or config.database.path
        self.batch_size = batch_size
        self.parser = MAUDEParser()
        self.transformer = DataTransformer()

    def process_change_file(
        self,
        filepath: Path,
        file_type: Optional[str] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> ChangeResult:
        """
        Process a CHANGE file and update existing records.

        CHANGE files have the same structure as regular files but contain
        corrections to existing records. We UPDATE rather than INSERT.

        Args:
            filepath: Path to the CHANGE file.
            file_type: Type of file (auto-detected if None).
            conn: Database connection (created if None).

        Returns:
            ChangeResult with processing statistics.
        """
        start_time = datetime.now()

        if file_type is None:
            file_type = self._detect_change_file_type(filepath)

        if file_type is None:
            raise ValueError(f"Could not detect file type for: {filepath}")

        result = ChangeResult(
            filename=filepath.name,
            file_type=file_type,
        )

        logger.info(f"Processing CHANGE file: {filepath.name} (type: {file_type})")

        # Detect schema from file header
        schema = self.parser.detect_schema_from_header(filepath, file_type)

        own_connection = conn is None
        if own_connection:
            conn = duckdb.connect(str(self.db_path))

        try:
            batch = []

            for record in self.parser.parse_file_dynamic(
                filepath,
                schema=schema,
                file_type=file_type,
                map_to_db_columns=True,
            ):
                result.records_processed += 1

                try:
                    # Validate mdr_report_key
                    mdr_key = record.get("mdr_report_key", "")
                    if not mdr_key or not str(mdr_key).isdigit():
                        result.records_skipped += 1
                        continue

                    # Transform record
                    transformed = transform_record(
                        record,
                        file_type,
                        self.transformer,
                        filepath.name,
                    )

                    batch.append(transformed)

                    # Process batch when full
                    if len(batch) >= self.batch_size:
                        stats = self._update_batch(conn, file_type, batch)
                        result.records_updated += stats["updated"]
                        result.records_not_found += stats["not_found"]
                        batch = []

                except Exception as e:
                    result.records_errors += 1
                    if len(result.error_messages) < 10:
                        result.error_messages.append(str(e))

            # Process remaining records
            if batch:
                stats = self._update_batch(conn, file_type, batch)
                result.records_updated += stats["updated"]
                result.records_not_found += stats["not_found"]

        finally:
            if own_connection:
                conn.close()

        result.duration_seconds = (datetime.now() - start_time).total_seconds()

        logger.info(
            f"Processed {filepath.name}: {result.records_updated:,} updated, "
            f"{result.records_not_found:,} not found, {result.records_errors:,} errors "
            f"in {result.duration_seconds:.1f}s"
        )

        return result

    def _update_batch(
        self,
        conn: duckdb.DuckDBPyConnection,
        file_type: str,
        batch: List[Dict[str, Any]],
    ) -> Dict[str, int]:
        """
        Update a batch of records in the database.

        Args:
            conn: Database connection.
            file_type: Type of records.
            batch: List of record dictionaries.

        Returns:
            Dictionary with 'updated' and 'not_found' counts.
        """
        stats = {"updated": 0, "not_found": 0}

        if not batch:
            return stats

        table_name = self._get_table_name(file_type)
        updateable = UPDATEABLE_COLUMNS.get(file_type, [])

        if not updateable:
            # For file types without specific update columns, just skip
            logger.warning(f"No updateable columns defined for {file_type}")
            return stats

        for record in batch:
            mdr_key = record.get("mdr_report_key")
            if not mdr_key:
                continue

            # Check if record exists
            existing = conn.execute(
                f"SELECT 1 FROM {table_name} WHERE mdr_report_key = ?",
                [mdr_key]
            ).fetchone()

            if not existing:
                stats["not_found"] += 1
                continue

            # Build UPDATE statement with only non-null values
            set_parts = []
            values = []

            for col in updateable:
                value = record.get(col)
                if value is not None:
                    set_parts.append(f"{col} = ?")
                    # Convert date objects to strings
                    if isinstance(value, date):
                        value = value.isoformat()
                    values.append(value)

            if not set_parts:
                continue

            # Always update date_changed
            set_parts.append("date_changed = CURRENT_TIMESTAMP")

            # Add mdr_key for WHERE clause
            values.append(mdr_key)

            sql = f"UPDATE {table_name} SET {', '.join(set_parts)} WHERE mdr_report_key = ?"

            try:
                conn.execute(sql, values)
                stats["updated"] += 1
            except Exception as e:
                logger.debug(f"Error updating {mdr_key}: {e}")

        return stats

    def _get_table_name(self, file_type: str) -> str:
        """Get database table name for file type."""
        table_map = {
            "master": "master_events",
            "device": "devices",
            "patient": "patients",
            "text": "mdr_text",
            "problem": "device_problems",
        }
        return table_map.get(file_type, file_type)

    def _detect_change_file_type(self, filepath: Path) -> Optional[str]:
        """Detect file type from CHANGE filename."""
        name = filepath.name.lower()

        if "mdrfoi" in name:
            return "master"
        elif "patient" in name:
            return "patient"
        elif "foidev" in name and "problem" not in name:
            return "device"
        elif "foitext" in name:
            return "text"

        return None


def process_change_file(
    filepath: Path,
    conn: duckdb.DuckDBPyConnection,
    file_type: Optional[str] = None,
) -> ChangeResult:
    """
    Convenience function to process a single CHANGE file.

    Args:
        filepath: Path to the CHANGE file.
        conn: Database connection.
        file_type: File type (auto-detected if None).

    Returns:
        ChangeResult with processing statistics.
    """
    processor = ChangeProcessor()
    return processor.process_change_file(filepath, file_type, conn)


def process_all_change_files(
    data_dir: Path,
    conn: duckdb.DuckDBPyConnection,
) -> List[ChangeResult]:
    """
    Process all CHANGE files in a directory.

    Args:
        data_dir: Directory containing CHANGE files.
        conn: Database connection.

    Returns:
        List of ChangeResult objects.
    """
    results = []
    processor = ChangeProcessor()

    # Find all CHANGE files
    # Pattern: *Change.txt (e.g., mdrfoiChange.txt, patientChange.txt)
    change_patterns = [
        "mdrfoiChange*.txt",
        "patientChange*.txt",
    ]

    for pattern in change_patterns:
        for filepath in data_dir.glob(pattern):
            logger.info(f"Found CHANGE file: {filepath.name}")
            result = processor.process_change_file(filepath, conn=conn)
            results.append(result)

    return results


if __name__ == "__main__":
    import argparse

    arg_parser = argparse.ArgumentParser(description="Process FDA CHANGE files")
    arg_parser.add_argument("file", type=Path, nargs="?", help="CHANGE file to process")
    arg_parser.add_argument("--data-dir", type=Path, default=config.data.raw_path)
    arg_parser.add_argument("--db", type=Path, default=config.database.path)
    arg_parser.add_argument("--all", action="store_true", help="Process all CHANGE files in data dir")

    args = arg_parser.parse_args()

    with get_connection(args.db) as conn:
        if args.all:
            results = process_all_change_files(args.data_dir, conn)
            print(f"\nProcessed {len(results)} CHANGE files:")
            for r in results:
                print(f"  {r.filename}: {r.records_updated} updated, {r.records_not_found} not found")
        elif args.file:
            result = process_change_file(args.file, conn)
            print(f"\nResult for {result.filename}:")
            print(f"  Processed: {result.records_processed:,}")
            print(f"  Updated: {result.records_updated:,}")
            print(f"  Not found: {result.records_not_found:,}")
            print(f"  Errors: {result.records_errors:,}")
        else:
            arg_parser.print_help()
