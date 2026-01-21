"""Load transformed MAUDE data into DuckDB."""

import duckdb
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, List, Optional, Generator
from dataclasses import dataclass, field
from tqdm import tqdm
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config, SCS_PRODUCT_CODES
from config.logging_config import get_logger
from src.database import get_connection, initialize_database
from src.ingestion.parser import MAUDEParser, FILE_COLUMNS
from src.ingestion.transformer import DataTransformer, transform_record

logger = get_logger("loader")


@dataclass
class LoadResult:
    """Result of a load operation."""

    file_type: str
    filename: str
    records_processed: int = 0
    records_loaded: int = 0
    records_skipped: int = 0
    records_errors: int = 0
    duration_seconds: float = 0
    error_messages: List[str] = field(default_factory=list)


class MAUDELoader:
    """Load MAUDE data into DuckDB database."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        batch_size: int = 10000,
        filter_product_codes: Optional[List[str]] = None,
    ):
        """
        Initialize the loader.

        Args:
            db_path: Path to database file.
            batch_size: Number of records to insert per batch.
            filter_product_codes: Only load records matching these codes.
        """
        self.db_path = db_path or config.database.path
        self.batch_size = batch_size
        self.filter_product_codes = filter_product_codes or SCS_PRODUCT_CODES
        self.parser = MAUDEParser()
        self.transformer = DataTransformer()

        # Track MDR keys for filtering related tables
        self._loaded_mdr_keys = set()

    def load_file(
        self,
        filepath: Path,
        file_type: Optional[str] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> LoadResult:
        """
        Load a single MAUDE file into the database.

        Args:
            filepath: Path to the file.
            file_type: Type of file (auto-detected if None).
            conn: Database connection (created if None).

        Returns:
            LoadResult with statistics.
        """
        start_time = datetime.now()

        if file_type is None:
            file_type = self.parser.detect_file_type(filepath)

        if file_type is None:
            raise ValueError(f"Could not detect file type for: {filepath}")

        result = LoadResult(
            file_type=file_type,
            filename=filepath.name,
        )

        logger.info(f"Loading {file_type} file: {filepath.name}")

        # Determine if we need to filter by product code
        should_filter = file_type in ["master", "device"] and self.filter_product_codes

        # For related tables, filter by loaded MDR keys
        filter_by_mdr = file_type in ["patient", "text", "problem"]

        own_connection = conn is None
        if own_connection:
            conn = duckdb.connect(str(self.db_path))

        try:
            batch = []

            for record in self.parser.parse_file(filepath, file_type):
                result.records_processed += 1

                try:
                    # Transform record
                    transformed = transform_record(
                        record,
                        file_type,
                        self.transformer,
                        filepath.name,
                    )

                    # Apply product code filter for master/device
                    if should_filter:
                        product_code = transformed.get(
                            "product_code" if file_type == "master" else "device_report_product_code"
                        )
                        if product_code not in self.filter_product_codes:
                            result.records_skipped += 1
                            continue

                    # Apply MDR key filter for related tables
                    if filter_by_mdr and self._loaded_mdr_keys:
                        mdr_key = transformed.get("mdr_report_key")
                        if mdr_key not in self._loaded_mdr_keys:
                            result.records_skipped += 1
                            continue

                    # Track MDR keys from master table
                    if file_type == "master":
                        mdr_key = transformed.get("mdr_report_key")
                        if mdr_key:
                            self._loaded_mdr_keys.add(mdr_key)

                    batch.append(transformed)

                    # Insert batch when full
                    if len(batch) >= self.batch_size:
                        inserted = self._insert_batch(conn, file_type, batch)
                        result.records_loaded += inserted
                        batch = []

                except Exception as e:
                    result.records_errors += 1
                    if len(result.error_messages) < 10:
                        result.error_messages.append(str(e))

            # Insert remaining records
            if batch:
                inserted = self._insert_batch(conn, file_type, batch)
                result.records_loaded += inserted

        finally:
            if own_connection:
                conn.close()

        result.duration_seconds = (datetime.now() - start_time).total_seconds()

        logger.info(
            f"Loaded {filepath.name}: {result.records_loaded:,} records "
            f"({result.records_skipped:,} skipped, {result.records_errors:,} errors) "
            f"in {result.duration_seconds:.1f}s"
        )

        return result

    def _insert_batch(
        self,
        conn: duckdb.DuckDBPyConnection,
        file_type: str,
        batch: List[Dict[str, Any]],
    ) -> int:
        """
        Insert a batch of records into the database.

        Args:
            conn: Database connection.
            file_type: Type of records.
            batch: List of record dictionaries.

        Returns:
            Number of records inserted.
        """
        if not batch:
            return 0

        table_name = self._get_table_name(file_type)
        columns = self._get_insert_columns(file_type)

        # Build INSERT statement
        placeholders = ", ".join(["?" for _ in columns])
        col_names = ", ".join(columns)

        sql = f"INSERT OR REPLACE INTO {table_name} ({col_names}) VALUES ({placeholders})"

        # Prepare values
        values = []
        for record in batch:
            row = []
            for col in columns:
                val = record.get(col)
                # Convert date objects to strings for DuckDB
                if isinstance(val, date):
                    val = val.isoformat()
                row.append(val)
            values.append(tuple(row))

        try:
            conn.executemany(sql, values)
            return len(values)
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            # Try inserting one by one to identify problematic records
            inserted = 0
            for val in values:
                try:
                    conn.execute(sql, val)
                    inserted += 1
                except Exception:
                    pass
            return inserted

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

    def _get_insert_columns(self, file_type: str) -> List[str]:
        """Get columns for INSERT statement."""
        base_columns = FILE_COLUMNS.get(file_type, [])

        # Add derived/metadata columns based on file type
        extra_columns = {
            "master": [
                "manufacturer_clean",
                "event_year",
                "event_month",
                "received_year",
                "received_month",
                "source_file",
            ],
            "device": ["manufacturer_d_clean", "source_file"],
            "patient": [
                "outcome_codes_raw",
                "outcome_death",
                "outcome_life_threatening",
                "outcome_hospitalization",
                "outcome_disability",
                "outcome_congenital_anomaly",
                "outcome_required_intervention",
                "outcome_other",
                "source_file",
            ],
            "text": ["source_file"],
            "problem": ["source_file"],
        }

        return base_columns + extra_columns.get(file_type, [])

    def load_all_files(
        self,
        data_dir: Path,
        file_types: Optional[List[str]] = None,
    ) -> Dict[str, List[LoadResult]]:
        """
        Load all MAUDE files from a directory.

        Args:
            data_dir: Directory containing MAUDE files.
            file_types: Types to load (default: all types).

        Returns:
            Dictionary mapping file type to list of results.
        """
        if file_types is None:
            file_types = ["master", "device", "patient", "text", "problem"]

        all_results = {}

        with get_connection(self.db_path) as conn:
            # Initialize schema
            initialize_database(conn)

            # Load files in order (master first to get MDR keys)
            for file_type in file_types:
                pattern = self._get_file_pattern(file_type)
                files = sorted(data_dir.glob(pattern))

                # Exclude problem files from device pattern
                if file_type == "device":
                    files = [f for f in files if "problem" not in f.name.lower()]

                if not files:
                    logger.warning(f"No {file_type} files found in {data_dir}")
                    continue

                logger.info(f"Loading {len(files)} {file_type} files...")

                results = []
                for filepath in tqdm(files, desc=f"Loading {file_type}"):
                    result = self.load_file(filepath, file_type, conn)
                    results.append(result)

                    # Log ingestion
                    self._log_ingestion(conn, result)

                all_results[file_type] = results

        return all_results

    def _get_file_pattern(self, file_type: str) -> str:
        """Get glob pattern for file type."""
        patterns = {
            "master": "mdrfoi*.txt",
            "device": "foidev*.txt",
            "patient": "patient*.txt",
            "text": "foitext*.txt",
            "problem": "*problem*.txt",
        }
        return patterns.get(file_type, "*.txt")

    def _log_ingestion(
        self, conn: duckdb.DuckDBPyConnection, result: LoadResult
    ) -> None:
        """Log ingestion result to database."""
        try:
            # Get next id
            next_id = conn.execute(
                "SELECT COALESCE(MAX(id), 0) + 1 FROM ingestion_log"
            ).fetchone()[0]

            conn.execute(
                """
                INSERT INTO ingestion_log
                (id, file_name, file_type, source, records_processed, records_loaded,
                 records_errors, started_at, completed_at, status, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    next_id,
                    result.filename,
                    result.file_type,
                    "FDA_DOWNLOAD",
                    result.records_processed,
                    result.records_loaded,
                    result.records_errors,
                    datetime.now(),
                    datetime.now(),
                    "COMPLETED" if result.records_errors == 0 else "COMPLETED_WITH_ERRORS",
                    "; ".join(result.error_messages) if result.error_messages else None,
                ),
            )
        except Exception as e:
            logger.warning(f"Could not log ingestion: {e}")

    def get_loaded_mdr_keys(self) -> set:
        """Get set of MDR keys that have been loaded."""
        return self._loaded_mdr_keys.copy()

    def clear_loaded_keys(self) -> None:
        """Clear the set of loaded MDR keys."""
        self._loaded_mdr_keys.clear()


def load_lookup_tables(conn: duckdb.DuckDBPyConnection, lookups_dir: Path) -> None:
    """
    Load lookup tables from CSV files.

    Args:
        conn: Database connection.
        lookups_dir: Directory containing lookup CSV files.
    """
    # Load product codes
    product_codes_file = lookups_dir / "product_codes.csv"
    if product_codes_file.exists():
        logger.info("Loading product codes lookup...")
        # Delete existing and insert new
        conn.execute("DELETE FROM product_codes")
        conn.execute(f"""
            INSERT INTO product_codes (product_code, device_name, device_class, medical_specialty, definition)
            SELECT product_code, device_name, device_class, medical_specialty, definition
            FROM read_csv_auto('{product_codes_file}')
        """)

    # Load manufacturer mappings
    manufacturer_file = lookups_dir / "manufacturer_mappings.csv"
    if manufacturer_file.exists():
        logger.info("Loading manufacturer mappings...")
        # Delete existing and insert new
        conn.execute("DELETE FROM manufacturers")
        conn.execute(f"""
            INSERT INTO manufacturers (id, raw_name, clean_name, parent_company, is_scs_manufacturer)
            SELECT
                ROW_NUMBER() OVER () as id,
                raw_name,
                clean_name,
                parent_company,
                is_scs_manufacturer::BOOLEAN
            FROM read_csv_auto('{manufacturer_file}')
        """)


if __name__ == "__main__":
    import argparse

    arg_parser = argparse.ArgumentParser(description="Load MAUDE data into DuckDB")
    arg_parser.add_argument("--data-dir", type=Path, default=config.data.raw_path)
    arg_parser.add_argument("--db", type=Path, default=config.database.path)
    arg_parser.add_argument("--type", help="File type to load")
    arg_parser.add_argument("--file", type=Path, help="Single file to load")
    arg_parser.add_argument(
        "--all-products",
        action="store_true",
        help="Load all products (not just SCS)",
    )

    args = arg_parser.parse_args()

    filter_codes = None if args.all_products else SCS_PRODUCT_CODES
    loader = MAUDELoader(db_path=args.db, filter_product_codes=filter_codes)

    if args.file:
        result = loader.load_file(args.file, args.type)
        print(f"Loaded: {result.records_loaded}, Errors: {result.records_errors}")
    else:
        types = [args.type] if args.type else None
        results = loader.load_all_files(args.data_dir, types)

        print("\nLoad Summary:")
        for file_type, res_list in results.items():
            total_loaded = sum(r.records_loaded for r in res_list)
            total_errors = sum(r.records_errors for r in res_list)
            print(f"  {file_type}: {total_loaded:,} loaded, {total_errors:,} errors")
