"""Load transformed MAUDE data into DuckDB with dynamic schema support."""

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
from src.ingestion.parser import MAUDEParser, FILE_COLUMNS, SchemaInfo
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
    schema_info: Optional[SchemaInfo] = None


# Expanded column lists for database insertion
# These match the new expanded schema in schema.py

MASTER_INSERT_COLUMNS = [
    # Primary key and identifiers
    "mdr_report_key", "event_key", "report_number", "report_source_code",
    "manufacturer_link_flag_old",
    # Event counts
    "number_devices_in_event", "number_patients_in_event",
    # Dates
    "date_received", "date_report", "date_of_event",
    # Flags
    "adverse_event_flag", "product_problem_flag", "reprocessed_and_reused_flag",
    # Reporter info
    "reporter_occupation_code", "health_professional", "initial_report_to_fda",
    "reporter_state_code", "reporter_country_code",
    # Facility dates
    "date_facility_aware", "report_date", "report_to_fda", "date_report_to_fda",
    # Event details
    "event_location", "event_type",
    # Report to manufacturer
    "date_report_to_manufacturer", "report_to_manufacturer", "date_manufacturer_received",
    # Manufacturer contact
    "manufacturer_contact_title", "manufacturer_contact_first_name",
    "manufacturer_contact_last_name", "manufacturer_contact_address_1",
    "manufacturer_contact_address_2", "manufacturer_contact_city",
    "manufacturer_contact_state", "manufacturer_contact_zip",
    "manufacturer_contact_zip_ext", "manufacturer_contact_country",
    "manufacturer_contact_postal",
    # Phone fields (FDA splits into components)
    "manufacturer_contact_area_code", "manufacturer_contact_exchange",
    "manufacturer_contact_phone_no", "manufacturer_contact_extension",
    "manufacturer_contact_pcountry", "manufacturer_contact_pcity",
    "manufacturer_contact_plocal",
    # Global manufacturer
    "manufacturer_g1_name", "manufacturer_g1_street_1", "manufacturer_g1_street_2",
    "manufacturer_g1_city", "manufacturer_g1_state", "manufacturer_g1_zip",
    "manufacturer_g1_zip_ext", "manufacturer_g1_country", "manufacturer_g1_postal",
    # Device manufacturing
    "device_date_of_manufacture",
    # Device flags
    "single_use_flag", "remedial_action", "previous_use_code",
    "removal_correction_number", "manufacturer_link_flag",
    # Distributor
    "distributor_name", "distributor_address_1", "distributor_address_2",
    "distributor_city", "distributor_state", "distributor_zip", "distributor_zip_ext",
    # Report type
    "type_of_report",
    # Main manufacturer
    "manufacturer_name", "manufacturer_address_1", "manufacturer_address_2",
    "manufacturer_city", "manufacturer_state", "manufacturer_zip",
    "manufacturer_zip_ext", "manufacturer_country", "manufacturer_postal",
    # Classification
    "mfr_report_type", "source_type",
    # Metadata dates
    "date_added", "date_changed",
    # Product identification (Note: master file does NOT have product_code - it's only in device file)
    "pma_pmn_number", "exemption_number", "summary_report_flag",
    # Supplemental
    "noe_summarized", "supplemental_dates_fda_received",
    "supplemental_dates_mfr_received",
    # Derived fields
    "manufacturer_clean", "event_year", "event_month",
    "received_year", "received_month", "source_file",
]

DEVICE_INSERT_COLUMNS = [
    "mdr_report_key", "device_event_key",
    "implant_flag", "date_removed_flag", "device_sequence_number",
    "date_received",
    "brand_name", "generic_name",
    "manufacturer_d_name", "manufacturer_d_address_1", "manufacturer_d_address_2",
    "manufacturer_d_city", "manufacturer_d_state", "manufacturer_d_zip",
    "manufacturer_d_zip_ext", "manufacturer_d_country", "manufacturer_d_postal",
    "expiration_date_of_device",
    "model_number", "catalog_number", "lot_number", "other_id_number",
    "device_operator", "device_availability", "date_returned_to_manufacturer",
    "device_report_product_code", "device_age_text", "device_evaluated_by_manufacturer",
    "manufacturer_d_clean", "source_file",
]

PATIENT_INSERT_COLUMNS = [
    "mdr_report_key", "patient_sequence_number", "date_received",
    "sequence_number_treatment", "sequence_number_outcome",
    "patient_age", "patient_sex", "patient_weight",
    "patient_ethnicity", "patient_race",
    "patient_age_numeric", "patient_age_unit",
    "outcome_codes_raw", "treatment_codes_raw",
    "outcome_death", "outcome_life_threatening", "outcome_hospitalization",
    "outcome_disability", "outcome_congenital_anomaly",
    "outcome_required_intervention", "outcome_other",
    "source_file",
]

TEXT_INSERT_COLUMNS = [
    "mdr_report_key", "mdr_text_key", "text_type_code",
    "patient_sequence_number", "date_report", "text_content",
    "source_file",
]

PROBLEM_INSERT_COLUMNS = [
    "mdr_report_key", "device_problem_code",
    "source_file",
]

PATIENT_PROBLEM_INSERT_COLUMNS = [
    "mdr_report_key", "patient_sequence_number", "patient_problem_code",
    "date_added", "date_changed",
    "source_file",
]

ASR_INSERT_COLUMNS = [
    "report_id", "report_year",
    "brand_name", "generic_name", "manufacturer_name",
    "product_code", "device_class",
    "report_count", "event_count",
    "death_count", "injury_count", "malfunction_count",
    "date_start", "date_end",
    "exemption_number", "pma_pmn_number", "submission_type",
    "summary_text",
    "source_file",
]

ASR_PPC_INSERT_COLUMNS = [
    "report_id", "patient_problem_code", "occurrence_count",
    "source_file",
]

DEN_INSERT_COLUMNS = [
    "mdr_report_key", "report_number", "report_source",
    "date_received", "date_of_event", "date_report",
    "brand_name", "generic_name",
    "model_number", "catalog_number", "lot_number",
    "device_operator",
    "manufacturer_name", "manufacturer_city", "manufacturer_state", "manufacturer_country",
    "event_type", "event_description", "patient_outcome",
    "report_year",
    "source_file",
]

DISCLAIMER_INSERT_COLUMNS = [
    "manufacturer_name", "disclaimer_text", "effective_date",
    "source_file",
]

PROBLEM_CODES_INSERT_COLUMNS = [
    "problem_code", "description",
]

PATIENT_PROBLEM_CODES_INSERT_COLUMNS = [
    "problem_code", "description",
]

# Map file type to insert columns
INSERT_COLUMNS = {
    "master": MASTER_INSERT_COLUMNS,
    "device": DEVICE_INSERT_COLUMNS,
    "patient": PATIENT_INSERT_COLUMNS,
    "text": TEXT_INSERT_COLUMNS,
    "problem": PROBLEM_INSERT_COLUMNS,
    "patient_problem": PATIENT_PROBLEM_INSERT_COLUMNS,
    "asr": ASR_INSERT_COLUMNS,
    "asr_ppc": ASR_PPC_INSERT_COLUMNS,
    "den": DEN_INSERT_COLUMNS,
    "disclaimer": DISCLAIMER_INSERT_COLUMNS,
    "problem_lookup": PROBLEM_CODES_INSERT_COLUMNS,
    "patient_problem_data": PATIENT_PROBLEM_CODES_INSERT_COLUMNS,
}


class MAUDELoader:
    """Load MAUDE data into DuckDB database with dynamic schema support."""

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
            filter_product_codes: Product codes to filter by.
                - None (default): no filtering (load all products)
                - List[str]: filter by specified codes
        """
        self.db_path = db_path or config.database.path
        self.batch_size = batch_size
        self.filter_product_codes = filter_product_codes
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
        Load a single MAUDE file into the database using dynamic schema detection.

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

        # Detect schema from file header
        schema = self.parser.detect_schema_from_header(filepath, file_type)

        result = LoadResult(
            file_type=file_type,
            filename=filepath.name,
            schema_info=schema,
        )

        logger.info(
            f"Loading {file_type} file: {filepath.name} "
            f"({schema.column_count} columns detected)"
        )

        if not schema.is_valid:
            logger.warning(f"Schema validation: {schema.validation_message}")

        # Determine if we need to filter by product code
        # NOTE: Only device files have product codes - master files don't have PRODUCT_CODE
        should_filter_by_product = file_type == "device" and self.filter_product_codes

        # For master and related tables, filter by MDR keys from loaded devices
        # This requires loading devices FIRST when using product code filtering
        # Only filter by MDR keys if we're also filtering by product codes
        filter_by_mdr = (
            file_type in ["master", "patient", "text", "problem"]
            and self.filter_product_codes is not None
        )

        own_connection = conn is None
        if own_connection:
            conn = duckdb.connect(str(self.db_path))

        try:
            batch = []

            # Choose appropriate parser based on file type
            if file_type in self.parser.CSV_FILE_TYPES:
                records_gen = self.parser.parse_csv_file(
                    filepath,
                    file_type=file_type,
                    map_to_db_columns=True,
                )
            elif file_type == "den":
                records_gen = self.parser.parse_den_file(
                    filepath,
                    map_to_db_columns=True,
                )
            else:
                records_gen = self.parser.parse_file_dynamic(
                    filepath,
                    schema=schema,
                    file_type=file_type,
                    map_to_db_columns=True,  # Get DB column names
                )

            # Use dynamic parsing
            for record in records_gen:
                result.records_processed += 1

                try:
                    # Validate mdr_report_key before processing
                    # FDA data has quality issues - some records have malformed keys
                    # due to embedded newlines in text fields
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

                    # Apply product code filter for device files only
                    if should_filter_by_product:
                        product_code = transformed.get("device_report_product_code")
                        if product_code not in self.filter_product_codes:
                            result.records_skipped += 1
                            continue

                    # Apply MDR key filter for related tables
                    if filter_by_mdr and self._loaded_mdr_keys:
                        mdr_key = transformed.get("mdr_report_key")
                        if mdr_key not in self._loaded_mdr_keys:
                            result.records_skipped += 1
                            continue

                    # Track MDR keys from device table (devices have product codes)
                    # These keys are used to filter master and related tables
                    if file_type == "device":
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
        Insert a batch of records into the database using fast bulk insert.

        Uses pandas DataFrame for efficient bulk loading into DuckDB.

        Deduplication Strategy:
        - master_events: Uses INSERT OR REPLACE on mdr_report_key
        - Child tables (devices, patients, mdr_text, device_problems):
          DELETE existing records for MDR keys in batch, then INSERT.
          This prevents duplicates when re-loading files.

        Args:
            conn: Database connection.
            file_type: Type of records.
            batch: List of record dictionaries.

        Returns:
            Number of records inserted.
        """
        if not batch:
            return 0

        import pandas as pd

        table_name = self._get_table_name(file_type)
        columns = self._get_insert_columns(file_type)

        # Convert batch to DataFrame for fast bulk insert
        # Only include columns that exist in INSERT_COLUMNS
        rows = []
        for record in batch:
            row = {}
            for col in columns:
                val = record.get(col)
                # Convert date objects to strings for DuckDB
                if isinstance(val, date):
                    val = val.isoformat()
                row[col] = val
            rows.append(row)

        df = pd.DataFrame(rows, columns=columns)

        try:
            # For child tables, DELETE existing records first to prevent duplicates
            # This is necessary because:
            # 1. Child tables don't have unique constraints on (mdr_report_key + sequence)
            # 2. Re-loading the same file would create duplicate rows
            # 3. Weekly files may overlap with previously loaded data
            if file_type in ("device", "patient", "text", "problem"):
                # Get unique MDR keys from this batch
                mdr_keys = list(set(
                    r.get("mdr_report_key")
                    for r in batch
                    if r.get("mdr_report_key")
                ))

                if mdr_keys:
                    # Delete existing records for these MDR keys
                    # Using chunked deletion for large batches
                    chunk_size = 500
                    for i in range(0, len(mdr_keys), chunk_size):
                        chunk = mdr_keys[i:i + chunk_size]
                        placeholders = ",".join(["?" for _ in chunk])
                        conn.execute(
                            f"DELETE FROM {table_name} WHERE mdr_report_key IN ({placeholders})",
                            chunk
                        )
                    logger.debug(
                        f"Deleted existing {file_type} records for {len(mdr_keys)} MDR keys"
                    )

            # Use DuckDB's fast DataFrame insertion
            # IMPORTANT: Specify column names in both INSERT and SELECT to ensure alignment
            col_names = ", ".join(columns)
            # Quote column names in SELECT to handle any special characters
            select_cols = ", ".join([f'"{c}"' for c in columns])

            # For master_events, use INSERT OR REPLACE to handle duplicates
            # The same report can appear in multiple files (mdrfoi.txt + mdrfoiAdd.txt)
            # and we want to keep the most recent version
            if file_type == "master":
                insert_cmd = "INSERT OR REPLACE INTO"
            else:
                # Child tables: use plain INSERT after DELETE
                insert_cmd = "INSERT INTO"

            conn.execute(f"{insert_cmd} {table_name} ({col_names}) SELECT {select_cols} FROM df")
            return len(df)
        except Exception as e:
            logger.error(f"Error inserting batch: {e}")
            # Log first few records for debugging
            if len(df) > 0:
                logger.error(f"First record mdr_report_key: {df.iloc[0].get('mdr_report_key', 'N/A')}")
                logger.error(f"First record columns: {list(df.columns[:5])}")
            # Fall back to slower method with INSERT OR REPLACE
            try:
                placeholders = ", ".join(["?" for _ in columns])
                sql = f"INSERT OR REPLACE INTO {table_name} ({col_names}) VALUES ({placeholders})"
                values = [tuple(row[col] for col in columns) for row in rows]
                conn.executemany(sql, values)
                return len(values)
            except Exception as e2:
                logger.error(f"Fallback insert also failed: {e2}")
                return 0

    def _get_table_name(self, file_type: str) -> str:
        """Get database table name for file type."""
        table_map = {
            "master": "master_events",
            "device": "devices",
            "patient": "patients",
            "text": "mdr_text",
            "problem": "device_problems",
            "patient_problem": "patient_problems",
            "asr": "asr_reports",
            "asr_ppc": "asr_patient_problems",
            "den": "den_reports",
            "disclaimer": "manufacturer_disclaimers",
            "problem_lookup": "problem_codes",
            "patient_problem_data": "patient_problem_codes",
        }
        return table_map.get(file_type, file_type)

    def _get_insert_columns(self, file_type: str) -> List[str]:
        """Get columns for INSERT statement from expanded column lists."""
        return INSERT_COLUMNS.get(file_type, [])

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
            # IMPORTANT: Device must be loaded FIRST when using product code filtering
            # because only device files have PRODUCT_CODE. MDR keys from devices are
            # then used to filter master and related tables.
            # New file types added: patient_problem, asr, asr_ppc, den, problem_lookup
            file_types = [
                "device", "master", "patient", "text", "problem",
                "patient_problem", "asr", "asr_ppc", "den",
                "problem_lookup", "patient_problem_data", "disclaimer"
            ]

        all_results = {}

        with get_connection(self.db_path) as conn:
            # Initialize schema
            initialize_database(conn)

            # Load files in order (device first to get MDR keys for product code filtering)
            for file_type in file_types:
                pattern = self._get_file_pattern(file_type)
                files = sorted(data_dir.glob(pattern))

                # Exclude problem files from device pattern
                if file_type == "device":
                    files = [f for f in files if "problem" not in f.name.lower()]
                    # Also include device{year}.txt files
                    device_year_files = sorted(data_dir.glob("device*.txt"))
                    device_year_files = [f for f in device_year_files if "problem" not in f.name.lower()]
                    files = sorted(set(files + device_year_files))

                # For ASR, exclude the asr_ppc files
                if file_type == "asr":
                    files = [f for f in files if "ppc" not in f.name.lower()]

                # For DEN, only include 2-digit year files (mdr84-mdr97)
                if file_type == "den":
                    valid_files = []
                    for f in files:
                        name = f.name.lower()
                        if name.startswith("mdr") and len(name) <= 9:
                            year_part = name[3:5]
                            if year_part.isdigit():
                                year = int(year_part)
                                if 84 <= year <= 97:
                                    valid_files.append(f)
                    files = valid_files

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
            "problem": "foidevproblem*.txt",
            "patient_problem": "patientproblemcode*.txt",
            "asr": "asr_*.txt",
            "asr_ppc": "asr_ppc*.txt",
            "den": "mdr*.txt",  # Will be filtered for 84-97 in load logic
            "disclaimer": "disclaim*.txt",
            "problem_lookup": "deviceproblemcodes.txt",
            "patient_problem_data": "patientproblemdata*.txt",
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

            # Prepare schema info JSON
            schema_info = None
            if result.schema_info:
                import json
                schema_info = json.dumps({
                    "column_count": result.schema_info.column_count,
                    "has_header": result.schema_info.has_header,
                    "is_valid": result.schema_info.is_valid,
                    "validation_message": result.schema_info.validation_message,
                })

            conn.execute(
                """
                INSERT INTO ingestion_log
                (id, file_name, file_type, source, records_processed, records_loaded,
                 records_errors, started_at, completed_at, status, error_message, schema_info)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    schema_info,
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

    def populate_master_from_devices(
        self, conn: duckdb.DuckDBPyConnection
    ) -> tuple[int, int]:
        """
        Populate manufacturer_clean and product_code in master_events from devices table.

        The FDA MAUDE data architecture stores manufacturer and product code information
        in the device file (foidev.txt), NOT in the master file (mdrfoi.txt). The master
        file's MANUFACTURER_NAME field is 99.99% empty by design.

        This method should be called after loading both devices and master_events tables
        to copy the manufacturer and product_code data from devices to master_events.

        Args:
            conn: Database connection.

        Returns:
            Tuple of (manufacturer_records_updated, product_code_records_updated).
        """
        logger.info("Populating master_events from devices table...")

        # Check current state
        before = conn.execute("""
            SELECT
                COUNT(manufacturer_clean) as has_mfr,
                COUNT(product_code) as has_product
            FROM master_events
        """).fetchone()

        # Update master_events with data from devices
        # Uses FIRST() aggregation since one master event may have multiple devices
        conn.execute("""
            UPDATE master_events
            SET
                manufacturer_clean = COALESCE(master_events.manufacturer_clean, sub.manufacturer_d_clean),
                product_code = COALESCE(master_events.product_code, sub.device_report_product_code)
            FROM (
                SELECT
                    mdr_report_key,
                    FIRST(manufacturer_d_clean) as manufacturer_d_clean,
                    FIRST(device_report_product_code) as device_report_product_code
                FROM devices
                WHERE manufacturer_d_clean IS NOT NULL
                   OR device_report_product_code IS NOT NULL
                GROUP BY mdr_report_key
            ) sub
            WHERE master_events.mdr_report_key = sub.mdr_report_key
              AND (master_events.manufacturer_clean IS NULL OR master_events.product_code IS NULL)
        """)

        # Check results
        after = conn.execute("""
            SELECT
                COUNT(manufacturer_clean) as has_mfr,
                COUNT(product_code) as has_product
            FROM master_events
        """).fetchone()

        mfr_added = after[0] - before[0]
        product_added = after[1] - before[1]

        logger.info(
            f"Populated master_events: {mfr_added:,} manufacturer records, "
            f"{product_added:,} product_code records added"
        )

        return mfr_added, product_added


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

    # Load problem codes
    problem_codes_file = lookups_dir / "problem_codes.csv"
    if problem_codes_file.exists():
        logger.info("Loading problem codes lookup...")
        conn.execute("DELETE FROM problem_codes")
        try:
            conn.execute(f"""
                INSERT INTO problem_codes (problem_code, description, category)
                SELECT problem_code, description, category
                FROM read_csv_auto('{problem_codes_file}')
            """)
        except Exception as e:
            # Try simpler format
            logger.warning(f"Could not load problem codes with category: {e}")
            try:
                conn.execute(f"""
                    INSERT INTO problem_codes (problem_code, description)
                    SELECT problem_code, description
                    FROM read_csv_auto('{problem_codes_file}')
                """)
            except Exception as e2:
                logger.warning(f"Could not load problem codes: {e2}")


if __name__ == "__main__":
    import argparse

    arg_parser = argparse.ArgumentParser(description="Load MAUDE data into DuckDB")
    arg_parser.add_argument("--data-dir", type=Path, default=config.data.raw_path)
    arg_parser.add_argument("--db", type=Path, default=config.database.path)
    arg_parser.add_argument("--type", help="File type to load")
    arg_parser.add_argument("--file", type=Path, help="Single file to load")
    arg_parser.add_argument(
        "--filter-scs",
        action="store_true",
        help="Filter to only load SCS product codes (default: load all)",
    )

    args = arg_parser.parse_args()

    filter_codes = SCS_PRODUCT_CODES if args.filter_scs else None
    loader = MAUDELoader(db_path=args.db, filter_product_codes=filter_codes)

    if args.file:
        result = loader.load_file(args.file, args.type)
        print(f"Loaded: {result.records_loaded}, Errors: {result.records_errors}")
        if result.schema_info:
            print(f"Schema: {result.schema_info.column_count} columns")
    else:
        types = [args.type] if args.type else None
        results = loader.load_all_files(args.data_dir, types)

        print("\nLoad Summary:")
        for file_type, res_list in results.items():
            total_loaded = sum(r.records_loaded for r in res_list)
            total_errors = sum(r.records_errors for r in res_list)
            print(f"  {file_type}: {total_loaded:,} loaded, {total_errors:,} errors")
