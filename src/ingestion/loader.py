"""Load transformed MAUDE data into DuckDB with dynamic schema support."""

import duckdb
import fnmatch
import re
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Any, List, Optional, Generator
from dataclasses import dataclass, field
from tqdm import tqdm
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger
from src.database import get_connection, initialize_database
from src.ingestion.parser import MAUDEParser, FILE_COLUMNS, SchemaInfo
from src.ingestion.transformer import DataTransformer, transform_record
from src.ingestion.validation_framework import ValidationPipeline, StageValidationResult

logger = get_logger("loader")


def glob_case_insensitive(directory: Path, pattern: str) -> List[Path]:
    """
    Case-insensitive glob matching for file patterns.

    Handles both uppercase (DEVICE2020.txt) and lowercase (device2020.txt) filenames
    that may be encountered on different systems or from different sources.

    Args:
        directory: Directory to search in.
        pattern: Glob pattern (e.g., "device*.txt").

    Returns:
        List of matching Path objects, sorted alphabetically.
    """
    # Convert glob pattern to regex pattern (case-insensitive)
    # Replace * with .* and ? with . for regex
    regex_pattern = fnmatch.translate(pattern)
    regex = re.compile(regex_pattern, re.IGNORECASE)

    matches = []
    try:
        for item in directory.iterdir():
            if item.is_file() and regex.match(item.name):
                matches.append(item)
    except OSError as e:
        logger.warning(f"Error reading directory {directory}: {e}")

    return sorted(matches)


def select_files_for_load(files: List[Path], file_type: str) -> List[Path]:
    """
    Select the correct subset of files to load for a given file type.

    CRITICAL: This function fixes the bug where ALL Thru files were loaded
    instead of just the latest one for cumulative file types.

    File Type Behaviors:
    - master, patient: Cumulative Thru files - only load LATEST Thru file
    - device: Incremental Thru + annual files - load ALL (no overlap)
    - text: Incremental annual files - load ALL (no overlap)

    For all types:
    - Load current file (e.g., mdrfoi.txt)
    - Load Add file (e.g., mdrfoiAdd.txt)
    - Load Change file LAST (e.g., mdrfoiChange.txt)

    Args:
        files: List of file paths matching the file type pattern
        file_type: Type of file (master, device, patient, text, problem)

    Returns:
        Filtered and correctly ordered list of files to load
    """
    if not files:
        return []

    # Categorize files
    thru_files = []
    annual_files = []
    current_files = []
    add_files = []
    change_files = []

    for f in files:
        name = f.name.lower()

        if "thru" in name:
            # Extract year from thru file
            match = re.search(r'thru(\d{4})', name, re.IGNORECASE)
            year = int(match.group(1)) if match else 0
            thru_files.append((year, f))
        elif "change" in name:
            change_files.append(f)
        elif "add" in name:
            add_files.append(f)
        elif re.search(r'\d{4}', name):
            # Annual file with year
            year_match = re.search(r'(\d{4})', name)
            year = int(year_match.group(1)) if year_match else 0
            annual_files.append((year, f))
        else:
            # Current file (no year in name)
            current_files.append(f)

    result = []

    # Handle Thru files based on file type
    if thru_files:
        if file_type in ["master", "patient"]:
            # CUMULATIVE: Only load the LATEST Thru file
            # mdrfoiThru2025 contains ALL data through 2025 (supersedes Thru2023)
            thru_files.sort(key=lambda x: x[0], reverse=True)
            latest_thru = thru_files[0]
            result.append(latest_thru[1])

            # Log if we're skipping older Thru files
            if len(thru_files) > 1:
                skipped = [f[1].name for f in thru_files[1:]]
                logger.info(
                    f"Using latest cumulative file {latest_thru[1].name}, "
                    f"skipping older: {skipped}"
                )
        else:
            # INCREMENTAL (device, text): Load all Thru files in order
            # foidevthru1997 is pre-1998 data, separate from annual files
            thru_files.sort(key=lambda x: x[0])
            result.extend([f[1] for f in thru_files])

    # Add annual files in chronological order
    if annual_files:
        annual_files.sort(key=lambda x: x[0])
        result.extend([f[1] for f in annual_files])

    # Add current file (base file without year)
    result.extend(current_files)

    # Add files come BEFORE Change files (critical ordering)
    result.extend(add_files)
    result.extend(change_files)

    return result


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
    # File audit tracking
    source_record_count: Optional[int] = None  # Count from source file (CSV-parsed, may be wrong)
    physical_line_count: Optional[int] = None  # Physical lines in file (ground truth)
    record_count_variance_pct: Optional[float] = None  # Difference between source and loaded
    column_mismatch_count: int = 0
    checksum: Optional[str] = None
    transaction_committed: bool = False
    # Three-stage validation results
    stage1_validation: Optional[StageValidationResult] = None
    stage2_validation_errors: int = 0
    stage2_validation_warnings: int = 0
    stage3_validation: Optional[StageValidationResult] = None
    duplicates_removed: int = 0
    # Quote-swallowing detection
    quote_swallowing_detected: bool = False
    # Batch insert tracking
    batches_committed: int = 0
    batch_insert_errors: int = 0


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
    # New columns in 2020+ format (will be NULL for pre-2020 files)
    "implant_date_year", "date_removed_year", "serviced_by_3rd_party_flag",
    "date_received",
    "brand_name", "generic_name",
    "manufacturer_d_name", "manufacturer_d_address_1", "manufacturer_d_address_2",
    "manufacturer_d_city", "manufacturer_d_state", "manufacturer_d_zip",
    "manufacturer_d_zip_ext", "manufacturer_d_country", "manufacturer_d_postal",
    "expiration_date_of_device",
    "model_number", "catalog_number", "lot_number", "other_id_number",
    "device_operator", "device_availability", "date_returned_to_manufacturer",
    "device_report_product_code", "device_age_text", "device_evaluated_by_manufacturer",
    # New columns at end in 2020+ format (will be NULL for pre-2020 files)
    "combination_product_flag", "udi_di", "udi_public",
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


# Unique constraint key columns for duplicate detection
# These define the natural keys that should be unique within each table
UNIQUE_CONSTRAINT_KEYS = {
    "master": ["mdr_report_key"],
    "device": ["mdr_report_key", "device_sequence_number"],
    "patient": ["mdr_report_key", "patient_sequence_number"],
    "text": ["mdr_report_key", "mdr_text_key"],
    "problem": ["mdr_report_key", "problem_code"],
    "patient_problem": ["mdr_report_key", "problem_code"],
}


def validate_after_file_load(
    conn: duckdb.DuckDBPyConnection,
    file_type: str,
    filename: str,
    expected_min: int = 0,
) -> tuple[bool, List[str]]:
    """
    Validate data integrity immediately after each file loads.

    This provides real-time validation during loading rather than only at the end,
    catching issues early before they compound.

    Args:
        conn: Database connection.
        file_type: Type of file just loaded (master, device, patient, text, problem).
        filename: Name of the file that was just loaded.
        expected_min: Minimum expected record count (0 = no minimum check).

    Returns:
        Tuple of (passed, list_of_issues).
    """
    issues = []
    table_map = {
        "master": "master_events",
        "device": "devices",
        "patient": "patients",
        "text": "mdr_text",
        "problem": "device_problems",
    }

    table_name = table_map.get(file_type)
    if not table_name:
        return True, []

    try:
        # Check 1: Record count is reasonable
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"Validation: {table_name} has {count:,} total records after loading {filename}")

        if expected_min > 0 and count < expected_min:
            issues.append(
                f"CRITICAL: {table_name} has {count:,} records, expected at least {expected_min:,}"
            )

        if file_type == "master":
            # Check 2: No NULL mdr_report_keys
            null_keys = conn.execute(
                "SELECT COUNT(*) FROM master_events WHERE mdr_report_key IS NULL"
            ).fetchone()[0]
            if null_keys > 0:
                issues.append(f"CRITICAL: {null_keys:,} NULL mdr_report_keys in master_events")

            # Check 3: Date range is reasonable (should span 1991-current)
            date_range = conn.execute("""
                SELECT MIN(date_received), MAX(date_received)
                FROM master_events
                WHERE date_received IS NOT NULL
            """).fetchone()
            if date_range[0] and date_range[1]:
                logger.info(f"Validation: master_events date range: {date_range[0]} to {date_range[1]}")

            # Check 4: Duplicate MDR keys (should be zero with proper dedup)
            dup_count = conn.execute("""
                SELECT COUNT(*) - COUNT(DISTINCT mdr_report_key) as duplicates
                FROM master_events
            """).fetchone()[0]
            if dup_count > 0:
                issues.append(f"WARNING: {dup_count:,} duplicate mdr_report_keys in master_events")

        elif file_type == "patient":
            # Check orphaned patients (no matching master record)
            # Only check if master_events has data
            master_count = conn.execute("SELECT COUNT(*) FROM master_events").fetchone()[0]
            if master_count > 0:
                orphans = conn.execute("""
                    SELECT COUNT(*) FROM patients p
                    WHERE NOT EXISTS (
                        SELECT 1 FROM master_events m
                        WHERE m.mdr_report_key = p.mdr_report_key
                    )
                """).fetchone()[0]
                if orphans > 0:
                    # This is a warning, not critical - some orphans expected
                    logger.warning(f"Validation: {orphans:,} orphaned patient records (no matching master)")

        elif file_type == "text":
            # Check orphaned text records
            master_count = conn.execute("SELECT COUNT(*) FROM master_events").fetchone()[0]
            if master_count > 0:
                orphans = conn.execute("""
                    SELECT COUNT(*) FROM mdr_text t
                    WHERE NOT EXISTS (
                        SELECT 1 FROM master_events m
                        WHERE m.mdr_report_key = t.mdr_report_key
                    )
                """).fetchone()[0]
                if orphans > 0:
                    logger.warning(f"Validation: {orphans:,} orphaned text records (no matching master)")

        elif file_type == "device":
            # Check device data quality
            null_product_codes = conn.execute("""
                SELECT COUNT(*) FROM devices
                WHERE device_report_product_code IS NULL OR device_report_product_code = ''
            """).fetchone()[0]
            total_devices = conn.execute("SELECT COUNT(*) FROM devices").fetchone()[0]
            if total_devices > 0:
                pct_null = (null_product_codes / total_devices) * 100
                if pct_null > 10:  # More than 10% missing is concerning
                    logger.warning(
                        f"Validation: {null_product_codes:,} devices ({pct_null:.1f}%) "
                        f"missing product code"
                    )

    except Exception as e:
        issues.append(f"Validation error: {e}")
        logger.error(f"Error during post-load validation: {e}")

    passed = len([i for i in issues if i.startswith("CRITICAL")]) == 0
    return passed, issues


class MAUDELoader:
    """Load MAUDE data into DuckDB database with dynamic schema support."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        batch_size: int = 10000,
        filter_product_codes: Optional[List[str]] = None,
        enable_transaction_safety: bool = True,
        variance_threshold_pct: float = 0.1,
        detect_duplicates: bool = True,
        enable_validation: bool = True,
        commit_every_n_batches: int = 50,
    ):
        """
        Initialize the loader.

        Args:
            db_path: Path to database file.
            batch_size: Number of records to insert per batch.
            filter_product_codes: Product codes to filter by.
                - None (default): no filtering (load all products)
                - List[str]: filter by specified codes
            enable_transaction_safety: Use explicit transactions with rollback on failure.
            variance_threshold_pct: Max acceptable variance between source and loaded counts.
            detect_duplicates: Check for duplicate keys within batches before insert.
            enable_validation: Enable three-stage validation pipeline.
            commit_every_n_batches: Commit transaction after this many batches to prevent OOM.
                Default 50 batches (500K records with default batch_size). Set to 0 to
                disable incremental commits (single transaction for entire file).
        """
        self.db_path = db_path or config.database.path
        self.batch_size = batch_size
        self.filter_product_codes = filter_product_codes
        self.enable_transaction_safety = enable_transaction_safety
        self.variance_threshold_pct = variance_threshold_pct
        self.detect_duplicates = detect_duplicates
        self.enable_validation = enable_validation
        self.commit_every_n_batches = commit_every_n_batches
        self.parser = MAUDEParser()
        self.transformer = DataTransformer()

        # Track MDR keys for filtering related tables
        self._loaded_mdr_keys = set()

        # Track duplicate key violations per file
        self._duplicate_count = 0
        self._duplicate_samples: List[Dict[str, Any]] = []

        # Initialize validation pipeline
        self._validation_pipeline = ValidationPipeline(db_path=self.db_path) if enable_validation else None
        self._stage2_errors = 0
        self._stage2_warnings = 0

    def _count_source_records(self, filepath: Path) -> int:
        """
        Count records in source file without full parsing.

        Args:
            filepath: Path to the source file.

        Returns:
            Number of data records (excluding header).
        """
        return self.parser.count_records(filepath)

    def _update_file_audit(
        self,
        conn: duckdb.DuckDBPyConnection,
        result: LoadResult,
        load_started: datetime,
        status: str = "COMPLETED"
    ) -> None:
        """
        Update file audit table with load results.

        Args:
            conn: Database connection.
            result: LoadResult with load statistics.
            load_started: When the load started.
            status: Load status (PENDING, IN_PROGRESS, COMPLETED, FAILED, PARTIAL).
        """
        try:
            # Calculate variance
            variance_pct = None
            if result.source_record_count and result.source_record_count > 0:
                variance_pct = abs(
                    result.source_record_count - result.records_loaded
                ) / result.source_record_count * 100

            # Check if file_audit table exists
            try:
                conn.execute("SELECT 1 FROM file_audit LIMIT 1")
            except Exception:
                # Table doesn't exist yet, skip audit
                return

            # Insert or update file audit record
            # Note: Use datetime.now() as parameter instead of CURRENT_TIMESTAMP
            # DuckDB has issues with CURRENT_TIMESTAMP in VALUES clause with parameters
            now = datetime.now()
            conn.execute("""
                INSERT INTO file_audit (
                    filename, file_type, source_record_count, loaded_record_count,
                    skipped_record_count, error_record_count, column_mismatch_count,
                    load_status, schema_version, detected_column_count,
                    load_started, load_completed, error_message, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (filename) DO UPDATE SET
                    source_record_count = EXCLUDED.source_record_count,
                    loaded_record_count = EXCLUDED.loaded_record_count,
                    skipped_record_count = EXCLUDED.skipped_record_count,
                    error_record_count = EXCLUDED.error_record_count,
                    column_mismatch_count = EXCLUDED.column_mismatch_count,
                    load_status = EXCLUDED.load_status,
                    load_completed = EXCLUDED.load_completed,
                    error_message = EXCLUDED.error_message,
                    updated_at = EXCLUDED.updated_at
            """, [
                result.filename,
                result.file_type,
                result.source_record_count,
                result.records_loaded,
                result.records_skipped,
                result.records_errors,
                result.column_mismatch_count,
                status,
                result.schema_info.validation_message if result.schema_info else None,
                result.schema_info.column_count if result.schema_info else None,
                load_started,
                now,
                "; ".join(result.error_messages[:5]) if result.error_messages else None,
                now,  # updated_at
            ])

            # Flag if variance exceeds threshold
            if variance_pct and variance_pct > self.variance_threshold_pct:
                logger.warning(
                    f"Record count variance {variance_pct:.2f}% exceeds threshold "
                    f"{self.variance_threshold_pct}% for {result.filename}"
                )

        except Exception as e:
            logger.warning(f"Could not update file audit: {e}")

    def _detect_batch_duplicates(
        self,
        batch: List[Dict[str, Any]],
        file_type: str,
    ) -> tuple[List[Dict[str, Any]], int]:
        """
        Detect and remove duplicate records within a batch based on unique constraint keys.

        This catches duplicates that would otherwise cause issues during insert,
        particularly for child tables where the same (mdr_report_key, sequence_number)
        combination might appear multiple times in the same file.

        Args:
            batch: List of record dictionaries.
            file_type: Type of records being loaded.

        Returns:
            Tuple of (deduplicated_batch, duplicate_count).
        """
        key_cols = UNIQUE_CONSTRAINT_KEYS.get(file_type)
        if not key_cols:
            return batch, 0

        seen_keys: set = set()
        deduplicated = []
        duplicates = 0

        for record in batch:
            # Build composite key from key columns
            key_values = []
            for col in key_cols:
                val = record.get(col)
                # Normalize None values for consistent hashing
                key_values.append(str(val) if val is not None else "")

            key = tuple(key_values)

            if key in seen_keys:
                duplicates += 1
                # Log sample duplicates (first 5)
                if len(self._duplicate_samples) < 5:
                    self._duplicate_samples.append({
                        "file_type": file_type,
                        "key_columns": key_cols,
                        "key_values": key_values,
                        "mdr_report_key": record.get("mdr_report_key"),
                    })
            else:
                seen_keys.add(key)
                deduplicated.append(record)

        if duplicates > 0:
            self._duplicate_count += duplicates
            logger.debug(
                f"Detected {duplicates} duplicate records in {file_type} batch "
                f"(key: {key_cols})"
            )

        return deduplicated, duplicates

    def load_file(
        self,
        filepath: Path,
        file_type: Optional[str] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> LoadResult:
        """
        Load a single MAUDE file into the database using dynamic schema detection.

        Features:
        - Pre-load source record counting for completeness tracking
        - Transaction safety with rollback on failure
        - Post-load record count verification
        - File audit table updates

        Args:
            filepath: Path to the file.
            file_type: Type of file (auto-detected if None).
            conn: Database connection (created if None).

        Returns:
            LoadResult with statistics.
        """
        start_time = datetime.now()
        load_started = datetime.now()

        # Reset tracking for new file
        self._duplicate_count = 0
        self._duplicate_samples = []
        self._stage2_errors = 0
        self._stage2_warnings = 0

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

        # STAGE 1: Pre-Parse Validation
        if self._validation_pipeline:
            result.stage1_validation = self._validation_pipeline.validate_stage1_preparse(
                filepath, file_type
            )
            if not result.stage1_validation.passed:
                logger.warning(
                    f"Stage 1 validation failed for {filepath.name}: "
                    f"{result.stage1_validation.error_count} errors, "
                    f"{result.stage1_validation.warning_count} warnings"
                )
                # Log first few issues
                for issue in result.stage1_validation.issues[:3]:
                    logger.warning(f"  [{issue.severity}] {issue.code}: {issue.message}")

        # Pre-load: Count source records for completeness tracking
        try:
            result.source_record_count = self._count_source_records(filepath)
            logger.info(
                f"Source file {filepath.name} contains {result.source_record_count:,} records"
            )
        except Exception as e:
            logger.warning(f"Could not count source records: {e}")

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
            # Set memory limit to prevent OOM errors during large batch inserts
            # Use 8GB limit (leaves room for system and Python)
            conn.execute("SET memory_limit='8GB'")
            # Reduce threads to lower memory pressure
            conn.execute("SET threads=4")

        transaction_started = False
        parse_result = None  # Track parse result for column mismatch stats
        batches_in_current_transaction = 0  # Track batches for incremental commit

        try:
            # Begin transaction for data integrity
            if self.enable_transaction_safety:
                conn.execute("BEGIN TRANSACTION")
                transaction_started = True
                logger.debug(f"Started transaction for {filepath.name}")

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

                    # STAGE 2: Post-Transform Validation
                    if self._validation_pipeline:
                        stage2_result = self._validation_pipeline.validate_stage2_post_transform(
                            transformed, file_type
                        )
                        self._stage2_errors += stage2_result.error_count
                        self._stage2_warnings += stage2_result.warning_count

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
                        try:
                            inserted = self._insert_batch(conn, file_type, batch)
                            result.records_loaded += inserted
                            result.batches_committed += 1
                            batches_in_current_transaction += 1
                        except Exception as batch_err:
                            result.batch_insert_errors += 1
                            if len(result.error_messages) < 10:
                                result.error_messages.append(f"Batch insert error: {batch_err}")
                            # CRITICAL FIX: Proper transaction recovery
                            # When a batch insert fails, the transaction is aborted.
                            # We must rollback and start a NEW transaction to continue.
                            if transaction_started:
                                try:
                                    # First try to rollback (may already be aborted)
                                    try:
                                        conn.execute("ROLLBACK")
                                    except Exception:
                                        pass  # Transaction may already be aborted
                                    # Start fresh transaction for remaining batches
                                    conn.execute("BEGIN TRANSACTION")
                                    batches_in_current_transaction = 0
                                    logger.warning(
                                        f"Recovered from batch error, starting new transaction. "
                                        f"Error was: {batch_err}"
                                    )
                                except Exception as recovery_err:
                                    logger.error(f"Failed to recover transaction: {recovery_err}")
                                    # Mark transaction as not started to avoid commit attempts
                                    transaction_started = False
                        batch = []

                        # Incremental commit to prevent OOM on large files
                        # Commit every N batches to release memory
                        if (self.commit_every_n_batches > 0 and
                            batches_in_current_transaction >= self.commit_every_n_batches and
                            transaction_started):
                            try:
                                conn.execute("COMMIT")
                                conn.execute("BEGIN TRANSACTION")
                                batches_in_current_transaction = 0
                                logger.debug(
                                    f"Incremental commit after {result.records_loaded:,} records"
                                )
                            except Exception as commit_err:
                                logger.error(f"Incremental commit failed: {commit_err}")
                                # Try to recover
                                try:
                                    conn.execute("ROLLBACK")
                                except Exception:
                                    pass
                                try:
                                    conn.execute("BEGIN TRANSACTION")
                                    batches_in_current_transaction = 0
                                except Exception:
                                    transaction_started = False

                except Exception as e:
                    result.records_errors += 1
                    if len(result.error_messages) < 10:
                        result.error_messages.append(str(e))

            # Insert remaining records
            if batch:
                try:
                    inserted = self._insert_batch(conn, file_type, batch)
                    result.records_loaded += inserted
                    result.batches_committed += 1
                except Exception as batch_err:
                    result.batch_insert_errors += 1
                    if len(result.error_messages) < 10:
                        result.error_messages.append(f"Final batch insert error: {batch_err}")

            # Commit transaction on success
            if self.enable_transaction_safety and transaction_started:
                conn.execute("COMMIT")
                result.transaction_committed = True
                logger.debug(f"Committed transaction for {filepath.name}")

            # Calculate record count variance
            if result.source_record_count and result.source_record_count > 0:
                result.record_count_variance_pct = abs(
                    result.source_record_count - result.records_loaded
                ) / result.source_record_count * 100

                if result.record_count_variance_pct > self.variance_threshold_pct:
                    logger.warning(
                        f"Record count variance {result.record_count_variance_pct:.2f}% "
                        f"exceeds threshold for {filepath.name}: "
                        f"source={result.source_record_count:,}, loaded={result.records_loaded:,}"
                    )

            # STAGE 3: Post-Load Validation
            if self._validation_pipeline:
                # Get physical line count from Stage 1 metrics (ground truth)
                physical_line_count = 0
                if result.stage1_validation and result.stage1_validation.metrics:
                    # Use valid_data_lines as the expected count (excludes header and orphan lines)
                    physical_line_count = result.stage1_validation.metrics.get("valid_data_lines", 0)

                result.stage3_validation = self._validation_pipeline.validate_stage3_post_load(
                    filename=filepath.name,
                    file_type=file_type,
                    expected_count=result.source_record_count or 0,
                    loaded_count=result.records_loaded,
                    physical_line_count=physical_line_count,
                )
                if not result.stage3_validation.passed:
                    logger.warning(
                        f"Stage 3 validation issues for {filepath.name}: "
                        f"{result.stage3_validation.error_count} errors"
                    )
                    # Log critical issues
                    for issue in result.stage3_validation.issues:
                        if issue.severity == "CRITICAL":
                            logger.error(f"  [{issue.code}] {issue.message}")

            # Capture stage 2 validation summary
            result.stage2_validation_errors = self._stage2_errors
            result.stage2_validation_warnings = self._stage2_warnings
            result.duplicates_removed = self._duplicate_count

            # Update file audit table
            self._update_file_audit(conn, result, load_started, "COMPLETED")

            # Real-time validation after file load
            # This catches data integrity issues immediately rather than at the end
            validation_passed, validation_issues = validate_after_file_load(
                conn, file_type, filepath.name, expected_min=0
            )
            if validation_issues:
                for issue in validation_issues:
                    if issue.startswith("CRITICAL"):
                        logger.error(f"Post-load validation: {issue}")
                        result.error_messages.append(issue)
                    else:
                        logger.warning(f"Post-load validation: {issue}")

        except Exception as e:
            # Rollback transaction on failure
            if self.enable_transaction_safety and transaction_started:
                try:
                    conn.execute("ROLLBACK")
                    logger.warning(f"Rolled back transaction for {filepath.name} due to error: {e}")
                except Exception as rollback_error:
                    logger.error(f"Failed to rollback transaction: {rollback_error}")

            # Update file audit with failure status
            self._update_file_audit(conn, result, load_started, "FAILED")
            result.error_messages.append(f"Load failed: {e}")
            raise

        finally:
            if own_connection:
                conn.close()

        result.duration_seconds = (datetime.now() - start_time).total_seconds()

        # Log duplicate detection summary
        if self._duplicate_count > 0:
            logger.warning(
                f"Detected {self._duplicate_count:,} duplicate records in {filepath.name}"
            )
            for sample in self._duplicate_samples[:3]:
                logger.warning(
                    f"  Duplicate sample: {sample['key_columns']} = {sample['key_values']}"
                )

        # Build validation summary for log
        validation_parts = []
        if self._duplicate_count > 0:
            validation_parts.append(f"{self._duplicate_count:,} duplicates removed")
        if self._stage2_errors > 0 or self._stage2_warnings > 0:
            validation_parts.append(
                f"validation: {self._stage2_errors} errors, {self._stage2_warnings} warnings"
            )
        if result.batch_insert_errors > 0:
            validation_parts.append(f"{result.batch_insert_errors} batch errors")

        validation_str = f", {', '.join(validation_parts)}" if validation_parts else ""

        logger.info(
            f"Loaded {filepath.name}: {result.records_loaded:,} records in {result.batches_committed} batches "
            f"({result.records_skipped:,} skipped, {result.records_errors:,} errors"
            f"{validation_str}) in {result.duration_seconds:.1f}s"
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
        1. Pre-insert: Detect and remove duplicates within the batch
           - devices: (mdr_report_key, device_sequence_number)
           - patients: (mdr_report_key, patient_sequence_number)
           - mdr_text: (mdr_report_key, mdr_text_key)
        2. master_events: Uses INSERT OR REPLACE on mdr_report_key
        3. Child tables (devices, patients, mdr_text, device_problems):
           DELETE existing records for MDR keys in batch, then INSERT.
           This prevents duplicates when re-loading files.

        Args:
            conn: Database connection.
            file_type: Type of records.
            batch: List of record dictionaries.

        Returns:
            Number of records inserted (after deduplication).
        """
        if not batch:
            return 0

        import pandas as pd

        # Step 1: Detect and remove duplicates within batch
        if self.detect_duplicates:
            batch, duplicate_count = self._detect_batch_duplicates(batch, file_type)
            if duplicate_count > 0:
                logger.debug(
                    f"Removed {duplicate_count} intra-batch duplicates for {file_type}"
                )
            if not batch:
                return 0

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

        # Define col_names before try block so it's available in except block
        col_names = ", ".join(columns)
        select_cols = ", ".join([f'"{c}"' for c in columns])

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
                # Use case-insensitive glob to handle DEVICE2020.txt vs device2020.txt
                files = glob_case_insensitive(data_dir, pattern)

                # Exclude problem files from device pattern
                if file_type == "device":
                    files = [f for f in files if "problem" not in f.name.lower()]
                    # Also include device{year}.txt files (case-insensitive)
                    device_year_files = glob_case_insensitive(data_dir, "device*.txt")
                    device_year_files = [f for f in device_year_files if "problem" not in f.name.lower()]
                    files = list(set(files + device_year_files))

                # CRITICAL FIX: Apply file selection logic
                # This fixes the bug where ALL Thru files were loaded instead of latest
                files = select_files_for_load(files, file_type)

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
        self, conn: duckdb.DuckDBPyConnection, result: LoadResult,
        parse_result: Optional['ParseResult'] = None
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
                schema_data = {
                    "column_count": result.schema_info.column_count,
                    "has_header": result.schema_info.has_header,
                    "is_valid": result.schema_info.is_valid,
                    "validation_message": result.schema_info.validation_message,
                }
                # Add column mismatch info from parse result if available
                if parse_result and hasattr(parse_result, 'column_mismatch_count'):
                    schema_data["column_mismatch_count"] = parse_result.column_mismatch_count
                    if parse_result.column_mismatch_samples:
                        schema_data["column_mismatch_samples"] = parse_result.column_mismatch_samples[:10]
                schema_info = json.dumps(schema_data)

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

    args = arg_parser.parse_args()

    loader = MAUDELoader(db_path=args.db)

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
