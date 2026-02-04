"""Parse FDA MAUDE data files with dynamic schema detection.

Enhanced with:
- Year-based schema detection for historical files
- Automatic encoding detection for older files
- Support for schema variations across FDA MAUDE file types
"""

import csv
import chardet
import re
from pathlib import Path
from typing import Generator, Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import get_logger
from config.schema_registry import (
    FDA_FILE_COLUMNS,
    HEADERLESS_FILES,
    get_fda_columns,
    is_headerless_file,
    get_expected_column_count,
    validate_schema,
    get_columns_for_count,
    get_alternative_column_counts,
    ASR_COLUMNS_FDA,
    ASR_PPC_COLUMNS_FDA,
    DEN_COLUMNS_FDA,
    PATIENT_PROBLEM_COLUMNS_FDA,
    PROBLEM_CODES_LOOKUP_COLUMNS_FDA,
    DISCLAIMER_COLUMNS_FDA,
)
from config.column_mappings import (
    COLUMN_MAPPINGS,
    get_db_column_name,
    map_record_columns,
)

# Import historical schema detection functions
try:
    from config.schemas import (
        get_device_schema,
        get_master_schema,
        get_text_schema,
    )
    HAS_HISTORICAL_SCHEMAS = True
except ImportError:
    HAS_HISTORICAL_SCHEMAS = False

logger = get_logger("parser")

# Increase CSV field size limit for large narrative text fields
csv.field_size_limit(sys.maxsize)


# =============================================================================
# EMBEDDED NEWLINE HANDLING
# =============================================================================

def preprocess_file_for_embedded_newlines(
    filepath: Path,
    encoding: str = "latin-1",
) -> Tuple[List[str], int]:
    """
    Preprocess a MAUDE file to handle embedded newlines in text fields.

    FDA MAUDE narrative text fields can contain embedded newlines, which cause
    records to be split across multiple physical lines. This function detects
    orphan lines (lines that don't start with a valid MDR_REPORT_KEY) and
    rejoins them with the previous record.

    STREAMING VERSION: Writes to a temp file to avoid loading entire file
    into memory. Returns an iterator over the preprocessed lines.

    Args:
        filepath: Path to the file.
        encoding: File encoding.

    Returns:
        Tuple of (iterable of rejoined lines, count of rejoined records).
    """
    import tempfile

    rejoin_count = 0
    temp_file = tempfile.NamedTemporaryFile(
        mode='w',
        encoding=encoding,
        delete=False,
        suffix='.preprocessed.txt'
    )
    temp_path = temp_file.name

    try:
        with open(filepath, "r", encoding=encoding, errors="replace") as f:
            current_line = None

            for line_num, line in enumerate(f):
                line = line.rstrip("\n\r")

                # First line is header
                if line_num == 0:
                    current_line = line
                    continue

                # Skip empty lines
                if not line.strip():
                    continue

                # Valid data lines have a numeric MDR_REPORT_KEY as the first field
                # MDR keys are exactly 8 digits; other numeric values (phone numbers,
                # zip codes) should be treated as orphan line continuations
                first_field = line.split('|', 1)[0]
                if first_field.isdigit() and len(first_field) == 8:
                    # This is a new record - write the current one and start fresh
                    if current_line is not None:
                        temp_file.write(current_line + "\n")
                    current_line = line
                else:
                    # This is an orphan line - append to current record with a space
                    current_line = current_line + " " + line.lstrip()
                    rejoin_count += 1

            # Don't forget the last record
            if current_line is not None:
                temp_file.write(current_line + "\n")

        temp_file.close()

        if rejoin_count > 0:
            logger.info(
                f"Preprocessed {filepath.name}: rejoined {rejoin_count} embedded newline fragments"
            )

        # Return an iterator that reads from the temp file
        # The caller should handle cleanup
        return PreprocessedFileIterator(temp_path, encoding), rejoin_count

    except Exception as e:
        logger.error(f"Error preprocessing {filepath} for embedded newlines: {e}")
        temp_file.close()
        try:
            Path(temp_path).unlink()
        except:
            pass
        return [], 0


class PreprocessedFileIterator:
    """Iterator that reads from a preprocessed temp file and cleans up when done."""

    def __init__(self, filepath: str, encoding: str = "latin-1"):
        self.filepath = filepath
        self.encoding = encoding
        self._file = None

    def __iter__(self):
        self._file = open(self.filepath, "r", encoding=self.encoding)
        return self

    def __next__(self):
        if self._file is None:
            raise StopIteration
        line = self._file.readline()
        if line:
            return line.rstrip("\n\r")
        else:
            self._cleanup()
            raise StopIteration

    def _cleanup(self):
        if self._file:
            self._file.close()
            self._file = None
        try:
            Path(self.filepath).unlink()
        except:
            pass

    def __del__(self):
        self._cleanup()


def count_physical_lines(
    filepath: Path,
    encoding: str = "latin-1",
) -> Tuple[int, int, int]:
    """
    Count physical lines in a MAUDE file for validation.

    This is critical for detecting quote-swallowing bugs where the CSV reader
    silently consumes records when encountering unmatched quotes.

    Args:
        filepath: Path to the file.
        encoding: File encoding.

    Returns:
        Tuple of (total_physical_lines, valid_data_lines, orphan_lines).
        - total_physical_lines: All lines including header
        - valid_data_lines: Lines starting with digit (proper records)
        - orphan_lines: Lines not starting with digit (embedded newline fragments)
    """
    total_lines = 0
    valid_data_lines = 0
    orphan_lines = 0

    try:
        with open(filepath, "r", encoding=encoding, errors="replace") as f:
            for i, line in enumerate(f):
                total_lines += 1
                if i == 0:
                    continue  # Skip header
                # Check if first field is a valid MDR_REPORT_KEY (exactly 8 digits)
                first_field = line.split('|', 1)[0]
                if first_field.isdigit() and len(first_field) == 8:
                    valid_data_lines += 1
                elif line.strip():
                    orphan_lines += 1
    except Exception as e:
        logger.error(f"Error counting physical lines in {filepath}: {e}")

    return total_lines, valid_data_lines, orphan_lines


# Legacy column definitions for backward compatibility
# These are the database column names (lowercase)
MASTER_COLUMNS = [
    "mdr_report_key", "event_key", "report_number", "report_source_code",
    "manufacturer_link_flag_old", "number_devices_in_event", "number_patients_in_event",
    "date_received", "adverse_event_flag", "product_problem_flag",
    "date_report", "date_of_event", "reprocessed_and_reused_flag",
    "reporter_occupation_code", "health_professional", "initial_report_to_fda",
    "date_facility_aware", "report_date", "report_to_fda", "date_report_to_fda",
    "event_location", "date_report_to_manufacturer",
    "manufacturer_contact_title", "manufacturer_contact_first_name",
    "manufacturer_contact_last_name", "manufacturer_contact_address_1",
    "manufacturer_contact_address_2", "manufacturer_contact_city",
    "manufacturer_contact_state", "manufacturer_contact_zip",
    "manufacturer_contact_zip_ext", "manufacturer_contact_country",
    "manufacturer_contact_postal", "manufacturer_contact_phone",
    "manufacturer_contact_extension", "manufacturer_contact_email",
    "manufacturer_g1_name", "manufacturer_g1_street_1", "manufacturer_g1_street_2",
    "manufacturer_g1_city", "manufacturer_g1_state", "manufacturer_g1_zip",
    "manufacturer_g1_zip_ext", "manufacturer_g1_country", "manufacturer_g1_postal",
    "date_manufacturer_received", "device_date_of_manufacture", "single_use_flag",
    "remedial_action", "previous_use_code", "removal_correction_number",
    "manufacturer_link_flag", "event_type", "distributor_name",
    "distributor_address_1", "distributor_address_2", "distributor_city",
    "distributor_state", "distributor_zip", "distributor_zip_ext",
    "report_to_manufacturer", "type_of_report", "manufacturer_name",
    "manufacturer_address_1", "manufacturer_address_2", "manufacturer_city",
    "manufacturer_state", "manufacturer_zip", "manufacturer_zip_ext",
    "manufacturer_country", "manufacturer_postal", "mfr_report_type",
    "source_type", "date_added", "date_changed", "product_code",
    "pma_pmn_number", "exemption_number", "summary_report_flag",
    "reporter_state_code", "reporter_country_code", "noe_summarized",
    "supplemental_dates_fda_received", "supplemental_dates_mfr_received",
    "baseline_report_number", "schema_version",
]

DEVICE_COLUMNS = [
    "mdr_report_key", "device_event_key", "implant_flag", "date_removed_flag",
    "device_sequence_number", "date_received", "brand_name", "generic_name",
    "manufacturer_d_name", "manufacturer_d_address_1", "manufacturer_d_address_2",
    "manufacturer_d_city", "manufacturer_d_state", "manufacturer_d_zip",
    "manufacturer_d_zip_ext", "manufacturer_d_country", "manufacturer_d_postal",
    "expiration_date_of_device", "model_number", "catalog_number", "lot_number",
    "other_id_number", "device_operator", "device_availability",
    "date_returned_to_manufacturer", "device_report_product_code",
    "device_age_text", "device_evaluated_by_manufacturer",
]

PATIENT_COLUMNS = [
    "mdr_report_key", "patient_sequence_number", "date_received",
    "sequence_number_treatment", "sequence_number_outcome",
    "patient_age", "patient_sex", "patient_weight",
    "patient_ethnicity", "patient_race",
]

TEXT_COLUMNS = [
    "mdr_report_key", "mdr_text_key", "text_type_code",
    "patient_sequence_number", "date_report", "text_content",
]

PROBLEM_COLUMNS = [
    "mdr_report_key", "device_problem_code",
]

# Map file type to database columns (lowercase)
FILE_COLUMNS = {
    "master": MASTER_COLUMNS,
    "device": DEVICE_COLUMNS,
    "patient": PATIENT_COLUMNS,
    "text": TEXT_COLUMNS,
    "problem": PROBLEM_COLUMNS,
}


# =============================================================================
# YEAR EXTRACTION FROM FILENAME
# =============================================================================

def extract_year_from_filename(filename: str) -> Optional[int]:
    """
    Extract year from FDA MAUDE filename.

    Patterns handled:
    - device2020.txt, device2025.txt
    - foidev2019.txt, foidevthru2023.txt
    - mdrfoi2024.txt, mdrfoithru2023.txt
    - patient2020.txt
    - foitext2020.txt
    - mdr84.txt through mdr97.txt (DEN legacy)

    Args:
        filename: Name of the file (case-insensitive).

    Returns:
        Extracted year or None if not found.
    """
    name = filename.lower()

    # Pattern 1: 4-digit year (2019, 2020, 2023, etc.)
    match = re.search(r'(19|20)\d{2}', name)
    if match:
        return int(match.group())

    # Pattern 2: 2-digit year for DEN files (84-97)
    if name.startswith('mdr') and not name.startswith('mdrfoi'):
        match = re.search(r'mdr(\d{2})\.', name)
        if match:
            year_2digit = int(match.group(1))
            if 84 <= year_2digit <= 97:
                return 1900 + year_2digit

    return None


def detect_encoding(filepath: Path, sample_size: int = 10000) -> str:
    """
    Detect file encoding by sampling the file content.

    Most FDA files use latin-1, but some older files may use different encodings.

    Args:
        filepath: Path to the file.
        sample_size: Number of bytes to sample for detection.

    Returns:
        Detected encoding (defaults to 'latin-1' if unsure).
    """
    try:
        with open(filepath, 'rb') as f:
            raw_data = f.read(sample_size)

        # Try chardet for automatic detection
        try:
            result = chardet.detect(raw_data)
            if result and result.get('encoding'):
                confidence = result.get('confidence', 0)
                encoding = result['encoding'].lower()

                # High confidence detection
                if confidence > 0.9:
                    # Map common encodings to their standard names
                    encoding_map = {
                        'iso-8859-1': 'latin-1',
                        'windows-1252': 'cp1252',
                        'ascii': 'ascii',
                    }
                    return encoding_map.get(encoding, encoding)
        except Exception:
            pass

        # Try to detect by examining content
        # Check for UTF-8 BOM
        if raw_data.startswith(b'\xef\xbb\xbf'):
            return 'utf-8-sig'

        # Check for UTF-16 BOM
        if raw_data.startswith(b'\xff\xfe') or raw_data.startswith(b'\xfe\xff'):
            return 'utf-16'

        # Default to latin-1 (handles most FDA files)
        return 'latin-1'

    except Exception as e:
        logger.warning(f"Error detecting encoding for {filepath}: {e}")
        return 'latin-1'


def get_schema_for_file(
    filepath: Path,
    file_type: str,
    column_count: Optional[int] = None,
) -> Tuple[List[str], str, str]:
    """
    Get appropriate schema columns, encoding, and notes for a file.

    Uses historical schema definitions when available to handle
    schema evolution across different years of FDA MAUDE data.

    Args:
        filepath: Path to the file.
        file_type: Type of file (master, device, patient, text, etc.).
        column_count: Detected column count from file header.

    Returns:
        Tuple of (columns, encoding, notes).
    """
    year = extract_year_from_filename(filepath.name)
    encoding = 'latin-1'  # Default
    notes = ""

    # Use historical schemas if available
    if HAS_HISTORICAL_SCHEMAS:
        if file_type == 'device':
            schema = get_device_schema(filepath.name, year, column_count)
            return schema.columns, schema.encoding, schema.notes
        elif file_type == 'master':
            schema = get_master_schema(filepath.name, year, column_count)
            return schema.columns, schema.encoding, schema.notes
        elif file_type == 'text':
            schema = get_text_schema(filepath.name, year, column_count)
            return schema.columns, schema.encoding, schema.notes

    # Fall back to standard schema detection
    columns = get_fda_columns(file_type, column_count)

    # Detect encoding for older files
    if year and year < 2000:
        encoding = detect_encoding(filepath)
        notes = f"Pre-2000 file, detected encoding: {encoding}"

    return columns, encoding, notes


@dataclass
class SchemaInfo:
    """Information about detected file schema."""
    columns: List[str]  # FDA column names (uppercase)
    column_count: int
    has_header: bool
    file_type: str
    is_valid: bool = True
    validation_message: str = ""
    encoding: str = "latin-1"  # Detected file encoding
    year: Optional[int] = None  # Extracted year from filename
    schema_notes: str = ""  # Notes about the schema version


@dataclass
class ParseResult:
    """Result of parsing a file."""
    filename: str
    file_type: str
    total_rows: int = 0
    parsed_rows: int = 0
    error_rows: int = 0
    errors: List[Tuple[int, str]] = field(default_factory=list)
    schema_info: Optional[SchemaInfo] = None
    # Column mismatch tracking for data quality auditing
    column_mismatch_count: int = 0
    column_mismatch_samples: List[Tuple[int, int, int]] = field(default_factory=list)  # (line_num, expected, actual)


class MAUDEParser:
    """Parser for FDA MAUDE files with dynamic schema detection.

    Supports:
    - Pipe-delimited files (most MAUDE files)
    - CSV files (ASR reports)
    - Various historical formats (DEN legacy 1984-1997)
    """

    # File types that use CSV format instead of pipe-delimited
    CSV_FILE_TYPES = {"asr", "asr_ppc"}

    def __init__(self, encoding: str = "latin-1"):
        """
        Initialize the parser.

        Args:
            encoding: File encoding (latin-1 handles most MAUDE files).
        """
        self.encoding = encoding

    def detect_file_type(self, filepath: Path) -> Optional[str]:
        """
        Detect the type of MAUDE file based on filename.

        Args:
            filepath: Path to the file.

        Returns:
            File type string or None if unknown.
        """
        filename = filepath.name.lower()

        # Handle historical files (thru{year}) and current files
        if "mdrfoi" in filename and "problem" not in filename:
            return "master"
        elif ("foidev" in filename or filename.startswith("device")) and "problem" not in filename:
            return "device"
        elif filename.startswith("patient") and "problem" not in filename:
            return "patient"
        elif "foitext" in filename:
            return "text"
        elif filename == "foidevproblem.txt" or "foidevproblem" in filename:
            return "problem"
        elif filename == "deviceproblemcodes.txt":
            return "problem_lookup"
        elif "patientproblemcode" in filename:
            return "patient_problem"
        elif "patientproblemdata" in filename:
            return "patient_problem_data"
        elif filename.startswith("asr_ppc") or filename == "asr_ppc.txt":
            return "asr_ppc"
        elif filename.startswith("asr"):
            return "asr"
        elif filename == "disclaim.txt":
            return "disclaimer"
        # DEN legacy files: mdr84.txt through mdr97.txt
        elif filename.startswith("mdr") and len(filename) <= 9:
            # Check if it's a 2-digit year format (mdr84.txt - mdr97.txt)
            year_part = filename[3:5]
            if year_part.isdigit():
                year = int(year_part)
                if 84 <= year <= 97:
                    return "den"

        return None

    def detect_schema_from_header(self, filepath: Path, file_type: Optional[str] = None) -> SchemaInfo:
        """
        Read the first line of a file and detect its column structure.

        Enhanced with:
        - Year-based schema detection for historical files
        - Automatic encoding detection for older files
        - Support for schema variations across FDA MAUDE file types

        Args:
            filepath: Path to the file.
            file_type: Known file type (auto-detected if None).

        Returns:
            SchemaInfo with detected columns and metadata.
        """
        if file_type is None:
            file_type = self.detect_file_type(filepath)

        if file_type is None:
            raise ValueError(f"Could not detect file type for: {filepath}")

        # Extract year from filename for schema selection
        year = extract_year_from_filename(filepath.name)

        # Detect encoding (especially important for older files)
        detected_encoding = detect_encoding(filepath) if year and year < 2005 else self.encoding

        # Check if this is a headerless file
        if is_headerless_file(file_type):
            fda_columns = get_fda_columns(file_type)
            return SchemaInfo(
                columns=fda_columns,
                column_count=len(fda_columns),
                has_header=False,
                file_type=file_type,
                is_valid=True,
                validation_message="Using predefined columns (headerless file)",
                encoding=detected_encoding,
                year=year,
            )

        # Read the first line to get header
        try:
            with open(filepath, "r", encoding=detected_encoding, errors="replace") as f:
                first_line = f.readline().strip()

            # Split by pipe delimiter
            header_parts = first_line.split("|")
            detected_count = len(header_parts)

            # Check if this looks like a header row
            if self._is_header_row(header_parts, file_type):
                # Normalize column names (uppercase, strip)
                columns = [col.strip().upper() for col in header_parts]
                has_header = True
            else:
                # No header - use predefined columns based on detected column count
                # Try historical schema detection first
                columns, schema_encoding, schema_notes = get_schema_for_file(
                    filepath, file_type, detected_count
                )
                has_header = False
                if schema_encoding != detected_encoding:
                    detected_encoding = schema_encoding
                logger.info(
                    f"File {filepath.name}: Using schema for year {year or 'unknown'}, "
                    f"{detected_count} columns detected. {schema_notes}"
                )

            # Validate schema
            is_valid, message = validate_schema(file_type, columns)

            return SchemaInfo(
                columns=columns,
                column_count=len(columns),
                has_header=has_header,
                file_type=file_type,
                is_valid=is_valid,
                validation_message=message,
                encoding=detected_encoding,
                year=year,
                schema_notes=f"Year: {year}, Encoding: {detected_encoding}" if year else "",
            )

        except Exception as e:
            logger.error(f"Error detecting schema for {filepath}: {e}")
            # Fall back to predefined columns
            fda_columns = get_fda_columns(file_type)
            return SchemaInfo(
                columns=fda_columns,
                column_count=len(fda_columns),
                has_header=True,  # Assume header exists
                file_type=file_type,
                is_valid=False,
                validation_message=f"Error detecting schema: {e}",
                encoding=detected_encoding,
                year=year,
            )

    def _is_header_row(self, parts: List[str], file_type: str) -> bool:
        """
        Determine if a row looks like a header row.

        Args:
            parts: List of values from first row.
            file_type: Type of file.

        Returns:
            True if this appears to be a header row.
        """
        if not parts:
            return False

        # Get expected first column name
        expected_columns = get_fda_columns(file_type)
        if not expected_columns:
            return False

        first_expected = expected_columns[0].upper()
        first_actual = parts[0].strip().upper()

        # Check if first column matches expected
        if first_actual == first_expected:
            return True

        # Check for common header patterns
        # Headers typically:
        # 1. Are all uppercase or mixed case text
        # 2. Don't start with numbers (MDR_REPORT_KEY vs "12345678")
        # 3. Contain underscores or text descriptions

        # If first value looks like an MDR key (numeric), it's data not header
        if parts[0].strip().isdigit():
            return False

        # If first value contains "KEY", "REPORT", etc., likely header
        header_indicators = ["KEY", "REPORT", "DATE", "NAME", "CODE", "NUMBER", "FLAG"]
        for indicator in header_indicators:
            if indicator in first_actual:
                return True

        return False

    def parse_file_dynamic(
        self,
        filepath: Path,
        schema: Optional[SchemaInfo] = None,
        file_type: Optional[str] = None,
        limit: Optional[int] = None,
        filter_product_codes: Optional[List[str]] = None,
        map_to_db_columns: bool = True,
    ) -> Generator[Dict[str, Any], None, ParseResult]:
        """
        Parse a MAUDE file using dynamic schema detection.

        Args:
            filepath: Path to the file.
            schema: Pre-detected schema (detected if None).
            file_type: Type of file (auto-detected if None).
            limit: Maximum number of records to return.
            filter_product_codes: Only return records matching these product codes.
            map_to_db_columns: If True, map FDA columns to database columns.

        Yields:
            Dictionary for each parsed record.

        Returns:
            ParseResult with statistics.
        """
        # Auto-detect file type if needed
        if file_type is None:
            file_type = self.detect_file_type(filepath)

        if file_type is None:
            raise ValueError(f"Could not detect file type for: {filepath}")

        # Detect schema if not provided
        if schema is None:
            schema = self.detect_schema_from_header(filepath, file_type)

        result = ParseResult(
            filename=filepath.name,
            file_type=file_type,
            schema_info=schema,
        )

        logger.info(
            f"Parsing {file_type} file: {filepath.name} "
            f"({schema.column_count} columns, header={schema.has_header})"
        )

        if not schema.is_valid:
            logger.warning(f"Schema validation: {schema.validation_message}")

        # Get column mapping for this file type
        column_mapping = COLUMN_MAPPINGS.get(file_type, {})

        # Determine which column to filter on
        filter_column = None
        if filter_product_codes:
            if file_type == "master":
                filter_column = "PRODUCT_CODE"
            elif file_type == "device":
                filter_column = "DEVICE_REPORT_PRODUCT_CODE"

        # File types known to have embedded newlines in text fields
        EMBEDDED_NEWLINE_FILE_TYPES = {"master", "text", "patient", "device"}

        try:
            # Use detected encoding from schema (important for older files)
            file_encoding = schema.encoding if schema else self.encoding

            # Preprocess files that may have embedded newlines
            # This rejoins split records before CSV parsing
            if file_type in EMBEDDED_NEWLINE_FILE_TYPES:
                preprocessed_lines, rejoin_count = preprocess_file_for_embedded_newlines(
                    filepath, encoding=file_encoding
                )
                if rejoin_count > 0:
                    logger.info(f"Rejoined {rejoin_count} split records in {filepath.name}")

                # IMPORTANT: Use QUOTE_NONE to disable quote handling
                # FDA MAUDE data contains literal quote characters (e.g., O"REILLY)
                # that are NOT field delimiters. Using quotechar='"' causes the CSV
                # reader to swallow millions of records when an unmatched quote appears.
                reader = csv.reader(preprocessed_lines, delimiter="|", quoting=csv.QUOTE_NONE)
            else:
                # For other file types, read directly from file
                # We need to keep file open for the generator, so use a different approach
                preprocessed_lines = None
                file_handle = open(filepath, "r", encoding=file_encoding, errors="replace")
                reader = csv.reader(file_handle, delimiter="|", quoting=csv.QUOTE_NONE)

            try:
                for line_num, row in enumerate(reader, 1):
                    result.total_rows += 1

                    # Skip header row if present
                    if line_num == 1 and schema.has_header:
                        continue

                    try:
                        # Parse row using detected columns
                        record = self._parse_row_dynamic(
                            row, schema.columns, file_type,
                            line_num=line_num, result=result
                        )

                        # Apply product code filter
                        if filter_product_codes and filter_column:
                            product_code = record.get(filter_column, "")
                            if product_code not in filter_product_codes:
                                continue

                        # Map to database column names if requested
                        if map_to_db_columns:
                            record = map_record_columns(record, file_type, to_db=True)

                        result.parsed_rows += 1
                        yield record

                        # Check limit
                        if limit and result.parsed_rows >= limit:
                            break

                    except Exception as e:
                        result.error_rows += 1
                        if len(result.errors) < 100:
                            result.errors.append((line_num, str(e)))
            finally:
                # Close file handle if we opened one
                if preprocessed_lines is None:
                    file_handle.close()

        except Exception as e:
            logger.error(f"Error reading file {filepath}: {e}")
            raise

        # Log parsing results including column mismatch stats
        log_msg = f"Parsed {filepath.name}: {result.parsed_rows} records, {result.error_rows} errors"
        if result.column_mismatch_count > 0:
            log_msg += f", {result.column_mismatch_count} column mismatches"
            logger.warning(log_msg)
        else:
            logger.info(log_msg)

        return result

    def _parse_row_dynamic(
        self, row: List[str], columns: List[str], file_type: str,
        line_num: int = 0, result: Optional[ParseResult] = None
    ) -> Dict[str, Any]:
        """
        Parse a single row into a dictionary using dynamic columns.

        Args:
            row: List of field values.
            columns: List of column names (FDA format, uppercase).
            file_type: Type of file being parsed.
            line_num: Line number in the source file (for logging).
            result: Optional ParseResult to track column mismatches.

        Returns:
            Dictionary with FDA column names as keys.
        """
        record = {}
        expected_count = len(columns)
        actual_count = len(row)

        # Track column mismatches for data quality auditing
        if actual_count != expected_count and result is not None:
            result.column_mismatch_count += 1
            # Store samples (up to 100) for debugging
            if len(result.column_mismatch_samples) < 100:
                result.column_mismatch_samples.append((line_num, expected_count, actual_count))
            # Log warning for first few mismatches
            if result.column_mismatch_count <= 5:
                logger.warning(
                    f"Column count mismatch in {file_type} file at line {line_num}: "
                    f"expected {expected_count}, got {actual_count}"
                )
            elif result.column_mismatch_count == 6:
                logger.warning(
                    f"Additional column mismatches in {file_type} file (suppressing further warnings)"
                )

        # Handle rows with fewer or more columns than expected
        for i, col_name in enumerate(columns):
            if i < len(row):
                value = row[i].strip() if row[i] else None
                # Convert empty strings to None
                record[col_name] = value if value else None
            else:
                record[col_name] = None

        return record

    def parse_csv_file(
        self,
        filepath: Path,
        file_type: str,
        limit: Optional[int] = None,
        map_to_db_columns: bool = True,
    ) -> Generator[Dict[str, Any], None, ParseResult]:
        """
        Parse a CSV format MAUDE file (used for ASR reports).

        Args:
            filepath: Path to the file.
            file_type: Type of file (asr, asr_ppc).
            limit: Maximum number of records to return.
            map_to_db_columns: If True, map FDA columns to database columns.

        Yields:
            Dictionary for each parsed record.

        Returns:
            ParseResult with statistics.
        """
        result = ParseResult(
            filename=filepath.name,
            file_type=file_type,
        )

        # Get columns for this file type
        columns = get_fda_columns(file_type)

        logger.info(f"Parsing CSV file: {filepath.name} (type: {file_type})")

        try:
            with open(filepath, "r", encoding=self.encoding, errors="replace") as f:
                # Use comma delimiter for CSV files
                # IMPORTANT: Use QUOTE_NONE to disable quote handling
                # FDA MAUDE data contains literal quote characters that are NOT field delimiters.
                # Using quotechar='"' causes the CSV reader to swallow records on unmatched quotes.
                reader = csv.reader(f, delimiter=",", quoting=csv.QUOTE_NONE)

                # First row is typically header
                header_row = next(reader, None)
                if header_row:
                    # Use header from file if present, otherwise use predefined
                    detected_columns = [col.strip().upper() for col in header_row]
                    if len(detected_columns) == len(columns):
                        columns = detected_columns
                    result.total_rows += 1

                for line_num, row in enumerate(reader, 2):
                    result.total_rows += 1

                    try:
                        record = self._parse_row_dynamic(row, columns, file_type)

                        # Map to database column names if requested
                        if map_to_db_columns:
                            record = map_record_columns(record, file_type, to_db=True)

                        result.parsed_rows += 1
                        yield record

                        if limit and result.parsed_rows >= limit:
                            break

                    except Exception as e:
                        result.error_rows += 1
                        if len(result.errors) < 100:
                            result.errors.append((line_num, str(e)))

        except Exception as e:
            logger.error(f"Error reading CSV file {filepath}: {e}")
            raise

        logger.info(
            f"Parsed CSV {filepath.name}: {result.parsed_rows} records, "
            f"{result.error_rows} errors"
        )

        return result

    def parse_den_file(
        self,
        filepath: Path,
        limit: Optional[int] = None,
        map_to_db_columns: bool = True,
    ) -> Generator[Dict[str, Any], None, ParseResult]:
        """
        Parse a DEN legacy file (1984-1997 format).

        DEN files may have variable formats depending on the year.
        This method attempts to handle the variations.

        Args:
            filepath: Path to the file.
            limit: Maximum number of records to return.
            map_to_db_columns: If True, map FDA columns to database columns.

        Yields:
            Dictionary for each parsed record.

        Returns:
            ParseResult with statistics.
        """
        result = ParseResult(
            filename=filepath.name,
            file_type="den",
        )

        # Extract year from filename (mdr84.txt -> 84 -> 1984)
        filename = filepath.name.lower()
        year_part = filename[3:5]
        report_year = int(f"19{year_part}") if year_part.isdigit() else None

        logger.info(f"Parsing DEN legacy file: {filepath.name} (year: {report_year})")

        # Get DEN columns - may need adjustment based on actual file format
        columns = DEN_COLUMNS_FDA.copy()

        try:
            # Read binary and remove NUL characters (common in legacy FDA files)
            with open(filepath, "rb") as f:
                content = f.read().replace(b'\x00', b'')
                content = content.decode(self.encoding, errors="replace")

            # Process the cleaned content
            lines = content.splitlines()
            if not lines:
                return result

            # Try to detect delimiter from first line
            first_line = lines[0] if lines else ""

            if "|" in first_line:
                delimiter = "|"
            elif "," in first_line and first_line.count(",") > 5:
                delimiter = ","
            else:
                delimiter = "|"

            # IMPORTANT: Use QUOTE_NONE to disable quote handling
            # FDA MAUDE data contains literal quote characters that are NOT field delimiters.
            # Using quotechar='"' causes the CSV reader to swallow records on unmatched quotes.
            reader = csv.reader(lines, delimiter=delimiter, quoting=csv.QUOTE_NONE)

            for line_num, row in enumerate(reader, 1):
                    result.total_rows += 1

                    # Skip header if present
                    if line_num == 1:
                        first_val = row[0].strip().upper() if row else ""
                        if "KEY" in first_val or "REPORT" in first_val or "MDR" in first_val:
                            continue

                    try:
                        record = {}
                        # Handle variable column counts in legacy files
                        for i, col_name in enumerate(columns):
                            if i < len(row):
                                value = row[i].strip() if row[i] else None
                                record[col_name] = value if value else None
                            else:
                                record[col_name] = None

                        # Add derived year field
                        record["REPORT_YEAR"] = report_year

                        # Map to database column names if requested
                        if map_to_db_columns:
                            record = map_record_columns(record, "den", to_db=True)

                        result.parsed_rows += 1
                        yield record

                        if limit and result.parsed_rows >= limit:
                            break

                    except Exception as e:
                        result.error_rows += 1
                        if len(result.errors) < 100:
                            result.errors.append((line_num, str(e)))

        except Exception as e:
            logger.error(f"Error reading DEN file {filepath}: {e}")
            raise

        logger.info(
            f"Parsed DEN {filepath.name}: {result.parsed_rows} records, "
            f"{result.error_rows} errors"
        )

        return result

    # Legacy method for backward compatibility
    def parse_file(
        self,
        filepath: Path,
        file_type: Optional[str] = None,
        limit: Optional[int] = None,
        filter_product_codes: Optional[List[str]] = None,
    ) -> Generator[Dict[str, Any], None, ParseResult]:
        """
        Parse a MAUDE file and yield records (legacy interface).

        This method uses the new dynamic parsing but returns results
        in the legacy format (database column names).

        Args:
            filepath: Path to the file.
            file_type: Type of file (auto-detected if None).
            limit: Maximum number of records to return.
            filter_product_codes: Only return records matching these product codes.

        Yields:
            Dictionary for each parsed record.

        Returns:
            ParseResult with statistics.
        """
        # Use the new dynamic parser with column mapping
        return self.parse_file_dynamic(
            filepath=filepath,
            file_type=file_type,
            limit=limit,
            filter_product_codes=filter_product_codes,
            map_to_db_columns=True,
        )

    def _parse_row(
        self, row: List[str], columns: List[str], file_type: str
    ) -> Dict[str, Any]:
        """
        Parse a single row into a dictionary (legacy method).

        Args:
            row: List of field values.
            columns: List of column names.
            file_type: Type of file being parsed.

        Returns:
            Dictionary with column names as keys.
        """
        record = {}

        for i, col_name in enumerate(columns):
            if i < len(row):
                value = row[i].strip() if row[i] else None
                record[col_name] = value if value else None
            else:
                record[col_name] = None

        return record

    def count_records(self, filepath: Path) -> int:
        """
        Count records in a file without full parsing.

        Args:
            filepath: Path to the file.

        Returns:
            Number of records (lines minus header).
        """
        count = 0
        has_header = True

        # Detect if file has header
        file_type = self.detect_file_type(filepath)
        if file_type:
            schema = self.detect_schema_from_header(filepath, file_type)
            has_header = schema.has_header

        try:
            with open(filepath, "r", encoding=self.encoding, errors="replace") as f:
                for _ in f:
                    count += 1
        except Exception as e:
            logger.error(f"Error counting records in {filepath}: {e}")

        # Subtract header if present
        if has_header:
            count = max(0, count - 1)

        return count

    def get_sample(
        self,
        filepath: Path,
        n: int = 10,
        file_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get a sample of records from a file.

        Args:
            filepath: Path to the file.
            n: Number of records to sample.
            file_type: Type of file.

        Returns:
            List of record dictionaries.
        """
        samples = []
        for record in self.parse_file(filepath, file_type, limit=n):
            samples.append(record)
        return samples

    def analyze_file_structure(self, filepath: Path) -> Dict[str, Any]:
        """
        Analyze the structure of a MAUDE file.

        Useful for debugging column mismatches.

        Args:
            filepath: Path to the file.

        Returns:
            Dictionary with analysis results.
        """
        file_type = self.detect_file_type(filepath)
        schema = self.detect_schema_from_header(filepath, file_type)

        # Read a few data rows to analyze
        sample_rows = []
        try:
            with open(filepath, "r", encoding=self.encoding, errors="replace") as f:
                # IMPORTANT: Use QUOTE_NONE to disable quote handling
                # FDA MAUDE data contains literal quote characters that are NOT field delimiters.
                # Using quotechar='"' causes the CSV reader to swallow records on unmatched quotes.
                reader = csv.reader(f, delimiter="|", quoting=csv.QUOTE_NONE)
                for i, row in enumerate(reader):
                    if i == 0 and schema.has_header:
                        continue
                    if i > 5:  # Sample 5 data rows
                        break
                    sample_rows.append(row)
        except Exception as e:
            logger.error(f"Error analyzing {filepath}: {e}")

        # Analyze column consistency
        column_counts = [len(row) for row in sample_rows]
        expected_count = get_expected_column_count(file_type)

        return {
            "filepath": str(filepath),
            "file_type": file_type,
            "schema": {
                "columns": schema.columns[:10],  # First 10 for brevity
                "total_columns": schema.column_count,
                "has_header": schema.has_header,
                "is_valid": schema.is_valid,
                "validation_message": schema.validation_message,
            },
            "expected_columns": expected_count,
            "sample_column_counts": column_counts,
            "column_mismatch": any(c != expected_count for c in column_counts),
        }


def get_product_code_filter_indices(
    columns: List[str], file_type: str
) -> Optional[int]:
    """
    Get the column index for product code filtering.

    Args:
        columns: List of column names.
        file_type: Type of file.

    Returns:
        Column index or None.
    """
    # Try FDA column names first
    if file_type == "master":
        for name in ["PRODUCT_CODE", "product_code"]:
            try:
                return columns.index(name)
            except ValueError:
                continue
    elif file_type == "device":
        for name in ["DEVICE_REPORT_PRODUCT_CODE", "device_report_product_code"]:
            try:
                return columns.index(name)
            except ValueError:
                continue
    return None


def parse_all_files(
    data_dir: Path,
    file_type: str,
    filter_product_codes: Optional[List[str]] = None,
) -> Generator[Dict[str, Any], None, None]:
    """
    Parse all files of a given type in a directory.

    Args:
        data_dir: Directory containing MAUDE files.
        file_type: Type of files to parse.
        filter_product_codes: Optional product codes to filter by.

    Yields:
        Record dictionaries from all files.
    """
    parser = MAUDEParser()

    # Find all files of this type
    patterns = {
        "master": ["mdrfoi*.txt", "mdrfoithru*.txt"],
        "device": ["foidev*.txt", "foidevthru*.txt", "device*.txt"],
        "patient": ["patient*.txt", "patientthru*.txt"],
        "text": ["foitext*.txt", "foitextthru*.txt"],
        "problem": ["foidevproblem*.txt"],
        "problem_lookup": ["deviceproblemcodes.txt"],
        "patient_problem": ["patientproblemcode*.txt"],
        "patient_problem_data": ["patientproblemdata*.txt"],
        "asr": ["asr_*.txt"],
        "asr_ppc": ["asr_ppc*.txt"],
        "den": ["mdr8*.txt", "mdr9*.txt"],
        "disclaimer": ["disclaim*.txt"],
    }

    file_patterns = patterns.get(file_type, [])
    if not file_patterns:
        raise ValueError(f"Unknown file type: {file_type}")

    # Gather all matching files
    files = []
    for pattern in file_patterns:
        files.extend(data_dir.glob(pattern))
    files = sorted(set(files))

    # Exclude problem files from device glob
    if file_type == "device":
        files = [f for f in files if "problem" not in f.name.lower()]

    # For ASR, exclude the asr_ppc files when parsing regular ASR
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

    logger.info(f"Found {len(files)} {file_type} files to parse")

    for filepath in files:
        try:
            # Use appropriate parser based on file type
            if file_type in parser.CSV_FILE_TYPES:
                yield from parser.parse_csv_file(
                    filepath,
                    file_type=file_type,
                )
            elif file_type == "den":
                yield from parser.parse_den_file(filepath)
            else:
                yield from parser.parse_file(
                    filepath,
                    file_type=file_type,
                    filter_product_codes=filter_product_codes,
                )
        except Exception as e:
            logger.error(f"Error parsing {filepath}: {e}")
            continue


if __name__ == "__main__":
    # Test parsing and schema detection
    import argparse

    arg_parser = argparse.ArgumentParser(description="Parse MAUDE files")
    arg_parser.add_argument("file", type=Path, help="File to parse")
    arg_parser.add_argument("--type", help="File type (auto-detected if not specified)")
    arg_parser.add_argument("--sample", type=int, default=5, help="Number of sample records")
    arg_parser.add_argument("--count", action="store_true", help="Just count records")
    arg_parser.add_argument("--analyze", action="store_true", help="Analyze file structure")

    args = arg_parser.parse_args()

    parser = MAUDEParser()

    if args.analyze:
        analysis = parser.analyze_file_structure(args.file)
        print("\nFile Structure Analysis:")
        print(f"  File: {analysis['filepath']}")
        print(f"  Type: {analysis['file_type']}")
        print(f"  Has Header: {analysis['schema']['has_header']}")
        print(f"  Detected Columns: {analysis['schema']['total_columns']}")
        print(f"  Expected Columns: {analysis['expected_columns']}")
        print(f"  Valid: {analysis['schema']['is_valid']}")
        print(f"  Message: {analysis['schema']['validation_message']}")
        print(f"  Sample Column Counts: {analysis['sample_column_counts']}")
        if analysis['column_mismatch']:
            print("  WARNING: Column count mismatch detected!")
    elif args.count:
        count = parser.count_records(args.file)
        print(f"Total records: {count:,}")
    else:
        # Show schema info
        file_type = args.type or parser.detect_file_type(args.file)
        schema = parser.detect_schema_from_header(args.file, file_type)
        print(f"\nSchema Info:")
        print(f"  File Type: {schema.file_type}")
        print(f"  Columns: {schema.column_count}")
        print(f"  Has Header: {schema.has_header}")
        print(f"  Valid: {schema.is_valid}")

        print(f"\nFirst {args.sample} records:")
        samples = parser.get_sample(args.file, n=args.sample, file_type=args.type)
        for i, record in enumerate(samples, 1):
            print(f"\n--- Record {i} ---")
            for key, value in list(record.items())[:15]:  # Show first 15 fields
                if value:
                    display_val = value[:80] + "..." if isinstance(value, str) and len(value) > 80 else value
                    print(f"  {key}: {display_val}")
