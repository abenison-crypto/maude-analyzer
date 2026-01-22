"""Parse FDA MAUDE data files."""

import csv
import re
from pathlib import Path
from typing import Generator, Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import get_logger

logger = get_logger("parser")

# Increase CSV field size limit for large narrative text fields
csv.field_size_limit(sys.maxsize)


# Column definitions for each file type
# These match the FDA MAUDE file specifications

MASTER_COLUMNS = [
    "mdr_report_key",
    "event_key",
    "report_number",
    "report_source_code",
    "manufacturer_link_flag",
    "number_devices_in_event",
    "number_patients_in_event",
    "date_received",
    "date_report",
    "date_of_event",
    "reprocessed_flag",
    "reporter_occupation_code",
    "health_professional",
    "initial_report_to_fda",
    "distributor_name",
    "distributor_address_1",
    "distributor_address_2",
    "distributor_city",
    "distributor_state",
    "distributor_zip",
    "distributor_zip_ext",
    "date_facility_aware",
    "report_date",
    "report_to_fda",
    "date_report_to_fda",
    "event_location",
    "report_to_manufacturer",
    "date_report_to_manufacturer",
    "date_manufacturer_received",
    "type_of_report",
    "product_problem_flag",
    "adverse_event_flag",
    "single_use_flag",
    "remedial_action",
    "removal_correction_number",
    "event_type",
    "manufacturer_contact_name",
    "manufacturer_contact_address_1",
    "manufacturer_contact_address_2",
    "manufacturer_contact_city",
    "manufacturer_contact_state",
    "manufacturer_contact_zip",
    "manufacturer_contact_zip_ext",
    "manufacturer_contact_country",
    "manufacturer_contact_postal",
    "manufacturer_contact_phone",
    "manufacturer_contact_extension",
    "manufacturer_contact_email",
    "manufacturer_name",
    "manufacturer_address_1",
    "manufacturer_address_2",
    "manufacturer_city",
    "manufacturer_state",
    "manufacturer_zip",
    "manufacturer_zip_ext",
    "manufacturer_country",
    "manufacturer_postal",
    "product_code",
    "pma_pmn_number",
    "exemption_number",
    "summary_report_flag",
]

DEVICE_COLUMNS = [
    "mdr_report_key",
    "device_sequence_number",
    "date_received",
    "brand_name",
    "generic_name",
    "manufacturer_d_name",
    "manufacturer_d_address_1",
    "manufacturer_d_address_2",
    "manufacturer_d_city",
    "manufacturer_d_state",
    "manufacturer_d_zip",
    "manufacturer_d_zip_ext",
    "manufacturer_d_country",
    "manufacturer_d_postal",
    "device_report_product_code",
    "model_number",
    "catalog_number",
    "lot_number",
    "other_id_number",
    "device_operator",
    "device_availability",
    "device_evaluated_by_manufacturer",
    "date_returned_to_manufacturer",
    "device_age_text",
    "combination_product_flag",
    "implant_flag",
    "date_removed_flag",
    "expiration_date_of_device",
]

PATIENT_COLUMNS = [
    "mdr_report_key",
    "patient_sequence_number",
    "date_received",
    "sequence_number_treatment",
    "sequence_number_outcome",
]

TEXT_COLUMNS = [
    "mdr_report_key",
    "text_type_code",
    "patient_sequence_number",
    "date_received",
    "text_content",
]

PROBLEM_COLUMNS = [
    "mdr_report_key",
    "device_problem_code",
]

# Map file type to columns
FILE_COLUMNS = {
    "master": MASTER_COLUMNS,
    "device": DEVICE_COLUMNS,
    "patient": PATIENT_COLUMNS,
    "text": TEXT_COLUMNS,
    "problem": PROBLEM_COLUMNS,
}


@dataclass
class ParseResult:
    """Result of parsing a file."""

    filename: str
    file_type: str
    total_rows: int = 0
    parsed_rows: int = 0
    error_rows: int = 0
    errors: List[Tuple[int, str]] = field(default_factory=list)


class MAUDEParser:
    """Parser for FDA MAUDE pipe-delimited files."""

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

        if filename.startswith("mdrfoi") and "thru" not in filename:
            return "master"
        elif filename.startswith("foidev") and "problem" not in filename:
            return "device"
        elif filename.startswith("patient"):
            return "patient"
        elif filename.startswith("foitext"):
            return "text"
        elif "problem" in filename:
            return "problem"

        return None

    def parse_file(
        self,
        filepath: Path,
        file_type: Optional[str] = None,
        limit: Optional[int] = None,
        filter_product_codes: Optional[List[str]] = None,
    ) -> Generator[Dict[str, Any], None, ParseResult]:
        """
        Parse a MAUDE file and yield records.

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
        if file_type is None:
            file_type = self.detect_file_type(filepath)

        if file_type is None:
            raise ValueError(f"Could not detect file type for: {filepath}")

        columns = FILE_COLUMNS.get(file_type)
        if columns is None:
            raise ValueError(f"Unknown file type: {file_type}")

        result = ParseResult(
            filename=filepath.name,
            file_type=file_type,
        )

        logger.info(f"Parsing {file_type} file: {filepath.name}")

        # Determine which column to filter on
        filter_column = None
        if filter_product_codes:
            if file_type == "master":
                filter_column = "product_code"
            elif file_type == "device":
                filter_column = "device_report_product_code"

        try:
            with open(filepath, "r", encoding=self.encoding, errors="replace") as f:
                # Use csv reader with pipe delimiter
                reader = csv.reader(f, delimiter="|", quotechar='"')

                for line_num, row in enumerate(reader, 1):
                    result.total_rows += 1

                    # Skip header row if present
                    if line_num == 1 and row and row[0].upper() == columns[0].upper():
                        continue

                    try:
                        record = self._parse_row(row, columns, file_type)

                        # Apply product code filter
                        if filter_product_codes and filter_column:
                            product_code = record.get(filter_column, "")
                            if product_code not in filter_product_codes:
                                continue

                        result.parsed_rows += 1
                        yield record

                        # Check limit
                        if limit and result.parsed_rows >= limit:
                            break

                    except Exception as e:
                        result.error_rows += 1
                        if len(result.errors) < 100:  # Limit stored errors
                            result.errors.append((line_num, str(e)))

        except Exception as e:
            logger.error(f"Error reading file {filepath}: {e}")
            raise

        logger.info(
            f"Parsed {filepath.name}: {result.parsed_rows} records, "
            f"{result.error_rows} errors"
        )

        return result

    def _parse_row(
        self, row: List[str], columns: List[str], file_type: str
    ) -> Dict[str, Any]:
        """
        Parse a single row into a dictionary.

        Args:
            row: List of field values.
            columns: List of column names.
            file_type: Type of file being parsed.

        Returns:
            Dictionary with column names as keys.
        """
        record = {}

        # Handle rows with fewer or more columns than expected
        for i, col_name in enumerate(columns):
            if i < len(row):
                value = row[i].strip() if row[i] else None
                # Convert empty strings to None
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
        try:
            with open(filepath, "r", encoding=self.encoding, errors="replace") as f:
                for _ in f:
                    count += 1
        except Exception as e:
            logger.error(f"Error counting records in {filepath}: {e}")

        # Subtract header if present
        return max(0, count - 1)

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
    if file_type == "master":
        try:
            return columns.index("product_code")
        except ValueError:
            return None
    elif file_type == "device":
        try:
            return columns.index("device_report_product_code")
        except ValueError:
            return None
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
        "master": "mdrfoi*.txt",
        "device": "foidev*.txt",
        "patient": "patient*.txt",
        "text": "foitext*.txt",
        "problem": "*problem*.txt",
    }

    pattern = patterns.get(file_type)
    if not pattern:
        raise ValueError(f"Unknown file type: {file_type}")

    files = sorted(data_dir.glob(pattern))

    # Exclude problem files from device glob
    if file_type == "device":
        files = [f for f in files if "problem" not in f.name.lower()]

    logger.info(f"Found {len(files)} {file_type} files to parse")

    for filepath in files:
        try:
            yield from parser.parse_file(
                filepath,
                file_type=file_type,
                filter_product_codes=filter_product_codes,
            )
        except Exception as e:
            logger.error(f"Error parsing {filepath}: {e}")
            continue


if __name__ == "__main__":
    # Test parsing
    import argparse

    arg_parser = argparse.ArgumentParser(description="Parse MAUDE files")
    arg_parser.add_argument("file", type=Path, help="File to parse")
    arg_parser.add_argument("--type", help="File type (auto-detected if not specified)")
    arg_parser.add_argument("--sample", type=int, default=5, help="Number of sample records")
    arg_parser.add_argument("--count", action="store_true", help="Just count records")

    args = arg_parser.parse_args()

    parser = MAUDEParser()

    if args.count:
        count = parser.count_records(args.file)
        print(f"Total records: {count:,}")
    else:
        samples = parser.get_sample(args.file, n=args.sample, file_type=args.type)
        for i, record in enumerate(samples, 1):
            print(f"\n--- Record {i} ---")
            for key, value in record.items():
                if value:
                    print(f"  {key}: {value[:100] if isinstance(value, str) and len(value) > 100 else value}")
