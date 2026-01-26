"""Parse FDA MAUDE ASR (Alternative Summary Report) files.

ASR files contain summarized adverse event data from 1999-2019.
These are manufacturer-submitted summary reports rather than individual reports.

File types:
- ASR_{year}.csv: Individual ASR summary reports
- ASR_PPCs.csv: Patient Problem Codes associated with ASR reports

Note: ASR files use CSV format (comma-delimited), not pipe-delimited.
"""

import csv
import sys
from pathlib import Path
from typing import Generator, Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.logging_config import get_logger

logger = get_logger("parse_asr")

# Increase CSV field size limit for any large text fields
csv.field_size_limit(sys.maxsize)


# =============================================================================
# ASR COLUMN DEFINITIONS (actual FDA file format)
# =============================================================================

# Columns in ASR_{year}.csv files (actual header names)
ASR_FILE_COLUMNS = [
    "exemptn_no",           # Exemption number
    "mfr_no",               # Manufacturer number (FDA ID)
    "mfr_name",             # Manufacturer name
    "report_id",            # Unique report ID within ASR
    "date_of_event",        # Date of the event
    "mfr_aware_date",       # Date manufacturer became aware
    "event_type",           # M=Malfunction, I=Injury, D=Death
    "dev_prob_cd",          # Device problem code
    "report_year",          # Year of report
    "report_qtr",           # Quarter of report (1-4)
    "initial_report_flag",  # I=Initial, F=Follow-up
    "dev_id",               # Device identifier
    "product_code",         # FDA product code (3-letter)
    "brand_name",           # Device brand name
    "model_no",             # Model number
    "catalog_no",           # Catalog number
    "impl_avail_for_eval",  # Implant available for evaluation
    "impl_ret_to_mfr",      # Implant returned to manufacturer
]

# Columns in ASR_PPCs.csv file (patient problem codes)
ASR_PPC_FILE_COLUMNS = [
    "exemptn_no",           # Exemption number (FK to ASR)
    "report_id",            # Report ID (FK to ASR)
    "product_code",         # FDA product code
    "report_year",          # Year of report
    "report_qtr",           # Quarter of report
    "patient_prob_cd",      # Patient problem code(s), semicolon-separated
]

# Mapping from ASR columns to standardized database columns
ASR_TO_DB_MAPPING = {
    "exemptn_no": "exemption_number",
    "mfr_no": "manufacturer_id",
    "mfr_name": "manufacturer_name",
    "report_id": "asr_report_id",
    "date_of_event": "date_of_event",
    "mfr_aware_date": "date_manufacturer_received",
    "event_type": "event_type",
    "dev_prob_cd": "device_problem_code",
    "report_year": "report_year",
    "report_qtr": "report_quarter",
    "initial_report_flag": "initial_report_flag",
    "dev_id": "device_id",
    "product_code": "product_code",
    "brand_name": "brand_name",
    "model_no": "model_number",
    "catalog_no": "catalog_number",
    "impl_avail_for_eval": "implant_available_for_eval",
    "impl_ret_to_mfr": "implant_returned_to_mfr",
}

ASR_PPC_TO_DB_MAPPING = {
    "exemptn_no": "exemption_number",
    "report_id": "asr_report_id",
    "product_code": "product_code",
    "report_year": "report_year",
    "report_qtr": "report_quarter",
    "patient_prob_cd": "patient_problem_codes",
}


@dataclass
class ASRParseResult:
    """Result of parsing an ASR file."""
    filename: str
    file_type: str  # 'asr' or 'asr_ppc'
    total_rows: int = 0
    parsed_rows: int = 0
    error_rows: int = 0
    errors: List[Tuple[int, str]] = field(default_factory=list)
    year: Optional[int] = None


class ASRParser:
    """Parser for FDA MAUDE ASR (Alternative Summary Report) files.

    ASR files are CSV format and contain summarized adverse event data
    that manufacturers submitted under exemption programs from 1999-2019.
    """

    def __init__(self, encoding: str = "latin-1"):
        """
        Initialize the ASR parser.

        Args:
            encoding: File encoding (latin-1 handles most FDA files).
        """
        self.encoding = encoding

    def extract_year_from_filename(self, filename: str) -> Optional[int]:
        """
        Extract year from ASR filename.

        Args:
            filename: Name of the file (e.g., ASR_1999.csv).

        Returns:
            Extracted year or None.
        """
        name = filename.upper()
        if name.startswith("ASR_") and name.endswith(".CSV"):
            try:
                year_str = name[4:-4]  # Extract between ASR_ and .csv
                if year_str.isdigit() and len(year_str) == 4:
                    return int(year_str)
            except ValueError:
                pass
        return None

    def parse_asr_file(
        self,
        filepath: Path,
        limit: Optional[int] = None,
        map_to_db_columns: bool = True,
        filter_product_codes: Optional[List[str]] = None,
    ) -> Generator[Dict[str, Any], None, ASRParseResult]:
        """
        Parse an ASR summary report file.

        Args:
            filepath: Path to the ASR_{year}.csv file.
            limit: Maximum number of records to return.
            map_to_db_columns: If True, map columns to database names.
            filter_product_codes: Only return records with these product codes.

        Yields:
            Dictionary for each parsed ASR record.

        Returns:
            ASRParseResult with statistics.
        """
        year = self.extract_year_from_filename(filepath.name)

        result = ASRParseResult(
            filename=filepath.name,
            file_type="asr",
            year=year,
        )

        logger.info(f"Parsing ASR file: {filepath.name} (year: {year})")

        try:
            with open(filepath, "r", encoding=self.encoding, errors="replace") as f:
                reader = csv.DictReader(f)

                for line_num, row in enumerate(reader, 2):  # 2 because header is line 1
                    result.total_rows += 1

                    try:
                        # Clean and normalize the record
                        record = self._clean_record(row)

                        # Apply product code filter
                        if filter_product_codes:
                            product_code = record.get("product_code", "")
                            if product_code not in filter_product_codes:
                                continue

                        # Map to database column names if requested
                        if map_to_db_columns:
                            record = self._map_to_db_columns(record, ASR_TO_DB_MAPPING)

                        # Add source tracking
                        record["source_file"] = filepath.name
                        record["source_type"] = "asr"

                        result.parsed_rows += 1
                        yield record

                        if limit and result.parsed_rows >= limit:
                            break

                    except Exception as e:
                        result.error_rows += 1
                        if len(result.errors) < 100:
                            result.errors.append((line_num, str(e)))

        except Exception as e:
            logger.error(f"Error reading ASR file {filepath}: {e}")
            raise

        logger.info(
            f"Parsed ASR {filepath.name}: {result.parsed_rows} records, "
            f"{result.error_rows} errors"
        )

        return result

    def parse_asr_ppc_file(
        self,
        filepath: Path,
        limit: Optional[int] = None,
        map_to_db_columns: bool = True,
        expand_problem_codes: bool = True,
    ) -> Generator[Dict[str, Any], None, ASRParseResult]:
        """
        Parse an ASR Patient Problem Codes file.

        Args:
            filepath: Path to the ASR_PPCs.csv file.
            limit: Maximum number of records to return.
            map_to_db_columns: If True, map columns to database names.
            expand_problem_codes: If True, split semicolon-separated codes
                                  into individual records.

        Yields:
            Dictionary for each parsed PPC record.

        Returns:
            ASRParseResult with statistics.
        """
        result = ASRParseResult(
            filename=filepath.name,
            file_type="asr_ppc",
        )

        logger.info(f"Parsing ASR PPC file: {filepath.name}")

        try:
            with open(filepath, "r", encoding=self.encoding, errors="replace") as f:
                reader = csv.DictReader(f)

                for line_num, row in enumerate(reader, 2):
                    result.total_rows += 1

                    try:
                        # Clean the record
                        record = self._clean_record(row)

                        # Handle semicolon-separated problem codes
                        if expand_problem_codes:
                            problem_codes_str = record.get("patient_prob_cd", "") or ""
                            problem_codes = [
                                pc.strip()
                                for pc in problem_codes_str.split(";")
                                if pc.strip()
                            ]

                            for pc in problem_codes:
                                expanded_record = record.copy()
                                expanded_record["patient_prob_cd"] = pc

                                if map_to_db_columns:
                                    expanded_record = self._map_to_db_columns(
                                        expanded_record, ASR_PPC_TO_DB_MAPPING
                                    )

                                expanded_record["source_file"] = filepath.name

                                result.parsed_rows += 1
                                yield expanded_record

                                if limit and result.parsed_rows >= limit:
                                    return result
                        else:
                            if map_to_db_columns:
                                record = self._map_to_db_columns(
                                    record, ASR_PPC_TO_DB_MAPPING
                                )
                            record["source_file"] = filepath.name

                            result.parsed_rows += 1
                            yield record

                            if limit and result.parsed_rows >= limit:
                                break

                    except Exception as e:
                        result.error_rows += 1
                        if len(result.errors) < 100:
                            result.errors.append((line_num, str(e)))

        except Exception as e:
            logger.error(f"Error reading ASR PPC file {filepath}: {e}")
            raise

        logger.info(
            f"Parsed ASR PPC {filepath.name}: {result.parsed_rows} records, "
            f"{result.error_rows} errors"
        )

        return result

    def _clean_record(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and normalize a record.

        Args:
            row: Raw record from CSV reader.

        Returns:
            Cleaned record with lowercase keys and trimmed values.
        """
        cleaned = {}
        for key, value in row.items():
            # Normalize key to lowercase
            clean_key = key.strip().lower()
            # Clean value
            if value is None:
                cleaned[clean_key] = None
            elif isinstance(value, str):
                clean_value = value.strip()
                cleaned[clean_key] = clean_value if clean_value else None
            else:
                cleaned[clean_key] = value
        return cleaned

    def _map_to_db_columns(
        self, record: Dict[str, Any], mapping: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Map record keys from FDA names to database column names.

        Args:
            record: Record with FDA column names.
            mapping: Column name mapping.

        Returns:
            Record with database column names.
        """
        mapped = {}
        for fda_col, db_col in mapping.items():
            if fda_col in record:
                mapped[db_col] = record[fda_col]
        return mapped

    def parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """
        Parse a date string from ASR format.

        ASR dates are typically MM/DD/YYYY format.

        Args:
            date_str: Date string to parse.

        Returns:
            ISO format date string (YYYY-MM-DD) or None.
        """
        if not date_str:
            return None

        # Try common formats
        formats = [
            "%m/%d/%Y",  # 10/22/1999
            "%Y-%m-%d",  # 1999-10-22
            "%m-%d-%Y",  # 10-22-1999
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        return None

    def get_asr_summary_stats(self, filepath: Path) -> Dict[str, Any]:
        """
        Get summary statistics for an ASR file.

        Args:
            filepath: Path to ASR file.

        Returns:
            Dictionary with summary statistics.
        """
        stats = {
            "file": filepath.name,
            "total_records": 0,
            "event_types": {},
            "product_codes": set(),
            "manufacturers": set(),
            "years": set(),
        }

        for record in self.parse_asr_file(filepath, map_to_db_columns=False):
            stats["total_records"] += 1

            # Count event types
            event_type = record.get("event_type", "Unknown")
            stats["event_types"][event_type] = stats["event_types"].get(event_type, 0) + 1

            # Track unique values
            if record.get("product_code"):
                stats["product_codes"].add(record["product_code"])
            if record.get("mfr_name"):
                stats["manufacturers"].add(record["mfr_name"])
            if record.get("report_year"):
                stats["years"].add(record["report_year"])

        # Convert sets to counts for JSON serialization
        stats["unique_product_codes"] = len(stats["product_codes"])
        stats["unique_manufacturers"] = len(stats["manufacturers"])
        stats["year_range"] = (
            f"{min(stats['years'])}-{max(stats['years'])}"
            if stats["years"]
            else "N/A"
        )
        del stats["product_codes"]
        del stats["manufacturers"]
        del stats["years"]

        return stats


def parse_all_asr_files(
    data_dir: Path,
    filter_product_codes: Optional[List[str]] = None,
) -> Generator[Dict[str, Any], None, None]:
    """
    Parse all ASR files in a directory.

    Args:
        data_dir: Directory containing ASR files.
        filter_product_codes: Optional product codes to filter by.

    Yields:
        Record dictionaries from all ASR files.
    """
    parser = ASRParser()

    # Find all ASR files (ASR_YYYY.csv)
    asr_files = sorted(data_dir.glob("ASR_[0-9][0-9][0-9][0-9].csv"))

    if not asr_files:
        # Try case-insensitive search
        asr_files = sorted([
            f for f in data_dir.iterdir()
            if f.name.upper().startswith("ASR_")
            and f.name.upper().endswith(".CSV")
            and f.name[4:8].isdigit()
        ])

    logger.info(f"Found {len(asr_files)} ASR files to parse")

    for filepath in asr_files:
        try:
            yield from parser.parse_asr_file(
                filepath,
                filter_product_codes=filter_product_codes,
            )
        except Exception as e:
            logger.error(f"Error parsing {filepath}: {e}")
            continue


def parse_asr_ppc_file(data_dir: Path) -> Generator[Dict[str, Any], None, None]:
    """
    Parse the ASR Patient Problem Codes file.

    Args:
        data_dir: Directory containing ASR_PPCs.csv.

    Yields:
        Record dictionaries for patient problem codes.
    """
    parser = ASRParser()

    # Find ASR_PPCs.csv file
    ppc_file = data_dir / "ASR_PPCs.csv"
    if not ppc_file.exists():
        # Try case variations
        for f in data_dir.iterdir():
            if f.name.upper() == "ASR_PPCS.CSV":
                ppc_file = f
                break

    if not ppc_file.exists():
        logger.warning("ASR_PPCs.csv not found in data directory")
        return

    try:
        yield from parser.parse_asr_ppc_file(ppc_file)
    except Exception as e:
        logger.error(f"Error parsing ASR PPC file: {e}")


if __name__ == "__main__":
    import argparse

    arg_parser = argparse.ArgumentParser(description="Parse ASR files")
    arg_parser.add_argument("file", type=Path, help="ASR file to parse")
    arg_parser.add_argument(
        "--sample", type=int, default=5, help="Number of sample records"
    )
    arg_parser.add_argument("--stats", action="store_true", help="Show file statistics")
    arg_parser.add_argument(
        "--product-code", help="Filter by product code"
    )

    args = arg_parser.parse_args()

    parser = ASRParser()

    if args.stats:
        print(f"\nAnalyzing {args.file.name}...")
        stats = parser.get_asr_summary_stats(args.file)
        print(f"\nFile Statistics:")
        print(f"  Total Records: {stats['total_records']:,}")
        print(f"  Unique Product Codes: {stats['unique_product_codes']}")
        print(f"  Unique Manufacturers: {stats['unique_manufacturers']}")
        print(f"  Year Range: {stats['year_range']}")
        print(f"  Event Types:")
        for event_type, count in sorted(stats["event_types"].items()):
            event_name = {
                "M": "Malfunction",
                "I": "Injury",
                "D": "Death",
            }.get(event_type, event_type)
            print(f"    {event_name}: {count:,}")
    else:
        filter_codes = [args.product_code] if args.product_code else None

        print(f"\nFirst {args.sample} records from {args.file.name}:")

        count = 0
        if "PPC" in args.file.name.upper():
            gen = parser.parse_asr_ppc_file(args.file)
        else:
            gen = parser.parse_asr_file(args.file, filter_product_codes=filter_codes)

        for record in gen:
            count += 1
            print(f"\n--- Record {count} ---")
            for key, value in record.items():
                if value:
                    display_val = (
                        value[:80] + "..."
                        if isinstance(value, str) and len(value) > 80
                        else value
                    )
                    print(f"  {key}: {display_val}")

            if count >= args.sample:
                break
