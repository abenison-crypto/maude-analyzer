"""Transform and clean MAUDE data."""

import re
from datetime import datetime, date
from typing import Dict, Any, Optional, List
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DATE_FORMATS, MANUFACTURER_MAPPINGS, OUTCOME_CODES
from config.logging_config import get_logger

logger = get_logger("transformer")


class DataTransformer:
    """Transform and clean MAUDE data records."""

    def __init__(self):
        """Initialize the transformer."""
        # Build uppercase manufacturer mapping for faster lookup
        self._manufacturer_map = {
            k.upper(): v for k, v in MANUFACTURER_MAPPINGS.items()
        }

        # Compile date patterns for performance
        self._date_patterns = [
            (re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$"), "%m/%d/%Y"),
            (re.compile(r"^\d{4}-\d{2}-\d{2}$"), "%Y-%m-%d"),
            (re.compile(r"^\d{8}$"), "%Y%m%d"),
            (re.compile(r"^\d{1,2}-[A-Za-z]{3}-\d{4}$"), "%d-%b-%Y"),
            (re.compile(r"^\d{1,2}/\d{1,2}/\d{2}$"), "%m/%d/%y"),
        ]

    def transform_master_record(
        self, record: Dict[str, Any], source_file: str = None
    ) -> Dict[str, Any]:
        """
        Transform a master events record.

        Args:
            record: Raw record dictionary.
            source_file: Source filename for tracking.

        Returns:
            Transformed record dictionary.
        """
        transformed = record.copy()

        # Parse dates
        date_fields = [
            "date_received",
            "date_report",
            "date_of_event",
            "date_facility_aware",
            "report_date",
            "date_report_to_fda",
            "date_report_to_manufacturer",
            "date_manufacturer_received",
        ]

        for field in date_fields:
            if field in transformed and transformed[field]:
                transformed[field] = self.parse_date(transformed[field])

        # Parse integer fields
        int_fields = ["number_devices_in_event", "number_patients_in_event"]
        for field in int_fields:
            if field in transformed and transformed[field]:
                transformed[field] = self.parse_int(transformed[field])

        # Standardize manufacturer name
        if transformed.get("manufacturer_name"):
            transformed["manufacturer_clean"] = self.standardize_manufacturer(
                transformed["manufacturer_name"]
            )

        # Extract year/month from dates for indexing
        if transformed.get("date_of_event"):
            dt = transformed["date_of_event"]
            if isinstance(dt, (date, datetime)):
                transformed["event_year"] = dt.year
                transformed["event_month"] = dt.month

        if transformed.get("date_received"):
            dt = transformed["date_received"]
            if isinstance(dt, (date, datetime)):
                transformed["received_year"] = dt.year
                transformed["received_month"] = dt.month

        # Clean event type
        if transformed.get("event_type"):
            transformed["event_type"] = transformed["event_type"].strip().upper()

        # Add source file tracking
        if source_file:
            transformed["source_file"] = source_file

        return transformed

    def transform_device_record(
        self, record: Dict[str, Any], source_file: str = None
    ) -> Dict[str, Any]:
        """
        Transform a device record.

        Args:
            record: Raw record dictionary.
            source_file: Source filename.

        Returns:
            Transformed record dictionary.
        """
        transformed = record.copy()

        # Parse dates
        date_fields = [
            "date_received",
            "date_returned_to_manufacturer",
            "expiration_date_of_device",
        ]

        for field in date_fields:
            if field in transformed and transformed[field]:
                transformed[field] = self.parse_date(transformed[field])

        # Parse integer fields
        if transformed.get("device_sequence_number"):
            transformed["device_sequence_number"] = self.parse_int(
                transformed["device_sequence_number"]
            )

        # Standardize manufacturer name
        if transformed.get("manufacturer_d_name"):
            transformed["manufacturer_d_clean"] = self.standardize_manufacturer(
                transformed["manufacturer_d_name"]
            )

        # Clean brand name (remove extra whitespace)
        if transformed.get("brand_name"):
            transformed["brand_name"] = " ".join(transformed["brand_name"].split())

        if source_file:
            transformed["source_file"] = source_file

        return transformed

    def transform_patient_record(
        self, record: Dict[str, Any], source_file: str = None
    ) -> Dict[str, Any]:
        """
        Transform a patient record.

        Args:
            record: Raw record dictionary.
            source_file: Source filename.

        Returns:
            Transformed record dictionary.
        """
        transformed = record.copy()

        # Parse date
        if transformed.get("date_received"):
            transformed["date_received"] = self.parse_date(transformed["date_received"])

        # Parse integer fields
        int_fields = [
            "patient_sequence_number",
            "sequence_number_treatment",
            "sequence_number_outcome",
        ]
        for field in int_fields:
            if field in transformed and transformed[field]:
                transformed[field] = self.parse_int(transformed[field])

        # Parse outcome codes (semicolon-separated like "D;H;R")
        # Note: The patient file structure varies, outcome codes might be in different columns
        # This handles the concatenated format
        outcome_raw = None

        # Try to find outcome codes in the record
        for key in ["outcome_codes_raw", "sequence_number_outcome"]:
            if transformed.get(key) and isinstance(transformed[key], str):
                if ";" in transformed[key] or transformed[key] in OUTCOME_CODES:
                    outcome_raw = transformed[key]
                    break

        if outcome_raw:
            transformed["outcome_codes_raw"] = outcome_raw
            outcomes = self.parse_outcome_codes(outcome_raw)

            transformed["outcome_death"] = outcomes.get("D", False)
            transformed["outcome_life_threatening"] = outcomes.get("L", False)
            transformed["outcome_hospitalization"] = outcomes.get("H", False)
            transformed["outcome_disability"] = outcomes.get("DS", False)
            transformed["outcome_congenital_anomaly"] = outcomes.get("CA", False)
            transformed["outcome_required_intervention"] = outcomes.get("RI", False)
            transformed["outcome_other"] = outcomes.get("OT", False)

        if source_file:
            transformed["source_file"] = source_file

        return transformed

    def transform_text_record(
        self, record: Dict[str, Any], source_file: str = None
    ) -> Dict[str, Any]:
        """
        Transform a text/narrative record.

        Args:
            record: Raw record dictionary.
            source_file: Source filename.

        Returns:
            Transformed record dictionary.
        """
        transformed = record.copy()

        # Parse date
        if transformed.get("date_received"):
            transformed["date_received"] = self.parse_date(transformed["date_received"])

        # Parse integer
        if transformed.get("patient_sequence_number"):
            transformed["patient_sequence_number"] = self.parse_int(
                transformed["patient_sequence_number"]
            )

        # Clean text content (remove control characters but preserve content)
        if transformed.get("text_content"):
            transformed["text_content"] = self.clean_text(transformed["text_content"])

        if source_file:
            transformed["source_file"] = source_file

        return transformed

    def transform_problem_record(
        self, record: Dict[str, Any], source_file: str = None
    ) -> Dict[str, Any]:
        """
        Transform a device problem record.

        Args:
            record: Raw record dictionary.
            source_file: Source filename.

        Returns:
            Transformed record dictionary.
        """
        transformed = record.copy()

        # Clean problem code
        if transformed.get("device_problem_code"):
            transformed["device_problem_code"] = (
                transformed["device_problem_code"].strip().upper()
            )

        if source_file:
            transformed["source_file"] = source_file

        return transformed

    def parse_date(self, date_str: str) -> Optional[date]:
        """
        Parse a date string to a date object.

        Args:
            date_str: Date string in various formats.

        Returns:
            date object or None if parsing fails.
        """
        if not date_str or not isinstance(date_str, str):
            return None

        date_str = date_str.strip()

        # Try each pattern
        for pattern, fmt in self._date_patterns:
            if pattern.match(date_str):
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue

        # Try additional formats
        for fmt in DATE_FORMATS:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue

        # Log unparseable dates (but don't flood logs)
        logger.debug(f"Could not parse date: {date_str}")
        return None

    def parse_int(self, value: str) -> Optional[int]:
        """
        Parse an integer string.

        Args:
            value: Integer string.

        Returns:
            Integer or None.
        """
        if not value or not isinstance(value, str):
            return None

        try:
            # Handle cases like "1.0"
            return int(float(value.strip()))
        except (ValueError, TypeError):
            return None

    def standardize_manufacturer(self, name: str) -> str:
        """
        Standardize a manufacturer name.

        Args:
            name: Raw manufacturer name.

        Returns:
            Standardized name or original if no mapping found.
        """
        if not name:
            return "Unknown"

        # Clean and uppercase for lookup
        clean_name = " ".join(name.upper().split())

        # Try exact match first
        if clean_name in self._manufacturer_map:
            return self._manufacturer_map[clean_name]

        # Try partial match for common variations
        for raw, standard in self._manufacturer_map.items():
            if raw in clean_name or clean_name in raw:
                return standard

        # Return original name (title case) if no mapping
        return name.strip().title()

    def parse_outcome_codes(self, codes_str: str) -> Dict[str, bool]:
        """
        Parse outcome codes string.

        Args:
            codes_str: Semicolon-separated outcome codes (e.g., "D;H;R").

        Returns:
            Dictionary mapping code to True if present.
        """
        outcomes = {}

        if not codes_str:
            return outcomes

        codes = codes_str.upper().split(";")

        for code in codes:
            code = code.strip()
            if code in OUTCOME_CODES:
                outcomes[code] = True

        return outcomes

    def clean_text(self, text: str) -> str:
        """
        Clean narrative text content.

        Args:
            text: Raw text content.

        Returns:
            Cleaned text.
        """
        if not text:
            return ""

        # Remove control characters except newlines and tabs
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)

        # Normalize whitespace (but preserve paragraph breaks)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)

        return text.strip()


def transform_record(
    record: Dict[str, Any],
    file_type: str,
    transformer: Optional[DataTransformer] = None,
    source_file: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Transform a record based on file type.

    Args:
        record: Raw record dictionary.
        file_type: Type of record (master, device, patient, text, problem).
        transformer: DataTransformer instance (created if None).
        source_file: Source filename.

    Returns:
        Transformed record.
    """
    if transformer is None:
        transformer = DataTransformer()

    transform_funcs = {
        "master": transformer.transform_master_record,
        "device": transformer.transform_device_record,
        "patient": transformer.transform_patient_record,
        "text": transformer.transform_text_record,
        "problem": transformer.transform_problem_record,
    }

    func = transform_funcs.get(file_type)
    if func is None:
        raise ValueError(f"Unknown file type: {file_type}")

    return func(record, source_file)


if __name__ == "__main__":
    # Test transformer
    transformer = DataTransformer()

    # Test date parsing
    test_dates = [
        "01/15/2024",
        "2024-01-15",
        "20240115",
        "15-Jan-2024",
        "01/15/24",
        "invalid",
        None,
    ]

    print("Date parsing tests:")
    for d in test_dates:
        result = transformer.parse_date(d)
        print(f"  {d!r} -> {result}")

    # Test manufacturer standardization
    test_manufacturers = [
        "ABBOTT NEUROMODULATION",
        "ST. JUDE MEDICAL, INC.",
        "MEDTRONIC INC",
        "Boston Scientific Corporation",
        "NEVRO CORP",
        "Unknown Company LLC",
    ]

    print("\nManufacturer standardization tests:")
    for m in test_manufacturers:
        result = transformer.standardize_manufacturer(m)
        print(f"  {m} -> {result}")

    # Test outcome parsing
    test_outcomes = ["D;H", "L;RI;OT", "H", ""]
    print("\nOutcome parsing tests:")
    for o in test_outcomes:
        result = transformer.parse_outcome_codes(o)
        print(f"  {o!r} -> {result}")
