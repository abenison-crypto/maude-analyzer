"""Transform and clean MAUDE data with schema-aware processing."""

import re
from datetime import datetime, date
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DATE_FORMATS, MANUFACTURER_MAPPINGS, OUTCOME_CODES, TREATMENT_CODES
from config.logging_config import get_logger
from config.schema_registry import DATE_COLUMNS, INTEGER_COLUMNS, FLAG_COLUMNS
from config.column_mappings import COLUMN_MAPPINGS, get_db_column_name

logger = get_logger("transformer")


class DataTransformer:
    """Transform and clean MAUDE data records with schema awareness."""

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

        # Age parsing pattern
        self._age_pattern = re.compile(
            r"^(\d+(?:\.\d+)?)\s*"
            r"(year|yr|y|month|mo|m|week|wk|w|day|d|hour|hr|h)?s?$",
            re.IGNORECASE
        )

        # Weight parsing pattern (e.g., "150 lbs", "68 kg")
        self._weight_pattern = re.compile(
            r"^(\d+(?:\.\d+)?)\s*(lb|lbs|pound|pounds|kg|kilogram|kilograms)?s?$",
            re.IGNORECASE
        )

    def transform_record(
        self,
        record: Dict[str, Any],
        file_type: str,
        source_columns: Optional[List[str]] = None,
        source_file: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Transform a record with schema awareness.

        This method:
        1. Applies the appropriate transformation based on file type
        2. Handles missing columns gracefully
        3. Adds derived fields

        Args:
            record: Raw record dictionary (with DB column names).
            file_type: Type of MAUDE file.
            source_columns: Original columns from source file.
            source_file: Source filename for tracking.

        Returns:
            Transformed record dictionary.
        """
        transform_funcs = {
            "master": self.transform_master_record,
            "device": self.transform_device_record,
            "patient": self.transform_patient_record,
            "text": self.transform_text_record,
            "problem": self.transform_problem_record,
            # Passthrough transformations for new file types
            "patient_problem": self.transform_passthrough,
            "asr": self.transform_passthrough,
            "asr_ppc": self.transform_passthrough,
            "den": self.transform_passthrough,
            "disclaimer": self.transform_passthrough,
            "problem_lookup": self.transform_passthrough,
            "patient_problem_data": self.transform_passthrough,
        }

        func = transform_funcs.get(file_type)
        if func is None:
            raise ValueError(f"Unknown file type: {file_type}")

        return func(record, source_file)

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

        # Parse all date fields
        date_fields = [
            "date_received", "date_report", "date_of_event",
            "date_facility_aware", "report_date", "date_report_to_fda",
            "date_report_to_manufacturer", "date_manufacturer_received",
            "device_date_of_manufacture", "date_added", "date_changed",
        ]

        for field in date_fields:
            if field in transformed and transformed[field]:
                transformed[field] = self.parse_date(transformed[field])

        # Parse integer fields
        int_fields = ["number_devices_in_event", "number_patients_in_event"]
        for field in int_fields:
            if field in transformed and transformed[field]:
                transformed[field] = self.parse_int(transformed[field])

        # Normalize flag fields (Y/N/blank)
        flag_fields = [
            "adverse_event_flag", "product_problem_flag",
            "reprocessed_and_reused_flag", "health_professional",
            "initial_report_to_fda", "report_to_fda", "report_to_manufacturer",
            "single_use_flag", "manufacturer_link_flag", "summary_report_flag",
            "noe_summarized",
        ]
        for field in flag_fields:
            if field in transformed:
                transformed[field] = self.normalize_flag(transformed.get(field))

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

        # Normalize flag fields
        flag_fields = ["implant_flag", "date_removed_flag"]
        for field in flag_fields:
            if field in transformed:
                transformed[field] = self.normalize_flag(transformed.get(field))

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
        Transform a patient record with demographic parsing.

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

        # Parse patient age to numeric + unit
        if transformed.get("patient_age"):
            age_numeric, age_unit = self.parse_patient_age(transformed["patient_age"])
            transformed["patient_age_numeric"] = age_numeric
            transformed["patient_age_unit"] = age_unit

        # Normalize patient sex
        if transformed.get("patient_sex"):
            sex = transformed["patient_sex"].strip().upper()
            if sex in ["M", "MALE"]:
                transformed["patient_sex"] = "M"
            elif sex in ["F", "FEMALE"]:
                transformed["patient_sex"] = "F"
            elif sex in ["U", "UNKNOWN", ""]:
                transformed["patient_sex"] = "U"

        # Parse outcome codes (semicolon-separated like "D;H;R")
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

        # Parse treatment codes (semicolon-separated like "1;3;8")
        treatment_raw = None

        # Try to find treatment codes in the record
        for key in ["treatment_codes_raw", "sequence_number_treatment"]:
            if transformed.get(key) and isinstance(transformed[key], str):
                if ";" in transformed[key] or transformed[key] in TREATMENT_CODES:
                    treatment_raw = transformed[key]
                    break

        if treatment_raw:
            transformed["treatment_codes_raw"] = treatment_raw
            treatments = self.parse_treatment_codes(treatment_raw)

            transformed["treatment_drug"] = treatments.get("1", False)
            transformed["treatment_device"] = treatments.get("2", False)
            transformed["treatment_surgery"] = treatments.get("3", False)
            transformed["treatment_other"] = treatments.get("4", False)
            transformed["treatment_unknown"] = treatments.get("5", False)
            transformed["treatment_no_information"] = treatments.get("6", False)
            transformed["treatment_blood_products"] = treatments.get("7", False)
            transformed["treatment_hospitalization"] = treatments.get("8", False)
            transformed["treatment_physical_therapy"] = treatments.get("9", False)

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
        if transformed.get("date_report"):
            transformed["date_report"] = self.parse_date(transformed["date_report"])

        # Legacy field name support
        if transformed.get("date_received") and not transformed.get("date_report"):
            transformed["date_report"] = self.parse_date(transformed["date_received"])

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

    def transform_passthrough(
        self, record: Dict[str, Any], source_file: str = None
    ) -> Dict[str, Any]:
        """
        Passthrough transformation - minimal processing for records that
        don't need special handling.

        Args:
            record: Raw record dictionary.
            source_file: Source filename for tracking.

        Returns:
            Record with source_file added.
        """
        transformed = record.copy()

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

        # Skip obvious non-dates
        if date_str.upper() in ["", "NA", "N/A", "UNKNOWN", "UNK", "NOT PROVIDED"]:
            return None

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

    def normalize_flag(self, value: Optional[str]) -> Optional[str]:
        """
        Normalize a flag value (Y/N).

        Args:
            value: Raw flag value.

        Returns:
            "Y", "N", or None.
        """
        if not value:
            return None

        val = str(value).strip().upper()
        if val in ["Y", "YES", "1", "TRUE"]:
            return "Y"
        elif val in ["N", "NO", "0", "FALSE"]:
            return "N"
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

    def parse_patient_age(self, age_str: str) -> Tuple[Optional[float], Optional[str]]:
        """
        Parse patient age string into numeric value and unit.

        Examples:
            "65 years" -> (65.0, "years")
            "6 months" -> (6.0, "months")
            "30 days" -> (30.0, "days")
            "NA" -> (None, None)

        Args:
            age_str: Age string from FDA data.

        Returns:
            Tuple of (numeric_age, unit) or (None, None) if unparseable.
        """
        if not age_str or not isinstance(age_str, str):
            return None, None

        age_str = age_str.strip()

        # Skip obvious non-values
        if age_str.upper() in ["", "NA", "N/A", "UNKNOWN", "UNK", "NOT PROVIDED"]:
            return None, None

        # Try to parse with pattern
        match = self._age_pattern.match(age_str)
        if match:
            numeric = float(match.group(1))
            unit_raw = match.group(2) or "year"
            unit_raw = unit_raw.lower()

            # Normalize unit
            if unit_raw in ["year", "yr", "y"]:
                unit = "years"
            elif unit_raw in ["month", "mo", "m"]:
                unit = "months"
            elif unit_raw in ["week", "wk", "w"]:
                unit = "weeks"
            elif unit_raw in ["day", "d"]:
                unit = "days"
            elif unit_raw in ["hour", "hr", "h"]:
                unit = "hours"
            else:
                unit = "years"  # Default

            return numeric, unit

        # Try simple numeric
        try:
            numeric = float(age_str)
            return numeric, "years"  # Assume years
        except ValueError:
            pass

        return None, None

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

    def parse_treatment_codes(self, codes_str: str) -> Dict[str, bool]:
        """
        Parse treatment codes string.

        Args:
            codes_str: Semicolon-separated treatment codes (e.g., "1;3;8").

        Returns:
            Dictionary mapping code to True if present.
        """
        treatments = {}

        if not codes_str:
            return treatments

        codes = str(codes_str).split(";")

        for code in codes:
            code = code.strip()
            if code in TREATMENT_CODES:
                treatments[code] = True

        return treatments

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


class SchemaAwareTransformer(DataTransformer):
    """
    Transformer that handles column mapping between FDA and DB formats.

    This class extends DataTransformer to add:
    - Automatic column mapping from FDA to DB names
    - Handling of missing columns (set to NULL)
    - Support for different source schemas
    """

    def transform_with_mapping(
        self,
        record: Dict[str, Any],
        file_type: str,
        source_columns: List[str],
        source_file: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Transform a record with column mapping.

        Args:
            record: Raw record with FDA column names.
            file_type: Type of MAUDE file.
            source_columns: Original FDA column names from source file.
            source_file: Source filename.

        Returns:
            Transformed record with DB column names.
        """
        # Get the column mapping for this file type
        mapping = COLUMN_MAPPINGS.get(file_type, {})

        # Map FDA columns to DB columns
        mapped_record = {}
        for fda_col, value in record.items():
            db_col = mapping.get(fda_col.upper(), fda_col.lower())
            mapped_record[db_col] = value

        # Apply type-specific transformation
        return self.transform_record(mapped_record, file_type, source_columns, source_file)


# Convenience function for backward compatibility
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

    return transformer.transform_record(record, file_type, source_file=source_file)


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

    # Test patient age parsing
    test_ages = [
        "65 years",
        "6 months",
        "30 days",
        "2.5 years",
        "NA",
        "72",
        "UNKNOWN",
        "8 weeks",
    ]

    print("\nPatient age parsing tests:")
    for a in test_ages:
        numeric, unit = transformer.parse_patient_age(a)
        print(f"  {a!r} -> ({numeric}, {unit!r})")

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

    # Test flag normalization
    test_flags = ["Y", "N", "YES", "NO", "1", "0", "true", "", None]
    print("\nFlag normalization tests:")
    for f in test_flags:
        result = transformer.normalize_flag(f)
        print(f"  {f!r} -> {result!r}")
