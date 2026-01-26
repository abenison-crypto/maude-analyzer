"""
FDA MAUDE Device File Schema Definitions.

Device files contain the actual manufacturer and product code information
that is NOT present in the master (MDR) file. The master file's
MANUFACTURER_G1_NAME is the SUBMITTER, not the device manufacturer.

Historical Schema Evolution:
- 1984-1997: DEN Legacy format (different structure entirely)
- 1998-2010: Transition period (28 columns, some variations)
- 2010-present: Modern format (28 columns, stable)

Device files:
- foidevthru1997.zip: Pre-1998 historical data
- foidev1998.zip - foidev2019.zip: Annual device files
- device2020.zip - device2025.zip: Recent annual files
- foidev.zip: Current year, updated weekly
"""

from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass


@dataclass
class SchemaDefinition:
    """Definition of a schema version."""
    name: str
    columns: List[str]
    year_start: Optional[int]
    year_end: Optional[int]
    column_count: int
    delimiter: str = "|"
    encoding: str = "latin-1"
    has_header: bool = True
    notes: str = ""


# =============================================================================
# CURRENT DEVICE SCHEMA (2010-present) - 28 columns
# =============================================================================

DEVICE_COLUMNS_CURRENT: List[str] = [
    "MDR_REPORT_KEY",                       # 1 - Foreign key to master
    "DEVICE_EVENT_KEY",                     # 2 - Unique device event key
    "IMPLANT_FLAG",                         # 3 - Y/N/blank
    "DATE_REMOVED_FLAG",                    # 4 - Y/N/blank
    "DEVICE_SEQUENCE_NO",                   # 5 - Sequence number (integer)
    "DATE_RECEIVED",                        # 6 - Date
    "BRAND_NAME",                           # 7 - Product brand name
    "GENERIC_NAME",                         # 8 - Generic device name
    "MANUFACTURER_D_NAME",                  # 9 - ACTUAL device manufacturer
    "MANUFACTURER_D_ADDRESS_1",             # 10
    "MANUFACTURER_D_ADDRESS_2",             # 11
    "MANUFACTURER_D_CITY",                  # 12
    "MANUFACTURER_D_STATE_CODE",            # 13
    "MANUFACTURER_D_ZIP_CODE",              # 14
    "MANUFACTURER_D_ZIP_CODE_EXT",          # 15
    "MANUFACTURER_D_COUNTRY_CODE",          # 16
    "MANUFACTURER_D_POSTAL_CODE",           # 17
    "EXPIRATION_DATE_OF_DEVICE",            # 18 - Date
    "MODEL_NUMBER",                         # 19
    "CATALOG_NUMBER",                       # 20
    "LOT_NUMBER",                           # 21
    "OTHER_ID_NUMBER",                      # 22
    "DEVICE_OPERATOR",                      # 23 - Who was using device
    "DEVICE_AVAILABILITY",                  # 24 - For evaluation status
    "DATE_RETURNED_TO_MANUFACTURER",        # 25 - Date
    "DEVICE_REPORT_PRODUCT_CODE",           # 26 - FDA product classification code
    "DEVICE_AGE_TEXT",                      # 27 - Age/use duration text
    "DEVICE_EVALUATED_BY_MANUFACTUR",       # 28 - Note: truncated in FDA
]

DEVICE_SCHEMA_CURRENT = SchemaDefinition(
    name="device_current",
    columns=DEVICE_COLUMNS_CURRENT,
    year_start=2010,
    year_end=None,  # Current
    column_count=28,
    notes="Modern device schema, stable since 2010",
)


# =============================================================================
# TRANSITIONAL DEVICE SCHEMA (1998-2010) - 28 columns but some variations
# =============================================================================

# The schema was mostly the same but had some encoding and data quality variations
DEVICE_COLUMNS_1998_2010: List[str] = DEVICE_COLUMNS_CURRENT.copy()

DEVICE_SCHEMA_1998_2010 = SchemaDefinition(
    name="device_1998_2010",
    columns=DEVICE_COLUMNS_1998_2010,
    year_start=1998,
    year_end=2009,
    column_count=28,
    notes="Transitional period - same columns but encoding variations",
)


# =============================================================================
# PRE-1998 DEVICE SCHEMA (foidevthru1997.zip)
# =============================================================================

# Pre-1998 device data is included in foidevthru1997.zip
# The format is similar but may have variations
DEVICE_COLUMNS_PRE_1998: List[str] = DEVICE_COLUMNS_CURRENT.copy()

DEVICE_SCHEMA_PRE_1998 = SchemaDefinition(
    name="device_pre_1998",
    columns=DEVICE_COLUMNS_PRE_1998,
    year_start=1984,
    year_end=1997,
    column_count=28,
    encoding="latin-1",
    notes="Pre-1998 data may have more quality issues and missing fields",
)


# =============================================================================
# DEN LEGACY DEVICE FORMAT (1984-1997 separate files)
# =============================================================================

# The DEN (Device Experience Network) files (mdr84.zip - mdr97.zip) had a
# completely different format with combined event/device data

DEN_LEGACY_COLUMNS: List[str] = [
    "MDR_REPORT_KEY",              # 1 - Report key (may be different format)
    "REPORT_NUMBER",               # 2
    "REPORT_SOURCE",               # 3
    "DATE_RECEIVED",               # 4
    "DATE_OF_EVENT",               # 5
    "DATE_REPORT",                 # 6
    "BRAND_NAME",                  # 7
    "GENERIC_NAME",                # 8
    "MODEL_NUMBER",                # 9
    "CATALOG_NUMBER",              # 10
    "LOT_NUMBER",                  # 11
    "DEVICE_OPERATOR",             # 12
    "MANUFACTURER_NAME",           # 13
    "MANUFACTURER_CITY",           # 14
    "MANUFACTURER_STATE",          # 15
    "MANUFACTURER_COUNTRY",        # 16
    "EVENT_TYPE",                  # 17
    "EVENT_DESCRIPTION",           # 18
    "PATIENT_OUTCOME",             # 19
]

DEN_LEGACY_SCHEMA = SchemaDefinition(
    name="den_legacy",
    columns=DEN_LEGACY_COLUMNS,
    year_start=1984,
    year_end=1997,
    column_count=19,
    notes="DEN legacy format - combined event/device data, not normalized",
)


# =============================================================================
# COLUMN MAPPING: FDA to Database
# =============================================================================

DEVICE_FDA_TO_DB_MAPPING: Dict[str, str] = {
    "MDR_REPORT_KEY": "mdr_report_key",
    "DEVICE_EVENT_KEY": "device_event_key",
    "IMPLANT_FLAG": "implant_flag",
    "DATE_REMOVED_FLAG": "date_removed_flag",
    "DEVICE_SEQUENCE_NO": "device_sequence_number",
    "DATE_RECEIVED": "date_received",
    "BRAND_NAME": "brand_name",
    "GENERIC_NAME": "generic_name",
    "MANUFACTURER_D_NAME": "manufacturer_d_name",
    "MANUFACTURER_D_ADDRESS_1": "manufacturer_d_address_1",
    "MANUFACTURER_D_ADDRESS_2": "manufacturer_d_address_2",
    "MANUFACTURER_D_CITY": "manufacturer_d_city",
    "MANUFACTURER_D_STATE_CODE": "manufacturer_d_state",
    "MANUFACTURER_D_ZIP_CODE": "manufacturer_d_zip",
    "MANUFACTURER_D_ZIP_CODE_EXT": "manufacturer_d_zip_ext",
    "MANUFACTURER_D_COUNTRY_CODE": "manufacturer_d_country",
    "MANUFACTURER_D_POSTAL_CODE": "manufacturer_d_postal",
    "EXPIRATION_DATE_OF_DEVICE": "expiration_date_of_device",
    "MODEL_NUMBER": "model_number",
    "CATALOG_NUMBER": "catalog_number",
    "LOT_NUMBER": "lot_number",
    "OTHER_ID_NUMBER": "other_id_number",
    "DEVICE_OPERATOR": "device_operator",
    "DEVICE_AVAILABILITY": "device_availability",
    "DATE_RETURNED_TO_MANUFACTURER": "date_returned_to_manufacturer",
    "DEVICE_REPORT_PRODUCT_CODE": "device_report_product_code",
    "DEVICE_AGE_TEXT": "device_age_text",
    "DEVICE_EVALUATED_BY_MANUFACTUR": "device_evaluated_by_manufacturer",
}


# =============================================================================
# SCHEMA DETECTION AND SELECTION
# =============================================================================

def get_device_schema(
    filename: str = None,
    year: int = None,
    column_count: int = None,
) -> SchemaDefinition:
    """
    Get the appropriate device schema based on file characteristics.

    Args:
        filename: Name of the device file.
        year: Year of the data (if known).
        column_count: Detected column count from file header.

    Returns:
        Appropriate SchemaDefinition.
    """
    # If year is provided, use it for selection
    if year is not None:
        if year >= 2010:
            return DEVICE_SCHEMA_CURRENT
        elif year >= 1998:
            return DEVICE_SCHEMA_1998_2010
        else:
            return DEVICE_SCHEMA_PRE_1998

    # Try to extract year from filename
    if filename:
        import re
        filename_lower = filename.lower()

        # Check for thru1997 pattern
        if "thru1997" in filename_lower:
            return DEVICE_SCHEMA_PRE_1998

        # Check for year in filename
        match = re.search(r'(\d{4})', filename_lower)
        if match:
            file_year = int(match.group(1))
            if file_year >= 2010:
                return DEVICE_SCHEMA_CURRENT
            elif file_year >= 1998:
                return DEVICE_SCHEMA_1998_2010

        # Check for DEN legacy files
        match = re.search(r'mdr(\d{2})\.', filename_lower)
        if match:
            year_2d = int(match.group(1))
            if 84 <= year_2d <= 97:
                return DEN_LEGACY_SCHEMA

    # Default to current schema
    return DEVICE_SCHEMA_CURRENT


def validate_device_schema(
    detected_columns: List[str],
    schema: SchemaDefinition,
) -> Tuple[bool, str]:
    """
    Validate detected columns against expected schema.

    Args:
        detected_columns: Columns detected from file header.
        schema: Expected schema definition.

    Returns:
        Tuple of (is_valid, message).
    """
    detected_count = len(detected_columns)
    expected_count = schema.column_count

    if detected_count == expected_count:
        return True, f"Column count matches: {detected_count}"

    # Check if it's within acceptable range
    if abs(detected_count - expected_count) <= 2:
        return True, f"Column count close: {detected_count} vs expected {expected_count}"

    return False, f"Column count mismatch: {detected_count} vs expected {expected_count}"


# =============================================================================
# KEY DEVICE FIELDS
# =============================================================================

# Critical fields for manufacturer analysis
DEVICE_KEY_FIELDS = [
    "mdr_report_key",               # Link to master event
    "manufacturer_d_name",          # Device manufacturer (the key data!)
    "device_report_product_code",   # Product classification
    "brand_name",                   # Brand name
    "generic_name",                 # Generic device name
]

# Date fields requiring parsing
DEVICE_DATE_FIELDS = [
    "date_received",
    "expiration_date_of_device",
    "date_returned_to_manufacturer",
]

# Boolean/flag fields
DEVICE_FLAG_FIELDS = [
    "implant_flag",
    "date_removed_flag",
]
