"""
FDA MAUDE Device File Schema Definitions.

Device files contain the actual manufacturer and product code information
that is NOT present in the master (MDR) file. The master file's
MANUFACTURER_G1_NAME is the SUBMITTER, not the device manufacturer.

Historical Schema Evolution:
- 1984-1997: DEN Legacy format (different structure entirely)
- 1998-2009: Transition period (28 columns, some variations)
- 2010-2019: Modern format (28 columns, stable) - foidev{year}.zip
- 2020-present: Extended format (34 columns) - device{year}.zip
  Added: IMPLANT_DATE_YEAR, DATE_REMOVED_YEAR, SERVICED_BY_3RD_PARTY_FLAG,
         COMBINATION_PRODUCT_FLAG, UDI-DI, UDI-PUBLIC

Device files:
- foidevthru1997.zip: Pre-1998 historical data
- foidev1998.zip - foidev2019.zip: Annual device files (28 columns)
- device2020.zip - device2025.zip: Recent annual files (34 columns - NEW FORMAT)
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

DEVICE_SCHEMA_2010_2019 = SchemaDefinition(
    name="device_2010_2019",
    columns=DEVICE_COLUMNS_CURRENT,
    year_start=2010,
    year_end=2019,
    column_count=28,
    notes="Modern device schema for foidev files (28 columns)",
)

# Alias for backwards compatibility
DEVICE_SCHEMA_CURRENT = DEVICE_SCHEMA_2010_2019


# =============================================================================
# EXTENDED DEVICE SCHEMA (2020-present) - 34 columns
# =============================================================================

# Starting in 2020, FDA added 6 new columns to device files:
# - IMPLANT_DATE_YEAR, DATE_REMOVED_YEAR, SERVICED_BY_3RD_PARTY_FLAG after col 5
# - COMBINATION_PRODUCT_FLAG, UDI-DI, UDI-PUBLIC at the end

DEVICE_COLUMNS_2020_PLUS: List[str] = [
    "MDR_REPORT_KEY",                       # 1 - Foreign key to master
    "DEVICE_EVENT_KEY",                     # 2 - Unique device event key
    "IMPLANT_FLAG",                         # 3 - Y/N/blank
    "DATE_REMOVED_FLAG",                    # 4 - Y/N/blank
    "DEVICE_SEQUENCE_NO",                   # 5 - Sequence number (integer)
    "IMPLANT_DATE_YEAR",                    # 6 - NEW in 2020
    "DATE_REMOVED_YEAR",                    # 7 - NEW in 2020
    "SERVICED_BY_3RD_PARTY_FLAG",           # 8 - NEW in 2020
    "DATE_RECEIVED",                        # 9 - Date (was col 6 in old format)
    "BRAND_NAME",                           # 10 - Product brand name
    "GENERIC_NAME",                         # 11 - Generic device name
    "MANUFACTURER_D_NAME",                  # 12 - ACTUAL device manufacturer
    "MANUFACTURER_D_ADDRESS_1",             # 13
    "MANUFACTURER_D_ADDRESS_2",             # 14
    "MANUFACTURER_D_CITY",                  # 15
    "MANUFACTURER_D_STATE_CODE",            # 16
    "MANUFACTURER_D_ZIP_CODE",              # 17
    "MANUFACTURER_D_ZIP_CODE_EXT",          # 18
    "MANUFACTURER_D_COUNTRY_CODE",          # 19
    "MANUFACTURER_D_POSTAL_CODE",           # 20
    "DEVICE_OPERATOR",                      # 21 - Who was using device
    "EXPIRATION_DATE_OF_DEVICE",            # 22 - Date
    "MODEL_NUMBER",                         # 23
    "CATALOG_NUMBER",                       # 24
    "LOT_NUMBER",                           # 25
    "OTHER_ID_NUMBER",                      # 26
    "DEVICE_AVAILABILITY",                  # 27 - For evaluation status
    "DATE_RETURNED_TO_MANUFACTURER",        # 28 - Date
    "DEVICE_REPORT_PRODUCT_CODE",           # 29 - FDA product classification code
    "DEVICE_AGE_TEXT",                      # 30 - Age/use duration text
    "DEVICE_EVALUATED_BY_MANUFACTUR",       # 31 - Note: truncated in FDA
    "COMBINATION_PRODUCT_FLAG",             # 32 - NEW in 2020
    "UDI_DI",                               # 33 - NEW in 2020: Unique Device Identifier - Device Identifier
    "UDI_PUBLIC",                           # 34 - NEW in 2020: Unique Device Identifier - Public
]

DEVICE_SCHEMA_2020_PLUS = SchemaDefinition(
    name="device_2020_plus",
    columns=DEVICE_COLUMNS_2020_PLUS,
    year_start=2020,
    year_end=None,  # Current
    column_count=34,
    notes="Extended device schema for device{year}.zip files (34 columns, added UDI and extra fields)",
)


# =============================================================================
# LEGACY DEVICE SCHEMA (1997-2008) - 45 columns with BASELINE_* fields
# =============================================================================

# IMPORTANT: Files from 1997-2008 have 45 columns, NOT 28!
# The extra 17 columns are BASELINE_* fields that were removed in 2009.
# These files: foidevthru1997.zip, foidev1998.zip - foidev2008.zip

DEVICE_COLUMNS_LEGACY_45: List[str] = [
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
    # BASELINE_* columns (legacy, removed in 2009)
    "BASELINE_BRAND_NAME",                  # 29
    "BASELINE_GENERIC_NAME",                # 30
    "BASELINE_MODEL_NO",                    # 31
    "BASELINE_CATALOG_NO",                  # 32
    "BASELINE_OTHER_ID_NO",                 # 33
    "BASELINE_DEVICE_FAMILY",               # 34
    "BASELINE_SHELF_LIFE_CONTAINED",        # 35
    "BASELINE_SHELF_LIFE_IN_MONTHS",        # 36
    "BASELINE_PMA_FLAG",                    # 37
    "BASELINE_PMA_NO",                      # 38
    "BASELINE_510_K__FLAG",                 # 39
    "BASELINE_510_K__NO",                   # 40
    "BASELINE_PREAMENDMENT",                # 41
    "BASELINE_TRANSITIONAL",                # 42
    "BASELINE_510_K__EXEMPT_FLAG",          # 43
    "BASELINE_DATE_FIRST_MARKETED",         # 44
    "BASELINE_DATE_CEASED_MARKETING",       # 45
]

DEVICE_SCHEMA_LEGACY_45 = SchemaDefinition(
    name="device_legacy_45",
    columns=DEVICE_COLUMNS_LEGACY_45,
    year_start=1984,
    year_end=2008,
    column_count=45,
    encoding="latin-1",
    notes="Legacy 45-column format with BASELINE_* fields (1997-2008)",
)

# Alias for backwards compatibility
DEVICE_SCHEMA_1998_2010 = DEVICE_SCHEMA_LEGACY_45
DEVICE_SCHEMA_PRE_1998 = DEVICE_SCHEMA_LEGACY_45


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
    # New columns in 2020+ format
    "IMPLANT_DATE_YEAR": "implant_date_year",
    "DATE_REMOVED_YEAR": "date_removed_year",
    "SERVICED_BY_3RD_PARTY_FLAG": "serviced_by_3rd_party_flag",
    # Core columns (continue from both formats)
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
    # New columns at end in 2020+ format
    "COMBINATION_PRODUCT_FLAG": "combination_product_flag",
    "UDI_DI": "udi_di",
    "UDI-DI": "udi_di",  # Handle hyphenated version
    "UDI_PUBLIC": "udi_public",
    "UDI-PUBLIC": "udi_public",  # Handle hyphenated version
    # Legacy BASELINE_* columns (1997-2008 files, 45 columns)
    # These are parsed but not stored in the database
    "BASELINE_BRAND_NAME": "baseline_brand_name",
    "BASELINE_GENERIC_NAME": "baseline_generic_name",
    "BASELINE_MODEL_NO": "baseline_model_no",
    "BASELINE_CATALOG_NO": "baseline_catalog_no",
    "BASELINE_OTHER_ID_NO": "baseline_other_id_no",
    "BASELINE_DEVICE_FAMILY": "baseline_device_family",
    "BASELINE_SHELF_LIFE_CONTAINED": "baseline_shelf_life_contained",
    "BASELINE_SHELF_LIFE_IN_MONTHS": "baseline_shelf_life_in_months",
    "BASELINE_PMA_FLAG": "baseline_pma_flag",
    "BASELINE_PMA_NO": "baseline_pma_no",
    "BASELINE_510_K__FLAG": "baseline_510k_flag",
    "BASELINE_510_K__NO": "baseline_510k_no",
    "BASELINE_PREAMENDMENT": "baseline_preamendment",
    "BASELINE_TRANSITIONAL": "baseline_transitional",
    "BASELINE_510_K__EXEMPT_FLAG": "baseline_510k_exempt_flag",
    "BASELINE_DATE_FIRST_MARKETED": "baseline_date_first_marketed",
    "BASELINE_DATE_CEASED_MARKETING": "baseline_date_ceased_marketing",
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
    import re

    # If column_count is provided, use it as primary indicator
    # This is the most reliable way to detect schema
    if column_count is not None:
        if column_count >= 45:
            # Legacy 45-column format (1997-2008)
            return DEVICE_SCHEMA_LEGACY_45
        elif column_count >= 34:
            return DEVICE_SCHEMA_2020_PLUS
        elif column_count >= 28:
            # 2009-2019 format (28 columns)
            return DEVICE_SCHEMA_2010_2019
        elif column_count <= 19:
            return DEN_LEGACY_SCHEMA

    # If year is provided, use it for selection
    if year is not None:
        if year >= 2020:
            return DEVICE_SCHEMA_2020_PLUS
        elif year >= 2010:
            return DEVICE_SCHEMA_2010_2019
        elif year >= 1998:
            return DEVICE_SCHEMA_1998_2010
        else:
            return DEVICE_SCHEMA_PRE_1998

    # Try to extract year from filename
    if filename:
        filename_lower = filename.lower()

        # Check for thru1997 pattern
        if "thru1997" in filename_lower:
            return DEVICE_SCHEMA_PRE_1998

        # Check for device{year}.txt pattern (2020+ format)
        # These files start with "device" not "foidev"
        if filename_lower.startswith("device") and not filename_lower.startswith("devicechange"):
            match = re.search(r'device(\d{4})', filename_lower)
            if match:
                file_year = int(match.group(1))
                if file_year >= 2020:
                    return DEVICE_SCHEMA_2020_PLUS

        # Check for foidev{year}.txt pattern (pre-2020 format)
        if filename_lower.startswith("foidev"):
            match = re.search(r'foidev(\d{4})', filename_lower)
            if match:
                file_year = int(match.group(1))
                if file_year >= 2010:
                    return DEVICE_SCHEMA_2010_2019
                elif file_year >= 1998:
                    return DEVICE_SCHEMA_1998_2010

        # Generic year extraction as fallback
        match = re.search(r'(\d{4})', filename_lower)
        if match:
            file_year = int(match.group(1))
            if file_year >= 2020:
                return DEVICE_SCHEMA_2020_PLUS
            elif file_year >= 2010:
                return DEVICE_SCHEMA_2010_2019
            elif file_year >= 1998:
                return DEVICE_SCHEMA_1998_2010

        # Check for DEN legacy files
        match = re.search(r'mdr(\d{2})\.', filename_lower)
        if match:
            year_2d = int(match.group(1))
            if 84 <= year_2d <= 97:
                return DEN_LEGACY_SCHEMA

    # Default to 2010-2019 schema (28 columns) for unknown files
    # This is safer than assuming 34-column format
    return DEVICE_SCHEMA_2010_2019


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
