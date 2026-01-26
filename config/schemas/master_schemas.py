"""
FDA MAUDE Master (MDR) File Schema Definitions.

The master file (mdrfoi*.txt) contains core event data but NOT the actual
device manufacturer - that's only in the device file.

CRITICAL: The MANUFACTURER_G1_NAME field in master is the SUBMITTER
(usually a law firm or FDA), NOT the device manufacturer!

Historical Schema Evolution:
- Pre-2024: 84 columns (no MFR_REPORT_TYPE, no REPORTER_STATE_CODE)
- 2024+: 86 columns (added MFR_REPORT_TYPE and REPORTER_STATE_CODE)

Master files:
- mdrfoithru2023.zip (or thru2025.zip): Historical data
- mdrfoi.zip: Current year, updated weekly
- mdrfoiAdd.zip: Weekly new records
- mdrfoiChange.zip: Weekly record updates
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
# CURRENT MASTER SCHEMA (2024+) - 86 columns
# =============================================================================

MASTER_COLUMNS_86: List[str] = [
    "MDR_REPORT_KEY",                    # 1 - Primary key
    "EVENT_KEY",                         # 2
    "REPORT_NUMBER",                     # 3
    "REPORT_SOURCE_CODE",                # 4
    "MANUFACTURER_LINK_FLAG_",           # 5 - Note trailing underscore
    "NUMBER_DEVICES_IN_EVENT",           # 6
    "NUMBER_PATIENTS_IN_EVENT",          # 7
    "DATE_RECEIVED",                     # 8
    "ADVERSE_EVENT_FLAG",                # 9
    "PRODUCT_PROBLEM_FLAG",              # 10
    "DATE_REPORT",                       # 11
    "DATE_OF_EVENT",                     # 12
    "REPROCESSED_AND_REUSED_FLAG",       # 13
    "REPORTER_OCCUPATION_CODE",          # 14
    "HEALTH_PROFESSIONAL",               # 15
    "INITIAL_REPORT_TO_FDA",             # 16
    "DATE_FACILITY_AWARE",               # 17
    "REPORT_DATE",                       # 18
    "REPORT_TO_FDA",                     # 19
    "DATE_REPORT_TO_FDA",                # 20
    "EVENT_LOCATION",                    # 21
    "DATE_REPORT_TO_MANUFACTURER",       # 22
    "MANUFACTURER_CONTACT_T_NAME",       # 23 - Submitter Title
    "MANUFACTURER_CONTACT_F_NAME",       # 24 - Submitter First name
    "MANUFACTURER_CONTACT_L_NAME",       # 25 - Submitter Last name
    "MANUFACTURER_CONTACT_STREET_1",     # 26
    "MANUFACTURER_CONTACT_STREET_2",     # 27
    "MANUFACTURER_CONTACT_CITY",         # 28
    "MANUFACTURER_CONTACT_STATE",        # 29
    "MANUFACTURER_CONTACT_ZIP_CODE",     # 30
    "MANUFACTURER_CONTACT_ZIP_EXT",      # 31
    "MANUFACTURER_CONTACT_COUNTRY",      # 32
    "MANUFACTURER_CONTACT_POSTAL",       # 33
    "MANUFACTURER_CONTACT_AREA_CODE",    # 34
    "MANUFACTURER_CONTACT_EXCHANGE",     # 35
    "MANUFACTURER_CONTACT_PHONE_NO",     # 36
    "MANUFACTURER_CONTACT_EXTENSION",    # 37
    "MANUFACTURER_CONTACT_PCOUNTRY",     # 38
    "MANUFACTURER_CONTACT_PCITY",        # 39
    "MANUFACTURER_CONTACT_PLOCAL",       # 40
    "MANUFACTURER_G1_NAME",              # 41 - SUBMITTER name (NOT device mfr!)
    "MANUFACTURER_G1_STREET_1",          # 42
    "MANUFACTURER_G1_STREET_2",          # 43
    "MANUFACTURER_G1_CITY",              # 44
    "MANUFACTURER_G1_STATE_CODE",        # 45
    "MANUFACTURER_G1_ZIP_CODE",          # 46
    "MANUFACTURER_G1_ZIP_CODE_EXT",      # 47
    "MANUFACTURER_G1_COUNTRY_CODE",      # 48
    "MANUFACTURER_G1_POSTAL_CODE",       # 49
    "DATE_MANUFACTURER_RECEIVED",        # 50
    "MFR_REPORT_TYPE",                   # 51 - NEW in 86-col format
    "DEVICE_DATE_OF_MANUFACTURE",        # 52
    "SINGLE_USE_FLAG",                   # 53
    "REMEDIAL_ACTION",                   # 54
    "PREVIOUS_USE_CODE",                 # 55
    "REMOVAL_CORRECTION_NUMBER",         # 56
    "EVENT_TYPE",                        # 57 - D/I/M/O
    "DISTRIBUTOR_NAME",                  # 58
    "DISTRIBUTOR_ADDRESS_1",             # 59
    "DISTRIBUTOR_ADDRESS_2",             # 60
    "DISTRIBUTOR_CITY",                  # 61
    "DISTRIBUTOR_STATE_CODE",            # 62
    "DISTRIBUTOR_ZIP_CODE",              # 63
    "DISTRIBUTOR_ZIP_CODE_EXT",          # 64
    "REPORT_TO_MANUFACTURER",            # 65
    "MANUFACTURER_NAME",                 # 66 - Often empty!
    "MANUFACTURER_ADDRESS_1",            # 67
    "MANUFACTURER_ADDRESS_2",            # 68
    "MANUFACTURER_CITY",                 # 69
    "MANUFACTURER_STATE_CODE",           # 70
    "MANUFACTURER_ZIP_CODE",             # 71
    "MANUFACTURER_ZIP_CODE_EXT",         # 72
    "MANUFACTURER_COUNTRY_CODE",         # 73
    "MANUFACTURER_POSTAL_CODE",          # 74
    "TYPE_OF_REPORT",                    # 75
    "SOURCE_TYPE",                       # 76
    "DATE_ADDED",                        # 77
    "DATE_CHANGED",                      # 78
    "REPORTER_STATE_CODE",               # 79 - NEW in 86-col format
    "REPORTER_COUNTRY_CODE",             # 80
    "PMA_PMN_NUM",                       # 81
    "EXEMPTION_NUMBER",                  # 82
    "SUMMARY_REPORT",                    # 83
    "NOE_SUMMARIZED",                    # 84
    "SUPPL_DATES_FDA_RECEIVED",          # 85
    "SUPPL_DATES_MFR_RECEIVED",          # 86
]

MASTER_SCHEMA_86_COLUMNS = SchemaDefinition(
    name="master_86_columns",
    columns=MASTER_COLUMNS_86,
    year_start=2024,
    year_end=None,  # Current
    column_count=86,
    notes="Current format with MFR_REPORT_TYPE and REPORTER_STATE_CODE",
)


# =============================================================================
# HISTORICAL MASTER SCHEMA (pre-2024) - 84 columns
# =============================================================================

MASTER_COLUMNS_84: List[str] = [
    "MDR_REPORT_KEY",                    # 1
    "EVENT_KEY",                         # 2
    "REPORT_NUMBER",                     # 3
    "REPORT_SOURCE_CODE",                # 4
    "MANUFACTURER_LINK_FLAG_",           # 5
    "NUMBER_DEVICES_IN_EVENT",           # 6
    "NUMBER_PATIENTS_IN_EVENT",          # 7
    "DATE_RECEIVED",                     # 8
    "ADVERSE_EVENT_FLAG",                # 9
    "PRODUCT_PROBLEM_FLAG",              # 10
    "DATE_REPORT",                       # 11
    "DATE_OF_EVENT",                     # 12
    "REPROCESSED_AND_REUSED_FLAG",       # 13
    "REPORTER_OCCUPATION_CODE",          # 14
    "HEALTH_PROFESSIONAL",               # 15
    "INITIAL_REPORT_TO_FDA",             # 16
    "DATE_FACILITY_AWARE",               # 17
    "REPORT_DATE",                       # 18
    "REPORT_TO_FDA",                     # 19
    "DATE_REPORT_TO_FDA",                # 20
    "EVENT_LOCATION",                    # 21
    "DATE_REPORT_TO_MANUFACTURER",       # 22
    "MANUFACTURER_CONTACT_T_NAME",       # 23
    "MANUFACTURER_CONTACT_F_NAME",       # 24
    "MANUFACTURER_CONTACT_L_NAME",       # 25
    "MANUFACTURER_CONTACT_STREET_1",     # 26
    "MANUFACTURER_CONTACT_STREET_2",     # 27
    "MANUFACTURER_CONTACT_CITY",         # 28
    "MANUFACTURER_CONTACT_STATE",        # 29
    "MANUFACTURER_CONTACT_ZIP_CODE",     # 30
    "MANUFACTURER_CONTACT_ZIP_EXT",      # 31
    "MANUFACTURER_CONTACT_COUNTRY",      # 32
    "MANUFACTURER_CONTACT_POSTAL",       # 33
    "MANUFACTURER_CONTACT_AREA_CODE",    # 34
    "MANUFACTURER_CONTACT_EXCHANGE",     # 35
    "MANUFACTURER_CONTACT_PHONE_NO",     # 36
    "MANUFACTURER_CONTACT_EXTENSION",    # 37
    "MANUFACTURER_CONTACT_PCOUNTRY",     # 38
    "MANUFACTURER_CONTACT_PCITY",        # 39
    "MANUFACTURER_CONTACT_PLOCAL",       # 40
    "MANUFACTURER_G1_NAME",              # 41
    "MANUFACTURER_G1_STREET_1",          # 42
    "MANUFACTURER_G1_STREET_2",          # 43
    "MANUFACTURER_G1_CITY",              # 44
    "MANUFACTURER_G1_STATE_CODE",        # 45
    "MANUFACTURER_G1_ZIP_CODE",          # 46
    "MANUFACTURER_G1_ZIP_CODE_EXT",      # 47
    "MANUFACTURER_G1_COUNTRY_CODE",      # 48
    "MANUFACTURER_G1_POSTAL_CODE",       # 49
    "DATE_MANUFACTURER_RECEIVED",        # 50
    # Note: NO MFR_REPORT_TYPE in 84-col
    "DEVICE_DATE_OF_MANUFACTURE",        # 51
    "SINGLE_USE_FLAG",                   # 52
    "REMEDIAL_ACTION",                   # 53
    "PREVIOUS_USE_CODE",                 # 54
    "REMOVAL_CORRECTION_NUMBER",         # 55
    "EVENT_TYPE",                        # 56
    "DISTRIBUTOR_NAME",                  # 57
    "DISTRIBUTOR_ADDRESS_1",             # 58
    "DISTRIBUTOR_ADDRESS_2",             # 59
    "DISTRIBUTOR_CITY",                  # 60
    "DISTRIBUTOR_STATE_CODE",            # 61
    "DISTRIBUTOR_ZIP_CODE",              # 62
    "DISTRIBUTOR_ZIP_CODE_EXT",          # 63
    "REPORT_TO_MANUFACTURER",            # 64
    "MANUFACTURER_NAME",                 # 65
    "MANUFACTURER_ADDRESS_1",            # 66
    "MANUFACTURER_ADDRESS_2",            # 67
    "MANUFACTURER_CITY",                 # 68
    "MANUFACTURER_STATE_CODE",           # 69
    "MANUFACTURER_ZIP_CODE",             # 70
    "MANUFACTURER_ZIP_CODE_EXT",         # 71
    "MANUFACTURER_COUNTRY_CODE",         # 72
    "MANUFACTURER_POSTAL_CODE",          # 73
    "TYPE_OF_REPORT",                    # 74
    "SOURCE_TYPE",                       # 75
    "DATE_ADDED",                        # 76
    "DATE_CHANGED",                      # 77
    # Note: NO REPORTER_STATE_CODE in 84-col
    "REPORTER_COUNTRY_CODE",             # 78
    "PMA_PMN_NUM",                       # 79
    "EXEMPTION_NUMBER",                  # 80
    "SUMMARY_REPORT",                    # 81
    "NOE_SUMMARIZED",                    # 82
    "SUPPL_DATES_FDA_RECEIVED",          # 83
    "SUPPL_DATES_MFR_RECEIVED",          # 84
]

MASTER_SCHEMA_84_COLUMNS = SchemaDefinition(
    name="master_84_columns",
    columns=MASTER_COLUMNS_84,
    year_start=None,
    year_end=2023,
    column_count=84,
    notes="Historical format without MFR_REPORT_TYPE and REPORTER_STATE_CODE",
)


# =============================================================================
# COLUMN MAPPING: FDA to Database
# =============================================================================

MASTER_FDA_TO_DB_MAPPING: Dict[str, str] = {
    "MDR_REPORT_KEY": "mdr_report_key",
    "EVENT_KEY": "event_key",
    "REPORT_NUMBER": "report_number",
    "REPORT_SOURCE_CODE": "report_source_code",
    "MANUFACTURER_LINK_FLAG_": "manufacturer_link_flag_old",
    "NUMBER_DEVICES_IN_EVENT": "number_devices_in_event",
    "NUMBER_PATIENTS_IN_EVENT": "number_patients_in_event",
    "DATE_RECEIVED": "date_received",
    "ADVERSE_EVENT_FLAG": "adverse_event_flag",
    "PRODUCT_PROBLEM_FLAG": "product_problem_flag",
    "DATE_REPORT": "date_report",
    "DATE_OF_EVENT": "date_of_event",
    "REPROCESSED_AND_REUSED_FLAG": "reprocessed_and_reused_flag",
    "REPORTER_OCCUPATION_CODE": "reporter_occupation_code",
    "HEALTH_PROFESSIONAL": "health_professional",
    "INITIAL_REPORT_TO_FDA": "initial_report_to_fda",
    "DATE_FACILITY_AWARE": "date_facility_aware",
    "REPORT_DATE": "report_date",
    "REPORT_TO_FDA": "report_to_fda",
    "DATE_REPORT_TO_FDA": "date_report_to_fda",
    "EVENT_LOCATION": "event_location",
    "EVENT_TYPE": "event_type",
    "DATE_REPORT_TO_MANUFACTURER": "date_report_to_manufacturer",
    "REPORT_TO_MANUFACTURER": "report_to_manufacturer",
    "DATE_MANUFACTURER_RECEIVED": "date_manufacturer_received",
    "MANUFACTURER_CONTACT_T_NAME": "manufacturer_contact_title",
    "MANUFACTURER_CONTACT_F_NAME": "manufacturer_contact_first_name",
    "MANUFACTURER_CONTACT_L_NAME": "manufacturer_contact_last_name",
    "MANUFACTURER_CONTACT_STREET_1": "manufacturer_contact_address_1",
    "MANUFACTURER_CONTACT_STREET_2": "manufacturer_contact_address_2",
    "MANUFACTURER_CONTACT_CITY": "manufacturer_contact_city",
    "MANUFACTURER_CONTACT_STATE": "manufacturer_contact_state",
    "MANUFACTURER_CONTACT_ZIP_CODE": "manufacturer_contact_zip",
    "MANUFACTURER_CONTACT_ZIP_EXT": "manufacturer_contact_zip_ext",
    "MANUFACTURER_CONTACT_COUNTRY": "manufacturer_contact_country",
    "MANUFACTURER_CONTACT_POSTAL": "manufacturer_contact_postal",
    "MANUFACTURER_CONTACT_AREA_CODE": "manufacturer_contact_area_code",
    "MANUFACTURER_CONTACT_EXCHANGE": "manufacturer_contact_exchange",
    "MANUFACTURER_CONTACT_PHONE_NO": "manufacturer_contact_phone_no",
    "MANUFACTURER_CONTACT_EXTENSION": "manufacturer_contact_extension",
    "MANUFACTURER_CONTACT_PCOUNTRY": "manufacturer_contact_pcountry",
    "MANUFACTURER_CONTACT_PCITY": "manufacturer_contact_pcity",
    "MANUFACTURER_CONTACT_PLOCAL": "manufacturer_contact_plocal",
    "MANUFACTURER_G1_NAME": "manufacturer_g1_name",
    "MANUFACTURER_G1_STREET_1": "manufacturer_g1_street_1",
    "MANUFACTURER_G1_STREET_2": "manufacturer_g1_street_2",
    "MANUFACTURER_G1_CITY": "manufacturer_g1_city",
    "MANUFACTURER_G1_STATE_CODE": "manufacturer_g1_state",
    "MANUFACTURER_G1_ZIP_CODE": "manufacturer_g1_zip",
    "MANUFACTURER_G1_ZIP_CODE_EXT": "manufacturer_g1_zip_ext",
    "MANUFACTURER_G1_COUNTRY_CODE": "manufacturer_g1_country",
    "MANUFACTURER_G1_POSTAL_CODE": "manufacturer_g1_postal",
    "MFR_REPORT_TYPE": "mfr_report_type",
    "DEVICE_DATE_OF_MANUFACTURE": "device_date_of_manufacture",
    "SINGLE_USE_FLAG": "single_use_flag",
    "REMEDIAL_ACTION": "remedial_action",
    "PREVIOUS_USE_CODE": "previous_use_code",
    "REMOVAL_CORRECTION_NUMBER": "removal_correction_number",
    "MANUFACTURER_LINK_FLAG": "manufacturer_link_flag",
    "DISTRIBUTOR_NAME": "distributor_name",
    "DISTRIBUTOR_ADDRESS_1": "distributor_address_1",
    "DISTRIBUTOR_ADDRESS_2": "distributor_address_2",
    "DISTRIBUTOR_CITY": "distributor_city",
    "DISTRIBUTOR_STATE_CODE": "distributor_state",
    "DISTRIBUTOR_ZIP_CODE": "distributor_zip",
    "DISTRIBUTOR_ZIP_CODE_EXT": "distributor_zip_ext",
    "MANUFACTURER_NAME": "manufacturer_name",
    "MANUFACTURER_ADDRESS_1": "manufacturer_address_1",
    "MANUFACTURER_ADDRESS_2": "manufacturer_address_2",
    "MANUFACTURER_CITY": "manufacturer_city",
    "MANUFACTURER_STATE_CODE": "manufacturer_state",
    "MANUFACTURER_ZIP_CODE": "manufacturer_zip",
    "MANUFACTURER_ZIP_CODE_EXT": "manufacturer_zip_ext",
    "MANUFACTURER_COUNTRY_CODE": "manufacturer_country",
    "MANUFACTURER_POSTAL_CODE": "manufacturer_postal",
    "TYPE_OF_REPORT": "type_of_report",
    "SOURCE_TYPE": "source_type",
    "DATE_ADDED": "date_added",
    "DATE_CHANGED": "date_changed",
    "REPORTER_STATE_CODE": "reporter_state_code",
    "REPORTER_COUNTRY_CODE": "reporter_country_code",
    "PMA_PMN_NUM": "pma_pmn_number",
    "EXEMPTION_NUMBER": "exemption_number",
    "SUMMARY_REPORT": "summary_report_flag",
    "NOE_SUMMARIZED": "noe_summarized",
    "SUPPL_DATES_FDA_RECEIVED": "supplemental_dates_fda_received",
    "SUPPL_DATES_MFR_RECEIVED": "supplemental_dates_mfr_received",
}


# =============================================================================
# SCHEMA DETECTION AND SELECTION
# =============================================================================

def get_master_schema(
    filename: str = None,
    year: int = None,
    column_count: int = None,
) -> SchemaDefinition:
    """
    Get the appropriate master schema based on file characteristics.

    Args:
        filename: Name of the master file.
        year: Year of the data (if known).
        column_count: Detected column count from file header.

    Returns:
        Appropriate SchemaDefinition.
    """
    # If column count is known, use it for selection
    if column_count is not None:
        if column_count == 86:
            return MASTER_SCHEMA_86_COLUMNS
        elif column_count == 84:
            return MASTER_SCHEMA_84_COLUMNS
        # Close enough - try to match
        elif column_count >= 85:
            return MASTER_SCHEMA_86_COLUMNS
        else:
            return MASTER_SCHEMA_84_COLUMNS

    # If year is provided, use it
    if year is not None:
        if year >= 2024:
            return MASTER_SCHEMA_86_COLUMNS
        else:
            return MASTER_SCHEMA_84_COLUMNS

    # Try to extract year from filename
    if filename:
        import re
        filename_lower = filename.lower()

        # mdrfoithru2023 -> 84 columns
        match = re.search(r'thru(\d{4})', filename_lower)
        if match:
            thru_year = int(match.group(1))
            if thru_year < 2024:
                return MASTER_SCHEMA_84_COLUMNS

        # Current year file - use latest schema
        if "mdrfoi.txt" in filename_lower or "mdrfoiadd" in filename_lower:
            return MASTER_SCHEMA_86_COLUMNS

    # Default to latest
    return MASTER_SCHEMA_86_COLUMNS


def validate_master_schema(
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

    # Master files can have 84 or 86 - both are valid
    if detected_count in (84, 86):
        return True, f"Valid master column count: {detected_count}"

    return False, f"Column count mismatch: {detected_count} vs expected {expected_count}"


# =============================================================================
# KEY MASTER FIELDS
# =============================================================================

# Critical fields for event analysis
MASTER_KEY_FIELDS = [
    "mdr_report_key",           # Primary key
    "date_received",            # Event date
    "event_type",               # D/I/M/O classification
    "adverse_event_flag",       # Is this an adverse event
    "product_problem_flag",     # Is this a product problem
]

# Date fields requiring parsing
MASTER_DATE_FIELDS = [
    "date_received",
    "date_report",
    "date_of_event",
    "date_facility_aware",
    "report_date",
    "date_report_to_fda",
    "date_report_to_manufacturer",
    "date_manufacturer_received",
    "device_date_of_manufacture",
    "date_added",
    "date_changed",
]

# Boolean/flag fields
MASTER_FLAG_FIELDS = [
    "adverse_event_flag",
    "product_problem_flag",
    "reprocessed_and_reused_flag",
    "health_professional",
    "initial_report_to_fda",
    "report_to_fda",
    "report_to_manufacturer",
    "single_use_flag",
    "summary_report_flag",
    "noe_summarized",
]
