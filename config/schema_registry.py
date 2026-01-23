"""
FDA MAUDE Schema Registry - Actual column definitions from FDA files.

This module defines the EXACT column structures as they appear in FDA MAUDE
data files. These were derived from analyzing actual FDA download files.

The FDA MAUDE database includes:
- Master (MDR) file: 86 columns (current) / 84 columns (historical thru 2023)
- Device file: 28 columns - Device information
- Patient file: 10 columns - Patient outcomes and demographics
- Text file: 6 columns - Narrative descriptions
- Problem file: 2 columns - Device problem codes (no header)

IMPORTANT: The columns are listed in the EXACT ORDER they appear in FDA files.
Column positions matter for correct parsing.
"""

from typing import List, Dict, Tuple

# =============================================================================
# FDA MASTER FILE COLUMNS (86 columns - current format 2024+)
# File: mdrfoi.txt, mdrfoiAdd.txt
# =============================================================================

MASTER_COLUMNS_FDA_86: List[str] = [
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
    "MANUFACTURER_CONTACT_T_NAME",       # 23 - Title
    "MANUFACTURER_CONTACT_F_NAME",       # 24 - First name
    "MANUFACTURER_CONTACT_L_NAME",       # 25 - Last name
    "MANUFACTURER_CONTACT_STREET_1",     # 26
    "MANUFACTURER_CONTACT_STREET_2",     # 27
    "MANUFACTURER_CONTACT_CITY",         # 28
    "MANUFACTURER_CONTACT_STATE",        # 29
    "MANUFACTURER_CONTACT_ZIP_CODE",     # 30
    "MANUFACTURER_CONTACT_ZIP_EXT",      # 31
    "MANUFACTURER_CONTACT_COUNTRY",      # 32
    "MANUFACTURER_CONTACT_POSTAL",       # 33 - Note: not POSTAL_CODE
    "MANUFACTURER_CONTACT_AREA_CODE",    # 34 - Phone area code
    "MANUFACTURER_CONTACT_EXCHANGE",     # 35 - Phone exchange
    "MANUFACTURER_CONTACT_PHONE_NO",     # 36 - Phone number
    "MANUFACTURER_CONTACT_EXTENSION",    # 37 - Phone extension
    "MANUFACTURER_CONTACT_PCOUNTRY",     # 38 - Phone country code
    "MANUFACTURER_CONTACT_PCITY",        # 39 - Phone city code
    "MANUFACTURER_CONTACT_PLOCAL",       # 40 - Phone local
    "MANUFACTURER_G1_NAME",              # 41 - Global manufacturer
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
    "EVENT_TYPE",                        # 57
    "DISTRIBUTOR_NAME",                  # 58
    "DISTRIBUTOR_ADDRESS_1",             # 59
    "DISTRIBUTOR_ADDRESS_2",             # 60
    "DISTRIBUTOR_CITY",                  # 61
    "DISTRIBUTOR_STATE_CODE",            # 62
    "DISTRIBUTOR_ZIP_CODE",              # 63
    "DISTRIBUTOR_ZIP_CODE_EXT",          # 64
    "REPORT_TO_MANUFACTURER",            # 65
    "MANUFACTURER_NAME",                 # 66
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
    "PMA_PMN_NUM",                       # 81 - Note: not PMA_PMN_NUMBER
    "EXEMPTION_NUMBER",                  # 82
    "SUMMARY_REPORT",                    # 83 - Note: not SUMMARY_REPORT_FLAG
    "NOE_SUMMARIZED",                    # 84
    "SUPPL_DATES_FDA_RECEIVED",          # 85 - Note: abbreviated
    "SUPPL_DATES_MFR_RECEIVED",          # 86 - Note: abbreviated
]

# =============================================================================
# FDA MASTER FILE COLUMNS (84 columns - historical format thru 2023)
# File: mdrfoiThru2023.txt and earlier
# =============================================================================

MASTER_COLUMNS_FDA_84: List[str] = [
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
    # Note: 84-col format does NOT have MFR_REPORT_TYPE here
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
    # Note: 84-col format does NOT have REPORTER_STATE_CODE here
    "REPORTER_COUNTRY_CODE",             # 78
    "PMA_PMN_NUM",                       # 79
    "EXEMPTION_NUMBER",                  # 80
    "SUMMARY_REPORT",                    # 81
    "NOE_SUMMARIZED",                    # 82
    "SUPPL_DATES_FDA_RECEIVED",          # 83
    "SUPPL_DATES_MFR_RECEIVED",          # 84
]

# Default to 86-column format (current)
MASTER_COLUMNS_FDA = MASTER_COLUMNS_FDA_86

# =============================================================================
# FDA DEVICE FILE COLUMNS (28 columns)
# File: foidevthru{year}.txt, foidev.txt
# =============================================================================

DEVICE_COLUMNS_FDA: List[str] = [
    "MDR_REPORT_KEY",                       # 1 - Foreign key to master
    "DEVICE_EVENT_KEY",                     # 2 - Unique device event key
    "IMPLANT_FLAG",                         # 3
    "DATE_REMOVED_FLAG",                    # 4
    "DEVICE_SEQUENCE_NO",                   # 5 - Note: NO not NUMBER
    "DATE_RECEIVED",                        # 6
    "BRAND_NAME",                           # 7
    "GENERIC_NAME",                         # 8
    "MANUFACTURER_D_NAME",                  # 9 - Device manufacturer
    "MANUFACTURER_D_ADDRESS_1",             # 10
    "MANUFACTURER_D_ADDRESS_2",             # 11
    "MANUFACTURER_D_CITY",                  # 12
    "MANUFACTURER_D_STATE_CODE",            # 13
    "MANUFACTURER_D_ZIP_CODE",              # 14
    "MANUFACTURER_D_ZIP_CODE_EXT",          # 15
    "MANUFACTURER_D_COUNTRY_CODE",          # 16
    "MANUFACTURER_D_POSTAL_CODE",           # 17
    "EXPIRATION_DATE_OF_DEVICE",            # 18
    "MODEL_NUMBER",                         # 19
    "CATALOG_NUMBER",                       # 20
    "LOT_NUMBER",                           # 21
    "OTHER_ID_NUMBER",                      # 22
    "DEVICE_OPERATOR",                      # 23
    "DEVICE_AVAILABILITY",                  # 24
    "DATE_RETURNED_TO_MANUFACTURER",        # 25
    "DEVICE_REPORT_PRODUCT_CODE",           # 26 - Product code for filtering
    "DEVICE_AGE_TEXT",                      # 27
    "DEVICE_EVALUATED_BY_MANUFACTUR",       # 28 - Note: truncated in FDA
]

# =============================================================================
# FDA PATIENT FILE COLUMNS (10 columns)
# File: patientthru{year}.txt, patient.txt
# =============================================================================

PATIENT_COLUMNS_FDA: List[str] = [
    "MDR_REPORT_KEY",              # 1 - Foreign key to master
    "PATIENT_SEQUENCE_NUMBER",     # 2
    "DATE_RECEIVED",               # 3
    "SEQUENCE_NUMBER_TREATMENT",   # 4
    "SEQUENCE_NUMBER_OUTCOME",     # 5
    "PATIENT_AGE",                 # 6
    "PATIENT_SEX",                 # 7
    "PATIENT_WEIGHT",              # 8
    "PATIENT_ETHNICITY",           # 9
    "PATIENT_RACE",                # 10
]

# =============================================================================
# FDA TEXT FILE COLUMNS (6 columns)
# File: foitextthru{year}.txt, foitext.txt
# =============================================================================

TEXT_COLUMNS_FDA: List[str] = [
    "MDR_REPORT_KEY",              # 1 - Foreign key to master
    "MDR_TEXT_KEY",                # 2 - Unique text key
    "TEXT_TYPE_CODE",              # 3 - D, H5, H6, UF
    "PATIENT_SEQUENCE_NUMBER",     # 4
    "DATE_REPORT",                 # 5
    "FOI_TEXT",                    # 6 - The actual narrative text
]

# =============================================================================
# FDA PROBLEM FILE COLUMNS (2 columns) - NO HEADER IN FILE
# File: foidevproblem.txt
# =============================================================================

PROBLEM_COLUMNS_FDA: List[str] = [
    "MDR_REPORT_KEY",              # 1 - Foreign key to master
    "DEVICE_PROBLEM_CODE",         # 2 - Problem code
]

# =============================================================================
# FDA PATIENT PROBLEM FILE COLUMNS (2 columns) - NO HEADER IN FILE
# File: patientproblemcode.txt
# =============================================================================

PATIENT_PROBLEM_COLUMNS_FDA: List[str] = [
    "MDR_REPORT_KEY",              # 1 - Foreign key to master
    "PATIENT_PROBLEM_CODE",        # 2 - Patient problem code
]

# =============================================================================
# FDA PATIENT PROBLEM DATA LOOKUP COLUMNS
# File: patientproblemdata.txt - Problem code descriptions
# =============================================================================

PATIENT_PROBLEM_DATA_COLUMNS_FDA: List[str] = [
    "PROBLEM_CODE",                # 1 - Problem code
    "PROBLEM_DESCRIPTION",         # 2 - Description
]

# =============================================================================
# FDA DEVICE PROBLEM CODES LOOKUP COLUMNS
# File: deviceproblemcodes.txt - Problem code descriptions
# =============================================================================

PROBLEM_CODES_LOOKUP_COLUMNS_FDA: List[str] = [
    "DEVICE_PROBLEM_CODE",         # 1 - Problem code
    "DEVICE_PROBLEM_DESCRIPTION",  # 2 - Description
]

# =============================================================================
# ASR (Alternative Summary Report) COLUMNS - CSV FORMAT
# File: ASR_{year}.zip (1999-2019)
# Note: ASR files are CSV format, not pipe-delimited
# =============================================================================

ASR_COLUMNS_FDA: List[str] = [
    "REPORT_ID",                   # 1 - Unique report identifier
    "REPORT_YEAR",                 # 2 - Year of report
    "BRAND_NAME",                  # 3
    "GENERIC_NAME",                # 4
    "MANUFACTURER_NAME",           # 5
    "PRODUCT_CODE",                # 6
    "DEVICE_CLASS",                # 7
    "REPORT_COUNT",                # 8 - Number of reports
    "EVENT_COUNT",                 # 9 - Number of events
    "DEATH_COUNT",                 # 10
    "INJURY_COUNT",                # 11
    "MALFUNCTION_COUNT",           # 12
    "DATE_START",                  # 13 - Report period start
    "DATE_END",                    # 14 - Report period end
    "EXEMPTION_NUMBER",            # 15
    "PMA_PMN_NUMBER",              # 16
    "SUBMISSION_TYPE",             # 17
    "SUMMARY_TEXT",                # 18 - Narrative summary
]

# =============================================================================
# ASR Patient Problem Codes COLUMNS
# File: ASR_PPC.zip
# =============================================================================

ASR_PPC_COLUMNS_FDA: List[str] = [
    "REPORT_ID",                   # 1 - Foreign key to ASR report
    "PATIENT_PROBLEM_CODE",        # 2 - Problem code
    "OCCURRENCE_COUNT",            # 3 - Count of occurrences
]

# =============================================================================
# DEN (Device Experience Network) LEGACY COLUMNS (1984-1997)
# File: mdr84.zip - mdr97.zip
# Note: Legacy format, columns may vary by year
# =============================================================================

DEN_COLUMNS_FDA: List[str] = [
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

# =============================================================================
# MANUFACTURER DISCLAIMERS COLUMNS
# File: disclaim.zip
# =============================================================================

DISCLAIMER_COLUMNS_FDA: List[str] = [
    "MANUFACTURER_NAME",           # 1
    "DISCLAIMER_TEXT",             # 2
    "EFFECTIVE_DATE",              # 3
]

# =============================================================================
# Files that do NOT have headers (need to use predefined columns)
# =============================================================================

HEADERLESS_FILES: Dict[str, List[str]] = {
    "problem": PROBLEM_COLUMNS_FDA,
    "patient_problem": PATIENT_PROBLEM_COLUMNS_FDA,
    "problem_lookup": PROBLEM_CODES_LOOKUP_COLUMNS_FDA,
}

# =============================================================================
# Map file type to FDA columns (default/current format)
# =============================================================================

FDA_FILE_COLUMNS: Dict[str, List[str]] = {
    "master": MASTER_COLUMNS_FDA,
    "device": DEVICE_COLUMNS_FDA,
    "patient": PATIENT_COLUMNS_FDA,
    "text": TEXT_COLUMNS_FDA,
    "problem": PROBLEM_COLUMNS_FDA,
    "patient_problem": PATIENT_PROBLEM_COLUMNS_FDA,
    "patient_problem_data": PATIENT_PROBLEM_DATA_COLUMNS_FDA,
    "problem_lookup": PROBLEM_CODES_LOOKUP_COLUMNS_FDA,
    "asr": ASR_COLUMNS_FDA,
    "asr_ppc": ASR_PPC_COLUMNS_FDA,
    "den": DEN_COLUMNS_FDA,
    "disclaimer": DISCLAIMER_COLUMNS_FDA,
}

# =============================================================================
# Expected column counts for validation
# Note: Master files can have 84 or 86 columns depending on year
# =============================================================================

EXPECTED_COLUMN_COUNTS: Dict[str, int] = {
    "master": 86,  # Current format
    "device": 28,
    "patient": 10,
    "text": 6,
    "problem": 2,
    "patient_problem": 2,
    "patient_problem_data": 2,
    "problem_lookup": 2,
    "asr": 18,
    "asr_ppc": 3,
    "den": 19,  # May vary by year
    "disclaimer": 3,
}

# Alternative column counts for historical files
ALTERNATIVE_COLUMN_COUNTS: Dict[str, List[int]] = {
    "master": [84, 86],  # Historical vs current
    "device": [28],
    "patient": [10],
    "text": [6],
    "problem": [2],
    "patient_problem": [2],
    "patient_problem_data": [2],
    "problem_lookup": [2],
    "asr": [18],  # May have variations
    "asr_ppc": [3],
    "den": [15, 17, 19],  # DEN format evolved over years
    "disclaimer": [2, 3],
}

# =============================================================================
# Date columns by file type (for transformation)
# =============================================================================

DATE_COLUMNS: Dict[str, List[str]] = {
    "master": [
        "DATE_RECEIVED",
        "DATE_REPORT",
        "DATE_OF_EVENT",
        "DATE_FACILITY_AWARE",
        "REPORT_DATE",
        "DATE_REPORT_TO_FDA",
        "DATE_REPORT_TO_MANUFACTURER",
        "DATE_MANUFACTURER_RECEIVED",
        "DEVICE_DATE_OF_MANUFACTURE",
        "DATE_ADDED",
        "DATE_CHANGED",
    ],
    "device": [
        "DATE_RECEIVED",
        "EXPIRATION_DATE_OF_DEVICE",
        "DATE_RETURNED_TO_MANUFACTURER",
    ],
    "patient": [
        "DATE_RECEIVED",
    ],
    "text": [
        "DATE_REPORT",
    ],
    "problem": [],
}

# =============================================================================
# Integer columns by file type (for transformation)
# =============================================================================

INTEGER_COLUMNS: Dict[str, List[str]] = {
    "master": [
        "NUMBER_DEVICES_IN_EVENT",
        "NUMBER_PATIENTS_IN_EVENT",
    ],
    "device": [
        "DEVICE_SEQUENCE_NO",
    ],
    "patient": [
        "PATIENT_SEQUENCE_NUMBER",
        "SEQUENCE_NUMBER_TREATMENT",
        "SEQUENCE_NUMBER_OUTCOME",
    ],
    "text": [
        "PATIENT_SEQUENCE_NUMBER",
    ],
    "problem": [],
}

# =============================================================================
# Flag/boolean columns (Y/N values)
# =============================================================================

FLAG_COLUMNS: Dict[str, List[str]] = {
    "master": [
        "ADVERSE_EVENT_FLAG",
        "PRODUCT_PROBLEM_FLAG",
        "REPROCESSED_AND_REUSED_FLAG",
        "HEALTH_PROFESSIONAL",
        "INITIAL_REPORT_TO_FDA",
        "REPORT_TO_FDA",
        "REPORT_TO_MANUFACTURER",
        "SINGLE_USE_FLAG",
        "MANUFACTURER_LINK_FLAG_",
        "SUMMARY_REPORT",
        "NOE_SUMMARIZED",
    ],
    "device": [
        "IMPLANT_FLAG",
        "DATE_REMOVED_FLAG",
    ],
    "patient": [],
    "text": [],
    "problem": [],
}

# =============================================================================
# Primary key columns by file type
# =============================================================================

PRIMARY_KEY_COLUMNS: Dict[str, str] = {
    "master": "MDR_REPORT_KEY",
    "device": "DEVICE_EVENT_KEY",
    "patient": "MDR_REPORT_KEY",  # Composite with PATIENT_SEQUENCE_NUMBER
    "text": "MDR_TEXT_KEY",
    "problem": "MDR_REPORT_KEY",  # Composite with DEVICE_PROBLEM_CODE
}

# =============================================================================
# Foreign key columns (link to master table)
# =============================================================================

FOREIGN_KEY_COLUMN = "MDR_REPORT_KEY"


def get_fda_columns(file_type: str, column_count: int = None) -> List[str]:
    """
    Get the FDA column names for a file type.

    Args:
        file_type: One of 'master', 'device', 'patient', 'text', 'problem'
        column_count: Optional specific column count (for master files)

    Returns:
        List of FDA column names
    """
    if file_type not in FDA_FILE_COLUMNS:
        raise ValueError(f"Unknown file type: {file_type}")

    # Handle master file with variable column counts
    if file_type == "master" and column_count:
        if column_count == 84:
            return MASTER_COLUMNS_FDA_84.copy()
        elif column_count == 86:
            return MASTER_COLUMNS_FDA_86.copy()

    return FDA_FILE_COLUMNS[file_type].copy()


def is_headerless_file(file_type: str) -> bool:
    """Check if a file type has no header row."""
    return file_type in HEADERLESS_FILES


def get_expected_column_count(file_type: str) -> int:
    """Get the expected number of columns for a file type."""
    return EXPECTED_COLUMN_COUNTS.get(file_type, 0)


def get_alternative_column_counts(file_type: str) -> List[int]:
    """Get all valid column counts for a file type (handles schema evolution)."""
    return ALTERNATIVE_COLUMN_COUNTS.get(file_type, [])


def validate_schema(file_type: str, detected_columns: List[str]) -> Tuple[bool, str]:
    """
    Validate detected columns against expected schema.

    Args:
        file_type: Type of MAUDE file
        detected_columns: Columns detected from file header

    Returns:
        Tuple of (is_valid, message)
    """
    detected_count = len(detected_columns)

    # Check against all valid column counts
    valid_counts = get_alternative_column_counts(file_type)
    if not valid_counts:
        valid_counts = [get_expected_column_count(file_type)]

    if detected_count in valid_counts:
        return True, f"Column count valid: {detected_count}"
    else:
        expected_str = "/".join(map(str, valid_counts))
        return False, f"Missing columns: expected {expected_str}, got {detected_count}"


def get_columns_for_count(file_type: str, column_count: int) -> List[str]:
    """
    Get the appropriate column list based on detected column count.

    This handles schema evolution (e.g., master files with 84 vs 86 columns).

    Args:
        file_type: Type of MAUDE file
        column_count: Detected column count

    Returns:
        List of FDA column names for that column count
    """
    if file_type == "master":
        if column_count == 84:
            return MASTER_COLUMNS_FDA_84.copy()
        elif column_count == 86:
            return MASTER_COLUMNS_FDA_86.copy()
        else:
            # Default to current format
            return MASTER_COLUMNS_FDA_86.copy()

    return get_fda_columns(file_type)
