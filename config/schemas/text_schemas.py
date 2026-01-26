"""
FDA MAUDE Text File Schema Definitions.

Text files (foitext*.txt) contain narrative descriptions of adverse events,
including problem descriptions, manufacturer narratives, and FDA evaluation notes.

Text Types:
- D: Description of event (patient problem)
- H5: Manufacturer narrative
- H6: Additional manufacturer comments
- UF: FDA evaluation/additional text

Historical Schema:
- The text file schema has been stable since 1996
- Pre-1996 data is in foitextthru1995.zip with same format
- Annual files: foitext1996.zip through foitext2025.zip
- Current year: foitext.zip (updated weekly)
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
# TEXT FILE SCHEMA - 6 columns (stable since 1996)
# =============================================================================

TEXT_COLUMNS: List[str] = [
    "MDR_REPORT_KEY",              # 1 - Foreign key to master
    "MDR_TEXT_KEY",                # 2 - Unique text key
    "TEXT_TYPE_CODE",              # 3 - D, H5, H6, UF
    "PATIENT_SEQUENCE_NUMBER",     # 4 - Links to patient record
    "DATE_REPORT",                 # 5 - Report date
    "FOI_TEXT",                    # 6 - The actual narrative text
]

TEXT_SCHEMA_CURRENT = SchemaDefinition(
    name="text_current",
    columns=TEXT_COLUMNS,
    year_start=1996,
    year_end=None,  # Current
    column_count=6,
    notes="Standard text schema, stable since 1996",
)

# Pre-1996 uses same schema
TEXT_SCHEMA_PRE_1996 = SchemaDefinition(
    name="text_pre_1996",
    columns=TEXT_COLUMNS,
    year_start=None,
    year_end=1995,
    column_count=6,
    notes="Pre-1996 text data, same schema",
)


# =============================================================================
# TEXT TYPE CODE DEFINITIONS
# =============================================================================

TEXT_TYPE_CODES: Dict[str, str] = {
    "D": "Description of Event",
    "H5": "Manufacturer Narrative",
    "H6": "Additional Manufacturer Comments",
    "UF": "FDA Evaluation / Additional Text",
    "B5": "Initial Report (5-day)",          # Rare
    "B10": "Initial Report (10-day)",        # Rare
    "D05": "Description (5-day)",            # Variant
    "D10": "Description (10-day)",           # Variant
}

# Primary text types for analysis
PRIMARY_TEXT_TYPES = ["D", "H5", "H6", "UF"]


# =============================================================================
# COLUMN MAPPING: FDA to Database
# =============================================================================

TEXT_FDA_TO_DB_MAPPING: Dict[str, str] = {
    "MDR_REPORT_KEY": "mdr_report_key",
    "MDR_TEXT_KEY": "mdr_text_key",
    "TEXT_TYPE_CODE": "text_type_code",
    "PATIENT_SEQUENCE_NUMBER": "patient_sequence_number",
    "DATE_REPORT": "date_report",
    "FOI_TEXT": "text_content",  # Renamed for clarity
}


# =============================================================================
# SCHEMA DETECTION AND SELECTION
# =============================================================================

def get_text_schema(
    filename: str = None,
    year: int = None,
    column_count: int = None,
) -> SchemaDefinition:
    """
    Get the appropriate text schema.

    Text schema has been stable, so this mainly handles edge cases.

    Args:
        filename: Name of the text file.
        year: Year of the data (if known).
        column_count: Detected column count from file header.

    Returns:
        Appropriate SchemaDefinition.
    """
    # Text schema has been stable since 1996
    # Only return different schema for pre-1996 (which has same columns anyway)
    if year is not None and year < 1996:
        return TEXT_SCHEMA_PRE_1996

    if filename:
        filename_lower = filename.lower()
        if "thru1995" in filename_lower:
            return TEXT_SCHEMA_PRE_1996

    return TEXT_SCHEMA_CURRENT


def validate_text_schema(
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

    return False, f"Column count mismatch: {detected_count} vs expected {expected_count}"


# =============================================================================
# PATIENT FILE SCHEMA - 10 columns
# =============================================================================

PATIENT_COLUMNS: List[str] = [
    "MDR_REPORT_KEY",              # 1 - Foreign key to master
    "PATIENT_SEQUENCE_NUMBER",     # 2
    "DATE_RECEIVED",               # 3
    "SEQUENCE_NUMBER_TREATMENT",   # 4 - Treatment code sequence
    "SEQUENCE_NUMBER_OUTCOME",     # 5 - Outcome code sequence
    "PATIENT_AGE",                 # 6 - Age text (e.g., "75 YR")
    "PATIENT_SEX",                 # 7 - M/F/U
    "PATIENT_WEIGHT",              # 8 - Weight text
    "PATIENT_ETHNICITY",           # 9
    "PATIENT_RACE",                # 10
]

PATIENT_SCHEMA = SchemaDefinition(
    name="patient_current",
    columns=PATIENT_COLUMNS,
    year_start=None,
    year_end=None,
    column_count=10,
    notes="Patient demographics and outcomes",
)

PATIENT_FDA_TO_DB_MAPPING: Dict[str, str] = {
    "MDR_REPORT_KEY": "mdr_report_key",
    "PATIENT_SEQUENCE_NUMBER": "patient_sequence_number",
    "DATE_RECEIVED": "date_received",
    "SEQUENCE_NUMBER_TREATMENT": "sequence_number_treatment",
    "SEQUENCE_NUMBER_OUTCOME": "sequence_number_outcome",
    "PATIENT_AGE": "patient_age",
    "PATIENT_SEX": "patient_sex",
    "PATIENT_WEIGHT": "patient_weight",
    "PATIENT_ETHNICITY": "patient_ethnicity",
    "PATIENT_RACE": "patient_race",
}


# =============================================================================
# PROBLEM CODE FILE SCHEMAS
# =============================================================================

# Device problem codes - NO HEADER in file
DEVICE_PROBLEM_COLUMNS: List[str] = [
    "MDR_REPORT_KEY",              # 1 - Foreign key to master
    "DEVICE_PROBLEM_CODE",         # 2 - Problem code
]

DEVICE_PROBLEM_SCHEMA = SchemaDefinition(
    name="device_problem",
    columns=DEVICE_PROBLEM_COLUMNS,
    year_start=None,
    year_end=None,
    column_count=2,
    has_header=False,  # This file has NO header
    notes="Device problem codes - no header row",
)

# Problem code lookup table
PROBLEM_CODES_LOOKUP_COLUMNS: List[str] = [
    "DEVICE_PROBLEM_CODE",         # 1 - Problem code
    "DEVICE_PROBLEM_DESCRIPTION",  # 2 - Description
]

PROBLEM_CODES_LOOKUP_SCHEMA = SchemaDefinition(
    name="problem_codes_lookup",
    columns=PROBLEM_CODES_LOOKUP_COLUMNS,
    year_start=None,
    year_end=None,
    column_count=2,
    has_header=False,
    notes="Device problem code descriptions lookup",
)

# Patient problem codes
PATIENT_PROBLEM_COLUMNS: List[str] = [
    "MDR_REPORT_KEY",              # 1 - Foreign key to master
    "PATIENT_SEQUENCE_NO",         # 2 - Patient sequence number
    "PROBLEM_CODE",                # 3 - Patient problem code
    "DATE_ADDED",                  # 4 - Date added
    "DATE_CHANGED",                # 5 - Date changed
]

PATIENT_PROBLEM_SCHEMA = SchemaDefinition(
    name="patient_problem",
    columns=PATIENT_PROBLEM_COLUMNS,
    year_start=None,
    year_end=None,
    column_count=5,
    notes="Patient problem codes with timestamps",
)

# Patient problem data lookup
PATIENT_PROBLEM_DATA_COLUMNS: List[str] = [
    "PROBLEM_CODE",                # 1 - Problem code
    "PROBLEM_DESCRIPTION",         # 2 - Description
]

PATIENT_PROBLEM_DATA_SCHEMA = SchemaDefinition(
    name="patient_problem_data",
    columns=PATIENT_PROBLEM_DATA_COLUMNS,
    year_start=None,
    year_end=None,
    column_count=2,
    notes="Patient problem code descriptions lookup",
)


# =============================================================================
# ASR (Alternative Summary Report) SCHEMAS
# =============================================================================

ASR_COLUMNS: List[str] = [
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

ASR_SCHEMA = SchemaDefinition(
    name="asr",
    columns=ASR_COLUMNS,
    year_start=1999,
    year_end=2019,
    column_count=18,
    delimiter=",",  # ASR files are CSV
    notes="Alternative Summary Reports 1999-2019",
)

ASR_PPC_COLUMNS: List[str] = [
    "REPORT_ID",                   # 1 - Foreign key to ASR report
    "PATIENT_PROBLEM_CODE",        # 2 - Problem code
    "OCCURRENCE_COUNT",            # 3 - Count of occurrences
]

ASR_PPC_SCHEMA = SchemaDefinition(
    name="asr_ppc",
    columns=ASR_PPC_COLUMNS,
    year_start=1999,
    year_end=2019,
    column_count=3,
    delimiter=",",
    notes="ASR Patient Problem Codes",
)


# =============================================================================
# DISCLAIMER SCHEMA
# =============================================================================

DISCLAIMER_COLUMNS: List[str] = [
    "MANUFACTURER_NAME",           # 1
    "DISCLAIMER_TEXT",             # 2
    "EFFECTIVE_DATE",              # 3
]

DISCLAIMER_SCHEMA = SchemaDefinition(
    name="disclaimer",
    columns=DISCLAIMER_COLUMNS,
    year_start=None,
    year_end=None,
    column_count=3,
    notes="Manufacturer disclaimers",
)


# =============================================================================
# KEY TEXT ANALYSIS FIELDS
# =============================================================================

# Fields useful for text analysis
TEXT_ANALYSIS_FIELDS = [
    "text_content",         # The actual narrative
    "text_type_code",       # Type classification
    "mdr_report_key",       # Link to event
]

# Common problem keywords to look for in narratives
PROBLEM_KEYWORDS = [
    "death", "died", "fatal",
    "hospitalization", "hospitalized", "admitted",
    "injury", "injured",
    "malfunction", "failure", "failed",
    "infection", "infected",
    "bleeding", "hemorrhage",
    "pain", "discomfort",
    "revision", "explant", "removal",
]
