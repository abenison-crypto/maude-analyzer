"""
Column and Schema Constants for MAUDE Analyzer API.

This module centralizes all column names, event type mappings, and table
definitions to avoid hardcoding throughout the codebase.

Import from this module instead of using string literals for column names.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional


# =============================================================================
# COLUMN NAME CONSTANTS
# =============================================================================

# Master Events Table
COLUMN_MDR_REPORT_KEY = "mdr_report_key"
COLUMN_EVENT_KEY = "event_key"
COLUMN_REPORT_NUMBER = "report_number"
COLUMN_DATE_RECEIVED = "date_received"
COLUMN_DATE_REPORT = "date_report"
COLUMN_DATE_OF_EVENT = "date_of_event"
COLUMN_EVENT_TYPE = "event_type"
COLUMN_EVENT_LOCATION = "event_location"
COLUMN_PRODUCT_CODE = "product_code"
COLUMN_MANUFACTURER_NAME = "manufacturer_name"
COLUMN_MANUFACTURER_CLEAN = "manufacturer_clean"
COLUMN_ADVERSE_EVENT_FLAG = "adverse_event_flag"
COLUMN_PRODUCT_PROBLEM_FLAG = "product_problem_flag"
COLUMN_HEALTH_PROFESSIONAL = "health_professional"
COLUMN_RECEIVED_YEAR = "received_year"
COLUMN_RECEIVED_MONTH = "received_month"
COLUMN_CREATED_AT = "created_at"
COLUMN_UPDATED_AT = "updated_at"

# Devices Table
COLUMN_DEVICE_SEQUENCE_NUMBER = "device_sequence_number"
COLUMN_BRAND_NAME = "brand_name"
COLUMN_GENERIC_NAME = "generic_name"
COLUMN_MODEL_NUMBER = "model_number"
COLUMN_CATALOG_NUMBER = "catalog_number"
COLUMN_LOT_NUMBER = "lot_number"
COLUMN_DEVICE_REPORT_PRODUCT_CODE = "device_report_product_code"
COLUMN_MANUFACTURER_D_NAME = "manufacturer_d_name"
COLUMN_MANUFACTURER_D_CLEAN = "manufacturer_d_clean"
COLUMN_IMPLANT_FLAG = "implant_flag"

# Patients Table
COLUMN_PATIENT_SEQUENCE_NUMBER = "patient_sequence_number"
COLUMN_PATIENT_AGE = "patient_age"
COLUMN_PATIENT_SEX = "patient_sex"
COLUMN_PATIENT_WEIGHT = "patient_weight"
COLUMN_PATIENT_AGE_NUMERIC = "patient_age_numeric"
COLUMN_OUTCOME_DEATH = "outcome_death"
COLUMN_OUTCOME_LIFE_THREATENING = "outcome_life_threatening"
COLUMN_OUTCOME_HOSPITALIZATION = "outcome_hospitalization"
COLUMN_OUTCOME_DISABILITY = "outcome_disability"

# MDR Text Table
COLUMN_MDR_TEXT_KEY = "mdr_text_key"
COLUMN_TEXT_TYPE_CODE = "text_type_code"
COLUMN_TEXT_CONTENT = "text_content"


# =============================================================================
# TABLE DEFINITIONS
# =============================================================================

@dataclass
class TableDefinition:
    """Definition of a database table."""
    name: str
    primary_key: str
    display_name: str
    description: str


class Tables:
    """Table name constants and definitions."""

    MASTER_EVENTS = "master_events"
    DEVICES = "devices"
    PATIENTS = "patients"
    MDR_TEXT = "mdr_text"
    DEVICE_PROBLEMS = "device_problems"
    PATIENT_PROBLEMS = "patient_problems"
    PRODUCT_CODES = "product_codes"
    PROBLEM_CODES = "problem_codes"
    MANUFACTURERS = "manufacturers"
    DAILY_AGGREGATES = "daily_aggregates"

    @classmethod
    def get_definition(cls, table_name: str) -> Optional[TableDefinition]:
        """Get table definition by name."""
        definitions = {
            cls.MASTER_EVENTS: TableDefinition(
                name=cls.MASTER_EVENTS,
                primary_key=COLUMN_MDR_REPORT_KEY,
                display_name="Master Events",
                description="Primary MDR event records"
            ),
            cls.DEVICES: TableDefinition(
                name=cls.DEVICES,
                primary_key="id",
                display_name="Devices",
                description="Device information linked to events"
            ),
            cls.PATIENTS: TableDefinition(
                name=cls.PATIENTS,
                primary_key="id",
                display_name="Patients",
                description="Patient information and outcomes"
            ),
            cls.MDR_TEXT: TableDefinition(
                name=cls.MDR_TEXT,
                primary_key="id",
                display_name="MDR Text",
                description="Narrative text records"
            ),
        }
        return definitions.get(table_name)


TABLES = Tables


# =============================================================================
# EVENT TYPE MAPPINGS
# =============================================================================

@dataclass
class EventTypeInfo:
    """Information about an event type."""
    code: str
    name: str
    description: str
    severity: int
    color: str
    bg_class: str
    text_class: str


# Event types used in the database
EVENT_TYPES: Dict[str, EventTypeInfo] = {
    "D": EventTypeInfo(
        code="D",
        name="Death",
        description="Patient death associated with device",
        severity=1,
        color="#dc2626",
        bg_class="bg-red-100",
        text_class="text-red-800"
    ),
    "IN": EventTypeInfo(
        code="IN",
        name="Injury",
        description="Patient injury associated with device",
        severity=2,
        color="#ea580c",
        bg_class="bg-orange-100",
        text_class="text-orange-800"
    ),
    "M": EventTypeInfo(
        code="M",
        name="Malfunction",
        description="Device malfunction",
        severity=3,
        color="#ca8a04",
        bg_class="bg-yellow-100",
        text_class="text-yellow-800"
    ),
    "O": EventTypeInfo(
        code="O",
        name="Other",
        description="Other event type",
        severity=4,
        color="#6b7280",
        bg_class="bg-gray-100",
        text_class="text-gray-800"
    ),
    "*": EventTypeInfo(
        code="*",
        name="Unknown",
        description="No answer provided",
        severity=5,
        color="#9ca3af",
        bg_class="bg-gray-50",
        text_class="text-gray-600"
    ),
}

# Code mapping for filter compatibility
# Frontend uses 'I' for injury, database uses 'IN'
EVENT_TYPE_FILTER_MAPPING: Dict[str, str] = {
    "I": "IN",  # Filter uses I, database uses IN
    "D": "D",
    "M": "M",
    "O": "O",
}

# Reverse mapping (database code to filter code)
EVENT_TYPE_CODE_TO_FILTER: Dict[str, str] = {
    "IN": "I",
    "D": "D",
    "M": "M",
    "O": "O",
}

# All valid event type codes in the database
EVENT_TYPE_CODES: List[str] = ["D", "IN", "M", "O", "*"]

# Display-friendly list for dropdowns/filters
EVENT_TYPE_OPTIONS: List[Dict[str, str]] = [
    {"value": "D", "label": "Death", "filter_code": "D"},
    {"value": "IN", "label": "Injury", "filter_code": "I"},
    {"value": "M", "label": "Malfunction", "filter_code": "M"},
    {"value": "O", "label": "Other", "filter_code": "O"},
]


# =============================================================================
# PATIENT OUTCOME MAPPINGS
# =============================================================================

@dataclass
class OutcomeInfo:
    """Information about a patient outcome."""
    code: str
    name: str
    field: str
    severity: int
    color_class: str


OUTCOME_CODES: Dict[str, OutcomeInfo] = {
    "D": OutcomeInfo(
        code="D",
        name="Death",
        field=COLUMN_OUTCOME_DEATH,
        severity=1,
        color_class="bg-red-100 text-red-800"
    ),
    "L": OutcomeInfo(
        code="L",
        name="Life Threatening",
        field=COLUMN_OUTCOME_LIFE_THREATENING,
        severity=2,
        color_class="bg-yellow-100 text-yellow-800"
    ),
    "H": OutcomeInfo(
        code="H",
        name="Hospitalization",
        field=COLUMN_OUTCOME_HOSPITALIZATION,
        severity=3,
        color_class="bg-orange-100 text-orange-800"
    ),
    "DS": OutcomeInfo(
        code="DS",
        name="Disability",
        field=COLUMN_OUTCOME_DISABILITY,
        severity=4,
        color_class="bg-purple-100 text-purple-800"
    ),
}


# =============================================================================
# TEXT TYPE MAPPINGS
# =============================================================================

@dataclass
class TextTypeInfo:
    """Information about a text type."""
    code: str
    name: str
    description: str
    priority: int


TEXT_TYPE_CODES: Dict[str, TextTypeInfo] = {
    "D": TextTypeInfo(
        code="D",
        name="Event Description",
        description="Primary event description narrative",
        priority=1
    ),
    "H": TextTypeInfo(
        code="H",
        name="Event History",
        description="Historical context of event",
        priority=2
    ),
    "M": TextTypeInfo(
        code="M",
        name="Manufacturer Narrative",
        description="Manufacturer's description",
        priority=3
    ),
    "E": TextTypeInfo(
        code="E",
        name="Evaluation Summary",
        description="Evaluation/assessment summary",
        priority=4
    ),
    "N": TextTypeInfo(
        code="N",
        name="Additional Information",
        description="Additional notes and information",
        priority=5
    ),
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_event_type_name(code: str) -> str:
    """
    Get the display name for an event type code.

    Args:
        code: Event type code (D, IN, M, O, *)

    Returns:
        Display name (e.g., "Death", "Injury")
    """
    event_type = EVENT_TYPES.get(code)
    return event_type.name if event_type else code


def get_event_type_code(filter_code: str) -> str:
    """
    Convert a filter code to database code.

    Handles the I -> IN conversion for injury.

    Args:
        filter_code: Filter code (D, I, M, O)

    Returns:
        Database code (D, IN, M, O)
    """
    return EVENT_TYPE_FILTER_MAPPING.get(filter_code, filter_code)


def get_event_type_display(code: str) -> Dict[str, str]:
    """
    Get full display information for an event type.

    Args:
        code: Event type code

    Returns:
        Dictionary with label and color class
    """
    event_type = EVENT_TYPES.get(code)
    if event_type:
        return {
            "label": event_type.name,
            "color": f"{event_type.bg_class} {event_type.text_class}"
        }
    return {"label": code or "Unknown", "color": "bg-gray-100 text-gray-800"}


def get_manufacturer_column(prefer_clean: bool = True) -> str:
    """
    Get the appropriate manufacturer column name.

    Args:
        prefer_clean: If True, prefer manufacturer_clean over manufacturer_name

    Returns:
        Column name to use for manufacturer queries
    """
    return COLUMN_MANUFACTURER_CLEAN if prefer_clean else COLUMN_MANUFACTURER_NAME


def convert_filter_event_types(event_types: List[str]) -> List[str]:
    """
    Convert filter event types to database event types.

    Args:
        event_types: List of filter codes (may include 'I')

    Returns:
        List of database codes (with 'I' converted to 'IN')
    """
    return [get_event_type_code(et) for et in event_types]


def get_text_type_name(code: str) -> str:
    """
    Get the display name for a text type code.

    Args:
        code: Text type code (D, H, M, E, N)

    Returns:
        Display name
    """
    text_type = TEXT_TYPE_CODES.get(code)
    return text_type.name if text_type else code


def get_outcome_name(code: str) -> str:
    """
    Get the display name for an outcome code.

    Args:
        code: Outcome code (D, L, H, DS)

    Returns:
        Display name
    """
    outcome = OUTCOME_CODES.get(code)
    return outcome.name if outcome else code
