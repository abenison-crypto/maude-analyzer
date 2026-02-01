"""
Column and Schema Constants for MAUDE Analyzer API.

This module provides backward-compatible column name constants and event type
mappings. The actual definitions now live in the Unified Schema Registry.

IMPORTANT: New code should import from config.unified_schema instead.
This module is maintained for backward compatibility.

For new code, use:
    from config.unified_schema import get_schema_registry
    registry = get_schema_registry()
    registry.get_column("master_events", "event_type")
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

# Import from unified schema for single source of truth
from config.unified_schema import (
    get_schema_registry,
    EVENT_TYPES as _EVENT_TYPES,
    EVENT_TYPE_FILTER_TO_DB,
    EVENT_TYPE_DB_TO_FILTER,
    EVENT_TYPE_CODES as _EVENT_TYPE_CODES,
    OUTCOME_CODES as _OUTCOME_CODES,
    TEXT_TYPE_CODES as _TEXT_TYPE_CODES,
    get_event_type_name,
    get_event_type_code,
    convert_filter_event_types,
)


# =============================================================================
# COLUMN NAME CONSTANTS (Backward Compatibility)
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
# TABLE DEFINITIONS (Backward Compatibility)
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
        # Try to get from unified schema registry first
        registry = get_schema_registry()
        table = registry.get_table(table_name)
        if table:
            return TableDefinition(
                name=table.name,
                primary_key=table.primary_key,
                display_name=table.display_name,
                description=table.description
            )

        # Fallback for tables not in registry (product_codes, etc.)
        definitions = {
            cls.PRODUCT_CODES: TableDefinition(
                name=cls.PRODUCT_CODES,
                primary_key="product_code",
                display_name="Product Codes",
                description="FDA product classification codes"
            ),
            cls.PROBLEM_CODES: TableDefinition(
                name=cls.PROBLEM_CODES,
                primary_key="problem_code",
                display_name="Problem Codes",
                description="Device problem code lookup"
            ),
            cls.MANUFACTURERS: TableDefinition(
                name=cls.MANUFACTURERS,
                primary_key="id",
                display_name="Manufacturers",
                description="Manufacturer lookup table"
            ),
            cls.DAILY_AGGREGATES: TableDefinition(
                name=cls.DAILY_AGGREGATES,
                primary_key="id",
                display_name="Daily Aggregates",
                description="Pre-computed daily statistics"
            ),
        }
        return definitions.get(table_name)


TABLES = Tables


# =============================================================================
# EVENT TYPE MAPPINGS (Now sourced from Unified Schema)
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


# Build EVENT_TYPES from unified schema for backward compatibility
EVENT_TYPES: Dict[str, EventTypeInfo] = {
    code: EventTypeInfo(
        code=et.db_code,
        name=et.name,
        description=et.description,
        severity=et.severity,
        color=et.color,
        bg_class=et.bg_class,
        text_class=et.text_class
    )
    for code, et in _EVENT_TYPES.items()
}

# Code mapping for filter compatibility (from unified schema)
# Frontend uses 'I' for injury, database uses 'IN'
EVENT_TYPE_FILTER_MAPPING: Dict[str, str] = EVENT_TYPE_FILTER_TO_DB

# Reverse mapping (database code to filter code)
EVENT_TYPE_CODE_TO_FILTER: Dict[str, str] = EVENT_TYPE_DB_TO_FILTER

# All valid event type codes in the database
EVENT_TYPE_CODES: List[str] = _EVENT_TYPE_CODES

# Display-friendly list for dropdowns/filters
EVENT_TYPE_OPTIONS: List[Dict[str, str]] = [
    {"value": et.db_code, "label": et.name, "filter_code": et.filter_code}
    for et in _EVENT_TYPES.values()
    if et.db_code != "*"  # Exclude unknown
]


# =============================================================================
# PATIENT OUTCOME MAPPINGS (Now sourced from Unified Schema)
# =============================================================================

@dataclass
class OutcomeInfo:
    """Information about a patient outcome."""
    code: str
    name: str
    field: str
    severity: int
    color_class: str


# Build OUTCOME_CODES from unified schema
OUTCOME_CODES: Dict[str, OutcomeInfo] = {
    code: OutcomeInfo(
        code=out.code,
        name=out.name,
        field=out.db_field,
        severity=out.severity,
        color_class=out.color_class
    )
    for code, out in _OUTCOME_CODES.items()
}


# =============================================================================
# TEXT TYPE MAPPINGS (Now sourced from Unified Schema)
# =============================================================================

@dataclass
class TextTypeInfo:
    """Information about a text type."""
    code: str
    name: str
    description: str
    priority: int


# Build TEXT_TYPE_CODES from unified schema
TEXT_TYPE_CODES: Dict[str, TextTypeInfo] = {
    code: TextTypeInfo(
        code=tt.code,
        name=tt.name,
        description=tt.description,
        priority=tt.priority
    )
    for code, tt in _TEXT_TYPE_CODES.items()
}


# =============================================================================
# HELPER FUNCTIONS (Now delegate to Unified Schema)
# =============================================================================

# get_event_type_name is imported from unified_schema
# get_event_type_code is imported from unified_schema
# convert_filter_event_types is imported from unified_schema


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
