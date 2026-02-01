"""
Unified Schema Registry - Single Source of Truth for MAUDE Analyzer.

This module provides a centralized schema definition that is used across:
- Database layer (column names, types, constraints)
- API layer (query building, validation)
- Frontend layer (TypeScript type generation)

IMPORTANT: This is the ONLY place where schema definitions should be maintained.
All other modules should import from here.

Version History:
- 2.0: Initial unified schema (consolidates column_mappings.py, schema_registry.py, api/constants/columns.py)
- 2.1: Added schema evolution tracking for historical data handling
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Any
import json


# =============================================================================
# SCHEMA VERSION
# =============================================================================

SCHEMA_VERSION = "2.1"


# =============================================================================
# TYPE DEFINITIONS
# =============================================================================

class DataType(Enum):
    """Database data types with TypeScript equivalents."""
    STRING = "VARCHAR"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    BOOLEAN = "BOOLEAN"
    FLOAT = "DOUBLE"


# Map DuckDB types to TypeScript types
DUCKDB_TO_TS_TYPE: Dict[str, str] = {
    "VARCHAR": "string",
    "INTEGER": "number",
    "BIGINT": "number",
    "DATE": "string",  # ISO date string
    "TIMESTAMP": "string",  # ISO timestamp string
    "BOOLEAN": "boolean",
    "DOUBLE": "number",
}


@dataclass
class ColumnDefinition:
    """Complete definition of a database column."""
    db_name: str              # Database column name (lowercase)
    fda_name: str             # Original FDA column name (uppercase)
    table: str                # Table name
    data_type: str            # DuckDB data type
    nullable: bool = True
    api_name: Optional[str] = None  # API response field name (defaults to db_name)
    ts_type: Optional[str] = None   # TypeScript type (auto-derived if not set)
    description: str = ""
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_table: Optional[str] = None
    is_optional: bool = False  # True for columns that may not exist in all schema versions

    def __post_init__(self):
        if self.api_name is None:
            self.api_name = self.db_name
        if self.ts_type is None:
            self.ts_type = DUCKDB_TO_TS_TYPE.get(self.data_type, "unknown")


@dataclass
class EventTypeDefinition:
    """Definition of an event type with all metadata."""
    db_code: str          # Database code (D, IN, M, O, *)
    filter_code: str      # URL/filter code (D, I, M, O)
    name: str             # Display name
    description: str
    severity: int         # 1 = most severe (death)
    color: str            # Hex color
    bg_class: str         # Tailwind background class
    text_class: str       # Tailwind text class


@dataclass
class OutcomeDefinition:
    """Definition of a patient outcome."""
    code: str
    name: str
    db_field: str         # Database column name
    severity: int
    color_class: str


@dataclass
class TextTypeDefinition:
    """Definition of a text type."""
    code: str
    name: str
    description: str
    priority: int


@dataclass
class TableDefinition:
    """Complete definition of a database table."""
    name: str
    display_name: str
    description: str
    primary_key: str
    columns: List[ColumnDefinition] = field(default_factory=list)
    foreign_keys: Dict[str, str] = field(default_factory=dict)

    def get_column(self, name: str) -> Optional[ColumnDefinition]:
        """Get column definition by database name."""
        for col in self.columns:
            if col.db_name == name:
                return col
        return None

    def has_column(self, name: str) -> bool:
        """Check if column exists."""
        return any(col.db_name == name for col in self.columns)

    def get_column_names(self) -> List[str]:
        """Get all column names."""
        return [col.db_name for col in self.columns]

    def get_required_columns(self) -> List[str]:
        """Get column names that are always present (not optional)."""
        return [col.db_name for col in self.columns if not col.is_optional]


# =============================================================================
# EVENT TYPE DEFINITIONS
# =============================================================================

EVENT_TYPES: Dict[str, EventTypeDefinition] = {
    "D": EventTypeDefinition(
        db_code="D",
        filter_code="D",
        name="Death",
        description="Patient death associated with device",
        severity=1,
        color="#dc2626",
        bg_class="bg-red-100",
        text_class="text-red-800",
    ),
    "IN": EventTypeDefinition(
        db_code="IN",
        filter_code="I",
        name="Injury",
        description="Patient injury associated with device",
        severity=2,
        color="#ea580c",
        bg_class="bg-orange-100",
        text_class="text-orange-800",
    ),
    "M": EventTypeDefinition(
        db_code="M",
        filter_code="M",
        name="Malfunction",
        description="Device malfunction",
        severity=3,
        color="#ca8a04",
        bg_class="bg-yellow-100",
        text_class="text-yellow-800",
    ),
    "O": EventTypeDefinition(
        db_code="O",
        filter_code="O",
        name="Other",
        description="Other event type",
        severity=4,
        color="#6b7280",
        bg_class="bg-gray-100",
        text_class="text-gray-800",
    ),
    "*": EventTypeDefinition(
        db_code="*",
        filter_code="*",
        name="Unknown",
        description="No answer provided",
        severity=5,
        color="#9ca3af",
        bg_class="bg-gray-50",
        text_class="text-gray-600",
    ),
}

# Mappings for event type conversion
EVENT_TYPE_FILTER_TO_DB: Dict[str, str] = {
    et.filter_code: et.db_code for et in EVENT_TYPES.values()
}

EVENT_TYPE_DB_TO_FILTER: Dict[str, str] = {
    et.db_code: et.filter_code for et in EVENT_TYPES.values()
}

# All valid event type codes in database
EVENT_TYPE_CODES: List[str] = list(EVENT_TYPES.keys())


# =============================================================================
# PATIENT OUTCOME DEFINITIONS
# =============================================================================

OUTCOME_CODES: Dict[str, OutcomeDefinition] = {
    "D": OutcomeDefinition(
        code="D",
        name="Death",
        db_field="outcome_death",
        severity=1,
        color_class="bg-red-100 text-red-800",
    ),
    "L": OutcomeDefinition(
        code="L",
        name="Life Threatening",
        db_field="outcome_life_threatening",
        severity=2,
        color_class="bg-yellow-100 text-yellow-800",
    ),
    "H": OutcomeDefinition(
        code="H",
        name="Hospitalization",
        db_field="outcome_hospitalization",
        severity=3,
        color_class="bg-orange-100 text-orange-800",
    ),
    "DS": OutcomeDefinition(
        code="DS",
        name="Disability",
        db_field="outcome_disability",
        severity=4,
        color_class="bg-purple-100 text-purple-800",
    ),
    "CA": OutcomeDefinition(
        code="CA",
        name="Congenital Anomaly",
        db_field="outcome_congenital_anomaly",
        severity=5,
        color_class="bg-pink-100 text-pink-800",
    ),
    "RI": OutcomeDefinition(
        code="RI",
        name="Required Intervention",
        db_field="outcome_required_intervention",
        severity=6,
        color_class="bg-blue-100 text-blue-800",
    ),
    "OT": OutcomeDefinition(
        code="OT",
        name="Other",
        db_field="outcome_other",
        severity=7,
        color_class="bg-gray-100 text-gray-800",
    ),
}


# =============================================================================
# TEXT TYPE DEFINITIONS
# =============================================================================

TEXT_TYPE_CODES: Dict[str, TextTypeDefinition] = {
    "D": TextTypeDefinition(
        code="D",
        name="Event Description",
        description="Primary event description narrative",
        priority=1,
    ),
    "H": TextTypeDefinition(
        code="H",
        name="Event History",
        description="Historical context of event",
        priority=2,
    ),
    "M": TextTypeDefinition(
        code="M",
        name="Manufacturer Narrative",
        description="Manufacturer's description",
        priority=3,
    ),
    "E": TextTypeDefinition(
        code="E",
        name="Evaluation Summary",
        description="Evaluation/assessment summary",
        priority=4,
    ),
    "N": TextTypeDefinition(
        code="N",
        name="Additional Information",
        description="Additional notes and information",
        priority=5,
    ),
}


# =============================================================================
# SCHEMA EVOLUTION - Historical Data Handling
# =============================================================================

@dataclass
class SchemaVersion:
    """Tracks schema changes for a specific column count."""
    column_count: int
    added_columns: List[str] = field(default_factory=list)
    removed_columns: List[str] = field(default_factory=list)
    year_introduced: Optional[int] = None
    notes: str = ""


class SchemaEvolution:
    """Tracks schema changes across FDA file versions."""

    # Master file schema versions
    MASTER_VERSIONS: Dict[int, SchemaVersion] = {
        84: SchemaVersion(
            column_count=84,
            removed_columns=["mfr_report_type", "reporter_state_code"],
            notes="Historical format through 2023",
        ),
        86: SchemaVersion(
            column_count=86,
            added_columns=["mfr_report_type", "reporter_state_code"],
            year_introduced=2024,
            notes="Current format 2024+",
        ),
    }

    # Device file schema versions
    DEVICE_VERSIONS: Dict[int, SchemaVersion] = {
        28: SchemaVersion(
            column_count=28,
            notes="Pre-2020 format (foidev*.txt)",
        ),
        34: SchemaVersion(
            column_count=34,
            added_columns=[
                "implant_date_year",
                "date_removed_year",
                "serviced_by_3rd_party_flag",
                "combination_product_flag",
                "udi_di",
                "udi_public",
            ],
            year_introduced=2020,
            notes="2020+ extended format with UDI fields",
        ),
    }

    @classmethod
    def get_optional_columns(cls, table: str) -> List[str]:
        """Get columns that may not exist in all schema versions."""
        if table == "master_events":
            # Combine all added/removed columns across versions
            optional = set()
            for version in cls.MASTER_VERSIONS.values():
                optional.update(version.added_columns)
                optional.update(version.removed_columns)
            return list(optional)
        elif table == "devices":
            optional = set()
            for version in cls.DEVICE_VERSIONS.values():
                optional.update(version.added_columns)
            return list(optional)
        return []

    @classmethod
    def get_version_for_column_count(cls, table: str, count: int) -> Optional[SchemaVersion]:
        """Get schema version info for a given column count."""
        if table in ("master", "master_events"):
            return cls.MASTER_VERSIONS.get(count)
        elif table in ("device", "devices"):
            return cls.DEVICE_VERSIONS.get(count)
        return None


# =============================================================================
# TABLE DEFINITIONS - MASTER_EVENTS
# =============================================================================

MASTER_EVENTS_COLUMNS = [
    # Key identifiers
    ColumnDefinition("mdr_report_key", "MDR_REPORT_KEY", "master_events", "BIGINT",
                     is_primary_key=True, nullable=False, description="Primary key"),
    ColumnDefinition("event_key", "EVENT_KEY", "master_events", "BIGINT"),
    ColumnDefinition("report_number", "REPORT_NUMBER", "master_events", "VARCHAR"),
    ColumnDefinition("report_source_code", "REPORT_SOURCE_CODE", "master_events", "VARCHAR"),

    # Event counts
    ColumnDefinition("number_devices_in_event", "NUMBER_DEVICES_IN_EVENT", "master_events", "INTEGER"),
    ColumnDefinition("number_patients_in_event", "NUMBER_PATIENTS_IN_EVENT", "master_events", "INTEGER"),

    # Key dates
    ColumnDefinition("date_received", "DATE_RECEIVED", "master_events", "DATE"),
    ColumnDefinition("date_report", "DATE_REPORT", "master_events", "DATE"),
    ColumnDefinition("date_of_event", "DATE_OF_EVENT", "master_events", "DATE"),

    # Event flags
    ColumnDefinition("adverse_event_flag", "ADVERSE_EVENT_FLAG", "master_events", "VARCHAR"),
    ColumnDefinition("product_problem_flag", "PRODUCT_PROBLEM_FLAG", "master_events", "VARCHAR"),
    ColumnDefinition("reprocessed_and_reused_flag", "REPROCESSED_AND_REUSED_FLAG", "master_events", "VARCHAR"),

    # Event classification
    ColumnDefinition("event_type", "EVENT_TYPE", "master_events", "VARCHAR",
                     description="D=Death, IN=Injury, M=Malfunction, O=Other"),
    ColumnDefinition("event_location", "EVENT_LOCATION", "master_events", "VARCHAR"),

    # Reporter information
    ColumnDefinition("reporter_occupation_code", "REPORTER_OCCUPATION_CODE", "master_events", "VARCHAR"),
    ColumnDefinition("health_professional", "HEALTH_PROFESSIONAL", "master_events", "VARCHAR"),
    ColumnDefinition("initial_report_to_fda", "INITIAL_REPORT_TO_FDA", "master_events", "VARCHAR"),
    ColumnDefinition("reporter_state_code", "REPORTER_STATE_CODE", "master_events", "VARCHAR",
                     is_optional=True, description="Added in 86-column format"),
    ColumnDefinition("reporter_country_code", "REPORTER_COUNTRY_CODE", "master_events", "VARCHAR"),

    # Manufacturer info
    ColumnDefinition("manufacturer_name", "MANUFACTURER_NAME", "master_events", "VARCHAR"),
    ColumnDefinition("manufacturer_city", "MANUFACTURER_CITY", "master_events", "VARCHAR"),
    ColumnDefinition("manufacturer_state", "MANUFACTURER_STATE_CODE", "master_events", "VARCHAR"),
    ColumnDefinition("manufacturer_country", "MANUFACTURER_COUNTRY_CODE", "master_events", "VARCHAR"),
    ColumnDefinition("manufacturer_postal", "MANUFACTURER_POSTAL_CODE", "master_events", "VARCHAR"),

    # Normalized manufacturer (computed column)
    ColumnDefinition("manufacturer_clean", "", "master_events", "VARCHAR",
                     description="Normalized manufacturer name for grouping"),

    # Product identification
    ColumnDefinition("product_code", "PMA_PMN_NUM", "master_events", "VARCHAR",
                     api_name="product_code"),
    ColumnDefinition("pma_pmn_number", "PMA_PMN_NUM", "master_events", "VARCHAR"),
    ColumnDefinition("exemption_number", "EXEMPTION_NUMBER", "master_events", "VARCHAR"),

    # Report type info
    ColumnDefinition("type_of_report", "TYPE_OF_REPORT", "master_events", "VARCHAR"),
    ColumnDefinition("source_type", "SOURCE_TYPE", "master_events", "VARCHAR"),
    ColumnDefinition("mfr_report_type", "MFR_REPORT_TYPE", "master_events", "VARCHAR",
                     is_optional=True, description="Added in 86-column format"),

    # Metadata dates
    ColumnDefinition("date_added", "DATE_ADDED", "master_events", "DATE"),
    ColumnDefinition("date_changed", "DATE_CHANGED", "master_events", "DATE"),

    # Derived columns (computed during ingestion)
    ColumnDefinition("received_year", "", "master_events", "INTEGER",
                     description="Year extracted from date_received"),
    ColumnDefinition("received_month", "", "master_events", "INTEGER",
                     description="Month extracted from date_received"),
]


# =============================================================================
# TABLE DEFINITIONS - DEVICES
# =============================================================================

DEVICES_COLUMNS = [
    # Keys
    ColumnDefinition("mdr_report_key", "MDR_REPORT_KEY", "devices", "BIGINT",
                     is_foreign_key=True, foreign_key_table="master_events", nullable=False),
    ColumnDefinition("device_event_key", "DEVICE_EVENT_KEY", "devices", "BIGINT"),
    ColumnDefinition("device_sequence_number", "DEVICE_SEQUENCE_NO", "devices", "INTEGER"),

    # Device identification
    ColumnDefinition("brand_name", "BRAND_NAME", "devices", "VARCHAR"),
    ColumnDefinition("generic_name", "GENERIC_NAME", "devices", "VARCHAR"),
    ColumnDefinition("model_number", "MODEL_NUMBER", "devices", "VARCHAR"),
    ColumnDefinition("catalog_number", "CATALOG_NUMBER", "devices", "VARCHAR"),
    ColumnDefinition("lot_number", "LOT_NUMBER", "devices", "VARCHAR"),
    ColumnDefinition("other_id_number", "OTHER_ID_NUMBER", "devices", "VARCHAR"),

    # Device manufacturer
    ColumnDefinition("manufacturer_d_name", "MANUFACTURER_D_NAME", "devices", "VARCHAR"),
    ColumnDefinition("manufacturer_d_city", "MANUFACTURER_D_CITY", "devices", "VARCHAR"),
    ColumnDefinition("manufacturer_d_state", "MANUFACTURER_D_STATE_CODE", "devices", "VARCHAR"),
    ColumnDefinition("manufacturer_d_country", "MANUFACTURER_D_COUNTRY_CODE", "devices", "VARCHAR"),
    ColumnDefinition("manufacturer_d_clean", "", "devices", "VARCHAR",
                     description="Normalized device manufacturer name"),

    # Product classification
    ColumnDefinition("device_report_product_code", "DEVICE_REPORT_PRODUCT_CODE", "devices", "VARCHAR"),

    # Device flags
    ColumnDefinition("implant_flag", "IMPLANT_FLAG", "devices", "VARCHAR"),
    ColumnDefinition("date_removed_flag", "DATE_REMOVED_FLAG", "devices", "VARCHAR"),
    ColumnDefinition("device_availability", "DEVICE_AVAILABILITY", "devices", "VARCHAR"),
    ColumnDefinition("device_operator", "DEVICE_OPERATOR", "devices", "VARCHAR"),

    # Dates
    ColumnDefinition("date_received", "DATE_RECEIVED", "devices", "DATE"),
    ColumnDefinition("expiration_date_of_device", "EXPIRATION_DATE_OF_DEVICE", "devices", "DATE"),
    ColumnDefinition("date_returned_to_manufacturer", "DATE_RETURNED_TO_MANUFACTURER", "devices", "DATE"),

    # 2020+ extended columns (optional)
    ColumnDefinition("implant_date_year", "IMPLANT_DATE_YEAR", "devices", "INTEGER",
                     is_optional=True, description="Added in 2020+ format"),
    ColumnDefinition("date_removed_year", "DATE_REMOVED_YEAR", "devices", "INTEGER",
                     is_optional=True, description="Added in 2020+ format"),
    ColumnDefinition("serviced_by_3rd_party_flag", "SERVICED_BY_3RD_PARTY_FLAG", "devices", "VARCHAR",
                     is_optional=True, description="Added in 2020+ format"),
    ColumnDefinition("combination_product_flag", "COMBINATION_PRODUCT_FLAG", "devices", "VARCHAR",
                     is_optional=True, description="Added in 2020+ format"),
    ColumnDefinition("udi_di", "UDI_DI", "devices", "VARCHAR",
                     is_optional=True, description="Unique Device Identifier - Device ID (2020+)"),
    ColumnDefinition("udi_public", "UDI_PUBLIC", "devices", "VARCHAR",
                     is_optional=True, description="Unique Device Identifier - Public (2020+)"),

    # Other
    ColumnDefinition("device_age_text", "DEVICE_AGE_TEXT", "devices", "VARCHAR"),
    ColumnDefinition("device_evaluated_by_manufacturer", "DEVICE_EVALUATED_BY_MANUFACTUR", "devices", "VARCHAR"),
]


# =============================================================================
# TABLE DEFINITIONS - PATIENTS
# =============================================================================

PATIENTS_COLUMNS = [
    ColumnDefinition("mdr_report_key", "MDR_REPORT_KEY", "patients", "BIGINT",
                     is_foreign_key=True, foreign_key_table="master_events", nullable=False),
    ColumnDefinition("patient_sequence_number", "PATIENT_SEQUENCE_NUMBER", "patients", "INTEGER"),
    ColumnDefinition("date_received", "DATE_RECEIVED", "patients", "DATE"),

    # Demographics
    ColumnDefinition("patient_age", "PATIENT_AGE", "patients", "VARCHAR"),
    ColumnDefinition("patient_sex", "PATIENT_SEX", "patients", "VARCHAR"),
    ColumnDefinition("patient_weight", "PATIENT_WEIGHT", "patients", "VARCHAR"),
    ColumnDefinition("patient_ethnicity", "PATIENT_ETHNICITY", "patients", "VARCHAR"),
    ColumnDefinition("patient_race", "PATIENT_RACE", "patients", "VARCHAR"),

    # Derived/computed
    ColumnDefinition("patient_age_numeric", "", "patients", "DOUBLE",
                     description="Numeric age extracted from patient_age"),

    # Outcome flags (computed from sequence_number_outcome)
    ColumnDefinition("outcome_death", "", "patients", "BOOLEAN"),
    ColumnDefinition("outcome_life_threatening", "", "patients", "BOOLEAN"),
    ColumnDefinition("outcome_hospitalization", "", "patients", "BOOLEAN"),
    ColumnDefinition("outcome_disability", "", "patients", "BOOLEAN"),
    ColumnDefinition("outcome_congenital_anomaly", "", "patients", "BOOLEAN"),
    ColumnDefinition("outcome_required_intervention", "", "patients", "BOOLEAN"),
    ColumnDefinition("outcome_other", "", "patients", "BOOLEAN"),
]


# =============================================================================
# TABLE DEFINITIONS - MDR_TEXT
# =============================================================================

MDR_TEXT_COLUMNS = [
    ColumnDefinition("mdr_report_key", "MDR_REPORT_KEY", "mdr_text", "BIGINT",
                     is_foreign_key=True, foreign_key_table="master_events", nullable=False),
    ColumnDefinition("mdr_text_key", "MDR_TEXT_KEY", "mdr_text", "BIGINT"),
    ColumnDefinition("text_type_code", "TEXT_TYPE_CODE", "mdr_text", "VARCHAR"),
    ColumnDefinition("patient_sequence_number", "PATIENT_SEQUENCE_NUMBER", "mdr_text", "INTEGER"),
    ColumnDefinition("date_report", "DATE_REPORT", "mdr_text", "DATE"),
    ColumnDefinition("text_content", "FOI_TEXT", "mdr_text", "VARCHAR"),
]


# =============================================================================
# TABLE DEFINITIONS - DEVICE_PROBLEMS
# =============================================================================

DEVICE_PROBLEMS_COLUMNS = [
    ColumnDefinition("mdr_report_key", "MDR_REPORT_KEY", "device_problems", "BIGINT",
                     is_foreign_key=True, foreign_key_table="master_events", nullable=False),
    ColumnDefinition("device_problem_code", "DEVICE_PROBLEM_CODE", "device_problems", "VARCHAR"),
]


# =============================================================================
# TABLE DEFINITIONS - PATIENT_PROBLEMS
# =============================================================================

PATIENT_PROBLEMS_COLUMNS = [
    ColumnDefinition("mdr_report_key", "MDR_REPORT_KEY", "patient_problems", "BIGINT",
                     is_foreign_key=True, foreign_key_table="master_events", nullable=False),
    ColumnDefinition("patient_sequence_number", "PATIENT_SEQUENCE_NO", "patient_problems", "INTEGER"),
    ColumnDefinition("patient_problem_code", "PROBLEM_CODE", "patient_problems", "VARCHAR"),
    ColumnDefinition("date_added", "DATE_ADDED", "patient_problems", "DATE"),
    ColumnDefinition("date_changed", "DATE_CHANGED", "patient_problems", "DATE"),
]


# =============================================================================
# UNIFIED SCHEMA REGISTRY
# =============================================================================

class UnifiedSchemaRegistry:
    """
    Central registry for all schema definitions.

    This is the single source of truth for:
    - Table structures and column definitions
    - Event type mappings and codes
    - Patient outcome definitions
    - Text type definitions
    - Schema evolution tracking
    """

    VERSION = SCHEMA_VERSION

    def __init__(self):
        self.tables: Dict[str, TableDefinition] = {}
        self._initialize_tables()

    def _initialize_tables(self):
        """Initialize all table definitions."""
        self.tables["master_events"] = TableDefinition(
            name="master_events",
            display_name="Master Events",
            description="Primary MDR event records",
            primary_key="mdr_report_key",
            columns=MASTER_EVENTS_COLUMNS,
        )

        self.tables["devices"] = TableDefinition(
            name="devices",
            display_name="Devices",
            description="Device information linked to events",
            primary_key="device_event_key",
            columns=DEVICES_COLUMNS,
            foreign_keys={"mdr_report_key": "master_events"},
        )

        self.tables["patients"] = TableDefinition(
            name="patients",
            display_name="Patients",
            description="Patient information and outcomes",
            primary_key="mdr_report_key",
            columns=PATIENTS_COLUMNS,
            foreign_keys={"mdr_report_key": "master_events"},
        )

        self.tables["mdr_text"] = TableDefinition(
            name="mdr_text",
            display_name="MDR Text",
            description="Narrative text records",
            primary_key="mdr_text_key",
            columns=MDR_TEXT_COLUMNS,
            foreign_keys={"mdr_report_key": "master_events"},
        )

        self.tables["device_problems"] = TableDefinition(
            name="device_problems",
            display_name="Device Problems",
            description="Device problem codes linked to events",
            primary_key="mdr_report_key",
            columns=DEVICE_PROBLEMS_COLUMNS,
            foreign_keys={"mdr_report_key": "master_events"},
        )

        self.tables["patient_problems"] = TableDefinition(
            name="patient_problems",
            display_name="Patient Problems",
            description="Patient problem codes linked to events",
            primary_key="mdr_report_key",
            columns=PATIENT_PROBLEMS_COLUMNS,
            foreign_keys={"mdr_report_key": "master_events"},
        )

    # -------------------------------------------------------------------------
    # Table/Column Access Methods
    # -------------------------------------------------------------------------

    def get_table(self, table_name: str) -> Optional[TableDefinition]:
        """Get table definition by name."""
        return self.tables.get(table_name)

    def get_column(self, table_name: str, column_name: str) -> Optional[ColumnDefinition]:
        """Get column definition by table and column name."""
        table = self.tables.get(table_name)
        if table:
            return table.get_column(column_name)
        return None

    def get_all_columns(self, table_name: str) -> List[str]:
        """Get all column names for a table."""
        table = self.tables.get(table_name)
        return table.get_column_names() if table else []

    def validate_columns_exist(self, table_name: str, columns: List[str]) -> Dict[str, bool]:
        """Check which columns exist in a table."""
        table = self.tables.get(table_name)
        if not table:
            return {col: False for col in columns}
        return {col: table.has_column(col) for col in columns}

    def get_available_columns(self, table_name: str, requested: List[str]) -> List[str]:
        """Filter requested columns to only those that exist."""
        table = self.tables.get(table_name)
        if not table:
            return []
        return [col for col in requested if table.has_column(col)]

    def get_optional_columns(self, table_name: str) -> List[str]:
        """Get columns that may not exist in all schema versions."""
        return SchemaEvolution.get_optional_columns(table_name)

    # -------------------------------------------------------------------------
    # Event Type Methods
    # -------------------------------------------------------------------------

    def get_event_type(self, code: str) -> Optional[EventTypeDefinition]:
        """Get event type definition by database code."""
        return EVENT_TYPES.get(code)

    def get_all_event_types(self) -> Dict[str, EventTypeDefinition]:
        """Get all event type definitions."""
        return EVENT_TYPES.copy()

    def convert_filter_to_db_code(self, filter_code: str) -> str:
        """Convert filter code to database code (e.g., I -> IN)."""
        return EVENT_TYPE_FILTER_TO_DB.get(filter_code, filter_code)

    def convert_db_to_filter_code(self, db_code: str) -> str:
        """Convert database code to filter code (e.g., IN -> I)."""
        return EVENT_TYPE_DB_TO_FILTER.get(db_code, db_code)

    def convert_filter_event_types(self, filter_codes: List[str]) -> List[str]:
        """Convert list of filter codes to database codes."""
        return [self.convert_filter_to_db_code(code) for code in filter_codes]

    def get_event_type_name(self, code: str) -> str:
        """Get display name for event type code."""
        event_type = EVENT_TYPES.get(code)
        return event_type.name if event_type else code

    # -------------------------------------------------------------------------
    # Outcome Methods
    # -------------------------------------------------------------------------

    def get_outcome(self, code: str) -> Optional[OutcomeDefinition]:
        """Get outcome definition by code."""
        return OUTCOME_CODES.get(code)

    def get_all_outcomes(self) -> Dict[str, OutcomeDefinition]:
        """Get all outcome definitions."""
        return OUTCOME_CODES.copy()

    def get_outcome_name(self, code: str) -> str:
        """Get display name for outcome code."""
        outcome = OUTCOME_CODES.get(code)
        return outcome.name if outcome else code

    # -------------------------------------------------------------------------
    # Text Type Methods
    # -------------------------------------------------------------------------

    def get_text_type(self, code: str) -> Optional[TextTypeDefinition]:
        """Get text type definition by code."""
        return TEXT_TYPE_CODES.get(code)

    def get_all_text_types(self) -> Dict[str, TextTypeDefinition]:
        """Get all text type definitions."""
        return TEXT_TYPE_CODES.copy()

    def get_text_type_name(self, code: str) -> str:
        """Get display name for text type code."""
        text_type = TEXT_TYPE_CODES.get(code)
        return text_type.name if text_type else code

    # -------------------------------------------------------------------------
    # Export Methods
    # -------------------------------------------------------------------------

    def export_to_dict(self) -> Dict[str, Any]:
        """Export entire schema registry to dictionary for JSON serialization."""
        return {
            "version": self.VERSION,
            "tables": {
                name: {
                    "name": table.name,
                    "display_name": table.display_name,
                    "description": table.description,
                    "primary_key": table.primary_key,
                    "columns": [
                        {
                            "db_name": col.db_name,
                            "fda_name": col.fda_name,
                            "data_type": col.data_type,
                            "nullable": col.nullable,
                            "api_name": col.api_name,
                            "ts_type": col.ts_type,
                            "description": col.description,
                            "is_primary_key": col.is_primary_key,
                            "is_foreign_key": col.is_foreign_key,
                            "is_optional": col.is_optional,
                        }
                        for col in table.columns
                    ],
                    "foreign_keys": table.foreign_keys,
                }
                for name, table in self.tables.items()
            },
            "event_types": {
                code: {
                    "db_code": et.db_code,
                    "filter_code": et.filter_code,
                    "name": et.name,
                    "description": et.description,
                    "severity": et.severity,
                    "color": et.color,
                    "bg_class": et.bg_class,
                    "text_class": et.text_class,
                }
                for code, et in EVENT_TYPES.items()
            },
            "outcome_codes": {
                code: {
                    "code": out.code,
                    "name": out.name,
                    "db_field": out.db_field,
                    "severity": out.severity,
                    "color_class": out.color_class,
                }
                for code, out in OUTCOME_CODES.items()
            },
            "text_type_codes": {
                code: {
                    "code": tt.code,
                    "name": tt.name,
                    "description": tt.description,
                    "priority": tt.priority,
                }
                for code, tt in TEXT_TYPE_CODES.items()
            },
        }

    def export_to_json(self, indent: int = 2) -> str:
        """Export entire schema registry to JSON string."""
        return json.dumps(self.export_to_dict(), indent=indent)


# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

_registry_instance: Optional[UnifiedSchemaRegistry] = None


def get_schema_registry() -> UnifiedSchemaRegistry:
    """Get singleton instance of the schema registry."""
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = UnifiedSchemaRegistry()
    return _registry_instance


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def get_event_type_code(filter_code: str) -> str:
    """Convert filter code to database code (convenience function)."""
    return get_schema_registry().convert_filter_to_db_code(filter_code)


def get_event_type_name(code: str) -> str:
    """Get display name for event type code (convenience function)."""
    return get_schema_registry().get_event_type_name(code)


def convert_filter_event_types(filter_codes: List[str]) -> List[str]:
    """Convert list of filter codes to database codes (convenience function)."""
    return get_schema_registry().convert_filter_event_types(filter_codes)


def validate_table_columns(table: str, columns: List[str]) -> Dict[str, bool]:
    """Validate which columns exist in a table (convenience function)."""
    return get_schema_registry().validate_columns_exist(table, columns)


# =============================================================================
# FDA COLUMN MAPPINGS (for backward compatibility with ingestion)
# =============================================================================

def get_fda_to_db_mapping(file_type: str) -> Dict[str, str]:
    """
    Get FDA column name to database column name mapping.

    This provides backward compatibility with the ingestion pipeline
    that expects column_mappings.py format.
    """
    registry = get_schema_registry()

    # Map file types to table names
    table_map = {
        "master": "master_events",
        "device": "devices",
        "patient": "patients",
        "text": "mdr_text",
        "problem": "device_problems",
        "patient_problem": "patient_problems",
    }

    table_name = table_map.get(file_type)
    if not table_name:
        return {}

    table = registry.get_table(table_name)
    if not table:
        return {}

    # Build mapping from FDA name to DB name
    return {
        col.fda_name: col.db_name
        for col in table.columns
        if col.fda_name  # Skip computed columns without FDA names
    }


def get_db_to_fda_mapping(file_type: str) -> Dict[str, str]:
    """Get reverse mapping (DB column name to FDA column name)."""
    fda_to_db = get_fda_to_db_mapping(file_type)
    return {v: k for k, v in fda_to_db.items() if v}
