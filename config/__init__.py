"""Configuration module for MAUDE Analyzer.

All defaults are "all products/manufacturers" - no product category is prioritized.
"""

from .settings import config, DatabaseConfig, APIConfig, AppConfig, Config
from .constants import (
    # Default filter values (always empty = all data)
    DEFAULT_FILTER_PRODUCT_CODES,
    DEFAULT_FILTER_MANUFACTURERS,
    DEFAULT_FILTER_EVENT_TYPES,
    # Code mappings
    EVENT_TYPES,
    OUTCOME_CODES,
    TREATMENT_CODES,
    TEXT_TYPE_CODES,
    # Colors
    MANUFACTURER_COLORS,
    EVENT_TYPE_COLORS,
    CHART_COLORS,
    # File patterns
    MAUDE_FILE_PATTERNS,
    DATE_FORMATS,
    # Mappings
    MANUFACTURER_MAPPINGS,
    PRODUCT_CODE_DESCRIPTIONS,
    FILTER_PRESETS,
    # Helper functions
    get_event_type_name,
    get_outcome_code_name,
    get_treatment_code_name,
    get_text_type_name,
    get_manufacturer_color,
    get_event_type_color,
    get_standardized_manufacturer,
    get_product_code_description,
    get_filter_presets_dict,
)
from .schema_registry import (
    FDA_FILE_COLUMNS,
    MASTER_COLUMNS_FDA,
    DEVICE_COLUMNS_FDA,
    PATIENT_COLUMNS_FDA,
    TEXT_COLUMNS_FDA,
    PROBLEM_COLUMNS_FDA,
    HEADERLESS_FILES,
    EXPECTED_COLUMN_COUNTS,
    DATE_COLUMNS,
    INTEGER_COLUMNS,
    FLAG_COLUMNS,
    get_fda_columns,
    is_headerless_file,
    get_expected_column_count,
    validate_schema,
)
from .column_mappings import (
    COLUMN_MAPPINGS,
    MASTER_COLUMN_MAPPING,
    DEVICE_COLUMN_MAPPING,
    PATIENT_COLUMN_MAPPING,
    TEXT_COLUMN_MAPPING,
    PROBLEM_COLUMN_MAPPING,
    get_db_column_name,
    get_fda_column_name,
    map_record_columns,
    get_all_db_columns,
)

__all__ = [
    # Settings
    "config",
    "DatabaseConfig",
    "APIConfig",
    "AppConfig",
    "Config",
    # Default filter values
    "DEFAULT_FILTER_PRODUCT_CODES",
    "DEFAULT_FILTER_MANUFACTURERS",
    "DEFAULT_FILTER_EVENT_TYPES",
    # Constants
    "EVENT_TYPES",
    "OUTCOME_CODES",
    "TREATMENT_CODES",
    "TEXT_TYPE_CODES",
    "MANUFACTURER_COLORS",
    "EVENT_TYPE_COLORS",
    "CHART_COLORS",
    "MAUDE_FILE_PATTERNS",
    "DATE_FORMATS",
    "MANUFACTURER_MAPPINGS",
    "PRODUCT_CODE_DESCRIPTIONS",
    "FILTER_PRESETS",
    # Helper functions
    "get_event_type_name",
    "get_outcome_code_name",
    "get_treatment_code_name",
    "get_text_type_name",
    "get_manufacturer_color",
    "get_event_type_color",
    "get_standardized_manufacturer",
    "get_product_code_description",
    "get_filter_presets_dict",
    # Schema Registry
    "FDA_FILE_COLUMNS",
    "MASTER_COLUMNS_FDA",
    "DEVICE_COLUMNS_FDA",
    "PATIENT_COLUMNS_FDA",
    "TEXT_COLUMNS_FDA",
    "PROBLEM_COLUMNS_FDA",
    "HEADERLESS_FILES",
    "EXPECTED_COLUMN_COUNTS",
    "DATE_COLUMNS",
    "INTEGER_COLUMNS",
    "FLAG_COLUMNS",
    "get_fda_columns",
    "is_headerless_file",
    "get_expected_column_count",
    "validate_schema",
    # Column Mappings
    "COLUMN_MAPPINGS",
    "MASTER_COLUMN_MAPPING",
    "DEVICE_COLUMN_MAPPING",
    "PATIENT_COLUMN_MAPPING",
    "TEXT_COLUMN_MAPPING",
    "PROBLEM_COLUMN_MAPPING",
    "get_db_column_name",
    "get_fda_column_name",
    "map_record_columns",
    "get_all_db_columns",
]
