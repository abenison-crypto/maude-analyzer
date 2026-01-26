"""Constants for MAUDE Analyzer application.

This module provides backward-compatible constants that are now loaded from
YAML configuration files. Import from here for static constants or use
config_loader for dynamic configuration access.

IMPORTANT: All defaults are EMPTY (all products/manufacturers/event types).
No product category is prioritized over another.
"""

from typing import Dict, List, Optional

# Import YAML-based configuration
try:
    from config.config_loader import (
        get_event_types,
        get_outcome_codes,
        get_text_type_codes,
        get_manufacturer_mappings,
        get_filter_presets,
        get_product_groups,
        get_ui_colors,
        get_table_config,
        get_data_quality_config,
        load_data_mappings,
        load_presets,
        load_ui_config,
    )
    _YAML_AVAILABLE = True
except ImportError:
    _YAML_AVAILABLE = False


# =============================================================================
# Default Filter Values - ALWAYS ALL DATA
# =============================================================================

# Default filter values - EMPTY means ALL data, no filtering
DEFAULT_FILTER_PRODUCT_CODES: List[str] = []  # Empty = ALL products
DEFAULT_FILTER_MANUFACTURERS: List[str] = []  # Empty = ALL manufacturers
DEFAULT_FILTER_EVENT_TYPES: List[str] = []    # Empty = ALL event types


# =============================================================================
# Product Code Helpers
# =============================================================================

def get_product_code_description(code: str) -> str:
    """Get description for a product code."""
    if _YAML_AVAILABLE:
        mappings = load_data_mappings()
        descriptions = mappings.get("product_code_descriptions", {})
        return descriptions.get(code, code)
    return PRODUCT_CODE_DESCRIPTIONS.get(code, code)


# Product code descriptions (static fallback, incomplete - use lookup table)
PRODUCT_CODE_DESCRIPTIONS = {
    "GZB": "Stimulator, Spinal-cord, Totally Implanted For Pain Relief",
    "LGW": "Stimulator, Spinal-cord, Totally Implanted For Pain Relief",
    "PMP": "Stimulator, Electrical, Implanted, For Pain Relief",
    "GZF": "Electrode, Spinal Cord Stimulator",
    "GZD": "Stimulator, Cerebellar",
    "GZE": "Stimulator, Transcranial",
}


# =============================================================================
# Manufacturer Helpers
# =============================================================================

def get_standardized_manufacturer(raw_name: Optional[str]) -> str:
    """Standardize a manufacturer name using YAML config or fallback."""
    if not raw_name:
        return "Unknown"
    if _YAML_AVAILABLE:
        mappings = get_manufacturer_mappings()
        return mappings.standardize(raw_name)
    # Fallback to static mapping
    return MANUFACTURER_MAPPINGS.get(raw_name.upper().strip(), raw_name)


def get_filter_presets_dict() -> Dict[str, Dict]:
    """Get filter presets from YAML config or fallback."""
    if _YAML_AVAILABLE:
        presets = get_filter_presets()
        return presets.get_all_presets()
    return FILTER_PRESETS


# =============================================================================
# Filter Presets - "All Products" is the ONLY default
# =============================================================================

FILTER_PRESETS = {
    "All Products": {
        "product_codes": [],
        "manufacturers": [],
        "event_types": [],
        "description": "Show all products and manufacturers in the database",
        "is_default": True,
    },
    "Spinal Cord Stimulators": {
        "product_codes": ["GZB", "LGW", "PMP"],
        "manufacturers": [],
        "event_types": [],
        "description": "Spinal Cord Stimulation devices (all manufacturers)",
        "is_default": False,
    },
    "Cardiac Devices": {
        "product_codes": ["DXY", "LWS", "NIK"],
        "manufacturers": [],
        "event_types": [],
        "description": "Pacemakers, ICDs, and cardiac leads",
        "is_default": False,
    },
    "Death Events Only": {
        "product_codes": [],
        "manufacturers": [],
        "event_types": ["D"],
        "description": "Only reports with patient death",
        "is_default": False,
    },
}

# Manufacturer name variations mapping to standardized names
MANUFACTURER_MAPPINGS = {
    # Abbott (including St. Jude acquisition)
    "ABBOTT LABORATORIES": "Abbott",
    "ABBOTT": "Abbott",
    "ABBOTT MEDICAL": "Abbott",
    "ABBOTT NEUROMODULATION": "Abbott",
    "ST. JUDE MEDICAL": "Abbott",
    "ST JUDE MEDICAL": "Abbott",
    "ST. JUDE MEDICAL, INC.": "Abbott",
    "ST JUDE MEDICAL INC": "Abbott",
    "ST. JUDE NEUROMODULATION": "Abbott",
    # Medtronic
    "MEDTRONIC": "Medtronic",
    "MEDTRONIC, INC.": "Medtronic",
    "MEDTRONIC INC": "Medtronic",
    "MEDTRONIC INC.": "Medtronic",
    "MEDTRONIC NEUROMODULATION": "Medtronic",
    "MEDTRONIC SOFAMOR DANEK": "Medtronic",
    "MEDTRONIC SPINE LLC": "Medtronic",
    # Boston Scientific (including ANS acquisition)
    "BOSTON SCIENTIFIC": "Boston Scientific",
    "BOSTON SCIENTIFIC CORPORATION": "Boston Scientific",
    "BOSTON SCIENTIFIC CORP": "Boston Scientific",
    "BOSTON SCIENTIFIC CORP.": "Boston Scientific",
    "BOSTON SCIENTIFIC NEUROMODULATION": "Boston Scientific",
    "BOSTON SCIENTIFIC NEUROMODULATION CORP": "Boston Scientific",
    "ADVANCED NEUROMODULATION SYSTEMS": "Boston Scientific",
    "ADVANCED NEUROMODULATION SYSTEMS, INC.": "Boston Scientific",
    "ANS": "Boston Scientific",
    # Nevro
    "NEVRO": "Nevro",
    "NEVRO CORP": "Nevro",
    "NEVRO CORP.": "Nevro",
    "NEVRO CORPORATION": "Nevro",
    # Stimwave
    "STIMWAVE": "Stimwave",
    "STIMWAVE TECHNOLOGIES": "Stimwave",
    "STIMWAVE TECHNOLOGIES INC": "Stimwave",
    "STIMWAVE TECHNOLOGIES INCORPORATED": "Stimwave",
    # Nalu Medical
    "NALU": "Nalu Medical",
    "NALU MEDICAL": "Nalu Medical",
    "NALU MEDICAL, INC.": "Nalu Medical",
    # Saluda Medical
    "SALUDA": "Saluda Medical",
    "SALUDA MEDICAL": "Saluda Medical",
    "SALUDA MEDICAL PTY LTD": "Saluda Medical",
}


# =============================================================================
# Event Types and Codes
# =============================================================================

def get_event_type_name(code: str) -> str:
    """Get display name for event type code."""
    if _YAML_AVAILABLE:
        event_types = get_event_types()
        return event_types.get_name(code)
    return EVENT_TYPES.get(code, code)


def get_outcome_code_name(code: str) -> str:
    """Get display name for outcome code."""
    if _YAML_AVAILABLE:
        outcome_codes = get_outcome_codes()
        return outcome_codes.get_name(code)
    return OUTCOME_CODES.get(code, code)


def get_text_type_name(code: str) -> str:
    """Get display name for text type code."""
    if _YAML_AVAILABLE:
        text_types = get_text_type_codes()
        return text_types.get_name(code)
    return TEXT_TYPE_CODES.get(code, code)


# Event Types
EVENT_TYPES = {
    "D": "Death",
    "IN": "Injury",
    "M": "Malfunction",
    "O": "Other",
    "*": "No Answer Provided",
}

# Patient Outcome Codes
OUTCOME_CODES = {
    "D": "Death",
    "L": "Life Threatening",
    "H": "Hospitalization",
    "DS": "Disability",
    "CA": "Congenital Anomaly",
    "RI": "Required Intervention",
    "OT": "Other",
}

# Patient Treatment Codes (FDA MAUDE treatment categories)
TREATMENT_CODES = {
    "1": "Drug",
    "2": "Device",
    "3": "Surgery",
    "4": "Other",
    "5": "Unknown",
    "6": "No Information",
    "7": "Blood Products",
    "8": "Hospitalization/Extended Care",
    "9": "Physical Therapy/Rehabilitation",
}


def get_treatment_code_name(code: str) -> str:
    """Get display name for treatment code."""
    return TREATMENT_CODES.get(str(code), f"Code {code}")


# Text Type Codes
TEXT_TYPE_CODES = {
    "D": "Event Description",
    "E": "Evaluation Summary",
    "H": "Event History",
    "M": "Manufacturer Narrative",
    "N": "Additional Information",
}


# =============================================================================
# Colors for Visualizations
# =============================================================================

def get_manufacturer_color(manufacturer: str) -> str:
    """Get color for a manufacturer."""
    if _YAML_AVAILABLE:
        colors = get_ui_colors()
        return colors.get_manufacturer_color(manufacturer)
    return MANUFACTURER_COLORS.get(manufacturer, MANUFACTURER_COLORS.get("Other", "#7f7f7f"))


def get_event_type_color(event_type: str) -> str:
    """Get color for an event type."""
    if _YAML_AVAILABLE:
        colors = get_ui_colors()
        return colors.get_event_type_color(event_type)
    return EVENT_TYPE_COLORS.get(event_type, EVENT_TYPE_COLORS.get("Other", "#7f7f7f"))


MANUFACTURER_COLORS = {
    "Abbott": "#1f77b4",
    "Medtronic": "#ff7f0e",
    "Boston Scientific": "#2ca02c",
    "Nevro": "#d62728",
    "Stimwave": "#9467bd",
    "Nalu Medical": "#8c564b",
    "Saluda Medical": "#e377c2",
    "Other": "#7f7f7f",
}

EVENT_TYPE_COLORS = {
    "Death": "#d62728",
    "Injury": "#ff7f0e",
    "Malfunction": "#1f77b4",
    "Other": "#7f7f7f",
}

SEQUENTIAL_PALETTES = {
    "blue": "Blues",
    "red": "Reds",
    "green": "Greens",
    "neutral": "Greys",
    "heatmap": "RdYlBu_r",
}

CHART_COLORS = {
    "primary": "#1f77b4",
    "secondary": "#2ca02c",
    "death": "#d62728",
    "injury": "#ff7f0e",
    "malfunction": "#1f77b4",
    "other": "#7f7f7f",
}


# =============================================================================
# File Patterns
# =============================================================================

MAUDE_FILE_PATTERNS = {
    "master": r"mdrfoi.*\.txt",
    "device": r"foidev.*\.txt",
    "patient": r"patient.*\.txt",
    "text": r"foitext.*\.txt",
    "problem": r"foidevproblem.*\.txt",
}

MAUDE_INCREMENTAL_PATTERNS = {
    "master": {
        "add": r"mdrfoiAdd.*\.txt",
        "change": r"mdrfoiChange.*\.txt",
    },
    "device": {
        "add": r"foidevAdd.*\.txt",
        "change": None,
    },
    "patient": {
        "add": r"patientAdd.*\.txt",
        "change": r"patientChange.*\.txt",
    },
    "text": {
        "add": r"foitextAdd.*\.txt",
        "change": None,
    },
    "problem": {
        "add": None,
        "change": None,
    },
}

FILE_CATEGORIES = {
    "current": "Current year files, updated weekly (e.g., mdrfoi.zip)",
    "add": "Weekly addition files with new records (e.g., mdrfoiAdd.zip)",
    "change": "Weekly change files with updates to existing records (e.g., mdrfoiChange.zip)",
    "annual": "Annual archive files with historical data (e.g., mdrfoithru2024.zip)",
}


# =============================================================================
# Date Formats
# =============================================================================

DATE_FORMATS = [
    "%m/%d/%Y",
    "%Y-%m-%d",
    "%Y%m%d",
    "%d-%b-%Y",
    "%B %d, %Y",
    "%m-%d-%Y",
]


# =============================================================================
# Time Aggregations
# =============================================================================

TIME_AGGREGATIONS = {
    "daily": {
        "sql_format": "DATE_TRUNC('day', date_received)",
        "display_format": "%Y-%m-%d",
        "typical_use": "Recent detailed analysis",
    },
    "weekly": {
        "sql_format": "DATE_TRUNC('week', date_received)",
        "display_format": "%Y-W%W",
        "typical_use": "Short-term trends",
    },
    "monthly": {
        "sql_format": "DATE_TRUNC('month', date_received)",
        "display_format": "%Y-%m",
        "typical_use": "Standard trend analysis",
    },
    "quarterly": {
        "sql_format": "DATE_TRUNC('quarter', date_received)",
        "display_format": "%Y-Q%q",
        "typical_use": "Business reporting",
    },
    "yearly": {
        "sql_format": "DATE_TRUNC('year', date_received)",
        "display_format": "%Y",
        "typical_use": "Long-term trends",
    },
}


# =============================================================================
# Search Operators
# =============================================================================

SEARCH_OPERATORS = {
    "equals": "=",
    "not_equals": "!=",
    "contains": "LIKE '%{value}%'",
    "starts_with": "LIKE '{value}%'",
    "ends_with": "LIKE '%{value}'",
    "in": "IN ({values})",
    "not_in": "NOT IN ({values})",
    "between": "BETWEEN {start} AND {end}",
    "greater_than": ">",
    "less_than": "<",
    "is_null": "IS NULL",
    "is_not_null": "IS NOT NULL",
}


# =============================================================================
# Export Configuration
# =============================================================================

EXCEL_EXPORT_CONFIG = {
    "sheets": {
        "Summary": {
            "columns": [
                "mdr_report_key",
                "date_received",
                "manufacturer_clean",
                "brand_name",
                "event_type",
                "product_code",
            ],
            "freeze_panes": "A2",
            "auto_filter": True,
        },
        "Device Details": {
            "columns": [
                "mdr_report_key",
                "brand_name",
                "generic_name",
                "model_number",
                "lot_number",
                "manufacturer_d_name",
            ],
        },
        "Patient Outcomes": {
            "columns": [
                "mdr_report_key",
                "outcome_death",
                "outcome_hospitalization",
                "outcome_disability",
                "outcome_life_threatening",
            ],
        },
        "Narratives": {
            "columns": ["mdr_report_key", "text_type_code", "text_content"],
            "text_wrap": True,
            "column_width": {"text_content": 100},
        },
    },
    "formatting": {
        "header_bg_color": "#4472C4",
        "header_font_color": "#FFFFFF",
        "date_format": "YYYY-MM-DD",
        "number_format": "#,##0",
    },
}

CHART_EXPORT_CONFIG = {
    "png": {
        "scale": 2,
        "width": 1200,
        "height": 800,
    },
    "svg": {
        "width": 1200,
        "height": 800,
    },
    "html": {
        "include_plotlyjs": True,
        "full_html": True,
    },
}
