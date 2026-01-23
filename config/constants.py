"""Constants for MAUDE Analyzer application."""

# =============================================================================
# Product Codes
# =============================================================================

# SCS Product Codes (Primary Focus)
SCS_PRODUCT_CODES = ["GZB", "LGW", "PMP"]

# SCS Related Codes (Leads, related devices)
SCS_RELATED_CODES = ["GZF", "GZD", "GZE"]

# All SCS-related product codes
ALL_SCS_CODES = SCS_PRODUCT_CODES + SCS_RELATED_CODES

# Product code descriptions
PRODUCT_CODE_DESCRIPTIONS = {
    "GZB": "Stimulator, Spinal-cord, Totally Implanted For Pain Relief",
    "LGW": "Stimulator, Spinal-cord, Totally Implanted For Pain Relief",
    "PMP": "Stimulator, Electrical, Implanted, For Pain Relief",
    "GZF": "Electrode, Spinal Cord Stimulator",
    "GZD": "Stimulator, Cerebellar",
    "GZE": "Stimulator, Transcranial",
}

# =============================================================================
# Manufacturers
# =============================================================================

# Major SCS Manufacturers
SCS_MANUFACTURERS = [
    "Abbott",
    "Medtronic",
    "Boston Scientific",
    "Nevro",
    "Stimwave",
    "Nalu Medical",
    "Saluda Medical",
]

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

MANUFACTURER_COLORS = {
    "Abbott": "#1f77b4",  # Blue
    "Medtronic": "#ff7f0e",  # Orange
    "Boston Scientific": "#2ca02c",  # Green
    "Nevro": "#d62728",  # Red
    "Stimwave": "#9467bd",  # Purple
    "Nalu Medical": "#8c564b",  # Brown
    "Saluda Medical": "#e377c2",  # Pink
    "Other": "#7f7f7f",  # Gray
}

EVENT_TYPE_COLORS = {
    "Death": "#d62728",  # Red
    "Injury": "#ff7f0e",  # Orange
    "Malfunction": "#1f77b4",  # Blue
    "Other": "#7f7f7f",  # Gray
}

SEQUENTIAL_PALETTES = {
    "blue": "Blues",
    "red": "Reds",
    "green": "Greens",
    "neutral": "Greys",
    "heatmap": "RdYlBu_r",
}

# Simple color scheme for charts
CHART_COLORS = {
    "primary": "#1f77b4",  # Blue
    "secondary": "#2ca02c",  # Green
    "death": "#d62728",  # Red
    "injury": "#ff7f0e",  # Orange
    "malfunction": "#1f77b4",  # Blue
    "other": "#7f7f7f",  # Gray
}

# =============================================================================
# File Patterns
# =============================================================================

# FDA MAUDE File Patterns (regex)
MAUDE_FILE_PATTERNS = {
    "master": r"mdrfoi.*\.txt",
    "device": r"foidev.*\.txt",
    "patient": r"patient.*\.txt",
    "text": r"foitext.*\.txt",
    "problem": r"foidevproblem.*\.txt",
}

# FDA Weekly Incremental File Patterns
# ADD files: New records added during the week
# CHANGE files: Updates/corrections to existing records
# Released on Thursdays
MAUDE_INCREMENTAL_PATTERNS = {
    "master": {
        "add": r"mdrfoiAdd.*\.txt",
        "change": r"mdrfoiChange.*\.txt",
    },
    "device": {
        "add": r"foidevAdd.*\.txt",
        "change": None,  # Device doesn't have change files
    },
    "patient": {
        "add": r"patientAdd.*\.txt",
        "change": r"patientChange.*\.txt",
    },
    "text": {
        "add": r"foitextAdd.*\.txt",
        "change": None,  # Text doesn't have change files
    },
    "problem": {
        "add": None,
        "change": None,
    },
}

# FDA File Categories
FILE_CATEGORIES = {
    "current": "Current year files, updated weekly (e.g., mdrfoi.zip)",
    "add": "Weekly addition files with new records (e.g., mdrfoiAdd.zip)",
    "change": "Weekly change files with updates to existing records (e.g., mdrfoiChange.zip)",
    "annual": "Annual archive files with historical data (e.g., mdrfoithru2024.zip)",
}

# =============================================================================
# Date Formats
# =============================================================================

# Date formats to try when parsing MAUDE data
DATE_FORMATS = [
    "%m/%d/%Y",  # 01/15/2024
    "%Y-%m-%d",  # 2024-01-15
    "%Y%m%d",  # 20240115
    "%d-%b-%Y",  # 15-Jan-2024
    "%B %d, %Y",  # January 15, 2024
    "%m-%d-%Y",  # 01-15-2024
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
        "scale": 2,  # 2x resolution for clarity
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
