"""YAML Configuration Loader for MAUDE Analyzer.

Loads and caches configuration from YAML files with fallback to defaults.
Provides type-safe access to configuration values.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, TypeVar, Union
from dataclasses import dataclass, field
from functools import lru_cache
import yaml

# Get config directory
CONFIG_DIR = Path(__file__).parent

T = TypeVar("T")


class ConfigurationError(Exception):
    """Raised when configuration loading or access fails."""

    pass


def _load_yaml_file(filename: str) -> Dict[str, Any]:
    """
    Load a YAML configuration file.

    Args:
        filename: Name of YAML file in config directory

    Returns:
        Parsed YAML content as dictionary

    Raises:
        ConfigurationError: If file cannot be loaded
    """
    filepath = CONFIG_DIR / filename
    if not filepath.exists():
        raise ConfigurationError(f"Configuration file not found: {filepath}")

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        raise ConfigurationError(f"Error parsing {filename}: {e}")
    except IOError as e:
        raise ConfigurationError(f"Error reading {filename}: {e}")


@lru_cache(maxsize=1)
def load_data_mappings() -> Dict[str, Any]:
    """Load data_mappings.yaml configuration."""
    try:
        return _load_yaml_file("data_mappings.yaml")
    except ConfigurationError:
        # Return minimal fallback defaults
        return {
            "event_types": {
                "codes": {"D": "Death", "IN": "Injury", "M": "Malfunction", "O": "Other"}
            },
            "outcome_codes": {"codes": {}},
            "text_type_codes": {"codes": {}},
            "manufacturer_mappings": {},
        }


@lru_cache(maxsize=1)
def load_presets() -> Dict[str, Any]:
    """Load presets.yaml configuration."""
    try:
        return _load_yaml_file("presets.yaml")
    except ConfigurationError:
        return {
            "presets": {
                "all_products": {
                    "name": "All Products",
                    "description": "Show all products and manufacturers",
                    "filters": {"product_codes": [], "manufacturers": []},
                    "is_default": True,
                }
            }
        }


@lru_cache(maxsize=1)
def load_ui_config() -> Dict[str, Any]:
    """Load ui_config.yaml configuration."""
    try:
        return _load_yaml_file("ui_config.yaml")
    except ConfigurationError:
        return {
            "colors": {
                "manufacturers": {"Other": "#7f7f7f"},
                "event_types": {"Death": "#d62728", "Injury": "#ff7f0e"},
            },
            "tables": {"pagination": {"default_page_size": 25}},
        }


def clear_config_cache() -> None:
    """Clear all cached configuration data."""
    load_data_mappings.cache_clear()
    load_presets.cache_clear()
    load_ui_config.cache_clear()


@dataclass
class EventTypes:
    """Event type configuration accessor."""

    _data: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        mappings = load_data_mappings()
        self._data = mappings.get("event_types", {})

    @property
    def codes(self) -> Dict[str, str]:
        """Get event type code to name mapping."""
        return self._data.get("codes", {})

    @property
    def aliases(self) -> Dict[str, str]:
        """Get event type aliases."""
        return self._data.get("aliases", {})

    @property
    def severity_order(self) -> List[str]:
        """Get event type severity ordering."""
        return self._data.get("severity_order", ["D", "IN", "M", "O"])

    def get_name(self, code: str) -> str:
        """Get display name for event type code."""
        return self.codes.get(code, code)

    def get_code(self, name: str) -> Optional[str]:
        """Get code for event type name."""
        for code, n in self.codes.items():
            if n.lower() == name.lower():
                return code
        return None


@dataclass
class OutcomeCodes:
    """Patient outcome code configuration accessor."""

    _data: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        mappings = load_data_mappings()
        self._data = mappings.get("outcome_codes", {})

    @property
    def codes(self) -> Dict[str, str]:
        """Get outcome code to name mapping."""
        return self._data.get("codes", {})

    @property
    def field_mapping(self) -> Dict[str, str]:
        """Get mapping of outcome codes to database field names."""
        return self._data.get("field_mapping", {})

    @property
    def serious_outcomes(self) -> List[str]:
        """Get list of serious outcome codes."""
        return self._data.get("serious_outcomes", ["D", "L", "H"])

    def get_name(self, code: str) -> str:
        """Get display name for outcome code."""
        return self.codes.get(code, code)

    def get_field_name(self, code: str) -> Optional[str]:
        """Get database field name for outcome code."""
        return self.field_mapping.get(code)

    def is_serious(self, code: str) -> bool:
        """Check if outcome code is considered serious."""
        return code in self.serious_outcomes


@dataclass
class TextTypeCodes:
    """Text type code configuration accessor."""

    _data: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        mappings = load_data_mappings()
        self._data = mappings.get("text_type_codes", {})

    @property
    def codes(self) -> Dict[str, str]:
        """Get text type code to name mapping."""
        return self._data.get("codes", {})

    @property
    def primary_types(self) -> List[str]:
        """Get primary text types for analysis."""
        return self._data.get("primary_types", ["D", "H"])

    @property
    def display_order(self) -> List[str]:
        """Get text types in display order."""
        return self._data.get("display_order", list(self.codes.keys()))

    def get_name(self, code: str) -> str:
        """Get display name for text type code."""
        return self.codes.get(code, code)


@dataclass
class ManufacturerMappings:
    """Manufacturer name standardization configuration."""

    _data: Dict[str, List[str]] = field(default_factory=dict)
    _reverse_lookup: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        mappings = load_data_mappings()
        self._data = mappings.get("manufacturer_mappings", {})
        # Build reverse lookup: raw name -> standard name
        self._reverse_lookup = {}
        for standard_name, raw_names in self._data.items():
            for raw_name in raw_names:
                self._reverse_lookup[raw_name.upper()] = standard_name

    def standardize(self, raw_name: Optional[str]) -> str:
        """
        Standardize a manufacturer name.

        Args:
            raw_name: Raw manufacturer name from data

        Returns:
            Standardized manufacturer name or original if no mapping
        """
        if not raw_name:
            return "Unknown"
        return self._reverse_lookup.get(raw_name.upper().strip(), raw_name)

    def get_raw_names(self, standard_name: str) -> List[str]:
        """Get all raw name variations for a standard name."""
        return self._data.get(standard_name, [])

    def get_all_standard_names(self) -> List[str]:
        """Get list of all standard manufacturer names."""
        return list(self._data.keys())

    def is_known_manufacturer(self, name: str) -> bool:
        """Check if manufacturer is in our mapping."""
        return name.upper().strip() in self._reverse_lookup


@dataclass
class FilterPresets:
    """Filter preset configuration accessor."""

    _data: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        presets = load_presets()
        self._data = presets.get("presets", {})

    def get_preset(self, key: str) -> Optional[Dict[str, Any]]:
        """Get a specific preset by key."""
        return self._data.get(key)

    def get_all_presets(self) -> Dict[str, Dict[str, Any]]:
        """Get all presets."""
        return self._data

    def get_default_preset(self) -> Optional[Dict[str, Any]]:
        """Get the default preset."""
        for preset in self._data.values():
            if preset.get("is_default"):
                return preset
        return list(self._data.values())[0] if self._data else None

    def get_preset_names(self) -> List[str]:
        """Get list of preset display names."""
        return [p.get("name", key) for key, p in self._data.items()]


@dataclass
class ProductGroups:
    """Product code group configuration accessor."""

    _data: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        presets = load_presets()
        self._data = presets.get("product_groups", {})

    def get_group(self, key: str) -> Optional[Dict[str, Any]]:
        """Get a specific product group."""
        return self._data.get(key)

    def get_codes(self, key: str) -> List[str]:
        """Get product codes for a group."""
        group = self._data.get(key, {})
        return group.get("codes", [])

    def get_all_groups(self) -> Dict[str, Dict[str, Any]]:
        """Get all product groups."""
        return self._data


@dataclass
class UIColors:
    """UI color configuration accessor."""

    _data: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        ui_config = load_ui_config()
        self._data = ui_config.get("colors", {})

    def get_manufacturer_color(self, manufacturer: str) -> str:
        """Get color for a manufacturer."""
        colors = self._data.get("manufacturers", {})
        return colors.get(manufacturer, colors.get("Other", "#7f7f7f"))

    def get_event_type_color(self, event_type: str) -> str:
        """Get color for an event type."""
        colors = self._data.get("event_types", {})
        return colors.get(event_type, colors.get("Other", "#7f7f7f"))

    def get_chart_color(self, key: str) -> str:
        """Get a chart color by key."""
        colors = self._data.get("chart", {})
        return colors.get(key, "#1f77b4")

    def get_status_color(self, status: str) -> str:
        """Get a status indicator color."""
        colors = self._data.get("status", {})
        return colors.get(status, "#7f7f7f")

    def get_data_quality_color(self, level: str) -> str:
        """Get data quality indicator color."""
        colors = self._data.get("data_quality", {})
        return colors.get(level, "#7f7f7f")


@dataclass
class TableConfig:
    """Table display configuration accessor."""

    _data: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        ui_config = load_ui_config()
        self._data = ui_config.get("tables", {})

    @property
    def default_page_size(self) -> int:
        """Get default pagination page size."""
        return self._data.get("pagination", {}).get("default_page_size", 25)

    @property
    def page_size_options(self) -> List[int]:
        """Get available page size options."""
        return self._data.get("pagination", {}).get("page_size_options", [10, 25, 50, 100])

    @property
    def max_page_size(self) -> int:
        """Get maximum allowed page size."""
        return self._data.get("pagination", {}).get("max_page_size", 500)

    @property
    def null_display(self) -> str:
        """Get display text for NULL values."""
        return self._data.get("display", {}).get("null_display", "N/A")

    @property
    def date_format(self) -> str:
        """Get date display format."""
        return self._data.get("display", {}).get("date_format", "%Y-%m-%d")

    @property
    def truncate_text_at(self) -> int:
        """Get text truncation length."""
        return self._data.get("display", {}).get("truncate_text_at", 100)


@dataclass
class ChartConfig:
    """Chart configuration accessor."""

    _data: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        ui_config = load_ui_config()
        self._data = ui_config.get("charts", {})

    @property
    def default_width(self) -> int:
        """Get default chart width."""
        return self._data.get("dimensions", {}).get("default_width", 800)

    @property
    def default_height(self) -> int:
        """Get default chart height."""
        return self._data.get("dimensions", {}).get("default_height", 400)

    @property
    def font_family(self) -> str:
        """Get chart font family."""
        return self._data.get("fonts", {}).get("family", "Arial, sans-serif")

    @property
    def title_size(self) -> int:
        """Get chart title font size."""
        return self._data.get("fonts", {}).get("title_size", 16)


@dataclass
class DataQualityConfig:
    """Data quality thresholds and display configuration."""

    _data: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        ui_config = load_ui_config()
        self._data = ui_config.get("data_quality", {})

    @property
    def thresholds(self) -> Dict[str, int]:
        """Get coverage level thresholds."""
        return self._data.get("thresholds", {
            "excellent": 90,
            "good": 70,
            "moderate": 50,
            "poor": 0,
        })

    def get_coverage_level(self, pct: float) -> str:
        """
        Get coverage level name for a percentage.

        Args:
            pct: Coverage percentage (0-100)

        Returns:
            Coverage level name (excellent, good, moderate, poor)
        """
        thresholds = self.thresholds
        if pct >= thresholds.get("excellent", 90):
            return "excellent"
        elif pct >= thresholds.get("good", 70):
            return "good"
        elif pct >= thresholds.get("moderate", 50):
            return "moderate"
        else:
            return "poor"

    def get_badge(self, level: str) -> Dict[str, str]:
        """Get badge configuration for a level."""
        badges = self._data.get("badges", {})
        return badges.get(level, {"text": level.title(), "color": "#7f7f7f"})


# Convenience singleton instances
_event_types: Optional[EventTypes] = None
_outcome_codes: Optional[OutcomeCodes] = None
_text_type_codes: Optional[TextTypeCodes] = None
_manufacturer_mappings: Optional[ManufacturerMappings] = None
_filter_presets: Optional[FilterPresets] = None
_product_groups: Optional[ProductGroups] = None
_ui_colors: Optional[UIColors] = None
_table_config: Optional[TableConfig] = None
_chart_config: Optional[ChartConfig] = None
_data_quality_config: Optional[DataQualityConfig] = None


def get_event_types() -> EventTypes:
    """Get event types configuration."""
    global _event_types
    if _event_types is None:
        _event_types = EventTypes()
    return _event_types


def get_outcome_codes() -> OutcomeCodes:
    """Get outcome codes configuration."""
    global _outcome_codes
    if _outcome_codes is None:
        _outcome_codes = OutcomeCodes()
    return _outcome_codes


def get_text_type_codes() -> TextTypeCodes:
    """Get text type codes configuration."""
    global _text_type_codes
    if _text_type_codes is None:
        _text_type_codes = TextTypeCodes()
    return _text_type_codes


def get_manufacturer_mappings() -> ManufacturerMappings:
    """Get manufacturer mappings configuration."""
    global _manufacturer_mappings
    if _manufacturer_mappings is None:
        _manufacturer_mappings = ManufacturerMappings()
    return _manufacturer_mappings


def get_filter_presets() -> FilterPresets:
    """Get filter presets configuration."""
    global _filter_presets
    if _filter_presets is None:
        _filter_presets = FilterPresets()
    return _filter_presets


def get_product_groups() -> ProductGroups:
    """Get product groups configuration."""
    global _product_groups
    if _product_groups is None:
        _product_groups = ProductGroups()
    return _product_groups


def get_ui_colors() -> UIColors:
    """Get UI colors configuration."""
    global _ui_colors
    if _ui_colors is None:
        _ui_colors = UIColors()
    return _ui_colors


def get_table_config() -> TableConfig:
    """Get table configuration."""
    global _table_config
    if _table_config is None:
        _table_config = TableConfig()
    return _table_config


def get_chart_config() -> ChartConfig:
    """Get chart configuration."""
    global _chart_config
    if _chart_config is None:
        _chart_config = ChartConfig()
    return _chart_config


def get_data_quality_config() -> DataQualityConfig:
    """Get data quality configuration."""
    global _data_quality_config
    if _data_quality_config is None:
        _data_quality_config = DataQualityConfig()
    return _data_quality_config


def reload_all_config() -> None:
    """Reload all configuration from YAML files."""
    global _event_types, _outcome_codes, _text_type_codes, _manufacturer_mappings
    global _filter_presets, _product_groups, _ui_colors, _table_config
    global _chart_config, _data_quality_config

    # Clear caches
    clear_config_cache()

    # Reset singletons
    _event_types = None
    _outcome_codes = None
    _text_type_codes = None
    _manufacturer_mappings = None
    _filter_presets = None
    _product_groups = None
    _ui_colors = None
    _table_config = None
    _chart_config = None
    _data_quality_config = None
