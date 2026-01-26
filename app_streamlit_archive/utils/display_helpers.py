"""NULL-aware display utilities for MAUDE Analyzer.

Provides consistent handling of NULL/missing values across the UI,
including formatting, coverage indicators, and data quality warnings.
"""

from typing import Any, Optional, Dict, List, Union
from datetime import date, datetime
from enum import Enum
from dataclasses import dataclass
import pandas as pd
import streamlit as st

# Try to import config, but provide fallbacks
try:
    from config.config_loader import get_data_quality_config, get_table_config
    _CONFIG_AVAILABLE = True
except ImportError:
    _CONFIG_AVAILABLE = False


class DataQualityLevel(Enum):
    """Data quality level based on coverage percentage."""
    EXCELLENT = "excellent"  # >= 90%
    GOOD = "good"            # >= 70%
    MODERATE = "moderate"    # >= 50%
    POOR = "poor"            # < 50%
    MISSING = "missing"      # 0%


@dataclass
class CoverageInfo:
    """Information about data coverage for a field."""
    field: str
    coverage_pct: float
    level: DataQualityLevel
    display_text: str
    color: str


# Default thresholds if config not available
DEFAULT_THRESHOLDS = {
    "excellent": 90,
    "good": 70,
    "moderate": 50,
    "poor": 0,
}

# Default colors if config not available
DEFAULT_COLORS = {
    "excellent": "#2ca02c",  # Green
    "good": "#1f77b4",       # Blue
    "moderate": "#ff7f0e",   # Orange
    "poor": "#d62728",       # Red
    "missing": "#7f7f7f",    # Gray
}


def _get_thresholds() -> Dict[str, int]:
    """Get coverage thresholds from config or defaults."""
    if _CONFIG_AVAILABLE:
        try:
            config = get_data_quality_config()
            return config.thresholds
        except Exception:
            pass
    return DEFAULT_THRESHOLDS


def _get_null_display() -> str:
    """Get the display text for NULL values."""
    if _CONFIG_AVAILABLE:
        try:
            config = get_table_config()
            return config.null_display
        except Exception:
            pass
    return "N/A"


def format_nullable(
    value: Any,
    default: str = None,
    prefix: str = "",
    suffix: str = "",
) -> str:
    """
    Format a potentially NULL/None value for display.

    Args:
        value: Value to format (may be None, NaN, or empty string)
        default: Default text to show for NULL values (uses config if None)
        prefix: Optional prefix to add if value exists
        suffix: Optional suffix to add if value exists

    Returns:
        Formatted string
    """
    if default is None:
        default = _get_null_display()

    # Check for various "null" representations
    if value is None:
        return default
    if isinstance(value, float) and pd.isna(value):
        return default
    if isinstance(value, str) and value.strip() == "":
        return default

    # Format the value
    formatted = str(value)
    if prefix:
        formatted = prefix + formatted
    if suffix:
        formatted = formatted + suffix

    return formatted


def format_number(
    value: Any,
    default: str = None,
    decimals: int = 0,
    thousands_sep: bool = True,
) -> str:
    """
    Format a numeric value with proper handling of NULL.

    Args:
        value: Numeric value (may be None or NaN)
        default: Default text for NULL values
        decimals: Number of decimal places
        thousands_sep: Whether to add thousands separator

    Returns:
        Formatted string
    """
    if default is None:
        default = _get_null_display()

    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default

    try:
        num = float(value)
        if decimals == 0:
            num = int(num)
            if thousands_sep:
                return f"{num:,}"
            return str(num)
        else:
            if thousands_sep:
                return f"{num:,.{decimals}f}"
            return f"{num:.{decimals}f}"
    except (ValueError, TypeError):
        return default


def format_percentage(
    value: Any,
    default: str = None,
    decimals: int = 1,
    multiply_by_100: bool = False,
) -> str:
    """
    Format a percentage value with proper handling of NULL.

    Args:
        value: Percentage value (may be None or NaN)
        default: Default text for NULL values
        decimals: Number of decimal places
        multiply_by_100: If True, multiply value by 100 (for 0-1 range)

    Returns:
        Formatted string with % symbol
    """
    if default is None:
        default = _get_null_display()

    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default

    try:
        num = float(value)
        if multiply_by_100:
            num = num * 100
        return f"{num:.{decimals}f}%"
    except (ValueError, TypeError):
        return default


def format_date(
    value: Any,
    default: str = None,
    format_str: str = "%Y-%m-%d",
) -> str:
    """
    Format a date value with proper handling of NULL.

    Args:
        value: Date value (may be None, NaT, or various date types)
        default: Default text for NULL values
        format_str: Date format string

    Returns:
        Formatted date string
    """
    if default is None:
        default = _get_null_display()

    if value is None:
        return default

    if isinstance(value, str):
        if value.strip() == "":
            return default
        return value  # Already a string

    if pd.isna(value):
        return default

    try:
        if isinstance(value, (date, datetime)):
            return value.strftime(format_str)
        if hasattr(value, "strftime"):
            return value.strftime(format_str)
        return str(value)
    except Exception:
        return default


def get_coverage_level(pct: float) -> DataQualityLevel:
    """
    Get the data quality level for a coverage percentage.

    Args:
        pct: Coverage percentage (0-100)

    Returns:
        DataQualityLevel enum value
    """
    if pct <= 0:
        return DataQualityLevel.MISSING

    thresholds = _get_thresholds()

    if pct >= thresholds.get("excellent", 90):
        return DataQualityLevel.EXCELLENT
    elif pct >= thresholds.get("good", 70):
        return DataQualityLevel.GOOD
    elif pct >= thresholds.get("moderate", 50):
        return DataQualityLevel.MODERATE
    else:
        return DataQualityLevel.POOR


def get_coverage_color(level: DataQualityLevel) -> str:
    """Get the color for a data quality level."""
    return DEFAULT_COLORS.get(level.value, "#7f7f7f")


def render_coverage_indicator(
    coverage_pct: float,
    show_text: bool = True,
) -> str:
    """
    Render a coverage indicator as colored text/emoji.

    Args:
        coverage_pct: Coverage percentage (0-100)
        show_text: Whether to include percentage text

    Returns:
        Formatted string with color indicator
    """
    level = get_coverage_level(coverage_pct)
    color = get_coverage_color(level)

    # Emoji indicators
    indicators = {
        DataQualityLevel.EXCELLENT: "●",
        DataQualityLevel.GOOD: "●",
        DataQualityLevel.MODERATE: "●",
        DataQualityLevel.POOR: "●",
        DataQualityLevel.MISSING: "○",
    }

    indicator = indicators.get(level, "●")

    if show_text:
        return f"<span style='color:{color}'>{indicator}</span> {coverage_pct:.0f}%"
    else:
        return f"<span style='color:{color}'>{indicator}</span>"


def render_coverage_badge(
    coverage_pct: float,
    field_name: Optional[str] = None,
) -> None:
    """
    Render a coverage badge using Streamlit.

    Args:
        coverage_pct: Coverage percentage (0-100)
        field_name: Optional field name to display
    """
    level = get_coverage_level(coverage_pct)
    color = get_coverage_color(level)

    # Badge labels
    labels = {
        DataQualityLevel.EXCELLENT: "Complete",
        DataQualityLevel.GOOD: "Good",
        DataQualityLevel.MODERATE: "Partial",
        DataQualityLevel.POOR: "Sparse",
        DataQualityLevel.MISSING: "Missing",
    }

    label = labels.get(level, "Unknown")

    # Build badge HTML
    badge_html = f"""
    <span style="
        background-color: {color};
        color: white;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 12px;
        font-weight: bold;
    ">{label} ({coverage_pct:.0f}%)</span>
    """

    if field_name:
        st.markdown(f"**{field_name}**: {badge_html}", unsafe_allow_html=True)
    else:
        st.markdown(badge_html, unsafe_allow_html=True)


def add_missing_data_warning(
    df: pd.DataFrame,
    column: str,
    threshold: float = 50.0,
) -> bool:
    """
    Add a warning message if column has significant missing data.

    Args:
        df: DataFrame to check
        column: Column name to check
        threshold: Threshold below which to show warning

    Returns:
        True if warning was shown, False otherwise
    """
    if column not in df.columns:
        st.warning(f"Column '{column}' not found in data")
        return True

    total = len(df)
    if total == 0:
        return False

    non_null = df[column].notna().sum()
    coverage_pct = (non_null / total) * 100

    if coverage_pct < threshold:
        st.warning(
            f"**Data Quality Note**: '{column}' is only {coverage_pct:.1f}% populated "
            f"({non_null:,} of {total:,} records). Results may be incomplete."
        )
        return True

    return False


def get_data_quality_summary(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
) -> Dict[str, CoverageInfo]:
    """
    Get data quality summary for specified columns.

    Args:
        df: DataFrame to analyze
        columns: Columns to check (None = all columns)

    Returns:
        Dict mapping column name to CoverageInfo
    """
    if columns is None:
        columns = list(df.columns)

    total = len(df)
    if total == 0:
        return {}

    summary = {}
    for col in columns:
        if col not in df.columns:
            continue

        non_null = df[col].notna().sum()
        coverage_pct = (non_null / total) * 100
        level = get_coverage_level(coverage_pct)
        color = get_coverage_color(level)

        # Display text based on level
        level_names = {
            DataQualityLevel.EXCELLENT: "Complete",
            DataQualityLevel.GOOD: "Good",
            DataQualityLevel.MODERATE: "Partial",
            DataQualityLevel.POOR: "Sparse",
            DataQualityLevel.MISSING: "Missing",
        }

        summary[col] = CoverageInfo(
            field=col,
            coverage_pct=coverage_pct,
            level=level,
            display_text=level_names.get(level, "Unknown"),
            color=color,
        )

    return summary


def render_data_quality_table(
    summary: Dict[str, CoverageInfo],
    title: str = "Data Quality Summary",
) -> None:
    """
    Render a data quality summary table using Streamlit.

    Args:
        summary: Dict from get_data_quality_summary
        title: Table title
    """
    if not summary:
        st.info("No data quality information available")
        return

    st.subheader(title)

    # Build table data
    table_data = []
    for col, info in summary.items():
        table_data.append({
            "Field": col,
            "Coverage": f"{info.coverage_pct:.1f}%",
            "Status": info.display_text,
        })

    # Create DataFrame and style it
    df = pd.DataFrame(table_data)
    st.dataframe(df, use_container_width=True, hide_index=True)


def highlight_null_values(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    null_color: str = "#ffcccc",
) -> "pd.io.formats.style.Styler":
    """
    Create a styled DataFrame that highlights NULL values.

    Args:
        df: DataFrame to style
        columns: Columns to check (None = all)
        null_color: Background color for NULL cells

    Returns:
        Styled DataFrame
    """
    if columns is None:
        columns = list(df.columns)

    def highlight_nulls(val):
        if pd.isna(val) or val is None or (isinstance(val, str) and val.strip() == ""):
            return f"background-color: {null_color}"
        return ""

    return df.style.applymap(highlight_nulls, subset=columns)


def prepare_display_df(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    date_columns: Optional[List[str]] = None,
    numeric_columns: Optional[List[str]] = None,
    null_display: str = None,
) -> pd.DataFrame:
    """
    Prepare a DataFrame for display with consistent NULL handling.

    Args:
        df: Source DataFrame
        columns: Columns to include (None = all)
        date_columns: Columns to format as dates
        numeric_columns: Columns to format as numbers
        null_display: Text to show for NULL values

    Returns:
        Display-ready DataFrame with formatted values
    """
    if null_display is None:
        null_display = _get_null_display()

    # Select columns
    if columns:
        available = [c for c in columns if c in df.columns]
        display_df = df[available].copy()
    else:
        display_df = df.copy()

    # Format date columns
    if date_columns:
        for col in date_columns:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda x: format_date(x, default=null_display)
                )

    # Format numeric columns
    if numeric_columns:
        for col in numeric_columns:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda x: format_number(x, default=null_display)
                )

    # Replace remaining NaN/None with null_display
    display_df = display_df.fillna(null_display)

    # Replace empty strings
    for col in display_df.columns:
        if display_df[col].dtype == object:
            display_df[col] = display_df[col].replace("", null_display)

    return display_df
