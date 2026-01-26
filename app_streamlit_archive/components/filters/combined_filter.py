"""Combined filter bar component for MAUDE Analyzer.

Provides a reusable filter bar with searchable selects and data quality awareness.
All filters default to empty (all data).
"""

import streamlit as st
from typing import Dict, Any, Optional
import duckdb

from .filter_state import (
    get_filter_state,
    update_filter_state,
    clear_filters,
    FilterState,
)
from .date_filter import render_date_filter
from .filter_presets import render_preset_chips

# Import from parent if available
try:
    from config import get_event_type_name
except ImportError:
    def get_event_type_name(code):
        defaults = {"D": "Death", "IN": "Injury", "M": "Malfunction", "O": "Other", "*": "No Answer"}
        return defaults.get(code, code)

try:
    from app.components.searchable_select import CachedSearchableSelect
    _SEARCHABLE_AVAILABLE = True
except ImportError:
    _SEARCHABLE_AVAILABLE = False


def render_filter_bar(
    filter_options: Dict[str, Any],
    conn: Optional[duckdb.DuckDBPyConnection] = None,
    product_descriptions: Optional[Dict[str, str]] = None,
    show_presets: bool = True,
    show_clear: bool = True,
    show_summary: bool = True,
    show_coverage: bool = True,
    compact: bool = False,
    key_prefix: str = "filter_bar",
) -> FilterState:
    """
    Render a complete filter bar with all filter components.

    Args:
        filter_options: Dict with 'manufacturers', 'product_codes', 'event_types' lists.
        conn: Optional database connection for searchable selects.
        product_descriptions: Optional dict of product code to description.
        show_presets: Whether to show preset buttons.
        show_clear: Whether to show clear button.
        show_summary: Whether to show active filter summary.
        show_coverage: Whether to show data coverage indicators.
        compact: Use compact mode for sidebars.
        key_prefix: Unique prefix for widget keys.

    Returns:
        Current FilterState after user interactions.
    """
    state = get_filter_state()

    if compact:
        return _render_compact_filter_bar(
            filter_options, conn, product_descriptions, show_presets,
            show_clear, show_coverage, key_prefix
        )

    # Header row with presets and clear
    if show_presets or show_clear or show_summary:
        header_cols = st.columns([3, 1, 1])

        with header_cols[0]:
            if show_summary:
                if state.active_filter_count > 0:
                    st.caption(state.get_summary())
                else:
                    st.caption("No filters active - showing all data")

        with header_cols[1]:
            if show_presets:
                render_preset_chips(key=f"{key_prefix}_presets")

        with header_cols[2]:
            if show_clear and state.active_filter_count > 0:
                if st.button("Clear All Filters", key=f"{key_prefix}_clear"):
                    clear_filters()
                    st.rerun()

    # Data coverage warning
    if show_coverage:
        _render_coverage_note(filter_options)

    # Main filter row
    col1, col2, col3, col4 = st.columns([2, 2, 2, 2])

    with col1:
        _render_product_filter_searchable(
            filter_options=filter_options,
            conn=conn,
            state=state,
            key_prefix=key_prefix,
        )

    with col2:
        _render_manufacturer_filter_searchable(
            filter_options=filter_options,
            conn=conn,
            state=state,
            key_prefix=key_prefix,
        )

    with col3:
        _render_event_type_filter(
            filter_options=filter_options,
            state=state,
            key_prefix=key_prefix,
        )

    with col4:
        render_date_filter(key=f"{key_prefix}_dates")

    return get_filter_state()


def _render_coverage_note(filter_options: Dict[str, Any]):
    """Show data coverage context for filters."""
    coverage = filter_options.get("_coverage", {})
    if not coverage:
        return

    issues = []
    mfr_cov = coverage.get("manufacturer_clean", 100)
    pc_cov = coverage.get("product_code", 100)

    if mfr_cov < 80:
        issues.append(f"manufacturer ({mfr_cov:.0f}%)")
    if pc_cov < 80:
        issues.append(f"product code ({pc_cov:.0f}%)")

    if issues:
        st.caption(f"Note: Some records have missing {', '.join(issues)} data")


def _render_product_filter_searchable(
    filter_options: Dict[str, Any],
    conn: Optional[duckdb.DuckDBPyConnection],
    state: FilterState,
    key_prefix: str,
):
    """Render product code filter with searchable select if available."""
    if _SEARCHABLE_AVAILABLE and conn is not None:
        st.markdown("**Product Codes** (type to search)")
        pc_select = CachedSearchableSelect(
            conn=conn,
            table="master_events",
            column="product_code",
            key=f"{key_prefix}_pc",
            label="Product Codes",
        )
        selected = pc_select.render(
            multi=True,
            default=list(state.product_codes) if state.product_codes else [],
            show_counts=True,
        )
        if selected != list(state.product_codes or []):
            update_filter_state(product_codes=selected)
    else:
        # Fallback to regular multiselect
        products = filter_options.get("product_codes", [])
        current_products = [p for p in (state.product_codes or []) if p in products]

        selected = st.multiselect(
            "Product Codes",
            options=products,
            default=current_products,
            help="Leave empty for all product codes",
            key=f"{key_prefix}_products_basic",
        )
        if selected != current_products:
            update_filter_state(product_codes=selected)


def _render_manufacturer_filter_searchable(
    filter_options: Dict[str, Any],
    conn: Optional[duckdb.DuckDBPyConnection],
    state: FilterState,
    key_prefix: str,
):
    """Render manufacturer filter with searchable select if available."""
    if _SEARCHABLE_AVAILABLE and conn is not None:
        st.markdown("**Manufacturers** (type to search)")
        mfr_select = CachedSearchableSelect(
            conn=conn,
            table="master_events",
            column="manufacturer_clean",
            key=f"{key_prefix}_mfr",
            label="Manufacturers",
        )
        selected = mfr_select.render(
            multi=True,
            default=list(state.manufacturers) if state.manufacturers else [],
            show_counts=True,
        )
        if selected != list(state.manufacturers or []):
            update_filter_state(manufacturers=selected)
    else:
        # Fallback to regular multiselect
        manufacturers = filter_options.get("manufacturers", [])
        current_mfrs = [m for m in (state.manufacturers or []) if m in manufacturers]

        selected = st.multiselect(
            "Manufacturers",
            options=manufacturers,
            default=current_mfrs,
            help="Leave empty for all manufacturers",
            key=f"{key_prefix}_manufacturers_basic",
        )
        if selected != current_mfrs:
            update_filter_state(manufacturers=selected)


def _render_event_type_filter(
    filter_options: Dict[str, Any],
    state: FilterState,
    key_prefix: str,
):
    """Render event type filter using config-driven labels."""
    event_types = filter_options.get("event_types", [])
    current_events = [et for et in (state.event_types or []) if et in event_types]

    selected_events = st.multiselect(
        "Event Types",
        options=event_types,
        default=current_events,
        format_func=get_event_type_name,
        help="Leave empty for all event types",
        key=f"{key_prefix}_events",
    )

    if selected_events != current_events:
        update_filter_state(event_types=selected_events)


def _render_compact_filter_bar(
    filter_options: Dict[str, Any],
    conn: Optional[duckdb.DuckDBPyConnection],
    product_descriptions: Optional[Dict[str, str]],
    show_presets: bool,
    show_clear: bool,
    show_coverage: bool,
    key_prefix: str,
) -> FilterState:
    """Render a compact vertical filter bar for sidebars."""
    state = get_filter_state()

    # Active filter indicator
    if state.active_filter_count > 0:
        st.info(f"{state.active_filter_count} active filter(s)")

        if show_clear:
            if st.button("Clear All", key=f"{key_prefix}_clear_compact"):
                clear_filters()
                st.rerun()
    else:
        st.caption("No filters - showing all data")

    # Presets
    if show_presets:
        with st.expander("Presets", expanded=False):
            render_preset_chips(key=f"{key_prefix}_presets_compact")

    st.divider()

    # Coverage warning
    if show_coverage:
        _render_coverage_note(filter_options)

    # Product filter (searchable if available)
    if _SEARCHABLE_AVAILABLE and conn is not None:
        st.markdown("**Product Codes**")
        pc_select = CachedSearchableSelect(
            conn=conn,
            table="master_events",
            column="product_code",
            key=f"{key_prefix}_pc_compact",
            label="Product Codes",
        )
        selected_products = pc_select.render(
            multi=True,
            default=list(state.product_codes) if state.product_codes else [],
            show_counts=True,
        )
        if selected_products != list(state.product_codes or []):
            update_filter_state(product_codes=selected_products)
    else:
        products = filter_options.get("product_codes", [])
        current_products = [p for p in (state.product_codes or []) if p in products]

        selected_products = st.multiselect(
            "Product Codes",
            options=products,
            default=current_products,
            key=f"{key_prefix}_products_compact",
        )
        if selected_products != current_products:
            update_filter_state(product_codes=selected_products)

    # Manufacturer filter (searchable if available)
    if _SEARCHABLE_AVAILABLE and conn is not None:
        st.markdown("**Manufacturers**")
        mfr_select = CachedSearchableSelect(
            conn=conn,
            table="master_events",
            column="manufacturer_clean",
            key=f"{key_prefix}_mfr_compact",
            label="Manufacturers",
        )
        selected_mfrs = mfr_select.render(
            multi=True,
            default=list(state.manufacturers) if state.manufacturers else [],
            show_counts=True,
        )
        if selected_mfrs != list(state.manufacturers or []):
            update_filter_state(manufacturers=selected_mfrs)
    else:
        manufacturers = filter_options.get("manufacturers", [])
        current_mfrs = [m for m in (state.manufacturers or []) if m in manufacturers]

        selected_mfrs = st.multiselect(
            "Manufacturers",
            options=manufacturers,
            default=current_mfrs,
            key=f"{key_prefix}_manufacturers_compact",
        )
        if selected_mfrs != current_mfrs:
            update_filter_state(manufacturers=selected_mfrs)

    # Event types (config-driven labels)
    event_types = filter_options.get("event_types", [])
    current_events = [et for et in (state.event_types or []) if et in event_types]

    selected_events = st.multiselect(
        "Event Types",
        options=event_types,
        default=current_events,
        format_func=get_event_type_name,
        key=f"{key_prefix}_events_compact",
    )
    if selected_events != current_events:
        update_filter_state(event_types=selected_events)

    # Date filter
    st.divider()
    render_date_filter(key=f"{key_prefix}_dates_compact")

    return get_filter_state()


def render_filter_indicator() -> None:
    """Render a small indicator showing active filter count."""
    state = get_filter_state()

    if state.active_filter_count > 0:
        st.caption(f"Filters active: {state.active_filter_count}")
    else:
        st.caption("No filters (showing all data)")


def get_query_filters(state: Optional[FilterState] = None) -> Dict[str, Any]:
    """
    Get filter values formatted for database queries.

    Args:
        state: FilterState to use, or get current if None.

    Returns:
        Dict with filter values ready for query functions.
    """
    if state is None:
        state = get_filter_state()

    return {
        "product_codes": state.product_codes if state.product_codes else None,
        "manufacturers": state.manufacturers if state.manufacturers else None,
        "event_types": state.event_types if state.event_types else None,
        "start_date": state.date_start,
        "end_date": state.date_end,
    }
