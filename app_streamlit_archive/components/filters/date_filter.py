"""Date filter component for MAUDE Analyzer."""

import streamlit as st
from datetime import date, timedelta
from typing import Tuple, Optional

from .filter_state import get_filter_state, update_filter_state


# Common date range presets
DATE_PRESETS = {
    "Last 30 days": lambda: (date.today() - timedelta(days=30), date.today()),
    "Last 90 days": lambda: (date.today() - timedelta(days=90), date.today()),
    "Last year": lambda: (date.today() - timedelta(days=365), date.today()),
    "Last 3 years": lambda: (date.today() - timedelta(days=365 * 3), date.today()),
    "Last 5 years": lambda: (date.today() - timedelta(days=365 * 5), date.today()),
    "Year to date": lambda: (date(date.today().year, 1, 1), date.today()),
    "All time": lambda: (None, None),
}


def render_date_filter(
    key: str = "date_filter",
    show_presets: bool = True,
) -> Tuple[Optional[date], Optional[date]]:
    """
    Render a date range filter with optional presets.

    Args:
        key: Unique key for the Streamlit widget.
        show_presets: Whether to show quick preset buttons.

    Returns:
        Tuple of (start_date, end_date), either may be None.
    """
    state = get_filter_state()

    # Default values
    start = state.date_start or (date.today() - timedelta(days=365 * 5))
    end = state.date_end or date.today()

    col1, col2 = st.columns(2)

    with col1:
        new_start = st.date_input(
            "Start Date",
            value=start,
            max_value=date.today(),
            help="Filter by date received (start)",
            key=f"{key}_start",
        )

    with col2:
        new_end = st.date_input(
            "End Date",
            value=end,
            max_value=date.today(),
            help="Filter by date received (end)",
            key=f"{key}_end",
        )

    # Update state if changed
    if new_start != state.date_start or new_end != state.date_end:
        update_filter_state(date_start=new_start, date_end=new_end)

    return new_start, new_end


def render_date_filter_with_presets(
    key: str = "date_filter_presets",
) -> Tuple[Optional[date], Optional[date]]:
    """
    Render a date range filter with preset selector.

    Args:
        key: Unique key for the Streamlit widget.

    Returns:
        Tuple of (start_date, end_date), either may be None.
    """
    state = get_filter_state()

    # Preset selector
    preset_names = list(DATE_PRESETS.keys())

    # Determine current preset (if any)
    current_preset = None
    for name, fn in DATE_PRESETS.items():
        preset_start, preset_end = fn()
        if state.date_start == preset_start and state.date_end == preset_end:
            current_preset = name
            break

    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        selected_preset = st.selectbox(
            "Date Range",
            options=["Custom"] + preset_names,
            index=(preset_names.index(current_preset) + 1) if current_preset else 0,
            key=f"{key}_preset",
        )

    if selected_preset != "Custom" and selected_preset in DATE_PRESETS:
        new_start, new_end = DATE_PRESETS[selected_preset]()
        if new_start != state.date_start or new_end != state.date_end:
            update_filter_state(date_start=new_start, date_end=new_end)
        return new_start, new_end

    # Custom date inputs
    start = state.date_start or (date.today() - timedelta(days=365 * 5))
    end = state.date_end or date.today()

    with col2:
        new_start = st.date_input(
            "From",
            value=start,
            max_value=date.today(),
            key=f"{key}_start",
        )

    with col3:
        new_end = st.date_input(
            "To",
            value=end,
            max_value=date.today(),
            key=f"{key}_end",
        )

    if new_start != state.date_start or new_end != state.date_end:
        update_filter_state(date_start=new_start, date_end=new_end)

    return new_start, new_end


def render_date_filter_compact(
    key: str = "date_filter_compact",
) -> Tuple[Optional[date], Optional[date]]:
    """
    Render a compact date filter (for sidebars).

    Args:
        key: Unique key for the Streamlit widget.

    Returns:
        Tuple of (start_date, end_date), either may be None.
    """
    state = get_filter_state()

    # Format current range for label
    if state.date_start and state.date_end:
        label = f"Date Range ({state.date_start.strftime('%Y-%m-%d')} to {state.date_end.strftime('%Y-%m-%d')})"
    else:
        label = "Date Range"

    with st.expander(label, expanded=False):
        # Preset buttons
        st.caption("Quick select:")
        cols = st.columns(3)

        preset_buttons = [
            ("Last 30 days", cols[0]),
            ("Last year", cols[1]),
            ("Last 5 years", cols[2]),
        ]

        for preset_name, col in preset_buttons:
            with col:
                if st.button(preset_name, key=f"{key}_{preset_name.replace(' ', '_')}"):
                    start, end = DATE_PRESETS[preset_name]()
                    update_filter_state(date_start=start, date_end=end)
                    st.rerun()

        st.divider()

        # Custom date inputs
        start = state.date_start or (date.today() - timedelta(days=365 * 5))
        end = state.date_end or date.today()

        new_start = st.date_input(
            "From",
            value=start,
            max_value=date.today(),
            key=f"{key}_start_compact",
        )

        new_end = st.date_input(
            "To",
            value=end,
            max_value=date.today(),
            key=f"{key}_end_compact",
        )

        if new_start != state.date_start or new_end != state.date_end:
            update_filter_state(date_start=new_start, date_end=new_end)

    return state.date_start, state.date_end
