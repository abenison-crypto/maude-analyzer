"""Manufacturer filter component for MAUDE Analyzer."""

import streamlit as st
from typing import List, Optional

from .filter_state import get_filter_state, update_filter_state


def render_manufacturer_filter(
    available_options: List[str],
    key: str = "manufacturer_filter",
    max_display: int = 100,
) -> List[str]:
    """
    Render a manufacturer multiselect filter.

    Args:
        available_options: List of available manufacturers from database.
        key: Unique key for the Streamlit widget.
        max_display: Maximum number of options to show.

    Returns:
        List of selected manufacturers.
    """
    state = get_filter_state()

    # Filter current selection to only include available options
    current_selection = [
        mfr for mfr in state.manufacturers
        if mfr in available_options
    ]

    # Limit options if needed (for performance)
    display_options = available_options[:max_display]
    if len(available_options) > max_display:
        help_text = (
            f"Showing top {max_display} manufacturers. "
            "Filter by product code first to narrow options."
        )
    else:
        help_text = "Filter by manufacturer. Leave empty to include all."

    selected = st.multiselect(
        "Manufacturers",
        options=display_options,
        default=current_selection,
        help=help_text,
        key=key,
    )

    # Update state if changed
    if selected != current_selection:
        update_filter_state(manufacturers=selected)

    return selected


def render_manufacturer_filter_compact(
    available_options: List[str],
    key: str = "manufacturer_filter_compact",
) -> List[str]:
    """
    Render a compact manufacturer filter (for sidebars).

    Args:
        available_options: List of available manufacturers from database.
        key: Unique key for the Streamlit widget.

    Returns:
        List of selected manufacturers.
    """
    state = get_filter_state()

    current_selection = [
        mfr for mfr in state.manufacturers
        if mfr in available_options
    ]

    label = "Manufacturers"
    if current_selection:
        label = f"Manufacturers ({len(current_selection)})"

    with st.expander(label, expanded=bool(current_selection)):
        selected = st.multiselect(
            "Select manufacturers",
            options=available_options,
            default=current_selection,
            label_visibility="collapsed",
            key=key,
        )

        if selected != current_selection:
            update_filter_state(manufacturers=selected)

    return selected
