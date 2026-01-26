"""Product code filter component for MAUDE Analyzer."""

import streamlit as st
from typing import List, Optional, Dict

from .filter_state import get_filter_state, update_filter_state


def render_product_filter(
    available_options: List[str],
    descriptions: Optional[Dict[str, str]] = None,
    key: str = "product_filter",
) -> List[str]:
    """
    Render a product code multiselect filter.

    Args:
        available_options: List of available product codes from database.
        descriptions: Optional dict mapping product codes to descriptions.
        key: Unique key for the Streamlit widget.

    Returns:
        List of selected product codes.
    """
    state = get_filter_state()

    # Filter current selection to only include available options
    current_selection = [
        code for code in state.product_codes
        if code in available_options
    ]

    def format_option(code: str) -> str:
        """Format product code with description if available."""
        if descriptions and code in descriptions:
            desc = descriptions[code]
            # Truncate long descriptions
            if len(desc) > 45:
                desc = desc[:42] + "..."
            return f"{code} - {desc}"
        return code

    selected = st.multiselect(
        "Product Codes",
        options=available_options,
        default=current_selection,
        format_func=format_option,
        help="Filter by FDA product codes. Leave empty to include all products.",
        key=key,
    )

    # Update state if changed
    if selected != current_selection:
        update_filter_state(product_codes=selected)

    return selected


def render_product_filter_compact(
    available_options: List[str],
    descriptions: Optional[Dict[str, str]] = None,
    key: str = "product_filter_compact",
) -> List[str]:
    """
    Render a compact product code filter (for sidebars).

    Args:
        available_options: List of available product codes from database.
        descriptions: Optional dict mapping product codes to descriptions.
        key: Unique key for the Streamlit widget.

    Returns:
        List of selected product codes.
    """
    state = get_filter_state()

    current_selection = [
        code for code in state.product_codes
        if code in available_options
    ]

    # Show count in label
    label = "Product Codes"
    if current_selection:
        label = f"Product Codes ({len(current_selection)})"

    with st.expander(label, expanded=bool(current_selection)):
        selected = st.multiselect(
            "Select product codes",
            options=available_options,
            default=current_selection,
            label_visibility="collapsed",
            key=key,
        )

        if selected != current_selection:
            update_filter_state(product_codes=selected)

    return selected
