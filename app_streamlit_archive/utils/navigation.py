"""Navigation utilities for MAUDE Analyzer.

Provides drill-down navigation, URL parameter handling, and cross-page linking.
"""

import streamlit as st
from datetime import date
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, parse_qs
import json
import base64


# Navigation target pages
class Pages:
    """Page identifiers for navigation."""
    DASHBOARD = "Dashboard"
    SEARCH = "Search"
    TRENDS = "Trends"
    COMPARISON = "Comparison"
    ANALYTICS = "Analytics"
    PRODUCT = "Product"


# Session state keys for navigation
NAV_TARGET_KEY = "nav_target"
NAV_PARAMS_KEY = "nav_params"
DRILLDOWN_KEY = "drilldown_context"


def navigate_to(
    page: str,
    manufacturers: Optional[List[str]] = None,
    product_codes: Optional[List[str]] = None,
    event_types: Optional[List[str]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    **extra_params,
) -> None:
    """
    Set up navigation to another page with filters.

    This stores the navigation intent in session state. The main app
    should check for navigation requests and switch pages accordingly.

    Args:
        page: Target page name.
        manufacturers: List of manufacturers to filter by.
        product_codes: List of product codes to filter by.
        event_types: List of event types to filter by.
        start_date: Start date for date range filter.
        end_date: End date for date range filter.
        **extra_params: Additional page-specific parameters.
    """
    nav_params = {
        "manufacturers": manufacturers or [],
        "product_codes": product_codes or [],
        "event_types": event_types or [],
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat() if end_date else None,
    }
    nav_params.update(extra_params)

    st.session_state[NAV_TARGET_KEY] = page
    st.session_state[NAV_PARAMS_KEY] = nav_params


def get_navigation_target() -> Optional[str]:
    """Get the current navigation target page, if any."""
    return st.session_state.get(NAV_TARGET_KEY)


def get_navigation_params() -> Dict[str, Any]:
    """Get the current navigation parameters."""
    return st.session_state.get(NAV_PARAMS_KEY, {})


def clear_navigation() -> None:
    """Clear any pending navigation."""
    if NAV_TARGET_KEY in st.session_state:
        del st.session_state[NAV_TARGET_KEY]
    if NAV_PARAMS_KEY in st.session_state:
        del st.session_state[NAV_PARAMS_KEY]


def apply_navigation_filters() -> bool:
    """
    Apply navigation parameters to filter state.

    Returns:
        True if filters were applied, False otherwise.
    """
    params = get_navigation_params()
    if not params:
        return False

    # Import here to avoid circular imports
    from app.components.filters.filter_state import update_filter_state

    # Parse dates back from ISO format
    start_date = None
    end_date = None
    if params.get("start_date"):
        start_date = date.fromisoformat(params["start_date"])
    if params.get("end_date"):
        end_date = date.fromisoformat(params["end_date"])

    # Update filter state
    update_filter_state(
        manufacturers=params.get("manufacturers", []),
        product_codes=params.get("product_codes", []),
        event_types=params.get("event_types", []),
        date_start=start_date,
        date_end=end_date,
    )

    # Clear navigation after applying
    clear_navigation()
    return True


def set_drilldown_context(
    source_page: str,
    context_type: str,
    context_value: Any,
    **extra_context,
) -> None:
    """
    Set drill-down context for tracking where user navigated from.

    Args:
        source_page: The page the user drilled down from.
        context_type: Type of drill-down (e.g., "kpi", "chart_click", "table_row").
        context_value: The value that was clicked/selected.
        **extra_context: Additional context data.
    """
    context = {
        "source_page": source_page,
        "context_type": context_type,
        "context_value": context_value,
        **extra_context,
    }
    st.session_state[DRILLDOWN_KEY] = context


def get_drilldown_context() -> Optional[Dict[str, Any]]:
    """Get the current drill-down context, if any."""
    return st.session_state.get(DRILLDOWN_KEY)


def clear_drilldown_context() -> None:
    """Clear the drill-down context."""
    if DRILLDOWN_KEY in st.session_state:
        del st.session_state[DRILLDOWN_KEY]


def render_back_button(default_page: str = Pages.DASHBOARD) -> bool:
    """
    Render a back button if there's drill-down context.

    Args:
        default_page: Page to return to if no context.

    Returns:
        True if back button was clicked.
    """
    context = get_drilldown_context()

    if context:
        source = context.get("source_page", default_page)
        if st.button(f"Back to {source}", key="drilldown_back"):
            clear_drilldown_context()
            navigate_to(source)
            return True

    return False


def render_breadcrumb() -> None:
    """Render a breadcrumb trail showing navigation path."""
    context = get_drilldown_context()

    if context:
        source = context.get("source_page", "Dashboard")
        context_type = context.get("context_type", "")
        context_value = context.get("context_value", "")

        # Build breadcrumb
        parts = [f"**{source}**"]

        if context_type == "kpi":
            parts.append(f"{context_value}")
        elif context_type == "manufacturer":
            parts.append(f"Manufacturer: {context_value}")
        elif context_type == "product_code":
            parts.append(f"Product: {context_value}")
        elif context_type == "event_type":
            parts.append(f"Event: {context_value}")

        st.caption(" → ".join(parts))


# =============================================================================
# URL Parameter Utilities
# =============================================================================

def encode_filters_to_url_params(
    manufacturers: Optional[List[str]] = None,
    product_codes: Optional[List[str]] = None,
    event_types: Optional[List[str]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> str:
    """
    Encode filter parameters to URL query string.

    Args:
        manufacturers: List of manufacturers.
        product_codes: List of product codes.
        event_types: List of event types.
        start_date: Start date.
        end_date: End date.

    Returns:
        URL query string (without leading '?').
    """
    params = {}

    if manufacturers:
        params["mfr"] = ",".join(manufacturers)
    if product_codes:
        params["pc"] = ",".join(product_codes)
    if event_types:
        params["evt"] = ",".join(event_types)
    if start_date:
        params["start"] = start_date.isoformat()
    if end_date:
        params["end"] = end_date.isoformat()

    return urlencode(params) if params else ""


def decode_filters_from_url_params(query_params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Decode filter parameters from URL query parameters.

    Args:
        query_params: Streamlit query_params dict.

    Returns:
        Dict with decoded filter values.
    """
    filters = {
        "manufacturers": [],
        "product_codes": [],
        "event_types": [],
        "start_date": None,
        "end_date": None,
    }

    if "mfr" in query_params:
        mfr_str = query_params.get("mfr", "")
        if mfr_str:
            filters["manufacturers"] = mfr_str.split(",")

    if "pc" in query_params:
        pc_str = query_params.get("pc", "")
        if pc_str:
            filters["product_codes"] = pc_str.split(",")

    if "evt" in query_params:
        evt_str = query_params.get("evt", "")
        if evt_str:
            filters["event_types"] = evt_str.split(",")

    if "start" in query_params:
        try:
            filters["start_date"] = date.fromisoformat(query_params["start"])
        except (ValueError, TypeError):
            pass

    if "end" in query_params:
        try:
            filters["end_date"] = date.fromisoformat(query_params["end"])
        except (ValueError, TypeError):
            pass

    return filters


def apply_url_params_to_filters() -> bool:
    """
    Apply URL query parameters to filter state if present.

    Should be called at app startup to restore filters from URL.

    Returns:
        True if URL params were applied, False otherwise.
    """
    try:
        query_params = st.query_params.to_dict()
    except Exception:
        return False

    if not query_params:
        return False

    # Check if any filter params are present
    filter_params = ["mfr", "pc", "evt", "start", "end"]
    if not any(p in query_params for p in filter_params):
        return False

    filters = decode_filters_from_url_params(query_params)

    # Import here to avoid circular imports
    from app.components.filters.filter_state import update_filter_state

    update_filter_state(
        manufacturers=filters["manufacturers"],
        product_codes=filters["product_codes"],
        event_types=filters["event_types"],
        date_start=filters["start_date"],
        date_end=filters["end_date"],
    )

    return True


def update_url_with_filters(
    manufacturers: Optional[List[str]] = None,
    product_codes: Optional[List[str]] = None,
    event_types: Optional[List[str]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> None:
    """
    Update the browser URL with current filter parameters.

    This allows users to bookmark or share filtered views.
    """
    params = {}

    if manufacturers:
        params["mfr"] = ",".join(manufacturers)
    if product_codes:
        params["pc"] = ",".join(product_codes)
    if event_types:
        params["evt"] = ",".join(event_types)
    if start_date:
        params["start"] = start_date.isoformat()
    if end_date:
        params["end"] = end_date.isoformat()

    try:
        st.query_params.update(params)
    except Exception:
        pass  # Gracefully handle if query_params not available


def clear_url_params() -> None:
    """Clear all filter-related URL parameters."""
    try:
        st.query_params.clear()
    except Exception:
        pass


# =============================================================================
# Clickable Components
# =============================================================================

def create_clickable_metric(
    label: str,
    value: Any,
    click_action: str,
    delta: Optional[str] = None,
    delta_color: str = "normal",
    help_text: Optional[str] = None,
    **nav_params,
) -> bool:
    """
    Create a clickable metric card that navigates on click.

    Args:
        label: Metric label.
        value: Metric value.
        click_action: Navigation action (page name).
        delta: Optional delta value.
        delta_color: Delta color ("normal", "inverse", "off").
        help_text: Optional help text.
        **nav_params: Parameters for navigation.

    Returns:
        True if clicked.
    """
    # Create a unique key from label
    key = f"metric_{label.lower().replace(' ', '_')}"

    # Use a container with button overlay approach
    col1, col2 = st.columns([4, 1])

    with col1:
        st.metric(
            label=label,
            value=value,
            delta=delta,
            delta_color=delta_color,
            help=help_text,
        )

    with col2:
        if st.button("→", key=key, help=f"View {label} details"):
            navigate_to(click_action, **nav_params)
            set_drilldown_context(
                source_page=Pages.DASHBOARD,
                context_type="kpi",
                context_value=label,
            )
            return True

    return False


def render_drilldown_link(
    text: str,
    target_page: str,
    key: str,
    **nav_params,
) -> bool:
    """
    Render a drill-down link button.

    Args:
        text: Link text.
        target_page: Target page.
        key: Unique button key.
        **nav_params: Navigation parameters.

    Returns:
        True if clicked.
    """
    if st.button(text, key=key, type="secondary"):
        navigate_to(target_page, **nav_params)
        return True
    return False
