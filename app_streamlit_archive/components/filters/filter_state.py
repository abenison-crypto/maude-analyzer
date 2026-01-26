"""Filter state management for MAUDE Analyzer.

Provides centralized filter state that persists across pages using Streamlit session state.
Supports cross-page navigation, filter history, and URL parameter encoding.
"""

from dataclasses import dataclass, field, asdict
from datetime import date, timedelta
from typing import List, Optional, Dict, Any, Tuple
import streamlit as st
import json
import base64


@dataclass
class FilterPreset:
    """A saved filter configuration."""

    name: str
    product_codes: List[str] = field(default_factory=list)
    manufacturers: List[str] = field(default_factory=list)
    event_types: List[str] = field(default_factory=list)
    date_start: Optional[date] = None
    date_end: Optional[date] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "name": self.name,
            "product_codes": self.product_codes,
            "manufacturers": self.manufacturers,
            "event_types": self.event_types,
            "date_start": self.date_start.isoformat() if self.date_start else None,
            "date_end": self.date_end.isoformat() if self.date_end else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FilterPreset":
        """Create from dictionary."""
        return cls(
            name=data["name"],
            product_codes=data.get("product_codes", []),
            manufacturers=data.get("manufacturers", []),
            event_types=data.get("event_types", []),
            date_start=date.fromisoformat(data["date_start"]) if data.get("date_start") else None,
            date_end=date.fromisoformat(data["date_end"]) if data.get("date_end") else None,
        )


@dataclass
class FilterState:
    """Current filter state for the application."""

    product_codes: List[str] = field(default_factory=list)
    manufacturers: List[str] = field(default_factory=list)
    event_types: List[str] = field(default_factory=list)
    date_start: Optional[date] = None
    date_end: Optional[date] = None

    @property
    def is_empty(self) -> bool:
        """Check if all filters are empty (showing all data)."""
        return (
            not self.product_codes
            and not self.manufacturers
            and not self.event_types
            and self.date_start is None
            and self.date_end is None
        )

    @property
    def active_filter_count(self) -> int:
        """Count of active filters."""
        count = 0
        if self.product_codes:
            count += 1
        if self.manufacturers:
            count += 1
        if self.event_types:
            count += 1
        if self.date_start is not None or self.date_end is not None:
            count += 1
        return count

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "product_codes": self.product_codes,
            "manufacturers": self.manufacturers,
            "event_types": self.event_types,
            "date_start": self.date_start.isoformat() if self.date_start else None,
            "date_end": self.date_end.isoformat() if self.date_end else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FilterState":
        """Create from dictionary."""
        return cls(
            product_codes=data.get("product_codes", []),
            manufacturers=data.get("manufacturers", []),
            event_types=data.get("event_types", []),
            date_start=date.fromisoformat(data["date_start"]) if data.get("date_start") else None,
            date_end=date.fromisoformat(data["date_end"]) if data.get("date_end") else None,
        )

    def apply_preset(self, preset: FilterPreset) -> None:
        """Apply a preset to the current filter state."""
        self.product_codes = list(preset.product_codes)
        self.manufacturers = list(preset.manufacturers)
        self.event_types = list(preset.event_types)
        self.date_start = preset.date_start
        self.date_end = preset.date_end

    def get_summary(self) -> str:
        """Get a human-readable summary of active filters."""
        parts = []

        if self.product_codes:
            if len(self.product_codes) <= 3:
                parts.append(f"Products: {', '.join(self.product_codes)}")
            else:
                parts.append(f"Products: {len(self.product_codes)} selected")

        if self.manufacturers:
            if len(self.manufacturers) <= 2:
                parts.append(f"Manufacturers: {', '.join(self.manufacturers)}")
            else:
                parts.append(f"Manufacturers: {len(self.manufacturers)} selected")

        if self.event_types:
            parts.append(f"Events: {', '.join(self.event_types)}")

        if self.date_start or self.date_end:
            start_str = self.date_start.strftime("%Y-%m-%d") if self.date_start else "..."
            end_str = self.date_end.strftime("%Y-%m-%d") if self.date_end else "..."
            parts.append(f"Dates: {start_str} to {end_str}")

        return " | ".join(parts) if parts else "All data (no filters)"

    def copy(self) -> "FilterState":
        """Create a copy of this filter state."""
        return FilterState(
            product_codes=list(self.product_codes),
            manufacturers=list(self.manufacturers),
            event_types=list(self.event_types),
            date_start=self.date_start,
            date_end=self.date_end,
        )

    def merge(self, other: "FilterState", override: bool = True) -> "FilterState":
        """
        Merge another filter state into this one.

        Args:
            other: FilterState to merge in.
            override: If True, non-empty values from other override this.
                      If False, only fill in empty values.

        Returns:
            A new merged FilterState.
        """
        if override:
            return FilterState(
                product_codes=other.product_codes if other.product_codes else self.product_codes,
                manufacturers=other.manufacturers if other.manufacturers else self.manufacturers,
                event_types=other.event_types if other.event_types else self.event_types,
                date_start=other.date_start if other.date_start else self.date_start,
                date_end=other.date_end if other.date_end else self.date_end,
            )
        else:
            return FilterState(
                product_codes=self.product_codes if self.product_codes else other.product_codes,
                manufacturers=self.manufacturers if self.manufacturers else other.manufacturers,
                event_types=self.event_types if self.event_types else other.event_types,
                date_start=self.date_start if self.date_start else other.date_start,
                date_end=self.date_end if self.date_end else other.date_end,
            )

    def to_url_params(self) -> Dict[str, str]:
        """
        Convert filter state to URL-friendly parameters.

        Returns:
            Dict of parameter name to string value.
        """
        params = {}

        if self.product_codes:
            params["pc"] = ",".join(self.product_codes)
        if self.manufacturers:
            params["mfr"] = ",".join(self.manufacturers)
        if self.event_types:
            params["evt"] = ",".join(self.event_types)
        if self.date_start:
            params["start"] = self.date_start.isoformat()
        if self.date_end:
            params["end"] = self.date_end.isoformat()

        return params

    @classmethod
    def from_url_params(cls, params: Dict[str, Any]) -> "FilterState":
        """
        Create filter state from URL parameters.

        Args:
            params: Dict of URL parameters.

        Returns:
            FilterState populated from parameters.
        """
        state = cls()

        if "pc" in params:
            pc_val = params["pc"]
            if isinstance(pc_val, list):
                pc_val = pc_val[0]
            state.product_codes = [p for p in pc_val.split(",") if p]

        if "mfr" in params:
            mfr_val = params["mfr"]
            if isinstance(mfr_val, list):
                mfr_val = mfr_val[0]
            state.manufacturers = [m for m in mfr_val.split(",") if m]

        if "evt" in params:
            evt_val = params["evt"]
            if isinstance(evt_val, list):
                evt_val = evt_val[0]
            state.event_types = [e for e in evt_val.split(",") if e]

        if "start" in params:
            try:
                start_val = params["start"]
                if isinstance(start_val, list):
                    start_val = start_val[0]
                state.date_start = date.fromisoformat(start_val)
            except (ValueError, TypeError):
                pass

        if "end" in params:
            try:
                end_val = params["end"]
                if isinstance(end_val, list):
                    end_val = end_val[0]
                state.date_end = date.fromisoformat(end_val)
            except (ValueError, TypeError):
                pass

        return state


# Session state keys for filter state
FILTER_STATE_KEY = "maude_filter_state"
FILTER_PRESETS_KEY = "maude_filter_presets"
FILTER_HISTORY_KEY = "maude_filter_history"
PENDING_FILTERS_KEY = "maude_pending_filters"


def get_filter_state() -> FilterState:
    """
    Get the current filter state from session state.

    Returns:
        Current FilterState, initialized with defaults if not set.
    """
    # First, check for pending filters from navigation
    if PENDING_FILTERS_KEY in st.session_state:
        pending = st.session_state[PENDING_FILTERS_KEY]
        del st.session_state[PENDING_FILTERS_KEY]

        # Apply pending filters to current state
        current = st.session_state.get(FILTER_STATE_KEY, FilterState())
        merged = current.merge(pending, override=True)
        st.session_state[FILTER_STATE_KEY] = merged
        return merged

    if FILTER_STATE_KEY not in st.session_state:
        # Initialize with empty filters (show all data by default)
        st.session_state[FILTER_STATE_KEY] = FilterState(
            date_start=date.today() - timedelta(days=365 * 5),
            date_end=date.today(),
        )

    return st.session_state[FILTER_STATE_KEY]


def update_filter_state(**kwargs) -> FilterState:
    """
    Update the filter state with new values.

    Args:
        **kwargs: Filter state attributes to update.

    Returns:
        Updated FilterState.
    """
    state = get_filter_state()

    # Save current state to history before updating
    _push_filter_history(state)

    for key, value in kwargs.items():
        if hasattr(state, key):
            setattr(state, key, value)

    st.session_state[FILTER_STATE_KEY] = state
    return state


def set_pending_filters(
    product_codes: Optional[List[str]] = None,
    manufacturers: Optional[List[str]] = None,
    event_types: Optional[List[str]] = None,
    date_start: Optional[date] = None,
    date_end: Optional[date] = None,
) -> None:
    """
    Set pending filters to be applied on next page load.

    This is used for cross-page navigation where filters should be
    applied when the destination page loads.

    Args:
        product_codes: Product codes to filter by.
        manufacturers: Manufacturers to filter by.
        event_types: Event types to filter by.
        date_start: Start date for date range.
        date_end: End date for date range.
    """
    pending = FilterState(
        product_codes=product_codes or [],
        manufacturers=manufacturers or [],
        event_types=event_types or [],
        date_start=date_start,
        date_end=date_end,
    )
    st.session_state[PENDING_FILTERS_KEY] = pending


def clear_filters() -> FilterState:
    """
    Reset all filters to defaults (empty).

    Returns:
        Reset FilterState.
    """
    # Save current state to history before clearing
    if FILTER_STATE_KEY in st.session_state:
        _push_filter_history(st.session_state[FILTER_STATE_KEY])

    state = FilterState(
        date_start=date.today() - timedelta(days=365 * 5),
        date_end=date.today(),
    )
    st.session_state[FILTER_STATE_KEY] = state
    return state


def _push_filter_history(state: FilterState) -> None:
    """
    Push current filter state to history stack.

    Args:
        state: FilterState to save.
    """
    if state.is_empty:
        return  # Don't save empty states

    history = st.session_state.get(FILTER_HISTORY_KEY, [])

    # Don't add duplicate consecutive entries
    if history and history[-1] == state.to_dict():
        return

    history.append(state.to_dict())

    # Keep history limited to last 10 states
    if len(history) > 10:
        history = history[-10:]

    st.session_state[FILTER_HISTORY_KEY] = history


def pop_filter_history() -> Optional[FilterState]:
    """
    Pop and return the previous filter state from history.

    Returns:
        Previous FilterState, or None if history is empty.
    """
    history = st.session_state.get(FILTER_HISTORY_KEY, [])

    if not history:
        return None

    prev_state_dict = history.pop()
    st.session_state[FILTER_HISTORY_KEY] = history

    return FilterState.from_dict(prev_state_dict)


def restore_previous_filters() -> bool:
    """
    Restore the previous filter state from history.

    Returns:
        True if filters were restored, False if no history.
    """
    prev = pop_filter_history()
    if prev:
        st.session_state[FILTER_STATE_KEY] = prev
        return True
    return False


def get_filter_history_count() -> int:
    """Get the number of filter states in history."""
    return len(st.session_state.get(FILTER_HISTORY_KEY, []))


def get_filter_presets() -> Dict[str, FilterPreset]:
    """
    Get all saved filter presets.

    Returns:
        Dictionary of preset name to FilterPreset.
    """
    if FILTER_PRESETS_KEY not in st.session_state:
        # Initialize with default presets
        st.session_state[FILTER_PRESETS_KEY] = get_default_presets()

    return st.session_state[FILTER_PRESETS_KEY]


def get_default_presets() -> Dict[str, FilterPreset]:
    """
    Get the default filter presets.

    Note: The only default is "All Products" (no filtering).
    Users can select specific product categories via the preset selector.
    """
    return {
        "All Products": FilterPreset(
            name="All Products",
            product_codes=[],  # Empty = all products
            manufacturers=[],  # Empty = all manufacturers
            event_types=[],    # Empty = all event types
        ),
        # Additional presets are optional selections, not defaults
        "Death Events": FilterPreset(
            name="Death Events",
            product_codes=[],
            manufacturers=[],
            event_types=["D"],
        ),
        "High Severity": FilterPreset(
            name="High Severity",
            product_codes=[],
            manufacturers=[],
            event_types=["D", "IN"],
        ),
    }


def save_preset(name: str, filter_state: FilterState) -> None:
    """
    Save current filter state as a preset.

    Args:
        name: Name for the preset.
        filter_state: FilterState to save.
    """
    presets = get_filter_presets()
    presets[name] = FilterPreset(
        name=name,
        product_codes=list(filter_state.product_codes),
        manufacturers=list(filter_state.manufacturers),
        event_types=list(filter_state.event_types),
        date_start=filter_state.date_start,
        date_end=filter_state.date_end,
    )
    st.session_state[FILTER_PRESETS_KEY] = presets


def delete_preset(name: str) -> bool:
    """
    Delete a saved preset.

    Args:
        name: Name of preset to delete.

    Returns:
        True if deleted, False if preset was a default.
    """
    presets = get_filter_presets()
    defaults = get_default_presets()

    # Don't delete default presets
    if name in defaults:
        return False

    if name in presets:
        del presets[name]
        st.session_state[FILTER_PRESETS_KEY] = presets
        return True

    return False


def apply_preset(name: str) -> FilterState:
    """
    Apply a preset to the current filter state.

    Args:
        name: Name of preset to apply.

    Returns:
        Updated FilterState.
    """
    presets = get_filter_presets()
    if name in presets:
        state = get_filter_state()
        _push_filter_history(state)  # Save before applying
        state.apply_preset(presets[name])
        st.session_state[FILTER_STATE_KEY] = state
        return state

    return get_filter_state()


def validate_filters_against_data(
    available_product_codes: Optional[List[str]] = None,
    available_manufacturers: Optional[List[str]] = None,
    available_event_types: Optional[List[str]] = None,
) -> Tuple[FilterState, List[str]]:
    """
    Validate current filter state against available data options.

    Removes any filter values that don't exist in the available data.

    Args:
        available_product_codes: List of valid product codes.
        available_manufacturers: List of valid manufacturers.
        available_event_types: List of valid event types.

    Returns:
        Tuple of (validated FilterState, list of removed values).
    """
    state = get_filter_state()
    removed = []

    if available_product_codes is not None and state.product_codes:
        available_set = set(available_product_codes)
        valid = [pc for pc in state.product_codes if pc in available_set]
        invalid = [pc for pc in state.product_codes if pc not in available_set]
        if invalid:
            removed.extend([f"Product: {pc}" for pc in invalid])
            state.product_codes = valid

    if available_manufacturers is not None and state.manufacturers:
        available_set = set(available_manufacturers)
        valid = [m for m in state.manufacturers if m in available_set]
        invalid = [m for m in state.manufacturers if m not in available_set]
        if invalid:
            removed.extend([f"Manufacturer: {m}" for m in invalid])
            state.manufacturers = valid

    if available_event_types is not None and state.event_types:
        available_set = set(available_event_types)
        valid = [e for e in state.event_types if e in available_set]
        invalid = [e for e in state.event_types if e not in available_set]
        if invalid:
            removed.extend([f"Event type: {e}" for e in invalid])
            state.event_types = valid

    if removed:
        st.session_state[FILTER_STATE_KEY] = state

    return state, removed


def sync_filters_to_url() -> None:
    """
    Sync current filter state to browser URL parameters.

    This enables bookmarking and sharing of filtered views.
    """
    state = get_filter_state()
    params = state.to_url_params()

    try:
        if params:
            st.query_params.update(params)
        else:
            st.query_params.clear()
    except Exception:
        pass  # Gracefully handle if query_params not available


def sync_filters_from_url() -> bool:
    """
    Sync filter state from browser URL parameters.

    Should be called once at app startup.

    Returns:
        True if URL params were applied, False otherwise.
    """
    try:
        params = st.query_params.to_dict()
    except Exception:
        return False

    if not params:
        return False

    # Check if any filter params are present
    filter_params = ["pc", "mfr", "evt", "start", "end"]
    if not any(p in params for p in filter_params):
        return False

    # Create filter state from URL params
    url_state = FilterState.from_url_params(params)

    # Merge with existing state (URL takes precedence)
    current = get_filter_state()
    merged = current.merge(url_state, override=True)
    st.session_state[FILTER_STATE_KEY] = merged

    return True


def render_filter_history_button() -> None:
    """Render a back button to restore previous filters if history exists."""
    history_count = get_filter_history_count()

    if history_count > 0:
        if st.button(
            f"Restore Previous Filters ({history_count})",
            key="restore_filters_btn",
            type="secondary",
        ):
            if restore_previous_filters():
                st.rerun()
