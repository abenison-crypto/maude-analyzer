"""Reusable filter components for MAUDE Analyzer.

This module provides centralized filter state management and reusable
filter UI components that can be used across all pages.

Usage:
    from app.components.filters import (
        get_filter_state,
        render_filter_bar,
        get_query_filters,
    )

    # In your page:
    filter_options = get_filter_options(conn)  # From database
    filter_state = render_filter_bar(filter_options)

    # Use filters in queries:
    filters = get_query_filters()
    data = my_query_function(
        manufacturers=filters['manufacturers'],
        product_codes=filters['product_codes'],
        ...
    )
"""

from .filter_state import (
    FilterState,
    FilterPreset,
    get_filter_state,
    update_filter_state,
    clear_filters,
    get_filter_presets,
    save_preset,
    delete_preset,
    apply_preset,
)

from .product_filter import (
    render_product_filter,
    render_product_filter_compact,
)

from .manufacturer_filter import (
    render_manufacturer_filter,
    render_manufacturer_filter_compact,
)

from .date_filter import (
    render_date_filter,
    render_date_filter_with_presets,
    render_date_filter_compact,
    DATE_PRESETS,
)

from .filter_presets import (
    render_preset_selector,
    render_preset_manager,
    render_preset_chips,
)

from .combined_filter import (
    render_filter_bar,
    render_filter_indicator,
    get_query_filters,
)


__all__ = [
    # State management
    "FilterState",
    "FilterPreset",
    "get_filter_state",
    "update_filter_state",
    "clear_filters",
    "get_filter_presets",
    "save_preset",
    "delete_preset",
    "apply_preset",
    # Product filter
    "render_product_filter",
    "render_product_filter_compact",
    # Manufacturer filter
    "render_manufacturer_filter",
    "render_manufacturer_filter_compact",
    # Date filter
    "render_date_filter",
    "render_date_filter_with_presets",
    "render_date_filter_compact",
    "DATE_PRESETS",
    # Presets
    "render_preset_selector",
    "render_preset_manager",
    "render_preset_chips",
    # Combined
    "render_filter_bar",
    "render_filter_indicator",
    "get_query_filters",
]
