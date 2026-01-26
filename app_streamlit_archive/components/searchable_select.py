"""Searchable select components for large option lists.

Provides server-side filtering for dropdowns with 50k+ options.
"""

from typing import List, Optional, Tuple, Any, Callable
from dataclasses import dataclass
import streamlit as st
import duckdb


@dataclass
class SelectOption:
    """An option for the searchable select."""
    value: str
    label: str
    count: Optional[int] = None

    def display_label(self) -> str:
        """Get display label with optional count."""
        if self.count is not None:
            return f"{self.label} ({self.count:,})"
        return self.label


class SearchableSelect:
    """
    A searchable select component that uses server-side filtering.

    Features:
    - Server-side LIKE queries for filtering
    - Shows top options by count as defaults
    - Supports single and multi-select
    - Optional count display next to each option
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        table: str,
        column: str,
        key: str,
        label: str = "Select",
        count_column: Optional[str] = None,
        max_options: int = 50,
        min_search_chars: int = 2,
    ):
        """
        Initialize searchable select.

        Args:
            conn: DuckDB connection
            table: Table to query
            column: Column containing option values
            key: Unique key for Streamlit widget
            label: Label to display
            count_column: Optional column to count for each option
            max_options: Maximum options to show
            min_search_chars: Minimum chars before searching
        """
        self.conn = conn
        self.table = table
        self.column = column
        self.key = key
        self.label = label
        self.count_column = count_column
        self.max_options = max_options
        self.min_search_chars = min_search_chars

    def _get_top_options(self) -> List[SelectOption]:
        """Get top options by count."""
        try:
            if self.count_column:
                sql = f"""
                    SELECT {self.column}, COUNT({self.count_column}) as cnt
                    FROM {self.table}
                    WHERE {self.column} IS NOT NULL
                    GROUP BY {self.column}
                    ORDER BY cnt DESC
                    LIMIT {self.max_options}
                """
            else:
                sql = f"""
                    SELECT {self.column}, COUNT(*) as cnt
                    FROM {self.table}
                    WHERE {self.column} IS NOT NULL
                    GROUP BY {self.column}
                    ORDER BY cnt DESC
                    LIMIT {self.max_options}
                """

            result = self.conn.execute(sql).fetchall()
            return [
                SelectOption(value=row[0], label=str(row[0]), count=row[1])
                for row in result
            ]
        except Exception as e:
            st.error(f"Error loading options: {e}")
            return []

    def _search_options(self, search_text: str) -> List[SelectOption]:
        """Search options using LIKE query."""
        try:
            search_pattern = f"%{search_text}%"

            if self.count_column:
                sql = f"""
                    SELECT {self.column}, COUNT({self.count_column}) as cnt
                    FROM {self.table}
                    WHERE {self.column} IS NOT NULL
                    AND {self.column} ILIKE ?
                    GROUP BY {self.column}
                    ORDER BY cnt DESC
                    LIMIT {self.max_options}
                """
            else:
                sql = f"""
                    SELECT {self.column}, COUNT(*) as cnt
                    FROM {self.table}
                    WHERE {self.column} IS NOT NULL
                    AND {self.column} ILIKE ?
                    GROUP BY {self.column}
                    ORDER BY cnt DESC
                    LIMIT {self.max_options}
                """

            result = self.conn.execute(sql, [search_pattern]).fetchall()
            return [
                SelectOption(value=row[0], label=str(row[0]), count=row[1])
                for row in result
            ]
        except Exception as e:
            st.error(f"Error searching options: {e}")
            return []

    def render(
        self,
        multi: bool = False,
        default: Optional[List[str]] = None,
        show_counts: bool = True,
        placeholder: str = "Type to search...",
    ) -> Optional[List[str]]:
        """
        Render the searchable select component.

        Args:
            multi: Allow multiple selections
            default: Default selected values
            show_counts: Show counts next to options
            placeholder: Placeholder text for search

        Returns:
            Selected values (list for multi, single-item list for single)
        """
        # Search input
        search_key = f"{self.key}_search"
        search_text = st.text_input(
            self.label,
            placeholder=placeholder,
            key=search_key,
        )

        # Get options based on search
        if len(search_text) >= self.min_search_chars:
            options = self._search_options(search_text)
        else:
            options = self._get_top_options()

        if not options:
            st.info("No matching options found")
            return default or []

        # Format options for display
        if show_counts:
            option_labels = [opt.display_label() for opt in options]
        else:
            option_labels = [opt.label for opt in options]

        option_values = [opt.value for opt in options]

        # Handle default values
        default_indices = []
        if default:
            for d in default:
                if d in option_values:
                    default_indices.append(option_values.index(d))

        # Render selection
        if multi:
            selected_labels = st.multiselect(
                "Select options",
                options=option_labels,
                default=[option_labels[i] for i in default_indices] if default_indices else None,
                key=f"{self.key}_select",
                label_visibility="collapsed",
            )
            # Map back to values
            selected = []
            for label in selected_labels:
                idx = option_labels.index(label)
                selected.append(option_values[idx])
            return selected
        else:
            # Single select
            if default_indices:
                default_idx = default_indices[0]
            else:
                default_idx = 0

            selected_label = st.selectbox(
                "Select option",
                options=option_labels,
                index=default_idx,
                key=f"{self.key}_select",
                label_visibility="collapsed",
            )
            idx = option_labels.index(selected_label)
            return [option_values[idx]]


def searchable_manufacturer_select(
    conn: duckdb.DuckDBPyConnection,
    key: str = "manufacturer",
    label: str = "Manufacturer",
    multi: bool = True,
    default: Optional[List[str]] = None,
    show_counts: bool = True,
) -> List[str]:
    """
    Render a searchable manufacturer select.

    Args:
        conn: DuckDB connection
        key: Unique widget key
        label: Display label
        multi: Allow multiple selections
        default: Default selected manufacturers
        show_counts: Show event counts

    Returns:
        List of selected manufacturers (empty = all)
    """
    # Try manufacturer_clean first, fall back to manufacturer_name
    try:
        conn.execute("SELECT manufacturer_clean FROM master_events LIMIT 1")
        column = "manufacturer_clean"
    except Exception:
        column = "manufacturer_name"

    select = SearchableSelect(
        conn=conn,
        table="master_events",
        column=column,
        key=key,
        label=label,
        max_options=100,
    )

    return select.render(
        multi=multi,
        default=default,
        show_counts=show_counts,
        placeholder="Type to search manufacturers...",
    ) or []


def searchable_product_code_select(
    conn: duckdb.DuckDBPyConnection,
    key: str = "product_code",
    label: str = "Product Code",
    multi: bool = True,
    default: Optional[List[str]] = None,
    show_counts: bool = True,
) -> List[str]:
    """
    Render a searchable product code select.

    Args:
        conn: DuckDB connection
        key: Unique widget key
        label: Display label
        multi: Allow multiple selections
        default: Default selected product codes
        show_counts: Show event counts

    Returns:
        List of selected product codes (empty = all)
    """
    select = SearchableSelect(
        conn=conn,
        table="master_events",
        column="product_code",
        key=key,
        label=label,
        max_options=100,
    )

    return select.render(
        multi=multi,
        default=default,
        show_counts=show_counts,
        placeholder="Type to search product codes...",
    ) or []


def render_searchable_filter(
    conn: duckdb.DuckDBPyConnection,
    filter_type: str,
    key: str,
    default: Optional[List[str]] = None,
    multi: bool = True,
) -> List[str]:
    """
    Render a searchable filter based on type.

    Args:
        conn: DuckDB connection
        filter_type: Type of filter ("manufacturer", "product_code")
        key: Unique widget key
        default: Default values
        multi: Allow multiple selections

    Returns:
        Selected values (empty = all)
    """
    if filter_type == "manufacturer":
        return searchable_manufacturer_select(
            conn, key=key, default=default, multi=multi
        )
    elif filter_type == "product_code":
        return searchable_product_code_select(
            conn, key=key, default=default, multi=multi
        )
    else:
        st.warning(f"Unknown filter type: {filter_type}")
        return []


class CachedSearchableSelect:
    """
    Searchable select with caching for improved performance.

    Caches the top options and uses session state to avoid
    re-querying the database on every interaction.
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        table: str,
        column: str,
        key: str,
        label: str = "Select",
        cache_ttl_seconds: int = 300,
        max_options: int = 100,
    ):
        """
        Initialize cached searchable select.

        Args:
            conn: DuckDB connection
            table: Table to query
            column: Column containing option values
            key: Unique key for Streamlit widget
            label: Label to display
            cache_ttl_seconds: Cache time-to-live in seconds
            max_options: Maximum options to cache
        """
        self.conn = conn
        self.table = table
        self.column = column
        self.key = key
        self.label = label
        self.cache_ttl = cache_ttl_seconds
        self.max_options = max_options

        # Cache key in session state
        self._cache_key = f"_cached_options_{key}"
        self._cache_time_key = f"_cached_options_time_{key}"

    def _get_cached_options(self) -> Optional[List[Tuple[str, int]]]:
        """Get cached options if still valid."""
        import time

        if self._cache_key not in st.session_state:
            return None

        cache_time = st.session_state.get(self._cache_time_key, 0)
        if time.time() - cache_time > self.cache_ttl:
            return None

        return st.session_state[self._cache_key]

    def _cache_options(self, options: List[Tuple[str, int]]) -> None:
        """Cache options in session state."""
        import time

        st.session_state[self._cache_key] = options
        st.session_state[self._cache_time_key] = time.time()

    def get_all_options(self) -> List[Tuple[str, int]]:
        """Get all options with counts, using cache if available."""
        cached = self._get_cached_options()
        if cached is not None:
            return cached

        try:
            sql = f"""
                SELECT {self.column}, COUNT(*) as cnt
                FROM {self.table}
                WHERE {self.column} IS NOT NULL
                GROUP BY {self.column}
                ORDER BY cnt DESC
                LIMIT {self.max_options}
            """
            result = self.conn.execute(sql).fetchall()
            self._cache_options(result)
            return result
        except Exception as e:
            st.error(f"Error loading options: {e}")
            return []

    def render(
        self,
        multi: bool = True,
        default: Optional[List[str]] = None,
        show_counts: bool = True,
    ) -> List[str]:
        """
        Render the cached searchable select.

        Args:
            multi: Allow multiple selections
            default: Default selected values
            show_counts: Show counts next to options

        Returns:
            Selected values
        """
        options = self.get_all_options()

        if not options:
            st.info("No options available")
            return default or []

        # Format for display
        if show_counts:
            option_labels = [f"{opt[0]} ({opt[1]:,})" for opt in options]
        else:
            option_labels = [opt[0] for opt in options]

        option_values = [opt[0] for opt in options]

        # Handle defaults
        default_indices = []
        if default:
            for d in default:
                if d in option_values:
                    default_indices.append(option_values.index(d))

        # Search/filter
        search_text = st.text_input(
            self.label,
            placeholder="Type to filter...",
            key=f"{self.key}_search",
        )

        # Filter options locally
        if search_text:
            filtered_indices = [
                i for i, label in enumerate(option_labels)
                if search_text.lower() in label.lower()
            ]
            display_labels = [option_labels[i] for i in filtered_indices]
            display_values = [option_values[i] for i in filtered_indices]
        else:
            display_labels = option_labels
            display_values = option_values

        if not display_labels:
            st.info("No matching options")
            return []

        # Render selection
        if multi:
            # Filter default to only those in display_values
            valid_defaults = [d for d in (default or []) if d in display_values]
            default_labels = [
                display_labels[display_values.index(d)]
                for d in valid_defaults
            ]

            selected_labels = st.multiselect(
                "Select",
                options=display_labels,
                default=default_labels,
                key=f"{self.key}_select",
                label_visibility="collapsed",
            )

            return [
                display_values[display_labels.index(label)]
                for label in selected_labels
            ]
        else:
            # Single select
            idx = 0
            if default and default[0] in display_values:
                idx = display_values.index(default[0])

            selected = st.selectbox(
                "Select",
                options=display_labels,
                index=idx,
                key=f"{self.key}_select",
                label_visibility="collapsed",
            )

            return [display_values[display_labels.index(selected)]]

    def invalidate_cache(self) -> None:
        """Invalidate the cached options."""
        if self._cache_key in st.session_state:
            del st.session_state[self._cache_key]
        if self._cache_time_key in st.session_state:
            del st.session_state[self._cache_time_key]
