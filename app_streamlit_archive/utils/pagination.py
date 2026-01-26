"""Server-side pagination for large datasets.

Provides efficient pagination for queries that return thousands/millions of rows.
"""

from typing import Optional, Tuple, Callable, Any, Dict, List
from dataclasses import dataclass
import pandas as pd
import streamlit as st
import duckdb

# Try to import config
try:
    from config.config_loader import get_table_config
    _CONFIG_AVAILABLE = True
except ImportError:
    _CONFIG_AVAILABLE = False


def _get_default_page_size() -> int:
    """Get default page size from config or fallback."""
    if _CONFIG_AVAILABLE:
        try:
            return get_table_config().default_page_size
        except Exception:
            pass
    return 25


def _get_page_size_options() -> List[int]:
    """Get available page size options."""
    if _CONFIG_AVAILABLE:
        try:
            return get_table_config().page_size_options
        except Exception:
            pass
    return [10, 25, 50, 100]


@dataclass
class PaginationState:
    """State for paginated queries."""
    page: int = 1
    page_size: int = 25
    total_count: Optional[int] = None
    _count_cached: bool = False

    @property
    def offset(self) -> int:
        """Calculate offset for current page."""
        return (self.page - 1) * self.page_size

    @property
    def total_pages(self) -> int:
        """Calculate total number of pages."""
        if self.total_count is None or self.total_count == 0:
            return 1
        return (self.total_count + self.page_size - 1) // self.page_size

    @property
    def has_previous(self) -> bool:
        """Check if there's a previous page."""
        return self.page > 1

    @property
    def has_next(self) -> bool:
        """Check if there's a next page."""
        return self.page < self.total_pages

    @property
    def start_row(self) -> int:
        """Get 1-based start row number for display."""
        if self.total_count == 0:
            return 0
        return self.offset + 1

    @property
    def end_row(self) -> int:
        """Get 1-based end row number for display."""
        if self.total_count is None:
            return self.offset + self.page_size
        return min(self.offset + self.page_size, self.total_count)

    def get_display_range(self) -> str:
        """Get formatted display range string."""
        if self.total_count is None or self.total_count == 0:
            return "No results"
        return f"Showing {self.start_row:,} - {self.end_row:,} of {self.total_count:,}"


class PaginatedQuery:
    """
    Executes paginated SQL queries with caching of total count.

    Usage:
        pq = PaginatedQuery(conn, base_sql, count_sql)
        df = pq.get_page(page=1, page_size=25)
        total = pq.total_count
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        base_sql: str,
        count_sql: Optional[str] = None,
        params: Optional[List[Any]] = None,
    ):
        """
        Initialize paginated query.

        Args:
            conn: DuckDB connection
            base_sql: Base SELECT query (without LIMIT/OFFSET)
            count_sql: Optional COUNT query (auto-generated if None)
            params: Query parameters
        """
        self.conn = conn
        self.base_sql = base_sql.rstrip().rstrip(";")
        self.params = params or []
        self._total_count: Optional[int] = None

        # Auto-generate count query if not provided
        if count_sql:
            self.count_sql = count_sql
        else:
            self.count_sql = self._generate_count_sql()

    def _generate_count_sql(self) -> str:
        """Generate a COUNT query from the base query."""
        # Simple approach: wrap in subquery
        return f"SELECT COUNT(*) FROM ({self.base_sql}) AS _count_subquery"

    @property
    def total_count(self) -> int:
        """Get total count, executing count query if needed."""
        if self._total_count is None:
            self._total_count = self._execute_count()
        return self._total_count

    def _execute_count(self) -> int:
        """Execute the count query."""
        try:
            result = self.conn.execute(self.count_sql, self.params).fetchone()
            return result[0] if result else 0
        except Exception as e:
            st.error(f"Error getting count: {e}")
            return 0

    def get_page(
        self,
        page: int = 1,
        page_size: int = 25,
    ) -> Tuple[pd.DataFrame, PaginationState]:
        """
        Get a single page of results.

        Args:
            page: Page number (1-based)
            page_size: Number of rows per page

        Returns:
            Tuple of (DataFrame, PaginationState)
        """
        offset = (page - 1) * page_size

        # Add LIMIT and OFFSET to query
        page_sql = f"{self.base_sql}\nLIMIT {page_size} OFFSET {offset}"

        try:
            df = self.conn.execute(page_sql, self.params).fetchdf()
        except Exception as e:
            st.error(f"Error executing query: {e}")
            df = pd.DataFrame()

        state = PaginationState(
            page=page,
            page_size=page_size,
            total_count=self.total_count,
        )

        return df, state

    def invalidate_count(self) -> None:
        """Invalidate cached count (call when filters change)."""
        self._total_count = None


def get_pagination_state(key: str = "pagination") -> PaginationState:
    """
    Get pagination state from Streamlit session state.

    Args:
        key: Session state key for this pagination instance

    Returns:
        PaginationState instance
    """
    state_key = f"_pagination_{key}"

    if state_key not in st.session_state:
        st.session_state[state_key] = PaginationState(
            page_size=_get_default_page_size()
        )

    return st.session_state[state_key]


def update_pagination_state(
    key: str = "pagination",
    **kwargs,
) -> PaginationState:
    """
    Update pagination state.

    Args:
        key: Session state key
        **kwargs: State attributes to update

    Returns:
        Updated PaginationState
    """
    state = get_pagination_state(key)

    for k, v in kwargs.items():
        if hasattr(state, k):
            setattr(state, k, v)

    st.session_state[f"_pagination_{key}"] = state
    return state


def reset_pagination(key: str = "pagination") -> None:
    """Reset pagination to page 1."""
    state = get_pagination_state(key)
    state.page = 1
    state.total_count = None
    st.session_state[f"_pagination_{key}"] = state


def render_pagination_controls(
    state: PaginationState,
    key: str = "pagination",
    show_page_size: bool = True,
    show_info: bool = True,
) -> Optional[int]:
    """
    Render pagination controls using Streamlit.

    Args:
        state: Current pagination state
        key: Unique key for this pagination instance
        show_page_size: Whether to show page size selector
        show_info: Whether to show "Showing X-Y of Z" info

    Returns:
        New page number if changed, None otherwise
    """
    new_page = None

    # Create columns for pagination controls
    if show_info:
        info_col, nav_col, size_col = st.columns([2, 2, 1])
    else:
        nav_col, size_col = st.columns([3, 1])
        info_col = None

    # Info display
    if info_col and show_info:
        with info_col:
            st.markdown(f"**{state.get_display_range()}**")

    # Navigation buttons
    with nav_col:
        btn_cols = st.columns([1, 1, 2, 1, 1])

        # First page
        with btn_cols[0]:
            if st.button("⏮", key=f"{key}_first", disabled=not state.has_previous):
                new_page = 1

        # Previous page
        with btn_cols[1]:
            if st.button("◀", key=f"{key}_prev", disabled=not state.has_previous):
                new_page = state.page - 1

        # Page indicator
        with btn_cols[2]:
            st.markdown(
                f"<div style='text-align: center; padding-top: 8px;'>"
                f"Page {state.page} of {state.total_pages}</div>",
                unsafe_allow_html=True
            )

        # Next page
        with btn_cols[3]:
            if st.button("▶", key=f"{key}_next", disabled=not state.has_next):
                new_page = state.page + 1

        # Last page
        with btn_cols[4]:
            if st.button("⏭", key=f"{key}_last", disabled=not state.has_next):
                new_page = state.total_pages

    # Page size selector
    if show_page_size:
        with size_col:
            page_sizes = _get_page_size_options()
            current_idx = page_sizes.index(state.page_size) if state.page_size in page_sizes else 0
            new_size = st.selectbox(
                "Per page",
                options=page_sizes,
                index=current_idx,
                key=f"{key}_page_size",
                label_visibility="collapsed",
            )
            if new_size != state.page_size:
                # Reset to page 1 when page size changes
                update_pagination_state(key, page_size=new_size, page=1)
                new_page = 1

    return new_page


def render_jump_to_page(
    state: PaginationState,
    key: str = "pagination",
) -> Optional[int]:
    """
    Render a "jump to page" input.

    Args:
        state: Current pagination state
        key: Unique key

    Returns:
        New page number if changed
    """
    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        new_page = st.number_input(
            "Go to page",
            min_value=1,
            max_value=max(1, state.total_pages),
            value=state.page,
            key=f"{key}_jump",
        )
        if new_page != state.page:
            return int(new_page)

    return None


class StreamlitPaginator:
    """
    High-level paginator that integrates with Streamlit session state.

    Usage:
        paginator = StreamlitPaginator("my_query", conn)
        df, state = paginator.paginate(base_sql, params)
        st.dataframe(df)
        paginator.render_controls()
    """

    def __init__(
        self,
        key: str,
        conn: duckdb.DuckDBPyConnection,
        page_size: Optional[int] = None,
    ):
        """
        Initialize paginator.

        Args:
            key: Unique key for this paginator
            conn: DuckDB connection
            page_size: Initial page size (uses config default if None)
        """
        self.key = key
        self.conn = conn
        self._query: Optional[PaginatedQuery] = None

        # Initialize state
        if page_size is None:
            page_size = _get_default_page_size()

        state = get_pagination_state(key)
        if state.page_size != page_size:
            update_pagination_state(key, page_size=page_size)

    @property
    def state(self) -> PaginationState:
        """Get current pagination state."""
        return get_pagination_state(self.key)

    def paginate(
        self,
        base_sql: str,
        params: Optional[List[Any]] = None,
        count_sql: Optional[str] = None,
    ) -> Tuple[pd.DataFrame, PaginationState]:
        """
        Execute paginated query and return results.

        Args:
            base_sql: Base SELECT query
            params: Query parameters
            count_sql: Optional custom count query

        Returns:
            Tuple of (DataFrame, PaginationState)
        """
        self._query = PaginatedQuery(
            self.conn,
            base_sql,
            count_sql,
            params,
        )

        state = self.state
        df, updated_state = self._query.get_page(
            page=state.page,
            page_size=state.page_size,
        )

        # Update session state with total count
        update_pagination_state(
            self.key,
            total_count=updated_state.total_count,
        )

        return df, self.state

    def render_controls(
        self,
        show_page_size: bool = True,
        show_info: bool = True,
    ) -> None:
        """
        Render pagination controls and handle page changes.

        Args:
            show_page_size: Whether to show page size selector
            show_info: Whether to show row count info
        """
        new_page = render_pagination_controls(
            self.state,
            self.key,
            show_page_size,
            show_info,
        )

        if new_page is not None:
            update_pagination_state(self.key, page=new_page)
            st.rerun()

    def reset(self) -> None:
        """Reset pagination to page 1."""
        reset_pagination(self.key)
        if self._query:
            self._query.invalidate_count()
