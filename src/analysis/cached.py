"""Cached versions of query functions for Streamlit.

Provides tiered caching strategy:
- Session cache: Per-session data
- Short TTL (5 min): Frequently changing data
- Long TTL (30 min): Expensive aggregations
- Pre-computed: Use daily_aggregates table when available
"""

import streamlit as st
import pandas as pd
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.database import get_connection
from src.analysis.queries import (
    get_mdr_summary,
    get_manufacturer_comparison,
    get_trend_data,
    get_event_type_breakdown,
    get_filter_options,
    get_schema_aware_summary,
    get_filter_options_with_counts,
)
from src.database.schema_inspector import get_inspector

# Cache TTL tiers (in seconds)
SHORT_TTL = 300      # 5 minutes - for data that changes often
DEFAULT_TTL = 300    # 5 minutes - standard operations
LONG_TTL = 1800      # 30 minutes - expensive aggregations
STATIC_TTL = 3600    # 1 hour - relatively static data

# Session-level cache keys
_SESSION_CACHE_PREFIX = "_cached_"


@st.cache_data(ttl=DEFAULT_TTL, show_spinner=False)
def cached_mdr_summary() -> Dict[str, Any]:
    """Get cached MDR summary statistics."""
    with get_connection() as conn:
        return get_mdr_summary(conn)


@st.cache_data(ttl=DEFAULT_TTL, show_spinner=False)
def cached_filter_options() -> Dict[str, List[str]]:
    """Get cached filter options (manufacturers, product codes, event types)."""
    with get_connection() as conn:
        return get_filter_options(conn)


@st.cache_data(ttl=DEFAULT_TTL, show_spinner=False)
def cached_manufacturer_comparison(
    manufacturers: Optional[Tuple[str, ...]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    """Get cached manufacturer comparison data.

    Note: Using Tuple instead of List for hashability with st.cache_data.
    """
    mfrs_list = list(manufacturers) if manufacturers else None
    with get_connection() as conn:
        return get_manufacturer_comparison(
            manufacturers=mfrs_list,
            start_date=start_date,
            end_date=end_date,
            conn=conn,
        )


@st.cache_data(ttl=DEFAULT_TTL, show_spinner=False)
def cached_trend_data(
    aggregation: str = "monthly",
    manufacturers: Optional[Tuple[str, ...]] = None,
    product_codes: Optional[Tuple[str, ...]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    """Get cached trend data.

    Note: Using Tuple instead of List for hashability with st.cache_data.
    """
    mfrs_list = list(manufacturers) if manufacturers else None
    codes_list = list(product_codes) if product_codes else None
    with get_connection() as conn:
        return get_trend_data(
            aggregation=aggregation,
            manufacturers=mfrs_list,
            product_codes=codes_list,
            start_date=start_date,
            end_date=end_date,
            conn=conn,
        )


@st.cache_data(ttl=DEFAULT_TTL, show_spinner=False)
def cached_event_type_breakdown(
    manufacturers: Optional[Tuple[str, ...]] = None,
    product_codes: Optional[Tuple[str, ...]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    """Get cached event type breakdown.

    Note: Using Tuple instead of List for hashability with st.cache_data.
    """
    mfrs_list = list(manufacturers) if manufacturers else None
    codes_list = list(product_codes) if product_codes else None
    with get_connection() as conn:
        return get_event_type_breakdown(
            manufacturers=mfrs_list,
            product_codes=codes_list,
            start_date=start_date,
            end_date=end_date,
            conn=conn,
        )


@st.cache_data(ttl=600, show_spinner=False)
def cached_dashboard_data() -> Dict[str, Any]:
    """Get all data needed for dashboard in a single cached call."""
    with get_connection() as conn:
        summary = get_mdr_summary(conn)

        # Monthly trend for last year
        trend = conn.execute("""
            SELECT
                DATE_TRUNC('month', date_received) as period,
                COUNT(*) as count
            FROM master_events
            WHERE date_received >= CURRENT_DATE - INTERVAL '1 year'
            GROUP BY DATE_TRUNC('month', date_received)
            ORDER BY period
        """).fetchdf()

        # Top manufacturers
        top_manufacturers = conn.execute("""
            SELECT
                manufacturer_clean,
                COUNT(*) as count
            FROM master_events
            GROUP BY manufacturer_clean
            ORDER BY count DESC
            LIMIT 10
        """).fetchdf()

        # Event type counts
        event_counts = conn.execute("""
            SELECT
                event_type,
                COUNT(*) as count
            FROM master_events
            GROUP BY event_type
            ORDER BY count DESC
        """).fetchdf()

        return {
            "summary": summary,
            "trend": trend,
            "top_manufacturers": top_manufacturers,
            "event_counts": event_counts,
        }


@st.cache_data(ttl=DEFAULT_TTL, show_spinner=False)
def cached_product_codes_with_counts() -> pd.DataFrame:
    """Get product codes with their MDR counts."""
    with get_connection() as conn:
        return conn.execute("""
            SELECT
                product_code,
                COUNT(*) as count
            FROM master_events
            WHERE product_code IS NOT NULL
            GROUP BY product_code
            ORDER BY count DESC
        """).fetchdf()


@st.cache_data(ttl=DEFAULT_TTL, show_spinner=False)
def cached_manufacturer_list() -> List[str]:
    """Get list of manufacturers sorted by MDR count."""
    with get_connection() as conn:
        return conn.execute("""
            SELECT manufacturer_clean
            FROM master_events
            WHERE manufacturer_clean IS NOT NULL
            GROUP BY manufacturer_clean
            ORDER BY COUNT(*) DESC
        """).fetchdf()["manufacturer_clean"].tolist()


# =============================================================================
# Enhanced Caching Functions
# =============================================================================

@st.cache_data(ttl=LONG_TTL, show_spinner=False)
def cached_schema_aware_summary() -> Dict[str, Any]:
    """Get MDR summary with schema awareness and data quality info."""
    with get_connection() as conn:
        return get_schema_aware_summary(conn)


@st.cache_data(ttl=LONG_TTL, show_spinner=False)
def cached_filter_options_with_counts(limit: int = 100) -> Dict[str, List[Tuple[str, int]]]:
    """Get filter options with counts for each value."""
    with get_connection() as conn:
        return get_filter_options_with_counts(conn, limit)


@st.cache_data(ttl=LONG_TTL, show_spinner=False)
def cached_dashboard_from_aggregates(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Optional[Dict[str, Any]]:
    """
    Get dashboard data from pre-computed daily_aggregates table.

    Falls back to None if aggregates table doesn't exist or is empty.
    """
    with get_connection() as conn:
        inspector = get_inspector(conn)

        # Check if daily_aggregates table exists and has data
        try:
            result = conn.execute("SELECT COUNT(*) FROM daily_aggregates").fetchone()
            if not result or result[0] == 0:
                return None
        except Exception:
            return None

        # Build date filter
        date_filter = ""
        params = []
        if start_date:
            date_filter += " AND date >= ?"
            params.append(start_date)
        if end_date:
            date_filter += " AND date <= ?"
            params.append(end_date)

        try:
            # Summary totals
            summary = conn.execute(f"""
                SELECT
                    SUM(event_count) as total_mdrs,
                    SUM(death_count) as deaths,
                    SUM(injury_count) as injuries,
                    SUM(malfunction_count) as malfunctions,
                    COUNT(DISTINCT manufacturer_clean) as unique_manufacturers,
                    COUNT(DISTINCT product_code) as unique_product_codes,
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date
                FROM daily_aggregates
                WHERE 1=1 {date_filter}
            """, params).fetchone()

            # Monthly trend
            trend = conn.execute(f"""
                SELECT
                    DATE_TRUNC('month', date) as period,
                    SUM(event_count) as count
                FROM daily_aggregates
                WHERE 1=1 {date_filter}
                GROUP BY DATE_TRUNC('month', date)
                ORDER BY period
            """, params).fetchdf()

            # Top manufacturers
            top_manufacturers = conn.execute(f"""
                SELECT
                    manufacturer_clean,
                    SUM(event_count) as count
                FROM daily_aggregates
                WHERE manufacturer_clean IS NOT NULL {date_filter}
                GROUP BY manufacturer_clean
                ORDER BY count DESC
                LIMIT 10
            """, params).fetchdf()

            # Event type distribution
            event_counts = conn.execute(f"""
                SELECT
                    event_type,
                    SUM(event_count) as count
                FROM daily_aggregates
                WHERE 1=1 {date_filter}
                GROUP BY event_type
                ORDER BY count DESC
            """, params).fetchdf()

            return {
                "summary": {
                    "total_mdrs": summary[0] or 0,
                    "deaths": summary[1] or 0,
                    "injuries": summary[2] or 0,
                    "malfunctions": summary[3] or 0,
                    "unique_manufacturers": summary[4] or 0,
                    "unique_product_codes": summary[5] or 0,
                    "earliest_date": summary[6],
                    "latest_date": summary[7],
                },
                "trend": trend,
                "top_manufacturers": top_manufacturers,
                "event_counts": event_counts,
                "_source": "daily_aggregates",
            }
        except Exception as e:
            return None


@st.cache_data(ttl=STATIC_TTL, show_spinner=False)
def cached_product_code_lookup() -> Dict[str, Dict[str, str]]:
    """Get product code to description lookup."""
    with get_connection() as conn:
        try:
            result = conn.execute("""
                SELECT product_code, device_name, device_class, medical_specialty
                FROM product_codes
            """).fetchdf()

            lookup = {}
            for _, row in result.iterrows():
                lookup[row["product_code"]] = {
                    "name": row["device_name"],
                    "class": row["device_class"],
                    "specialty": row["medical_specialty"],
                }
            return lookup
        except Exception:
            return {}


@st.cache_data(ttl=STATIC_TTL, show_spinner=False)
def cached_data_quality_summary() -> Dict[str, Any]:
    """Get data quality summary for key columns."""
    with get_connection() as conn:
        inspector = get_inspector(conn)

        key_columns = [
            "manufacturer_clean",
            "product_code",
            "event_type",
            "date_received",
            "date_of_event",
        ]

        coverage = inspector.get_data_coverage("master_events", key_columns)
        schema = inspector.get_table_schema("master_events")

        return {
            "coverage": {col: cov.coverage_pct for col, cov in coverage.items()},
            "sparse_columns": inspector.get_sparse_columns("master_events"),
            "well_populated_columns": inspector.get_well_populated_columns("master_events"),
            "total_rows": schema.row_count if schema else 0,
            "total_columns": len(schema.columns) if schema else 0,
        }


def get_or_compute_dashboard_data(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Get dashboard data, preferring pre-computed aggregates.

    Falls back to real-time computation if aggregates aren't available.
    """
    # Try aggregates first
    agg_data = cached_dashboard_from_aggregates(start_date, end_date)
    if agg_data is not None:
        return agg_data

    # Fall back to cached real-time computation
    return cached_dashboard_data()


# =============================================================================
# Session-Level Caching Utilities
# =============================================================================

def get_session_cache(key: str) -> Optional[Any]:
    """Get a value from session-level cache."""
    cache_key = f"{_SESSION_CACHE_PREFIX}{key}"
    return st.session_state.get(cache_key)


def set_session_cache(key: str, value: Any) -> None:
    """Set a value in session-level cache."""
    cache_key = f"{_SESSION_CACHE_PREFIX}{key}"
    st.session_state[cache_key] = value


def clear_session_cache(key: Optional[str] = None) -> None:
    """Clear session cache. If key provided, only that key; otherwise all."""
    if key:
        cache_key = f"{_SESSION_CACHE_PREFIX}{key}"
        if cache_key in st.session_state:
            del st.session_state[cache_key]
    else:
        keys_to_delete = [
            k for k in st.session_state.keys()
            if k.startswith(_SESSION_CACHE_PREFIX)
        ]
        for k in keys_to_delete:
            del st.session_state[k]


def clear_all_caches() -> None:
    """Clear all caches (both Streamlit and session)."""
    st.cache_data.clear()
    clear_session_cache()


@st.cache_data(ttl=LONG_TTL, show_spinner="Loading data coverage...")
def cached_column_coverage(table: str) -> Dict[str, float]:
    """Get coverage percentages for all columns in a table."""
    with get_connection() as conn:
        inspector = get_inspector(conn)
        coverage = inspector.get_data_coverage(table)
        return {col: cov.coverage_pct for col, cov in coverage.items()}
