"""Cached versions of query functions for Streamlit."""

import streamlit as st
import pandas as pd
from datetime import date
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
)


# Cache TTL in seconds (5 minutes for most queries)
DEFAULT_TTL = 300


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
