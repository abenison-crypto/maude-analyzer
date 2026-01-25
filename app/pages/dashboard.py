"""Dashboard page for MAUDE Analyzer."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config, EVENT_TYPES, CHART_COLORS
from src.database import get_connection
from src.analysis import (
    get_mdr_summary,
    get_manufacturer_comparison,
    get_trend_data,
    get_event_type_breakdown,
    cached_dashboard_data,
    cached_mdr_summary,
)


def render_dashboard():
    """Render the dashboard page."""
    # Check database
    if not config.database.path.exists():
        st.warning("Database not found. Please run initial_load.py first.")
        st.code("python scripts/initial_load.py", language="bash")
        return

    try:
        # Use cached dashboard data for better performance
        dashboard_data = cached_dashboard_data()
        summary = dashboard_data["summary"]

        # KPI Cards
        render_kpi_cards(summary)

        st.divider()

        # Main charts row
        col1, col2 = st.columns(2)

        with col1:
            render_trend_chart_cached(dashboard_data)

        with col2:
            render_event_type_chart_cached(dashboard_data)

        st.divider()

        # Manufacturer comparison
        render_manufacturer_comparison_cached(dashboard_data)

    except Exception as e:
        st.error(f"Error loading dashboard: {e}")
        import traceback
        st.code(traceback.format_exc())


def render_kpi_cards(summary: dict):
    """Render KPI metric cards."""
    col1, col2, col3, col4 = st.columns(4)

    total = summary.get("total_mdrs", 0)
    deaths = summary.get("deaths", 0)
    injuries = summary.get("injuries", 0)
    malfunctions = summary.get("malfunctions", 0)

    with col1:
        st.metric(
            "Total MDRs",
            f"{total:,}",
            help="Total Medical Device Reports",
        )

    with col2:
        death_pct = (deaths / total * 100) if total > 0 else 0
        st.metric(
            "Deaths",
            f"{deaths:,}",
            delta=f"{death_pct:.1f}%",
            delta_color="inverse",
            help="Total death events (Event Type: D)",
        )

    with col3:
        injury_pct = (injuries / total * 100) if total > 0 else 0
        st.metric(
            "Injuries",
            f"{injuries:,}",
            delta=f"{injury_pct:.1f}%",
            delta_color="inverse",
            help="Total injury events (Event Type: IN)",
        )

    with col4:
        malfunction_pct = (malfunctions / total * 100) if total > 0 else 0
        st.metric(
            "Malfunctions",
            f"{malfunctions:,}",
            delta=f"{malfunction_pct:.1f}%",
            delta_color="off",
            help="Total malfunction events (Event Type: M)",
        )

    # Secondary stats
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Manufacturers",
            summary.get("unique_manufacturers", 0),
            help="Unique standardized manufacturer names",
        )

    with col2:
        st.metric(
            "Product Codes",
            summary.get("unique_product_codes", 0),
            help="Unique FDA product codes",
        )

    with col3:
        earliest = summary.get("earliest_date")
        if earliest:
            st.metric(
                "Data From",
                str(earliest)[:10] if earliest else "N/A",
                help="Earliest report date",
            )
        else:
            st.metric("Data From", "N/A")

    with col4:
        latest = summary.get("latest_date")
        if latest:
            st.metric(
                "Data To",
                str(latest)[:10] if latest else "N/A",
                help="Latest report date",
            )
        else:
            st.metric("Data To", "N/A")


def render_trend_chart_cached(dashboard_data: dict):
    """Render MDR trend over time chart using cached data."""
    st.subheader("MDR Trends Over Time")

    try:
        trend_df = dashboard_data.get("trend", pd.DataFrame())

        if trend_df.empty:
            st.info("No trend data available.")
            return

        # Convert period to datetime
        trend_df = trend_df.copy()
        trend_df["period"] = pd.to_datetime(trend_df["period"].astype(str))

        # Create figure
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=trend_df["period"],
            y=trend_df["count"],
            name="Total MDRs",
            line=dict(color=CHART_COLORS["primary"], width=2),
            mode="lines",
            fill="tozeroy",
            fillcolor=f"rgba(31, 119, 180, 0.2)",
        ))

        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Number of Reports",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            margin=dict(l=0, r=0, t=30, b=0),
            height=400,
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading trend data: {e}")


def render_event_type_chart_cached(dashboard_data: dict):
    """Render event type distribution pie chart using cached data."""
    st.subheader("Event Type Distribution")

    try:
        event_df = dashboard_data.get("event_counts", pd.DataFrame())

        if event_df.empty:
            st.info("No event type data available.")
            return

        # Map event types to labels
        event_df = event_df.copy()
        event_df["label"] = event_df["event_type"].map(EVENT_TYPES)

        # Color mapping
        color_map = {
            "D": CHART_COLORS["death"],
            "IN": CHART_COLORS["injury"],
            "M": CHART_COLORS["malfunction"],
            "O": CHART_COLORS["other"],
        }

        fig = px.pie(
            event_df,
            values="count",
            names="label",
            color="event_type",
            color_discrete_map={k: v for k, v in color_map.items()},
            hole=0.4,
        )

        fig.update_traces(
            textposition="inside",
            textinfo="percent+label",
        )

        fig.update_layout(
            showlegend=False,
            margin=dict(l=0, r=0, t=30, b=0),
            height=400,
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading event type data: {e}")


def render_manufacturer_comparison_cached(dashboard_data: dict):
    """Render manufacturer comparison bar chart using cached data."""
    st.subheader("MDRs by Manufacturer")

    try:
        comparison_df = dashboard_data.get("top_manufacturers", pd.DataFrame())

        if comparison_df.empty:
            st.info("No manufacturer data available.")
            return

        # Create horizontal bar chart
        fig = px.bar(
            comparison_df.sort_values("count", ascending=True),
            x="count",
            y="manufacturer_clean",
            orientation="h",
            color="count",
            color_continuous_scale="Blues",
            labels={
                "count": "Total MDRs",
                "manufacturer_clean": "Manufacturer",
            },
        )

        fig.update_layout(
            showlegend=False,
            coloraxis_showscale=False,
            xaxis_title="Number of Reports",
            yaxis_title="",
            margin=dict(l=0, r=0, t=10, b=0),
            height=450,
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading manufacturer comparison: {e}")
