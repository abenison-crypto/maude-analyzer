"""Manufacturer Comparison page for MAUDE Analyzer."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config, EVENT_TYPES, CHART_COLORS, MANUFACTURER_COLORS, SCS_MANUFACTURERS
from src.database import get_connection
from src.analysis import (
    get_manufacturer_comparison,
    get_trend_data,
    get_event_type_breakdown,
    get_filter_options,
)


def render_comparison():
    """Render the manufacturer comparison page."""
    st.markdown("Compare adverse event profiles between manufacturers.")

    # Check database
    if not config.database.path.exists():
        st.warning("Database not found. Please run initial_load.py first.")
        return

    try:
        with get_connection() as conn:
            filter_options = get_filter_options(conn)
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return

    # Manufacturer selection
    st.subheader("Select Manufacturers to Compare")

    available_manufacturers = filter_options.get("manufacturers", [])
    default_manufacturers = [m for m in SCS_MANUFACTURERS if m in available_manufacturers][:4]

    selected_manufacturers = st.multiselect(
        "Manufacturers (select 2-6 for comparison)",
        options=available_manufacturers,
        default=default_manufacturers,
        help="Select manufacturers to compare side-by-side",
    )

    if len(selected_manufacturers) < 2:
        st.info("Please select at least 2 manufacturers to compare.")
        return

    if len(selected_manufacturers) > 6:
        st.warning("For best visualization, select 6 or fewer manufacturers.")

    # Date range filter
    col1, col2 = st.columns(2)
    with col1:
        date_range = st.date_input(
            "Date Range",
            value=(date.today() - timedelta(days=365 * 5), date.today()),
        )
    with col2:
        aggregation = st.selectbox(
            "Trend Aggregation",
            options=["monthly", "quarterly", "yearly"],
            index=0,
        )

    start_date = date_range[0] if len(date_range) > 0 else None
    end_date = date_range[1] if len(date_range) > 1 else None

    st.divider()

    # Get comparison data
    try:
        with get_connection() as conn:
            comparison_df = get_manufacturer_comparison(
                manufacturers=selected_manufacturers,
                start_date=start_date,
                end_date=end_date,
                conn=conn,
            )

            trend_df = get_trend_data(
                aggregation=aggregation,
                manufacturers=selected_manufacturers,
                start_date=start_date,
                end_date=end_date,
                conn=conn,
            )

            event_df = get_event_type_breakdown(
                manufacturers=selected_manufacturers,
                start_date=start_date,
                end_date=end_date,
                conn=conn,
            )
    except Exception as e:
        st.error(f"Error loading comparison data: {e}")
        return

    if comparison_df.empty:
        st.info("No data found for the selected manufacturers.")
        return

    # Summary comparison table
    st.subheader("Summary Comparison")
    render_summary_table(comparison_df)

    st.divider()

    # Charts row 1
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Total MDRs by Manufacturer")
        render_total_mdrs_chart(comparison_df)

    with col2:
        st.subheader("Event Type Distribution")
        render_event_distribution_chart(event_df)

    st.divider()

    # Trend comparison
    st.subheader("MDR Trends Comparison")
    render_trend_comparison(trend_df)

    st.divider()

    # Charts row 2
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Death Rate Comparison")
        render_death_rate_chart(comparison_df)

    with col2:
        st.subheader("Event Type Breakdown (Stacked)")
        render_stacked_event_chart(comparison_df)

    st.divider()

    # Radar chart comparison
    st.subheader("Multi-Metric Comparison")
    render_radar_chart(comparison_df)


def render_summary_table(df: pd.DataFrame):
    """Render summary comparison table."""
    display_df = df[[
        "manufacturer_clean",
        "total_mdrs",
        "deaths",
        "injuries",
        "malfunctions",
        "death_rate",
    ]].copy()

    display_df.columns = [
        "Manufacturer",
        "Total MDRs",
        "Deaths",
        "Injuries",
        "Malfunctions",
        "Death Rate (%)",
    ]

    # Sort by total MDRs
    display_df = display_df.sort_values("Total MDRs", ascending=False)

    # Style the dataframe
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Total MDRs": st.column_config.NumberColumn(format="%d"),
            "Deaths": st.column_config.NumberColumn(format="%d"),
            "Injuries": st.column_config.NumberColumn(format="%d"),
            "Malfunctions": st.column_config.NumberColumn(format="%d"),
            "Death Rate (%)": st.column_config.NumberColumn(format="%.2f%%"),
        },
    )


def render_total_mdrs_chart(df: pd.DataFrame):
    """Render total MDRs bar chart."""
    fig = px.bar(
        df.sort_values("total_mdrs", ascending=True),
        x="total_mdrs",
        y="manufacturer_clean",
        orientation="h",
        color="manufacturer_clean",
        color_discrete_map=MANUFACTURER_COLORS,
        labels={
            "total_mdrs": "Total MDRs",
            "manufacturer_clean": "Manufacturer",
        },
    )

    fig.update_layout(
        showlegend=False,
        margin=dict(l=0, r=0, t=10, b=0),
        height=350,
    )

    st.plotly_chart(fig, use_container_width=True)


def render_event_distribution_chart(df: pd.DataFrame):
    """Render event type distribution grouped bar chart."""
    if df.empty:
        st.info("No event data available.")
        return

    # Pivot the data
    pivot_df = df.pivot(
        index="manufacturer_clean",
        columns="event_type",
        values="count",
    ).fillna(0).reset_index()

    # Melt back for plotly
    melted = pivot_df.melt(
        id_vars=["manufacturer_clean"],
        var_name="event_type",
        value_name="count",
    )

    # Map event types to names
    melted["event_label"] = melted["event_type"].map(EVENT_TYPES)

    color_map = {
        "D": CHART_COLORS["death"],
        "IN": CHART_COLORS["injury"],
        "M": CHART_COLORS["malfunction"],
        "O": CHART_COLORS["other"],
    }

    fig = px.bar(
        melted,
        x="manufacturer_clean",
        y="count",
        color="event_type",
        color_discrete_map=color_map,
        barmode="group",
        labels={
            "manufacturer_clean": "Manufacturer",
            "count": "Count",
            "event_type": "Event Type",
        },
    )

    # Update legend labels
    fig.for_each_trace(lambda t: t.update(name=EVENT_TYPES.get(t.name, t.name)))

    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        margin=dict(l=0, r=0, t=30, b=0),
        height=350,
    )

    st.plotly_chart(fig, use_container_width=True)


def render_trend_comparison(df: pd.DataFrame):
    """Render trend line comparison chart."""
    if df.empty:
        st.info("No trend data available.")
        return

    df = df.copy()
    df["period"] = pd.to_datetime(df["period"].astype(str))

    fig = px.line(
        df,
        x="period",
        y="total_mdrs",
        color="manufacturer_clean",
        color_discrete_map=MANUFACTURER_COLORS,
        labels={
            "period": "Date",
            "total_mdrs": "MDR Count",
            "manufacturer_clean": "Manufacturer",
        },
    )

    fig.update_layout(
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        margin=dict(l=0, r=0, t=30, b=0),
        height=400,
        hovermode="x unified",
    )

    st.plotly_chart(fig, use_container_width=True)

    # Export buttons
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        st.download_button(
            label="📥 Export PNG",
            data=fig.to_image(format="png", scale=2),
            file_name="manufacturer_trends.png",
            mime="image/png",
        )
    with col2:
        st.download_button(
            label="📥 Export HTML",
            data=fig.to_html(include_plotlyjs=True, full_html=True),
            file_name="manufacturer_trends.html",
            mime="text/html",
        )


def render_death_rate_chart(df: pd.DataFrame):
    """Render death rate comparison bar chart."""
    fig = px.bar(
        df.sort_values("death_rate", ascending=True),
        x="death_rate",
        y="manufacturer_clean",
        orientation="h",
        color="manufacturer_clean",
        color_discrete_map=MANUFACTURER_COLORS,
        labels={
            "death_rate": "Death Rate (%)",
            "manufacturer_clean": "Manufacturer",
        },
    )

    fig.update_layout(
        showlegend=False,
        margin=dict(l=0, r=0, t=10, b=0),
        height=350,
    )

    st.plotly_chart(fig, use_container_width=True)


def render_stacked_event_chart(df: pd.DataFrame):
    """Render stacked bar chart of event types."""
    fig = go.Figure()

    for event_type, color in [
        ("deaths", CHART_COLORS["death"]),
        ("injuries", CHART_COLORS["injury"]),
        ("malfunctions", CHART_COLORS["malfunction"]),
    ]:
        fig.add_trace(go.Bar(
            name=event_type.title(),
            x=df["manufacturer_clean"],
            y=df[event_type],
            marker_color=color,
        ))

    fig.update_layout(
        barmode="stack",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        margin=dict(l=0, r=0, t=30, b=0),
        height=350,
        xaxis_title="Manufacturer",
        yaxis_title="Count",
    )

    st.plotly_chart(fig, use_container_width=True)


def render_radar_chart(df: pd.DataFrame):
    """Render radar/spider chart for multi-metric comparison."""
    if len(df) < 2:
        st.info("Need at least 2 manufacturers for radar chart.")
        return

    # Normalize metrics for radar chart (0-100 scale)
    metrics_df = df[["manufacturer_clean", "total_mdrs", "deaths", "injuries", "malfunctions"]].copy()

    # Calculate percentages of total for each metric
    for col in ["total_mdrs", "deaths", "injuries", "malfunctions"]:
        max_val = metrics_df[col].max()
        if max_val > 0:
            metrics_df[f"{col}_norm"] = (metrics_df[col] / max_val) * 100
        else:
            metrics_df[f"{col}_norm"] = 0

    # Create radar chart
    fig = go.Figure()

    categories = ["Total MDRs", "Deaths", "Injuries", "Malfunctions"]

    for _, row in metrics_df.iterrows():
        manufacturer = row["manufacturer_clean"]
        values = [
            row["total_mdrs_norm"],
            row["deaths_norm"],
            row["injuries_norm"],
            row["malfunctions_norm"],
        ]
        # Close the polygon
        values.append(values[0])

        color = MANUFACTURER_COLORS.get(manufacturer, CHART_COLORS["other"])

        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=categories + [categories[0]],
            fill="toself",
            name=manufacturer,
            line_color=color,
            fillcolor=color,
            opacity=0.3,
        ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100],
            ),
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5,
        ),
        margin=dict(l=80, r=80, t=30, b=80),
        height=500,
    )

    st.plotly_chart(fig, use_container_width=True)

    st.caption("Note: Values are normalized to percentage of maximum for each metric.")
