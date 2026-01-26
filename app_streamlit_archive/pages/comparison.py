"""Manufacturer Comparison page for MAUDE Analyzer.

Compare adverse event profiles between manufacturers with searchable filters.
No manufacturer is prioritized by default.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config, CHART_COLORS, get_event_type_name, get_event_type_color
from src.database import get_connection
from src.analysis import (
    get_manufacturer_comparison,
    get_trend_data,
    get_event_type_breakdown,
)
from src.analysis.cached import cached_data_quality_summary
from app.components.searchable_select import CachedSearchableSelect
from app.utils.display_helpers import format_nullable


def render_comparison():
    """Render the manufacturer comparison page."""
    st.markdown("Compare adverse event profiles between manufacturers.")
    st.caption("Search and select manufacturers to compare")

    # Check database
    if not config.database.path.exists():
        st.warning("Database not found. Please run initial_load.py first.")
        return

    try:
        with get_connection() as conn:
            render_comparison_content(conn)
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        import traceback
        st.code(traceback.format_exc())


def render_comparison_content(conn):
    """Render comparison content with searchable manufacturer selection."""
    # Manufacturer selection
    st.subheader("Select Manufacturers to Compare")

    # Searchable manufacturer select
    st.markdown("**Manufacturers** (type to search, select 2-6 for comparison)")
    mfr_select = CachedSearchableSelect(
        conn=conn,
        table="master_events",
        column="manufacturer_clean",
        key="comparison_mfr",
        label="Manufacturers",
    )
    selected_manufacturers = mfr_select.render(
        multi=True,
        default=[],
        show_counts=True,
    )

    if len(selected_manufacturers) < 2:
        st.info("Please select at least 2 manufacturers to compare.")
        render_top_manufacturers_hint(conn)
        return

    if len(selected_manufacturers) > 6:
        st.warning("For best visualization, select 6 or fewer manufacturers.")

    # Date range and options
    col1, col2, col3 = st.columns(3)

    with col1:
        date_range_option = st.selectbox(
            "Date Range",
            options=["Last 5 Years", "Last 3 Years", "Last Year", "All Time", "Custom"],
            index=0,
        )

    with col2:
        aggregation = st.selectbox(
            "Trend Aggregation",
            options=["monthly", "quarterly", "yearly"],
            index=0,
        )

    with col3:
        if date_range_option == "Custom":
            pass  # Will show date inputs below

    # Handle date range
    if date_range_option == "Custom":
        date_col1, date_col2 = st.columns(2)
        with date_col1:
            start_date = st.date_input("Start Date", value=date.today() - timedelta(days=365*5))
        with date_col2:
            end_date = st.date_input("End Date", value=date.today())
    elif date_range_option == "All Time":
        start_date = None
        end_date = None
    elif date_range_option == "Last Year":
        start_date = date.today() - timedelta(days=365)
        end_date = date.today()
    elif date_range_option == "Last 3 Years":
        start_date = date.today() - timedelta(days=365*3)
        end_date = date.today()
    else:  # Last 5 Years
        start_date = date.today() - timedelta(days=365*5)
        end_date = date.today()

    st.divider()

    # Data quality note
    render_data_quality_note()

    # Get comparison data
    try:
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


def render_top_manufacturers_hint(conn):
    """Show hint about top manufacturers when none selected."""
    try:
        top_mfrs = conn.execute("""
            SELECT manufacturer_clean, COUNT(*) as count
            FROM master_events
            WHERE manufacturer_clean IS NOT NULL
            GROUP BY manufacturer_clean
            ORDER BY count DESC
            LIMIT 10
        """).fetchdf()

        if not top_mfrs.empty:
            with st.expander("Top Manufacturers by MDR Count"):
                st.caption("These are the manufacturers with the most reports. Select from above to compare.")
                st.dataframe(
                    top_mfrs.rename(columns={
                        "manufacturer_clean": "Manufacturer",
                        "count": "MDR Count"
                    }),
                    use_container_width=True,
                    hide_index=True,
                )
    except Exception:
        pass


def render_data_quality_note():
    """Show data quality context for comparison."""
    try:
        quality = cached_data_quality_summary()
        coverage = quality.get("coverage", {})
        mfr_coverage = coverage.get("manufacturer_clean", 100)

        if mfr_coverage < 80:
            st.info(
                f"Note: Manufacturer data is {mfr_coverage:.0f}% populated. "
                "Some reports may not be included in comparison."
            )
    except Exception:
        pass


def render_summary_table(df: pd.DataFrame):
    """Render summary comparison table."""
    # Check which columns exist
    available_cols = ["manufacturer_clean"]
    col_mapping = {
        "manufacturer_clean": "Manufacturer",
        "total_mdrs": "Total MDRs",
        "deaths": "Deaths",
        "injuries": "Injuries",
        "malfunctions": "Malfunctions",
        "death_rate": "Death Rate (%)",
    }

    for col in ["total_mdrs", "deaths", "injuries", "malfunctions", "death_rate"]:
        if col in df.columns:
            available_cols.append(col)

    display_df = df[available_cols].copy()

    # Handle NULL manufacturers
    if "manufacturer_clean" in display_df.columns:
        display_df["manufacturer_clean"] = display_df["manufacturer_clean"].fillna("Unknown")

    # Rename columns
    display_df.columns = [col_mapping.get(c, c) for c in display_df.columns]

    # Sort by total MDRs if available
    if "Total MDRs" in display_df.columns:
        display_df = display_df.sort_values("Total MDRs", ascending=False)

    # Build column config dynamically
    column_config = {}
    if "Total MDRs" in display_df.columns:
        column_config["Total MDRs"] = st.column_config.NumberColumn(format="%d")
    if "Deaths" in display_df.columns:
        column_config["Deaths"] = st.column_config.NumberColumn(format="%d")
    if "Injuries" in display_df.columns:
        column_config["Injuries"] = st.column_config.NumberColumn(format="%d")
    if "Malfunctions" in display_df.columns:
        column_config["Malfunctions"] = st.column_config.NumberColumn(format="%d")
    if "Death Rate (%)" in display_df.columns:
        column_config["Death Rate (%)"] = st.column_config.NumberColumn(format="%.2f%%")

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config=column_config,
    )


def render_total_mdrs_chart(df: pd.DataFrame):
    """Render total MDRs bar chart."""
    if "total_mdrs" not in df.columns:
        st.info("MDR count data not available.")
        return

    plot_df = df.copy()
    plot_df["manufacturer_clean"] = plot_df["manufacturer_clean"].fillna("Unknown")

    fig = px.bar(
        plot_df.sort_values("total_mdrs", ascending=True),
        x="total_mdrs",
        y="manufacturer_clean",
        orientation="h",
        color="manufacturer_clean",
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
    """Render event type distribution grouped bar chart using config-driven colors."""
    if df.empty:
        st.info("No event data available.")
        return

    # Handle NULL manufacturer names
    df = df.copy()
    if "manufacturer_clean" in df.columns:
        df["manufacturer_clean"] = df["manufacturer_clean"].fillna("Unknown")

    # Check if we have the required columns
    if "event_type" not in df.columns or "count" not in df.columns:
        st.info("Event type distribution not available.")
        return

    # Pivot the data
    try:
        pivot_df = df.pivot(
            index="manufacturer_clean",
            columns="event_type",
            values="count",
        ).fillna(0).reset_index()
    except Exception:
        st.info("Could not create event distribution chart.")
        return

    # Melt back for plotly
    event_cols = [c for c in pivot_df.columns if c != "manufacturer_clean"]
    melted = pivot_df.melt(
        id_vars=["manufacturer_clean"],
        value_vars=event_cols,
        var_name="event_type",
        value_name="count",
    )

    # Map event types to display names using config
    melted["event_label"] = melted["event_type"].apply(get_event_type_name)

    # Get colors from config
    color_map = {}
    for event_code in melted["event_type"].unique():
        color_map[event_code] = get_event_type_color(event_code)

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

    # Update legend labels to use config names
    fig.for_each_trace(lambda t: t.update(name=get_event_type_name(t.name)))

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

    # Check for required columns
    if "period" not in df.columns:
        st.info("Period data not available for trend analysis.")
        return

    df["period"] = pd.to_datetime(df["period"].astype(str))

    # Handle NULL manufacturers
    if "manufacturer_clean" in df.columns:
        df["manufacturer_clean"] = df["manufacturer_clean"].fillna("Unknown")

    # Determine y-axis column
    y_col = "total_mdrs" if "total_mdrs" in df.columns else "count" if "count" in df.columns else None
    if y_col is None:
        st.info("No count data available for trend analysis.")
        return

    fig = px.line(
        df,
        x="period",
        y=y_col,
        color="manufacturer_clean",
        labels={
            "period": "Date",
            y_col: "MDR Count",
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
        try:
            st.download_button(
                label="Export PNG",
                data=fig.to_image(format="png", scale=2),
                file_name="manufacturer_trends.png",
                mime="image/png",
            )
        except Exception:
            st.caption("PNG export requires kaleido")
    with col2:
        st.download_button(
            label="Export HTML",
            data=fig.to_html(include_plotlyjs=True, full_html=True),
            file_name="manufacturer_trends.html",
            mime="text/html",
        )


def render_death_rate_chart(df: pd.DataFrame):
    """Render death rate comparison bar chart."""
    if "death_rate" not in df.columns:
        st.info("Death rate data not available.")
        return

    plot_df = df.copy()
    plot_df["manufacturer_clean"] = plot_df["manufacturer_clean"].fillna("Unknown")

    fig = px.bar(
        plot_df.sort_values("death_rate", ascending=True),
        x="death_rate",
        y="manufacturer_clean",
        orientation="h",
        color="manufacturer_clean",
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
    """Render stacked bar chart of event types using config-driven colors."""
    plot_df = df.copy()
    plot_df["manufacturer_clean"] = plot_df["manufacturer_clean"].fillna("Unknown")

    fig = go.Figure()

    # Build traces for available columns
    event_cols = [
        ("deaths", "D"),
        ("injuries", "IN"),
        ("malfunctions", "M"),
    ]

    for col_name, event_code in event_cols:
        if col_name in plot_df.columns:
            fig.add_trace(go.Bar(
                name=get_event_type_name(event_code),
                x=plot_df["manufacturer_clean"],
                y=plot_df[col_name],
                marker_color=get_event_type_color(event_code),
            ))

    if len(fig.data) == 0:
        st.info("No event breakdown data available.")
        return

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

    # Check for required columns
    required_cols = ["manufacturer_clean", "total_mdrs"]
    for col in required_cols:
        if col not in df.columns:
            st.info(f"Missing data for radar chart: {col}")
            return

    # Build metrics based on available columns
    metric_cols = []
    metric_labels = []
    for col, label in [
        ("total_mdrs", "Total MDRs"),
        ("deaths", "Deaths"),
        ("injuries", "Injuries"),
        ("malfunctions", "Malfunctions"),
    ]:
        if col in df.columns:
            metric_cols.append(col)
            metric_labels.append(label)

    if len(metric_cols) < 2:
        st.info("Not enough metrics available for radar chart.")
        return

    # Normalize metrics for radar chart (0-100 scale)
    metrics_df = df[["manufacturer_clean"] + metric_cols].copy()
    metrics_df["manufacturer_clean"] = metrics_df["manufacturer_clean"].fillna("Unknown")

    for col in metric_cols:
        max_val = metrics_df[col].max()
        if max_val > 0:
            metrics_df[f"{col}_norm"] = (metrics_df[col] / max_val) * 100
        else:
            metrics_df[f"{col}_norm"] = 0

    # Create radar chart
    fig = go.Figure()

    # Use plotly's default color sequence
    colors = px.colors.qualitative.Plotly

    for idx, (_, row) in enumerate(metrics_df.iterrows()):
        manufacturer = row["manufacturer_clean"]
        values = [row[f"{col}_norm"] for col in metric_cols]
        # Close the polygon
        values.append(values[0])

        color = colors[idx % len(colors)]

        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=metric_labels + [metric_labels[0]],
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
