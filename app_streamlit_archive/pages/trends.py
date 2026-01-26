"""Trends page for MAUDE Analyzer.

Config-driven time series visualizations with data quality awareness.
Defaults to all products/manufacturers - no product category is prioritized.
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
from src.analysis import get_trend_data
from src.analysis.cached import (
    cached_trend_data,
    cached_data_quality_summary,
)
from app.components.searchable_select import CachedSearchableSelect
from app.utils.display_helpers import (
    format_number,
    get_coverage_level,
    DataQualityLevel,
)


def render_trends():
    """Render the trends analysis page."""
    st.markdown("Analyze MDR trends over time with interactive charts.")
    st.caption("Leave filters empty to view all data")

    # Check database
    if not config.database.path.exists():
        st.warning("Database not found. Please run initial_load.py first.")
        return

    try:
        with get_connection() as conn:
            render_trend_content(conn)
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        import traceback
        st.code(traceback.format_exc())


def render_trend_content(conn):
    """Render trend analysis content with filters."""
    # Filters section
    st.subheader("Filters")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        aggregation = st.selectbox(
            "Time Aggregation",
            options=["monthly", "quarterly", "yearly", "weekly", "daily"],
            index=0,
            help="Group data by time period",
        )

    with col2:
        # Searchable manufacturer select
        st.markdown("**Manufacturers** (type to search)")
        mfr_select = CachedSearchableSelect(
            conn=conn,
            table="master_events",
            column="manufacturer_clean",
            key="trends_mfr",
            label="Manufacturers",
        )
        selected_manufacturers = mfr_select.render(
            multi=True,
            default=[],
            show_counts=True,
        )

    with col3:
        # Searchable product code select
        st.markdown("**Product Codes** (type to search)")
        pc_select = CachedSearchableSelect(
            conn=conn,
            table="master_events",
            column="product_code",
            key="trends_pc",
            label="Product Codes",
        )
        selected_product_codes = pc_select.render(
            multi=True,
            default=[],
            show_counts=True,
        )

    with col4:
        date_range_option = st.selectbox(
            "Date Range",
            options=["Last 5 Years", "Last 3 Years", "Last Year", "All Time", "Custom"],
            index=0,
        )

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

    # Chart options
    col1, col2, col3 = st.columns(3)

    with col1:
        chart_type = st.selectbox(
            "Chart Type",
            options=["Line Chart", "Area Chart", "Stacked Area", "Bar Chart"],
            index=0,
        )

    with col2:
        metric = st.selectbox(
            "Metric",
            options=["Total MDRs", "Deaths", "Injuries", "Malfunctions"],
            index=0,
        )

    with col3:
        show_rolling_avg = st.checkbox("Show Rolling Average", value=False)
        rolling_window = 3 if show_rolling_avg else None

    st.divider()

    # Data quality warning
    render_data_quality_note()

    # Get trend data
    try:
        # Convert lists to tuples for caching
        mfr_tuple = tuple(selected_manufacturers) if selected_manufacturers else None
        pc_tuple = tuple(selected_product_codes) if selected_product_codes else None

        trend_df = cached_trend_data(
            aggregation=aggregation,
            manufacturers=mfr_tuple,
            product_codes=pc_tuple,
            start_date=start_date,
            end_date=end_date,
        )
    except Exception as e:
        st.error(f"Error loading trend data: {e}")
        return

    if trend_df.empty:
        st.info("No data found for the selected filters.")
        return

    # Map metric to column
    metric_col = {
        "Total MDRs": "total_mdrs",
        "Deaths": "deaths",
        "Injuries": "injuries",
        "Malfunctions": "malfunctions",
    }.get(metric, "total_mdrs")

    # Verify column exists
    if metric_col not in trend_df.columns:
        st.warning(f"Metric column '{metric_col}' not found in data. Showing total count.")
        metric_col = "count" if "count" in trend_df.columns else trend_df.columns[1]

    # Convert period to datetime
    if "period" in trend_df.columns:
        trend_df["period"] = pd.to_datetime(trend_df["period"].astype(str))

    # Main trend chart
    st.subheader(f"{metric} Over Time")

    fig = create_trend_chart(
        trend_df,
        metric_col=metric_col,
        chart_type=chart_type,
        show_rolling_avg=show_rolling_avg,
        rolling_window=rolling_window,
    )

    st.plotly_chart(fig, use_container_width=True)

    # Chart export buttons
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        try:
            st.download_button(
                label="Export PNG",
                data=fig.to_image(format="png", scale=2),
                file_name=f"mdr_trends_{metric_col}.png",
                mime="image/png",
            )
        except Exception:
            st.caption("PNG export requires kaleido")
    with col2:
        st.download_button(
            label="Export HTML",
            data=fig.to_html(include_plotlyjs=True, full_html=True),
            file_name=f"mdr_trends_{metric_col}.html",
            mime="text/html",
        )

    st.divider()

    # Additional analysis charts
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Year-over-Year Comparison")
        render_yoy_chart(trend_df, metric_col)

    with col2:
        st.subheader("Event Type Breakdown")
        render_event_breakdown_chart(trend_df)

    st.divider()

    # Data table
    with st.expander("View Raw Data"):
        render_data_table(trend_df)


def render_data_quality_note():
    """Show data quality context for trend analysis."""
    try:
        quality = cached_data_quality_summary()
        coverage = quality.get("coverage", {})

        # Check key fields
        mfr_coverage = coverage.get("manufacturer_clean", 100)
        pc_coverage = coverage.get("product_code", 100)
        date_coverage = coverage.get("date_received", 100)

        warnings = []
        if mfr_coverage < 80:
            warnings.append(f"manufacturer ({mfr_coverage:.0f}% populated)")
        if pc_coverage < 80:
            warnings.append(f"product code ({pc_coverage:.0f}% populated)")
        if date_coverage < 95:
            warnings.append(f"date received ({date_coverage:.0f}% populated)")

        if warnings:
            st.info(
                f"Note: Some records have missing data for: {', '.join(warnings)}. "
                "Filtered results may not include all relevant records."
            )
    except Exception:
        pass


def create_trend_chart(
    df: pd.DataFrame,
    metric_col: str,
    chart_type: str,
    show_rolling_avg: bool = False,
    rolling_window: int = 3,
) -> go.Figure:
    """Create the main trend chart."""
    # Check if we have manufacturer breakdown
    has_manufacturers = "manufacturer_clean" in df.columns and df["manufacturer_clean"].nunique() > 1

    # Get colors from config
    primary_color = CHART_COLORS.get("primary", "#1f77b4")

    if has_manufacturers:
        # Group by period and manufacturer
        plot_df = df.groupby(["period", "manufacturer_clean"])[metric_col].sum().reset_index()

        # Handle NULL manufacturers
        plot_df["manufacturer_clean"] = plot_df["manufacturer_clean"].fillna("Unknown")

        if chart_type == "Line Chart":
            fig = px.line(
                plot_df,
                x="period",
                y=metric_col,
                color="manufacturer_clean",
                labels={
                    "period": "Date",
                    metric_col: metric_col.replace("_", " ").title(),
                    "manufacturer_clean": "Manufacturer",
                },
            )
        elif chart_type == "Area Chart":
            fig = px.area(
                plot_df,
                x="period",
                y=metric_col,
                color="manufacturer_clean",
                labels={
                    "period": "Date",
                    metric_col: metric_col.replace("_", " ").title(),
                    "manufacturer_clean": "Manufacturer",
                },
            )
        elif chart_type == "Stacked Area":
            fig = px.area(
                plot_df,
                x="period",
                y=metric_col,
                color="manufacturer_clean",
                groupnorm="",
                labels={
                    "period": "Date",
                    metric_col: metric_col.replace("_", " ").title(),
                    "manufacturer_clean": "Manufacturer",
                },
            )
        else:  # Bar Chart
            fig = px.bar(
                plot_df,
                x="period",
                y=metric_col,
                color="manufacturer_clean",
                barmode="stack",
                labels={
                    "period": "Date",
                    metric_col: metric_col.replace("_", " ").title(),
                    "manufacturer_clean": "Manufacturer",
                },
            )
    else:
        # Aggregate totals
        plot_df = df.groupby("period")[metric_col].sum().reset_index()

        if chart_type == "Line Chart":
            fig = px.line(
                plot_df,
                x="period",
                y=metric_col,
                labels={
                    "period": "Date",
                    metric_col: metric_col.replace("_", " ").title(),
                },
            )
            fig.update_traces(line_color=primary_color)
        elif chart_type in ["Area Chart", "Stacked Area"]:
            fig = px.area(
                plot_df,
                x="period",
                y=metric_col,
                labels={
                    "period": "Date",
                    metric_col: metric_col.replace("_", " ").title(),
                },
            )
            fig.update_traces(fillcolor=primary_color, line_color=primary_color)
        else:  # Bar Chart
            fig = px.bar(
                plot_df,
                x="period",
                y=metric_col,
                labels={
                    "period": "Date",
                    metric_col: metric_col.replace("_", " ").title(),
                },
            )
            fig.update_traces(marker_color=primary_color)

        # Add rolling average
        if show_rolling_avg and rolling_window:
            plot_df["rolling_avg"] = plot_df[metric_col].rolling(window=rolling_window, min_periods=1).mean()
            fig.add_trace(
                go.Scatter(
                    x=plot_df["period"],
                    y=plot_df["rolling_avg"],
                    mode="lines",
                    name=f"{rolling_window}-period Rolling Avg",
                    line=dict(color="red", dash="dash", width=2),
                )
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
        height=450,
        hovermode="x unified",
    )

    return fig


def render_yoy_chart(df: pd.DataFrame, metric_col: str):
    """Render year-over-year comparison chart."""
    if "period" not in df.columns:
        st.info("Period column not available for YoY analysis.")
        return

    df = df.copy()
    df["year"] = df["period"].dt.year
    df["month"] = df["period"].dt.month

    # Group by year and month
    yoy_df = df.groupby(["year", "month"])[metric_col].sum().reset_index()

    if yoy_df.empty or yoy_df["year"].nunique() < 2:
        st.info("Need at least 2 years of data for YoY comparison.")
        return

    # Create chart
    fig = px.line(
        yoy_df,
        x="month",
        y=metric_col,
        color="year",
        labels={
            "month": "Month",
            metric_col: metric_col.replace("_", " ").title(),
            "year": "Year",
        },
    )

    fig.update_layout(
        xaxis=dict(
            tickmode="array",
            tickvals=list(range(1, 13)),
            ticktext=["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        margin=dict(l=0, r=0, t=10, b=0),
        height=350,
    )

    st.plotly_chart(fig, use_container_width=True)


def render_event_breakdown_chart(df: pd.DataFrame):
    """Render event type breakdown over time using config-driven colors."""
    # Check which columns exist
    available_cols = []
    for col in ["deaths", "injuries", "malfunctions"]:
        if col in df.columns:
            available_cols.append(col)

    if not available_cols:
        st.info("Event type breakdown not available.")
        return

    # Aggregate by period
    agg_dict = {col: "sum" for col in available_cols}
    breakdown_df = df.groupby("period").agg(agg_dict).reset_index()

    # Melt for stacked chart
    melted = breakdown_df.melt(
        id_vars=["period"],
        value_vars=available_cols,
        var_name="event_type",
        value_name="count",
    )

    # Map to display names using config
    display_name_map = {
        "deaths": get_event_type_name("D"),
        "injuries": get_event_type_name("IN"),
        "malfunctions": get_event_type_name("M"),
    }
    melted["event_label"] = melted["event_type"].map(display_name_map)

    # Get colors from config
    color_map = {
        display_name_map["deaths"]: get_event_type_color("D"),
        display_name_map["injuries"]: get_event_type_color("IN"),
        display_name_map["malfunctions"]: get_event_type_color("M"),
    }

    fig = px.area(
        melted,
        x="period",
        y="count",
        color="event_label",
        color_discrete_map=color_map,
        labels={
            "period": "Date",
            "count": "Count",
            "event_label": "Event Type",
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
        margin=dict(l=0, r=0, t=10, b=0),
        height=350,
    )

    st.plotly_chart(fig, use_container_width=True)


def render_data_table(df: pd.DataFrame):
    """Render the raw data table."""
    # Check which columns exist for aggregation
    agg_cols = {}
    col_mapping = {
        "total_mdrs": "Total MDRs",
        "deaths": "Deaths",
        "injuries": "Injuries",
        "malfunctions": "Malfunctions",
        "count": "Count",
    }

    for col, display in col_mapping.items():
        if col in df.columns:
            agg_cols[col] = "sum"

    if not agg_cols:
        st.info("No numeric columns available for summary.")
        return

    # Aggregate for display
    display_df = df.groupby("period").agg(agg_cols).reset_index()

    # Rename columns
    rename_map = {"period": "Period"}
    rename_map.update({k: v for k, v in col_mapping.items() if k in agg_cols})
    display_df = display_df.rename(columns=rename_map)

    # Format period
    display_df["Period"] = display_df["Period"].dt.strftime("%Y-%m-%d")

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Export option
    csv = display_df.to_csv(index=False)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name="trend_data.csv",
        mime="text/csv",
    )
