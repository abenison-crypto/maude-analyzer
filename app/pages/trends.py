"""Trends page for MAUDE Analyzer - Interactive time series visualizations."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config, EVENT_TYPES, CHART_COLORS, MANUFACTURER_COLORS
from src.database import get_connection
from src.analysis import get_trend_data, get_filter_options


def render_trends():
    """Render the trends analysis page."""
    st.markdown("Analyze MDR trends over time with interactive charts.")

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

    # Filters in sidebar-style columns
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
        selected_manufacturers = st.multiselect(
            "Manufacturers",
            options=filter_options.get("manufacturers", []),
            help="Select manufacturers to compare (leave empty for all)",
        )

    with col3:
        selected_product_codes = st.multiselect(
            "Product Codes",
            options=filter_options.get("product_codes", []),
            help="Filter by product codes (leave empty for all)",
        )

    with col4:
        date_range = st.date_input(
            "Date Range",
            value=(date.today() - timedelta(days=365 * 5), date.today()),
            help="Filter by date received",
        )

    # Parse date range
    start_date = date_range[0] if len(date_range) > 0 else None
    end_date = date_range[1] if len(date_range) > 1 else None

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

    # Get trend data
    try:
        with get_connection() as conn:
            trend_df = get_trend_data(
                aggregation=aggregation,
                manufacturers=selected_manufacturers if selected_manufacturers else None,
                product_codes=selected_product_codes if selected_product_codes else None,
                start_date=start_date,
                end_date=end_date,
                conn=conn,
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

    # Convert period to datetime
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

    # Chart export
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        # PNG export
        st.download_button(
            label="📥 Export PNG",
            data=fig.to_image(format="png", scale=2),
            file_name=f"mdr_trends_{metric_col}.png",
            mime="image/png",
        )
    with col2:
        # HTML export
        st.download_button(
            label="📥 Export HTML",
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
        # Aggregate for display
        display_df = trend_df.groupby("period").agg({
            "total_mdrs": "sum",
            "deaths": "sum",
            "injuries": "sum",
            "malfunctions": "sum",
        }).reset_index()

        display_df.columns = ["Period", "Total MDRs", "Deaths", "Injuries", "Malfunctions"]
        display_df["Period"] = display_df["Period"].dt.strftime("%Y-%m-%d")

        st.dataframe(display_df, use_container_width=True, hide_index=True)


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

    if has_manufacturers:
        # Group by period and manufacturer
        plot_df = df.groupby(["period", "manufacturer_clean"])[metric_col].sum().reset_index()

        if chart_type == "Line Chart":
            fig = px.line(
                plot_df,
                x="period",
                y=metric_col,
                color="manufacturer_clean",
                color_discrete_map=MANUFACTURER_COLORS,
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
                color_discrete_map=MANUFACTURER_COLORS,
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
                color_discrete_map=MANUFACTURER_COLORS,
                groupnorm="",  # Stack without normalization
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
                color_discrete_map=MANUFACTURER_COLORS,
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
            fig.update_traces(line_color=CHART_COLORS["primary"])
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
            fig.update_traces(fillcolor=CHART_COLORS["primary"], line_color=CHART_COLORS["primary"])
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
            fig.update_traces(marker_color=CHART_COLORS["primary"])

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
    # Add year column
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
    """Render event type breakdown over time."""
    # Aggregate by period
    breakdown_df = df.groupby("period").agg({
        "deaths": "sum",
        "injuries": "sum",
        "malfunctions": "sum",
    }).reset_index()

    # Melt for stacked chart
    melted = breakdown_df.melt(
        id_vars=["period"],
        value_vars=["deaths", "injuries", "malfunctions"],
        var_name="event_type",
        value_name="count",
    )

    # Map colors
    color_map = {
        "deaths": CHART_COLORS["death"],
        "injuries": CHART_COLORS["injury"],
        "malfunctions": CHART_COLORS["malfunction"],
    }

    fig = px.area(
        melted,
        x="period",
        y="count",
        color="event_type",
        color_discrete_map=color_map,
        labels={
            "period": "Date",
            "count": "Count",
            "event_type": "Event Type",
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
