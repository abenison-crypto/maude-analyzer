"""Dashboard page for MAUDE Analyzer.

Dynamic, schema-aware dashboard with data quality indicators and drill-down navigation.
All filters default to "all products/manufacturers" - no product category is prioritized.
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

from config import config, CHART_COLORS, get_event_type_name
from src.database import get_connection
from src.analysis.cached import (
    cached_dashboard_data,
    cached_schema_aware_summary,
    cached_data_quality_summary,
    get_or_compute_dashboard_data,
)
from app.utils.display_helpers import (
    format_number,
    format_date,
    render_coverage_badge,
    get_coverage_level,
    DataQualityLevel,
)
from app.utils.navigation import (
    Pages,
    navigate_to,
    get_navigation_target,
    clear_navigation,
    set_drilldown_context,
    get_drilldown_context,
    render_breadcrumb,
    apply_url_params_to_filters,
)


def render_dashboard():
    """Render the dashboard page."""
    # Check database
    if not config.database.path.exists():
        st.warning("Database not found. Please run initial_load.py first.")
        st.code("python scripts/initial_load.py", language="bash")
        return

    # Apply URL parameters on first load
    if "url_params_applied" not in st.session_state:
        apply_url_params_to_filters()
        st.session_state["url_params_applied"] = True

    try:
        # Use optimized dashboard data (prefers aggregates if available)
        dashboard_data = get_or_compute_dashboard_data()
        summary = dashboard_data["summary"]

        # Show data source indicator
        if dashboard_data.get("_source") == "daily_aggregates":
            st.caption("Using pre-computed aggregates for fast loading")

        # KPI Cards with drill-down capability
        render_kpi_cards(summary)

        st.divider()

        # Data quality indicator (collapsible)
        render_data_quality_section()

        st.divider()

        # Main charts row
        col1, col2 = st.columns(2)

        with col1:
            render_trend_chart(dashboard_data)

        with col2:
            render_event_type_chart(dashboard_data)

        st.divider()

        # Manufacturer comparison with click-through
        render_manufacturer_comparison(dashboard_data)

    except Exception as e:
        st.error(f"Error loading dashboard: {e}")
        import traceback
        st.code(traceback.format_exc())


def render_kpi_cards(summary: dict):
    """Render KPI metric cards with drill-down navigation."""
    total = summary.get("total_mdrs", 0)
    deaths = summary.get("deaths", 0)
    injuries = summary.get("injuries", 0)
    malfunctions = summary.get("malfunctions", 0)

    # Primary metrics row with drill-down links
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        _render_clickable_kpi(
            label="Total MDRs",
            value=format_number(total),
            help_text="Total Medical Device Reports in database",
            click_key="kpi_total",
            nav_target=Pages.SEARCH,
            nav_params={},  # All records
        )

    with col2:
        death_pct = (deaths / total * 100) if total > 0 else 0
        _render_clickable_kpi(
            label="Deaths",
            value=format_number(deaths),
            delta=f"{death_pct:.1f}%",
            delta_color="inverse",
            help_text="Reports with Death event type",
            click_key="kpi_deaths",
            nav_target=Pages.SEARCH,
            nav_params={"event_types": ["D"]},
        )

    with col3:
        injury_pct = (injuries / total * 100) if total > 0 else 0
        _render_clickable_kpi(
            label="Injuries",
            value=format_number(injuries),
            delta=f"{injury_pct:.1f}%",
            delta_color="inverse",
            help_text="Reports with Injury event type",
            click_key="kpi_injuries",
            nav_target=Pages.SEARCH,
            nav_params={"event_types": ["IN"]},
        )

    with col4:
        malfunction_pct = (malfunctions / total * 100) if total > 0 else 0
        _render_clickable_kpi(
            label="Malfunctions",
            value=format_number(malfunctions),
            delta=f"{malfunction_pct:.1f}%",
            delta_color="off",
            help_text="Reports with Malfunction event type",
            click_key="kpi_malfunctions",
            nav_target=Pages.SEARCH,
            nav_params={"event_types": ["M"]},
        )

    # Secondary stats row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        mfr_count = summary.get("unique_manufacturers", 0)
        _render_clickable_kpi(
            label="Manufacturers",
            value=format_number(mfr_count) if mfr_count else "N/A",
            help_text="Unique manufacturer names - click to compare",
            click_key="kpi_manufacturers",
            nav_target=Pages.COMPARISON,
            nav_params={},
        )

    with col2:
        pc_count = summary.get("unique_product_codes", 0)
        _render_clickable_kpi(
            label="Product Codes",
            value=format_number(pc_count) if pc_count else "N/A",
            help_text="Unique FDA product codes - click to browse",
            click_key="kpi_products",
            nav_target=Pages.PRODUCT,
            nav_params={},
        )

    with col3:
        earliest = summary.get("earliest_date")
        st.metric(
            "Data From",
            format_date(earliest),
            help="Earliest report date",
        )

    with col4:
        latest = summary.get("latest_date")
        st.metric(
            "Data To",
            format_date(latest),
            help="Latest report date",
        )


def _render_clickable_kpi(
    label: str,
    value: str,
    help_text: str,
    click_key: str,
    nav_target: str,
    nav_params: dict,
    delta: str = None,
    delta_color: str = "normal",
):
    """Render a KPI metric with a drill-down button."""
    # Container for metric + button
    metric_col, btn_col = st.columns([5, 1])

    with metric_col:
        if delta:
            st.metric(
                label=label,
                value=value,
                delta=delta,
                delta_color=delta_color,
                help=help_text,
            )
        else:
            st.metric(
                label=label,
                value=value,
                help=help_text,
            )

    with btn_col:
        # Drill-down button
        if st.button("→", key=click_key, help=f"View {label} details"):
            # Set drill-down context for breadcrumb navigation
            set_drilldown_context(
                source_page=Pages.DASHBOARD,
                context_type="kpi",
                context_value=label,
            )
            # Navigate with parameters
            navigate_to(nav_target, **nav_params)
            st.rerun()


def render_data_quality_section():
    """Render collapsible data quality information."""
    with st.expander("Data Quality Summary", expanded=False):
        try:
            quality = cached_data_quality_summary()

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Key Field Coverage**")

                coverage = quality.get("coverage", {})
                for field, pct in coverage.items():
                    level = get_coverage_level(pct)
                    color = {
                        DataQualityLevel.EXCELLENT: "green",
                        DataQualityLevel.GOOD: "blue",
                        DataQualityLevel.MODERATE: "orange",
                        DataQualityLevel.POOR: "red",
                        DataQualityLevel.MISSING: "gray",
                    }.get(level, "gray")

                    # Format field name nicely
                    display_name = field.replace("_", " ").title()
                    st.markdown(
                        f"- {display_name}: "
                        f"<span style='color:{color}'>{pct:.1f}%</span>",
                        unsafe_allow_html=True
                    )

            with col2:
                st.markdown("**Database Stats**")
                st.write(f"Total rows: {format_number(quality.get('total_rows', 0))}")
                st.write(f"Total columns: {quality.get('total_columns', 0)}")

                sparse_cols = quality.get("sparse_columns", [])
                if sparse_cols:
                    st.markdown("**Sparse columns (<50% populated):**")
                    st.caption(", ".join(sparse_cols[:10]))
                    if len(sparse_cols) > 10:
                        st.caption(f"...and {len(sparse_cols) - 10} more")

        except Exception as e:
            st.warning(f"Could not load data quality info: {e}")


def render_trend_chart(dashboard_data: dict):
    """Render MDR trend over time chart with drill-down on click."""
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
            fillcolor="rgba(31, 119, 180, 0.2)",
            customdata=trend_df["period"].dt.strftime("%Y-%m"),
            hovertemplate="<b>%{x|%b %Y}</b><br>Reports: %{y:,.0f}<extra></extra>",
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
            hovermode="x unified",
        )

        st.plotly_chart(fig, use_container_width=True, key="trend_chart")

        # Add drill-down hint
        st.caption("View detailed trends on the Trends page")

        # Quick link to trends page
        if st.button("Open Trends Analysis →", key="link_to_trends", type="secondary"):
            set_drilldown_context(
                source_page=Pages.DASHBOARD,
                context_type="chart",
                context_value="trends",
            )
            navigate_to(Pages.TRENDS)
            st.rerun()

    except Exception as e:
        st.error(f"Error loading trend data: {e}")


def render_event_type_chart(dashboard_data: dict):
    """Render event type distribution chart with drill-down capability."""
    st.subheader("Event Type Distribution")

    try:
        event_df = dashboard_data.get("event_counts", pd.DataFrame())

        if event_df.empty:
            st.info("No event type data available.")
            return

        # Map event types to labels using config
        event_df = event_df.copy()
        event_df["label"] = event_df["event_type"].apply(get_event_type_name)

        # Handle NULL/Unknown event types
        event_df["label"] = event_df["label"].fillna("Unknown")

        # Store event type codes for drill-down
        event_df["code"] = event_df["event_type"]

        # Color mapping
        color_map = {
            "Death": CHART_COLORS.get("death", "#d62728"),
            "Injury": CHART_COLORS.get("injury", "#ff7f0e"),
            "Malfunction": CHART_COLORS.get("malfunction", "#1f77b4"),
            "Other": CHART_COLORS.get("other", "#7f7f7f"),
            "Unknown": "#bcbd22",
            "No Answer Provided": "#bcbd22",
        }

        fig = px.pie(
            event_df,
            values="count",
            names="label",
            color="label",
            color_discrete_map=color_map,
            hole=0.4,
            custom_data=["code"],
        )

        fig.update_traces(
            textposition="inside",
            textinfo="percent+label",
            hovertemplate="<b>%{label}</b><br>Count: %{value:,.0f}<br>Percentage: %{percent}<extra></extra>",
        )

        fig.update_layout(
            showlegend=False,
            margin=dict(l=0, r=0, t=30, b=0),
            height=400,
        )

        st.plotly_chart(fig, use_container_width=True, key="event_chart")

        # Event type drill-down buttons
        st.caption("Click to view reports by event type:")
        btn_cols = st.columns(len(event_df))
        for i, (_, row) in enumerate(event_df.iterrows()):
            with btn_cols[i]:
                if st.button(
                    row["label"],
                    key=f"event_drilldown_{row['code']}",
                    type="secondary",
                    use_container_width=True,
                ):
                    set_drilldown_context(
                        source_page=Pages.DASHBOARD,
                        context_type="event_type",
                        context_value=row["label"],
                    )
                    navigate_to(Pages.SEARCH, event_types=[row["code"]])
                    st.rerun()

    except Exception as e:
        st.error(f"Error loading event type data: {e}")


def render_manufacturer_comparison(dashboard_data: dict):
    """Render manufacturer comparison chart with click-through navigation."""
    st.subheader("Top Manufacturers by MDR Count")

    try:
        comparison_df = dashboard_data.get("top_manufacturers", pd.DataFrame())

        if comparison_df.empty:
            st.info("No manufacturer data available.")
            return

        # Handle potential NULL manufacturers
        comparison_df = comparison_df.copy()

        # Detect column name (might be manufacturer_clean or manufacturer_name)
        mfr_col = None
        for col in ["manufacturer_clean", "manufacturer_name"]:
            if col in comparison_df.columns:
                mfr_col = col
                break

        if mfr_col is None:
            st.warning("Manufacturer column not found in data")
            return

        # Replace NULL with "Unknown"
        comparison_df[mfr_col] = comparison_df[mfr_col].fillna("Unknown")

        # Create horizontal bar chart
        fig = px.bar(
            comparison_df.sort_values("count", ascending=True),
            x="count",
            y=mfr_col,
            orientation="h",
            color="count",
            color_continuous_scale="Blues",
            labels={
                "count": "Total MDRs",
                mfr_col: "Manufacturer",
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

        fig.update_traces(
            hovertemplate="<b>%{y}</b><br>Reports: %{x:,.0f}<extra></extra>",
        )

        st.plotly_chart(fig, use_container_width=True, key="mfr_chart")

        # Drill-down section
        st.caption("Select a manufacturer to view detailed analysis:")

        # Show top manufacturers as clickable buttons
        mfr_list = comparison_df[mfr_col].tolist()
        num_buttons_per_row = 3
        rows = [mfr_list[i:i + num_buttons_per_row] for i in range(0, len(mfr_list), num_buttons_per_row)]

        for row in rows:
            cols = st.columns(len(row))
            for i, mfr in enumerate(row):
                with cols[i]:
                    if st.button(
                        mfr[:30] + "..." if len(mfr) > 30 else mfr,
                        key=f"mfr_drilldown_{mfr}",
                        type="secondary",
                        use_container_width=True,
                        help=f"View reports for {mfr}",
                    ):
                        set_drilldown_context(
                            source_page=Pages.DASHBOARD,
                            context_type="manufacturer",
                            context_value=mfr,
                        )
                        navigate_to(Pages.SEARCH, manufacturers=[mfr])
                        st.rerun()

        # Link to comparison page
        st.divider()
        if st.button("Compare Multiple Manufacturers →", key="link_to_comparison", type="primary"):
            set_drilldown_context(
                source_page=Pages.DASHBOARD,
                context_type="chart",
                context_value="manufacturer_comparison",
            )
            navigate_to(Pages.COMPARISON)
            st.rerun()

    except Exception as e:
        st.error(f"Error loading manufacturer comparison: {e}")


def render_quick_filters():
    """Render quick filter presets in sidebar for drilling down."""
    st.sidebar.markdown("### Quick Filters")
    st.sidebar.caption("Click to view filtered reports")

    # Define presets with navigation parameters
    presets = [
        ("All Reports", {}, "View all MDRs"),
        ("Deaths Only", {"event_types": ["D"]}, "View death reports"),
        ("Injuries Only", {"event_types": ["IN"]}, "View injury reports"),
        ("High Severity", {"event_types": ["D", "IN"]}, "View deaths and injuries"),
    ]

    for name, nav_params, help_text in presets:
        if st.sidebar.button(
            name,
            key=f"preset_{name}",
            use_container_width=True,
            help=help_text,
        ):
            set_drilldown_context(
                source_page=Pages.DASHBOARD,
                context_type="preset",
                context_value=name,
            )
            navigate_to(Pages.SEARCH, **nav_params)
            st.rerun()

    st.sidebar.divider()

    # Recent period filters
    st.sidebar.markdown("### Time Periods")
    today = date.today()

    period_presets = [
        ("Last 30 Days", today - timedelta(days=30), today),
        ("Last 90 Days", today - timedelta(days=90), today),
        ("Last Year", today - timedelta(days=365), today),
        ("Year to Date", date(today.year, 1, 1), today),
    ]

    for name, start, end in period_presets:
        if st.sidebar.button(
            name,
            key=f"period_{name}",
            use_container_width=True,
        ):
            set_drilldown_context(
                source_page=Pages.DASHBOARD,
                context_type="time_period",
                context_value=name,
            )
            navigate_to(Pages.SEARCH, start_date=start, end_date=end)
            st.rerun()
