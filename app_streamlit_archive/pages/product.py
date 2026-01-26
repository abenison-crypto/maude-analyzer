"""Product Analysis page for MAUDE Analyzer.

Analyze MDR patterns by product code with searchable selection.
Handles sparse product code data (54% populated) with data quality warnings.
No product code is prioritized by default.
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
from src.analysis.cached import cached_data_quality_summary, cached_product_code_lookup
from app.components.searchable_select import CachedSearchableSelect
from app.utils.display_helpers import format_nullable, format_number


def render_product_analysis():
    """Render the product analysis page."""
    st.markdown("Analyze MDR patterns by product code and device category.")
    st.caption("Search for a product code to view detailed analysis")

    # Check database
    if not config.database.path.exists():
        st.warning("Database not found. Please run initial_load.py first.")
        return

    try:
        with get_connection() as conn:
            render_product_content(conn)
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        import traceback
        st.code(traceback.format_exc())


def render_product_content(conn):
    """Render product analysis content with searchable selection."""
    # Data quality warning
    render_data_quality_note()

    # Product code selection
    st.subheader("Select Product Code")

    col1, col2 = st.columns([1, 1])

    with col1:
        # Searchable product code select
        st.markdown("**Product Code** (type to search)")
        pc_select = CachedSearchableSelect(
            conn=conn,
            table="master_events",
            column="product_code",
            key="product_pc",
            label="Product Code",
        )
        selected_product_code = pc_select.render(
            multi=False,
            default=None,
            show_counts=True,
        )

    with col2:
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

    if not selected_product_code:
        st.info("Please search for and select a product code to analyze.")
        render_top_products_hint(conn)
        return

    st.divider()

    # Get product-specific data
    try:
        product_data = get_product_analysis_data(
            conn, selected_product_code, start_date, end_date
        )
    except Exception as e:
        st.error(f"Error loading product data: {e}")
        return

    if product_data["summary"]["total"] == 0:
        st.info(f"No data found for product code {selected_product_code}.")
        return

    # Product header
    st.subheader(f"Product Code: {selected_product_code}")

    # Get product description from lookup
    try:
        lookup = cached_product_code_lookup()
        product_info = lookup.get(selected_product_code, {})
        description = product_info.get("name", "Unknown Device")
        device_class = product_info.get("class", "")
        st.markdown(f"**{description}**")
        if device_class:
            st.caption(f"Device Class: {device_class}")
    except Exception:
        st.markdown("**Device description not available**")

    # KPI cards
    render_product_kpis(product_data["summary"])

    st.divider()

    # Charts row 1
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("MDRs by Manufacturer")
        render_manufacturer_breakdown(product_data["by_manufacturer"])

    with col2:
        st.subheader("Event Type Distribution")
        render_event_pie(product_data["summary"])

    st.divider()

    # Trend chart
    st.subheader("MDR Trends for This Product")
    render_product_trend(product_data["trends"])

    st.divider()

    # Sankey diagram
    st.subheader("Event Flow: Manufacturer to Event Type")
    render_sankey_diagram(product_data["by_manufacturer"], product_data["event_breakdown"])

    st.divider()

    # Top brands treemap
    if product_data["top_brands"] is not None and not product_data["top_brands"].empty:
        st.subheader("Top Brands (Treemap)")
        render_brand_treemap(product_data["top_brands"])


def render_data_quality_note():
    """Show data quality context for product analysis."""
    try:
        quality = cached_data_quality_summary()
        coverage = quality.get("coverage", {})
        pc_coverage = coverage.get("product_code", 100)

        if pc_coverage < 80:
            st.info(
                f"Note: Product code data is {pc_coverage:.0f}% populated. "
                f"Approximately {100-pc_coverage:.0f}% of MDRs do not have a product code assigned."
            )
    except Exception:
        pass


def render_top_products_hint(conn):
    """Show hint about top product codes when none selected."""
    try:
        top_products = conn.execute("""
            SELECT product_code, COUNT(*) as count
            FROM master_events
            WHERE product_code IS NOT NULL
            GROUP BY product_code
            ORDER BY count DESC
            LIMIT 10
        """).fetchdf()

        if not top_products.empty:
            # Try to get descriptions
            try:
                lookup = cached_product_code_lookup()
                top_products["description"] = top_products["product_code"].apply(
                    lambda x: lookup.get(x, {}).get("name", "Unknown")[:50]
                )
            except Exception:
                top_products["description"] = "Unknown"

            with st.expander("Top Product Codes by MDR Count"):
                st.caption("These are the product codes with the most reports. Search above to select one.")
                st.dataframe(
                    top_products.rename(columns={
                        "product_code": "Product Code",
                        "count": "MDR Count",
                        "description": "Description"
                    }),
                    use_container_width=True,
                    hide_index=True,
                )
    except Exception:
        pass


def get_product_analysis_data(conn, product_code: str, start_date, end_date) -> dict:
    """Get all data needed for product analysis."""
    params = [product_code]
    date_filter = ""

    if start_date:
        date_filter += " AND date_received >= ?"
        params.append(start_date)
    if end_date:
        date_filter += " AND date_received <= ?"
        params.append(end_date)

    # Summary stats
    summary = conn.execute(f"""
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE event_type = 'D') as deaths,
            COUNT(*) FILTER (WHERE event_type = 'IN') as injuries,
            COUNT(*) FILTER (WHERE event_type = 'M') as malfunctions,
            COUNT(DISTINCT manufacturer_clean) as manufacturers,
            MIN(date_received) as first_report,
            MAX(date_received) as last_report
        FROM master_events
        WHERE product_code = ?{date_filter}
    """, params).fetchone()

    summary_dict = {
        "total": summary[0] or 0,
        "deaths": summary[1] or 0,
        "injuries": summary[2] or 0,
        "malfunctions": summary[3] or 0,
        "manufacturers": summary[4] or 0,
        "first_report": summary[5],
        "last_report": summary[6],
    }

    # By manufacturer
    by_manufacturer = conn.execute(f"""
        SELECT
            manufacturer_clean,
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE event_type = 'D') as deaths,
            COUNT(*) FILTER (WHERE event_type = 'IN') as injuries,
            COUNT(*) FILTER (WHERE event_type = 'M') as malfunctions
        FROM master_events
        WHERE product_code = ?{date_filter}
        GROUP BY manufacturer_clean
        ORDER BY total DESC
    """, params).fetchdf()

    # Event breakdown by manufacturer
    event_breakdown = conn.execute(f"""
        SELECT
            manufacturer_clean,
            event_type,
            COUNT(*) as count
        FROM master_events
        WHERE product_code = ?{date_filter}
        GROUP BY manufacturer_clean, event_type
        ORDER BY manufacturer_clean, event_type
    """, params).fetchdf()

    # Trends
    trends = conn.execute(f"""
        SELECT
            DATE_TRUNC('month', date_received) as period,
            manufacturer_clean,
            COUNT(*) as count
        FROM master_events
        WHERE product_code = ?{date_filter}
        GROUP BY DATE_TRUNC('month', date_received), manufacturer_clean
        ORDER BY period
    """, params).fetchdf()

    # Top brands (from devices table if available)
    try:
        top_brands = conn.execute(f"""
            SELECT
                d.brand_name,
                d.manufacturer_d_clean,
                COUNT(*) as count
            FROM devices d
            JOIN master_events m ON d.mdr_report_key = m.mdr_report_key
            WHERE m.product_code = ?{date_filter}
                AND d.brand_name IS NOT NULL
            GROUP BY d.brand_name, d.manufacturer_d_clean
            ORDER BY count DESC
            LIMIT 20
        """, params).fetchdf()
    except Exception:
        top_brands = pd.DataFrame()

    return {
        "summary": summary_dict,
        "by_manufacturer": by_manufacturer,
        "event_breakdown": event_breakdown,
        "trends": trends,
        "top_brands": top_brands if not top_brands.empty else None,
    }


def render_product_kpis(summary: dict):
    """Render product KPI cards."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total MDRs", format_number(summary['total']))

    with col2:
        death_pct = (summary['deaths'] / summary['total'] * 100) if summary['total'] > 0 else 0
        st.metric("Deaths", format_number(summary['deaths']), delta=f"{death_pct:.1f}%", delta_color="inverse")

    with col3:
        injury_pct = (summary['injuries'] / summary['total'] * 100) if summary['total'] > 0 else 0
        st.metric("Injuries", format_number(summary['injuries']), delta=f"{injury_pct:.1f}%", delta_color="inverse")

    with col4:
        st.metric("Manufacturers", format_number(summary['manufacturers']))


def render_manufacturer_breakdown(df: pd.DataFrame):
    """Render manufacturer breakdown bar chart."""
    if df.empty:
        st.info("No manufacturer data available.")
        return

    # Handle NULL manufacturers
    df = df.copy()
    df["manufacturer_clean"] = df["manufacturer_clean"].fillna("Unknown")

    fig = px.bar(
        df.head(10).sort_values("total", ascending=True),
        x="total",
        y="manufacturer_clean",
        orientation="h",
        color="manufacturer_clean",
        labels={
            "total": "Total MDRs",
            "manufacturer_clean": "Manufacturer",
        },
    )

    fig.update_layout(
        showlegend=False,
        margin=dict(l=0, r=0, t=10, b=0),
        height=350,
    )

    st.plotly_chart(fig, use_container_width=True)


def render_event_pie(summary: dict):
    """Render event type pie chart using config-driven colors."""
    data = pd.DataFrame([
        {"event": get_event_type_name("D"), "count": summary["deaths"], "color": get_event_type_color("D")},
        {"event": get_event_type_name("IN"), "count": summary["injuries"], "color": get_event_type_color("IN")},
        {"event": get_event_type_name("M"), "count": summary["malfunctions"], "color": get_event_type_color("M")},
    ])

    # Filter out zeros
    data = data[data["count"] > 0]

    if data.empty:
        st.info("No event type data available.")
        return

    color_map = {row["event"]: row["color"] for _, row in data.iterrows()}

    fig = px.pie(
        data,
        values="count",
        names="event",
        color="event",
        color_discrete_map=color_map,
        hole=0.4,
    )

    fig.update_traces(textposition="inside", textinfo="percent+label")

    fig.update_layout(
        showlegend=False,
        margin=dict(l=0, r=0, t=10, b=0),
        height=350,
    )

    st.plotly_chart(fig, use_container_width=True)


def render_product_trend(df: pd.DataFrame):
    """Render product trend chart."""
    if df.empty:
        st.info("No trend data available.")
        return

    df = df.copy()
    df["period"] = pd.to_datetime(df["period"].astype(str))

    # Handle NULL manufacturers
    df["manufacturer_clean"] = df["manufacturer_clean"].fillna("Unknown")

    # Check if multiple manufacturers
    if df["manufacturer_clean"].nunique() > 1:
        fig = px.line(
            df,
            x="period",
            y="count",
            color="manufacturer_clean",
            labels={
                "period": "Date",
                "count": "MDR Count",
                "manufacturer_clean": "Manufacturer",
            },
        )
    else:
        agg_df = df.groupby("period")["count"].sum().reset_index()
        fig = px.line(
            agg_df,
            x="period",
            y="count",
            labels={
                "period": "Date",
                "count": "MDR Count",
            },
        )
        fig.update_traces(line_color=CHART_COLORS.get("primary", "#1f77b4"))

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


def render_sankey_diagram(manufacturer_df: pd.DataFrame, event_df: pd.DataFrame):
    """Render Sankey diagram showing flow from manufacturer to event type."""
    if manufacturer_df.empty or event_df.empty:
        st.info("Insufficient data for Sankey diagram.")
        return

    # Handle NULL manufacturers
    manufacturer_df = manufacturer_df.copy()
    event_df = event_df.copy()
    manufacturer_df["manufacturer_clean"] = manufacturer_df["manufacturer_clean"].fillna("Unknown")
    event_df["manufacturer_clean"] = event_df["manufacturer_clean"].fillna("Unknown")

    # Get top manufacturers
    top_manufacturers = manufacturer_df.head(6)["manufacturer_clean"].tolist()

    # Filter event data
    event_filtered = event_df[event_df["manufacturer_clean"].isin(top_manufacturers)]

    if event_filtered.empty:
        st.info("No event data for selected manufacturers.")
        return

    # Build Sankey data
    manufacturers = event_filtered["manufacturer_clean"].unique().tolist()
    event_types = event_filtered["event_type"].dropna().unique().tolist()

    if not event_types:
        st.info("No event types available.")
        return

    # Create node labels using config-driven names
    labels = manufacturers + [get_event_type_name(e) for e in event_types]

    # Create source, target, value lists
    source = []
    target = []
    value = []
    colors = []

    for _, row in event_filtered.iterrows():
        mfr = row["manufacturer_clean"]
        evt = row["event_type"]
        cnt = row["count"]

        if pd.notna(evt) and mfr in manufacturers and evt in event_types:
            source.append(manufacturers.index(mfr))
            target.append(len(manufacturers) + event_types.index(evt))
            value.append(cnt)
            colors.append(get_event_type_color(evt))

    if not source:
        st.info("No data available for Sankey diagram.")
        return

    # Use plotly's default colors for manufacturers
    mfr_colors = px.colors.qualitative.Plotly[:len(manufacturers)]

    # Create Sankey diagram
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=labels,
            color=list(mfr_colors) + [get_event_type_color(e) for e in event_types],
        ),
        link=dict(
            source=source,
            target=target,
            value=value,
            color=[c + "66" if not c.startswith("rgb") else c.replace(")", ", 0.4)").replace("rgb", "rgba") for c in colors],
        ),
    )])

    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        height=450,
    )

    st.plotly_chart(fig, use_container_width=True)


def render_brand_treemap(df: pd.DataFrame):
    """Render brand treemap."""
    if df.empty:
        st.info("No brand data available.")
        return

    # Handle NULLs
    df = df.copy()
    if "manufacturer_d_clean" in df.columns:
        df["manufacturer_d_clean"] = df["manufacturer_d_clean"].fillna("Unknown")
        df["label"] = df["brand_name"] + " (" + df["manufacturer_d_clean"] + ")"
    else:
        df["label"] = df["brand_name"]

    fig = px.treemap(
        df,
        path=["label"],
        values="count",
        color="count",
        color_continuous_scale="Blues",
    )

    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=0),
        height=400,
    )

    st.plotly_chart(fig, use_container_width=True)
