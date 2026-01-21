"""Product Analysis page for MAUDE Analyzer."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config, EVENT_TYPES, CHART_COLORS, MANUFACTURER_COLORS, PRODUCT_CODE_DESCRIPTIONS
from src.database import get_connection
from src.analysis import get_filter_options


def render_product_analysis():
    """Render the product analysis page."""
    st.markdown("Analyze MDR patterns by product code and device category.")

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

    # Product code selection
    st.subheader("Select Product Code")

    col1, col2 = st.columns([1, 2])

    with col1:
        selected_product_code = st.selectbox(
            "Product Code",
            options=filter_options.get("product_codes", []),
            format_func=lambda x: f"{x} - {PRODUCT_CODE_DESCRIPTIONS.get(x, 'Unknown')[:40]}...",
        )

    with col2:
        date_range = st.date_input(
            "Date Range",
            value=(date.today() - timedelta(days=365 * 5), date.today()),
        )

    start_date = date_range[0] if len(date_range) > 0 else None
    end_date = date_range[1] if len(date_range) > 1 else None

    if not selected_product_code:
        st.info("Please select a product code.")
        return

    st.divider()

    # Get product-specific data
    try:
        with get_connection() as conn:
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
    st.markdown(f"**{PRODUCT_CODE_DESCRIPTIONS.get(selected_product_code, 'Unknown Device')}**")

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
    st.subheader("Event Flow: Manufacturer → Event Type")
    render_sankey_diagram(product_data["by_manufacturer"], product_data["event_breakdown"])

    st.divider()

    # Top brands treemap
    if product_data["top_brands"]:
        st.subheader("Top Brands (Treemap)")
        render_brand_treemap(product_data["top_brands"])


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

    # Top brands (from devices table)
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
        st.metric("Total MDRs", f"{summary['total']:,}")

    with col2:
        death_pct = (summary['deaths'] / summary['total'] * 100) if summary['total'] > 0 else 0
        st.metric("Deaths", f"{summary['deaths']:,}", delta=f"{death_pct:.1f}%", delta_color="inverse")

    with col3:
        injury_pct = (summary['injuries'] / summary['total'] * 100) if summary['total'] > 0 else 0
        st.metric("Injuries", f"{summary['injuries']:,}", delta=f"{injury_pct:.1f}%", delta_color="inverse")

    with col4:
        st.metric("Manufacturers", summary['manufacturers'])


def render_manufacturer_breakdown(df: pd.DataFrame):
    """Render manufacturer breakdown bar chart."""
    if df.empty:
        st.info("No manufacturer data available.")
        return

    fig = px.bar(
        df.head(10).sort_values("total", ascending=True),
        x="total",
        y="manufacturer_clean",
        orientation="h",
        color="manufacturer_clean",
        color_discrete_map=MANUFACTURER_COLORS,
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
    """Render event type pie chart."""
    data = pd.DataFrame([
        {"event": "Deaths", "count": summary["deaths"], "color": CHART_COLORS["death"]},
        {"event": "Injuries", "count": summary["injuries"], "color": CHART_COLORS["injury"]},
        {"event": "Malfunctions", "count": summary["malfunctions"], "color": CHART_COLORS["malfunction"]},
    ])

    fig = px.pie(
        data,
        values="count",
        names="event",
        color="event",
        color_discrete_map={
            "Deaths": CHART_COLORS["death"],
            "Injuries": CHART_COLORS["injury"],
            "Malfunctions": CHART_COLORS["malfunction"],
        },
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

    # Check if multiple manufacturers
    if df["manufacturer_clean"].nunique() > 1:
        fig = px.line(
            df,
            x="period",
            y="count",
            color="manufacturer_clean",
            color_discrete_map=MANUFACTURER_COLORS,
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
        fig.update_traces(line_color=CHART_COLORS["primary"])

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

    # Get top manufacturers
    top_manufacturers = manufacturer_df.head(6)["manufacturer_clean"].tolist()

    # Filter event data
    event_filtered = event_df[event_df["manufacturer_clean"].isin(top_manufacturers)]

    if event_filtered.empty:
        st.info("No event data for selected manufacturers.")
        return

    # Build Sankey data
    manufacturers = event_filtered["manufacturer_clean"].unique().tolist()
    event_types = event_filtered["event_type"].unique().tolist()

    # Create node labels
    labels = manufacturers + [EVENT_TYPES.get(e, e) for e in event_types]

    # Create source, target, value lists
    source = []
    target = []
    value = []
    colors = []

    event_color_map = {
        "D": CHART_COLORS["death"],
        "IN": CHART_COLORS["injury"],
        "M": CHART_COLORS["malfunction"],
        "O": CHART_COLORS["other"],
    }

    for _, row in event_filtered.iterrows():
        mfr = row["manufacturer_clean"]
        evt = row["event_type"]
        cnt = row["count"]

        if mfr in manufacturers and evt in event_types:
            source.append(manufacturers.index(mfr))
            target.append(len(manufacturers) + event_types.index(evt))
            value.append(cnt)
            colors.append(event_color_map.get(evt, CHART_COLORS["other"]))

    # Create Sankey diagram
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=labels,
            color=[MANUFACTURER_COLORS.get(m, "#888") for m in manufacturers] +
                  [event_color_map.get(e, CHART_COLORS["other"]) for e in event_types],
        ),
        link=dict(
            source=source,
            target=target,
            value=value,
            color=[c.replace(")", ", 0.4)").replace("rgb", "rgba") if c.startswith("rgb") else c + "66" for c in colors],
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

    # Add manufacturer if available
    if "manufacturer_d_clean" in df.columns:
        df = df.copy()
        df["label"] = df["brand_name"] + " (" + df["manufacturer_d_clean"].fillna("Unknown") + ")"
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
