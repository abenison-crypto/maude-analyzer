"""
MAUDE Database Analyzer - Main Streamlit Application

Run with: streamlit run app/main.py
"""

import streamlit as st
from pathlib import Path
import sys

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from src.database import get_connection, get_table_counts

# Import page modules
from app.pages.dashboard import render_dashboard
from app.pages.search import render_search
from app.pages.trends import render_trends
from app.pages.comparison import render_comparison
from app.pages.product import render_product_analysis
from app.pages.data_management import render_data_management
from app.pages.analytics import render_analytics


def main():
    """Main application entry point."""
    # Page configuration
    st.set_page_config(
        page_title=config.app.name,
        page_icon="🏥",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Sidebar
    with st.sidebar:
        st.title("🏥 MAUDE Analyzer")
        st.caption(f"v{config.app.version}")

        st.divider()

        # Navigation
        st.subheader("Navigation")
        page = st.radio(
            "Go to",
            options=[
                "Dashboard",
                "Search",
                "Trends",
                "Compare Manufacturers",
                "Product Analysis",
                "Analytics",
                "Data Management",
                "Data Explorer",
                "Settings",
            ],
            label_visibility="collapsed",
        )

        st.divider()

        # Database status
        st.subheader("Database Status")
        db_path = config.database.path
        if db_path.exists():
            st.success("Database connected")
            try:
                with get_connection() as conn:
                    counts = get_table_counts(conn)
                    st.caption(f"📊 {counts.get('master_events', 0):,} MDRs")
            except Exception:
                pass
            st.caption(f"Path: {db_path.name}")
        else:
            st.warning("Database not found")
            st.caption("Run initial_load.py to create database")

        st.divider()

        # Quick links
        st.subheader("Quick Links")
        st.markdown(
            """
            - [FDA MAUDE](https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfmaude/search.cfm)
            - [openFDA API](https://open.fda.gov/apis/device/event/)
            """
        )

    # Main content area
    st.title(f"📊 {page}")

    if page == "Dashboard":
        render_dashboard()
    elif page == "Search":
        render_search()
    elif page == "Trends":
        render_trends()
    elif page == "Compare Manufacturers":
        render_comparison()
    elif page == "Product Analysis":
        render_product_analysis()
    elif page == "Analytics":
        render_analytics()
    elif page == "Data Management":
        render_data_management()
    elif page == "Data Explorer":
        render_data_explorer()
    elif page == "Settings":
        render_settings()


def render_data_explorer():
    """Render the data explorer page."""
    st.markdown("Execute SQL queries directly against the database.")

    if not config.database.path.exists():
        st.warning("Database not found. Please run initial_load.py first.")
        return

    # SQL input
    query = st.text_area(
        "SQL Query",
        value="SELECT * FROM master_events LIMIT 10",
        height=150,
        help="Enter a SQL query to execute against the MAUDE database",
    )

    col1, col2 = st.columns([1, 4])
    with col1:
        execute = st.button("▶️ Run Query", type="primary")

    if execute and query:
        try:
            with get_connection() as conn:
                result = conn.execute(query).fetchdf()

            st.success(f"Query returned {len(result):,} rows")
            st.dataframe(result, use_container_width=True)

            # Export option
            if not result.empty:
                csv = result.to_csv(index=False)
                st.download_button(
                    label="📥 Export CSV",
                    data=csv,
                    file_name="query_results.csv",
                    mime="text/csv",
                )
        except Exception as e:
            st.error(f"Query error: {e}")

    # Quick queries
    st.divider()
    st.subheader("Quick Queries")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("Table Counts"):
            try:
                with get_connection() as conn:
                    counts = get_table_counts(conn)
                st.json(counts)
            except Exception as e:
                st.error(str(e))

    with col2:
        if st.button("Recent MDRs"):
            try:
                with get_connection() as conn:
                    result = conn.execute("""
                        SELECT mdr_report_key, date_received, manufacturer_clean,
                               product_code, event_type
                        FROM master_events
                        ORDER BY date_received DESC
                        LIMIT 20
                    """).fetchdf()
                st.dataframe(result, use_container_width=True)
            except Exception as e:
                st.error(str(e))

    with col3:
        if st.button("Manufacturer Summary"):
            try:
                with get_connection() as conn:
                    result = conn.execute("""
                        SELECT manufacturer_clean, COUNT(*) as count
                        FROM master_events
                        GROUP BY manufacturer_clean
                        ORDER BY count DESC
                        LIMIT 10
                    """).fetchdf()
                st.dataframe(result, use_container_width=True)
            except Exception as e:
                st.error(str(e))


def render_settings():
    """Render the settings page."""
    st.subheader("Application Settings")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Database**")
        st.text_input("Database Path", value=str(config.database.path), disabled=True)

        if config.database.path.exists():
            size_mb = config.database.path.stat().st_size / 1024 / 1024
            st.text_input("Database Size", value=f"{size_mb:.1f} MB", disabled=True)

    with col2:
        st.markdown("**API**")
        st.text_input(
            "FDA API Key",
            value="Configured" if config.api.fda_api_key else "Not configured",
            disabled=True,
        )
        st.text_input(
            "Rate Limit",
            value=f"{config.api.rate_limit_per_minute}/min",
            disabled=True,
        )

    st.divider()

    st.subheader("Data Statistics")

    if config.database.path.exists():
        try:
            with get_connection() as conn:
                counts = get_table_counts(conn)

                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("MDR Reports", f"{counts.get('master_events', 0):,}")
                with col2:
                    st.metric("Devices", f"{counts.get('devices', 0):,}")
                with col3:
                    st.metric("Patients", f"{counts.get('patients', 0):,}")
                with col4:
                    st.metric("Narratives", f"{counts.get('mdr_text', 0):,}")

                # Date range
                date_range = conn.execute("""
                    SELECT MIN(date_received), MAX(date_received)
                    FROM master_events
                """).fetchone()

                if date_range[0]:
                    st.info(f"Data range: {date_range[0]} to {date_range[1]}")

        except Exception as e:
            st.error(f"Error loading statistics: {e}")
    else:
        st.warning("Database not found.")

    st.divider()

    st.subheader("Cache Management")
    if st.button("🗑️ Clear Streamlit Cache"):
        st.cache_data.clear()
        st.success("Cache cleared!")

    st.divider()

    st.subheader("About")
    st.markdown(
        f"""
        **{config.app.name}** v{config.app.version}

        A local desktop application for analyzing FDA MAUDE (Manufacturer and User
        Facility Device Experience) database records. Supports the full MAUDE database
        with comprehensive filtering by product code, manufacturer, date range, and more.

        Built with:
        - DuckDB for fast analytics
        - Streamlit for the web interface
        - Plotly for interactive visualizations
        """
    )


if __name__ == "__main__":
    main()
