"""Search page for MAUDE Analyzer.

Updated with server-side pagination and searchable filters.
Defaults to all products/manufacturers - no product category is prioritized.
"""

import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config, get_event_type_name
from src.database import get_connection
from src.analysis import (
    SearchQuery,
    get_filter_options,
    get_record_detail,
    DataExporter,
)
from src.analysis.queries import SchemaAwareSearchQuery
from app.utils.pagination import StreamlitPaginator, reset_pagination
from app.utils.display_helpers import (
    format_nullable,
    format_date,
    add_missing_data_warning,
    prepare_display_df,
)
from app.components.searchable_select import (
    searchable_manufacturer_select,
    searchable_product_code_select,
    CachedSearchableSelect,
)


def render_search():
    """Render the search page."""
    # Initialize session state
    if "search_query_params" not in st.session_state:
        st.session_state.search_query_params = None
    if "search_executed" not in st.session_state:
        st.session_state.search_executed = False

    # Check database
    if not config.database.path.exists():
        st.warning("Database not found. Please run initial_load.py first.")
        return

    # Search filters
    st.subheader("Search Filters")
    st.caption("Leave all filters empty to search all data")

    with get_connection() as conn:
        render_search_form(conn)

        # Display results if search was executed
        if st.session_state.search_executed:
            st.divider()
            render_search_results(conn)


def render_search_form(conn):
    """Render the search form with searchable filters."""

    with st.form("search_form"):
        # Row 1: Searchable filters
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Manufacturer** (type to search)")
            # Use cached searchable select for manufacturers
            mfr_select = CachedSearchableSelect(
                conn=conn,
                table="master_events",
                column="manufacturer_clean",
                key="mfr_search",
                label="Manufacturer",
            )
            selected_manufacturers = mfr_select.render(
                multi=True,
                default=[],
                show_counts=True,
            )

        with col2:
            st.markdown("**Product Code** (type to search)")
            pc_select = CachedSearchableSelect(
                conn=conn,
                table="master_events",
                column="product_code",
                key="pc_search",
                label="Product Code",
            )
            selected_product_codes = pc_select.render(
                multi=True,
                default=[],
                show_counts=True,
            )

        # Row 2: Event type and date range
        col1, col2, col3 = st.columns([1, 1, 2])

        with col1:
            # Get event types
            event_types = conn.execute("""
                SELECT DISTINCT event_type
                FROM master_events
                WHERE event_type IS NOT NULL
                ORDER BY event_type
            """).fetchdf()["event_type"].tolist()

            selected_event_types = st.multiselect(
                "Event Type",
                options=event_types,
                format_func=lambda x: get_event_type_name(x),
                help="D=Death, IN=Injury, M=Malfunction",
            )

        with col2:
            # Date filter type
            date_filter_type = st.selectbox(
                "Date Range",
                options=["All Time", "Last 30 days", "Last 90 days", "Last 12 months", "Custom"],
                index=0,
            )

        with col3:
            if date_filter_type == "Custom":
                date_col1, date_col2 = st.columns(2)
                with date_col1:
                    start_date = st.date_input("Start Date", value=None)
                with date_col2:
                    end_date = st.date_input("End Date", value=None)
            else:
                start_date = None
                end_date = None
                if date_filter_type == "Last 30 days":
                    start_date = date.today() - timedelta(days=30)
                elif date_filter_type == "Last 90 days":
                    start_date = date.today() - timedelta(days=90)
                elif date_filter_type == "Last 12 months":
                    start_date = date.today() - timedelta(days=365)
                # "All Time" = no date filter
                st.empty()  # Placeholder

        # Row 3: Text search
        col1, col2 = st.columns([3, 1])

        with col1:
            text_search = st.text_input(
                "Text Search",
                placeholder="Search in event narratives...",
                help="Search in event descriptions and manufacturer narratives",
            )

        with col2:
            include_text = st.checkbox(
                "Include text in results",
                value=False,
                help="Include narrative text columns in results (slower)",
            )

        # Advanced options
        with st.expander("Advanced Options"):
            adv_col1, adv_col2, adv_col3 = st.columns(3)

            with adv_col1:
                mdr_key = st.text_input(
                    "MDR Report Key",
                    placeholder="e.g., 1234567",
                    help="Search by specific MDR key",
                )

            with adv_col2:
                report_number = st.text_input(
                    "Report Number",
                    placeholder="e.g., 1234567-2024-00001",
                )

            with adv_col3:
                brand_name = st.text_input(
                    "Brand Name",
                    placeholder="Search in device brand names",
                )

        # Results options
        col1, col2, col3 = st.columns([2, 1, 1])

        with col1:
            results_per_page = st.selectbox(
                "Results per page",
                options=[25, 50, 100, 250],
                index=0,
            )

        with col2:
            sort_by = st.selectbox(
                "Sort by",
                options=["date_received", "mdr_report_key", "event_type"],
                format_func=lambda x: x.replace("_", " ").title(),
            )

        with col3:
            sort_order = st.selectbox(
                "Order",
                options=["DESC", "ASC"],
                format_func=lambda x: "Newest First" if x == "DESC" else "Oldest First",
            )

        # Submit button
        submitted = st.form_submit_button("Search", type="primary", use_container_width=True)

    if submitted:
        # Store query parameters
        st.session_state.search_query_params = {
            "manufacturers": selected_manufacturers or [],
            "product_codes": selected_product_codes or [],
            "event_types": selected_event_types or [],
            "start_date": start_date,
            "end_date": end_date,
            "text_search": text_search,
            "mdr_key": mdr_key,
            "report_number": report_number,
            "brand_name": brand_name,
            "include_text": include_text,
            "page_size": results_per_page,
            "sort_by": sort_by,
            "sort_order": sort_order,
        }
        st.session_state.search_executed = True
        reset_pagination("search")


def render_search_results(conn):
    """Render search results with pagination."""
    params = st.session_state.search_query_params

    if not params:
        return

    # Build schema-aware query
    query = SchemaAwareSearchQuery(
        limit=params["page_size"],
        sort_by=params["sort_by"],
        sort_order=params["sort_order"],
        include_text=params["include_text"],
    )
    query.set_connection(conn)

    # Add filters
    if params["manufacturers"]:
        query.add_manufacturers(params["manufacturers"])

    if params["product_codes"]:
        query.add_product_codes(params["product_codes"])

    if params["event_types"]:
        query.add_event_types(params["event_types"])

    if params["start_date"] or params["end_date"]:
        query.add_date_range("date_received", params["start_date"], params["end_date"])

    if params["text_search"]:
        query.add_text_search(params["text_search"])

    if params["mdr_key"]:
        query.add_condition("mdr_report_key", "equals", params["mdr_key"])

    if params["report_number"]:
        query.add_condition("report_number", "contains", params["report_number"])

    # Get count first
    total_count = query.count(conn)

    # Show skipped conditions warning
    skipped = query.get_skipped_conditions()
    if skipped:
        st.warning(f"Some filters were skipped (columns not found): {', '.join(skipped)}")

    # Results header
    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        st.subheader(f"Results: {total_count:,} records found")

    # Pagination
    paginator = StreamlitPaginator("search", conn, params["page_size"])

    # Build paginated SQL
    base_sql, query_params = query.build_sql()
    # Remove LIMIT/OFFSET from base SQL (paginator will add them)
    base_sql_no_limit = base_sql.rsplit("LIMIT", 1)[0].strip()

    count_sql, _ = query.build_count_sql()

    # Execute paginated query
    try:
        results_df, state = paginator.paginate(
            base_sql_no_limit,
            query_params,
            count_sql,
        )
    except Exception as e:
        st.error(f"Search error: {e}")
        return

    # Export buttons
    with col2:
        if not results_df.empty:
            exporter = DataExporter()
            csv_buffer = exporter.export_to_csv_buffer(results_df)
            st.download_button(
                label="Export CSV",
                data=csv_buffer.getvalue(),
                file_name=f"maude_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
            )

    with col3:
        if not results_df.empty:
            excel_buffer = exporter.export_to_excel_buffer(results_df)
            st.download_button(
                label="Export Excel",
                data=excel_buffer.getvalue(),
                file_name=f"maude_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    # Pagination controls
    paginator.render_controls()

    if results_df.empty:
        st.info("No records found matching your search criteria.")
        return

    # Format and display results
    display_df = format_results_for_display(results_df)

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "MDR Key": st.column_config.TextColumn(width="small"),
            "Date Received": st.column_config.DateColumn(width="small"),
            "Event Date": st.column_config.DateColumn(width="small"),
            "Manufacturer": st.column_config.TextColumn(width="medium"),
            "Product Code": st.column_config.TextColumn(width="small"),
            "Event Type": st.column_config.TextColumn(width="small"),
        },
    )

    # Record detail viewer
    st.divider()
    render_record_detail_section(results_df, conn)


def format_results_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """Format results DataFrame for display."""
    display_cols = {
        "mdr_report_key": "MDR Key",
        "date_received": "Date Received",
        "date_of_event": "Event Date",
        "manufacturer_clean": "Manufacturer",
        "manufacturer_name": "Manufacturer",  # Fallback
        "product_code": "Product Code",
        "event_type": "Event Type",
        "type_of_report": "Report Type",
    }

    # Select and rename available columns
    result_cols = []
    result_names = []

    for col, name in display_cols.items():
        if col in df.columns and name not in result_names:
            result_cols.append(col)
            result_names.append(name)

    display_df = df[result_cols].copy()
    display_df.columns = result_names

    # Format event type using config
    if "Event Type" in display_df.columns:
        display_df["Event Type"] = display_df["Event Type"].apply(
            lambda x: get_event_type_name(x) if pd.notna(x) else "Unknown"
        )

    # Replace NaN with "N/A"
    display_df = display_df.fillna("N/A")

    return display_df


def render_record_detail_section(results_df: pd.DataFrame, conn):
    """Render the record detail section."""
    st.subheader("Record Detail")

    if results_df.empty:
        return

    # Build options with manufacturer info
    options = []
    for _, row in results_df.iterrows():
        key = row.get("mdr_report_key", "")
        mfr = row.get("manufacturer_clean") or row.get("manufacturer_name", "")
        options.append(f"{key} - {mfr}")

    selected_option = st.selectbox(
        "Select MDR Key to view details",
        options=options,
    )

    if selected_option:
        selected_key = selected_option.split(" - ")[0]
        render_record_detail(selected_key, conn)


def render_record_detail(mdr_key: str, conn):
    """Render detailed view of a single record."""
    try:
        detail = get_record_detail(mdr_key, conn)
    except Exception as e:
        st.error(f"Error loading record: {e}")
        return

    if not detail:
        st.warning("Record not found.")
        return

    master = detail.get("master", {})

    # Master record info
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**Report Information**")
        st.write(f"MDR Key: `{format_nullable(master.get('mdr_report_key'))}`")
        st.write(f"Report Number: `{format_nullable(master.get('report_number'))}`")
        st.write(f"Event Type: {get_event_type_name(master.get('event_type', ''))}")
        st.write(f"Product Code: `{format_nullable(master.get('product_code'))}`")

    with col2:
        st.markdown("**Dates**")
        st.write(f"Date Received: {format_date(master.get('date_received'))}")
        st.write(f"Date of Event: {format_date(master.get('date_of_event'))}")
        st.write(f"Date Reported: {format_date(master.get('date_report'))}")

    with col3:
        st.markdown("**Manufacturer**")
        st.write(f"Name: {format_nullable(master.get('manufacturer_name'))}")
        st.write(f"Standardized: {format_nullable(master.get('manufacturer_clean'))}")
        st.write(f"PMA/510(k): {format_nullable(master.get('pma_pmn_number'))}")

    # Devices
    devices = detail.get("devices", [])
    if devices:
        st.markdown("**Devices**")
        devices_df = pd.DataFrame(devices)
        display_device_cols = ["brand_name", "generic_name", "model_number", "lot_number", "manufacturer_d_name"]
        available_device_cols = [c for c in display_device_cols if c in devices_df.columns]
        if available_device_cols:
            st.dataframe(
                devices_df[available_device_cols].fillna("N/A"),
                use_container_width=True,
                hide_index=True,
            )

    # Patients
    patients = detail.get("patients", [])
    if patients:
        st.markdown("**Patient Outcomes**")
        patients_df = pd.DataFrame(patients)
        outcome_cols = [c for c in patients_df.columns if c.startswith("outcome_")]
        if outcome_cols:
            outcome_flags = []
            for _, row in patients_df.iterrows():
                flags = [c.replace("outcome_", "").replace("_", " ").title()
                         for c in outcome_cols if row.get(c)]
                outcome_flags.append(", ".join(flags) if flags else "None recorded")
            st.write("Outcomes: " + "; ".join(outcome_flags))
        else:
            st.caption("Note: Patient outcome boolean fields may not be populated")

    # Narratives
    texts = detail.get("text", [])
    if texts:
        st.markdown("**Event Narratives**")
        for text_record in texts:
            text_type = text_record.get("text_type_code", "")
            type_label = {
                "D": "Event Description",
                "E": "Manufacturer Evaluation",
                "H": "Device Description",
                "M": "Manufacturer Narrative",
                "N": "Additional Information",
            }.get(text_type, f"Text ({text_type})")

            with st.expander(type_label, expanded=(text_type == "D")):
                content = text_record.get("text_content") or "No content"
                st.text_area(
                    label=type_label,
                    value=content,
                    height=200,
                    disabled=True,
                    label_visibility="collapsed",
                )

    # Problems
    problems = detail.get("problems", [])
    if problems:
        st.markdown("**Device Problem Codes**")
        problem_codes = [p.get("device_problem_code") for p in problems if p.get("device_problem_code")]
        st.write(", ".join(problem_codes) if problem_codes else "None")
