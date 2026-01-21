"""Search page for MAUDE Analyzer."""

import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config, EVENT_TYPES
from src.database import get_connection
from src.analysis import (
    SearchQuery,
    get_filter_options,
    get_record_detail,
    DataExporter,
)


def render_search():
    """Render the search page."""
    # Initialize session state
    if "search_results" not in st.session_state:
        st.session_state.search_results = None
    if "search_count" not in st.session_state:
        st.session_state.search_count = 0
    if "current_page" not in st.session_state:
        st.session_state.current_page = 0
    if "selected_record" not in st.session_state:
        st.session_state.selected_record = None

    # Check database
    if not config.database.path.exists():
        st.warning("Database not found. Please run initial_load.py first.")
        return

    # Get filter options
    try:
        with get_connection() as conn:
            filter_options = get_filter_options(conn)
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        return

    # Search filters
    st.subheader("Search Filters")

    with st.form("search_form"):
        # Row 1: Basic filters
        col1, col2, col3 = st.columns(3)

        with col1:
            selected_manufacturers = st.multiselect(
                "Manufacturer",
                options=filter_options.get("manufacturers", []),
                help="Select one or more manufacturers",
            )

        with col2:
            selected_product_codes = st.multiselect(
                "Product Code",
                options=filter_options.get("product_codes", []),
                default=["GZB", "LGW", "PMP"] if set(["GZB", "LGW", "PMP"]).issubset(
                    set(filter_options.get("product_codes", []))
                ) else [],
                help="SCS codes: GZB, LGW, PMP",
            )

        with col3:
            selected_event_types = st.multiselect(
                "Event Type",
                options=filter_options.get("event_types", []),
                format_func=lambda x: EVENT_TYPES.get(x, x),
                help="D=Death, IN=Injury, M=Malfunction",
            )

        # Row 2: Date range and text search
        col1, col2 = st.columns(2)

        with col1:
            date_col1, date_col2 = st.columns(2)
            with date_col1:
                start_date = st.date_input(
                    "Start Date",
                    value=date.today() - timedelta(days=365 * 5),
                    help="Filter by date received",
                )
            with date_col2:
                end_date = st.date_input(
                    "End Date",
                    value=date.today(),
                )

        with col2:
            text_search = st.text_input(
                "Text Search",
                placeholder="Search in narratives...",
                help="Search in event descriptions and manufacturer narratives",
            )

        # Row 3: Advanced options
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
                    placeholder="e.g., Proclaim",
                    help="Search in device brand names",
                )

        # Search options
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            results_per_page = st.selectbox(
                "Results per page",
                options=[25, 50, 100, 250, 500],
                index=1,
            )
        with col2:
            sort_by = st.selectbox(
                "Sort by",
                options=["date_received", "mdr_report_key", "manufacturer_clean"],
                format_func=lambda x: x.replace("_", " ").title(),
            )
        with col3:
            sort_order = st.selectbox(
                "Order",
                options=["DESC", "ASC"],
                format_func=lambda x: "Newest First" if x == "DESC" else "Oldest First",
            )

        # Search button
        submitted = st.form_submit_button("🔍 Search", type="primary", use_container_width=True)

    if submitted:
        # Build query
        query = SearchQuery(
            limit=results_per_page,
            offset=0,
            sort_by=sort_by,
            sort_order=sort_order,
        )

        if selected_manufacturers:
            query.add_manufacturers(selected_manufacturers)

        if selected_product_codes:
            query.add_product_codes(selected_product_codes)

        if selected_event_types:
            query.add_event_types(selected_event_types)

        if start_date and end_date:
            query.add_date_range("date_received", start_date, end_date)

        if text_search:
            query.add_text_search(text_search)

        if mdr_key:
            query.add_condition("mdr_report_key", "equals", mdr_key)

        if report_number:
            query.add_condition("report_number", "contains", report_number)

        # Execute query
        try:
            with get_connection() as conn:
                st.session_state.search_results = query.execute(conn)
                st.session_state.search_count = query.count(conn)
                st.session_state.current_page = 0
        except Exception as e:
            st.error(f"Search error: {e}")
            return

    # Display results
    if st.session_state.search_results is not None:
        results_df = st.session_state.search_results
        total_count = st.session_state.search_count

        st.divider()

        # Results header
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            st.subheader(f"Results: {total_count:,} records found")
            if len(results_df) < total_count:
                st.caption(f"Showing {len(results_df)} of {total_count:,}")

        with col2:
            # CSV Export
            if not results_df.empty:
                exporter = DataExporter()
                csv_buffer = exporter.export_to_csv_buffer(results_df)
                st.download_button(
                    label="📥 Export CSV",
                    data=csv_buffer.getvalue(),
                    file_name=f"maude_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                )

        with col3:
            # Excel Export
            if not results_df.empty:
                excel_buffer = exporter.export_to_excel_buffer(results_df)
                st.download_button(
                    label="📥 Export Excel",
                    data=excel_buffer.getvalue(),
                    file_name=f"maude_search_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

        if results_df.empty:
            st.info("No records found matching your search criteria.")
        else:
            # Format results for display
            display_df = format_results_for_display(results_df)

            # Results table
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
                    "Report Type": st.column_config.TextColumn(width="medium"),
                },
            )

            # Record detail viewer
            st.divider()
            st.subheader("Record Detail")

            selected_key = st.selectbox(
                "Select MDR Key to view details",
                options=results_df["mdr_report_key"].tolist(),
                format_func=lambda x: f"{x} - {results_df[results_df['mdr_report_key'] == x]['manufacturer_clean'].values[0] if len(results_df[results_df['mdr_report_key'] == x]) > 0 else ''}",
            )

            if selected_key:
                render_record_detail(selected_key)


def format_results_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """Format results DataFrame for display."""
    display_cols = {
        "mdr_report_key": "MDR Key",
        "date_received": "Date Received",
        "date_of_event": "Event Date",
        "manufacturer_clean": "Manufacturer",
        "product_code": "Product Code",
        "event_type": "Event Type",
        "type_of_report": "Report Type",
    }

    # Select and rename columns
    available_cols = [c for c in display_cols.keys() if c in df.columns]
    display_df = df[available_cols].copy()
    display_df.columns = [display_cols[c] for c in available_cols]

    # Format event type
    if "Event Type" in display_df.columns:
        display_df["Event Type"] = display_df["Event Type"].map(EVENT_TYPES).fillna(display_df["Event Type"])

    return display_df


def render_record_detail(mdr_key: str):
    """Render detailed view of a single record."""
    try:
        with get_connection() as conn:
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
        st.write(f"MDR Key: `{master.get('mdr_report_key')}`")
        st.write(f"Report Number: `{master.get('report_number')}`")
        st.write(f"Event Type: {EVENT_TYPES.get(master.get('event_type'), master.get('event_type'))}")
        st.write(f"Product Code: `{master.get('product_code')}`")

    with col2:
        st.markdown("**Dates**")
        st.write(f"Date Received: {master.get('date_received')}")
        st.write(f"Date of Event: {master.get('date_of_event')}")
        st.write(f"Date Reported: {master.get('date_report')}")

    with col3:
        st.markdown("**Manufacturer**")
        st.write(f"Name: {master.get('manufacturer_name')}")
        st.write(f"Standardized: {master.get('manufacturer_clean')}")
        st.write(f"PMA/510(k): {master.get('pma_pmn_number')}")

    # Devices
    devices = detail.get("devices", [])
    if devices:
        st.markdown("**Devices**")
        devices_df = pd.DataFrame(devices)
        display_device_cols = ["brand_name", "generic_name", "model_number", "lot_number", "manufacturer_d_name"]
        available_device_cols = [c for c in display_device_cols if c in devices_df.columns]
        if available_device_cols:
            st.dataframe(devices_df[available_device_cols], use_container_width=True, hide_index=True)

    # Patients
    patients = detail.get("patients", [])
    if patients:
        st.markdown("**Patient Outcomes**")
        patients_df = pd.DataFrame(patients)
        outcome_cols = [c for c in patients_df.columns if c.startswith("outcome_")]
        if outcome_cols:
            # Show outcomes as flags
            outcome_flags = []
            for _, row in patients_df.iterrows():
                flags = [c.replace("outcome_", "").title() for c in outcome_cols if row.get(c)]
                outcome_flags.append(", ".join(flags) if flags else "None recorded")
            st.write("Outcomes: " + "; ".join(outcome_flags))

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
                "B": "Patient Description",
            }.get(text_type, f"Text ({text_type})")

            with st.expander(type_label, expanded=True):
                content = text_record.get("text_content", "No content")
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
        st.write(", ".join(problem_codes))
