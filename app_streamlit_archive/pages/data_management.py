"""Data Management page for MAUDE Analyzer."""

import streamlit as st
import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from src.database import (
    get_connection,
    get_table_counts,
    vacuum_database,
    analyze_tables,
    get_table_statistics,
    create_backup,
    list_backups,
    get_ingestion_history,
    run_full_maintenance,
)
from src.ingestion import (
    DataUpdater,
    get_update_status,
)


def render_data_management():
    """Render the data management page."""
    st.markdown("Manage data updates, backups, and database maintenance.")

    # Check database
    if not config.database.path.exists():
        st.warning("Database not found. Please run initial_load.py first.")
        st.code("python scripts/initial_load.py", language="bash")
        return

    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "Status & Updates",
        "Ingestion History",
        "Backups",
        "Maintenance",
    ])

    with tab1:
        render_status_tab()

    with tab2:
        render_history_tab()

    with tab3:
        render_backups_tab()

    with tab4:
        render_maintenance_tab()


def render_status_tab():
    """Render the status and updates tab."""
    st.subheader("Database Status")

    # Get current status
    status = get_update_status()

    # Status cards
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total MDRs", f"{status.total_mdrs:,}")

    with col2:
        st.metric("Database Size", f"{status.database_size_mb:.1f} MB")

    with col3:
        if status.latest_date_received:
            st.metric("Latest Data", str(status.latest_date_received))
        else:
            st.metric("Latest Data", "N/A")

    with col4:
        if status.last_update:
            last_update_str = status.last_update.strftime("%Y-%m-%d %H:%M")
            st.metric("Last Update", last_update_str)
        else:
            st.metric("Last Update", "Never")

    # Table counts
    st.divider()
    st.subheader("Table Counts")

    if status.table_counts:
        counts_df = pd.DataFrame([
            {"Table": k, "Records": f"{v:,}"}
            for k, v in status.table_counts.items()
        ])
        st.dataframe(counts_df, use_container_width=True, hide_index=True)

    # Update section
    st.divider()
    st.subheader("Data Updates")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Fetch from openFDA API**")
        st.caption("Get recent MDRs via the openFDA API (recommended for incremental updates)")

        days = st.number_input("Days to fetch", min_value=1, max_value=365, value=30)

        if st.button("Fetch Recent Updates", type="primary"):
            with st.spinner("Fetching from openFDA API..."):
                try:
                    updater = DataUpdater()
                    result = updater.update_from_openfda(days=days)

                    if result.success:
                        st.success(
                            f"Update complete: {result.records_added} added, "
                            f"{result.records_updated} updated"
                        )
                    else:
                        st.error(f"Update failed: {result.errors}")

                    # Clear cached data
                    st.cache_data.clear()

                except Exception as e:
                    st.error(f"Error: {e}")

    with col2:
        st.markdown("**Check FDA Downloads**")
        st.caption("Check for new FDA bulk download files")

        if st.button("Check for New Files"):
            with st.spinner("Checking FDA downloads..."):
                try:
                    updater = DataUpdater()
                    missing = updater.check_for_fda_updates()

                    if missing:
                        st.info("New files available:")
                        for ftype, files in missing.items():
                            st.write(f"- {ftype}: {len(files)} files")
                    else:
                        st.success("All FDA files are up to date")

                except Exception as e:
                    st.error(f"Error: {e}")

    # Advanced options
    with st.expander("Advanced Update Options"):
        st.markdown("**Download and Load New FDA Files**")
        st.caption("Download any missing FDA files and load them into the database")

        col1, col2 = st.columns(2)

        with col1:
            if st.button("Download New Files"):
                with st.spinner("Downloading new FDA files..."):
                    try:
                        updater = DataUpdater()
                        results = updater.download_fda_updates()

                        if results:
                            for ftype, res in results.items():
                                success = sum(1 for r in res if r.success)
                                st.write(f"{ftype}: {success}/{len(res)} downloaded")
                        else:
                            st.info("No new files to download")

                    except Exception as e:
                        st.error(f"Error: {e}")

        with col2:
            if st.button("Load New Files"):
                with st.spinner("Loading new files..."):
                    try:
                        updater = DataUpdater()
                        result = updater.load_new_files()

                        st.success(
                            f"Loaded: {result.records_added} records, "
                            f"Skipped: {result.records_skipped}"
                        )

                        if result.errors:
                            st.warning(f"Errors: {result.errors[:3]}")

                        st.cache_data.clear()

                    except Exception as e:
                        st.error(f"Error: {e}")


def render_history_tab():
    """Render the ingestion history tab."""
    st.subheader("Ingestion History")

    history = get_ingestion_history(limit=50)

    if not history:
        st.info("No ingestion history found.")
        return

    # Convert to DataFrame
    df = pd.DataFrame(history)

    # Format columns
    if "completed_at" in df.columns:
        df["completed_at"] = pd.to_datetime(df["completed_at"]).dt.strftime("%Y-%m-%d %H:%M")

    if "started_at" in df.columns:
        df["started_at"] = pd.to_datetime(df["started_at"]).dt.strftime("%Y-%m-%d %H:%M")

    # Select columns to display
    display_cols = [
        "file_name", "file_type", "source", "records_loaded",
        "records_errors", "status", "completed_at"
    ]
    display_cols = [c for c in display_cols if c in df.columns]

    st.dataframe(
        df[display_cols],
        use_container_width=True,
        hide_index=True,
        column_config={
            "file_name": "File",
            "file_type": "Type",
            "source": "Source",
            "records_loaded": st.column_config.NumberColumn("Loaded", format="%d"),
            "records_errors": st.column_config.NumberColumn("Errors", format="%d"),
            "status": "Status",
            "completed_at": "Completed",
        }
    )

    # Summary stats
    st.divider()

    col1, col2, col3 = st.columns(3)

    with col1:
        total_loaded = df["records_loaded"].sum() if "records_loaded" in df.columns else 0
        st.metric("Total Records Loaded", f"{total_loaded:,}")

    with col2:
        total_errors = df["records_errors"].sum() if "records_errors" in df.columns else 0
        st.metric("Total Errors", f"{total_errors:,}")

    with col3:
        st.metric("Total Ingestions", len(df))


def render_backups_tab():
    """Render the backups tab."""
    st.subheader("Database Backups")

    # List existing backups
    backups = list_backups()

    if backups:
        st.markdown(f"**{len(backups)} backup(s) available:**")

        backup_df = pd.DataFrame(backups)
        backup_df["created"] = pd.to_datetime(backup_df["created"]).dt.strftime("%Y-%m-%d %H:%M")
        backup_df["size_mb"] = backup_df["size_mb"].round(1)

        st.dataframe(
            backup_df[["filename", "size_mb", "created"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "filename": "Backup File",
                "size_mb": st.column_config.NumberColumn("Size (MB)", format="%.1f"),
                "created": "Created",
            }
        )
    else:
        st.info("No backups found.")

    st.divider()

    # Create backup
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Create New Backup**")
        if st.button("Create Backup Now", type="primary"):
            with st.spinner("Creating backup..."):
                result = create_backup()

                if result.success:
                    st.success(
                        f"Backup created: {result.details.get('backup_path')}\n"
                        f"Size: {result.details.get('backup_size_mb', 0):.1f} MB"
                    )
                else:
                    st.error(f"Backup failed: {result.error}")

    with col2:
        st.markdown("**Restore from Backup**")
        if backups:
            selected_backup = st.selectbox(
                "Select backup to restore",
                options=[b["filename"] for b in backups],
                label_visibility="collapsed",
            )

            if st.button("Restore Selected", type="secondary"):
                st.warning("Restore functionality disabled in UI for safety. Use CLI instead.")
                st.code(f"python -m src.database.maintenance --restore {selected_backup}")
        else:
            st.caption("No backups available to restore")


def render_maintenance_tab():
    """Render the maintenance tab."""
    st.subheader("Database Maintenance")

    # Table statistics
    st.markdown("**Table Statistics**")

    stats = get_table_statistics()

    if stats:
        stats_data = []
        for table, info in stats.items():
            row = {
                "Table": table,
                "Rows": f"{info.get('row_count', 0):,}",
                "Columns": info.get("column_count", 0),
            }
            if "min_date" in info:
                row["Date Range"] = f"{info['min_date']} to {info['max_date']}"
            stats_data.append(row)

        st.dataframe(
            pd.DataFrame(stats_data),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    # Maintenance actions
    st.markdown("**Maintenance Actions**")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**Analyze Tables**")
        st.caption("Update query statistics")
        if st.button("Run ANALYZE"):
            with st.spinner("Analyzing tables..."):
                result = analyze_tables()
                if result.success:
                    st.success(f"Analyzed in {result.duration_seconds:.1f}s")
                else:
                    st.error(f"Failed: {result.error}")

    with col2:
        st.markdown("**Vacuum Database**")
        st.caption("Reclaim disk space")
        if st.button("Run VACUUM"):
            with st.spinner("Running VACUUM..."):
                result = vacuum_database()
                if result.success:
                    saved = result.details.get("space_saved_mb", 0)
                    st.success(f"Vacuumed in {result.duration_seconds:.1f}s, saved {saved:.1f} MB")
                else:
                    st.error(f"Failed: {result.error}")

    with col3:
        st.markdown("**Full Maintenance**")
        st.caption("Backup + Analyze + Vacuum")
        if st.button("Run Full Maintenance"):
            with st.spinner("Running full maintenance..."):
                results = run_full_maintenance()
                success_count = sum(1 for r in results.values() if r.success)
                st.success(f"Completed {success_count}/{len(results)} operations")

    st.divider()

    # Clear cache
    st.markdown("**Cache Management**")

    if st.button("Clear Streamlit Cache"):
        st.cache_data.clear()
        st.success("Cache cleared!")

    # Danger zone
    st.divider()
    with st.expander("Danger Zone", expanded=False):
        st.warning("These actions are destructive and cannot be undone!")

        st.markdown("**Clear All Data**")
        st.caption("Delete all records from the database (keeps schema)")

        confirm_text = st.text_input(
            "Type 'DELETE ALL DATA' to confirm",
            key="confirm_delete"
        )

        if st.button("Clear All Data", type="secondary"):
            if confirm_text == "DELETE ALL DATA":
                with st.spinner("Clearing all data..."):
                    from src.database import clear_all_data
                    result = clear_all_data(confirm=True)
                    if result.success:
                        st.success("All data cleared!")
                        st.cache_data.clear()
                    else:
                        st.error(f"Failed: {result.error}")
            else:
                st.error("Please type 'DELETE ALL DATA' to confirm")
