"""Database module for DuckDB operations."""

from .connection import (
    DatabaseConnection,
    get_connection,
    get_memory_connection,
    get_db,
    close_db,
)
from .schema import (
    initialize_database,
    create_all_tables,
    create_all_indexes,
    get_table_counts,
    drop_all_tables,
)
from .maintenance import (
    MaintenanceResult,
    vacuum_database,
    analyze_tables,
    get_table_statistics,
    create_backup,
    list_backups,
    restore_backup,
    delete_old_backups,
    get_ingestion_history,
    clear_all_data,
    run_full_maintenance,
)

__all__ = [
    # Connection
    "DatabaseConnection",
    "get_connection",
    "get_memory_connection",
    "get_db",
    "close_db",
    # Schema
    "initialize_database",
    "create_all_tables",
    "create_all_indexes",
    "get_table_counts",
    "drop_all_tables",
    # Maintenance
    "MaintenanceResult",
    "vacuum_database",
    "analyze_tables",
    "get_table_statistics",
    "create_backup",
    "list_backups",
    "restore_backup",
    "delete_old_backups",
    "get_ingestion_history",
    "clear_all_data",
    "run_full_maintenance",
]
