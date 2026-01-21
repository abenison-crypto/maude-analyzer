"""Database maintenance utilities for MAUDE Analyzer."""

import duckdb
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger
from src.database import get_connection, get_table_counts

logger = get_logger("maintenance")


@dataclass
class MaintenanceResult:
    """Result of a maintenance operation."""

    operation: str
    success: bool
    duration_seconds: float = 0.0
    details: Dict[str, Any] = None
    error: Optional[str] = None

    def __post_init__(self):
        if self.details is None:
            self.details = {}


def vacuum_database(db_path: Optional[Path] = None) -> MaintenanceResult:
    """
    Run VACUUM to reclaim space and optimize database.

    Args:
        db_path: Path to database file.

    Returns:
        MaintenanceResult with operation details.
    """
    db_path = db_path or config.database.path
    start_time = datetime.now()

    try:
        size_before = db_path.stat().st_size if db_path.exists() else 0

        with get_connection(db_path) as conn:
            conn.execute("VACUUM")

        size_after = db_path.stat().st_size
        saved_bytes = size_before - size_after

        duration = (datetime.now() - start_time).total_seconds()

        return MaintenanceResult(
            operation="vacuum",
            success=True,
            duration_seconds=duration,
            details={
                "size_before_mb": size_before / (1024 * 1024),
                "size_after_mb": size_after / (1024 * 1024),
                "space_saved_mb": saved_bytes / (1024 * 1024),
            }
        )

    except Exception as e:
        logger.error(f"VACUUM failed: {e}")
        return MaintenanceResult(
            operation="vacuum",
            success=False,
            duration_seconds=(datetime.now() - start_time).total_seconds(),
            error=str(e),
        )


def analyze_tables(db_path: Optional[Path] = None) -> MaintenanceResult:
    """
    Run ANALYZE to update table statistics for query optimization.

    Args:
        db_path: Path to database file.

    Returns:
        MaintenanceResult with operation details.
    """
    db_path = db_path or config.database.path
    start_time = datetime.now()

    try:
        with get_connection(db_path) as conn:
            # Get list of tables
            tables = conn.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
            """).fetchall()

            analyzed = []
            for (table_name,) in tables:
                conn.execute(f"ANALYZE {table_name}")
                analyzed.append(table_name)

        duration = (datetime.now() - start_time).total_seconds()

        return MaintenanceResult(
            operation="analyze",
            success=True,
            duration_seconds=duration,
            details={"tables_analyzed": analyzed},
        )

    except Exception as e:
        logger.error(f"ANALYZE failed: {e}")
        return MaintenanceResult(
            operation="analyze",
            success=False,
            duration_seconds=(datetime.now() - start_time).total_seconds(),
            error=str(e),
        )


def get_table_statistics(db_path: Optional[Path] = None) -> Dict[str, Dict[str, Any]]:
    """
    Get detailed statistics for all tables.

    Args:
        db_path: Path to database file.

    Returns:
        Dictionary with table statistics.
    """
    db_path = db_path or config.database.path
    stats = {}

    try:
        with get_connection(db_path) as conn:
            # Get table counts
            counts = get_table_counts(conn)

            # Get additional stats for each table
            for table_name, count in counts.items():
                table_stats = {"row_count": count}

                # Get column count
                columns = conn.execute(f"""
                    SELECT COUNT(*)
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                """).fetchone()[0]
                table_stats["column_count"] = columns

                # For master_events, get additional stats
                if table_name == "master_events" and count > 0:
                    additional = conn.execute("""
                        SELECT
                            MIN(date_received) as min_date,
                            MAX(date_received) as max_date,
                            COUNT(DISTINCT manufacturer_clean) as unique_manufacturers,
                            COUNT(DISTINCT product_code) as unique_products
                        FROM master_events
                    """).fetchone()

                    table_stats["min_date"] = additional[0]
                    table_stats["max_date"] = additional[1]
                    table_stats["unique_manufacturers"] = additional[2]
                    table_stats["unique_products"] = additional[3]

                stats[table_name] = table_stats

    except Exception as e:
        logger.error(f"Error getting table statistics: {e}")

    return stats


def create_backup(
    db_path: Optional[Path] = None,
    backup_dir: Optional[Path] = None,
) -> MaintenanceResult:
    """
    Create a backup of the database.

    Args:
        db_path: Path to database file.
        backup_dir: Directory for backups (default: data/backups).

    Returns:
        MaintenanceResult with backup path.
    """
    db_path = db_path or config.database.path
    backup_dir = backup_dir or (config.data.base_path / "backups")
    backup_dir.mkdir(parents=True, exist_ok=True)

    start_time = datetime.now()
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"maude_backup_{timestamp}.duckdb"

    try:
        # Copy the database file
        shutil.copy2(db_path, backup_path)

        duration = (datetime.now() - start_time).total_seconds()
        backup_size = backup_path.stat().st_size / (1024 * 1024)

        logger.info(f"Backup created: {backup_path} ({backup_size:.1f} MB)")

        return MaintenanceResult(
            operation="backup",
            success=True,
            duration_seconds=duration,
            details={
                "backup_path": str(backup_path),
                "backup_size_mb": backup_size,
            }
        )

    except Exception as e:
        logger.error(f"Backup failed: {e}")
        return MaintenanceResult(
            operation="backup",
            success=False,
            duration_seconds=(datetime.now() - start_time).total_seconds(),
            error=str(e),
        )


def list_backups(backup_dir: Optional[Path] = None) -> List[Dict[str, Any]]:
    """
    List available database backups.

    Args:
        backup_dir: Directory containing backups.

    Returns:
        List of backup information dictionaries.
    """
    backup_dir = backup_dir or (config.data.base_path / "backups")

    if not backup_dir.exists():
        return []

    backups = []
    for backup_file in sorted(backup_dir.glob("maude_backup_*.duckdb"), reverse=True):
        stat = backup_file.stat()
        backups.append({
            "filename": backup_file.name,
            "path": str(backup_file),
            "size_mb": stat.st_size / (1024 * 1024),
            "created": datetime.fromtimestamp(stat.st_mtime),
        })

    return backups


def restore_backup(
    backup_path: Path,
    db_path: Optional[Path] = None,
) -> MaintenanceResult:
    """
    Restore database from a backup.

    Args:
        backup_path: Path to backup file.
        db_path: Destination database path.

    Returns:
        MaintenanceResult with operation details.
    """
    db_path = db_path or config.database.path
    start_time = datetime.now()

    try:
        if not backup_path.exists():
            raise FileNotFoundError(f"Backup not found: {backup_path}")

        # Create a safety backup of current database
        if db_path.exists():
            safety_backup = db_path.with_suffix(".duckdb.bak")
            shutil.copy2(db_path, safety_backup)
            logger.info(f"Created safety backup: {safety_backup}")

        # Restore from backup
        shutil.copy2(backup_path, db_path)

        duration = (datetime.now() - start_time).total_seconds()

        return MaintenanceResult(
            operation="restore",
            success=True,
            duration_seconds=duration,
            details={
                "restored_from": str(backup_path),
                "restored_to": str(db_path),
            }
        )

    except Exception as e:
        logger.error(f"Restore failed: {e}")
        return MaintenanceResult(
            operation="restore",
            success=False,
            duration_seconds=(datetime.now() - start_time).total_seconds(),
            error=str(e),
        )


def delete_old_backups(
    keep_count: int = 5,
    backup_dir: Optional[Path] = None,
) -> MaintenanceResult:
    """
    Delete old backups, keeping the most recent ones.

    Args:
        keep_count: Number of backups to keep.
        backup_dir: Directory containing backups.

    Returns:
        MaintenanceResult with deleted files.
    """
    backup_dir = backup_dir or (config.data.base_path / "backups")
    start_time = datetime.now()

    try:
        backups = list_backups(backup_dir)

        if len(backups) <= keep_count:
            return MaintenanceResult(
                operation="delete_old_backups",
                success=True,
                duration_seconds=0,
                details={"deleted_count": 0, "kept_count": len(backups)},
            )

        # Delete old backups
        deleted = []
        for backup in backups[keep_count:]:
            Path(backup["path"]).unlink()
            deleted.append(backup["filename"])
            logger.info(f"Deleted old backup: {backup['filename']}")

        duration = (datetime.now() - start_time).total_seconds()

        return MaintenanceResult(
            operation="delete_old_backups",
            success=True,
            duration_seconds=duration,
            details={
                "deleted_count": len(deleted),
                "deleted_files": deleted,
                "kept_count": keep_count,
            }
        )

    except Exception as e:
        logger.error(f"Delete old backups failed: {e}")
        return MaintenanceResult(
            operation="delete_old_backups",
            success=False,
            duration_seconds=(datetime.now() - start_time).total_seconds(),
            error=str(e),
        )


def get_ingestion_history(
    db_path: Optional[Path] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Get ingestion history from the database.

    Args:
        db_path: Path to database file.
        limit: Maximum records to return.

    Returns:
        List of ingestion log entries.
    """
    db_path = db_path or config.database.path

    try:
        with get_connection(db_path) as conn:
            result = conn.execute(f"""
                SELECT
                    id, file_name, file_type, source,
                    records_processed, records_loaded, records_errors,
                    started_at, completed_at, status, error_message
                FROM ingestion_log
                ORDER BY completed_at DESC
                LIMIT {limit}
            """).fetchdf()

            return result.to_dict("records")

    except Exception as e:
        logger.error(f"Error getting ingestion history: {e}")
        return []


def clear_all_data(
    db_path: Optional[Path] = None,
    confirm: bool = False,
) -> MaintenanceResult:
    """
    Clear all data from the database (keeps schema).

    Args:
        db_path: Path to database file.
        confirm: Must be True to proceed.

    Returns:
        MaintenanceResult with operation details.
    """
    if not confirm:
        return MaintenanceResult(
            operation="clear_all_data",
            success=False,
            error="Confirmation required (confirm=True)",
        )

    db_path = db_path or config.database.path
    start_time = datetime.now()

    try:
        with get_connection(db_path) as conn:
            # Get table counts before
            counts_before = get_table_counts(conn)

            # Delete from all tables
            tables = [
                "device_problems",
                "mdr_text",
                "patients",
                "devices",
                "master_events",
                "ingestion_log",
            ]

            for table in tables:
                conn.execute(f"DELETE FROM {table}")

            # Vacuum to reclaim space
            conn.execute("VACUUM")

        duration = (datetime.now() - start_time).total_seconds()

        return MaintenanceResult(
            operation="clear_all_data",
            success=True,
            duration_seconds=duration,
            details={"records_deleted": counts_before},
        )

    except Exception as e:
        logger.error(f"Clear data failed: {e}")
        return MaintenanceResult(
            operation="clear_all_data",
            success=False,
            duration_seconds=(datetime.now() - start_time).total_seconds(),
            error=str(e),
        )


def run_full_maintenance(db_path: Optional[Path] = None) -> Dict[str, MaintenanceResult]:
    """
    Run all maintenance operations.

    Args:
        db_path: Path to database file.

    Returns:
        Dictionary of results by operation name.
    """
    results = {}

    logger.info("Starting full maintenance...")

    # Create backup first
    results["backup"] = create_backup(db_path)

    # Analyze tables
    results["analyze"] = analyze_tables(db_path)

    # Vacuum
    results["vacuum"] = vacuum_database(db_path)

    # Clean old backups
    results["cleanup"] = delete_old_backups(keep_count=5)

    logger.info("Maintenance complete")
    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Database maintenance utilities")
    parser.add_argument("--vacuum", action="store_true", help="Run VACUUM")
    parser.add_argument("--analyze", action="store_true", help="Run ANALYZE")
    parser.add_argument("--stats", action="store_true", help="Show table statistics")
    parser.add_argument("--backup", action="store_true", help="Create backup")
    parser.add_argument("--list-backups", action="store_true", help="List backups")
    parser.add_argument("--history", action="store_true", help="Show ingestion history")
    parser.add_argument("--full", action="store_true", help="Run full maintenance")

    args = parser.parse_args()

    if args.vacuum:
        result = vacuum_database()
        print(f"VACUUM: {'OK' if result.success else 'FAILED'}")
        if result.details:
            print(f"  Space saved: {result.details.get('space_saved_mb', 0):.2f} MB")

    elif args.analyze:
        result = analyze_tables()
        print(f"ANALYZE: {'OK' if result.success else 'FAILED'}")
        if result.details:
            print(f"  Tables: {result.details.get('tables_analyzed', [])}")

    elif args.stats:
        stats = get_table_statistics()
        print("Table Statistics:")
        for table, info in stats.items():
            print(f"  {table}: {info['row_count']:,} rows")

    elif args.backup:
        result = create_backup()
        print(f"Backup: {'OK' if result.success else 'FAILED'}")
        if result.details:
            print(f"  Path: {result.details.get('backup_path')}")

    elif args.list_backups:
        backups = list_backups()
        print(f"Available backups ({len(backups)}):")
        for b in backups:
            print(f"  {b['filename']}: {b['size_mb']:.1f} MB ({b['created']})")

    elif args.history:
        history = get_ingestion_history(limit=10)
        print("Recent ingestion history:")
        for h in history:
            print(f"  {h['file_name']}: {h['records_loaded']:,} loaded ({h['status']})")

    elif args.full:
        results = run_full_maintenance()
        print("Maintenance Results:")
        for op, result in results.items():
            status = "OK" if result.success else "FAILED"
            print(f"  {op}: {status} ({result.duration_seconds:.1f}s)")

    else:
        parser.print_help()
