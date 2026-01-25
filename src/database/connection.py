"""DuckDB connection management for MAUDE Analyzer."""

import duckdb
from pathlib import Path
from typing import Optional
from contextlib import contextmanager
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger

logger = get_logger("database")


class DatabaseConnection:
    """Manages DuckDB database connections."""

    def __init__(self, db_path: Optional[Path] = None, read_only: bool = False):
        """
        Initialize database connection manager.

        Args:
            db_path: Path to database file. Defaults to config setting.
            read_only: Open database in read-only mode.
        """
        self.db_path = db_path or config.database.path
        self.read_only = read_only
        self._connection: Optional[duckdb.DuckDBPyConnection] = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        """
        Establish connection to the database.

        Returns:
            DuckDB connection object.
        """
        if self._connection is not None:
            return self._connection

        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Connect to database
        self._connection = duckdb.connect(
            str(self.db_path),
            read_only=self.read_only,
        )

        # Configure connection
        self._configure_connection()

        logger.info(f"Connected to database: {self.db_path}")
        return self._connection

    def _configure_connection(self) -> None:
        """Configure connection settings for optimal performance."""
        if self._connection is None:
            return

        # Set memory limit (use higher limit for bulk loading)
        self._connection.execute(
            f"SET memory_limit = '{config.database.memory_limit}'"
        )

        # Disable insertion order preservation to reduce memory usage
        # during bulk inserts with INSERT OR REPLACE
        self._connection.execute("SET preserve_insertion_order = false")

        # Enable progress bar for long operations
        self._connection.execute("SET enable_progress_bar = true")

        # Set threads
        if config.database.threads > 0:
            self._connection.execute(f"SET threads = {config.database.threads}")

    def close(self) -> None:
        """Close the database connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.info("Database connection closed")

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get the current connection, establishing if needed."""
        if self._connection is None:
            return self.connect()
        return self._connection

    def execute(self, query: str, parameters: Optional[list] = None):
        """
        Execute a SQL query.

        Args:
            query: SQL query string.
            parameters: Optional query parameters.

        Returns:
            Query result.
        """
        conn = self.connection
        if parameters:
            return conn.execute(query, parameters)
        return conn.execute(query)

    def executemany(self, query: str, parameters: list):
        """
        Execute a SQL query with multiple parameter sets.

        Args:
            query: SQL query string.
            parameters: List of parameter tuples.

        Returns:
            Query result.
        """
        return self.connection.executemany(query, parameters)

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False


@contextmanager
def get_connection(
    db_path: Optional[Path] = None, read_only: bool = False
) -> duckdb.DuckDBPyConnection:
    """
    Context manager for database connections.

    Args:
        db_path: Path to database file.
        read_only: Open in read-only mode.

    Yields:
        DuckDB connection object.

    Example:
        with get_connection() as conn:
            result = conn.execute("SELECT * FROM master_events LIMIT 10")
    """
    db = DatabaseConnection(db_path, read_only)
    try:
        yield db.connect()
    finally:
        db.close()


def get_memory_connection() -> duckdb.DuckDBPyConnection:
    """
    Get an in-memory database connection for testing.

    Returns:
        In-memory DuckDB connection.
    """
    return duckdb.connect(":memory:")


# Global connection instance for convenience
_global_db: Optional[DatabaseConnection] = None


def get_db() -> DatabaseConnection:
    """
    Get the global database connection instance.

    Returns:
        Global DatabaseConnection instance.
    """
    global _global_db
    if _global_db is None:
        _global_db = DatabaseConnection()
    return _global_db


def close_db() -> None:
    """Close the global database connection."""
    global _global_db
    if _global_db is not None:
        _global_db.close()
        _global_db = None
