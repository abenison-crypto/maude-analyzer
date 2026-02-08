"""Database connection service for FastAPI."""

import duckdb
from functools import lru_cache
from typing import Optional
from pathlib import Path

from api.config import get_settings


class DatabaseService:
    """Manages DuckDB database connections for FastAPI."""

    def __init__(self, db_path: Optional[Path] = None):
        """Initialize database service.

        Args:
            db_path: Path to database file.
        """
        settings = get_settings()
        self.db_path = db_path or settings.database_path
        self._connection: Optional[duckdb.DuckDBPyConnection] = None

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Get or create database connection."""
        if self._connection is None:
            self._connection = duckdb.connect(str(self.db_path), read_only=True)
            self._configure()
        return self._connection

    def _configure(self) -> None:
        """Configure connection for optimal read performance."""
        if self._connection:
            self._connection.execute("SET memory_limit = '4GB'")
            self._connection.execute("SET threads = 4")

    def execute(self, query: str, params: Optional[list] = None):
        """Execute a query and return results."""
        conn = self.connect()
        if params:
            return conn.execute(query, params)
        return conn.execute(query)

    def fetch_one(self, query: str, params: Optional[list] = None):
        """Execute query and fetch one result."""
        return self.execute(query, params).fetchone()

    def fetch_all(self, query: str, params: Optional[list] = None):
        """Execute query and fetch all results."""
        return self.execute(query, params).fetchall()

    def fetch_df(self, query: str, params: Optional[list] = None):
        """Execute query and return as DataFrame."""
        return self.execute(query, params).df()

    def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def reconnect(self) -> duckdb.DuckDBPyConnection:
        """Close and reopen the database connection."""
        self.close()
        return self.connect()


# Global database instance
_db_service: Optional[DatabaseService] = None


def get_db() -> DatabaseService:
    """Get global database service instance."""
    global _db_service
    if _db_service is None:
        _db_service = DatabaseService()
    return _db_service


def close_db() -> None:
    """Close the global database connection."""
    global _db_service
    if _db_service is not None:
        _db_service.close()


def reconnect_db() -> None:
    """Reconnect the global database."""
    global _db_service
    if _db_service is not None:
        _db_service.reconnect()
