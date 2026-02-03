"""Safe column accessor for schema-resilient query building.

Provides safe column access with fallbacks to handle schema evolution
and missing columns gracefully.
"""

from typing import Optional, List, Dict, Any, Set, Union
from dataclasses import dataclass, field
import duckdb

from config.logging_config import get_logger
from src.database.schema_inspector import SchemaInspector, get_inspector

logger = get_logger("safe_columns")


@dataclass
class ColumnCheck:
    """Result of checking a column's availability."""
    column: str
    exists: bool
    table: str
    coverage_pct: Optional[float] = None
    data_type: Optional[str] = None


class SafeColumnAccessor:
    """
    Provides safe, schema-aware column access for queries.

    Features:
    - Check column existence before using in queries
    - Get available columns from a requested list
    - Provide fallback values for missing columns
    - Track and warn about schema mismatches
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection, table: str):
        """
        Initialize SafeColumnAccessor for a table.

        Args:
            conn: DuckDB connection
            table: Table name to access
        """
        self.conn = conn
        self.table = table
        self.inspector = get_inspector(conn)
        self._schema = self.inspector.get_table_schema(table)
        self._missing_columns: Set[str] = set()
        self._warned_columns: Set[str] = set()

    def has_column(self, column: str) -> bool:
        """
        Check if a column exists in the table.

        Args:
            column: Column name to check

        Returns:
            True if column exists
        """
        if self._schema is None:
            return False
        return self._schema.has_column(column)

    def get(self, column: str, fallback: Optional[str] = None) -> Optional[str]:
        """
        Get column name if it exists, or fallback.

        Args:
            column: Column name to get
            fallback: Fallback column name if primary doesn't exist

        Returns:
            Column name if exists, fallback if provided, None otherwise
        """
        if self.has_column(column):
            return column

        # Track missing column
        if column not in self._missing_columns:
            self._missing_columns.add(column)
            if column not in self._warned_columns:
                logger.warning(f"Column '{column}' not found in table '{self.table}'")
                self._warned_columns.add(column)

        # Try fallback
        if fallback and self.has_column(fallback):
            return fallback

        return None

    def get_or_null(self, column: str, alias: Optional[str] = None) -> str:
        """
        Get column for SELECT, returning NULL literal if column doesn't exist.

        Args:
            column: Column name
            alias: Optional alias for the column

        Returns:
            SQL expression (column name or NULL AS alias)
        """
        alias_part = f" AS {alias}" if alias else f" AS {column}"

        if self.has_column(column):
            return column + (f" AS {alias}" if alias else "")

        # Return NULL with appropriate alias
        if column not in self._warned_columns:
            logger.debug(f"Using NULL for missing column '{column}' in '{self.table}'")
            self._warned_columns.add(column)

        return f"NULL{alias_part}"

    def select_available(self, columns: List[str]) -> List[str]:
        """
        Filter a list of columns to only those that exist.

        Args:
            columns: List of desired columns

        Returns:
            List of columns that exist in the table
        """
        available = []
        for col in columns:
            if self.has_column(col):
                available.append(col)
            else:
                if col not in self._missing_columns:
                    self._missing_columns.add(col)
                    logger.debug(f"Column '{col}' not available in '{self.table}'")

        return available

    def select_with_nulls(self, columns: List[str]) -> List[str]:
        """
        Get all columns, using NULL for missing ones.

        Args:
            columns: List of desired columns

        Returns:
            List of SQL expressions (column or NULL AS column)
        """
        result = []
        for col in columns:
            result.append(self.get_or_null(col))
        return result

    def check_columns(self, columns: List[str]) -> Dict[str, ColumnCheck]:
        """
        Check multiple columns and return detailed status.

        Args:
            columns: List of columns to check

        Returns:
            Dict mapping column name to ColumnCheck with details
        """
        coverage = self.inspector.get_data_coverage(self.table, columns)

        results = {}
        for col in columns:
            exists = self.has_column(col)
            cov_pct = coverage.get(col, None)

            col_info = None
            if self._schema and col in self._schema.columns:
                col_info = self._schema.columns[col]

            results[col] = ColumnCheck(
                column=col,
                exists=exists,
                table=self.table,
                coverage_pct=cov_pct.coverage_pct if cov_pct else None,
                data_type=col_info.data_type if col_info else None
            )

        return results

    def get_all_columns(self) -> List[str]:
        """Get all column names in the table."""
        if self._schema is None:
            return []
        return self._schema.get_column_names()

    def get_columns_by_type(self, type_pattern: str) -> List[str]:
        """
        Get columns matching a type pattern.

        Args:
            type_pattern: Type to match (e.g., "VARCHAR", "DATE", "INTEGER")

        Returns:
            List of matching column names
        """
        if self._schema is None:
            return []
        return self._schema.get_columns_of_type(type_pattern)

    def get_date_columns(self) -> List[str]:
        """Get all date/timestamp columns."""
        return self.get_columns_by_type("DATE") + self.get_columns_by_type("TIMESTAMP")

    def get_numeric_columns(self) -> List[str]:
        """Get all numeric columns."""
        return (
            self.get_columns_by_type("INTEGER") +
            self.get_columns_by_type("BIGINT") +
            self.get_columns_by_type("DECIMAL") +
            self.get_columns_by_type("DOUBLE") +
            self.get_columns_by_type("FLOAT")
        )

    def get_text_columns(self) -> List[str]:
        """Get all text/varchar columns."""
        return self.get_columns_by_type("VARCHAR") + self.get_columns_by_type("TEXT")

    def get_missing_columns(self) -> Set[str]:
        """Get set of columns that were requested but don't exist."""
        return self._missing_columns.copy()

    def build_safe_where(
        self,
        conditions: Dict[str, Any],
        skip_missing: bool = True
    ) -> str:
        """
        Build WHERE clause, skipping conditions for missing columns.

        Args:
            conditions: Dict of column -> value for equality conditions
            skip_missing: If True, skip missing columns; if False, raise error

        Returns:
            WHERE clause string (without WHERE keyword)
        """
        parts = []

        for col, value in conditions.items():
            if not self.has_column(col):
                if skip_missing:
                    logger.debug(f"Skipping condition on missing column '{col}'")
                    continue
                else:
                    raise ValueError(f"Column '{col}' not found in table '{self.table}'")

            if value is None:
                parts.append(f"{col} IS NULL")
            elif isinstance(value, (list, tuple)):
                if value:
                    quoted = [f"'{v}'" if isinstance(v, str) else str(v) for v in value]
                    parts.append(f"{col} IN ({', '.join(quoted)})")
            elif isinstance(value, str):
                parts.append(f"{col} = '{value}'")
            else:
                parts.append(f"{col} = {value}")

        return " AND ".join(parts) if parts else "1=1"


def get_safe_columns(conn: duckdb.DuckDBPyConnection, table: str) -> SafeColumnAccessor:
    """
    Factory function to create a SafeColumnAccessor.

    Args:
        conn: DuckDB connection
        table: Table name

    Returns:
        SafeColumnAccessor instance
    """
    return SafeColumnAccessor(conn, table)


class MultiTableAccessor:
    """
    Safe column accessor for queries spanning multiple tables.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        """
        Initialize MultiTableAccessor.

        Args:
            conn: DuckDB connection
        """
        self.conn = conn
        self._accessors: Dict[str, SafeColumnAccessor] = {}

    def get_accessor(self, table: str) -> SafeColumnAccessor:
        """Get or create accessor for a table."""
        if table not in self._accessors:
            self._accessors[table] = SafeColumnAccessor(self.conn, table)
        return self._accessors[table]

    def has_column(self, table: str, column: str) -> bool:
        """Check if column exists in table."""
        return self.get_accessor(table).has_column(column)

    def get_qualified(self, table: str, column: str) -> Optional[str]:
        """Get fully qualified column name (table.column) if exists."""
        accessor = self.get_accessor(table)
        if accessor.has_column(column):
            return f"{table}.{column}"
        return None

    def select_available_from(
        self,
        table_columns: Dict[str, List[str]]
    ) -> Dict[str, List[str]]:
        """
        Filter columns per table to only available ones.

        Args:
            table_columns: Dict of table -> list of columns

        Returns:
            Dict of table -> available columns
        """
        result = {}
        for table, columns in table_columns.items():
            accessor = self.get_accessor(table)
            result[table] = accessor.select_available(columns)
        return result

    def get_all_missing(self) -> Dict[str, Set[str]]:
        """Get all missing columns across all tables."""
        return {
            table: accessor.get_missing_columns()
            for table, accessor in self._accessors.items()
            if accessor.get_missing_columns()
        }
