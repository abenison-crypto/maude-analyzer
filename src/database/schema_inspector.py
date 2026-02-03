"""Runtime schema discovery and introspection for MAUDE database.

Provides dynamic schema inspection to avoid hardcoded column dependencies
and enable graceful handling of schema changes.
"""

from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, field
from functools import lru_cache
import duckdb

from config.logging_config import get_logger

logger = get_logger("schema_inspector")


@dataclass
class ColumnInfo:
    """Information about a database column."""
    name: str
    data_type: str
    nullable: bool = True
    default_value: Optional[str] = None
    is_primary_key: bool = False


@dataclass
class TableSchema:
    """Schema information for a database table."""
    name: str
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)
    row_count: int = 0

    def has_column(self, column: str) -> bool:
        """Check if column exists in table."""
        return column.lower() in {c.lower() for c in self.columns}

    def get_column_names(self) -> List[str]:
        """Get list of column names."""
        return list(self.columns.keys())

    def get_columns_of_type(self, data_type: str) -> List[str]:
        """Get columns matching a data type pattern."""
        data_type_lower = data_type.lower()
        return [
            name for name, info in self.columns.items()
            if data_type_lower in info.data_type.lower()
        ]


@dataclass
class DataCoverage:
    """Data coverage statistics for a column."""
    column: str
    total_rows: int
    non_null_count: int
    null_count: int
    coverage_pct: float
    distinct_count: Optional[int] = None

    @property
    def is_sparse(self) -> bool:
        """Column is considered sparse if < 50% populated."""
        return self.coverage_pct < 50.0

    @property
    def is_mostly_null(self) -> bool:
        """Column is mostly null if < 10% populated."""
        return self.coverage_pct < 10.0


class SchemaInspector:
    """
    Runtime schema discovery for the MAUDE database.

    Provides methods to:
    - Discover table structures dynamically
    - Check column existence before querying
    - Analyze data coverage and sparsity
    - Discover enum/categorical values
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        """
        Initialize schema inspector.

        Args:
            conn: Active DuckDB connection
        """
        self.conn = conn
        self._schema_cache: Dict[str, TableSchema] = {}
        self._coverage_cache: Dict[str, Dict[str, DataCoverage]] = {}

    def get_tables(self) -> List[str]:
        """
        Get list of all tables in the database.

        Returns:
            List of table names
        """
        try:
            result = self.conn.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                ORDER BY table_name
            """).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []

    def get_table_schema(self, table: str, use_cache: bool = True) -> Optional[TableSchema]:
        """
        Get schema information for a table.

        Args:
            table: Table name
            use_cache: Whether to use cached schema info

        Returns:
            TableSchema object or None if table doesn't exist
        """
        if use_cache and table in self._schema_cache:
            return self._schema_cache[table]

        try:
            # Get column info using DESCRIBE
            result = self.conn.execute(f"DESCRIBE {table}").fetchall()

            columns = {}
            for row in result:
                col_name = row[0]
                col_type = row[1]
                nullable = row[2] == "YES" if len(row) > 2 else True
                default_val = row[3] if len(row) > 3 else None
                is_pk = row[4] == "PRI" if len(row) > 4 else False

                columns[col_name] = ColumnInfo(
                    name=col_name,
                    data_type=col_type,
                    nullable=nullable,
                    default_value=default_val,
                    is_primary_key=is_pk
                )

            # Get row count
            count_result = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            row_count = count_result[0] if count_result else 0

            schema = TableSchema(name=table, columns=columns, row_count=row_count)
            self._schema_cache[table] = schema

            logger.debug(f"Discovered schema for {table}: {len(columns)} columns, {row_count} rows")
            return schema

        except Exception as e:
            logger.error(f"Error getting schema for {table}: {e}")
            return None

    def validate_column_exists(self, table: str, column: str) -> bool:
        """
        Check if a column exists in a table.

        Args:
            table: Table name
            column: Column name

        Returns:
            True if column exists
        """
        schema = self.get_table_schema(table)
        if schema is None:
            return False
        return schema.has_column(column)

    def validate_columns_exist(self, table: str, columns: List[str]) -> Dict[str, bool]:
        """
        Check which columns exist in a table.

        Args:
            table: Table name
            columns: List of column names to check

        Returns:
            Dict mapping column name to existence boolean
        """
        schema = self.get_table_schema(table)
        if schema is None:
            return {col: False for col in columns}

        return {col: schema.has_column(col) for col in columns}

    def get_available_columns(self, table: str, requested: List[str]) -> List[str]:
        """
        Filter requested columns to only those that exist.

        Args:
            table: Table name
            requested: List of desired columns

        Returns:
            List of columns that actually exist
        """
        schema = self.get_table_schema(table)
        if schema is None:
            return []

        available = []
        for col in requested:
            if schema.has_column(col):
                available.append(col)
            else:
                logger.warning(f"Column '{col}' not found in table '{table}'")

        return available

    def get_data_coverage(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        sample_size: Optional[int] = None,
        use_cache: bool = True
    ) -> Dict[str, DataCoverage]:
        """
        Analyze data coverage (% populated) for columns.

        Args:
            table: Table name
            columns: Specific columns to analyze (None = all)
            sample_size: Sample rows for large tables (None = all)
            use_cache: Whether to use cached coverage data

        Returns:
            Dict mapping column name to DataCoverage
        """
        cache_key = f"{table}:{sample_size}"
        if use_cache and cache_key in self._coverage_cache:
            cached = self._coverage_cache[cache_key]
            if columns:
                return {c: cached[c] for c in columns if c in cached}
            return cached

        schema = self.get_table_schema(table)
        if schema is None:
            return {}

        cols_to_check = columns or schema.get_column_names()
        cols_to_check = [c for c in cols_to_check if schema.has_column(c)]

        if not cols_to_check:
            return {}

        # Build coverage query
        coverage_parts = []
        for col in cols_to_check:
            coverage_parts.append(f"COUNT({col}) as count_{col}")
            coverage_parts.append(f"COUNT(DISTINCT {col}) as distinct_{col}")

        sample_clause = ""
        if sample_size:
            sample_clause = f"USING SAMPLE {sample_size}"

        try:
            query = f"""
                SELECT
                    COUNT(*) as total_rows,
                    {', '.join(coverage_parts)}
                FROM {table}
                {sample_clause}
            """
            result = self.conn.execute(query).fetchone()

            if not result:
                return {}

            total_rows = result[0]
            coverage = {}

            for i, col in enumerate(cols_to_check):
                non_null_count = result[1 + i * 2]
                distinct_count = result[2 + i * 2]
                null_count = total_rows - non_null_count
                coverage_pct = (non_null_count / total_rows * 100) if total_rows > 0 else 0

                coverage[col] = DataCoverage(
                    column=col,
                    total_rows=total_rows,
                    non_null_count=non_null_count,
                    null_count=null_count,
                    coverage_pct=coverage_pct,
                    distinct_count=distinct_count
                )

            self._coverage_cache[cache_key] = coverage
            return coverage

        except Exception as e:
            logger.error(f"Error analyzing coverage for {table}: {e}")
            return {}

    def discover_enum_values(
        self,
        table: str,
        column: str,
        limit: int = 100,
        min_count: int = 1,
        order_by_count: bool = True
    ) -> List[Tuple[Any, int]]:
        """
        Discover distinct values for a column (useful for filters/dropdowns).

        Args:
            table: Table name
            column: Column to analyze
            limit: Maximum values to return
            min_count: Minimum occurrence count to include
            order_by_count: Order by frequency (True) or value (False)

        Returns:
            List of (value, count) tuples
        """
        if not self.validate_column_exists(table, column):
            logger.warning(f"Column '{column}' not found in table '{table}'")
            return []

        order_clause = "cnt DESC" if order_by_count else f"{column}"

        try:
            query = f"""
                SELECT {column}, COUNT(*) as cnt
                FROM {table}
                WHERE {column} IS NOT NULL
                GROUP BY {column}
                HAVING COUNT(*) >= {min_count}
                ORDER BY {order_clause}
                LIMIT {limit}
            """
            result = self.conn.execute(query).fetchall()
            return [(row[0], row[1]) for row in result]

        except Exception as e:
            logger.error(f"Error discovering enum values for {table}.{column}: {e}")
            return []

    def get_top_values(
        self,
        table: str,
        column: str,
        n: int = 10
    ) -> List[Tuple[Any, int]]:
        """
        Get top N most frequent values for a column.

        Args:
            table: Table name
            column: Column to analyze
            n: Number of top values to return

        Returns:
            List of (value, count) tuples sorted by frequency
        """
        return self.discover_enum_values(table, column, limit=n, order_by_count=True)

    def get_date_range(self, table: str, column: str) -> Optional[Tuple[Any, Any]]:
        """
        Get min/max date range for a date column.

        Args:
            table: Table name
            column: Date column name

        Returns:
            Tuple of (min_date, max_date) or None
        """
        if not self.validate_column_exists(table, column):
            return None

        try:
            query = f"""
                SELECT MIN({column}), MAX({column})
                FROM {table}
                WHERE {column} IS NOT NULL
            """
            result = self.conn.execute(query).fetchone()
            if result and result[0] is not None:
                return (result[0], result[1])
            return None

        except Exception as e:
            logger.error(f"Error getting date range for {table}.{column}: {e}")
            return None

    def get_numeric_stats(
        self,
        table: str,
        column: str
    ) -> Optional[Dict[str, float]]:
        """
        Get basic statistics for a numeric column.

        Args:
            table: Table name
            column: Numeric column name

        Returns:
            Dict with min, max, avg, median, stddev
        """
        if not self.validate_column_exists(table, column):
            return None

        try:
            query = f"""
                SELECT
                    MIN({column}) as min_val,
                    MAX({column}) as max_val,
                    AVG({column}) as avg_val,
                    MEDIAN({column}) as median_val,
                    STDDEV({column}) as stddev_val
                FROM {table}
                WHERE {column} IS NOT NULL
            """
            result = self.conn.execute(query).fetchone()
            if result:
                return {
                    "min": result[0],
                    "max": result[1],
                    "avg": result[2],
                    "median": result[3],
                    "stddev": result[4]
                }
            return None

        except Exception as e:
            logger.error(f"Error getting numeric stats for {table}.{column}: {e}")
            return None

    def get_sparse_columns(self, table: str, threshold: float = 50.0) -> List[str]:
        """
        Get columns with coverage below threshold percentage.

        Args:
            table: Table name
            threshold: Coverage threshold (default 50%)

        Returns:
            List of sparse column names
        """
        coverage = self.get_data_coverage(table)
        return [
            col for col, cov in coverage.items()
            if cov.coverage_pct < threshold
        ]

    def get_well_populated_columns(self, table: str, threshold: float = 80.0) -> List[str]:
        """
        Get columns with coverage above threshold percentage.

        Args:
            table: Table name
            threshold: Coverage threshold (default 80%)

        Returns:
            List of well-populated column names
        """
        coverage = self.get_data_coverage(table)
        return [
            col for col, cov in coverage.items()
            if cov.coverage_pct >= threshold
        ]

    def clear_cache(self) -> None:
        """Clear all cached schema and coverage data."""
        self._schema_cache.clear()
        self._coverage_cache.clear()
        logger.debug("Schema inspector cache cleared")

    def get_table_summary(self, table: str) -> Dict[str, Any]:
        """
        Get a comprehensive summary of a table.

        Args:
            table: Table name

        Returns:
            Dict with table metadata and column statistics
        """
        schema = self.get_table_schema(table)
        if schema is None:
            return {"error": f"Table '{table}' not found"}

        coverage = self.get_data_coverage(table)

        # Categorize columns by coverage
        well_populated = []
        sparse = []
        mostly_null = []

        for col, cov in coverage.items():
            if cov.coverage_pct >= 80:
                well_populated.append(col)
            elif cov.coverage_pct >= 10:
                sparse.append(col)
            else:
                mostly_null.append(col)

        return {
            "table": table,
            "row_count": schema.row_count,
            "column_count": len(schema.columns),
            "columns": schema.get_column_names(),
            "well_populated_columns": well_populated,
            "sparse_columns": sparse,
            "mostly_null_columns": mostly_null,
            "coverage_summary": {
                col: round(cov.coverage_pct, 1)
                for col, cov in coverage.items()
            }
        }


# Convenience function for quick schema checks
def get_inspector(conn: duckdb.DuckDBPyConnection) -> SchemaInspector:
    """
    Get a SchemaInspector instance.

    Args:
        conn: DuckDB connection

    Returns:
        SchemaInspector instance
    """
    return SchemaInspector(conn)
