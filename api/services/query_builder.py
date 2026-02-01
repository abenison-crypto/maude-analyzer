"""
Schema-Aware Query Builder for MAUDE Analyzer.

This module provides a SQL query builder that is aware of the schema registry,
ensuring all queries use validated column names and proper type handling.

All SQL queries should be built through this module to ensure:
- Column names are validated against the schema registry
- Event type codes are properly converted (I -> IN)
- Missing columns are handled gracefully
- Consistent query patterns across the codebase
"""

from dataclasses import dataclass, field
from datetime import date
from typing import Optional, List, Tuple, Dict, Any, Union

from config.unified_schema import (
    get_schema_registry,
    UnifiedSchemaRegistry,
    EVENT_TYPES,
)


@dataclass
class QueryColumn:
    """Represents a column in a query with optional alias."""
    name: str
    alias: Optional[str] = None
    table_alias: Optional[str] = None
    aggregate: Optional[str] = None  # COUNT, SUM, AVG, etc.
    distinct: bool = False

    def to_sql(self) -> str:
        """Convert to SQL column expression."""
        col = self.name
        if self.table_alias:
            col = f"{self.table_alias}.{col}"

        if self.aggregate:
            if self.distinct:
                col = f"{self.aggregate}(DISTINCT {col})"
            else:
                col = f"{self.aggregate}({col})"

        if self.alias:
            col = f"{col} as {self.alias}"

        return col


@dataclass
class QueryCondition:
    """Represents a WHERE condition."""
    column: str
    operator: str
    value: Any
    table_alias: Optional[str] = None
    is_list: bool = False

    def to_sql(self) -> Tuple[str, List[Any]]:
        """Convert to SQL condition with parameter placeholders."""
        col = self.column
        if self.table_alias:
            col = f"{self.table_alias}.{col}"

        if self.is_list and isinstance(self.value, (list, tuple)):
            placeholders = ", ".join(["?" for _ in self.value])
            return f"{col} {self.operator} ({placeholders})", list(self.value)

        return f"{col} {self.operator} ?", [self.value]


class SchemaAwareQueryBuilder:
    """
    SQL query builder that validates columns against the schema registry.

    Usage:
        builder = SchemaAwareQueryBuilder()
        query, params = (
            builder
            .select("master_events", ["mdr_report_key", "event_type", "date_received"])
            .alias("m")
            .where_equal("event_type", "D")
            .where_in("product_code", ["ABC", "DEF"])
            .where_date_range("date_received", date_from, date_to)
            .order_by("date_received", desc=True)
            .limit(100)
            .build()
        )
    """

    def __init__(self, registry: Optional[UnifiedSchemaRegistry] = None):
        self.registry = registry or get_schema_registry()
        self._reset()

    def _reset(self):
        """Reset builder state for new query."""
        self._table: Optional[str] = None
        self._table_alias: Optional[str] = None
        self._columns: List[QueryColumn] = []
        self._conditions: List[QueryCondition] = []
        self._condition_strings: List[str] = []
        self._params: List[Any] = []
        self._joins: List[str] = []
        self._group_by: List[str] = []
        self._having: List[str] = []
        self._order_by: List[str] = []
        self._limit_val: Optional[int] = None
        self._offset_val: Optional[int] = None

    def select(self, table: str, columns: List[str],
               validate: bool = True) -> "SchemaAwareQueryBuilder":
        """
        Start a SELECT query on a table.

        Args:
            table: Table name
            columns: List of column names to select
            validate: Whether to validate columns exist (default True)

        Returns:
            Self for chaining
        """
        self._reset()
        self._table = table

        if validate:
            available = self.registry.get_available_columns(table, columns)
            for col in available:
                self._columns.append(QueryColumn(name=col))
        else:
            for col in columns:
                self._columns.append(QueryColumn(name=col))

        return self

    def alias(self, table_alias: str) -> "SchemaAwareQueryBuilder":
        """Set table alias."""
        self._table_alias = table_alias
        for col in self._columns:
            col.table_alias = table_alias
        return self

    def add_column(self, name: str, alias: Optional[str] = None,
                   aggregate: Optional[str] = None,
                   distinct: bool = False) -> "SchemaAwareQueryBuilder":
        """Add a column to the select list."""
        self._columns.append(QueryColumn(
            name=name,
            alias=alias,
            table_alias=self._table_alias,
            aggregate=aggregate,
            distinct=distinct,
        ))
        return self

    def add_count(self, column: str = "*", alias: str = "count",
                  distinct: bool = False) -> "SchemaAwareQueryBuilder":
        """Add COUNT aggregate."""
        self._columns.append(QueryColumn(
            name=column,
            alias=alias,
            table_alias=self._table_alias if column != "*" else None,
            aggregate="COUNT",
            distinct=distinct,
        ))
        return self

    def add_case_count(self, condition_column: str, condition_value: str,
                       alias: str) -> "SchemaAwareQueryBuilder":
        """Add COUNT(CASE WHEN ... THEN 1 END) for conditional counting."""
        table_prefix = f"{self._table_alias}." if self._table_alias else ""
        case_expr = f"COUNT(CASE WHEN {table_prefix}{condition_column} = '{condition_value}' THEN 1 END)"
        self._columns.append(QueryColumn(name=case_expr, alias=alias))
        return self

    def join(self, join_table: str, join_alias: str,
             on_left: str, on_right: str,
             join_type: str = "JOIN") -> "SchemaAwareQueryBuilder":
        """Add a JOIN clause."""
        left_col = f"{self._table_alias}.{on_left}" if self._table_alias else on_left
        right_col = f"{join_alias}.{on_right}"
        self._joins.append(f"{join_type} {join_table} {join_alias} ON {left_col} = {right_col}")
        return self

    # -------------------------------------------------------------------------
    # WHERE Conditions
    # -------------------------------------------------------------------------

    def where(self, sql: str, params: Optional[List[Any]] = None) -> "SchemaAwareQueryBuilder":
        """Add raw WHERE condition."""
        self._condition_strings.append(sql)
        if params:
            self._params.extend(params)
        return self

    def where_equal(self, column: str, value: Any,
                    table_alias: Optional[str] = None) -> "SchemaAwareQueryBuilder":
        """Add WHERE column = value condition."""
        alias = table_alias or self._table_alias
        self._conditions.append(QueryCondition(
            column=column,
            operator="=",
            value=value,
            table_alias=alias,
        ))
        return self

    def where_not_equal(self, column: str, value: Any,
                        table_alias: Optional[str] = None) -> "SchemaAwareQueryBuilder":
        """Add WHERE column != value condition."""
        alias = table_alias or self._table_alias
        self._conditions.append(QueryCondition(
            column=column,
            operator="!=",
            value=value,
            table_alias=alias,
        ))
        return self

    def where_in(self, column: str, values: List[Any],
                 table_alias: Optional[str] = None) -> "SchemaAwareQueryBuilder":
        """Add WHERE column IN (...) condition."""
        if not values:
            return self

        alias = table_alias or self._table_alias
        self._conditions.append(QueryCondition(
            column=column,
            operator="IN",
            value=values,
            table_alias=alias,
            is_list=True,
        ))
        return self

    def where_like(self, column: str, pattern: str,
                   case_insensitive: bool = True,
                   table_alias: Optional[str] = None) -> "SchemaAwareQueryBuilder":
        """Add WHERE column LIKE pattern condition."""
        alias = table_alias or self._table_alias
        col = f"{alias}.{column}" if alias else column

        if case_insensitive:
            sql = f"LOWER({col}) LIKE ?"
            self._condition_strings.append(sql)
            self._params.append(pattern.lower())
        else:
            sql = f"{col} LIKE ?"
            self._condition_strings.append(sql)
            self._params.append(pattern)

        return self

    def where_not_null(self, column: str,
                       table_alias: Optional[str] = None) -> "SchemaAwareQueryBuilder":
        """Add WHERE column IS NOT NULL condition."""
        alias = table_alias or self._table_alias
        col = f"{alias}.{column}" if alias else column
        self._condition_strings.append(f"{col} IS NOT NULL")
        return self

    def where_null(self, column: str,
                   table_alias: Optional[str] = None) -> "SchemaAwareQueryBuilder":
        """Add WHERE column IS NULL condition."""
        alias = table_alias or self._table_alias
        col = f"{alias}.{column}" if alias else column
        self._condition_strings.append(f"{col} IS NULL")
        return self

    def where_date_range(self, column: str,
                         date_from: Optional[date] = None,
                         date_to: Optional[date] = None,
                         table_alias: Optional[str] = None) -> "SchemaAwareQueryBuilder":
        """Add WHERE date column between dates."""
        alias = table_alias or self._table_alias
        col = f"{alias}.{column}" if alias else column

        if date_from:
            self._condition_strings.append(f"{col} >= ?")
            self._params.append(date_from.isoformat() if hasattr(date_from, 'isoformat') else str(date_from))

        if date_to:
            self._condition_strings.append(f"{col} <= ?")
            self._params.append(date_to.isoformat() if hasattr(date_to, 'isoformat') else str(date_to))

        return self

    def where_event_types(self, event_types: List[str],
                          column: str = "event_type",
                          table_alias: Optional[str] = None) -> "SchemaAwareQueryBuilder":
        """
        Add WHERE event_type IN (...) with automatic code conversion.

        Converts filter codes (I) to database codes (IN).

        Args:
            event_types: List of event type codes (filter or DB codes)
            column: Event type column name (default "event_type")
            table_alias: Optional table alias
        """
        if not event_types:
            return self

        # Convert filter codes to database codes
        db_codes = self.registry.convert_filter_event_types(event_types)

        return self.where_in(column, db_codes, table_alias)

    # -------------------------------------------------------------------------
    # GROUP BY, HAVING, ORDER BY, LIMIT
    # -------------------------------------------------------------------------

    def group_by(self, *columns: str) -> "SchemaAwareQueryBuilder":
        """Add GROUP BY clause."""
        for col in columns:
            if self._table_alias and "." not in col:
                self._group_by.append(f"{self._table_alias}.{col}")
            else:
                self._group_by.append(col)
        return self

    def having(self, sql: str, params: Optional[List[Any]] = None) -> "SchemaAwareQueryBuilder":
        """Add HAVING clause."""
        self._having.append(sql)
        if params:
            self._params.extend(params)
        return self

    def order_by(self, column: str, desc: bool = False,
                 table_alias: Optional[str] = None) -> "SchemaAwareQueryBuilder":
        """Add ORDER BY clause."""
        alias = table_alias or self._table_alias
        col = f"{alias}.{column}" if alias and "." not in column else column
        direction = "DESC" if desc else "ASC"
        self._order_by.append(f"{col} {direction}")
        return self

    def limit(self, limit: int) -> "SchemaAwareQueryBuilder":
        """Add LIMIT clause."""
        self._limit_val = limit
        return self

    def offset(self, offset: int) -> "SchemaAwareQueryBuilder":
        """Add OFFSET clause."""
        self._offset_val = offset
        return self

    def paginate(self, page: int, page_size: int) -> "SchemaAwareQueryBuilder":
        """Add pagination (LIMIT and OFFSET)."""
        self._limit_val = page_size
        self._offset_val = (page - 1) * page_size
        return self

    # -------------------------------------------------------------------------
    # Build Methods
    # -------------------------------------------------------------------------

    def build(self) -> Tuple[str, List[Any]]:
        """
        Build the final SQL query and parameters.

        Returns:
            Tuple of (sql_query, parameters)
        """
        if not self._table:
            raise ValueError("No table specified. Call select() first.")

        # Build SELECT clause
        if self._columns:
            select_cols = ", ".join(col.to_sql() for col in self._columns)
        else:
            select_cols = "*"

        # Build FROM clause
        table_clause = self._table
        if self._table_alias:
            table_clause = f"{self._table} {self._table_alias}"

        # Build query parts
        parts = [f"SELECT {select_cols}", f"FROM {table_clause}"]

        # Add JOINs
        if self._joins:
            parts.extend(self._joins)

        # Build WHERE clause
        all_conditions = []
        all_params = []

        for cond in self._conditions:
            sql, params = cond.to_sql()
            all_conditions.append(sql)
            all_params.extend(params)

        all_conditions.extend(self._condition_strings)
        all_params.extend(self._params)

        if all_conditions:
            parts.append(f"WHERE {' AND '.join(all_conditions)}")

        # Add GROUP BY
        if self._group_by:
            parts.append(f"GROUP BY {', '.join(self._group_by)}")

        # Add HAVING
        if self._having:
            parts.append(f"HAVING {' AND '.join(self._having)}")

        # Add ORDER BY
        if self._order_by:
            parts.append(f"ORDER BY {', '.join(self._order_by)}")

        # Add LIMIT/OFFSET
        if self._limit_val is not None:
            parts.append(f"LIMIT {self._limit_val}")
        if self._offset_val is not None:
            parts.append(f"OFFSET {self._offset_val}")

        return "\n".join(parts), all_params

    def build_count(self) -> Tuple[str, List[Any]]:
        """
        Build a COUNT(*) query using the same conditions.

        Returns:
            Tuple of (count_query, parameters)
        """
        if not self._table:
            raise ValueError("No table specified. Call select() first.")

        # Build FROM clause
        table_clause = self._table
        if self._table_alias:
            table_clause = f"{self._table} {self._table_alias}"

        parts = [f"SELECT COUNT(*) as count", f"FROM {table_clause}"]

        # Add JOINs
        if self._joins:
            parts.extend(self._joins)

        # Build WHERE clause
        all_conditions = []
        all_params = []

        for cond in self._conditions:
            sql, params = cond.to_sql()
            all_conditions.append(sql)
            all_params.extend(params)

        all_conditions.extend(self._condition_strings)
        all_params.extend(self._params)

        if all_conditions:
            parts.append(f"WHERE {' AND '.join(all_conditions)}")

        return "\n".join(parts), all_params


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def build_event_stats_query(
    table_alias: str = "m",
    manufacturers: Optional[List[str]] = None,
    product_codes: Optional[List[str]] = None,
    event_types: Optional[List[str]] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> Tuple[str, List[Any]]:
    """
    Build a query to get event statistics (total, deaths, injuries, malfunctions).

    Returns:
        Tuple of (sql_query, parameters)
    """
    registry = get_schema_registry()

    builder = (
        SchemaAwareQueryBuilder(registry)
        .select("master_events", [], validate=False)
        .alias(table_alias)
        .add_count(alias="total")
    )

    # Add case counts for each event type
    for code, event_type in EVENT_TYPES.items():
        if code == "*":
            continue
        builder.add_case_count("event_type", code, event_type.name.lower() + "s" if code != "O" else "other")

    # Add filters
    if manufacturers:
        builder.where_in("manufacturer_clean", manufacturers)
    if product_codes:
        builder.where_in("product_code", product_codes)
    if event_types:
        builder.where_event_types(event_types)
    if date_from or date_to:
        builder.where_date_range("date_received", date_from, date_to)

    return builder.build()


def build_events_list_query(
    select_columns: List[str],
    table_alias: str = "m",
    manufacturers: Optional[List[str]] = None,
    product_codes: Optional[List[str]] = None,
    event_types: Optional[List[str]] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    search_text: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> Tuple[str, List[Any]]:
    """
    Build a query to get paginated event list.

    Returns:
        Tuple of (sql_query, parameters)
    """
    registry = get_schema_registry()

    builder = (
        SchemaAwareQueryBuilder(registry)
        .select("master_events", select_columns)
        .alias(table_alias)
    )

    # Add filters
    if manufacturers:
        builder.where_in("manufacturer_clean", manufacturers)
    if product_codes:
        builder.where_in("product_code", product_codes)
    if event_types:
        builder.where_event_types(event_types)
    if date_from or date_to:
        builder.where_date_range("date_received", date_from, date_to)
    if search_text:
        builder.where_like("manufacturer_clean", f"%{search_text}%")

    # Add ordering and pagination
    builder.order_by("date_received", desc=True)
    builder.paginate(page, page_size)

    return builder.build()


def validate_columns(table: str, columns: List[str]) -> Dict[str, bool]:
    """
    Validate which columns exist in a table.

    Args:
        table: Table name
        columns: List of column names to validate

    Returns:
        Dict mapping column name to existence boolean
    """
    registry = get_schema_registry()
    return registry.validate_columns_exist(table, columns)


def get_available_columns(table: str, requested: List[str]) -> List[str]:
    """
    Filter requested columns to only those that exist.

    Args:
        table: Table name
        requested: List of desired column names

    Returns:
        List of column names that exist in the table
    """
    registry = get_schema_registry()
    return registry.get_available_columns(table, requested)


def convert_event_types(filter_codes: List[str]) -> List[str]:
    """
    Convert filter event type codes to database codes.

    Args:
        filter_codes: List of filter codes (e.g., ["D", "I", "M"])

    Returns:
        List of database codes (e.g., ["D", "IN", "M"])
    """
    registry = get_schema_registry()
    return registry.convert_filter_event_types(filter_codes)
