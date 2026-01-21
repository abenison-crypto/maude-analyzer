"""Search and analysis queries for MAUDE data."""

import duckdb
import pandas as pd
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import date, datetime
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import config
from config.logging_config import get_logger
from src.database import get_connection

logger = get_logger("queries")


# Search operators mapping
OPERATORS = {
    "equals": "= ?",
    "not_equals": "!= ?",
    "contains": "ILIKE ?",
    "starts_with": "ILIKE ?",
    "ends_with": "ILIKE ?",
    "in": "IN",
    "not_in": "NOT IN",
    "between": "BETWEEN ? AND ?",
    "greater_than": "> ?",
    "less_than": "< ?",
    "greater_equal": ">= ?",
    "less_equal": "<= ?",
    "is_null": "IS NULL",
    "is_not_null": "IS NOT NULL",
}


@dataclass
class SearchCondition:
    """A single search condition."""

    field: str
    operator: str
    value: Any = None
    value2: Any = None  # For 'between' operator

    def to_sql(self) -> Tuple[str, List[Any]]:
        """Convert condition to SQL clause and parameters."""
        params = []

        if self.operator == "is_null":
            return f"{self.field} IS NULL", []
        elif self.operator == "is_not_null":
            return f"{self.field} IS NOT NULL", []
        elif self.operator == "contains":
            return f"{self.field} ILIKE ?", [f"%{self.value}%"]
        elif self.operator == "starts_with":
            return f"{self.field} ILIKE ?", [f"{self.value}%"]
        elif self.operator == "ends_with":
            return f"{self.field} ILIKE ?", [f"%{self.value}"]
        elif self.operator == "in":
            if not isinstance(self.value, (list, tuple)):
                self.value = [self.value]
            placeholders = ", ".join(["?" for _ in self.value])
            return f"{self.field} IN ({placeholders})", list(self.value)
        elif self.operator == "not_in":
            if not isinstance(self.value, (list, tuple)):
                self.value = [self.value]
            placeholders = ", ".join(["?" for _ in self.value])
            return f"{self.field} NOT IN ({placeholders})", list(self.value)
        elif self.operator == "between":
            return f"{self.field} BETWEEN ? AND ?", [self.value, self.value2]
        else:
            # Standard operators: equals, not_equals, greater_than, etc.
            op_sql = OPERATORS.get(self.operator, "= ?")
            return f"{self.field} {op_sql}", [self.value]


@dataclass
class SearchQuery:
    """Build and execute complex search queries."""

    conditions: List[SearchCondition] = field(default_factory=list)
    sort_by: str = "date_received"
    sort_order: str = "DESC"
    limit: int = 1000
    offset: int = 0
    include_devices: bool = False
    include_patients: bool = False
    include_text: bool = False

    def add_condition(
        self,
        field: str,
        operator: str,
        value: Any = None,
        value2: Any = None,
    ) -> "SearchQuery":
        """Add a search condition."""
        self.conditions.append(
            SearchCondition(field=field, operator=operator, value=value, value2=value2)
        )
        return self

    def add_date_range(
        self,
        field: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> "SearchQuery":
        """Add a date range condition."""
        if start_date and end_date:
            self.add_condition(field, "between", start_date, end_date)
        elif start_date:
            self.add_condition(field, "greater_equal", start_date)
        elif end_date:
            self.add_condition(field, "less_equal", end_date)
        return self

    def add_manufacturers(self, manufacturers: List[str]) -> "SearchQuery":
        """Add manufacturer filter."""
        if manufacturers:
            self.add_condition("manufacturer_clean", "in", manufacturers)
        return self

    def add_product_codes(self, codes: List[str]) -> "SearchQuery":
        """Add product code filter."""
        if codes:
            self.add_condition("product_code", "in", codes)
        return self

    def add_event_types(self, event_types: List[str]) -> "SearchQuery":
        """Add event type filter."""
        if event_types:
            self.add_condition("event_type", "in", event_types)
        return self

    def add_text_search(self, search_text: str) -> "SearchQuery":
        """Add full-text search on narratives."""
        if search_text:
            self.include_text = True
            self.add_condition("text_content", "contains", search_text)
        return self

    def clear(self) -> "SearchQuery":
        """Clear all conditions."""
        self.conditions = []
        return self

    def build_sql(self) -> Tuple[str, List[Any]]:
        """Generate SQL query and parameters."""
        params = []

        # Base SELECT
        select_fields = """
            m.mdr_report_key,
            m.event_key,
            m.report_number,
            m.date_received,
            m.date_of_event,
            m.manufacturer_name,
            m.manufacturer_clean,
            m.product_code,
            m.event_type,
            m.type_of_report,
            m.product_problem_flag,
            m.adverse_event_flag,
            m.report_source_code,
            m.event_location,
            m.pma_pmn_number,
            m.received_year,
            m.received_month
        """

        from_clause = "FROM master_events m"
        joins = []

        # Add text join if needed
        if self.include_text or any(c.field == "text_content" for c in self.conditions):
            joins.append("LEFT JOIN mdr_text t ON m.mdr_report_key = t.mdr_report_key")
            select_fields += ", t.text_content, t.text_type_code"

        # Build WHERE clause
        where_clauses = []
        for condition in self.conditions:
            sql, cond_params = condition.to_sql()
            # Handle table prefixes
            if condition.field == "text_content":
                sql = sql.replace("text_content", "t.text_content")
            elif not sql.startswith("t."):
                sql = "m." + sql
            where_clauses.append(sql)
            params.extend(cond_params)

        # Assemble query
        sql = f"SELECT {select_fields}\n{from_clause}"

        if joins:
            sql += "\n" + "\n".join(joins)

        if where_clauses:
            sql += "\nWHERE " + " AND ".join(where_clauses)

        # Sort
        sort_field = f"m.{self.sort_by}" if not self.sort_by.startswith("t.") else self.sort_by
        sql += f"\nORDER BY {sort_field} {self.sort_order}"

        # Pagination
        sql += f"\nLIMIT {self.limit} OFFSET {self.offset}"

        return sql, params

    def build_count_sql(self) -> Tuple[str, List[Any]]:
        """Generate COUNT query and parameters."""
        params = []

        from_clause = "FROM master_events m"
        joins = []

        if self.include_text or any(c.field == "text_content" for c in self.conditions):
            joins.append("LEFT JOIN mdr_text t ON m.mdr_report_key = t.mdr_report_key")

        # Build WHERE clause
        where_clauses = []
        for condition in self.conditions:
            sql, cond_params = condition.to_sql()
            if condition.field == "text_content":
                sql = sql.replace("text_content", "t.text_content")
            elif not sql.startswith("t."):
                sql = "m." + sql
            where_clauses.append(sql)
            params.extend(cond_params)

        sql = f"SELECT COUNT(DISTINCT m.mdr_report_key)\n{from_clause}"

        if joins:
            sql += "\n" + "\n".join(joins)

        if where_clauses:
            sql += "\nWHERE " + " AND ".join(where_clauses)

        return sql, params

    def execute(
        self, conn: Optional[duckdb.DuckDBPyConnection] = None
    ) -> pd.DataFrame:
        """Execute query and return results as DataFrame."""
        sql, params = self.build_sql()
        logger.debug(f"Executing query: {sql}")
        logger.debug(f"Parameters: {params}")

        own_connection = conn is None
        if own_connection:
            conn = duckdb.connect(str(config.database.path))

        try:
            result = conn.execute(sql, params).fetchdf()
            return result
        finally:
            if own_connection:
                conn.close()

    def count(self, conn: Optional[duckdb.DuckDBPyConnection] = None) -> int:
        """Get count of matching records."""
        sql, params = self.build_count_sql()

        own_connection = conn is None
        if own_connection:
            conn = duckdb.connect(str(config.database.path))

        try:
            result = conn.execute(sql, params).fetchone()
            return result[0] if result else 0
        finally:
            if own_connection:
                conn.close()


def get_mdr_summary(
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> Dict[str, Any]:
    """Get high-level MDR summary statistics."""
    own_connection = conn is None
    if own_connection:
        conn = duckdb.connect(str(config.database.path))

    try:
        result = conn.execute("""
            SELECT
                COUNT(*) as total_mdrs,
                COUNT(*) FILTER (WHERE event_type = 'D') as deaths,
                COUNT(*) FILTER (WHERE event_type = 'IN') as injuries,
                COUNT(*) FILTER (WHERE event_type = 'M') as malfunctions,
                COUNT(DISTINCT manufacturer_clean) as unique_manufacturers,
                COUNT(DISTINCT product_code) as unique_product_codes,
                MIN(date_received) as earliest_date,
                MAX(date_received) as latest_date
            FROM master_events
        """).fetchone()

        return {
            "total_mdrs": result[0] or 0,
            "deaths": result[1] or 0,
            "injuries": result[2] or 0,
            "malfunctions": result[3] or 0,
            "unique_manufacturers": result[4] or 0,
            "unique_product_codes": result[5] or 0,
            "earliest_date": result[6],
            "latest_date": result[7],
        }
    finally:
        if own_connection:
            conn.close()


def get_manufacturer_comparison(
    manufacturers: Optional[List[str]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> pd.DataFrame:
    """Get comparison metrics across manufacturers."""
    own_connection = conn is None
    if own_connection:
        conn = duckdb.connect(str(config.database.path))

    try:
        params = []
        where_clauses = []

        if manufacturers:
            placeholders = ", ".join(["?" for _ in manufacturers])
            where_clauses.append(f"manufacturer_clean IN ({placeholders})")
            params.extend(manufacturers)

        if start_date:
            where_clauses.append("date_received >= ?")
            params.append(start_date)

        if end_date:
            where_clauses.append("date_received <= ?")
            params.append(end_date)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        sql = f"""
            SELECT
                manufacturer_clean,
                COUNT(*) as total_mdrs,
                COUNT(*) FILTER (WHERE event_type = 'D') as deaths,
                COUNT(*) FILTER (WHERE event_type = 'IN') as injuries,
                COUNT(*) FILTER (WHERE event_type = 'M') as malfunctions,
                ROUND(COUNT(*) FILTER (WHERE event_type = 'D') * 100.0 / NULLIF(COUNT(*), 0), 2) as death_rate,
                MIN(date_received) as first_report,
                MAX(date_received) as last_report
            FROM master_events
            {where_sql}
            GROUP BY manufacturer_clean
            ORDER BY total_mdrs DESC
        """

        return conn.execute(sql, params).fetchdf()
    finally:
        if own_connection:
            conn.close()


def get_trend_data(
    aggregation: str = "monthly",
    manufacturers: Optional[List[str]] = None,
    product_codes: Optional[List[str]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> pd.DataFrame:
    """Get time series trend data."""
    own_connection = conn is None
    if own_connection:
        conn = duckdb.connect(str(config.database.path))

    try:
        # Determine date truncation
        trunc_map = {
            "daily": "day",
            "weekly": "week",
            "monthly": "month",
            "quarterly": "quarter",
            "yearly": "year",
        }
        trunc = trunc_map.get(aggregation, "month")

        params = []
        where_clauses = []

        if manufacturers:
            placeholders = ", ".join(["?" for _ in manufacturers])
            where_clauses.append(f"manufacturer_clean IN ({placeholders})")
            params.extend(manufacturers)

        if product_codes:
            placeholders = ", ".join(["?" for _ in product_codes])
            where_clauses.append(f"product_code IN ({placeholders})")
            params.extend(product_codes)

        if start_date:
            where_clauses.append("date_received >= ?")
            params.append(start_date)

        if end_date:
            where_clauses.append("date_received <= ?")
            params.append(end_date)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        sql = f"""
            SELECT
                DATE_TRUNC('{trunc}', date_received) as period,
                manufacturer_clean,
                COUNT(*) as total_mdrs,
                COUNT(*) FILTER (WHERE event_type = 'D') as deaths,
                COUNT(*) FILTER (WHERE event_type = 'IN') as injuries,
                COUNT(*) FILTER (WHERE event_type = 'M') as malfunctions
            FROM master_events
            {where_sql}
            GROUP BY DATE_TRUNC('{trunc}', date_received), manufacturer_clean
            ORDER BY period, manufacturer_clean
        """

        return conn.execute(sql, params).fetchdf()
    finally:
        if own_connection:
            conn.close()


def get_event_type_breakdown(
    manufacturers: Optional[List[str]] = None,
    product_codes: Optional[List[str]] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> pd.DataFrame:
    """Get event type breakdown by manufacturer."""
    own_connection = conn is None
    if own_connection:
        conn = duckdb.connect(str(config.database.path))

    try:
        params = []
        where_clauses = []

        if manufacturers:
            placeholders = ", ".join(["?" for _ in manufacturers])
            where_clauses.append(f"manufacturer_clean IN ({placeholders})")
            params.extend(manufacturers)

        if product_codes:
            placeholders = ", ".join(["?" for _ in product_codes])
            where_clauses.append(f"product_code IN ({placeholders})")
            params.extend(product_codes)

        if start_date:
            where_clauses.append("date_received >= ?")
            params.append(start_date)

        if end_date:
            where_clauses.append("date_received <= ?")
            params.append(end_date)

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        sql = f"""
            SELECT
                manufacturer_clean,
                event_type,
                COUNT(*) as count
            FROM master_events
            {where_sql}
            GROUP BY manufacturer_clean, event_type
            ORDER BY manufacturer_clean, event_type
        """

        return conn.execute(sql, params).fetchdf()
    finally:
        if own_connection:
            conn.close()


def get_record_detail(
    mdr_report_key: str,
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> Dict[str, Any]:
    """Get full detail for a single MDR record."""
    own_connection = conn is None
    if own_connection:
        conn = duckdb.connect(str(config.database.path))

    try:
        # Get master record
        master = conn.execute(
            "SELECT * FROM master_events WHERE mdr_report_key = ?",
            [mdr_report_key]
        ).fetchdf()

        if master.empty:
            return None

        # Get devices
        devices = conn.execute(
            "SELECT * FROM devices WHERE mdr_report_key = ?",
            [mdr_report_key]
        ).fetchdf()

        # Get patients
        patients = conn.execute(
            "SELECT * FROM patients WHERE mdr_report_key = ?",
            [mdr_report_key]
        ).fetchdf()

        # Get text
        text = conn.execute(
            "SELECT * FROM mdr_text WHERE mdr_report_key = ?",
            [mdr_report_key]
        ).fetchdf()

        # Get problems
        problems = conn.execute(
            "SELECT * FROM device_problems WHERE mdr_report_key = ?",
            [mdr_report_key]
        ).fetchdf()

        return {
            "master": master.to_dict("records")[0] if not master.empty else {},
            "devices": devices.to_dict("records"),
            "patients": patients.to_dict("records"),
            "text": text.to_dict("records"),
            "problems": problems.to_dict("records"),
        }
    finally:
        if own_connection:
            conn.close()


def get_filter_options(
    conn: Optional[duckdb.DuckDBPyConnection] = None,
) -> Dict[str, List[str]]:
    """Get available filter options from database."""
    own_connection = conn is None
    if own_connection:
        conn = duckdb.connect(str(config.database.path))

    try:
        manufacturers = conn.execute("""
            SELECT DISTINCT manufacturer_clean
            FROM master_events
            WHERE manufacturer_clean IS NOT NULL
            ORDER BY manufacturer_clean
        """).fetchdf()["manufacturer_clean"].tolist()

        product_codes = conn.execute("""
            SELECT DISTINCT product_code
            FROM master_events
            WHERE product_code IS NOT NULL
            ORDER BY product_code
        """).fetchdf()["product_code"].tolist()

        event_types = conn.execute("""
            SELECT DISTINCT event_type
            FROM master_events
            WHERE event_type IS NOT NULL
            ORDER BY event_type
        """).fetchdf()["event_type"].tolist()

        return {
            "manufacturers": manufacturers,
            "product_codes": product_codes,
            "event_types": event_types,
        }
    finally:
        if own_connection:
            conn.close()
