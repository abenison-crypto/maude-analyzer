"""Filter building utilities for SQL queries."""

from typing import Optional
from datetime import date

from api.constants.columns import EVENT_TYPE_FILTER_MAPPING


def build_filter_clause(
    manufacturers: Optional[list[str]] = None,
    product_codes: Optional[list[str]] = None,
    event_types: Optional[list[str]] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    search_text: Optional[str] = None,
    table_alias: str = "m",
) -> tuple[str, list]:
    """Build WHERE clause and parameters for event filtering.

    Args:
        manufacturers: List of manufacturer names to filter by.
        product_codes: List of product codes to filter by.
        event_types: List of event types (D, I, M, O) to filter by.
        date_from: Start date for date range filter.
        date_to: End date for date range filter.
        search_text: Text to search in event narratives.
        table_alias: SQL table alias to use.

    Returns:
        Tuple of (WHERE clause string, list of parameters).
    """
    conditions = []
    params = []

    if manufacturers:
        placeholders = ", ".join(["?" for _ in manufacturers])
        conditions.append(f"{table_alias}.manufacturer_clean IN ({placeholders})")
        params.extend(manufacturers)

    if product_codes:
        placeholders = ", ".join(["?" for _ in product_codes])
        conditions.append(f"{table_alias}.product_code IN ({placeholders})")
        params.extend(product_codes)

    if event_types:
        # Map filter codes to database codes (e.g., I -> IN for injury)
        db_types = [EVENT_TYPE_FILTER_MAPPING.get(t, t) for t in event_types]
        placeholders = ", ".join(["?" for _ in db_types])
        conditions.append(f"{table_alias}.event_type IN ({placeholders})")
        params.extend(db_types)

    if date_from:
        conditions.append(f"{table_alias}.date_received >= ?")
        params.append(date_from.isoformat())

    if date_to:
        conditions.append(f"{table_alias}.date_received <= ?")
        params.append(date_to.isoformat())

    if search_text:
        conditions.append(f"""
            EXISTS (
                SELECT 1 FROM mdr_text t
                WHERE t.mdr_report_key = {table_alias}.mdr_report_key
                AND LOWER(t.text_content) LIKE ?
            )
        """)
        params.append(f"%{search_text.lower()}%")

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    return where_clause, params


def build_count_query(
    base_table: str = "master_events",
    where_clause: str = "1=1",
    table_alias: str = "m",
) -> str:
    """Build a COUNT query.

    Args:
        base_table: Table to query.
        where_clause: WHERE conditions.
        table_alias: Table alias.

    Returns:
        SQL query string.
    """
    return f"SELECT COUNT(*) FROM {base_table} {table_alias} WHERE {where_clause}"


def build_paginated_query(
    select_clause: str,
    base_table: str = "master_events",
    where_clause: str = "1=1",
    order_by: str = "date_received DESC",
    table_alias: str = "m",
    page: int = 1,
    page_size: int = 50,
) -> str:
    """Build a paginated SELECT query.

    Args:
        select_clause: Columns to select.
        base_table: Table to query.
        where_clause: WHERE conditions.
        order_by: ORDER BY clause.
        table_alias: Table alias.
        page: Page number (1-indexed).
        page_size: Results per page.

    Returns:
        SQL query string.
    """
    offset = (page - 1) * page_size
    return f"""
        SELECT {select_clause}
        FROM {base_table} {table_alias}
        WHERE {where_clause}
        ORDER BY {order_by}
        LIMIT {page_size} OFFSET {offset}
    """
