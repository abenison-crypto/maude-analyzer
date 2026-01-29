"""Filter building utilities for SQL queries."""

from typing import Optional
from datetime import date
from dataclasses import dataclass

from api.constants.columns import EVENT_TYPE_FILTER_MAPPING


@dataclass
class DeviceFilters:
    """Container for device-specific filter parameters."""
    brand_names: Optional[list[str]] = None
    generic_names: Optional[list[str]] = None
    device_manufacturers: Optional[list[str]] = None
    model_numbers: Optional[list[str]] = None
    implant_flag: Optional[str] = None  # 'Y', 'N', or None for any
    device_product_codes: Optional[list[str]] = None


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


def build_extended_filter_clause(
    manufacturers: Optional[list[str]] = None,
    product_codes: Optional[list[str]] = None,
    event_types: Optional[list[str]] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    search_text: Optional[str] = None,
    device_filters: Optional[DeviceFilters] = None,
    table_alias: str = "m",
) -> tuple[str, list]:
    """Build WHERE clause with extended device filter support.

    Uses EXISTS subqueries for device table filters to maintain performance
    without requiring a JOIN on the main query.

    Args:
        manufacturers: List of manufacturer names to filter by.
        product_codes: List of product codes to filter by.
        event_types: List of event types (D, I, M, O) to filter by.
        date_from: Start date for date range filter.
        date_to: End date for date range filter.
        search_text: Text to search in event narratives.
        device_filters: Device-specific filter parameters.
        table_alias: SQL table alias to use.

    Returns:
        Tuple of (WHERE clause string, list of parameters).
    """
    # Start with the base filter clause
    where_clause, params = build_filter_clause(
        manufacturers=manufacturers,
        product_codes=product_codes,
        event_types=event_types,
        date_from=date_from,
        date_to=date_to,
        search_text=search_text,
        table_alias=table_alias,
    )

    # Add device filters using EXISTS subqueries
    if device_filters:
        device_conditions = _build_device_conditions(device_filters, table_alias)
        if device_conditions:
            device_clause, device_params = device_conditions
            if where_clause == "1=1":
                where_clause = device_clause
            else:
                where_clause = f"{where_clause} AND {device_clause}"
            params.extend(device_params)

    return where_clause, params


def _build_device_conditions(
    device_filters: DeviceFilters,
    table_alias: str = "m",
) -> Optional[tuple[str, list]]:
    """Build EXISTS subquery conditions for device filters.

    Args:
        device_filters: Device filter parameters.
        table_alias: Main table alias.

    Returns:
        Tuple of (condition string, parameters) or None if no filters.
    """
    device_conditions = []
    params = []

    # Brand names filter
    if device_filters.brand_names:
        placeholders = ", ".join(["?" for _ in device_filters.brand_names])
        device_conditions.append(f"d.brand_name IN ({placeholders})")
        params.extend(device_filters.brand_names)

    # Generic names filter (case-insensitive ILIKE)
    if device_filters.generic_names:
        generic_conditions = []
        for name in device_filters.generic_names:
            generic_conditions.append("d.generic_name ILIKE ?")
            params.append(f"%{name}%")
        device_conditions.append(f"({' OR '.join(generic_conditions)})")

    # Device manufacturer filter
    if device_filters.device_manufacturers:
        placeholders = ", ".join(["?" for _ in device_filters.device_manufacturers])
        device_conditions.append(f"d.manufacturer_d_name IN ({placeholders})")
        params.extend(device_filters.device_manufacturers)

    # Model numbers filter (exact match)
    if device_filters.model_numbers:
        placeholders = ", ".join(["?" for _ in device_filters.model_numbers])
        device_conditions.append(f"d.model_number IN ({placeholders})")
        params.extend(device_filters.model_numbers)

    # Implant flag filter
    if device_filters.implant_flag in ('Y', 'N'):
        device_conditions.append("d.implant_flag = ?")
        params.append(device_filters.implant_flag)

    # Device product codes filter
    if device_filters.device_product_codes:
        placeholders = ", ".join(["?" for _ in device_filters.device_product_codes])
        device_conditions.append(f"d.device_report_product_code IN ({placeholders})")
        params.extend(device_filters.device_product_codes)

    if not device_conditions:
        return None

    # Build EXISTS subquery
    device_where = " AND ".join(device_conditions)
    exists_clause = f"""
        EXISTS (
            SELECT 1 FROM devices d
            WHERE d.mdr_report_key = {table_alias}.mdr_report_key
            AND {device_where}
        )
    """

    return exists_clause, params


def has_device_filters(device_filters: Optional[DeviceFilters]) -> bool:
    """Check if any device filters are active.

    Args:
        device_filters: Device filter parameters.

    Returns:
        True if any device filter is set.
    """
    if not device_filters:
        return False

    return any([
        device_filters.brand_names,
        device_filters.generic_names,
        device_filters.device_manufacturers,
        device_filters.model_numbers,
        device_filters.implant_flag in ('Y', 'N'),
        device_filters.device_product_codes,
    ])


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
