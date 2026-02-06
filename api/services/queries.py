"""Query service for common database operations."""

from typing import Optional
from datetime import date

from api.services.database import get_db
from api.services.filters import (
    build_filter_clause,
    build_extended_filter_clause,
    build_count_query,
    build_paginated_query,
    DeviceFilters,
)
from api.services.query_builder import SchemaAwareQueryBuilder
from config.unified_schema import EVENT_TYPES, get_schema_registry


class QueryService:
    """Service for executing common queries."""

    def __init__(self):
        self.db = get_db()

    def get_event_stats(
        self,
        manufacturers: Optional[list[str]] = None,
        product_codes: Optional[list[str]] = None,
        event_types: Optional[list[str]] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        device_filters: Optional[DeviceFilters] = None,
    ) -> dict:
        """Get summary statistics for events.

        Returns:
            Dictionary with total, deaths, injuries, malfunctions counts.
        """
        # Build the base query using SchemaAwareQueryBuilder
        builder = (
            SchemaAwareQueryBuilder()
            .select("master_events", [], validate=False)
            .alias("m")
            .add_count(alias="total")
        )

        # Add case counts for each event type from schema
        for code, event_type in EVENT_TYPES.items():
            if code == "*":
                continue
            # Map code to output alias (deaths, injuries, malfunctions, other)
            alias_map = {"D": "deaths", "IN": "injuries", "M": "malfunctions", "O": "other"}
            alias = alias_map.get(code, event_type.name.lower())
            builder.add_case_count("event_type", code, alias)

        # Add filters
        if manufacturers:
            builder.where_manufacturer(manufacturers)
        if product_codes:
            builder.where_in("product_code", product_codes)
        if event_types:
            builder.where_event_types(event_types)
        if date_from or date_to:
            builder.where_date_range("date_received", date_from, date_to)

        # Handle device filters via extended filter clause
        if device_filters:
            where_clause, params = build_extended_filter_clause(
                device_filters=device_filters,
                table_alias="m",
            )
            if where_clause != "1=1":
                builder.where(where_clause, params)

        query, params = builder.build()
        result = self.db.fetch_one(query, params)
        return {
            "total": result[0],
            "deaths": result[1],
            "injuries": result[2],
            "malfunctions": result[3],
            "other": result[4],
        }

    def get_events(
        self,
        manufacturers: Optional[list[str]] = None,
        product_codes: Optional[list[str]] = None,
        event_types: Optional[list[str]] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        search_text: Optional[str] = None,
        device_filters: Optional[DeviceFilters] = None,
        page: int = 1,
        page_size: int = 50,
    ) -> dict:
        """Get paginated list of events.

        Returns:
            Dictionary with events list and pagination info.
        """
        where_clause, params = build_extended_filter_clause(
            manufacturers=manufacturers,
            product_codes=product_codes,
            event_types=event_types,
            date_from=date_from,
            date_to=date_to,
            search_text=search_text,
            device_filters=device_filters,
        )

        # Get total count
        count_query = build_count_query(where_clause=where_clause)
        total = self.db.fetch_one(count_query, params)[0]

        # Get paginated results
        select_clause = """
            m.mdr_report_key,
            m.report_number,
            m.date_received,
            m.date_of_event,
            m.event_type,
            m.manufacturer_clean,
            m.product_code,
            m.manufacturer_name
        """
        events_query = build_paginated_query(
            select_clause=select_clause,
            where_clause=where_clause,
            page=page,
            page_size=page_size,
        )

        results = self.db.fetch_all(events_query, params)

        events = [
            {
                "mdr_report_key": row[0],
                "report_number": row[1],
                "date_received": str(row[2]) if row[2] else None,
                "date_of_event": str(row[3]) if row[3] else None,
                "event_type": row[4],
                "manufacturer": row[5],
                "product_code": row[6],
                "manufacturer_name": row[7],
            }
            for row in results
        ]

        return {
            "events": events,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "total_pages": (total + page_size - 1) // page_size,
            },
        }

    def get_event_detail(self, mdr_report_key: str) -> Optional[dict]:
        """Get detailed information for a single event.

        Args:
            mdr_report_key: The MDR report key.

        Returns:
            Event details or None if not found.
        """
        # Get main event data
        event_query = """
            SELECT
                mdr_report_key, report_number, date_received, date_of_event,
                event_type, manufacturer_clean, product_code,
                manufacturer_name, manufacturer_city, manufacturer_state,
                manufacturer_country, adverse_event_flag, product_problem_flag
            FROM master_events
            WHERE mdr_report_key = ?
        """
        event = self.db.fetch_one(event_query, [mdr_report_key])
        if not event:
            return None

        # Get devices
        devices_query = """
            SELECT
                brand_name, generic_name, model_number,
                manufacturer_d_clean, device_report_product_code
            FROM devices
            WHERE mdr_report_key = ?
        """
        devices = self.db.fetch_all(devices_query, [mdr_report_key])

        # Get narrative text
        text_query = """
            SELECT text_type_code, text_content
            FROM mdr_text
            WHERE mdr_report_key = ?
            ORDER BY text_type_code
        """
        texts = self.db.fetch_all(text_query, [mdr_report_key])

        # Get patient information
        patient_query = """
            SELECT
                patient_sequence_number, patient_age, patient_sex,
                outcome_death, outcome_life_threatening, outcome_hospitalization,
                outcome_disability, outcome_other
            FROM patients
            WHERE mdr_report_key = ?
        """
        patients = self.db.fetch_all(patient_query, [mdr_report_key])

        return {
            "mdr_report_key": event[0],
            "report_number": event[1],
            "date_received": str(event[2]) if event[2] else None,
            "date_of_event": str(event[3]) if event[3] else None,
            "event_type": event[4],
            "manufacturer": event[5],
            "product_code": event[6],
            "manufacturer_name": event[7],
            "manufacturer_city": event[8],
            "manufacturer_state": event[9],
            "manufacturer_country": event[10],
            "adverse_event_flag": event[11],
            "product_problem_flag": event[12],
            "devices": [
                {
                    "brand_name": d[0],
                    "generic_name": d[1],
                    "model_number": d[2],
                    "manufacturer": d[3],
                    "product_code": d[4],
                }
                for d in devices
            ],
            "narratives": [
                {"type": t[0], "text": t[1]}
                for t in texts
            ],
            "patients": [
                {
                    "sequence": p[0],
                    "age": p[1],
                    "sex": p[2],
                    "outcomes": {
                        "death": bool(p[3]),
                        "life_threatening": bool(p[4]),
                        "hospitalization": bool(p[5]),
                        "disability": bool(p[6]),
                        "other": bool(p[7]),
                    },
                }
                for p in patients
            ],
        }

    def get_manufacturer_list(self, search: Optional[str] = None, limit: int = 100) -> list[dict]:
        """Get list of manufacturers for autocomplete.

        Args:
            search: Optional search term.
            limit: Maximum results to return.

        Returns:
            List of manufacturer names with counts.
        """
        # Query devices.manufacturer_d_name which has 99.45% coverage
        # (master_events.manufacturer_clean is NULL for all records)
        if search:
            query = """
                SELECT d.manufacturer_d_name, COUNT(DISTINCT d.mdr_report_key) as count
                FROM devices d
                WHERE d.manufacturer_d_name IS NOT NULL
                AND LOWER(d.manufacturer_d_name) LIKE ?
                GROUP BY d.manufacturer_d_name
                ORDER BY count DESC
                LIMIT ?
            """
            params = [f"%{search.lower()}%", limit]
        else:
            query = """
                SELECT d.manufacturer_d_name, COUNT(DISTINCT d.mdr_report_key) as count
                FROM devices d
                WHERE d.manufacturer_d_name IS NOT NULL
                GROUP BY d.manufacturer_d_name
                ORDER BY count DESC
                LIMIT ?
            """
            params = [limit]

        results = self.db.fetch_all(query, params)
        return [{"name": r[0], "count": r[1]} for r in results]

    def get_product_code_list(self, search: Optional[str] = None, limit: int = 100) -> list[dict]:
        """Get list of product codes for autocomplete.

        Args:
            search: Optional search term.
            limit: Maximum results to return.

        Returns:
            List of product codes with counts.
        """
        if search:
            query = """
                SELECT m.product_code, pc.device_name, COUNT(*) as count
                FROM master_events m
                LEFT JOIN product_codes pc ON m.product_code = pc.product_code
                WHERE m.product_code IS NOT NULL
                AND (LOWER(m.product_code) LIKE ? OR LOWER(pc.device_name) LIKE ?)
                GROUP BY m.product_code, pc.device_name
                ORDER BY count DESC
                LIMIT ?
            """
            search_term = f"%{search.lower()}%"
            params = [search_term, search_term, limit]
        else:
            query = """
                SELECT m.product_code, pc.device_name, COUNT(*) as count
                FROM master_events m
                LEFT JOIN product_codes pc ON m.product_code = pc.product_code
                WHERE m.product_code IS NOT NULL
                GROUP BY m.product_code, pc.device_name
                ORDER BY count DESC
                LIMIT ?
            """
            params = [limit]

        results = self.db.fetch_all(query, params)
        return [{"code": r[0], "name": r[1], "count": r[2]} for r in results]

    def get_trends(
        self,
        manufacturers: Optional[list[str]] = None,
        product_codes: Optional[list[str]] = None,
        event_types: Optional[list[str]] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        group_by: str = "month",
        date_field: str = "date_received",
        device_filters: Optional[DeviceFilters] = None,
    ) -> list[dict]:
        """Get event trends over time.

        Args:
            group_by: "day", "month", or "year".
            date_field: Which date field to use for grouping. Options:
                - "date_received": When FDA received the report (default)
                - "date_of_event": When the event actually occurred
            device_filters: Optional device filters to apply.

        Returns:
            List of time periods with counts.
        """
        # Validate date_field
        if date_field not in ("date_received", "date_of_event"):
            date_field = "date_received"

        # Determine date expression based on grouping
        if group_by == "day":
            date_expr = date_field
            date_expr_full = f"m.{date_field}"
        elif group_by == "year":
            date_expr = f"DATE_TRUNC('year', m.{date_field})"
            date_expr_full = date_expr
        else:
            date_expr = f"DATE_TRUNC('month', m.{date_field})"
            date_expr_full = date_expr

        # Build query using SchemaAwareQueryBuilder
        builder = (
            SchemaAwareQueryBuilder()
            .select("master_events", [], validate=False)
            .alias("m")
        )

        # Add the period column (raw expression, no table alias prefix)
        from api.services.query_builder import QueryColumn
        builder._columns.append(QueryColumn(name=date_expr_full, alias="period"))
        builder.add_count(alias="total")

        # Add case counts for D, IN, M event types from schema
        for code in ["D", "IN", "M"]:
            event_type = EVENT_TYPES.get(code)
            if event_type:
                alias_map = {"D": "deaths", "IN": "injuries", "M": "malfunctions"}
                builder.add_case_count("event_type", code, alias_map[code])

        # Add filters
        if manufacturers:
            builder.where_manufacturer(manufacturers)
        if product_codes:
            builder.where_in("product_code", product_codes)
        if event_types:
            builder.where_event_types(event_types)
        if date_from or date_to:
            builder.where_date_range(date_field, date_from, date_to)

        # Handle device filters via extended filter clause
        if device_filters:
            where_clause, params = build_extended_filter_clause(
                device_filters=device_filters,
                table_alias="m",
            )
            if where_clause != "1=1":
                builder.where(where_clause, params)

        # Ensure date field is not null
        builder.where_not_null(date_field)

        # Group by the date expression and order by period
        builder._group_by.append(date_expr_full)
        builder._order_by.append("period ASC")

        query, params = builder.build()
        results = self.db.fetch_all(query, params)
        return [
            {
                "period": str(r[0]) if r[0] else None,
                "total": r[1],
                "deaths": r[2],
                "injuries": r[3],
                "malfunctions": r[4],
            }
            for r in results
        ]

    def get_manufacturer_comparison(
        self,
        manufacturers: list[str],
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
    ) -> list[dict]:
        """Compare multiple manufacturers.

        Args:
            manufacturers: List of manufacturers to compare.
            date_from: Start date.
            date_to: End date.

        Returns:
            List of manufacturer statistics.
        """
        # Build query using SchemaAwareQueryBuilder
        builder = (
            SchemaAwareQueryBuilder()
            .select("master_events", [], validate=False)
            .add_column("manufacturer_clean")
            .add_count(alias="total")
        )

        # Add case counts for D, IN, M event types from schema
        for code in ["D", "IN", "M"]:
            event_type = EVENT_TYPES.get(code)
            if event_type:
                alias_map = {"D": "deaths", "IN": "injuries", "M": "malfunctions"}
                builder.add_case_count("event_type", code, alias_map[code])

        # Add manufacturer filter
        builder.where_in("manufacturer_clean", manufacturers)

        # Add date filters
        if date_from or date_to:
            builder.where_date_range("date_received", date_from, date_to)

        # Group and order
        builder.group_by("manufacturer_clean")
        builder.order_by("total", desc=True, table_alias=None)

        query, params = builder.build()
        results = self.db.fetch_all(query, params)
        return [
            {
                "manufacturer": r[0],
                "total": r[1],
                "deaths": r[2],
                "injuries": r[3],
                "malfunctions": r[4],
            }
            for r in results
        ]
