"""Query service for common database operations."""

from typing import Optional
from datetime import date

from api.services.database import get_db
from api.services.filters import build_filter_clause, build_count_query, build_paginated_query
from api.constants.columns import (
    COLUMN_EVENT_TYPE,
    COLUMN_MANUFACTURER_CLEAN,
    COLUMN_PRODUCT_CODE,
    EVENT_TYPE_CODES,
)

# Event type codes for SQL queries (avoid string literals)
EVENT_CODE_DEATH = "D"
EVENT_CODE_INJURY = "IN"
EVENT_CODE_MALFUNCTION = "M"
EVENT_CODE_OTHER = "O"


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
    ) -> dict:
        """Get summary statistics for events.

        Returns:
            Dictionary with total, deaths, injuries, malfunctions counts.
        """
        where_clause, params = build_filter_clause(
            manufacturers=manufacturers,
            product_codes=product_codes,
            event_types=event_types,
            date_from=date_from,
            date_to=date_to,
        )

        query = f"""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN event_type = '{EVENT_CODE_DEATH}' THEN 1 END) as deaths,
                COUNT(CASE WHEN event_type = '{EVENT_CODE_INJURY}' THEN 1 END) as injuries,
                COUNT(CASE WHEN event_type = '{EVENT_CODE_MALFUNCTION}' THEN 1 END) as malfunctions,
                COUNT(CASE WHEN event_type = '{EVENT_CODE_OTHER}' THEN 1 END) as other
            FROM master_events m
            WHERE {where_clause}
        """

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
        page: int = 1,
        page_size: int = 50,
    ) -> dict:
        """Get paginated list of events.

        Returns:
            Dictionary with events list and pagination info.
        """
        where_clause, params = build_filter_clause(
            manufacturers=manufacturers,
            product_codes=product_codes,
            event_types=event_types,
            date_from=date_from,
            date_to=date_to,
            search_text=search_text,
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
        if search:
            query = """
                SELECT manufacturer_clean, COUNT(*) as count
                FROM master_events
                WHERE manufacturer_clean IS NOT NULL
                AND LOWER(manufacturer_clean) LIKE ?
                GROUP BY manufacturer_clean
                ORDER BY count DESC
                LIMIT ?
            """
            params = [f"%{search.lower()}%", limit]
        else:
            query = """
                SELECT manufacturer_clean, COUNT(*) as count
                FROM master_events
                WHERE manufacturer_clean IS NOT NULL
                GROUP BY manufacturer_clean
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
    ) -> list[dict]:
        """Get event trends over time.

        Args:
            group_by: "day", "month", or "year".

        Returns:
            List of time periods with counts.
        """
        where_clause, params = build_filter_clause(
            manufacturers=manufacturers,
            product_codes=product_codes,
            event_types=event_types,
            date_from=date_from,
            date_to=date_to,
        )

        if group_by == "day":
            date_expr = "date_received"
        elif group_by == "year":
            date_expr = "DATE_TRUNC('year', date_received)"
        else:
            date_expr = "DATE_TRUNC('month', date_received)"

        query = f"""
            SELECT
                {date_expr} as period,
                COUNT(*) as total,
                COUNT(CASE WHEN event_type = '{EVENT_CODE_DEATH}' THEN 1 END) as deaths,
                COUNT(CASE WHEN event_type = '{EVENT_CODE_INJURY}' THEN 1 END) as injuries,
                COUNT(CASE WHEN event_type = '{EVENT_CODE_MALFUNCTION}' THEN 1 END) as malfunctions
            FROM master_events m
            WHERE {where_clause}
            AND date_received IS NOT NULL
            GROUP BY {date_expr}
            ORDER BY period
        """

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
        conditions = ["manufacturer_clean IN ({})".format(
            ", ".join(["?" for _ in manufacturers])
        )]
        params = list(manufacturers)

        if date_from:
            conditions.append("date_received >= ?")
            params.append(date_from.isoformat())
        if date_to:
            conditions.append("date_received <= ?")
            params.append(date_to.isoformat())

        where_clause = " AND ".join(conditions)

        query = f"""
            SELECT
                manufacturer_clean,
                COUNT(*) as total,
                COUNT(CASE WHEN event_type = '{EVENT_CODE_DEATH}' THEN 1 END) as deaths,
                COUNT(CASE WHEN event_type = '{EVENT_CODE_INJURY}' THEN 1 END) as injuries,
                COUNT(CASE WHEN event_type = '{EVENT_CODE_MALFUNCTION}' THEN 1 END) as malfunctions
            FROM master_events
            WHERE {where_clause}
            GROUP BY manufacturer_clean
            ORDER BY total DESC
        """

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
