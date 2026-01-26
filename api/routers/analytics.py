"""Analytics API router."""

from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import date
import re
from collections import Counter

from api.services.queries import QueryService
from api.services.database import get_db
from api.models.schemas import (
    TrendData,
    ManufacturerComparison,
    TextFrequencyResult,
)

router = APIRouter()


@router.get("/trends", response_model=list[TrendData])
async def get_trends(
    manufacturers: Optional[str] = Query(None, description="Comma-separated manufacturer names"),
    product_codes: Optional[str] = Query(None, description="Comma-separated product codes"),
    event_types: Optional[str] = Query(None, description="Comma-separated event types (D,I,M,O)"),
    date_from: Optional[date] = Query(None, description="Start date"),
    date_to: Optional[date] = Query(None, description="End date"),
    group_by: str = Query("month", description="Group by: day, month, or year"),
):
    """Get event trends over time."""
    if group_by not in ("day", "month", "year"):
        raise HTTPException(status_code=400, detail="group_by must be day, month, or year")

    query_service = QueryService()

    mfr_list = manufacturers.split(",") if manufacturers else None
    code_list = product_codes.split(",") if product_codes else None
    type_list = event_types.split(",") if event_types else None

    return query_service.get_trends(
        manufacturers=mfr_list,
        product_codes=code_list,
        event_types=type_list,
        date_from=date_from,
        date_to=date_to,
        group_by=group_by,
    )


@router.get("/compare", response_model=list[ManufacturerComparison])
async def compare_manufacturers(
    manufacturers: str = Query(..., description="Comma-separated manufacturer names to compare"),
    date_from: Optional[date] = Query(None, description="Start date"),
    date_to: Optional[date] = Query(None, description="End date"),
):
    """Compare multiple manufacturers."""
    mfr_list = [m.strip() for m in manufacturers.split(",") if m.strip()]

    if len(mfr_list) < 1:
        raise HTTPException(status_code=400, detail="At least one manufacturer required")
    if len(mfr_list) > 10:
        raise HTTPException(status_code=400, detail="Maximum 10 manufacturers can be compared")

    query_service = QueryService()
    return query_service.get_manufacturer_comparison(
        manufacturers=mfr_list,
        date_from=date_from,
        date_to=date_to,
    )


@router.get("/signals")
async def detect_signals(
    manufacturers: Optional[str] = Query(None, description="Comma-separated manufacturer names"),
    product_codes: Optional[str] = Query(None, description="Comma-separated product codes"),
    lookback_months: int = Query(12, ge=1, le=60, description="Months to analyze"),
    min_threshold: int = Query(10, ge=1, description="Minimum events to consider"),
):
    """Detect potential safety signals based on event patterns.

    Identifies manufacturers/products with unusual increases in adverse events.
    Uses available data with manufacturer information (data with manufacturer_clean populated).
    """
    db = get_db()

    # Find date range where manufacturer_clean data is substantially available
    date_range_query = """
        SELECT MIN(date_received), MAX(date_received)
        FROM master_events
        WHERE manufacturer_clean IS NOT NULL
        AND date_received >= DATE '2010-01-01'
    """
    date_result = db.fetch_all(date_range_query)
    if not date_result or not date_result[0][0]:
        return {"lookback_months": lookback_months, "signals": [], "data_note": "No manufacturer data available"}

    min_date, max_date = date_result[0]

    # Use end of 2019 as reference if max_date is in a sparse period (2020+)
    # This is a workaround for incomplete manufacturer_clean data in recent years
    reference_date = max_date
    if max_date.year >= 2020:
        # Check if there's better data in 2019
        check_query = """
            SELECT MAX(date_received) FROM master_events
            WHERE manufacturer_clean IS NOT NULL AND EXTRACT(YEAR FROM date_received) = 2019
        """
        check_result = db.fetch_all(check_query)
        if check_result and check_result[0][0]:
            reference_date = check_result[0][0]

    # Build base filter using the reference date
    start_date = f"DATE '{reference_date}' - INTERVAL '{lookback_months}' MONTH"
    conditions = [f"date_received >= {start_date}"]
    conditions.append(f"date_received <= DATE '{reference_date}'")
    params = []

    if manufacturers:
        mfr_list = manufacturers.split(",")
        placeholders = ", ".join(["?" for _ in mfr_list])
        conditions.append(f"manufacturer_clean IN ({placeholders})")
        params.extend(mfr_list)

    if product_codes:
        code_list = product_codes.split(",")
        placeholders = ", ".join(["?" for _ in code_list])
        conditions.append(f"product_code IN ({placeholders})")
        params.extend(code_list)

    where_clause = " AND ".join(conditions)

    # Get monthly trends by manufacturer with latest month calculation
    query = f"""
        WITH monthly_counts AS (
            SELECT
                manufacturer_clean,
                DATE_TRUNC('month', date_received) as month,
                COUNT(*) as event_count,
                COUNT(CASE WHEN event_type = 'D' THEN 1 END) as death_count
            FROM master_events
            WHERE {where_clause}
            AND manufacturer_clean IS NOT NULL
            GROUP BY manufacturer_clean, DATE_TRUNC('month', date_received)
        ),
        latest_months AS (
            SELECT
                manufacturer_clean,
                MAX(month) as max_month
            FROM monthly_counts
            GROUP BY manufacturer_clean
        ),
        latest_counts AS (
            SELECT
                mc.manufacturer_clean,
                mc.event_count as latest_month_events
            FROM monthly_counts mc
            JOIN latest_months lm ON mc.manufacturer_clean = lm.manufacturer_clean
                AND mc.month = lm.max_month
        ),
        manufacturer_stats AS (
            SELECT
                mc.manufacturer_clean,
                AVG(mc.event_count) as avg_events,
                STDDEV_SAMP(mc.event_count) as std_events,
                SUM(mc.event_count) as total_events,
                SUM(mc.death_count) as total_deaths,
                MAX(mc.event_count) as max_month_events,
                lc.latest_month_events
            FROM monthly_counts mc
            JOIN latest_counts lc ON mc.manufacturer_clean = lc.manufacturer_clean
            GROUP BY mc.manufacturer_clean, lc.latest_month_events
            HAVING SUM(mc.event_count) >= ?
        )
        SELECT
            manufacturer_clean,
            ROUND(avg_events, 1) as avg_monthly,
            ROUND(COALESCE(std_events, 0), 1) as std_monthly,
            total_events,
            total_deaths,
            latest_month_events,
            CASE
                WHEN std_events > 0 THEN ROUND((latest_month_events - avg_events) / std_events, 2)
                ELSE 0
            END as z_score
        FROM manufacturer_stats
        ORDER BY z_score DESC
        LIMIT 20
    """

    params.append(min_threshold)

    results = db.fetch_all(query, params)

    signals = []
    for row in results:
        z_score = float(row[6]) if row[6] else 0.0
        if z_score > 2:
            signal_type = "high"
        elif z_score > 1:
            signal_type = "elevated"
        else:
            signal_type = "normal"

        signals.append({
            "manufacturer": row[0],
            "avg_monthly": float(row[1]) if row[1] else 0.0,
            "std_monthly": float(row[2]) if row[2] else 0.0,
            "total_events": int(row[3]) if row[3] else 0,
            "total_deaths": int(row[4]) if row[4] else 0,
            "latest_month": int(row[5]) if row[5] else 0,
            "z_score": z_score,
            "signal_type": signal_type,
        })

    return {
        "lookback_months": lookback_months,
        "signals": signals,
    }


@router.get("/text-frequency", response_model=list[TextFrequencyResult])
async def analyze_text_frequency(
    manufacturers: Optional[str] = Query(None, description="Comma-separated manufacturer names"),
    product_codes: Optional[str] = Query(None, description="Comma-separated product codes"),
    event_types: Optional[str] = Query(None, description="Comma-separated event types"),
    date_from: Optional[date] = Query(None, description="Start date"),
    date_to: Optional[date] = Query(None, description="End date"),
    min_word_length: int = Query(4, ge=2, le=20, description="Minimum word length"),
    top_n: int = Query(50, ge=10, le=200, description="Number of top terms"),
    sample_size: int = Query(1000, ge=100, le=10000, description="Number of records to sample"),
):
    """Analyze text frequency in event narratives.

    Returns most common terms found in event descriptions.
    """
    db = get_db()

    # Build filter conditions
    conditions = ["t.text_content IS NOT NULL"]
    params = []

    if manufacturers:
        mfr_list = manufacturers.split(",")
        placeholders = ", ".join(["?" for _ in mfr_list])
        conditions.append(f"m.manufacturer_clean IN ({placeholders})")
        params.extend(mfr_list)

    if product_codes:
        code_list = product_codes.split(",")
        placeholders = ", ".join(["?" for _ in code_list])
        conditions.append(f"m.product_code IN ({placeholders})")
        params.extend(code_list)

    if event_types:
        type_list = event_types.split(",")
        type_map = {"D": "D", "I": "IN", "M": "M", "O": "O"}
        db_types = [type_map.get(t, t) for t in type_list]
        placeholders = ", ".join(["?" for _ in db_types])
        conditions.append(f"m.event_type IN ({placeholders})")
        params.extend(db_types)

    if date_from:
        conditions.append("m.date_received >= ?")
        params.append(date_from.isoformat())

    if date_to:
        conditions.append("m.date_received <= ?")
        params.append(date_to.isoformat())

    where_clause = " AND ".join(conditions)
    params.append(sample_size)

    # Get sample of text content
    query = f"""
        SELECT t.text_content
        FROM mdr_text t
        JOIN master_events m ON t.mdr_report_key = m.mdr_report_key
        WHERE {where_clause}
        ORDER BY RANDOM()
        LIMIT ?
    """

    results = db.fetch_all(query, params)

    # Tokenize and count words
    word_counter = Counter()
    stopwords = {
        "the", "and", "was", "for", "that", "with", "this", "from",
        "have", "has", "had", "were", "been", "being", "are", "but",
        "not", "they", "their", "what", "when", "which", "would", "there",
        "could", "about", "into", "than", "then", "them", "these", "some",
        "other", "such", "only", "also", "after", "before", "while", "during",
        "patient", "device", "reported", "report", "event", "manufacturer",
    }

    for row in results:
        text = row[0]
        if text:
            # Tokenize: extract words, lowercase, filter
            words = re.findall(r'\b[a-zA-Z]+\b', text.lower())
            for word in words:
                if len(word) >= min_word_length and word not in stopwords:
                    word_counter[word] += 1

    total_words = sum(word_counter.values())
    top_terms = word_counter.most_common(top_n)

    return [
        {
            "term": term,
            "count": count,
            "percentage": round(count * 100 / total_words, 2) if total_words > 0 else 0,
        }
        for term, count in top_terms
    ]


@router.get("/event-type-distribution")
async def get_event_type_distribution(
    manufacturers: Optional[str] = Query(None, description="Comma-separated manufacturer names"),
    product_codes: Optional[str] = Query(None, description="Comma-separated product codes"),
    date_from: Optional[date] = Query(None, description="Start date"),
    date_to: Optional[date] = Query(None, description="End date"),
):
    """Get distribution of event types."""
    db = get_db()

    conditions = []
    params = []

    if manufacturers:
        mfr_list = manufacturers.split(",")
        placeholders = ", ".join(["?" for _ in mfr_list])
        conditions.append(f"manufacturer_clean IN ({placeholders})")
        params.extend(mfr_list)

    if product_codes:
        code_list = product_codes.split(",")
        placeholders = ", ".join(["?" for _ in code_list])
        conditions.append(f"product_code IN ({placeholders})")
        params.extend(code_list)

    if date_from:
        conditions.append("date_received >= ?")
        params.append(date_from.isoformat())

    if date_to:
        conditions.append("date_received <= ?")
        params.append(date_to.isoformat())

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    query = f"""
        SELECT
            CASE event_type
                WHEN 'D' THEN 'Death'
                WHEN 'IN' THEN 'Injury'
                WHEN 'M' THEN 'Malfunction'
                WHEN 'O' THEN 'Other'
                ELSE 'Unknown'
            END as event_type_label,
            COUNT(*) as count
        FROM master_events
        WHERE {where_clause}
        GROUP BY event_type
        ORDER BY count DESC
    """

    results = db.fetch_all(query, params)

    total = sum(r[1] for r in results)
    return [
        {
            "type": r[0],
            "count": r[1],
            "percentage": round(r[1] * 100 / total, 2) if total > 0 else 0,
        }
        for r in results
    ]
