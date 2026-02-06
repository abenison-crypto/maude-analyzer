"""Admin API router."""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import Optional

from api.services.database import get_db
from api.models.schemas import DatabaseStatus, IngestionLogEntry

router = APIRouter()


@router.get("/status", response_model=DatabaseStatus)
async def get_database_status():
    """Get database status and statistics."""
    db = get_db()

    # Get table counts
    # Note: manufacturer coverage is from devices table where manufacturer data lives
    counts_query = """
        SELECT
            (SELECT COUNT(*) FROM master_events) as total_events,
            (SELECT COUNT(*) FROM devices) as total_devices,
            (SELECT COUNT(*) FROM patients) as total_patients,
            (SELECT COUNT(manufacturer_d_name) * 100.0 / NULLIF(COUNT(*), 0) FROM devices) as mfr_coverage
    """
    counts = db.fetch_one(counts_query)

    # Get date range
    date_query = """
        SELECT MIN(date_received), MAX(date_received)
        FROM master_events
        WHERE date_received IS NOT NULL
    """
    dates = db.fetch_one(date_query)

    # Get last refresh time
    refresh_query = """
        SELECT MAX(completed_at)
        FROM ingestion_log
        WHERE status = 'COMPLETED'
    """
    last_refresh = db.fetch_one(refresh_query)

    return {
        "total_events": counts[0],
        "total_devices": counts[1],
        "total_patients": counts[2],
        "manufacturer_coverage_pct": round(counts[3], 2) if counts[3] else 0,
        "date_range_start": str(dates[0]) if dates[0] else None,
        "date_range_end": str(dates[1]) if dates[1] else None,
        "last_refresh": str(last_refresh[0]) if last_refresh and last_refresh[0] else None,
    }


@router.get("/history", response_model=list[IngestionLogEntry])
async def get_ingestion_history(
    limit: int = 50,
    file_type: Optional[str] = None,
):
    """Get ingestion history log."""
    db = get_db()

    conditions = []
    params = []

    if file_type:
        conditions.append("file_type = ?")
        params.append(file_type)

    where_clause = " AND ".join(conditions) if conditions else "1=1"
    params.append(limit)

    query = f"""
        SELECT
            id, file_name, file_type, records_loaded, records_errors,
            started_at, completed_at, status
        FROM ingestion_log
        WHERE {where_clause}
        ORDER BY started_at DESC
        LIMIT ?
    """

    results = db.fetch_all(query, params)

    return [
        {
            "id": r[0],
            "file_name": r[1],
            "file_type": r[2],
            "records_loaded": r[3],
            "records_errors": r[4],
            "started_at": str(r[5]) if r[5] else None,
            "completed_at": str(r[6]) if r[6] else None,
            "status": r[7],
        }
        for r in results
    ]


@router.get("/data-quality")
async def get_data_quality_report():
    """Get comprehensive data quality report."""
    db = get_db()

    # Field completeness
    field_query = """
        SELECT
            'date_received' as field, COUNT(date_received) * 100.0 / COUNT(*) as pct FROM master_events
        UNION ALL SELECT 'date_of_event', COUNT(date_of_event) * 100.0 / COUNT(*) FROM master_events
        UNION ALL SELECT 'event_type', COUNT(event_type) * 100.0 / COUNT(*) FROM master_events
        UNION ALL SELECT 'manufacturer_clean', COUNT(manufacturer_clean) * 100.0 / COUNT(*) FROM master_events
        UNION ALL SELECT 'product_code', COUNT(product_code) * 100.0 / COUNT(*) FROM master_events
    """
    field_results = db.fetch_all(field_query)

    # Event type distribution
    type_query = """
        SELECT event_type, COUNT(*) as count
        FROM master_events
        GROUP BY event_type
        ORDER BY count DESC
    """
    type_results = db.fetch_all(type_query)

    # Orphan analysis
    orphan_query = """
        SELECT
            (SELECT COUNT(*) FROM devices d
             WHERE NOT EXISTS (SELECT 1 FROM master_events m WHERE m.mdr_report_key = d.mdr_report_key)
            ) as orphaned_devices,
            (SELECT COUNT(*) FROM master_events m
             WHERE NOT EXISTS (SELECT 1 FROM devices d WHERE d.mdr_report_key = m.mdr_report_key)
            ) as events_without_devices
    """
    orphan_results = db.fetch_one(orphan_query)

    return {
        "field_completeness": [
            {"field": r[0], "percentage": round(r[1], 2)}
            for r in field_results
        ],
        "event_type_distribution": [
            {"type": r[0] or "NULL", "count": r[1]}
            for r in type_results
        ],
        "orphan_analysis": {
            "orphaned_devices": orphan_results[0],
            "events_without_devices": orphan_results[1],
        },
    }


@router.post("/refresh")
async def trigger_refresh(background_tasks: BackgroundTasks):
    """Trigger a data refresh from FDA (runs in background)."""
    # This would trigger the actual refresh
    # For now, return a message indicating it's not implemented
    return {
        "status": "not_implemented",
        "message": "Data refresh must be run via CLI: python scripts/weekly_refresh.py",
    }


@router.get("/table-counts")
async def get_table_counts():
    """Get row counts for all tables."""
    db = get_db()

    tables = [
        "master_events", "devices", "patients", "mdr_text",
        "device_problems", "patient_problems", "ingestion_log"
    ]

    counts = {}
    for table in tables:
        try:
            result = db.fetch_one(f"SELECT COUNT(*) FROM {table}")
            counts[table] = result[0]
        except Exception as e:
            counts[table] = f"Error: {str(e)}"

    return counts
