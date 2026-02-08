"""Admin API router."""

import os
import json
import subprocess
from pathlib import Path
from datetime import datetime
from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import Optional

from api.services.database import get_db, close_db, reconnect_db
from api.models.schemas import DatabaseStatus, IngestionLogEntry

router = APIRouter()

# Refresh status file
REFRESH_STATUS_FILE = Path(__file__).parent.parent.parent / "data" / ".refresh_status.json"


def _get_refresh_status() -> dict:
    """Get current refresh status."""
    if REFRESH_STATUS_FILE.exists():
        try:
            with open(REFRESH_STATUS_FILE) as f:
                return json.load(f)
        except:
            pass
    return {"status": "idle", "last_refresh": None, "message": None}


def _set_refresh_status(status: str, message: str = None):
    """Set refresh status."""
    data = {
        "status": status,
        "message": message,
        "updated_at": datetime.now().isoformat(),
    }
    if status == "completed":
        data["last_refresh"] = datetime.now().isoformat()

    REFRESH_STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(REFRESH_STATUS_FILE, "w") as f:
        json.dump(data, f)


def _run_refresh_task():
    """Run the refresh task in a separate subprocess to avoid connection conflicts."""
    project_dir = Path(__file__).parent.parent.parent

    import time
    import gc

    try:
        # Close API's database connection to release lock
        _set_refresh_status("running", "Releasing database lock...")
        close_db()
        gc.collect()  # Force garbage collection to release any lingering references
        time.sleep(2)  # Give DuckDB time to fully release the lock

        _set_refresh_status("running", "Starting refresh subprocess...")

        # Create a Python script to run in subprocess
        refresh_script = '''
import sys
import json
import urllib.request
import zipfile
import io
from pathlib import Path

project_dir = Path("{project_dir}")
sys.path.insert(0, str(project_dir))

status_file = project_dir / "data" / ".refresh_status.json"

def set_status(status, message):
    from datetime import datetime
    data = {{"status": status, "message": message, "updated_at": datetime.now().isoformat()}}
    if status == "completed":
        data["last_refresh"] = datetime.now().isoformat()
    with open(status_file, "w") as f:
        json.dump(data, f)

try:
    data_dir = project_dir / "data" / "raw"

    # Download ADD files
    add_files = ["mdrfoiAdd", "foidevAdd", "foitextAdd", "patientAdd"]
    for filename in add_files:
        set_status("running", f"Downloading {{filename}}.zip...")
        url = f"https://www.accessdata.fda.gov/MAUDE/ftparea/{{filename}}.zip"
        with urllib.request.urlopen(url, timeout=300) as response:
            zip_data = response.read()
            with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
                zf.extractall(data_dir)

    # Load ADD files
    set_status("running", "Loading ADD files into database...")
    from src.ingestion.loader import MAUDELoader

    loader = MAUDELoader(
        db_path=project_dir / "data" / "maude.duckdb",
        batch_size=10000,
        enable_transaction_safety=True,
        enable_validation=True,
    )

    files_to_load = [
        (data_dir / "mdrfoiAdd.txt", "master"),
        (data_dir / "foidevAdd.txt", "device"),
        (data_dir / "foitextAdd.txt", "text"),
        (data_dir / "patientAdd.txt", "patient"),
    ]

    total_loaded = 0
    for filepath, file_type in files_to_load:
        set_status("running", f"Loading {{filepath.name}}...")
        result = loader.load_file(filepath, file_type=file_type)
        total_loaded += result.records_loaded

    set_status("completed", f"Refresh completed. Loaded {{total_loaded:,}} records.")

except Exception as e:
    set_status("failed", f"Refresh error: {{str(e)}}")
    sys.exit(1)
'''.format(project_dir=str(project_dir))

        # Run in subprocess
        result = subprocess.run(
            ["python3", "-c", refresh_script],
            cwd=str(project_dir),
            capture_output=True,
            text=True,
            timeout=1800,  # 30 minute timeout
        )

        if result.returncode != 0:
            # Check if status was set by the script
            current = _get_refresh_status()
            if current.get("status") != "failed":
                _set_refresh_status("failed", f"Subprocess failed: {result.stderr[:500]}")

        # Reconnect API's database
        reconnect_db()

    except subprocess.TimeoutExpired:
        reconnect_db()
        _set_refresh_status("failed", "Refresh timed out after 30 minutes")
    except Exception as e:
        reconnect_db()
        _set_refresh_status("failed", f"Refresh error: {str(e)}")


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
    # Check if refresh is already running
    current_status = _get_refresh_status()
    if current_status.get("status") == "running":
        raise HTTPException(
            status_code=409,
            detail="A refresh is already in progress"
        )

    # Start refresh in background
    background_tasks.add_task(_run_refresh_task)
    _set_refresh_status("starting", "Refresh initiated...")

    return {
        "status": "started",
        "message": "Data refresh started in background. Check /api/admin/refresh/status for progress.",
    }


@router.get("/refresh/status")
async def get_refresh_status():
    """Get the current status of data refresh."""
    status = _get_refresh_status()

    # Only check database if not currently refreshing (to avoid reopening connection)
    if status.get("status") not in ["running", "starting"]:
        try:
            db = get_db()
            freshness = db.fetch_one("""
                SELECT MAX(date_added) as latest_date
                FROM master_events
                WHERE date_added IS NOT NULL
            """)
            if freshness and freshness[0]:
                status["data_freshness"] = str(freshness[0])
        except:
            pass

    return status


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
