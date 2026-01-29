"""Filter presets API router.

CRUD endpoints for managing saved filter presets.
Presets are stored in a local SQLite database for persistence.
"""

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import uuid
import json
import sqlite3
from pathlib import Path

router = APIRouter()

# Preset storage path
PRESETS_DB_PATH = Path(__file__).parent.parent.parent / "data" / "presets.db"


def get_presets_db():
    """Get connection to presets database, creating it if needed."""
    PRESETS_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(PRESETS_DB_PATH))
    conn.row_factory = sqlite3.Row

    # Create table if not exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS filter_presets (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            filters TEXT NOT NULL,
            is_built_in INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


# Request/Response models
class CreatePresetRequest(BaseModel):
    name: str
    description: Optional[str] = None
    filters: dict


class UpdatePresetRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    filters: Optional[dict] = None


class PresetResponse(BaseModel):
    id: str
    name: str
    description: Optional[str]
    filters: dict
    isBuiltIn: bool
    createdAt: str
    updatedAt: str


# Built-in presets (read-only)
BUILT_IN_PRESETS = [
    {
        "id": "recent-deaths",
        "name": "Recent Deaths",
        "description": "Death events from the last 30 days",
        "filters": {"eventTypes": ["D"]},
        "isBuiltIn": True,
    },
    {
        "id": "implant-devices",
        "name": "Implant Devices",
        "description": "Events involving implanted medical devices",
        "filters": {"implantFlag": "Y"},
        "isBuiltIn": True,
    },
    {
        "id": "high-severity",
        "name": "High Severity Events",
        "description": "Deaths and injuries only",
        "filters": {"eventTypes": ["D", "I"]},
        "isBuiltIn": True,
    },
]


@router.get("")
async def list_presets(
    include_built_in: bool = Query(True, description="Include built-in presets"),
):
    """List all filter presets."""
    presets = []

    # Add built-in presets
    if include_built_in:
        now = datetime.utcnow().isoformat() + "Z"
        for preset in BUILT_IN_PRESETS:
            presets.append({
                **preset,
                "createdAt": "2024-01-01T00:00:00Z",
                "updatedAt": "2024-01-01T00:00:00Z",
            })

    # Add user presets from database
    try:
        conn = get_presets_db()
        cursor = conn.execute("""
            SELECT id, name, description, filters, is_built_in, created_at, updated_at
            FROM filter_presets
            ORDER BY created_at DESC
        """)
        rows = cursor.fetchall()
        conn.close()

        for row in rows:
            presets.append({
                "id": row["id"],
                "name": row["name"],
                "description": row["description"],
                "filters": json.loads(row["filters"]),
                "isBuiltIn": bool(row["is_built_in"]),
                "createdAt": row["created_at"],
                "updatedAt": row["updated_at"],
            })
    except Exception:
        pass  # Database not available, just return built-in presets

    return presets


@router.get("/{preset_id}")
async def get_preset(preset_id: str):
    """Get a single preset by ID."""
    # Check built-in presets first
    for preset in BUILT_IN_PRESETS:
        if preset["id"] == preset_id:
            return {
                **preset,
                "createdAt": "2024-01-01T00:00:00Z",
                "updatedAt": "2024-01-01T00:00:00Z",
            }

    # Check user presets
    try:
        conn = get_presets_db()
        cursor = conn.execute(
            "SELECT * FROM filter_presets WHERE id = ?",
            [preset_id]
        )
        row = cursor.fetchone()
        conn.close()

        if row:
            return {
                "id": row["id"],
                "name": row["name"],
                "description": row["description"],
                "filters": json.loads(row["filters"]),
                "isBuiltIn": bool(row["is_built_in"]),
                "createdAt": row["created_at"],
                "updatedAt": row["updated_at"],
            }
    except Exception:
        pass

    raise HTTPException(status_code=404, detail="Preset not found")


@router.post("", status_code=201)
async def create_preset(request: CreatePresetRequest):
    """Create a new filter preset."""
    preset_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat() + "Z"

    conn = get_presets_db()
    conn.execute(
        """
        INSERT INTO filter_presets (id, name, description, filters, is_built_in, created_at, updated_at)
        VALUES (?, ?, ?, ?, 0, ?, ?)
        """,
        [
            preset_id,
            request.name,
            request.description,
            json.dumps(request.filters),
            now,
            now,
        ]
    )
    conn.commit()
    conn.close()

    return {
        "id": preset_id,
        "name": request.name,
        "description": request.description,
        "filters": request.filters,
        "isBuiltIn": False,
        "createdAt": now,
        "updatedAt": now,
    }


@router.put("/{preset_id}")
async def update_preset(preset_id: str, request: UpdatePresetRequest):
    """Update an existing preset."""
    # Can't update built-in presets
    for preset in BUILT_IN_PRESETS:
        if preset["id"] == preset_id:
            raise HTTPException(status_code=403, detail="Cannot modify built-in presets")

    conn = get_presets_db()

    # Check if preset exists
    cursor = conn.execute("SELECT * FROM filter_presets WHERE id = ?", [preset_id])
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Preset not found")

    # Build update
    updates = []
    params = []

    if request.name is not None:
        updates.append("name = ?")
        params.append(request.name)
    if request.description is not None:
        updates.append("description = ?")
        params.append(request.description)
    if request.filters is not None:
        updates.append("filters = ?")
        params.append(json.dumps(request.filters))

    now = datetime.utcnow().isoformat() + "Z"
    updates.append("updated_at = ?")
    params.append(now)
    params.append(preset_id)

    conn.execute(
        f"UPDATE filter_presets SET {', '.join(updates)} WHERE id = ?",
        params
    )
    conn.commit()

    # Fetch updated record
    cursor = conn.execute("SELECT * FROM filter_presets WHERE id = ?", [preset_id])
    row = cursor.fetchone()
    conn.close()

    return {
        "id": row["id"],
        "name": row["name"],
        "description": row["description"],
        "filters": json.loads(row["filters"]),
        "isBuiltIn": bool(row["is_built_in"]),
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
    }


@router.delete("/{preset_id}", status_code=204)
async def delete_preset(preset_id: str):
    """Delete a preset."""
    # Can't delete built-in presets
    for preset in BUILT_IN_PRESETS:
        if preset["id"] == preset_id:
            raise HTTPException(status_code=403, detail="Cannot delete built-in presets")

    conn = get_presets_db()

    # Check if preset exists
    cursor = conn.execute("SELECT id FROM filter_presets WHERE id = ?", [preset_id])
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Preset not found")

    conn.execute("DELETE FROM filter_presets WHERE id = ?", [preset_id])
    conn.commit()
    conn.close()

    return None
