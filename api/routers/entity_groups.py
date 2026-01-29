"""Entity groups API router.

CRUD endpoints for managing entity groups (manufacturers, brands, etc.).
Groups are stored in a local SQLite database for persistence.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import datetime
import uuid
import json
import sqlite3
from pathlib import Path
from os.path import commonprefix

from api.models.entity_group_schemas import (
    EntityType,
    CreateEntityGroupRequest,
    UpdateEntityGroupRequest,
    EntityGroupResponse,
    EntityGroupListResponse,
    SuggestNameRequest,
    SuggestNameResponse,
    AvailableEntity,
    AvailableEntitiesResponse,
)
from api.services.database import get_db

router = APIRouter()

# Entity groups storage path (same DB as presets)
PRESETS_DB_PATH = Path(__file__).parent.parent.parent / "data" / "presets.db"


def get_entity_groups_db():
    """Get connection to entity groups database, creating table if needed."""
    PRESETS_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(PRESETS_DB_PATH))
    conn.row_factory = sqlite3.Row

    # Create table if not exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_groups (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            entity_type TEXT NOT NULL DEFAULT 'manufacturer',
            members TEXT NOT NULL,
            display_name TEXT,
            is_active INTEGER DEFAULT 1,
            is_built_in INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def generate_group_name(members: list[str]) -> str:
    """Auto-generate a display name from group members."""
    if not members:
        return "Empty Group"

    if len(members) == 1:
        return members[0]

    if len(members) == 2:
        # Shorten names if too long
        name1 = members[0][:25] if len(members[0]) > 25 else members[0]
        name2 = members[1][:25] if len(members[1]) > 25 else members[1]
        return f"{name1} + {name2}"

    # Try to find common prefix
    prefix = commonprefix([m.lower() for m in members])
    if len(prefix) > 3:
        # Clean up the prefix (capitalize, remove trailing spaces/punctuation)
        clean_prefix = prefix.strip().rstrip('- ').rstrip(',').title()
        if clean_prefix:
            return f"{clean_prefix} Group ({len(members)})"

    # Fallback: use first member + count
    first = members[0][:25] if len(members[0]) > 25 else members[0]
    return f"{first} + {len(members) - 1} more"


def row_to_response(row: sqlite3.Row) -> EntityGroupResponse:
    """Convert a database row to an EntityGroupResponse."""
    members = json.loads(row["members"])
    custom_display = row["display_name"]
    display_name = custom_display if custom_display else generate_group_name(members)

    return EntityGroupResponse(
        id=row["id"],
        name=row["name"],
        description=row["description"],
        entity_type=EntityType(row["entity_type"]),
        members=members,
        display_name=display_name,
        is_active=bool(row["is_active"]),
        is_built_in=bool(row["is_built_in"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# Built-in groups (read-only examples)
BUILT_IN_GROUPS = [
    {
        "id": "abbott-group",
        "name": "Abbott (with St. Jude)",
        "description": "Abbott including acquired St. Jude Medical",
        "entity_type": "manufacturer",
        "members": ["ABBOTT", "ABBOTT LABORATORIES", "ST. JUDE MEDICAL"],
        "display_name": "Abbott Group",
        "is_built_in": True,
    },
    {
        "id": "medtronic-group",
        "name": "Medtronic (All Divisions)",
        "description": "Medtronic including all divisions and subsidiaries",
        "entity_type": "manufacturer",
        "members": ["MEDTRONIC", "MEDTRONIC, INC.", "MEDTRONIC SPINE"],
        "display_name": "Medtronic Group",
        "is_built_in": True,
    },
]


@router.get("", response_model=EntityGroupListResponse)
async def list_entity_groups(
    entity_type: Optional[EntityType] = Query(None, description="Filter by entity type"),
    include_built_in: bool = Query(True, description="Include built-in groups"),
    active_only: bool = Query(False, description="Only return active groups"),
):
    """List all entity groups, optionally filtered by type."""
    groups = []

    # Add built-in groups
    if include_built_in:
        now = datetime.utcnow().isoformat() + "Z"
        for group in BUILT_IN_GROUPS:
            if entity_type and group["entity_type"] != entity_type.value:
                continue
            groups.append(EntityGroupResponse(
                id=group["id"],
                name=group["name"],
                description=group["description"],
                entity_type=EntityType(group["entity_type"]),
                members=group["members"],
                display_name=group["display_name"],
                is_active=False,  # Built-in groups start inactive
                is_built_in=True,
                created_at="2024-01-01T00:00:00Z",
                updated_at="2024-01-01T00:00:00Z",
            ))

    # Add user groups from database
    try:
        conn = get_entity_groups_db()

        conditions = []
        params = []

        if entity_type:
            conditions.append("entity_type = ?")
            params.append(entity_type.value)

        if active_only:
            conditions.append("is_active = 1")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        cursor = conn.execute(f"""
            SELECT id, name, description, entity_type, members, display_name,
                   is_active, is_built_in, created_at, updated_at
            FROM entity_groups
            {where_clause}
            ORDER BY created_at DESC
        """, params)

        rows = cursor.fetchall()
        conn.close()

        for row in rows:
            groups.append(row_to_response(row))

    except Exception as e:
        # Log error but don't fail - return what we have
        print(f"Error fetching entity groups: {e}")

    return EntityGroupListResponse(groups=groups, total=len(groups))


@router.get("/suggest-name", response_model=SuggestNameResponse)
async def suggest_group_name(
    members: str = Query(..., description="Comma-separated list of member names"),
):
    """Generate a suggested name for a group based on its members."""
    member_list = [m.strip() for m in members.split(",") if m.strip()]

    if not member_list:
        raise HTTPException(status_code=400, detail="At least one member is required")

    suggested = generate_group_name(member_list)

    return SuggestNameResponse(
        suggested_name=suggested,
        member_count=len(member_list),
    )


@router.get("/available-entities", response_model=AvailableEntitiesResponse)
async def get_available_entities(
    entity_type: EntityType = Query(default=EntityType.MANUFACTURER),
    product_codes: Optional[str] = Query(None, description="Comma-separated product codes filter"),
    event_types: Optional[str] = Query(None, description="Comma-separated event types filter"),
    search: Optional[str] = Query(None, description="Text search filter"),
    limit: int = Query(default=100, le=500, description="Max results"),
):
    """Get available entities for group selection with event counts and group assignments.

    Returns manufacturers (or other entity types) with their event counts,
    filtered by current global filters, with information about existing group assignments.
    """
    db = get_db()

    # Build WHERE conditions
    conditions = []
    params = []

    if entity_type == EntityType.MANUFACTURER:
        entity_column = "manufacturer_clean"
    elif entity_type == EntityType.BRAND:
        entity_column = "brand_name"
    else:  # generic_name
        entity_column = "generic_name"

    conditions.append(f"{entity_column} IS NOT NULL")

    if product_codes:
        code_list = [c.strip() for c in product_codes.split(",") if c.strip()]
        if code_list:
            placeholders = ", ".join(["?" for _ in code_list])
            conditions.append(f"product_code IN ({placeholders})")
            params.extend(code_list)

    if event_types:
        type_list = [t.strip() for t in event_types.split(",") if t.strip()]
        if type_list:
            placeholders = ", ".join(["?" for _ in type_list])
            conditions.append(f"event_type IN ({placeholders})")
            params.extend(type_list)

    if search:
        conditions.append(f"LOWER({entity_column}) LIKE ?")
        params.append(f"%{search.lower()}%")

    where_clause = " AND ".join(conditions)

    # Query for entities with event counts
    # Use different tables based on entity type
    if entity_type == EntityType.MANUFACTURER:
        count_query = f"""
            SELECT {entity_column} as name, COUNT(*) as event_count
            FROM master_events
            WHERE {where_clause}
            GROUP BY {entity_column}
            ORDER BY event_count DESC
            LIMIT ?
        """
    else:
        # For brand/generic names, we need to join with devices table
        count_query = f"""
            SELECT d.{entity_column} as name, COUNT(DISTINCT m.mdr_report_key) as event_count
            FROM devices d
            JOIN master_events m ON d.mdr_report_key = m.mdr_report_key
            WHERE d.{entity_column} IS NOT NULL
            {"AND m.product_code IN (" + ", ".join(["?" for _ in product_codes.split(",")]) + ")" if product_codes else ""}
            {"AND m.event_type IN (" + ", ".join(["?" for _ in event_types.split(",")]) + ")" if event_types else ""}
            {"AND LOWER(d." + entity_column + ") LIKE ?" if search else ""}
            GROUP BY d.{entity_column}
            ORDER BY event_count DESC
            LIMIT ?
        """
        # Rebuild params for device-based query
        params = []
        if product_codes:
            params.extend([c.strip() for c in product_codes.split(",") if c.strip()])
        if event_types:
            params.extend([t.strip() for t in event_types.split(",") if t.strip()])
        if search:
            params.append(f"%{search.lower()}%")

    params.append(limit)
    results = db.fetch_all(count_query, params)

    # Get group assignments for these entities
    entity_names = [r[0] for r in results]
    group_assignments = {}

    if entity_names:
        # Query entity_groups to find which entities are assigned
        conn = get_entity_groups_db()
        cursor = conn.execute(
            "SELECT id, name, display_name, members, entity_type FROM entity_groups WHERE entity_type = ?",
            [entity_type.value]
        )
        rows = cursor.fetchall()
        conn.close()

        # Build assignment map
        for row in rows:
            members = json.loads(row[3])
            for member in members:
                if member in entity_names:
                    group_assignments[member] = {
                        "id": row[0],
                        "name": row[1] if row[1] else row[2],  # Use name or display_name
                    }

        # Also check built-in groups
        for builtin in BUILT_IN_GROUPS:
            if builtin["entity_type"] == entity_type.value:
                for member in builtin["members"]:
                    if member in entity_names and member not in group_assignments:
                        group_assignments[member] = {
                            "id": builtin["id"],
                            "name": builtin["display_name"],
                        }

    # Build response
    entities = []
    for row in results:
        name = row[0]
        event_count = row[1]
        assignment = group_assignments.get(name)

        entities.append(AvailableEntity(
            name=name,
            event_count=event_count,
            assigned_group_id=assignment["id"] if assignment else None,
            assigned_group_name=assignment["name"] if assignment else None,
        ))

    return AvailableEntitiesResponse(
        entities=entities,
        total=len(entities),
    )


@router.get("/{group_id}", response_model=EntityGroupResponse)
async def get_entity_group(group_id: str):
    """Get a single entity group by ID."""
    # Check built-in groups first
    for group in BUILT_IN_GROUPS:
        if group["id"] == group_id:
            return EntityGroupResponse(
                id=group["id"],
                name=group["name"],
                description=group["description"],
                entity_type=EntityType(group["entity_type"]),
                members=group["members"],
                display_name=group["display_name"],
                is_active=False,
                is_built_in=True,
                created_at="2024-01-01T00:00:00Z",
                updated_at="2024-01-01T00:00:00Z",
            )

    # Check user groups
    try:
        conn = get_entity_groups_db()
        cursor = conn.execute(
            "SELECT * FROM entity_groups WHERE id = ?",
            [group_id]
        )
        row = cursor.fetchone()
        conn.close()

        if row:
            return row_to_response(row)
    except Exception:
        pass

    raise HTTPException(status_code=404, detail="Entity group not found")


@router.post("", status_code=201, response_model=EntityGroupResponse)
async def create_entity_group(request: CreateEntityGroupRequest):
    """Create a new entity group."""
    group_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat() + "Z"

    # Generate display name if not provided
    display_name = request.display_name or generate_group_name(request.members)

    conn = get_entity_groups_db()
    conn.execute(
        """
        INSERT INTO entity_groups (
            id, name, description, entity_type, members, display_name,
            is_active, is_built_in, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, 1, 0, ?, ?)
        """,
        [
            group_id,
            request.name,
            request.description,
            request.entity_type.value,
            json.dumps(request.members),
            display_name if request.display_name else None,  # Store null for auto-generated
            now,
            now,
        ]
    )
    conn.commit()
    conn.close()

    return EntityGroupResponse(
        id=group_id,
        name=request.name,
        description=request.description,
        entity_type=request.entity_type,
        members=request.members,
        display_name=display_name,
        is_active=True,
        is_built_in=False,
        created_at=now,
        updated_at=now,
    )


@router.put("/{group_id}", response_model=EntityGroupResponse)
async def update_entity_group(group_id: str, request: UpdateEntityGroupRequest):
    """Update an existing entity group."""
    # Can't update built-in groups
    for group in BUILT_IN_GROUPS:
        if group["id"] == group_id:
            raise HTTPException(status_code=403, detail="Cannot modify built-in groups")

    conn = get_entity_groups_db()

    # Check if group exists
    cursor = conn.execute("SELECT * FROM entity_groups WHERE id = ?", [group_id])
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Entity group not found")

    # Build update
    updates = []
    params = []

    if request.name is not None:
        updates.append("name = ?")
        params.append(request.name)
    if request.description is not None:
        updates.append("description = ?")
        params.append(request.description)
    if request.members is not None:
        updates.append("members = ?")
        params.append(json.dumps(request.members))
    if request.display_name is not None:
        updates.append("display_name = ?")
        params.append(request.display_name if request.display_name else None)
    if request.is_active is not None:
        updates.append("is_active = ?")
        params.append(1 if request.is_active else 0)

    if updates:
        now = datetime.utcnow().isoformat() + "Z"
        updates.append("updated_at = ?")
        params.append(now)
        params.append(group_id)

        conn.execute(
            f"UPDATE entity_groups SET {', '.join(updates)} WHERE id = ?",
            params
        )
        conn.commit()

    # Fetch updated record
    cursor = conn.execute("SELECT * FROM entity_groups WHERE id = ?", [group_id])
    row = cursor.fetchone()
    conn.close()

    return row_to_response(row)


@router.delete("/{group_id}", status_code=204)
async def delete_entity_group(group_id: str):
    """Delete an entity group."""
    # Can't delete built-in groups
    for group in BUILT_IN_GROUPS:
        if group["id"] == group_id:
            raise HTTPException(status_code=403, detail="Cannot delete built-in groups")

    conn = get_entity_groups_db()

    # Check if group exists
    cursor = conn.execute("SELECT id FROM entity_groups WHERE id = ?", [group_id])
    if not cursor.fetchone():
        conn.close()
        raise HTTPException(status_code=404, detail="Entity group not found")

    conn.execute("DELETE FROM entity_groups WHERE id = ?", [group_id])
    conn.commit()
    conn.close()

    return None


@router.post("/{group_id}/activate", response_model=EntityGroupResponse)
async def activate_entity_group(group_id: str):
    """Activate an entity group."""
    return await _set_group_active(group_id, True)


@router.post("/{group_id}/deactivate", response_model=EntityGroupResponse)
async def deactivate_entity_group(group_id: str):
    """Deactivate an entity group."""
    return await _set_group_active(group_id, False)


async def _set_group_active(group_id: str, active: bool) -> EntityGroupResponse:
    """Helper to set group active state."""
    # Built-in groups need special handling - we store their active state
    for builtin in BUILT_IN_GROUPS:
        if builtin["id"] == group_id:
            # Create a user record for built-in group to track active state
            conn = get_entity_groups_db()
            cursor = conn.execute("SELECT * FROM entity_groups WHERE id = ?", [group_id])
            existing = cursor.fetchone()

            now = datetime.utcnow().isoformat() + "Z"

            if existing:
                # Update existing record
                conn.execute(
                    "UPDATE entity_groups SET is_active = ?, updated_at = ? WHERE id = ?",
                    [1 if active else 0, now, group_id]
                )
            else:
                # Create new record for built-in
                conn.execute(
                    """
                    INSERT INTO entity_groups (
                        id, name, description, entity_type, members, display_name,
                        is_active, is_built_in, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                    """,
                    [
                        builtin["id"],
                        builtin["name"],
                        builtin["description"],
                        builtin["entity_type"],
                        json.dumps(builtin["members"]),
                        builtin["display_name"],
                        1 if active else 0,
                        now,
                        now,
                    ]
                )

            conn.commit()

            # Fetch and return
            cursor = conn.execute("SELECT * FROM entity_groups WHERE id = ?", [group_id])
            row = cursor.fetchone()
            conn.close()

            return row_to_response(row)

    # Regular user groups
    conn = get_entity_groups_db()
    cursor = conn.execute("SELECT * FROM entity_groups WHERE id = ?", [group_id])
    row = cursor.fetchone()

    if not row:
        conn.close()
        raise HTTPException(status_code=404, detail="Entity group not found")

    now = datetime.utcnow().isoformat() + "Z"
    conn.execute(
        "UPDATE entity_groups SET is_active = ?, updated_at = ? WHERE id = ?",
        [1 if active else 0, now, group_id]
    )
    conn.commit()

    cursor = conn.execute("SELECT * FROM entity_groups WHERE id = ?", [group_id])
    row = cursor.fetchone()
    conn.close()

    return row_to_response(row)


@router.get("/active/all", response_model=EntityGroupListResponse)
async def get_active_groups(
    entity_type: Optional[EntityType] = Query(None, description="Filter by entity type"),
):
    """Get all currently active entity groups."""
    return await list_entity_groups(
        entity_type=entity_type,
        include_built_in=True,
        active_only=True,
    )
