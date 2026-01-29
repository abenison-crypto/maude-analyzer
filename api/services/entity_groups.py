"""Entity group service for query transformation and group management."""

import json
import sqlite3
from pathlib import Path
from typing import Optional

from api.models.entity_group_schemas import (
    EntityType,
    ActiveGroupInfo,
    GroupTransformationConfig,
)


PRESETS_DB_PATH = Path(__file__).parent.parent.parent / "data" / "presets.db"


class EntityGroupService:
    """Service for managing entity groups and transforming queries."""

    # Column mappings for entity types
    ENTITY_COLUMNS = {
        EntityType.MANUFACTURER: "m.manufacturer_clean",
        EntityType.BRAND: "d.brand_name",
        EntityType.GENERIC_NAME: "d.generic_name",
    }

    def __init__(self):
        self._cached_groups: Optional[list[ActiveGroupInfo]] = None

    def get_active_groups(self, entity_type: Optional[EntityType] = None) -> list[ActiveGroupInfo]:
        """Get all active entity groups, optionally filtered by type."""
        try:
            conn = sqlite3.connect(str(PRESETS_DB_PATH))
            conn.row_factory = sqlite3.Row

            conditions = ["is_active = 1"]
            params = []

            if entity_type:
                conditions.append("entity_type = ?")
                params.append(entity_type.value)

            cursor = conn.execute(f"""
                SELECT id, name, display_name, members, entity_type
                FROM entity_groups
                WHERE {' AND '.join(conditions)}
            """, params)

            rows = cursor.fetchall()
            conn.close()

            groups = []
            for row in rows:
                members = json.loads(row["members"])
                display_name = row["display_name"] or self._generate_display_name(members)
                groups.append(ActiveGroupInfo(
                    id=row["id"],
                    name=row["name"],
                    display_name=display_name,
                    members=members,
                    entity_type=EntityType(row["entity_type"]),
                ))

            return groups

        except Exception as e:
            print(f"Error fetching active groups: {e}")
            return []

    def _generate_display_name(self, members: list[str]) -> str:
        """Generate display name from members."""
        if len(members) == 1:
            return members[0]
        elif len(members) == 2:
            return f"{members[0][:20]} + {members[1][:20]}"
        else:
            return f"{members[0][:20]} + {len(members) - 1} more"

    def get_transformation_config(
        self,
        entity_type: Optional[EntityType] = None
    ) -> GroupTransformationConfig:
        """Get configuration for applying group transformations."""
        active_groups = self.get_active_groups(entity_type)
        return GroupTransformationConfig(active_groups=active_groups)

    def build_case_when_clause(
        self,
        column: str,
        groups: list[ActiveGroupInfo],
        alias: str = "grouped_entity"
    ) -> str:
        """Build a CASE WHEN clause for grouping entities.

        Args:
            column: The column to transform (e.g., "m.manufacturer_clean")
            groups: List of active groups to apply
            alias: Column alias for the result

        Returns:
            SQL CASE WHEN clause string
        """
        if not groups:
            return f"{column} as {alias}"

        cases = []
        for group in groups:
            if not group.members:
                continue

            # Build IN clause with proper quoting
            placeholders = ", ".join([f"'{m.replace(chr(39), chr(39)+chr(39))}'" for m in group.members])
            display = group.display_name.replace("'", "''")
            cases.append(f"WHEN {column} IN ({placeholders}) THEN '{display}'")

        if not cases:
            return f"{column} as {alias}"

        return f"""CASE
    {chr(10).join('    ' + c for c in cases)}
    ELSE {column}
END as {alias}"""

    def build_manufacturer_grouping_clause(
        self,
        groups: Optional[list[ActiveGroupInfo]] = None,
        alias: str = "grouped_manufacturer"
    ) -> str:
        """Build CASE WHEN clause specifically for manufacturer grouping.

        Args:
            groups: List of active groups (fetched if not provided)
            alias: Column alias for the result

        Returns:
            SQL CASE WHEN clause string
        """
        if groups is None:
            groups = self.get_active_groups(EntityType.MANUFACTURER)

        return self.build_case_when_clause(
            column="m.manufacturer_clean",
            groups=groups,
            alias=alias,
        )

    def get_group_members_set(
        self,
        entity_type: EntityType = EntityType.MANUFACTURER
    ) -> dict[str, str]:
        """Get a mapping of member -> display_name for all active groups.

        Useful for post-processing query results.

        Returns:
            Dict mapping member names to their group display names
        """
        groups = self.get_active_groups(entity_type)
        member_map = {}
        for group in groups:
            for member in group.members:
                member_map[member] = group.display_name
        return member_map

    def transform_entity_results(
        self,
        results: list[dict],
        entity_key: str = "entity",
        entity_type: EntityType = EntityType.MANUFACTURER
    ) -> list[dict]:
        """Transform query results to apply group display names.

        This is an alternative to SQL-based transformation, useful when
        the query can't be modified directly.

        Args:
            results: List of dicts with entity data
            entity_key: Key in dict containing the entity name
            entity_type: Type of entity being transformed

        Returns:
            Transformed results with grouped entities combined
        """
        member_map = self.get_group_members_set(entity_type)

        # First pass: map entities to their group names
        grouped_results = {}
        for result in results:
            entity = result.get(entity_key)
            if not entity:
                continue

            # Check if entity belongs to a group
            group_name = member_map.get(entity, entity)

            if group_name not in grouped_results:
                grouped_results[group_name] = {
                    entity_key: group_name,
                    **{k: 0 for k in result if k != entity_key and isinstance(result[k], (int, float))}
                }

            # Aggregate numeric values
            for key, value in result.items():
                if key != entity_key and isinstance(value, (int, float)):
                    grouped_results[group_name][key] = grouped_results[group_name].get(key, 0) + value

        return list(grouped_results.values())

    def is_grouped_entity(
        self,
        entity_name: str,
        entity_type: EntityType = EntityType.MANUFACTURER
    ) -> bool:
        """Check if an entity name is a group display name."""
        groups = self.get_active_groups(entity_type)
        return any(g.display_name == entity_name for g in groups)

    def get_group_by_display_name(
        self,
        display_name: str,
        entity_type: EntityType = EntityType.MANUFACTURER
    ) -> Optional[ActiveGroupInfo]:
        """Get group info by its display name."""
        groups = self.get_active_groups(entity_type)
        for group in groups:
            if group.display_name == display_name:
                return group
        return None

    def expand_entity_to_members(
        self,
        entity_name: str,
        entity_type: EntityType = EntityType.MANUFACTURER
    ) -> list[str]:
        """If entity is a group, return its members. Otherwise return [entity_name]."""
        group = self.get_group_by_display_name(entity_name, entity_type)
        if group:
            return group.members
        return [entity_name]


# Singleton instance
_entity_group_service: Optional[EntityGroupService] = None


def get_entity_group_service() -> EntityGroupService:
    """Get singleton instance of EntityGroupService."""
    global _entity_group_service
    if _entity_group_service is None:
        _entity_group_service = EntityGroupService()
    return _entity_group_service
