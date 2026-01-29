"""Pydantic schemas for entity group management."""

from pydantic import BaseModel, Field
from typing import Optional, Literal
from enum import Enum


class EntityType(str, Enum):
    """Types of entities that can be grouped."""
    MANUFACTURER = "manufacturer"
    BRAND = "brand"
    GENERIC_NAME = "generic_name"


class EntityGroupBase(BaseModel):
    """Base schema for entity groups."""
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None
    entity_type: EntityType = EntityType.MANUFACTURER
    members: list[str] = Field(..., min_length=1)
    display_name: Optional[str] = Field(
        default=None,
        description="Custom display name. If null, auto-generated from members."
    )


class CreateEntityGroupRequest(EntityGroupBase):
    """Request schema for creating an entity group."""
    pass


class UpdateEntityGroupRequest(BaseModel):
    """Request schema for updating an entity group."""
    name: Optional[str] = Field(default=None, min_length=1, max_length=200)
    description: Optional[str] = None
    members: Optional[list[str]] = Field(default=None, min_length=1)
    display_name: Optional[str] = None
    is_active: Optional[bool] = None


class EntityGroupResponse(BaseModel):
    """Response schema for entity groups."""
    id: str
    name: str
    description: Optional[str]
    entity_type: EntityType
    members: list[str]
    display_name: str  # Always populated (auto-generated if not custom)
    is_active: bool
    is_built_in: bool
    created_at: str
    updated_at: str


class EntityGroupListResponse(BaseModel):
    """Response schema for listing entity groups."""
    groups: list[EntityGroupResponse]
    total: int


class SuggestNameRequest(BaseModel):
    """Request schema for suggesting a group name."""
    members: list[str] = Field(..., min_length=1)
    entity_type: EntityType = EntityType.MANUFACTURER


class SuggestNameResponse(BaseModel):
    """Response schema for name suggestions."""
    suggested_name: str
    member_count: int


class ActiveGroupInfo(BaseModel):
    """Information about an active group for query transformation."""
    id: str
    name: str
    display_name: str
    members: list[str]
    entity_type: EntityType


class GroupTransformationConfig(BaseModel):
    """Configuration for applying group transformations to queries."""
    active_groups: list[ActiveGroupInfo] = []

    def get_manufacturer_groups(self) -> list[ActiveGroupInfo]:
        """Get only manufacturer groups."""
        return [g for g in self.active_groups if g.entity_type == EntityType.MANUFACTURER]

    def get_brand_groups(self) -> list[ActiveGroupInfo]:
        """Get only brand groups."""
        return [g for g in self.active_groups if g.entity_type == EntityType.BRAND]

    def get_generic_name_groups(self) -> list[ActiveGroupInfo]:
        """Get only generic name groups."""
        return [g for g in self.active_groups if g.entity_type == EntityType.GENERIC_NAME]


class AvailableEntity(BaseModel):
    """An entity available for group selection."""
    name: str
    event_count: int
    assigned_group_id: Optional[str] = None
    assigned_group_name: Optional[str] = None


class AvailableEntitiesResponse(BaseModel):
    """Response for available entities endpoint."""
    entities: list[AvailableEntity]
    total: int
