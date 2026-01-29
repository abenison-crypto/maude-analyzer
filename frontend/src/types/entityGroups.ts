/**
 * Entity Groups Types
 *
 * TypeScript types for entity grouping functionality.
 * Allows grouping manufacturers, brands, etc. into named groups
 * that are treated as single entities in analysis.
 */

export type EntityType = 'manufacturer' | 'brand' | 'generic_name'

export interface EntityGroup {
  id: string
  name: string
  description: string | null
  entityType: EntityType
  members: string[]
  displayName: string
  isActive: boolean
  isBuiltIn: boolean
  createdAt: string
  updatedAt: string
}

export interface EntityGroupListResponse {
  groups: EntityGroup[]
  total: number
}

export interface CreateEntityGroupRequest {
  name: string
  description?: string
  entity_type?: EntityType
  members: string[]
  display_name?: string
}

export interface UpdateEntityGroupRequest {
  name?: string
  description?: string
  members?: string[]
  display_name?: string
  is_active?: boolean
}

export interface SuggestNameResponse {
  suggested_name: string
  member_count: number
}

export interface AvailableEntity {
  name: string
  event_count: number
  assigned_group_id: string | null
  assigned_group_name: string | null
}

export interface AvailableEntitiesResponse {
  entities: AvailableEntity[]
  total: number
}

/**
 * Active group info for passing to signal queries.
 * This is sent to the backend to apply group transformations.
 */
export interface ActiveEntityGroup {
  id: string
  display_name: string
  members: string[]
  entity_type: EntityType
}

/**
 * Converts an EntityGroup to ActiveEntityGroup format for API calls.
 */
export function toActiveGroup(group: EntityGroup): ActiveEntityGroup {
  return {
    id: group.id,
    display_name: group.displayName,
    members: group.members,
    entity_type: group.entityType,
  }
}

/**
 * Converts API response to EntityGroup (camelCase conversion).
 */
export function fromApiResponse(data: Record<string, unknown>): EntityGroup {
  return {
    id: data.id as string,
    name: data.name as string,
    description: data.description as string | null,
    entityType: data.entity_type as EntityType,
    members: data.members as string[],
    displayName: data.display_name as string,
    isActive: data.is_active as boolean,
    isBuiltIn: data.is_built_in as boolean,
    createdAt: data.created_at as string,
    updatedAt: data.updated_at as string,
  }
}

/**
 * Converts API list response.
 */
export function fromApiListResponse(data: Record<string, unknown>): EntityGroupListResponse {
  const groups = (data.groups as Record<string, unknown>[]).map(fromApiResponse)
  return {
    groups,
    total: data.total as number,
  }
}
