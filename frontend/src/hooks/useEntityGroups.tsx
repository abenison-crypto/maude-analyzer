/**
 * Entity Groups Hook
 *
 * Context provider and hook for managing entity groups.
 * Handles CRUD operations, active state, and provides groups to signal analysis.
 */

import {
  createContext,
  useContext,
  useState,
  useCallback,
  useEffect,
  ReactNode,
  useMemo,
} from 'react'
import { useSearchParams } from 'react-router-dom'
import { api } from '../api/client'
import type {
  EntityGroup,
  EntityType,
  CreateEntityGroupRequest,
  UpdateEntityGroupRequest,
  ActiveEntityGroup,
} from '../types/entityGroups'
import { toActiveGroup } from '../types/entityGroups'

// URL param key for active groups
const ACTIVE_GROUPS_PARAM = 'activeGroups'

interface EntityGroupsContextValue {
  // All groups
  groups: EntityGroup[]
  loading: boolean
  error: string | null

  // Active groups (for signal analysis)
  activeGroups: EntityGroup[]
  activeGroupCount: number

  // Actions
  refreshGroups: () => Promise<void>
  createGroup: (request: CreateEntityGroupRequest) => Promise<EntityGroup>
  updateGroup: (groupId: string, request: UpdateEntityGroupRequest) => Promise<EntityGroup>
  deleteGroup: (groupId: string) => Promise<void>
  activateGroup: (groupId: string) => Promise<void>
  deactivateGroup: (groupId: string) => Promise<void>
  toggleGroup: (groupId: string) => Promise<void>

  // For signal queries
  getActiveGroupsForQuery: (entityType?: EntityType) => ActiveEntityGroup[]

  // Name suggestion
  suggestName: (members: string[]) => Promise<string>
}

const EntityGroupsContext = createContext<EntityGroupsContextValue | undefined>(undefined)

interface EntityGroupsProviderProps {
  children: ReactNode
}

export function EntityGroupsProvider({ children }: EntityGroupsProviderProps) {
  const [searchParams, setSearchParams] = useSearchParams()
  const [groups, setGroups] = useState<EntityGroup[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Parse active group IDs from URL
  const activeGroupIds = useMemo(() => {
    const param = searchParams.get(ACTIVE_GROUPS_PARAM)
    if (!param) return new Set<string>()
    return new Set(param.split(',').filter(Boolean))
  }, [searchParams])

  // Update URL with active group IDs
  const updateActiveGroupsUrl = useCallback((activeIds: Set<string>) => {
    const newParams = new URLSearchParams(searchParams)
    if (activeIds.size > 0) {
      newParams.set(ACTIVE_GROUPS_PARAM, Array.from(activeIds).join(','))
    } else {
      newParams.delete(ACTIVE_GROUPS_PARAM)
    }
    setSearchParams(newParams)
  }, [searchParams, setSearchParams])

  // Fetch all groups
  const refreshGroups = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const response = await api.getEntityGroups({ includeBuiltIn: true })
      setGroups(response.groups)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load entity groups')
      setGroups([])
    } finally {
      setLoading(false)
    }
  }, [])

  // Load groups on mount
  useEffect(() => {
    refreshGroups()
  }, [refreshGroups])

  // Sync isActive state with URL params after loading
  useEffect(() => {
    if (!loading && groups.length > 0 && activeGroupIds.size > 0) {
      // Update group active states based on URL
      setGroups(prev => prev.map(g => ({
        ...g,
        isActive: activeGroupIds.has(g.id),
      })))
    }
  }, [loading, activeGroupIds])

  // Compute active groups
  const activeGroups = useMemo(() => {
    return groups.filter(g => activeGroupIds.has(g.id) || g.isActive)
  }, [groups, activeGroupIds])

  const activeGroupCount = activeGroups.length

  // CRUD operations
  const createGroup = useCallback(async (request: CreateEntityGroupRequest): Promise<EntityGroup> => {
    const newGroup = await api.createEntityGroup(request)
    await refreshGroups()
    return newGroup
  }, [refreshGroups])

  const updateGroup = useCallback(async (groupId: string, request: UpdateEntityGroupRequest): Promise<EntityGroup> => {
    const updated = await api.updateEntityGroup(groupId, request)
    await refreshGroups()
    return updated
  }, [refreshGroups])

  const deleteGroup = useCallback(async (groupId: string): Promise<void> => {
    await api.deleteEntityGroup(groupId)
    // Remove from active groups if present
    if (activeGroupIds.has(groupId)) {
      const newActiveIds = new Set(activeGroupIds)
      newActiveIds.delete(groupId)
      updateActiveGroupsUrl(newActiveIds)
    }
    await refreshGroups()
  }, [refreshGroups, activeGroupIds, updateActiveGroupsUrl])

  const activateGroup = useCallback(async (groupId: string): Promise<void> => {
    await api.activateEntityGroup(groupId)
    const newActiveIds = new Set(activeGroupIds)
    newActiveIds.add(groupId)
    updateActiveGroupsUrl(newActiveIds)
    setGroups(prev => prev.map(g =>
      g.id === groupId ? { ...g, isActive: true } : g
    ))
  }, [activeGroupIds, updateActiveGroupsUrl])

  const deactivateGroup = useCallback(async (groupId: string): Promise<void> => {
    await api.deactivateEntityGroup(groupId)
    const newActiveIds = new Set(activeGroupIds)
    newActiveIds.delete(groupId)
    updateActiveGroupsUrl(newActiveIds)
    setGroups(prev => prev.map(g =>
      g.id === groupId ? { ...g, isActive: false } : g
    ))
  }, [activeGroupIds, updateActiveGroupsUrl])

  const toggleGroup = useCallback(async (groupId: string): Promise<void> => {
    const group = groups.find(g => g.id === groupId)
    if (!group) return

    if (activeGroupIds.has(groupId) || group.isActive) {
      await deactivateGroup(groupId)
    } else {
      await activateGroup(groupId)
    }
  }, [groups, activeGroupIds, activateGroup, deactivateGroup])

  // Get active groups formatted for signal query
  const getActiveGroupsForQuery = useCallback((entityType?: EntityType): ActiveEntityGroup[] => {
    let filtered = activeGroups
    if (entityType) {
      filtered = filtered.filter(g => g.entityType === entityType)
    }
    return filtered.map(toActiveGroup)
  }, [activeGroups])

  // Name suggestion
  const suggestName = useCallback(async (members: string[]): Promise<string> => {
    if (members.length === 0) return ''
    const response = await api.suggestGroupName(members)
    return response.suggested_name
  }, [])

  const value: EntityGroupsContextValue = {
    groups,
    loading,
    error,
    activeGroups,
    activeGroupCount,
    refreshGroups,
    createGroup,
    updateGroup,
    deleteGroup,
    activateGroup,
    deactivateGroup,
    toggleGroup,
    getActiveGroupsForQuery,
    suggestName,
  }

  return (
    <EntityGroupsContext.Provider value={value}>
      {children}
    </EntityGroupsContext.Provider>
  )
}

export function useEntityGroups() {
  const context = useContext(EntityGroupsContext)
  if (!context) {
    throw new Error('useEntityGroups must be used within an EntityGroupsProvider')
  }
  return context
}

/**
 * Hook to get just the active groups for query use.
 * Lighter weight than full useEntityGroups context.
 */
export function useActiveEntityGroups(entityType?: EntityType) {
  const { activeGroups, activeGroupCount, getActiveGroupsForQuery } = useEntityGroups()

  const queryGroups = useMemo(() => {
    return getActiveGroupsForQuery(entityType)
  }, [getActiveGroupsForQuery, entityType])

  return {
    activeGroups,
    activeGroupCount,
    queryGroups,
  }
}
