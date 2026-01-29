/**
 * Entity Selection Table
 *
 * A searchable, filterable table for selecting entities (manufacturers, brands, etc.)
 * to add to a group. Shows event counts and existing group assignments.
 * Respects global filters (product codes, event types).
 */

import { useState, useEffect, useMemo, useCallback } from 'react'
import { api } from '../../api/client'
import { useAdvancedFilters } from '../../hooks/useAdvancedFilters'
import type { EntityType, AvailableEntity } from '../../types/entityGroups'

interface EntitySelectionTableProps {
  entityType: EntityType
  selectedMembers: string[]
  onSelectionChange: (members: string[]) => void
  excludeGroupId?: string // Don't show "assigned" for the group being edited
}

export function EntitySelectionTable({
  entityType,
  selectedMembers,
  onSelectionChange,
  excludeGroupId,
}: EntitySelectionTableProps) {
  const { filters } = useAdvancedFilters()
  const [entities, setEntities] = useState<AvailableEntity[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [search, setSearch] = useState('')

  // Fetch available entities when filters change
  useEffect(() => {
    const fetchEntities = async () => {
      setLoading(true)
      setError(null)
      try {
        const response = await api.getAvailableEntities({
          entityType,
          productCodes: filters.productCodes,
          eventTypes: filters.eventTypes,
          limit: 200,
        })
        setEntities(response.entities)
      } catch (err) {
        console.error('Failed to fetch available entities:', err)
        setError('Failed to load entities')
      } finally {
        setLoading(false)
      }
    }

    fetchEntities()
  }, [entityType, filters.productCodes, filters.eventTypes])

  // Filter entities by search term
  const filteredEntities = useMemo(() => {
    if (!search.trim()) return entities
    const searchLower = search.toLowerCase()
    return entities.filter(e => e.name.toLowerCase().includes(searchLower))
  }, [entities, search])

  // Check if an entity is assigned to another group (not the one being edited)
  const isAssignedToOtherGroup = useCallback((entity: AvailableEntity) => {
    if (!entity.assigned_group_id) return false
    if (excludeGroupId && entity.assigned_group_id === excludeGroupId) return false
    return true
  }, [excludeGroupId])

  // Selection handlers
  const isSelected = useCallback((name: string) => {
    return selectedMembers.includes(name)
  }, [selectedMembers])

  const toggleSelection = useCallback((name: string) => {
    if (isSelected(name)) {
      onSelectionChange(selectedMembers.filter(m => m !== name))
    } else {
      onSelectionChange([...selectedMembers, name])
    }
  }, [selectedMembers, onSelectionChange, isSelected])

  const selectAll = useCallback(() => {
    const newMembers = new Set(selectedMembers)
    filteredEntities.forEach(e => newMembers.add(e.name))
    onSelectionChange(Array.from(newMembers))
  }, [filteredEntities, selectedMembers, onSelectionChange])

  const deselectAll = useCallback(() => {
    const filteredNames = new Set(filteredEntities.map(e => e.name))
    onSelectionChange(selectedMembers.filter(m => !filteredNames.has(m)))
  }, [filteredEntities, selectedMembers, onSelectionChange])

  // Check if all visible entities are selected
  const allVisibleSelected = useMemo(() => {
    if (filteredEntities.length === 0) return false
    return filteredEntities.every(e => selectedMembers.includes(e.name))
  }, [filteredEntities, selectedMembers])

  const someVisibleSelected = useMemo(() => {
    return filteredEntities.some(e => selectedMembers.includes(e.name)) && !allVisibleSelected
  }, [filteredEntities, selectedMembers, allVisibleSelected])

  // Format event count
  const formatCount = (count: number) => {
    return count.toLocaleString()
  }

  // Get entity type label
  const entityLabel = entityType === 'manufacturer' ? 'manufacturers' :
    entityType === 'brand' ? 'brands' : 'generic names'

  if (error) {
    return (
      <div className="p-4 bg-red-50 border border-red-200 rounded-md text-sm text-red-600">
        {error}
      </div>
    )
  }

  return (
    <div className="border border-gray-200 rounded-md">
      {/* Header with search and select all */}
      <div className="px-3 py-2 bg-gray-50 border-b border-gray-200 flex items-center gap-3">
        <div className="relative flex-1">
          <svg
            className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400"
            fill="none"
            stroke="currentColor"
            viewBox="0 0 24 24"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
            />
          </svg>
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="w-full pl-9 pr-3 py-1.5 text-sm border border-gray-300 rounded focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
            placeholder={`Search ${entityLabel}...`}
          />
        </div>
        <button
          type="button"
          onClick={allVisibleSelected ? deselectAll : selectAll}
          className="text-sm text-blue-600 hover:text-blue-700 whitespace-nowrap"
        >
          {allVisibleSelected ? 'Deselect All' : 'Select All'}
        </button>
      </div>

      {/* Global filter indicator */}
      {(filters.productCodes?.length || filters.eventTypes?.length) && (
        <div className="px-3 py-1.5 bg-blue-50 border-b border-blue-100 text-xs text-blue-700">
          Filtered by: {filters.productCodes?.length ? `${filters.productCodes.length} product code(s)` : ''}
          {filters.productCodes?.length && filters.eventTypes?.length ? ', ' : ''}
          {filters.eventTypes?.length ? `${filters.eventTypes.length} event type(s)` : ''}
        </div>
      )}

      {/* Table */}
      <div className="max-h-64 overflow-y-auto">
        {loading ? (
          <div className="p-8 text-center text-gray-500">
            <svg className="animate-spin h-6 w-6 mx-auto mb-2" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
            </svg>
            Loading {entityLabel}...
          </div>
        ) : filteredEntities.length === 0 ? (
          <div className="p-8 text-center text-gray-500">
            {search ? `No ${entityLabel} match "${search}"` : `No ${entityLabel} found`}
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead className="bg-gray-50 sticky top-0">
              <tr>
                <th className="w-10 px-3 py-2 text-left">
                  <input
                    type="checkbox"
                    checked={allVisibleSelected}
                    ref={(el) => {
                      if (el) el.indeterminate = someVisibleSelected
                    }}
                    onChange={() => allVisibleSelected ? deselectAll() : selectAll()}
                    className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                  />
                </th>
                <th className="px-3 py-2 text-left font-medium text-gray-700">
                  {entityType === 'manufacturer' ? 'Manufacturer' :
                   entityType === 'brand' ? 'Brand' : 'Generic Name'}
                </th>
                <th className="px-3 py-2 text-right font-medium text-gray-700 w-24">Events</th>
                <th className="px-3 py-2 text-left font-medium text-gray-700 w-32">Group</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {filteredEntities.map((entity) => {
                const selected = isSelected(entity.name)
                const assignedOther = isAssignedToOtherGroup(entity)

                return (
                  <tr
                    key={entity.name}
                    className={`hover:bg-gray-50 cursor-pointer ${selected ? 'bg-blue-50' : ''}`}
                    onClick={() => toggleSelection(entity.name)}
                  >
                    <td className="px-3 py-2">
                      <input
                        type="checkbox"
                        checked={selected}
                        onChange={() => toggleSelection(entity.name)}
                        onClick={(e) => e.stopPropagation()}
                        className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                      />
                    </td>
                    <td className="px-3 py-2 truncate max-w-[200px]" title={entity.name}>
                      {entity.name}
                    </td>
                    <td className="px-3 py-2 text-right text-gray-600 tabular-nums">
                      {formatCount(entity.event_count)}
                    </td>
                    <td className="px-3 py-2">
                      {assignedOther && entity.assigned_group_name && (
                        <span className="inline-flex items-center px-2 py-0.5 rounded text-xs bg-amber-100 text-amber-800">
                          In: {entity.assigned_group_name.length > 15
                            ? entity.assigned_group_name.slice(0, 15) + '...'
                            : entity.assigned_group_name}
                        </span>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </div>

      {/* Footer with selection count */}
      <div className="px-3 py-2 bg-gray-50 border-t border-gray-200 text-sm text-gray-600">
        {selectedMembers.length} selected
        {filteredEntities.length !== entities.length && (
          <span className="ml-2 text-gray-400">
            (showing {filteredEntities.length} of {entities.length})
          </span>
        )}
      </div>
    </div>
  )
}

export default EntitySelectionTable
