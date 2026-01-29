/**
 * Entity Groups Manager
 *
 * Main UI component for managing entity groups.
 * Shows list of groups with toggle, edit, and delete actions.
 */

import { useState } from 'react'
import { useEntityGroups } from '../../hooks/useEntityGroups'
import type { EntityGroup } from '../../types/entityGroups'
import { CreateGroupDialog } from './CreateGroupDialog'

interface EntityGroupsManagerProps {
  entityType?: 'manufacturer' | 'brand' | 'generic_name'
}

export function EntityGroupsManager({ entityType = 'manufacturer' }: EntityGroupsManagerProps) {
  const {
    groups,
    loading,
    error,
    toggleGroup,
    deleteGroup,
    refreshGroups,
  } = useEntityGroups()

  const [showCreateDialog, setShowCreateDialog] = useState(false)
  const [editingGroup, setEditingGroup] = useState<EntityGroup | null>(null)
  const [deleteConfirmId, setDeleteConfirmId] = useState<string | null>(null)

  // Filter groups by entity type
  const filteredGroups = groups.filter(g => g.entityType === entityType)
  const activeCount = filteredGroups.filter(g => g.isActive).length

  const handleDelete = async (groupId: string) => {
    try {
      await deleteGroup(groupId)
      setDeleteConfirmId(null)
    } catch (err) {
      console.error('Failed to delete group:', err)
    }
  }

  const handleToggle = async (groupId: string) => {
    try {
      await toggleGroup(groupId)
    } catch (err) {
      console.error('Failed to toggle group:', err)
    }
  }

  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="animate-pulse space-y-4">
          <div className="h-6 bg-gray-200 rounded w-1/4"></div>
          <div className="h-10 bg-gray-200 rounded"></div>
          <div className="h-10 bg-gray-200 rounded"></div>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="text-red-600 flex items-center gap-2">
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <span>Failed to load entity groups: {error}</span>
        </div>
        <button
          onClick={refreshGroups}
          className="mt-4 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
        >
          Retry
        </button>
      </div>
    )
  }

  return (
    <div className="bg-white rounded-lg shadow">
      {/* Header */}
      <div className="px-6 py-4 border-b border-gray-200 flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold text-gray-900">Entity Groups</h3>
          <p className="text-sm text-gray-500 mt-1">
            Group related {entityType === 'manufacturer' ? 'manufacturers' : entityType === 'brand' ? 'brands' : 'generic names'} to analyze them as single entities
          </p>
        </div>
        <div className="flex items-center gap-4">
          {activeCount > 0 && (
            <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
              {activeCount} active
            </span>
          )}
          <button
            onClick={() => setShowCreateDialog(true)}
            className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-md hover:bg-blue-700 flex items-center gap-2"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
            </svg>
            Create Group
          </button>
        </div>
      </div>

      {/* Groups List */}
      <div className="divide-y divide-gray-200">
        {filteredGroups.length === 0 ? (
          <div className="px-6 py-12 text-center">
            <svg className="mx-auto h-12 w-12 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
            </svg>
            <h3 className="mt-2 text-sm font-medium text-gray-900">No groups yet</h3>
            <p className="mt-1 text-sm text-gray-500">
              Create a group to combine related entities for analysis.
            </p>
            <button
              onClick={() => setShowCreateDialog(true)}
              className="mt-4 px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-md hover:bg-blue-700"
            >
              Create your first group
            </button>
          </div>
        ) : (
          filteredGroups.map(group => (
            <GroupRow
              key={group.id}
              group={group}
              onToggle={() => handleToggle(group.id)}
              onEdit={() => setEditingGroup(group)}
              onDelete={() => setDeleteConfirmId(group.id)}
              showDeleteConfirm={deleteConfirmId === group.id}
              onConfirmDelete={() => handleDelete(group.id)}
              onCancelDelete={() => setDeleteConfirmId(null)}
            />
          ))
        )}
      </div>

      {/* Create/Edit Dialog */}
      {(showCreateDialog || editingGroup) && (
        <CreateGroupDialog
          entityType={entityType}
          group={editingGroup}
          onClose={() => {
            setShowCreateDialog(false)
            setEditingGroup(null)
          }}
          onSuccess={() => {
            setShowCreateDialog(false)
            setEditingGroup(null)
            refreshGroups()
          }}
        />
      )}
    </div>
  )
}

interface GroupRowProps {
  group: EntityGroup
  onToggle: () => void
  onEdit: () => void
  onDelete: () => void
  showDeleteConfirm: boolean
  onConfirmDelete: () => void
  onCancelDelete: () => void
}

function GroupRow({
  group,
  onToggle,
  onEdit,
  onDelete,
  showDeleteConfirm,
  onConfirmDelete,
  onCancelDelete,
}: GroupRowProps) {
  return (
    <div className={`px-6 py-4 ${group.isActive ? 'bg-blue-50' : ''}`}>
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4 min-w-0 flex-1">
          {/* Toggle Switch */}
          <button
            onClick={onToggle}
            className={`relative inline-flex h-6 w-11 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 ease-in-out focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 ${
              group.isActive ? 'bg-blue-600' : 'bg-gray-200'
            }`}
            role="switch"
            aria-checked={group.isActive}
          >
            <span
              className={`pointer-events-none inline-block h-5 w-5 transform rounded-full bg-white shadow ring-0 transition duration-200 ease-in-out ${
                group.isActive ? 'translate-x-5' : 'translate-x-0'
              }`}
            />
          </button>

          {/* Group Info */}
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <h4 className="text-sm font-medium text-gray-900 truncate">
                {group.displayName}
              </h4>
              {group.isBuiltIn && (
                <span className="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-600">
                  Built-in
                </span>
              )}
            </div>
            <p className="text-sm text-gray-500 truncate">
              {group.members.length} member{group.members.length !== 1 ? 's' : ''}: {group.members.slice(0, 3).join(', ')}
              {group.members.length > 3 && ` +${group.members.length - 3} more`}
            </p>
            {group.description && (
              <p className="text-xs text-gray-400 mt-1 truncate">{group.description}</p>
            )}
          </div>
        </div>

        {/* Actions */}
        <div className="flex items-center gap-2 ml-4">
          {showDeleteConfirm ? (
            <>
              <button
                onClick={onCancelDelete}
                className="px-3 py-1.5 text-sm text-gray-600 hover:text-gray-800"
              >
                Cancel
              </button>
              <button
                onClick={onConfirmDelete}
                className="px-3 py-1.5 text-sm bg-red-600 text-white rounded hover:bg-red-700"
              >
                Delete
              </button>
            </>
          ) : (
            <>
              {!group.isBuiltIn && (
                <>
                  <button
                    onClick={onEdit}
                    className="p-2 text-gray-400 hover:text-gray-600"
                    title="Edit group"
                  >
                    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z" />
                    </svg>
                  </button>
                  <button
                    onClick={onDelete}
                    className="p-2 text-gray-400 hover:text-red-600"
                    title="Delete group"
                  >
                    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                    </svg>
                  </button>
                </>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  )
}

export default EntityGroupsManager
