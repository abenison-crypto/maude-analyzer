/**
 * Create/Edit Group Dialog
 *
 * Modal dialog for creating or editing entity groups.
 * Supports member selection via filterable table with event counts.
 */

import { useState, useEffect, useCallback } from 'react'
import { useEntityGroups } from '../../hooks/useEntityGroups'
import { EntitySelectionTable } from './EntitySelectionTable'
import type { EntityGroup, EntityType, CreateEntityGroupRequest } from '../../types/entityGroups'

interface CreateGroupDialogProps {
  entityType: EntityType
  group?: EntityGroup | null // If provided, edit mode
  onClose: () => void
  onSuccess: () => void
}

export function CreateGroupDialog({
  entityType,
  group,
  onClose,
  onSuccess,
}: CreateGroupDialogProps) {
  const { createGroup, updateGroup, suggestName } = useEntityGroups()

  const isEdit = !!group
  const [name, setName] = useState(group?.name || '')
  const [description, setDescription] = useState(group?.description || '')
  const [displayName, setDisplayName] = useState(group?.displayName || '')
  const [useAutoName, setUseAutoName] = useState(!group?.displayName)
  const [members, setMembers] = useState<string[]>(group?.members || [])
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Auto-generate display name when members change
  useEffect(() => {
    if (useAutoName && members.length > 0) {
      suggestName(members).then(suggested => {
        setDisplayName(suggested)
      })
    }
  }, [members, useAutoName, suggestName])

  const handleMembersChange = useCallback((newMembers: string[]) => {
    setMembers(newMembers)
  }, [])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError(null)

    if (!name.trim()) {
      setError('Group name is required')
      return
    }
    if (members.length < 1) {
      setError('At least one member is required')
      return
    }

    setSaving(true)
    try {
      if (isEdit && group) {
        await updateGroup(group.id, {
          name: name.trim(),
          description: description.trim() || undefined,
          members,
          display_name: useAutoName ? undefined : displayName.trim(),
        })
      } else {
        const request: CreateEntityGroupRequest = {
          name: name.trim(),
          description: description.trim() || undefined,
          entity_type: entityType,
          members,
          display_name: useAutoName ? undefined : displayName.trim(),
        }
        await createGroup(request)
      }
      onSuccess()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to save group')
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="fixed inset-0 z-50 overflow-y-auto">
      <div className="flex min-h-screen items-center justify-center p-4">
        {/* Backdrop */}
        <div
          className="fixed inset-0 bg-black bg-opacity-50 transition-opacity"
          onClick={onClose}
        />

        {/* Dialog */}
        <div className="relative bg-white rounded-lg shadow-xl max-w-lg w-full">
          <form onSubmit={handleSubmit}>
            {/* Header */}
            <div className="px-6 py-4 border-b border-gray-200">
              <h3 className="text-lg font-semibold text-gray-900">
                {isEdit ? 'Edit Group' : 'Create Entity Group'}
              </h3>
              <p className="text-sm text-gray-500 mt-1">
                Group related {entityType === 'manufacturer' ? 'manufacturers' : entityType === 'brand' ? 'brands' : 'generic names'} together
              </p>
            </div>

            {/* Body */}
            <div className="px-6 py-4 space-y-4">
              {error && (
                <div className="p-3 bg-red-50 border border-red-200 rounded-md text-sm text-red-600">
                  {error}
                </div>
              )}

              {/* Group Name */}
              <div>
                <label className="block text-sm font-medium text-gray-700">
                  Group Name <span className="text-red-500">*</span>
                </label>
                <input
                  type="text"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  className="mt-1 block w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
                  placeholder="e.g., Abbott (with St. Jude)"
                />
              </div>

              {/* Description */}
              <div>
                <label className="block text-sm font-medium text-gray-700">
                  Description
                </label>
                <input
                  type="text"
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                  className="mt-1 block w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
                  placeholder="Optional description"
                />
              </div>

              {/* Members */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Select Members <span className="text-red-500">*</span>
                </label>
                <EntitySelectionTable
                  entityType={entityType}
                  selectedMembers={members}
                  onSelectionChange={handleMembersChange}
                  excludeGroupId={isEdit ? group?.id : undefined}
                />
              </div>

              {/* Display Name */}
              <div>
                <div className="flex items-center justify-between mb-1">
                  <label className="block text-sm font-medium text-gray-700">
                    Display Name
                  </label>
                  <label className="flex items-center gap-2 text-sm text-gray-600">
                    <input
                      type="checkbox"
                      checked={useAutoName}
                      onChange={(e) => setUseAutoName(e.target.checked)}
                      className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                    />
                    Auto-generate
                  </label>
                </div>
                <input
                  type="text"
                  value={displayName}
                  onChange={(e) => {
                    setDisplayName(e.target.value)
                    setUseAutoName(false)
                  }}
                  disabled={useAutoName}
                  className={`mt-1 block w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500 ${
                    useAutoName ? 'bg-gray-50 text-gray-500' : ''
                  }`}
                  placeholder="Name shown in charts and tables"
                />
                <p className="text-xs text-gray-500 mt-1">
                  This name will appear in analysis results instead of individual member names.
                </p>
              </div>
            </div>

            {/* Footer */}
            <div className="px-6 py-4 border-t border-gray-200 flex justify-end gap-3">
              <button
                type="button"
                onClick={onClose}
                className="px-4 py-2 text-sm font-medium text-gray-700 hover:text-gray-800"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={saving}
                className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
              >
                {saving && (
                  <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                  </svg>
                )}
                {isEdit ? 'Save Changes' : 'Create Group'}
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  )
}

export default CreateGroupDialog
