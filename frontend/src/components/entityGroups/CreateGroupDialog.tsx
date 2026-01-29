/**
 * Create/Edit Group Dialog
 *
 * Modal dialog for creating or editing entity groups.
 * Supports member selection with autocomplete and name suggestion.
 */

import { useState, useEffect, useCallback, useRef } from 'react'
import { useEntityGroups } from '../../hooks/useEntityGroups'
import { api } from '../../api/client'
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
  const [memberSearch, setMemberSearch] = useState('')
  const [suggestions, setSuggestions] = useState<Array<{ value: string; label: string; count?: number }>>([])
  const [showSuggestions, setShowSuggestions] = useState(false)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const searchInputRef = useRef<HTMLInputElement>(null)
  const suggestionsRef = useRef<HTMLDivElement>(null)

  // Fetch manufacturer/entity suggestions
  useEffect(() => {
    if (!memberSearch.trim()) {
      setSuggestions([])
      return
    }

    const fetchSuggestions = async () => {
      try {
        let items: Array<{ value: string; label: string; count?: number }> = []

        if (entityType === 'manufacturer') {
          const mfrs = await api.getManufacturers(memberSearch, 10)
          items = mfrs.map(m => ({ value: m.name, label: m.name, count: m.count }))
        } else if (entityType === 'brand') {
          const brands = await api.getBrandNames(memberSearch, 10)
          items = brands
        } else if (entityType === 'generic_name') {
          const generics = await api.getGenericNames(memberSearch, 10)
          items = generics
        }

        // Filter out already selected members
        items = items.filter(item => !members.includes(item.value))
        setSuggestions(items)
        setShowSuggestions(items.length > 0)
      } catch (err) {
        console.error('Failed to fetch suggestions:', err)
      }
    }

    const debounce = setTimeout(fetchSuggestions, 200)
    return () => clearTimeout(debounce)
  }, [memberSearch, entityType, members])

  // Auto-generate display name when members change
  useEffect(() => {
    if (useAutoName && members.length > 0) {
      suggestName(members).then(suggested => {
        setDisplayName(suggested)
      })
    }
  }, [members, useAutoName, suggestName])

  // Close suggestions on click outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (
        suggestionsRef.current &&
        !suggestionsRef.current.contains(event.target as Node) &&
        searchInputRef.current &&
        !searchInputRef.current.contains(event.target as Node)
      ) {
        setShowSuggestions(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  const handleAddMember = useCallback((value: string) => {
    if (value && !members.includes(value)) {
      setMembers(prev => [...prev, value])
    }
    setMemberSearch('')
    setShowSuggestions(false)
    searchInputRef.current?.focus()
  }, [members])

  const handleRemoveMember = useCallback((value: string) => {
    setMembers(prev => prev.filter(m => m !== value))
  }, [])

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && memberSearch.trim()) {
      e.preventDefault()
      // Add first suggestion or raw text
      if (suggestions.length > 0) {
        handleAddMember(suggestions[0].value)
      } else {
        handleAddMember(memberSearch.trim())
      }
    } else if (e.key === 'Escape') {
      setShowSuggestions(false)
    }
  }

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
                  Members <span className="text-red-500">*</span>
                </label>

                {/* Member chips */}
                {members.length > 0 && (
                  <div className="flex flex-wrap gap-2 mb-2">
                    {members.map(member => (
                      <span
                        key={member}
                        className="inline-flex items-center gap-1 px-2 py-1 bg-blue-100 text-blue-800 text-sm rounded"
                      >
                        <span className="truncate max-w-[200px]">{member}</span>
                        <button
                          type="button"
                          onClick={() => handleRemoveMember(member)}
                          className="hover:text-blue-600"
                        >
                          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                          </svg>
                        </button>
                      </span>
                    ))}
                  </div>
                )}

                {/* Search input */}
                <div className="relative">
                  <input
                    ref={searchInputRef}
                    type="text"
                    value={memberSearch}
                    onChange={(e) => setMemberSearch(e.target.value)}
                    onFocus={() => suggestions.length > 0 && setShowSuggestions(true)}
                    onKeyDown={handleKeyDown}
                    className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
                    placeholder={`Search ${entityType === 'manufacturer' ? 'manufacturers' : entityType === 'brand' ? 'brands' : 'generic names'}...`}
                  />

                  {/* Suggestions dropdown */}
                  {showSuggestions && suggestions.length > 0 && (
                    <div
                      ref={suggestionsRef}
                      className="absolute z-10 w-full mt-1 bg-white border border-gray-200 rounded-md shadow-lg max-h-48 overflow-auto"
                    >
                      {suggestions.map(item => (
                        <button
                          key={item.value}
                          type="button"
                          onClick={() => handleAddMember(item.value)}
                          className="w-full text-left px-3 py-2 hover:bg-gray-100 text-sm flex items-center justify-between"
                        >
                          <span className="truncate">{item.label}</span>
                          {item.count !== undefined && (
                            <span className="text-gray-400 text-xs ml-2">
                              {item.count.toLocaleString()}
                            </span>
                          )}
                        </button>
                      ))}
                    </div>
                  )}
                </div>
                <p className="text-xs text-gray-500 mt-1">
                  Type to search and select members. Press Enter to add.
                </p>
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
