/**
 * Group Indicator Badge
 *
 * Shows a badge indicating how many entity groups are active.
 * Clicking shows a dropdown with active groups.
 */

import { useState, useRef, useEffect } from 'react'
import { useEntityGroups } from '../../hooks/useEntityGroups'

interface GroupIndicatorProps {
  className?: string
}

export function GroupIndicator({ className = '' }: GroupIndicatorProps) {
  const { activeGroups, activeGroupCount, toggleGroup } = useEntityGroups()
  const [showDropdown, setShowDropdown] = useState(false)
  const dropdownRef = useRef<HTMLDivElement>(null)

  // Close dropdown on click outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setShowDropdown(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  if (activeGroupCount === 0) {
    return null
  }

  return (
    <div className={`relative ${className}`} ref={dropdownRef}>
      <button
        onClick={() => setShowDropdown(!showDropdown)}
        className="inline-flex items-center gap-1.5 px-2.5 py-1.5 bg-blue-100 text-blue-700 text-sm font-medium rounded-md hover:bg-blue-200 transition-colors"
      >
        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
        </svg>
        <span>{activeGroupCount} Group{activeGroupCount !== 1 ? 's' : ''} Active</span>
        <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {/* Dropdown */}
      {showDropdown && (
        <div className="absolute right-0 mt-1 w-72 bg-white border border-gray-200 rounded-lg shadow-lg z-50">
          <div className="px-4 py-2 border-b border-gray-200">
            <h4 className="text-sm font-medium text-gray-900">Active Entity Groups</h4>
            <p className="text-xs text-gray-500">
              These groups are applied to all analysis
            </p>
          </div>

          <div className="max-h-64 overflow-y-auto">
            {activeGroups.map(group => (
              <div
                key={group.id}
                className="px-4 py-3 border-b border-gray-100 last:border-b-0 hover:bg-gray-50"
              >
                <div className="flex items-center justify-between">
                  <div className="min-w-0 flex-1">
                    <h5 className="text-sm font-medium text-gray-900 truncate">
                      {group.displayName}
                    </h5>
                    <p className="text-xs text-gray-500 truncate">
                      {group.members.length} member{group.members.length !== 1 ? 's' : ''}
                    </p>
                  </div>
                  <button
                    onClick={() => toggleGroup(group.id)}
                    className="ml-2 p-1 text-gray-400 hover:text-red-500"
                    title="Deactivate group"
                  >
                    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                    </svg>
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

/**
 * Compact version showing just a badge with count.
 */
export function GroupBadge({ className = '' }: GroupIndicatorProps) {
  const { activeGroupCount } = useEntityGroups()

  if (activeGroupCount === 0) {
    return null
  }

  return (
    <span
      className={`inline-flex items-center justify-center w-5 h-5 text-xs font-bold bg-blue-600 text-white rounded-full ${className}`}
      title={`${activeGroupCount} entity group${activeGroupCount !== 1 ? 's' : ''} active`}
    >
      {activeGroupCount}
    </span>
  )
}

export default GroupIndicator
