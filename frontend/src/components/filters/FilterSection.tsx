/**
 * FilterSection Component
 *
 * Collapsible section for grouping related filters
 */

import { ReactNode } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import type { FilterCategory } from '../../types/filters'
import { getCategoryDefinition } from '../../constants/filterRegistry'

interface FilterSectionProps {
  category: FilterCategory
  isCollapsed: boolean
  onToggle: () => void
  children: ReactNode
  activeCount?: number
}

export default function FilterSection({
  category,
  isCollapsed,
  onToggle,
  children,
  activeCount = 0,
}: FilterSectionProps) {
  const categoryDef = getCategoryDefinition(category)
  if (!categoryDef) return null

  return (
    <div className="border-b border-gray-200 last:border-b-0">
      <button
        type="button"
        onClick={onToggle}
        className="w-full flex items-center justify-between py-3 text-left hover:bg-gray-50 transition-colors"
      >
        <div className="flex items-center gap-2">
          {isCollapsed ? (
            <ChevronRight className="w-4 h-4 text-gray-400" />
          ) : (
            <ChevronDown className="w-4 h-4 text-gray-400" />
          )}
          <span className="font-medium text-gray-900">{categoryDef.label}</span>
          {activeCount > 0 && (
            <span className="px-2 py-0.5 text-xs font-medium bg-blue-100 text-blue-700 rounded-full">
              {activeCount}
            </span>
          )}
        </div>
        <span className="text-xs text-gray-500">{categoryDef.description}</span>
      </button>

      {!isCollapsed && (
        <div className="pb-4 space-y-4">
          {children}
        </div>
      )}
    </div>
  )
}
