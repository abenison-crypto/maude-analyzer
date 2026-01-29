/**
 * FilterCustomizer Component
 *
 * Modal/popover to customize which filters are visible
 */

import { useState, useRef, useEffect } from 'react'
import { Settings, X, Check } from 'lucide-react'
import { useAdvancedFilters } from '../../hooks/useAdvancedFilters'
import { FILTER_REGISTRY, FILTER_CATEGORIES } from '../../constants/filterRegistry'
import type { FilterValues } from '../../types/filters'

export default function FilterCustomizer() {
  const { visibilityConfig, toggleFilterVisibility } = useAdvancedFilters()
  const [isOpen, setIsOpen] = useState(false)
  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
        setIsOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  const handleToggle = (filterId: keyof FilterValues) => {
    toggleFilterVisibility(filterId)
  }

  // Group filters by category
  const filtersByCategory = FILTER_CATEGORIES.map((category) => ({
    ...category,
    filters: Object.values(FILTER_REGISTRY).filter((f) => f.category === category.id),
  }))

  return (
    <div className="relative" ref={containerRef}>
      <button
        type="button"
        onClick={() => setIsOpen(!isOpen)}
        className="p-2 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-md transition-colors"
        title="Customize filters"
      >
        <Settings className="w-4 h-4" />
      </button>

      {isOpen && (
        <div className="absolute right-0 mt-1 w-80 bg-white border border-gray-200 rounded-lg shadow-lg z-30">
          <div className="flex items-center justify-between px-4 py-3 border-b">
            <h3 className="font-medium text-gray-900">Customize Filters</h3>
            <button
              type="button"
              onClick={() => setIsOpen(false)}
              className="text-gray-400 hover:text-gray-600"
            >
              <X className="w-4 h-4" />
            </button>
          </div>

          <div className="max-h-96 overflow-auto p-2">
            {filtersByCategory.map((category) => (
              <div key={category.id} className="mb-3">
                <div className="px-2 py-1 text-xs font-medium text-gray-500 uppercase">
                  {category.label}
                </div>
                {category.filters.map((filter) => {
                  const isVisible = visibilityConfig.visibleFilters.has(filter.id)
                  return (
                    <button
                      key={filter.id}
                      type="button"
                      onClick={() => handleToggle(filter.id)}
                      className="w-full flex items-center justify-between px-2 py-1.5 text-sm hover:bg-gray-100 rounded"
                    >
                      <div className="flex items-center gap-2">
                        <div
                          className={`w-4 h-4 rounded border flex items-center justify-center ${
                            isVisible
                              ? 'bg-blue-600 border-blue-600'
                              : 'border-gray-300'
                          }`}
                        >
                          {isVisible && <Check className="w-3 h-3 text-white" />}
                        </div>
                        <span className="text-gray-700">{filter.label}</span>
                      </div>
                      {filter.coverage && (
                        <span className="text-xs text-gray-400">{filter.coverage}% coverage</span>
                      )}
                    </button>
                  )
                })}
              </div>
            ))}
          </div>

          <div className="px-4 py-3 border-t bg-gray-50 text-xs text-gray-500">
            Toggle filters to show or hide them in the filter bar
          </div>
        </div>
      )}
    </div>
  )
}
