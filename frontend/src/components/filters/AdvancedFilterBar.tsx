/**
 * AdvancedFilterBar Component
 *
 * Main filter bar component with:
 * - Core and device filters
 * - Collapsible sections
 * - Preset selector
 * - Filter customizer
 * - Clear all button
 * - Active filter count badge
 */

import { Filter, X, Undo2, Redo2 } from 'lucide-react'
import { useAdvancedFilters } from '../../hooks/useAdvancedFilters'
import { api } from '../../api/client'
import FilterSection from './FilterSection'
import PresetSelector from './PresetSelector'
import FilterCustomizer from './FilterCustomizer'
import MultiSelectFilter from './fields/MultiSelectFilter'
import SingleSelectFilter from './fields/SingleSelectFilter'
import TextFilter from './fields/TextFilter'
import DateRangeFilter from './fields/DateRangeFilter'
import EventTypeFilter from './fields/EventTypeFilter'
import { FILTER_REGISTRY } from '../../constants/filterRegistry'

export default function AdvancedFilterBar() {
  const {
    filters,
    setFilter,
    setFilters,
    clearFilters,
    hasActiveFilters,
    activeFilterCount,
    visibilityConfig,
    toggleSection,
    canUndo,
    canRedo,
    undo,
    redo,
  } = useAdvancedFilters()

  // Count active filters per category
  const coreActiveCount = [
    filters.manufacturers.length > 0,
    filters.productCodes.length > 0,
    filters.eventTypes.length > 0,
    filters.dateFrom !== '',
    filters.dateTo !== '',
    filters.searchText !== '',
  ].filter(Boolean).length

  const deviceActiveCount = [
    filters.brandNames.length > 0,
    filters.genericNames.length > 0,
    filters.deviceManufacturers.length > 0,
    filters.modelNumbers.length > 0,
    filters.implantFlag !== '',
    filters.deviceProductCodes.length > 0,
  ].filter(Boolean).length

  const isVisible = (key: keyof typeof filters) => visibilityConfig.visibleFilters.has(key)

  return (
    <div className="bg-white rounded-lg shadow">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b">
        <div className="flex items-center gap-2">
          <Filter className="w-5 h-5 text-gray-500" />
          <span className="font-medium text-gray-900">Filters</span>
          {activeFilterCount > 0 && (
            <span className="px-2 py-0.5 text-xs font-medium bg-blue-100 text-blue-700 rounded-full">
              {activeFilterCount} active
            </span>
          )}
        </div>

        <div className="flex items-center gap-1">
          {/* Undo/Redo */}
          <button
            type="button"
            onClick={undo}
            disabled={!canUndo}
            className="p-2 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-md disabled:opacity-30 disabled:cursor-not-allowed"
            title="Undo"
          >
            <Undo2 className="w-4 h-4" />
          </button>
          <button
            type="button"
            onClick={redo}
            disabled={!canRedo}
            className="p-2 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-md disabled:opacity-30 disabled:cursor-not-allowed"
            title="Redo"
          >
            <Redo2 className="w-4 h-4" />
          </button>

          <div className="w-px h-5 bg-gray-200 mx-1" />

          {/* Presets */}
          <PresetSelector />

          {/* Customizer */}
          <FilterCustomizer />

          {/* Clear all */}
          {hasActiveFilters && (
            <button
              type="button"
              onClick={clearFilters}
              className="flex items-center gap-1 px-3 py-2 text-sm text-red-600 hover:text-red-700 hover:bg-red-50 rounded-md transition-colors"
            >
              <X className="w-4 h-4" />
              Clear all
            </button>
          )}
        </div>
      </div>

      {/* Core Filters Section */}
      <div className="px-4">
        <FilterSection
          category="core"
          isCollapsed={visibilityConfig.collapsedSections.core}
          onToggle={() => toggleSection('core')}
          activeCount={coreActiveCount}
        >
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
            {/* Search Text */}
            {isVisible('searchText') && (
              <TextFilter
                label={FILTER_REGISTRY.searchText.label}
                placeholder={FILTER_REGISTRY.searchText.placeholder}
                value={filters.searchText}
                onChange={(value) => setFilter('searchText', value)}
              />
            )}

            {/* Manufacturers */}
            {isVisible('manufacturers') && (
              <MultiSelectFilter
                label={FILTER_REGISTRY.manufacturers.label}
                placeholder={FILTER_REGISTRY.manufacturers.placeholder}
                values={filters.manufacturers}
                onChange={(values) => setFilter('manufacturers', values)}
                fetchOptions={(search) =>
                  api.getManufacturers(search, 20).then((items) =>
                    items.map((i) => ({ value: i.name, label: i.name, count: i.count }))
                  )
                }
              />
            )}

            {/* Product Codes */}
            {isVisible('productCodes') && (
              <MultiSelectFilter
                label={FILTER_REGISTRY.productCodes.label}
                placeholder={FILTER_REGISTRY.productCodes.placeholder}
                values={filters.productCodes}
                onChange={(values) => setFilter('productCodes', values)}
                fetchOptions={(search) =>
                  api.getProductCodes(search, 20).then((items) =>
                    items.map((i) => ({
                      value: i.code,
                      label: i.name ? `${i.code} - ${i.name}` : i.code,
                      count: i.count,
                    }))
                  )
                }
              />
            )}

            {/* Event Types */}
            {isVisible('eventTypes') && (
              <EventTypeFilter
                label={FILTER_REGISTRY.eventTypes.label}
                values={filters.eventTypes}
                onChange={(values) => setFilter('eventTypes', values)}
              />
            )}

            {/* Date Range */}
            {(isVisible('dateFrom') || isVisible('dateTo')) && (
              <DateRangeFilter
                label="Date Range"
                dateFrom={filters.dateFrom}
                dateTo={filters.dateTo}
                onChange={(from, to) => setFilters({ dateFrom: from, dateTo: to })}
              />
            )}
          </div>
        </FilterSection>

        {/* Device Filters Section */}
        <FilterSection
          category="device"
          isCollapsed={visibilityConfig.collapsedSections.device}
          onToggle={() => toggleSection('device')}
          activeCount={deviceActiveCount}
        >
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
            {/* Brand Names */}
            {isVisible('brandNames') && (
              <MultiSelectFilter
                label={FILTER_REGISTRY.brandNames.label}
                placeholder={FILTER_REGISTRY.brandNames.placeholder}
                values={filters.brandNames}
                onChange={(values) => setFilter('brandNames', values)}
                fetchOptions={(search) => api.getBrandNames(search, 20)}
              />
            )}

            {/* Generic Names */}
            {isVisible('genericNames') && (
              <MultiSelectFilter
                label={FILTER_REGISTRY.genericNames.label}
                placeholder={FILTER_REGISTRY.genericNames.placeholder}
                values={filters.genericNames}
                onChange={(values) => setFilter('genericNames', values)}
                fetchOptions={(search) => api.getGenericNames(search, 20)}
              />
            )}

            {/* Device Manufacturers */}
            {isVisible('deviceManufacturers') && (
              <MultiSelectFilter
                label={FILTER_REGISTRY.deviceManufacturers.label}
                placeholder={FILTER_REGISTRY.deviceManufacturers.placeholder}
                values={filters.deviceManufacturers}
                onChange={(values) => setFilter('deviceManufacturers', values)}
                fetchOptions={(search) => api.getDeviceManufacturers(search, 20)}
              />
            )}

            {/* Model Numbers */}
            {isVisible('modelNumbers') && (
              <MultiSelectFilter
                label={FILTER_REGISTRY.modelNumbers.label}
                placeholder={FILTER_REGISTRY.modelNumbers.placeholder}
                values={filters.modelNumbers}
                onChange={(values) => setFilter('modelNumbers', values)}
                fetchOptions={(search) => api.getModelNumbers(search, 20)}
              />
            )}

            {/* Implant Flag */}
            {isVisible('implantFlag') && (
              <SingleSelectFilter
                label={FILTER_REGISTRY.implantFlag.label}
                value={filters.implantFlag}
                onChange={(value) => setFilter('implantFlag', value as 'Y' | 'N' | '')}
                options={FILTER_REGISTRY.implantFlag.options || []}
              />
            )}

            {/* Device Product Codes */}
            {isVisible('deviceProductCodes') && (
              <MultiSelectFilter
                label={FILTER_REGISTRY.deviceProductCodes.label}
                placeholder={FILTER_REGISTRY.deviceProductCodes.placeholder}
                values={filters.deviceProductCodes}
                onChange={(values) => setFilter('deviceProductCodes', values)}
                fetchOptions={(search) => api.getDeviceProductCodes(search, 20)}
              />
            )}
          </div>
        </FilterSection>
      </div>
    </div>
  )
}
