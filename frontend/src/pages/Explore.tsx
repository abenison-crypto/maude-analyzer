import { useState, useRef, useEffect } from 'react'
import { Download, ChevronDown } from 'lucide-react'
import { AdvancedFilterBar } from '../components/filters'
import EventsTable from '../components/EventsTable'
import { useAdvancedFilters } from '../hooks/useAdvancedFilters'
import { useEventStats } from '../hooks/useEvents'

export default function ExplorePage() {
  const { filters, hasActiveFilters } = useAdvancedFilters()
  const { data: stats } = useEventStats()
  const [showExportMenu, setShowExportMenu] = useState(false)
  const exportRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (exportRef.current && !exportRef.current.contains(event.target as Node)) {
        setShowExportMenu(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  const handleExport = (format: 'csv' | 'xlsx') => {
    const params = new URLSearchParams()
    // Core filters
    if (filters.manufacturers.length) params.set('manufacturers', filters.manufacturers.join(','))
    if (filters.productCodes.length) params.set('product_codes', filters.productCodes.join(','))
    if (filters.eventTypes.length) params.set('event_types', filters.eventTypes.join(','))
    if (filters.dateFrom) params.set('date_from', filters.dateFrom)
    if (filters.dateTo) params.set('date_to', filters.dateTo)
    if (filters.searchText) params.set('search_text', filters.searchText)
    // Device filters
    if (filters.brandNames.length) params.set('brand_names', filters.brandNames.join(','))
    if (filters.genericNames.length) params.set('generic_names', filters.genericNames.join(','))
    if (filters.deviceManufacturers.length) params.set('device_manufacturers', filters.deviceManufacturers.join(','))
    if (filters.modelNumbers.length) params.set('model_numbers', filters.modelNumbers.join(','))
    if (filters.implantFlag) params.set('implant_flag', filters.implantFlag)
    if (filters.deviceProductCodes.length) params.set('device_product_codes', filters.deviceProductCodes.join(','))
    params.set('format', format)

    window.open(`/api/events/export?${params.toString()}`, '_blank')
    setShowExportMenu(false)
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Explore Events</h1>
          <p className="text-gray-600 mt-1">
            Search and browse FDA MAUDE adverse event reports
          </p>
        </div>
        <div className="relative" ref={exportRef}>
          <button
            onClick={() => setShowExportMenu(!showExportMenu)}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
          >
            <Download className="w-4 h-4" />
            Export
            <ChevronDown className="w-4 h-4" />
          </button>
          {showExportMenu && (
            <div className="absolute right-0 mt-2 w-48 bg-white rounded-md shadow-lg border border-gray-200 z-10">
              <button
                onClick={() => handleExport('csv')}
                className="w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100 rounded-t-md"
              >
                Export as CSV
              </button>
              <button
                onClick={() => handleExport('xlsx')}
                className="w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100 rounded-b-md"
              >
                Export as Excel (.xlsx)
              </button>
            </div>
          )}
        </div>
      </div>

      {/* Filters */}
      <AdvancedFilterBar />

      {/* Results Summary */}
      {stats && (
        <div className="text-sm text-gray-600">
          {hasActiveFilters ? (
            <span>
              Found <strong>{stats.total.toLocaleString()}</strong> events matching your filters
              ({stats.deaths.toLocaleString()} deaths, {stats.injuries.toLocaleString()} injuries, {stats.malfunctions.toLocaleString()} malfunctions)
            </span>
          ) : (
            <span>
              Showing all <strong>{stats.total.toLocaleString()}</strong> events
            </span>
          )}
        </div>
      )}

      {/* Events Table */}
      <EventsTable />
    </div>
  )
}
