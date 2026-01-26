import { Download } from 'lucide-react'
import FilterBar from '../components/FilterBar'
import EventsTable from '../components/EventsTable'
import { useFilters } from '../hooks/useFilters'
import { useEventStats } from '../hooks/useEvents'

export default function ExplorePage() {
  const { filters, hasActiveFilters } = useFilters()
  const { data: stats } = useEventStats()

  const handleExport = () => {
    const params = new URLSearchParams()
    if (filters.manufacturers.length) params.set('manufacturers', filters.manufacturers.join(','))
    if (filters.productCodes.length) params.set('product_codes', filters.productCodes.join(','))
    if (filters.eventTypes.length) params.set('event_types', filters.eventTypes.join(','))
    if (filters.dateFrom) params.set('date_from', filters.dateFrom)
    if (filters.dateTo) params.set('date_to', filters.dateTo)
    if (filters.searchText) params.set('search_text', filters.searchText)

    window.open(`/api/events/export?${params.toString()}`, '_blank')
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
        <button
          onClick={handleExport}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
        >
          <Download className="w-4 h-4" />
          Export CSV
        </button>
      </div>

      {/* Filters */}
      <FilterBar />

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
