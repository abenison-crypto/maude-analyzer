import { useState } from 'react'
import { Search, X, Filter } from 'lucide-react'
import { useFilters } from '../hooks/useFilters'
import { useManufacturers, useProductCodes } from '../hooks/useEvents'

const EVENT_TYPES = [
  { value: 'D', label: 'Death' },
  { value: 'I', label: 'Injury' },
  { value: 'M', label: 'Malfunction' },
  { value: 'O', label: 'Other' },
]

export default function FilterBar() {
  const {
    filters,
    setManufacturers,
    setProductCodes,
    setEventTypes,
    setDateRange,
    setSearchText,
    clearFilters,
    hasActiveFilters,
  } = useFilters()

  const [mfrSearch, setMfrSearch] = useState('')
  const [showMfrDropdown, setShowMfrDropdown] = useState(false)
  const { data: manufacturers } = useManufacturers(mfrSearch)

  const [productSearch, setProductSearch] = useState('')
  const [showProductDropdown, setShowProductDropdown] = useState(false)
  const { data: productCodes } = useProductCodes(productSearch)

  const handleMfrSelect = (name: string) => {
    if (!filters.manufacturers.includes(name)) {
      setManufacturers([...filters.manufacturers, name])
    }
    setMfrSearch('')
    setShowMfrDropdown(false)
  }

  const removeMfr = (name: string) => {
    setManufacturers(filters.manufacturers.filter((m) => m !== name))
  }

  const handleProductSelect = (code: string) => {
    if (!filters.productCodes.includes(code)) {
      setProductCodes([...filters.productCodes, code])
    }
    setProductSearch('')
    setShowProductDropdown(false)
  }

  const removeProduct = (code: string) => {
    setProductCodes(filters.productCodes.filter((c) => c !== code))
  }

  const toggleEventType = (type: string) => {
    if (filters.eventTypes.includes(type)) {
      setEventTypes(filters.eventTypes.filter((t) => t !== type))
    } else {
      setEventTypes([...filters.eventTypes, type])
    }
  }

  return (
    <div className="bg-white rounded-lg shadow p-4 space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-gray-700">
          <Filter className="w-5 h-5" />
          <span className="font-medium">Filters</span>
        </div>
        {hasActiveFilters && (
          <button
            onClick={clearFilters}
            className="text-sm text-blue-600 hover:text-blue-800 flex items-center gap-1"
          >
            <X className="w-4 h-4" />
            Clear all
          </button>
        )}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {/* Search Text */}
        <div className="relative">
          <label className="block text-sm font-medium text-gray-700 mb-1">Search</label>
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              value={filters.searchText}
              onChange={(e) => setSearchText(e.target.value)}
              placeholder="Search narratives..."
              className="w-full pl-9 pr-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
        </div>

        {/* Manufacturer */}
        <div className="relative">
          <label className="block text-sm font-medium text-gray-700 mb-1">Manufacturer</label>
          <input
            type="text"
            value={mfrSearch}
            onChange={(e) => {
              setMfrSearch(e.target.value)
              setShowMfrDropdown(true)
            }}
            onFocus={() => setShowMfrDropdown(true)}
            placeholder="Search manufacturers..."
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
          />
          {showMfrDropdown && manufacturers && manufacturers.length > 0 && (
            <div className="absolute z-10 w-full mt-1 bg-white border border-gray-300 rounded-md shadow-lg max-h-60 overflow-auto">
              {manufacturers.slice(0, 10).map((mfr) => (
                <button
                  key={mfr.name}
                  onClick={() => handleMfrSelect(mfr.name)}
                  className="w-full text-left px-3 py-2 hover:bg-gray-100 text-sm"
                >
                  {mfr.name} <span className="text-gray-500">({mfr.count.toLocaleString()})</span>
                </button>
              ))}
            </div>
          )}
          {filters.manufacturers.length > 0 && (
            <div className="flex flex-wrap gap-1 mt-2">
              {filters.manufacturers.map((m) => (
                <span
                  key={m}
                  className="inline-flex items-center gap-1 px-2 py-1 bg-blue-100 text-blue-800 text-xs rounded"
                >
                  {m.substring(0, 20)}...
                  <button onClick={() => removeMfr(m)}>
                    <X className="w-3 h-3" />
                  </button>
                </span>
              ))}
            </div>
          )}
        </div>

        {/* Date Range */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Date Range</label>
          <div className="flex gap-2">
            <input
              type="date"
              value={filters.dateFrom}
              onChange={(e) => setDateRange(e.target.value, filters.dateTo)}
              className="flex-1 px-2 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
            />
            <input
              type="date"
              value={filters.dateTo}
              onChange={(e) => setDateRange(filters.dateFrom, e.target.value)}
              className="flex-1 px-2 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
            />
          </div>
        </div>

        {/* Event Types */}
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Event Type</label>
          <div className="flex flex-wrap gap-2">
            {EVENT_TYPES.map((type) => (
              <button
                key={type.value}
                onClick={() => toggleEventType(type.value)}
                className={`px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
                  filters.eventTypes.includes(type.value)
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                {type.label}
              </button>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}
