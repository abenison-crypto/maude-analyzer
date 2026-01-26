import { createContext, useContext, useState, useCallback, ReactNode } from 'react'
import { useSearchParams } from 'react-router-dom'

export interface Filters {
  manufacturers: string[]
  productCodes: string[]
  eventTypes: string[]
  dateFrom: string
  dateTo: string
  searchText: string
}

interface FilterContextType {
  filters: Filters
  setManufacturers: (manufacturers: string[]) => void
  setProductCodes: (codes: string[]) => void
  setEventTypes: (types: string[]) => void
  setDateRange: (from: string, to: string) => void
  setSearchText: (text: string) => void
  clearFilters: () => void
  hasActiveFilters: boolean
}

const defaultFilters: Filters = {
  manufacturers: [],
  productCodes: [],
  eventTypes: [],
  dateFrom: '',
  dateTo: '',
  searchText: '',
}

const FilterContext = createContext<FilterContextType | undefined>(undefined)

export function FilterProvider({ children }: { children: ReactNode }) {
  const [searchParams, setSearchParams] = useSearchParams()

  // Initialize filters from URL params
  const getInitialFilters = (): Filters => {
    return {
      manufacturers: searchParams.get('manufacturers')?.split(',').filter(Boolean) || [],
      productCodes: searchParams.get('product_codes')?.split(',').filter(Boolean) || [],
      eventTypes: searchParams.get('event_types')?.split(',').filter(Boolean) || [],
      dateFrom: searchParams.get('date_from') || '',
      dateTo: searchParams.get('date_to') || '',
      searchText: searchParams.get('search') || '',
    }
  }

  const [filters, setFilters] = useState<Filters>(getInitialFilters)

  // Sync filters to URL
  const updateURL = useCallback((newFilters: Filters) => {
    const params = new URLSearchParams()
    if (newFilters.manufacturers.length) params.set('manufacturers', newFilters.manufacturers.join(','))
    if (newFilters.productCodes.length) params.set('product_codes', newFilters.productCodes.join(','))
    if (newFilters.eventTypes.length) params.set('event_types', newFilters.eventTypes.join(','))
    if (newFilters.dateFrom) params.set('date_from', newFilters.dateFrom)
    if (newFilters.dateTo) params.set('date_to', newFilters.dateTo)
    if (newFilters.searchText) params.set('search', newFilters.searchText)
    setSearchParams(params)
  }, [setSearchParams])

  const setManufacturers = useCallback((manufacturers: string[]) => {
    const newFilters = { ...filters, manufacturers }
    setFilters(newFilters)
    updateURL(newFilters)
  }, [filters, updateURL])

  const setProductCodes = useCallback((productCodes: string[]) => {
    const newFilters = { ...filters, productCodes }
    setFilters(newFilters)
    updateURL(newFilters)
  }, [filters, updateURL])

  const setEventTypes = useCallback((eventTypes: string[]) => {
    const newFilters = { ...filters, eventTypes }
    setFilters(newFilters)
    updateURL(newFilters)
  }, [filters, updateURL])

  const setDateRange = useCallback((dateFrom: string, dateTo: string) => {
    const newFilters = { ...filters, dateFrom, dateTo }
    setFilters(newFilters)
    updateURL(newFilters)
  }, [filters, updateURL])

  const setSearchText = useCallback((searchText: string) => {
    const newFilters = { ...filters, searchText }
    setFilters(newFilters)
    updateURL(newFilters)
  }, [filters, updateURL])

  const clearFilters = useCallback(() => {
    setFilters(defaultFilters)
    setSearchParams(new URLSearchParams())
  }, [setSearchParams])

  const hasActiveFilters =
    filters.manufacturers.length > 0 ||
    filters.productCodes.length > 0 ||
    filters.eventTypes.length > 0 ||
    filters.dateFrom !== '' ||
    filters.dateTo !== '' ||
    filters.searchText !== ''

  return (
    <FilterContext.Provider
      value={{
        filters,
        setManufacturers,
        setProductCodes,
        setEventTypes,
        setDateRange,
        setSearchText,
        clearFilters,
        hasActiveFilters,
      }}
    >
      {children}
    </FilterContext.Provider>
  )
}

export function useFilters() {
  const context = useContext(FilterContext)
  if (!context) {
    throw new Error('useFilters must be used within a FilterProvider')
  }
  return context
}
