/**
 * Advanced Filter System Types
 *
 * Comprehensive type definitions for the expanded filtering system including
 * device filters, presets, visibility configuration, and filter history.
 */

// =============================================================================
// CORE FILTER VALUES
// =============================================================================

/**
 * Complete filter values interface including all filter categories
 */
export interface FilterValues {
  // Core event filters (existing)
  manufacturers: string[]
  productCodes: string[]
  eventTypes: string[]
  dateFrom: string
  dateTo: string
  searchText: string

  // Device filters (new)
  brandNames: string[]
  genericNames: string[]
  deviceManufacturers: string[]
  modelNumbers: string[]
  implantFlag: 'Y' | 'N' | '' // Y = Yes, N = No, '' = Any
  deviceProductCodes: string[]
}

/**
 * Default empty filter values
 */
export const defaultFilterValues: FilterValues = {
  manufacturers: [],
  productCodes: [],
  eventTypes: [],
  dateFrom: '',
  dateTo: '',
  searchText: '',
  brandNames: [],
  genericNames: [],
  deviceManufacturers: [],
  modelNumbers: [],
  implantFlag: '',
  deviceProductCodes: [],
}

// =============================================================================
// FILTER FIELD DEFINITIONS
// =============================================================================

/**
 * Types of filter input controls
 */
export type FilterFieldType =
  | 'multi-select'     // Multiple values with autocomplete
  | 'single-select'    // Single value dropdown (Y/N/Any)
  | 'text'             // Free text search
  | 'date-range'       // From/To date pickers

/**
 * Filter category for grouping in UI
 */
export type FilterCategory = 'core' | 'device' | 'event' | 'patient'

/**
 * Definition of a single filter field
 */
export interface FilterFieldDefinition {
  id: keyof FilterValues
  label: string
  category: FilterCategory
  type: FilterFieldType
  dbColumn: string
  dbTable: 'master_events' | 'devices' | 'patients' | 'mdr_text'
  urlParam: string
  placeholder?: string
  coverage?: number // Approximate % of records with this field populated
  autocompleteEndpoint?: string // API endpoint for autocomplete options
  options?: SelectOption[] // Static options for single-select
}

/**
 * Option for single-select dropdowns
 */
export interface SelectOption {
  value: string
  label: string
}

// =============================================================================
// FILTER VISIBILITY & UI CONFIGURATION
// =============================================================================

/**
 * Configuration for which filters are visible and section states
 */
export interface FilterVisibilityConfig {
  // Which individual filters are visible
  visibleFilters: Set<keyof FilterValues>

  // Which sections are collapsed (false = expanded, true = collapsed)
  collapsedSections: {
    core: boolean
    device: boolean
    event: boolean
    patient: boolean
  }

  // Compact mode (fewer visible fields)
  compactMode: boolean
}

/**
 * Default visibility configuration
 */
export const defaultVisibilityConfig: FilterVisibilityConfig = {
  visibleFilters: new Set([
    'manufacturers',
    'productCodes',
    'eventTypes',
    'dateFrom',
    'dateTo',
    'searchText',
    'brandNames',
    'implantFlag',
  ] as (keyof FilterValues)[]),
  collapsedSections: {
    core: false,
    device: true,
    event: true,
    patient: true,
  },
  compactMode: false,
}

// =============================================================================
// FILTER PRESETS
// =============================================================================

/**
 * A saved filter preset
 */
export interface FilterPreset {
  id: string
  name: string
  description?: string
  filters: Partial<FilterValues>
  isBuiltIn: boolean // Built-in presets cannot be deleted
  createdAt: string // ISO date string
  updatedAt: string // ISO date string
}

/**
 * Built-in preset definitions
 */
export const builtInPresets: FilterPreset[] = [
  {
    id: 'recent-deaths',
    name: 'Recent Deaths',
    description: 'Death events from the last 30 days',
    filters: {
      eventTypes: ['D'],
      // dateFrom will be set dynamically
    },
    isBuiltIn: true,
    createdAt: '2024-01-01T00:00:00Z',
    updatedAt: '2024-01-01T00:00:00Z',
  },
  {
    id: 'implant-devices',
    name: 'Implant Devices',
    description: 'Events involving implanted medical devices',
    filters: {
      implantFlag: 'Y',
    },
    isBuiltIn: true,
    createdAt: '2024-01-01T00:00:00Z',
    updatedAt: '2024-01-01T00:00:00Z',
  },
  {
    id: 'high-severity',
    name: 'High Severity Events',
    description: 'Deaths and injuries only',
    filters: {
      eventTypes: ['D', 'I'],
    },
    isBuiltIn: true,
    createdAt: '2024-01-01T00:00:00Z',
    updatedAt: '2024-01-01T00:00:00Z',
  },
]

// =============================================================================
// FILTER HISTORY
// =============================================================================

/**
 * A historical filter state for undo/redo and recent filters
 */
export interface FilterHistoryEntry {
  id: string
  filters: FilterValues
  timestamp: string // ISO date string
  label?: string // Optional user-provided label
}

/**
 * Maximum number of history entries to keep
 */
export const MAX_HISTORY_ENTRIES = 10

// =============================================================================
// API TYPES
// =============================================================================

/**
 * Parameters for filter autocomplete API calls
 */
export interface AutocompleteParams {
  search?: string
  limit?: number
  filterContext?: Partial<FilterValues> // Current filters for context-aware suggestions
}

/**
 * Response item from autocomplete endpoints
 */
export interface AutocompleteItem {
  value: string
  label: string
  count?: number
}

/**
 * Preset API request/response types
 */
export interface CreatePresetRequest {
  name: string
  description?: string
  filters: Partial<FilterValues>
}

export interface UpdatePresetRequest {
  name?: string
  description?: string
  filters?: Partial<FilterValues>
}

export interface PresetResponse extends FilterPreset {
  userId?: string // For user-specific presets
}

// =============================================================================
// URL ENCODING HELPERS
// =============================================================================

/**
 * URL parameter keys for each filter field
 */
export const URL_PARAM_MAP: Record<keyof FilterValues, string> = {
  manufacturers: 'manufacturers',
  productCodes: 'product_codes',
  eventTypes: 'event_types',
  dateFrom: 'date_from',
  dateTo: 'date_to',
  searchText: 'search',
  brandNames: 'brand_names',
  genericNames: 'generic_names',
  deviceManufacturers: 'device_manufacturers',
  modelNumbers: 'model_numbers',
  implantFlag: 'implant_flag',
  deviceProductCodes: 'device_product_codes',
}

/**
 * Reverse map from URL param to filter key
 */
export const PARAM_TO_FILTER_MAP: Record<string, keyof FilterValues> =
  Object.entries(URL_PARAM_MAP).reduce((acc, [key, value]) => {
    acc[value] = key as keyof FilterValues
    return acc
  }, {} as Record<string, keyof FilterValues>)

// =============================================================================
// STORAGE KEYS
// =============================================================================

/**
 * localStorage keys for persistent data
 */
export const STORAGE_KEYS = {
  VISIBILITY_CONFIG: 'maude_filter_visibility',
  USER_PRESETS: 'maude_filter_presets',
  FILTER_HISTORY: 'maude_filter_history',
  LAST_FILTERS: 'maude_last_filters',
} as const

// =============================================================================
// CONTEXT TYPES
// =============================================================================

/**
 * Advanced filter context value
 */
export interface AdvancedFilterContextValue {
  // Current filter values
  filters: FilterValues

  // Filter setters
  setFilter: <K extends keyof FilterValues>(key: K, value: FilterValues[K]) => void
  setFilters: (updates: Partial<FilterValues>) => void
  clearFilters: () => void

  // Computed properties
  hasActiveFilters: boolean
  activeFilterCount: number

  // Visibility config
  visibilityConfig: FilterVisibilityConfig
  setVisibilityConfig: (config: FilterVisibilityConfig) => void
  toggleFilterVisibility: (filterId: keyof FilterValues) => void
  toggleSection: (section: FilterCategory) => void

  // Presets
  presets: FilterPreset[]
  savePreset: (name: string, description?: string) => Promise<FilterPreset>
  loadPreset: (presetId: string) => void
  deletePreset: (presetId: string) => Promise<void>

  // History
  history: FilterHistoryEntry[]
  undo: () => void
  redo: () => void
  canUndo: boolean
  canRedo: boolean
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Count number of active filters
 */
export function countActiveFilters(filters: FilterValues): number {
  let count = 0

  if (filters.manufacturers.length > 0) count++
  if (filters.productCodes.length > 0) count++
  if (filters.eventTypes.length > 0) count++
  if (filters.dateFrom) count++
  if (filters.dateTo) count++
  if (filters.searchText) count++
  if (filters.brandNames.length > 0) count++
  if (filters.genericNames.length > 0) count++
  if (filters.deviceManufacturers.length > 0) count++
  if (filters.modelNumbers.length > 0) count++
  if (filters.implantFlag) count++
  if (filters.deviceProductCodes.length > 0) count++

  return count
}

/**
 * Check if any filters are active
 */
export function hasActiveFilters(filters: FilterValues): boolean {
  return countActiveFilters(filters) > 0
}

/**
 * Serialize filters to URL search params
 */
export function filtersToURLParams(filters: FilterValues): URLSearchParams {
  const params = new URLSearchParams()

  // Array fields - join with comma
  if (filters.manufacturers.length) params.set(URL_PARAM_MAP.manufacturers, filters.manufacturers.join(','))
  if (filters.productCodes.length) params.set(URL_PARAM_MAP.productCodes, filters.productCodes.join(','))
  if (filters.eventTypes.length) params.set(URL_PARAM_MAP.eventTypes, filters.eventTypes.join(','))
  if (filters.brandNames.length) params.set(URL_PARAM_MAP.brandNames, filters.brandNames.join(','))
  if (filters.genericNames.length) params.set(URL_PARAM_MAP.genericNames, filters.genericNames.join(','))
  if (filters.deviceManufacturers.length) params.set(URL_PARAM_MAP.deviceManufacturers, filters.deviceManufacturers.join(','))
  if (filters.modelNumbers.length) params.set(URL_PARAM_MAP.modelNumbers, filters.modelNumbers.join(','))
  if (filters.deviceProductCodes.length) params.set(URL_PARAM_MAP.deviceProductCodes, filters.deviceProductCodes.join(','))

  // String fields
  if (filters.dateFrom) params.set(URL_PARAM_MAP.dateFrom, filters.dateFrom)
  if (filters.dateTo) params.set(URL_PARAM_MAP.dateTo, filters.dateTo)
  if (filters.searchText) params.set(URL_PARAM_MAP.searchText, filters.searchText)
  if (filters.implantFlag) params.set(URL_PARAM_MAP.implantFlag, filters.implantFlag)

  return params
}

/**
 * Parse URL search params to filter values
 */
export function urlParamsToFilters(params: URLSearchParams): FilterValues {
  const parseArray = (key: string): string[] => {
    const value = params.get(key)
    return value ? value.split(',').filter(Boolean) : []
  }

  return {
    manufacturers: parseArray(URL_PARAM_MAP.manufacturers),
    productCodes: parseArray(URL_PARAM_MAP.productCodes),
    eventTypes: parseArray(URL_PARAM_MAP.eventTypes),
    dateFrom: params.get(URL_PARAM_MAP.dateFrom) || '',
    dateTo: params.get(URL_PARAM_MAP.dateTo) || '',
    searchText: params.get(URL_PARAM_MAP.searchText) || '',
    brandNames: parseArray(URL_PARAM_MAP.brandNames),
    genericNames: parseArray(URL_PARAM_MAP.genericNames),
    deviceManufacturers: parseArray(URL_PARAM_MAP.deviceManufacturers),
    modelNumbers: parseArray(URL_PARAM_MAP.modelNumbers),
    implantFlag: (params.get(URL_PARAM_MAP.implantFlag) as 'Y' | 'N' | '') || '',
    deviceProductCodes: parseArray(URL_PARAM_MAP.deviceProductCodes),
  }
}
