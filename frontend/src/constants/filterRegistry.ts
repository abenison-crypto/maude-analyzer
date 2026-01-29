/**
 * Filter Registry - Central registry of all filterable fields
 *
 * This file defines metadata for all available filter fields including:
 * - Display labels and placeholders
 * - Database column mappings
 * - Field types and categories
 * - Autocomplete endpoints
 * - Coverage statistics
 */

import type {
  FilterFieldDefinition,
  FilterCategory,
  FilterValues,
  SelectOption,
} from '../types/filters'

// =============================================================================
// FILTER FIELD DEFINITIONS
// =============================================================================

/**
 * Complete registry of all filter fields
 */
export const FILTER_REGISTRY: Record<keyof FilterValues, FilterFieldDefinition> = {
  // ---------------------------------------------------------------------------
  // Core Event Filters
  // ---------------------------------------------------------------------------
  manufacturers: {
    id: 'manufacturers',
    label: 'Manufacturer',
    category: 'core',
    type: 'multi-select',
    dbColumn: 'manufacturer_clean',
    dbTable: 'master_events',
    urlParam: 'manufacturers',
    placeholder: 'Search manufacturers...',
    coverage: 95,
    autocompleteEndpoint: '/api/events/manufacturers',
  },

  productCodes: {
    id: 'productCodes',
    label: 'Product Code',
    category: 'core',
    type: 'multi-select',
    dbColumn: 'product_code',
    dbTable: 'master_events',
    urlParam: 'product_codes',
    placeholder: 'Search product codes...',
    coverage: 98,
    autocompleteEndpoint: '/api/events/product-codes',
  },

  eventTypes: {
    id: 'eventTypes',
    label: 'Event Type',
    category: 'core',
    type: 'multi-select',
    dbColumn: 'event_type',
    dbTable: 'master_events',
    urlParam: 'event_types',
    placeholder: 'Select event types...',
    coverage: 100,
    // Static options, no autocomplete needed
  },

  dateFrom: {
    id: 'dateFrom',
    label: 'Date From',
    category: 'core',
    type: 'date-range',
    dbColumn: 'date_received',
    dbTable: 'master_events',
    urlParam: 'date_from',
    placeholder: 'Start date',
    coverage: 100,
  },

  dateTo: {
    id: 'dateTo',
    label: 'Date To',
    category: 'core',
    type: 'date-range',
    dbColumn: 'date_received',
    dbTable: 'master_events',
    urlParam: 'date_to',
    placeholder: 'End date',
    coverage: 100,
  },

  searchText: {
    id: 'searchText',
    label: 'Search Narratives',
    category: 'core',
    type: 'text',
    dbColumn: 'text_content',
    dbTable: 'mdr_text',
    urlParam: 'search',
    placeholder: 'Search in event narratives...',
    coverage: 90,
  },

  // ---------------------------------------------------------------------------
  // Device Filters
  // ---------------------------------------------------------------------------
  brandNames: {
    id: 'brandNames',
    label: 'Brand Name',
    category: 'device',
    type: 'multi-select',
    dbColumn: 'brand_name',
    dbTable: 'devices',
    urlParam: 'brand_names',
    placeholder: 'Search brand names...',
    coverage: 80,
    autocompleteEndpoint: '/api/filters/brand-names',
  },

  genericNames: {
    id: 'genericNames',
    label: 'Generic Name',
    category: 'device',
    type: 'multi-select',
    dbColumn: 'generic_name',
    dbTable: 'devices',
    urlParam: 'generic_names',
    placeholder: 'Search generic names...',
    coverage: 70,
    autocompleteEndpoint: '/api/filters/generic-names',
  },

  deviceManufacturers: {
    id: 'deviceManufacturers',
    label: 'Device Manufacturer',
    category: 'device',
    type: 'multi-select',
    dbColumn: 'manufacturer_d_name',
    dbTable: 'devices',
    urlParam: 'device_manufacturers',
    placeholder: 'Search device manufacturers...',
    coverage: 70,
    autocompleteEndpoint: '/api/filters/device-manufacturers',
  },

  modelNumbers: {
    id: 'modelNumbers',
    label: 'Model Number',
    category: 'device',
    type: 'multi-select',
    dbColumn: 'model_number',
    dbTable: 'devices',
    urlParam: 'model_numbers',
    placeholder: 'Search model numbers...',
    coverage: 60,
    autocompleteEndpoint: '/api/filters/model-numbers',
  },

  implantFlag: {
    id: 'implantFlag',
    label: 'Implant Device',
    category: 'device',
    type: 'single-select',
    dbColumn: 'implant_flag',
    dbTable: 'devices',
    urlParam: 'implant_flag',
    placeholder: 'Any',
    coverage: 70,
    options: [
      { value: '', label: 'Any' },
      { value: 'Y', label: 'Yes - Implant' },
      { value: 'N', label: 'No - Not Implant' },
    ],
  },

  deviceProductCodes: {
    id: 'deviceProductCodes',
    label: 'Device Product Code',
    category: 'device',
    type: 'multi-select',
    dbColumn: 'device_report_product_code',
    dbTable: 'devices',
    urlParam: 'device_product_codes',
    placeholder: 'Search device product codes...',
    coverage: 60,
    autocompleteEndpoint: '/api/filters/device-product-codes',
  },
}

// =============================================================================
// CATEGORY DEFINITIONS
// =============================================================================

/**
 * Filter category metadata
 */
export interface FilterCategoryDefinition {
  id: FilterCategory
  label: string
  description: string
  icon: string // Lucide icon name
  defaultExpanded: boolean
}

/**
 * All filter categories in display order
 */
export const FILTER_CATEGORIES: FilterCategoryDefinition[] = [
  {
    id: 'core',
    label: 'Core Filters',
    description: 'Primary event and manufacturer filters',
    icon: 'Filter',
    defaultExpanded: true,
  },
  {
    id: 'device',
    label: 'Device Filters',
    description: 'Filter by device-specific attributes',
    icon: 'Cpu',
    defaultExpanded: false,
  },
  {
    id: 'event',
    label: 'Event Details',
    description: 'Filter by event characteristics',
    icon: 'AlertCircle',
    defaultExpanded: false,
  },
  {
    id: 'patient',
    label: 'Patient Filters',
    description: 'Filter by patient demographics',
    icon: 'User',
    defaultExpanded: false,
  },
]

/**
 * Get category definition by ID
 */
export function getCategoryDefinition(id: FilterCategory): FilterCategoryDefinition | undefined {
  return FILTER_CATEGORIES.find((cat) => cat.id === id)
}

// =============================================================================
// FILTER FIELD HELPERS
// =============================================================================

/**
 * Get all filter fields for a category
 */
export function getFiltersByCategory(category: FilterCategory): FilterFieldDefinition[] {
  return Object.values(FILTER_REGISTRY).filter((field) => field.category === category)
}

/**
 * Get filter field definition by ID
 */
export function getFilterDefinition(id: keyof FilterValues): FilterFieldDefinition {
  return FILTER_REGISTRY[id]
}

/**
 * Get all filter fields that have autocomplete
 */
export function getAutocompleteFilters(): FilterFieldDefinition[] {
  return Object.values(FILTER_REGISTRY).filter((field) => field.autocompleteEndpoint)
}

/**
 * Get all multi-select filter fields
 */
export function getMultiSelectFilters(): FilterFieldDefinition[] {
  return Object.values(FILTER_REGISTRY).filter((field) => field.type === 'multi-select')
}

/**
 * Check if a field requires EXISTS subquery (device table filters)
 */
export function requiresDeviceJoin(field: FilterFieldDefinition): boolean {
  return field.dbTable === 'devices'
}

// =============================================================================
// EVENT TYPE OPTIONS
// =============================================================================

/**
 * Static options for event type filter
 */
export const EVENT_TYPE_FILTER_OPTIONS: SelectOption[] = [
  { value: 'D', label: 'Death' },
  { value: 'I', label: 'Injury' },
  { value: 'M', label: 'Malfunction' },
  { value: 'O', label: 'Other' },
]

/**
 * Event type colors for UI
 */
export const EVENT_TYPE_COLORS: Record<string, { bg: string; text: string; border: string }> = {
  D: { bg: 'bg-red-100', text: 'text-red-800', border: 'border-red-300' },
  I: { bg: 'bg-orange-100', text: 'text-orange-800', border: 'border-orange-300' },
  M: { bg: 'bg-yellow-100', text: 'text-yellow-800', border: 'border-yellow-300' },
  O: { bg: 'bg-gray-100', text: 'text-gray-800', border: 'border-gray-300' },
}

// =============================================================================
// FIELD ORDERING
// =============================================================================

/**
 * Default order of filter fields within each category
 */
export const FILTER_ORDER: Record<FilterCategory, (keyof FilterValues)[]> = {
  core: ['searchText', 'manufacturers', 'productCodes', 'eventTypes', 'dateFrom', 'dateTo'],
  device: ['brandNames', 'genericNames', 'deviceManufacturers', 'modelNumbers', 'implantFlag', 'deviceProductCodes'],
  event: [], // Reserved for future event detail filters
  patient: [], // Reserved for future patient filters
}

/**
 * Get ordered filter fields for a category
 */
export function getOrderedFilters(category: FilterCategory): FilterFieldDefinition[] {
  const order = FILTER_ORDER[category]
  return order.map((id) => FILTER_REGISTRY[id]).filter(Boolean)
}

// =============================================================================
// VALIDATION
// =============================================================================

/**
 * Validate a filter value against its field definition
 */
export function validateFilterValue(
  field: FilterFieldDefinition,
  value: unknown
): { valid: boolean; error?: string } {
  switch (field.type) {
    case 'multi-select':
      if (!Array.isArray(value)) {
        return { valid: false, error: 'Value must be an array' }
      }
      return { valid: true }

    case 'single-select':
      if (field.options) {
        const validValues = field.options.map((o) => o.value)
        if (value !== '' && !validValues.includes(value as string)) {
          return { valid: false, error: `Value must be one of: ${validValues.join(', ')}` }
        }
      }
      return { valid: true }

    case 'text':
      if (typeof value !== 'string') {
        return { valid: false, error: 'Value must be a string' }
      }
      return { valid: true }

    case 'date-range':
      if (value !== '' && typeof value === 'string') {
        const dateRegex = /^\d{4}-\d{2}-\d{2}$/
        if (!dateRegex.test(value)) {
          return { valid: false, error: 'Date must be in YYYY-MM-DD format' }
        }
      }
      return { valid: true }

    default:
      return { valid: true }
  }
}

// =============================================================================
// DISPLAY HELPERS
// =============================================================================

/**
 * Get display label for a filter value
 */
export function getFilterValueLabel(fieldId: keyof FilterValues, value: string): string {
  const field = FILTER_REGISTRY[fieldId]

  // For single-select with options, find the matching label
  if (field.type === 'single-select' && field.options) {
    const option = field.options.find((o) => o.value === value)
    return option?.label || value
  }

  // For event types, use display names
  if (fieldId === 'eventTypes') {
    const eventOption = EVENT_TYPE_FILTER_OPTIONS.find((o) => o.value === value)
    return eventOption?.label || value
  }

  return value
}

/**
 * Get a human-readable summary of active filters
 */
export function getFilterSummary(filters: FilterValues): string {
  const parts: string[] = []

  if (filters.manufacturers.length) {
    parts.push(`${filters.manufacturers.length} manufacturer${filters.manufacturers.length > 1 ? 's' : ''}`)
  }
  if (filters.eventTypes.length) {
    parts.push(filters.eventTypes.map((t) => getFilterValueLabel('eventTypes', t)).join(', '))
  }
  if (filters.dateFrom || filters.dateTo) {
    if (filters.dateFrom && filters.dateTo) {
      parts.push(`${filters.dateFrom} to ${filters.dateTo}`)
    } else if (filters.dateFrom) {
      parts.push(`from ${filters.dateFrom}`)
    } else {
      parts.push(`until ${filters.dateTo}`)
    }
  }
  if (filters.searchText) {
    parts.push(`"${filters.searchText}"`)
  }
  if (filters.brandNames.length) {
    parts.push(`${filters.brandNames.length} brand${filters.brandNames.length > 1 ? 's' : ''}`)
  }
  if (filters.implantFlag) {
    parts.push(filters.implantFlag === 'Y' ? 'implants only' : 'non-implants')
  }

  return parts.length > 0 ? parts.join(' | ') : 'No filters applied'
}
