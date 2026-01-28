/**
 * Schema Constants for MAUDE Analyzer Frontend
 *
 * This module centralizes all schema-related constants including event types,
 * outcome codes, and column configurations. Import from here instead of
 * hardcoding values in components.
 */

// =============================================================================
// EVENT TYPE DEFINITIONS
// =============================================================================

export interface EventTypeInfo {
  code: string
  dbCode: string // Code used in database (e.g., 'IN' for injury)
  filterCode: string // Code used in filters/URL (e.g., 'I' for injury)
  name: string
  description: string
  severity: number
  color: string
  bgClass: string
  textClass: string
}

/**
 * Event types with full metadata.
 * Note: Database uses 'IN' for injury, but filter/URL uses 'I' for brevity.
 */
export const EVENT_TYPES: Record<string, EventTypeInfo> = {
  D: {
    code: 'D',
    dbCode: 'D',
    filterCode: 'D',
    name: 'Death',
    description: 'Patient death associated with device',
    severity: 1,
    color: '#dc2626',
    bgClass: 'bg-red-100',
    textClass: 'text-red-800',
  },
  IN: {
    code: 'IN',
    dbCode: 'IN',
    filterCode: 'I',
    name: 'Injury',
    description: 'Patient injury associated with device',
    severity: 2,
    color: '#ea580c',
    bgClass: 'bg-orange-100',
    textClass: 'text-orange-800',
  },
  M: {
    code: 'M',
    dbCode: 'M',
    filterCode: 'M',
    name: 'Malfunction',
    description: 'Device malfunction',
    severity: 3,
    color: '#ca8a04',
    bgClass: 'bg-yellow-100',
    textClass: 'text-yellow-800',
  },
  O: {
    code: 'O',
    dbCode: 'O',
    filterCode: 'O',
    name: 'Other',
    description: 'Other event type',
    severity: 4,
    color: '#6b7280',
    bgClass: 'bg-gray-100',
    textClass: 'text-gray-800',
  },
}

/**
 * Event type options for filter dropdowns/buttons.
 * Uses filter codes (I instead of IN) for URL compatibility.
 */
export const EVENT_TYPE_OPTIONS = [
  { value: 'D', label: 'Death' },
  { value: 'I', label: 'Injury' },
  { value: 'M', label: 'Malfunction' },
  { value: 'O', label: 'Other' },
] as const

/**
 * Maps filter codes to database codes.
 * Filter uses 'I' for injury, database uses 'IN'.
 */
export const FILTER_TO_DB_CODE: Record<string, string> = {
  D: 'D',
  I: 'IN',
  M: 'M',
  O: 'O',
}

/**
 * Maps database codes to filter codes.
 */
export const DB_TO_FILTER_CODE: Record<string, string> = {
  D: 'D',
  IN: 'I',
  M: 'M',
  O: 'O',
}

// =============================================================================
// EVENT TYPE DISPLAY CONFIGURATION
// =============================================================================

export interface EventTypeDisplay {
  label: string
  color: string
}

/**
 * Display configuration for event type badges/labels.
 * Keyed by database code (D, IN, M, O).
 */
export const EVENT_TYPE_LABELS: Record<string, EventTypeDisplay> = {
  D: { label: 'Death', color: 'bg-red-100 text-red-800' },
  IN: { label: 'Injury', color: 'bg-orange-100 text-orange-800' },
  M: { label: 'Malfunction', color: 'bg-yellow-100 text-yellow-800' },
  O: { label: 'Other', color: 'bg-gray-100 text-gray-800' },
}

// =============================================================================
// PATIENT OUTCOME DEFINITIONS
// =============================================================================

export interface OutcomeInfo {
  code: string
  name: string
  field: string
  severity: number
  colorClass: string
}

export const OUTCOME_CODES: Record<string, OutcomeInfo> = {
  D: {
    code: 'D',
    name: 'Death',
    field: 'outcome_death',
    severity: 1,
    colorClass: 'bg-red-100 text-red-800',
  },
  L: {
    code: 'L',
    name: 'Life Threatening',
    field: 'outcome_life_threatening',
    severity: 2,
    colorClass: 'bg-yellow-100 text-yellow-800',
  },
  H: {
    code: 'H',
    name: 'Hospitalization',
    field: 'outcome_hospitalization',
    severity: 3,
    colorClass: 'bg-orange-100 text-orange-800',
  },
  DS: {
    code: 'DS',
    name: 'Disability',
    field: 'outcome_disability',
    severity: 4,
    colorClass: 'bg-purple-100 text-purple-800',
  },
  CA: {
    code: 'CA',
    name: 'Congenital Anomaly',
    field: 'outcome_congenital_anomaly',
    severity: 5,
    colorClass: 'bg-pink-100 text-pink-800',
  },
  RI: {
    code: 'RI',
    name: 'Required Intervention',
    field: 'outcome_required_intervention',
    severity: 6,
    colorClass: 'bg-blue-100 text-blue-800',
  },
  OT: {
    code: 'OT',
    name: 'Other',
    field: 'outcome_other',
    severity: 7,
    colorClass: 'bg-gray-100 text-gray-800',
  },
}

/**
 * Outcome badges for patient detail display.
 */
export const OUTCOME_BADGES = [
  { key: 'death', label: 'Death', colorClass: 'bg-red-100 text-red-800' },
  { key: 'hospitalization', label: 'Hospitalization', colorClass: 'bg-orange-100 text-orange-800' },
  { key: 'life_threatening', label: 'Life Threatening', colorClass: 'bg-yellow-100 text-yellow-800' },
  { key: 'disability', label: 'Disability', colorClass: 'bg-purple-100 text-purple-800' },
] as const

// =============================================================================
// TEXT TYPE DEFINITIONS
// =============================================================================

export interface TextTypeInfo {
  code: string
  name: string
  description: string
  priority: number
}

export const TEXT_TYPE_CODES: Record<string, TextTypeInfo> = {
  D: {
    code: 'D',
    name: 'Event Description',
    description: 'Primary event description narrative',
    priority: 1,
  },
  H: {
    code: 'H',
    name: 'Event History',
    description: 'Historical context of event',
    priority: 2,
  },
  M: {
    code: 'M',
    name: 'Manufacturer Narrative',
    description: "Manufacturer's description",
    priority: 3,
  },
  E: {
    code: 'E',
    name: 'Evaluation Summary',
    description: 'Evaluation/assessment summary',
    priority: 4,
  },
  N: {
    code: 'N',
    name: 'Additional Information',
    description: 'Additional notes and information',
    priority: 5,
  },
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Get event type display info by database code.
 */
export function getEventTypeDisplay(dbCode: string): EventTypeDisplay {
  return (
    EVENT_TYPE_LABELS[dbCode] || {
      label: dbCode || 'Unknown',
      color: 'bg-gray-100 text-gray-800',
    }
  )
}

/**
 * Convert filter code to database code.
 * Handles I -> IN conversion for injury.
 */
export function filterToDbCode(filterCode: string): string {
  return FILTER_TO_DB_CODE[filterCode] || filterCode
}

/**
 * Convert database code to filter code.
 * Handles IN -> I conversion for injury.
 */
export function dbToFilterCode(dbCode: string): string {
  return DB_TO_FILTER_CODE[dbCode] || dbCode
}

/**
 * Get text type display name.
 */
export function getTextTypeName(code: string): string {
  return TEXT_TYPE_CODES[code]?.name || code
}

/**
 * Get outcome display name.
 */
export function getOutcomeName(code: string): string {
  return OUTCOME_CODES[code]?.name || code
}

// =============================================================================
// COLUMN VISIBILITY CONFIGURATION
// =============================================================================

export interface ColumnConfig {
  key: string
  label: string
  visible: boolean
  sortable: boolean
  width?: string
}

/**
 * Default column configuration for events table.
 */
export const DEFAULT_EVENT_COLUMNS: ColumnConfig[] = [
  { key: 'report_number', label: 'Report #', visible: true, sortable: true },
  { key: 'date_received', label: 'Date', visible: true, sortable: true },
  { key: 'event_type', label: 'Type', visible: true, sortable: true },
  { key: 'manufacturer', label: 'Manufacturer', visible: true, sortable: true },
  { key: 'product_code', label: 'Product Code', visible: true, sortable: true },
  { key: 'actions', label: 'Actions', visible: true, sortable: false },
]

// =============================================================================
// API FIELD NAMES
// =============================================================================

/**
 * Field names returned by the API.
 * Use these instead of string literals for type safety.
 */
export const API_FIELDS = {
  // Event fields
  MDR_REPORT_KEY: 'mdr_report_key',
  REPORT_NUMBER: 'report_number',
  DATE_RECEIVED: 'date_received',
  DATE_OF_EVENT: 'date_of_event',
  EVENT_TYPE: 'event_type',
  PRODUCT_CODE: 'product_code',
  MANUFACTURER: 'manufacturer',
  MANUFACTURER_NAME: 'manufacturer_name',

  // Device fields
  BRAND_NAME: 'brand_name',
  GENERIC_NAME: 'generic_name',
  MODEL_NUMBER: 'model_number',

  // Patient fields
  PATIENT_SEX: 'sex',
  PATIENT_AGE: 'age',
  SEQUENCE: 'sequence',

  // Outcomes
  OUTCOMES: 'outcomes',
} as const
