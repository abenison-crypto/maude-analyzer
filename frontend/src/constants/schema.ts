/**
 * Schema Constants for MAUDE Analyzer Frontend
 *
 * This module provides backward-compatible exports while sourcing data from
 * the auto-generated schema types.
 *
 * IMPORTANT: The actual type definitions are now in types/generated/schema.ts.
 * This file re-exports them for backward compatibility.
 *
 * For new code, prefer importing directly from:
 *   import { EVENT_TYPES, FIELDS, ... } from '../types/generated/schema'
 */

// Re-export everything from generated schema
export {
  SCHEMA_VERSION,
  // Table interfaces
  type MasterEvents,
  type Devices,
  type Patients,
  type MdrText,
  type DeviceProblems,
  type PatientProblems,
  // Field constants
  FIELDS,
  type FieldName,
  // Event types
  type EventTypeInfo,
  EVENT_TYPES,
  FILTER_TO_DB_CODE,
  DB_TO_FILTER_CODE,
  EVENT_TYPE_OPTIONS,
  type EventTypeDbCode,
  type EventTypeFilterCode,
  // Outcomes
  type OutcomeInfo,
  OUTCOME_CODES,
  type OutcomeCode,
  // Text types
  type TextTypeInfo,
  TEXT_TYPE_CODES,
  type TextTypeCode,
  // Helper functions
  filterToDbCode,
  dbToFilterCode,
  getEventTypeDisplay,
  getTextTypeName,
  getOutcomeName,
  convertFilterEventTypes,
} from '../types/generated/schema'

// =============================================================================
// BACKWARD COMPATIBILITY EXPORTS
// =============================================================================

// These are kept for backward compatibility with existing code that uses
// the old interface style. New code should use the generated types.

// Note: EVENT_TYPES, EVENT_TYPE_OPTIONS, FILTER_TO_DB_CODE, and DB_TO_FILTER_CODE
// are now exported from types/generated/schema.ts above.

// =============================================================================
// EVENT TYPE DISPLAY CONFIGURATION (Backward Compatibility)
// =============================================================================

import { EVENT_TYPES as _ET, getEventTypeDisplay as _getDisplay } from '../types/generated/schema'

export interface EventTypeDisplay {
  label: string
  color: string
}

/**
 * Display configuration for event type badges/labels.
 * Keyed by database code (D, IN, M, O).
 * @deprecated Use getEventTypeDisplay() from generated schema instead
 */
export const EVENT_TYPE_LABELS: Record<string, EventTypeDisplay> = {
  D: { label: _ET.D.name, color: `${_ET.D.bgClass} ${_ET.D.textClass}` },
  IN: { label: _ET.IN.name, color: `${_ET.IN.bgClass} ${_ET.IN.textClass}` },
  M: { label: _ET.M.name, color: `${_ET.M.bgClass} ${_ET.M.textClass}` },
  O: { label: _ET.O.name, color: `${_ET.O.bgClass} ${_ET.O.textClass}` },
}

// =============================================================================
// PATIENT OUTCOME DEFINITIONS (Now from Generated Schema)
// =============================================================================

// OUTCOME_CODES is now exported from types/generated/schema.ts above.

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
// TEXT TYPE DEFINITIONS (Now from Generated Schema)
// =============================================================================

// TEXT_TYPE_CODES is now exported from types/generated/schema.ts above.

// =============================================================================
// HELPER FUNCTIONS (Now from Generated Schema)
// =============================================================================

// All helper functions are now exported from types/generated/schema.ts above:
// - getEventTypeDisplay
// - filterToDbCode
// - dbToFilterCode
// - getTextTypeName
// - getOutcomeName
// - convertFilterEventTypes

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
// API FIELD NAMES (Backward Compatibility)
// =============================================================================

/**
 * Field names returned by the API.
 * @deprecated Use FIELDS from types/generated/schema.ts instead
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
