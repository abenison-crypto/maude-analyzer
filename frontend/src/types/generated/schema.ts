/**
 * AUTO-GENERATED FILE - DO NOT EDIT DIRECTLY
 *
 * Generated from UnifiedSchemaRegistry by generate_frontend_types.py
 * Generated at: 2026-02-01T01:15:09.813610
 * Schema Version: 2.1
 *
 * This file provides type-safe access to schema definitions that match
 * the backend database schema exactly.
 */

// =============================================================================
// SCHEMA VERSION
// =============================================================================

export const SCHEMA_VERSION = '2.1' as const

// =============================================================================
// TABLE INTERFACES
// =============================================================================

/**
 * Master Events
 * Primary MDR event records
 */
export interface MasterEvents {
  /** Primary key */
  mdr_report_key: number
  event_key: number | null
  report_number: string | null
  report_source_code: string | null
  number_devices_in_event: number | null
  number_patients_in_event: number | null
  date_received: string | null
  date_report: string | null
  date_of_event: string | null
  adverse_event_flag: string | null
  product_problem_flag: string | null
  reprocessed_and_reused_flag: string | null
  /** D=Death, IN=Injury, M=Malfunction, O=Other */
  event_type: string | null
  event_location: string | null
  reporter_occupation_code: string | null
  health_professional: string | null
  initial_report_to_fda: string | null
  /** Added in 86-column format */
  reporter_state_code?: string | null
  reporter_country_code: string | null
  manufacturer_name: string | null
  manufacturer_city: string | null
  manufacturer_state: string | null
  manufacturer_country: string | null
  manufacturer_postal: string | null
  /** Normalized manufacturer name for grouping */
  manufacturer_clean: string | null
  product_code: string | null
  pma_pmn_number: string | null
  exemption_number: string | null
  type_of_report: string | null
  source_type: string | null
  /** Added in 86-column format */
  mfr_report_type?: string | null
  date_added: string | null
  date_changed: string | null
  /** Year extracted from date_received */
  received_year: number | null
  /** Month extracted from date_received */
  received_month: number | null
}

/**
 * Devices
 * Device information linked to events
 */
export interface Devices {
  mdr_report_key: number
  device_event_key: number | null
  device_sequence_number: number | null
  brand_name: string | null
  generic_name: string | null
  model_number: string | null
  catalog_number: string | null
  lot_number: string | null
  other_id_number: string | null
  manufacturer_d_name: string | null
  manufacturer_d_city: string | null
  manufacturer_d_state: string | null
  manufacturer_d_country: string | null
  /** Normalized device manufacturer name */
  manufacturer_d_clean: string | null
  device_report_product_code: string | null
  implant_flag: string | null
  date_removed_flag: string | null
  device_availability: string | null
  device_operator: string | null
  date_received: string | null
  expiration_date_of_device: string | null
  date_returned_to_manufacturer: string | null
  /** Added in 2020+ format */
  implant_date_year?: number | null
  /** Added in 2020+ format */
  date_removed_year?: number | null
  /** Added in 2020+ format */
  serviced_by_3rd_party_flag?: string | null
  /** Added in 2020+ format */
  combination_product_flag?: string | null
  /** Unique Device Identifier - Device ID (2020+) */
  udi_di?: string | null
  /** Unique Device Identifier - Public (2020+) */
  udi_public?: string | null
  device_age_text: string | null
  device_evaluated_by_manufacturer: string | null
}

/**
 * Patients
 * Patient information and outcomes
 */
export interface Patients {
  mdr_report_key: number
  patient_sequence_number: number | null
  date_received: string | null
  patient_age: string | null
  patient_sex: string | null
  patient_weight: string | null
  patient_ethnicity: string | null
  patient_race: string | null
  /** Numeric age extracted from patient_age */
  patient_age_numeric: number | null
  outcome_death: boolean | null
  outcome_life_threatening: boolean | null
  outcome_hospitalization: boolean | null
  outcome_disability: boolean | null
  outcome_congenital_anomaly: boolean | null
  outcome_required_intervention: boolean | null
  outcome_other: boolean | null
}

/**
 * MDR Text
 * Narrative text records
 */
export interface MdrText {
  mdr_report_key: number
  mdr_text_key: number | null
  text_type_code: string | null
  patient_sequence_number: number | null
  date_report: string | null
  text_content: string | null
}

/**
 * Device Problems
 * Device problem codes linked to events
 */
export interface DeviceProblems {
  mdr_report_key: number
  device_problem_code: string | null
}

/**
 * Patient Problems
 * Patient problem codes linked to events
 */
export interface PatientProblems {
  mdr_report_key: number
  patient_sequence_number: number | null
  patient_problem_code: string | null
  date_added: string | null
  date_changed: string | null
}

// =============================================================================
// FIELD NAME CONSTANTS
// =============================================================================

/**
 * Field names as used in API responses, organized by table.
 * Use these instead of string literals for type safety.
 */
export const FIELDS = {
  masterEvents: {
    MDR_REPORT_KEY: 'mdr_report_key',
    EVENT_KEY: 'event_key',
    REPORT_NUMBER: 'report_number',
    REPORT_SOURCE_CODE: 'report_source_code',
    NUMBER_DEVICES_IN_EVENT: 'number_devices_in_event',
    NUMBER_PATIENTS_IN_EVENT: 'number_patients_in_event',
    DATE_RECEIVED: 'date_received',
    DATE_REPORT: 'date_report',
    DATE_OF_EVENT: 'date_of_event',
    ADVERSE_EVENT_FLAG: 'adverse_event_flag',
    PRODUCT_PROBLEM_FLAG: 'product_problem_flag',
    REPROCESSED_AND_REUSED_FLAG: 'reprocessed_and_reused_flag',
    EVENT_TYPE: 'event_type',
    EVENT_LOCATION: 'event_location',
    REPORTER_OCCUPATION_CODE: 'reporter_occupation_code',
    HEALTH_PROFESSIONAL: 'health_professional',
    INITIAL_REPORT_TO_FDA: 'initial_report_to_fda',
    REPORTER_STATE_CODE: 'reporter_state_code',
    REPORTER_COUNTRY_CODE: 'reporter_country_code',
    MANUFACTURER_NAME: 'manufacturer_name',
    MANUFACTURER_CITY: 'manufacturer_city',
    MANUFACTURER_STATE: 'manufacturer_state',
    MANUFACTURER_COUNTRY: 'manufacturer_country',
    MANUFACTURER_POSTAL: 'manufacturer_postal',
    MANUFACTURER_CLEAN: 'manufacturer_clean',
    PRODUCT_CODE: 'product_code',
    PMA_PMN_NUMBER: 'pma_pmn_number',
    EXEMPTION_NUMBER: 'exemption_number',
    TYPE_OF_REPORT: 'type_of_report',
    SOURCE_TYPE: 'source_type',
    MFR_REPORT_TYPE: 'mfr_report_type',
    DATE_ADDED: 'date_added',
    DATE_CHANGED: 'date_changed',
    RECEIVED_YEAR: 'received_year',
    RECEIVED_MONTH: 'received_month',
  },
  devices: {
    MDR_REPORT_KEY: 'mdr_report_key',
    DEVICE_EVENT_KEY: 'device_event_key',
    DEVICE_SEQUENCE_NUMBER: 'device_sequence_number',
    BRAND_NAME: 'brand_name',
    GENERIC_NAME: 'generic_name',
    MODEL_NUMBER: 'model_number',
    CATALOG_NUMBER: 'catalog_number',
    LOT_NUMBER: 'lot_number',
    OTHER_ID_NUMBER: 'other_id_number',
    MANUFACTURER_D_NAME: 'manufacturer_d_name',
    MANUFACTURER_D_CITY: 'manufacturer_d_city',
    MANUFACTURER_D_STATE: 'manufacturer_d_state',
    MANUFACTURER_D_COUNTRY: 'manufacturer_d_country',
    MANUFACTURER_D_CLEAN: 'manufacturer_d_clean',
    DEVICE_REPORT_PRODUCT_CODE: 'device_report_product_code',
    IMPLANT_FLAG: 'implant_flag',
    DATE_REMOVED_FLAG: 'date_removed_flag',
    DEVICE_AVAILABILITY: 'device_availability',
    DEVICE_OPERATOR: 'device_operator',
    DATE_RECEIVED: 'date_received',
    EXPIRATION_DATE_OF_DEVICE: 'expiration_date_of_device',
    DATE_RETURNED_TO_MANUFACTURER: 'date_returned_to_manufacturer',
    IMPLANT_DATE_YEAR: 'implant_date_year',
    DATE_REMOVED_YEAR: 'date_removed_year',
    SERVICED_BY_3RD_PARTY_FLAG: 'serviced_by_3rd_party_flag',
    COMBINATION_PRODUCT_FLAG: 'combination_product_flag',
    UDI_DI: 'udi_di',
    UDI_PUBLIC: 'udi_public',
    DEVICE_AGE_TEXT: 'device_age_text',
    DEVICE_EVALUATED_BY_MANUFACTURER: 'device_evaluated_by_manufacturer',
  },
  patients: {
    MDR_REPORT_KEY: 'mdr_report_key',
    PATIENT_SEQUENCE_NUMBER: 'patient_sequence_number',
    DATE_RECEIVED: 'date_received',
    PATIENT_AGE: 'patient_age',
    PATIENT_SEX: 'patient_sex',
    PATIENT_WEIGHT: 'patient_weight',
    PATIENT_ETHNICITY: 'patient_ethnicity',
    PATIENT_RACE: 'patient_race',
    PATIENT_AGE_NUMERIC: 'patient_age_numeric',
    OUTCOME_DEATH: 'outcome_death',
    OUTCOME_LIFE_THREATENING: 'outcome_life_threatening',
    OUTCOME_HOSPITALIZATION: 'outcome_hospitalization',
    OUTCOME_DISABILITY: 'outcome_disability',
    OUTCOME_CONGENITAL_ANOMALY: 'outcome_congenital_anomaly',
    OUTCOME_REQUIRED_INTERVENTION: 'outcome_required_intervention',
    OUTCOME_OTHER: 'outcome_other',
  },
  mdrText: {
    MDR_REPORT_KEY: 'mdr_report_key',
    MDR_TEXT_KEY: 'mdr_text_key',
    TEXT_TYPE_CODE: 'text_type_code',
    PATIENT_SEQUENCE_NUMBER: 'patient_sequence_number',
    DATE_REPORT: 'date_report',
    TEXT_CONTENT: 'text_content',
  },
  deviceProblems: {
    MDR_REPORT_KEY: 'mdr_report_key',
    DEVICE_PROBLEM_CODE: 'device_problem_code',
  },
  patientProblems: {
    MDR_REPORT_KEY: 'mdr_report_key',
    PATIENT_SEQUENCE_NUMBER: 'patient_sequence_number',
    PATIENT_PROBLEM_CODE: 'patient_problem_code',
    DATE_ADDED: 'date_added',
    DATE_CHANGED: 'date_changed',
  },
} as const

/**
 * Common field names (deduplicated across tables).
 */
export const COMMON_FIELDS = {
  MDR_REPORT_KEY: 'mdr_report_key',
  EVENT_KEY: 'event_key',
  REPORT_NUMBER: 'report_number',
  REPORT_SOURCE_CODE: 'report_source_code',
  NUMBER_DEVICES_IN_EVENT: 'number_devices_in_event',
  NUMBER_PATIENTS_IN_EVENT: 'number_patients_in_event',
  DATE_RECEIVED: 'date_received',
  DATE_REPORT: 'date_report',
  DATE_OF_EVENT: 'date_of_event',
  ADVERSE_EVENT_FLAG: 'adverse_event_flag',
  PRODUCT_PROBLEM_FLAG: 'product_problem_flag',
  REPROCESSED_AND_REUSED_FLAG: 'reprocessed_and_reused_flag',
  EVENT_TYPE: 'event_type',
  EVENT_LOCATION: 'event_location',
  REPORTER_OCCUPATION_CODE: 'reporter_occupation_code',
  HEALTH_PROFESSIONAL: 'health_professional',
  INITIAL_REPORT_TO_FDA: 'initial_report_to_fda',
  REPORTER_STATE_CODE: 'reporter_state_code',
  REPORTER_COUNTRY_CODE: 'reporter_country_code',
  MANUFACTURER_NAME: 'manufacturer_name',
  MANUFACTURER_CITY: 'manufacturer_city',
  MANUFACTURER_STATE: 'manufacturer_state',
  MANUFACTURER_COUNTRY: 'manufacturer_country',
  MANUFACTURER_POSTAL: 'manufacturer_postal',
  MANUFACTURER_CLEAN: 'manufacturer_clean',
  PRODUCT_CODE: 'product_code',
  PMA_PMN_NUMBER: 'pma_pmn_number',
  EXEMPTION_NUMBER: 'exemption_number',
  TYPE_OF_REPORT: 'type_of_report',
  SOURCE_TYPE: 'source_type',
  MFR_REPORT_TYPE: 'mfr_report_type',
  DATE_ADDED: 'date_added',
  DATE_CHANGED: 'date_changed',
  RECEIVED_YEAR: 'received_year',
  RECEIVED_MONTH: 'received_month',
  DEVICE_EVENT_KEY: 'device_event_key',
  DEVICE_SEQUENCE_NUMBER: 'device_sequence_number',
  BRAND_NAME: 'brand_name',
  GENERIC_NAME: 'generic_name',
  MODEL_NUMBER: 'model_number',
  CATALOG_NUMBER: 'catalog_number',
  LOT_NUMBER: 'lot_number',
  OTHER_ID_NUMBER: 'other_id_number',
  MANUFACTURER_D_NAME: 'manufacturer_d_name',
  MANUFACTURER_D_CITY: 'manufacturer_d_city',
  MANUFACTURER_D_STATE: 'manufacturer_d_state',
  MANUFACTURER_D_COUNTRY: 'manufacturer_d_country',
  MANUFACTURER_D_CLEAN: 'manufacturer_d_clean',
  DEVICE_REPORT_PRODUCT_CODE: 'device_report_product_code',
  IMPLANT_FLAG: 'implant_flag',
  DATE_REMOVED_FLAG: 'date_removed_flag',
  DEVICE_AVAILABILITY: 'device_availability',
  DEVICE_OPERATOR: 'device_operator',
  EXPIRATION_DATE_OF_DEVICE: 'expiration_date_of_device',
  DATE_RETURNED_TO_MANUFACTURER: 'date_returned_to_manufacturer',
  IMPLANT_DATE_YEAR: 'implant_date_year',
  DATE_REMOVED_YEAR: 'date_removed_year',
  SERVICED_BY_3RD_PARTY_FLAG: 'serviced_by_3rd_party_flag',
  COMBINATION_PRODUCT_FLAG: 'combination_product_flag',
  UDI_DI: 'udi_di',
  UDI_PUBLIC: 'udi_public',
  DEVICE_AGE_TEXT: 'device_age_text',
  DEVICE_EVALUATED_BY_MANUFACTURER: 'device_evaluated_by_manufacturer',
  PATIENT_SEQUENCE_NUMBER: 'patient_sequence_number',
  PATIENT_AGE: 'patient_age',
  PATIENT_SEX: 'patient_sex',
  PATIENT_WEIGHT: 'patient_weight',
  PATIENT_ETHNICITY: 'patient_ethnicity',
  PATIENT_RACE: 'patient_race',
  PATIENT_AGE_NUMERIC: 'patient_age_numeric',
  OUTCOME_DEATH: 'outcome_death',
  OUTCOME_LIFE_THREATENING: 'outcome_life_threatening',
  OUTCOME_HOSPITALIZATION: 'outcome_hospitalization',
  OUTCOME_DISABILITY: 'outcome_disability',
  OUTCOME_CONGENITAL_ANOMALY: 'outcome_congenital_anomaly',
  OUTCOME_REQUIRED_INTERVENTION: 'outcome_required_intervention',
  OUTCOME_OTHER: 'outcome_other',
  MDR_TEXT_KEY: 'mdr_text_key',
  TEXT_TYPE_CODE: 'text_type_code',
  TEXT_CONTENT: 'text_content',
  DEVICE_PROBLEM_CODE: 'device_problem_code',
  PATIENT_PROBLEM_CODE: 'patient_problem_code',
} as const

export type FieldName = (typeof COMMON_FIELDS)[keyof typeof COMMON_FIELDS]

// =============================================================================
// EVENT TYPE DEFINITIONS
// =============================================================================

export interface EventTypeInfo {
  dbCode: string
  filterCode: string
  name: string
  description: string
  severity: number
  color: string
  bgClass: string
  textClass: string
}

/**
 * Event types with full metadata.
 * Keys are database codes (D, IN, M, O).
 */
export const EVENT_TYPES: Record<string, EventTypeInfo> = {
  'D': {
    dbCode: 'D',
    filterCode: 'D',
    name: 'Death',
    description: 'Patient death associated with device',
    severity: 1,
    color: '#dc2626',
    bgClass: 'bg-red-100',
    textClass: 'text-red-800',
  },
  'IN': {
    dbCode: 'IN',
    filterCode: 'I',
    name: 'Injury',
    description: 'Patient injury associated with device',
    severity: 2,
    color: '#ea580c',
    bgClass: 'bg-orange-100',
    textClass: 'text-orange-800',
  },
  'M': {
    dbCode: 'M',
    filterCode: 'M',
    name: 'Malfunction',
    description: 'Device malfunction',
    severity: 3,
    color: '#ca8a04',
    bgClass: 'bg-yellow-100',
    textClass: 'text-yellow-800',
  },
  'O': {
    dbCode: 'O',
    filterCode: 'O',
    name: 'Other',
    description: 'Other event type',
    severity: 4,
    color: '#6b7280',
    bgClass: 'bg-gray-100',
    textClass: 'text-gray-800',
  },
  '*': {
    dbCode: '*',
    filterCode: '*',
    name: 'Unknown',
    description: 'No answer provided',
    severity: 5,
    color: '#9ca3af',
    bgClass: 'bg-gray-50',
    textClass: 'text-gray-600',
  },
} as const

/**
 * Maps filter codes to database codes.
 * Filter uses 'I' for injury, database uses 'IN'.
 */
export const FILTER_TO_DB_CODE: Record<string, string> = {
  'D': 'D',
  'I': 'IN',
  'M': 'M',
  'O': 'O',
  '*': '*',
} as const

/**
 * Maps database codes to filter codes.
 */
export const DB_TO_FILTER_CODE: Record<string, string> = {
  'D': 'D',
  'IN': 'I',
  'M': 'M',
  'O': 'O',
  '*': '*',
} as const

/**
 * Event type options for filter dropdowns/buttons.
 * Excludes 'Unknown' (*) type.
 */
export const EVENT_TYPE_OPTIONS = [
  { value: 'D', label: 'Death', dbCode: 'D' },
  { value: 'I', label: 'Injury', dbCode: 'IN' },
  { value: 'M', label: 'Malfunction', dbCode: 'M' },
  { value: 'O', label: 'Other', dbCode: 'O' },
] as const

export type EventTypeDbCode = keyof typeof EVENT_TYPES
export type EventTypeFilterCode = (typeof EVENT_TYPE_OPTIONS)[number]['value']

// =============================================================================
// PATIENT OUTCOME DEFINITIONS
// =============================================================================

export interface OutcomeInfo {
  code: string
  name: string
  dbField: string
  severity: number
  colorClass: string
}

export const OUTCOME_CODES: Record<string, OutcomeInfo> = {
  'D': {
    code: 'D',
    name: 'Death',
    dbField: 'outcome_death',
    severity: 1,
    colorClass: 'bg-red-100 text-red-800',
  },
  'L': {
    code: 'L',
    name: 'Life Threatening',
    dbField: 'outcome_life_threatening',
    severity: 2,
    colorClass: 'bg-yellow-100 text-yellow-800',
  },
  'H': {
    code: 'H',
    name: 'Hospitalization',
    dbField: 'outcome_hospitalization',
    severity: 3,
    colorClass: 'bg-orange-100 text-orange-800',
  },
  'DS': {
    code: 'DS',
    name: 'Disability',
    dbField: 'outcome_disability',
    severity: 4,
    colorClass: 'bg-purple-100 text-purple-800',
  },
  'CA': {
    code: 'CA',
    name: 'Congenital Anomaly',
    dbField: 'outcome_congenital_anomaly',
    severity: 5,
    colorClass: 'bg-pink-100 text-pink-800',
  },
  'RI': {
    code: 'RI',
    name: 'Required Intervention',
    dbField: 'outcome_required_intervention',
    severity: 6,
    colorClass: 'bg-blue-100 text-blue-800',
  },
  'OT': {
    code: 'OT',
    name: 'Other',
    dbField: 'outcome_other',
    severity: 7,
    colorClass: 'bg-gray-100 text-gray-800',
  },
} as const

export type OutcomeCode = keyof typeof OUTCOME_CODES

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
  'D': {
    code: 'D',
    name: 'Event Description',
    description: 'Primary event description narrative',
    priority: 1,
  },
  'H': {
    code: 'H',
    name: 'Event History',
    description: 'Historical context of event',
    priority: 2,
  },
  'M': {
    code: 'M',
    name: 'Manufacturer Narrative',
    description: 'Manufacturer\'s description',
    priority: 3,
  },
  'E': {
    code: 'E',
    name: 'Evaluation Summary',
    description: 'Evaluation/assessment summary',
    priority: 4,
  },
  'N': {
    code: 'N',
    name: 'Additional Information',
    description: 'Additional notes and information',
    priority: 5,
  },
} as const

export type TextTypeCode = keyof typeof TEXT_TYPE_CODES

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

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
 * Get event type display info by database code.
 */
export function getEventTypeDisplay(dbCode: string): { label: string; color: string } {
  const eventType = EVENT_TYPES[dbCode]
  if (eventType) {
    return {
      label: eventType.name,
      color: `${eventType.bgClass} ${eventType.textClass}`,
    }
  }
  return { label: dbCode || 'Unknown', color: 'bg-gray-100 text-gray-800' }
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

/**
 * Convert list of filter codes to database codes.
 */
export function convertFilterEventTypes(filterCodes: string[]): string[] {
  return filterCodes.map(filterToDbCode)
}
