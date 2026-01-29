/**
 * TypeScript types for the advanced signal detection system.
 */

export type SignalMethod = 'zscore' | 'prr' | 'ror' | 'ebgm' | 'cusum' | 'yoy' | 'pop' | 'rolling'

export type DrillDownLevel = 'manufacturer' | 'brand' | 'generic' | 'model'

export type TimeComparisonMode = 'lookback' | 'custom' | 'yoy' | 'rolling'

export type ComparisonPopulation = 'all' | 'same_product_code' | 'custom'

export type SignalStrength = 'high' | 'elevated' | 'normal'

export interface TimePeriod {
  start_date: string
  end_date: string
}

export interface TimeComparisonConfig {
  mode: TimeComparisonMode
  lookback_months?: number
  period_a?: TimePeriod
  period_b?: TimePeriod
  current_year?: number
  comparison_year?: number
  quarter?: 1 | 2 | 3 | 4
  rolling_window_months?: number
  comparison_month?: string  // YYYY-MM format - specific month for z-score comparison
}

import type { ActiveEntityGroup } from './entityGroups'

export interface SignalRequest {
  methods: SignalMethod[]
  time_config: TimeComparisonConfig
  level: DrillDownLevel
  parent_value?: string | null
  product_codes?: string[]
  event_types?: string[]
  comparison_population?: ComparisonPopulation
  comparison_filters?: Record<string, unknown>
  active_groups?: ActiveEntityGroup[]
  min_events?: number
  limit?: number
  zscore_high_threshold?: number
  zscore_elevated_threshold?: number
  prr_threshold?: number
  ror_threshold?: number
  change_pct_high?: number
  change_pct_elevated?: number
}

// Typed detail interfaces for each method
export interface ZScoreDetails {
  avg_monthly: number
  std_monthly: number
  latest_month: number
  monthly_series?: MonthlyDataPoint[]
}

export interface PRRDetails {
  a: number  // Entity target events (deaths)
  b: number  // Entity other events
  c: number  // Other entities target events
  d: number  // Other entities other events
}

export interface RORDetails {
  a: number
  b: number
  c: number
  d: number
}

export interface EBGMDetails {
  observed: number
  expected: number
  rr: number
}

export interface CUSUMDetails {
  mean: number
  std: number
  control_limit: number
  cusum_series?: CUSUMDataPoint[]
}

export interface YoYDetails {
  current_period: number
  comparison_period: number
}

export interface RollingDetails {
  rolling_avg: number
  rolling_std: number
  latest: number
  window_months: number
  monthly_series?: MonthlyDataPoint[]
}

export interface MonthlyDataPoint {
  month: string
  count: number
}

export interface CUSUMDataPoint {
  month: string
  cusum: number
  count: number
}

export type MethodDetails =
  | ZScoreDetails
  | PRRDetails
  | RORDetails
  | EBGMDetails
  | CUSUMDetails
  | YoYDetails
  | RollingDetails

export interface MethodResult {
  method: SignalMethod
  value: number | null
  lower_ci?: number | null
  upper_ci?: number | null
  is_signal: boolean
  signal_strength: SignalStrength
  details?: MethodDetails
}

export interface SignalResult {
  entity: string
  entity_level: DrillDownLevel
  total_events: number
  deaths: number
  injuries: number
  malfunctions: number
  current_period_events?: number | null
  comparison_period_events?: number | null
  change_pct?: number | null
  method_results: MethodResult[]
  signal_type: SignalStrength
  has_children: boolean
  child_level?: DrillDownLevel | null
}

export interface TimeInfo {
  mode: TimeComparisonMode
  analysis_start: string
  analysis_end: string
  comparison_start?: string | null
  comparison_end?: string | null
  rolling_window?: number | null
}

export interface DataCompleteness {
  last_complete_month: string
  incomplete_months: string[]
  estimated_lag_months: number
}

export interface SignalResponse {
  level: DrillDownLevel
  parent_value?: string | null
  methods_applied: SignalMethod[]
  time_info: TimeInfo
  signals: SignalResult[]
  total_entities_analyzed: number
  high_signal_count: number
  elevated_signal_count: number
  normal_count: number
  data_note?: string | null
  data_completeness?: DataCompleteness
}

// UI State types

export interface DrillDownState {
  level: DrillDownLevel
  path: Array<{ level: DrillDownLevel; value: string }>
}

export interface SignalMethodOption {
  id: SignalMethod
  label: string
  description: string
  category: 'time' | 'disproportionality'
}

export const SIGNAL_METHODS: SignalMethodOption[] = [
  { id: 'zscore', label: 'Z-Score', description: 'Statistical anomaly vs historical', category: 'time' },
  { id: 'yoy', label: 'Year-over-Year', description: 'Compare same period across years', category: 'time' },
  { id: 'pop', label: 'Period-over-Period', description: 'Custom range comparison', category: 'time' },
  { id: 'rolling', label: 'Rolling Average', description: 'Compare to moving baseline', category: 'time' },
  { id: 'cusum', label: 'CUSUM', description: 'Detect drift over time', category: 'time' },
  { id: 'prr', label: 'PRR', description: 'Proportional Reporting Ratio', category: 'disproportionality' },
  { id: 'ror', label: 'ROR', description: 'Reporting Odds Ratio', category: 'disproportionality' },
  { id: 'ebgm', label: 'EBGM', description: 'Empirical Bayes (FDA method)', category: 'disproportionality' },
]

export const LEVEL_LABELS: Record<DrillDownLevel, string> = {
  manufacturer: 'Manufacturer',
  brand: 'Brand Name',
  generic: 'Generic Name',
  model: 'Model Number',
}

export const TIME_MODE_LABELS: Record<TimeComparisonMode, string> = {
  lookback: 'Lookback Period',
  custom: 'Custom Range',
  yoy: 'Year-over-Year',
  rolling: 'Rolling Average',
}
