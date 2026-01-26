const API_BASE = '/api'

export interface EventFilters {
  manufacturers?: string[]
  productCodes?: string[]
  eventTypes?: string[]
  dateFrom?: string
  dateTo?: string
  searchText?: string
  page?: number
  pageSize?: number
}

export interface EventSummary {
  mdr_report_key: string
  report_number: string | null
  date_received: string | null
  date_of_event: string | null
  event_type: string | null
  manufacturer: string | null
  product_code: string | null
}

export interface EventDetail extends EventSummary {
  manufacturer_name: string | null
  manufacturer_city: string | null
  manufacturer_state: string | null
  manufacturer_country: string | null
  adverse_event_flag: string | null
  product_problem_flag: string | null
  devices: Array<{
    brand_name: string | null
    generic_name: string | null
    model_number: string | null
    manufacturer: string | null
    product_code: string | null
  }>
  narratives: Array<{
    type: string | null
    text: string | null
  }>
  patients: Array<{
    sequence: number | null
    age: string | null
    sex: string | null
    outcomes: {
      death: boolean
      life_threatening: boolean
      hospitalization: boolean
      disability: boolean
      other: boolean
    }
  }>
}

export interface PaginationInfo {
  page: number
  page_size: number
  total: number
  total_pages: number
}

export interface EventListResponse {
  events: EventSummary[]
  pagination: PaginationInfo
}

export interface StatsResponse {
  total: number
  deaths: number
  injuries: number
  malfunctions: number
  other: number
}

export interface TrendData {
  period: string | null
  total: number
  deaths: number
  injuries: number
  malfunctions: number
}

export interface ManufacturerItem {
  name: string
  count: number
}

export interface ProductCodeItem {
  code: string
  name: string | null
  count: number
}

export interface DatabaseStatus {
  total_events: number
  total_devices: number
  total_patients: number
  manufacturer_coverage_pct: number
  date_range_start: string | null
  date_range_end: string | null
  last_refresh: string | null
}

function buildQueryString(filters: EventFilters): string {
  const params = new URLSearchParams()

  if (filters.manufacturers?.length) {
    params.set('manufacturers', filters.manufacturers.join(','))
  }
  if (filters.productCodes?.length) {
    params.set('product_codes', filters.productCodes.join(','))
  }
  if (filters.eventTypes?.length) {
    params.set('event_types', filters.eventTypes.join(','))
  }
  if (filters.dateFrom) {
    params.set('date_from', filters.dateFrom)
  }
  if (filters.dateTo) {
    params.set('date_to', filters.dateTo)
  }
  if (filters.searchText) {
    params.set('search_text', filters.searchText)
  }
  if (filters.page) {
    params.set('page', String(filters.page))
  }
  if (filters.pageSize) {
    params.set('page_size', String(filters.pageSize))
  }

  const str = params.toString()
  return str ? `?${str}` : ''
}

async function fetchJSON<T>(url: string): Promise<T> {
  const response = await fetch(url)
  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`)
  }
  return response.json()
}

export const api = {
  // Events
  getEvents: (filters: EventFilters = {}): Promise<EventListResponse> =>
    fetchJSON(`${API_BASE}/events${buildQueryString(filters)}`),

  getEventStats: (filters: Omit<EventFilters, 'page' | 'pageSize' | 'searchText'> = {}): Promise<StatsResponse> =>
    fetchJSON(`${API_BASE}/events/stats${buildQueryString(filters)}`),

  getEventDetail: (mdrReportKey: string): Promise<EventDetail> =>
    fetchJSON(`${API_BASE}/events/${mdrReportKey}`),

  getManufacturers: (search?: string, limit = 100): Promise<ManufacturerItem[]> =>
    fetchJSON(`${API_BASE}/events/manufacturers?${new URLSearchParams({ ...(search && { search }), limit: String(limit) })}`),

  getProductCodes: (search?: string, limit = 100): Promise<ProductCodeItem[]> =>
    fetchJSON(`${API_BASE}/events/product-codes?${new URLSearchParams({ ...(search && { search }), limit: String(limit) })}`),

  // Analytics
  getTrends: (
    filters: Omit<EventFilters, 'page' | 'pageSize' | 'searchText'> & { groupBy?: 'day' | 'month' | 'year' } = {}
  ): Promise<TrendData[]> => {
    const params = new URLSearchParams()
    if (filters.manufacturers?.length) params.set('manufacturers', filters.manufacturers.join(','))
    if (filters.productCodes?.length) params.set('product_codes', filters.productCodes.join(','))
    if (filters.eventTypes?.length) params.set('event_types', filters.eventTypes.join(','))
    if (filters.dateFrom) params.set('date_from', filters.dateFrom)
    if (filters.dateTo) params.set('date_to', filters.dateTo)
    if (filters.groupBy) params.set('group_by', filters.groupBy)
    return fetchJSON(`${API_BASE}/analytics/trends?${params}`)
  },

  compareManufacturers: (
    manufacturers: string[],
    dateFrom?: string,
    dateTo?: string
  ): Promise<Array<{ manufacturer: string; total: number; deaths: number; injuries: number; malfunctions: number }>> => {
    const params = new URLSearchParams({ manufacturers: manufacturers.join(',') })
    if (dateFrom) params.set('date_from', dateFrom)
    if (dateTo) params.set('date_to', dateTo)
    return fetchJSON(`${API_BASE}/analytics/compare?${params}`)
  },

  getEventTypeDistribution: (filters: Omit<EventFilters, 'page' | 'pageSize' | 'searchText'> = {}): Promise<
    Array<{ type: string; count: number; percentage: number }>
  > =>
    fetchJSON(`${API_BASE}/analytics/event-type-distribution${buildQueryString(filters)}`),

  // Admin
  getDatabaseStatus: (): Promise<DatabaseStatus> =>
    fetchJSON(`${API_BASE}/admin/status`),

  getDataQuality: (): Promise<{
    field_completeness: Array<{ field: string; percentage: number }>
    event_type_distribution: Array<{ type: string; count: number }>
    orphan_analysis: { orphaned_devices: number; events_without_devices: number }
  }> =>
    fetchJSON(`${API_BASE}/admin/data-quality`),

  getIngestionHistory: (limit = 50): Promise<
    Array<{
      id: number
      file_name: string
      file_type: string
      records_loaded: number
      records_errors: number
      started_at: string
      completed_at: string
      status: string
    }>
  > =>
    fetchJSON(`${API_BASE}/admin/history?limit=${limit}`),
}
