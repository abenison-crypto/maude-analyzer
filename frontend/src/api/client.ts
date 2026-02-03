import type { SignalRequest, SignalResponse } from '../types/signals'
import type {
  EntityGroup,
  EntityGroupListResponse,
  CreateEntityGroupRequest,
  UpdateEntityGroupRequest,
  SuggestNameResponse,
  EntityType,
  AvailableEntitiesResponse,
} from '../types/entityGroups'
import { fromApiResponse, fromApiListResponse } from '../types/entityGroups'

const API_BASE = '/api'

export interface EventFilters {
  // Core filters
  manufacturers?: string[]
  productCodes?: string[]
  eventTypes?: string[]
  dateFrom?: string
  dateTo?: string
  searchText?: string
  // Device filters
  brandNames?: string[]
  genericNames?: string[]
  deviceManufacturers?: string[]
  modelNumbers?: string[]
  implantFlag?: 'Y' | 'N' | ''
  deviceProductCodes?: string[]
  // Pagination
  page?: number
  pageSize?: number
}

export interface AutocompleteItem {
  value: string
  label: string
  count?: number
}

export interface FilterPreset {
  id: string
  name: string
  description?: string
  filters: Partial<EventFilters>
  isBuiltIn: boolean
  createdAt: string
  updatedAt: string
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

  // Core filters
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

  // Device filters
  if (filters.brandNames?.length) {
    params.set('brand_names', filters.brandNames.join(','))
  }
  if (filters.genericNames?.length) {
    params.set('generic_names', filters.genericNames.join(','))
  }
  if (filters.deviceManufacturers?.length) {
    params.set('device_manufacturers', filters.deviceManufacturers.join(','))
  }
  if (filters.modelNumbers?.length) {
    params.set('model_numbers', filters.modelNumbers.join(','))
  }
  if (filters.implantFlag) {
    params.set('implant_flag', filters.implantFlag)
  }
  if (filters.deviceProductCodes?.length) {
    params.set('device_product_codes', filters.deviceProductCodes.join(','))
  }

  // Pagination
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

  // Device filter autocomplete
  getBrandNames: (search?: string, limit = 50): Promise<AutocompleteItem[]> =>
    fetchJSON(`${API_BASE}/filters/brand-names?${new URLSearchParams({ ...(search && { search }), limit: String(limit) })}`),

  getGenericNames: (search?: string, limit = 50): Promise<AutocompleteItem[]> =>
    fetchJSON(`${API_BASE}/filters/generic-names?${new URLSearchParams({ ...(search && { search }), limit: String(limit) })}`),

  getDeviceManufacturers: (search?: string, limit = 50): Promise<AutocompleteItem[]> =>
    fetchJSON(`${API_BASE}/filters/device-manufacturers?${new URLSearchParams({ ...(search && { search }), limit: String(limit) })}`),

  getModelNumbers: (search?: string, limit = 50): Promise<AutocompleteItem[]> =>
    fetchJSON(`${API_BASE}/filters/model-numbers?${new URLSearchParams({ ...(search && { search }), limit: String(limit) })}`),

  getDeviceProductCodes: (search?: string, limit = 50): Promise<AutocompleteItem[]> =>
    fetchJSON(`${API_BASE}/filters/device-product-codes?${new URLSearchParams({ ...(search && { search }), limit: String(limit) })}`),

  // Presets
  getPresets: (includeBuiltIn = true): Promise<FilterPreset[]> =>
    fetchJSON(`${API_BASE}/presets?include_built_in=${includeBuiltIn}`),

  getPreset: (presetId: string): Promise<FilterPreset> =>
    fetchJSON(`${API_BASE}/presets/${presetId}`),

  createPreset: async (name: string, description: string | undefined, filters: Partial<EventFilters>): Promise<FilterPreset> => {
    const response = await fetch(`${API_BASE}/presets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, description, filters }),
    })
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`)
    return response.json()
  },

  updatePreset: async (presetId: string, updates: { name?: string; description?: string; filters?: Partial<EventFilters> }): Promise<FilterPreset> => {
    const response = await fetch(`${API_BASE}/presets/${presetId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(updates),
    })
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`)
    return response.json()
  },

  deletePreset: async (presetId: string): Promise<void> => {
    const response = await fetch(`${API_BASE}/presets/${presetId}`, {
      method: 'DELETE',
    })
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`)
  },

  // Analytics
  getTrends: (
    filters: Omit<EventFilters, 'page' | 'pageSize' | 'searchText'> & {
      groupBy?: 'day' | 'month' | 'year'
      dateField?: 'date_received' | 'date_of_event'
    } = {}
  ): Promise<TrendData[]> => {
    const params = new URLSearchParams()
    if (filters.manufacturers?.length) params.set('manufacturers', filters.manufacturers.join(','))
    if (filters.productCodes?.length) params.set('product_codes', filters.productCodes.join(','))
    if (filters.eventTypes?.length) params.set('event_types', filters.eventTypes.join(','))
    if (filters.dateFrom) params.set('date_from', filters.dateFrom)
    if (filters.dateTo) params.set('date_to', filters.dateTo)
    if (filters.groupBy) params.set('group_by', filters.groupBy)
    if (filters.dateField) params.set('date_field', filters.dateField)
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

  // Safety Signals
  getSafetySignals: (params: {
    // Core filters
    manufacturers?: string[]
    productCodes?: string[]
    eventTypes?: string[]
    dateFrom?: string
    dateTo?: string
    // Device filters
    brandNames?: string[]
    genericNames?: string[]
    deviceManufacturers?: string[]
    modelNumbers?: string[]
    implantFlag?: string
    deviceProductCodes?: string[]
    // Signal params
    lookbackMonths?: number
    minThreshold?: number
  } = {}): Promise<{
    lookback_months: number
    signals: Array<{
      manufacturer: string
      avg_monthly: number
      std_monthly: number
      total_events: number
      total_deaths: number
      latest_month: number
      z_score: number
      signal_type: 'high' | 'elevated' | 'normal'
    }>
  }> => {
    const urlParams = new URLSearchParams()
    // Core filters
    if (params.manufacturers?.length) urlParams.set('manufacturers', params.manufacturers.join(','))
    if (params.productCodes?.length) urlParams.set('product_codes', params.productCodes.join(','))
    if (params.eventTypes?.length) urlParams.set('event_types', params.eventTypes.join(','))
    if (params.dateFrom) urlParams.set('date_from', params.dateFrom)
    if (params.dateTo) urlParams.set('date_to', params.dateTo)
    // Device filters
    if (params.brandNames?.length) urlParams.set('brand_names', params.brandNames.join(','))
    if (params.genericNames?.length) urlParams.set('generic_names', params.genericNames.join(','))
    if (params.deviceManufacturers?.length) urlParams.set('device_manufacturers', params.deviceManufacturers.join(','))
    if (params.modelNumbers?.length) urlParams.set('model_numbers', params.modelNumbers.join(','))
    if (params.implantFlag) urlParams.set('implant_flag', params.implantFlag)
    if (params.deviceProductCodes?.length) urlParams.set('device_product_codes', params.deviceProductCodes.join(','))
    // Signal params
    if (params.lookbackMonths) urlParams.set('lookback_months', String(params.lookbackMonths))
    if (params.minThreshold) urlParams.set('min_threshold', String(params.minThreshold))
    return fetchJSON(`${API_BASE}/analytics/signals?${urlParams}`)
  },

  // Advanced Safety Signals (POST)
  postAdvancedSignals: async (request: SignalRequest): Promise<SignalResponse> => {
    const response = await fetch(`${API_BASE}/analytics/signals/advanced`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(request),
    })
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }
    return response.json()
  },

  // Text Frequency
  getTextFrequency: (filters: Omit<EventFilters, 'page' | 'pageSize'> & {
    minWordLength?: number
    topN?: number
    sampleSize?: number
  } = {}): Promise<Array<{ term: string; count: number; percentage: number }>> => {
    const params = new URLSearchParams()
    if (filters.manufacturers?.length) params.set('manufacturers', filters.manufacturers.join(','))
    if (filters.productCodes?.length) params.set('product_codes', filters.productCodes.join(','))
    if (filters.eventTypes?.length) params.set('event_types', filters.eventTypes.join(','))
    if (filters.dateFrom) params.set('date_from', filters.dateFrom)
    if (filters.dateTo) params.set('date_to', filters.dateTo)
    if (filters.minWordLength) params.set('min_word_length', String(filters.minWordLength))
    if (filters.topN) params.set('top_n', String(filters.topN))
    if (filters.sampleSize) params.set('sample_size', String(filters.sampleSize))
    return fetchJSON(`${API_BASE}/analytics/text-frequency?${params}`)
  },

  // Entity Groups
  getEntityGroups: async (params: {
    entityType?: EntityType
    includeBuiltIn?: boolean
    activeOnly?: boolean
  } = {}): Promise<EntityGroupListResponse> => {
    const urlParams = new URLSearchParams()
    if (params.entityType) urlParams.set('entity_type', params.entityType)
    if (params.includeBuiltIn !== undefined) urlParams.set('include_built_in', String(params.includeBuiltIn))
    if (params.activeOnly) urlParams.set('active_only', 'true')
    const data = await fetchJSON<Record<string, unknown>>(`${API_BASE}/entity-groups?${urlParams}`)
    return fromApiListResponse(data)
  },

  getEntityGroup: async (groupId: string): Promise<EntityGroup> => {
    const data = await fetchJSON<Record<string, unknown>>(`${API_BASE}/entity-groups/${groupId}`)
    return fromApiResponse(data)
  },

  createEntityGroup: async (request: CreateEntityGroupRequest): Promise<EntityGroup> => {
    const response = await fetch(`${API_BASE}/entity-groups`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(request),
    })
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`)
    const data = await response.json()
    return fromApiResponse(data)
  },

  updateEntityGroup: async (groupId: string, request: UpdateEntityGroupRequest): Promise<EntityGroup> => {
    const response = await fetch(`${API_BASE}/entity-groups/${groupId}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(request),
    })
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`)
    const data = await response.json()
    return fromApiResponse(data)
  },

  deleteEntityGroup: async (groupId: string): Promise<void> => {
    const response = await fetch(`${API_BASE}/entity-groups/${groupId}`, {
      method: 'DELETE',
    })
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`)
  },

  activateEntityGroup: async (groupId: string): Promise<EntityGroup> => {
    const response = await fetch(`${API_BASE}/entity-groups/${groupId}/activate`, {
      method: 'POST',
    })
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`)
    const data = await response.json()
    return fromApiResponse(data)
  },

  deactivateEntityGroup: async (groupId: string): Promise<EntityGroup> => {
    const response = await fetch(`${API_BASE}/entity-groups/${groupId}/deactivate`, {
      method: 'POST',
    })
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`)
    const data = await response.json()
    return fromApiResponse(data)
  },

  suggestGroupName: async (members: string[]): Promise<SuggestNameResponse> => {
    const params = new URLSearchParams({ members: members.join(',') })
    return fetchJSON(`${API_BASE}/entity-groups/suggest-name?${params}`)
  },

  getAvailableEntities: async (params: {
    entityType?: EntityType
    productCodes?: string[]
    eventTypes?: string[]
    search?: string
    limit?: number
  } = {}): Promise<AvailableEntitiesResponse> => {
    const urlParams = new URLSearchParams()
    if (params.entityType) urlParams.set('entity_type', params.entityType)
    if (params.productCodes?.length) urlParams.set('product_codes', params.productCodes.join(','))
    if (params.eventTypes?.length) urlParams.set('event_types', params.eventTypes.join(','))
    if (params.search) urlParams.set('search', params.search)
    if (params.limit) urlParams.set('limit', String(params.limit))
    return fetchJSON(`${API_BASE}/entity-groups/available-entities?${urlParams}`)
  },

  getActiveEntityGroups: async (entityType?: EntityType): Promise<EntityGroupListResponse> => {
    const urlParams = new URLSearchParams()
    if (entityType) urlParams.set('entity_type', entityType)
    const data = await fetchJSON<Record<string, unknown>>(`${API_BASE}/entity-groups/active/all?${urlParams}`)
    return fromApiListResponse(data)
  },
}
