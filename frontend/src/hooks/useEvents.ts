import { useQuery } from '@tanstack/react-query'
import { api, EventFilters } from '../api/client'
import { useFilters } from './useFilters'

export function useEventStats() {
  const { filters } = useFilters()

  return useQuery({
    queryKey: ['eventStats', filters.manufacturers, filters.productCodes, filters.eventTypes, filters.dateFrom, filters.dateTo],
    queryFn: () =>
      api.getEventStats({
        manufacturers: filters.manufacturers.length ? filters.manufacturers : undefined,
        productCodes: filters.productCodes.length ? filters.productCodes : undefined,
        eventTypes: filters.eventTypes.length ? filters.eventTypes : undefined,
        dateFrom: filters.dateFrom || undefined,
        dateTo: filters.dateTo || undefined,
      }),
  })
}

export function useEvents(page = 1, pageSize = 50) {
  const { filters } = useFilters()

  return useQuery({
    queryKey: ['events', filters, page, pageSize],
    queryFn: () =>
      api.getEvents({
        manufacturers: filters.manufacturers.length ? filters.manufacturers : undefined,
        productCodes: filters.productCodes.length ? filters.productCodes : undefined,
        eventTypes: filters.eventTypes.length ? filters.eventTypes : undefined,
        dateFrom: filters.dateFrom || undefined,
        dateTo: filters.dateTo || undefined,
        searchText: filters.searchText || undefined,
        page,
        pageSize,
      }),
  })
}

export function useEventDetail(mdrReportKey: string) {
  return useQuery({
    queryKey: ['eventDetail', mdrReportKey],
    queryFn: () => api.getEventDetail(mdrReportKey),
    enabled: !!mdrReportKey,
  })
}

export function useManufacturers(search?: string) {
  return useQuery({
    queryKey: ['manufacturers', search],
    queryFn: () => api.getManufacturers(search),
    staleTime: 10 * 60 * 1000, // 10 minutes
  })
}

export function useProductCodes(search?: string) {
  return useQuery({
    queryKey: ['productCodes', search],
    queryFn: () => api.getProductCodes(search),
    staleTime: 10 * 60 * 1000,
  })
}
