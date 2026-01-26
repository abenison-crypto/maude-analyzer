import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client'
import { useFilters } from './useFilters'

export function useTrends(groupBy: 'day' | 'month' | 'year' = 'month') {
  const { filters } = useFilters()

  return useQuery({
    queryKey: ['trends', filters.manufacturers, filters.productCodes, filters.eventTypes, filters.dateFrom, filters.dateTo, groupBy],
    queryFn: () =>
      api.getTrends({
        manufacturers: filters.manufacturers.length ? filters.manufacturers : undefined,
        productCodes: filters.productCodes.length ? filters.productCodes : undefined,
        eventTypes: filters.eventTypes.length ? filters.eventTypes : undefined,
        dateFrom: filters.dateFrom || undefined,
        dateTo: filters.dateTo || undefined,
        groupBy,
      }),
  })
}

export function useManufacturerComparison(manufacturers: string[]) {
  const { filters } = useFilters()

  return useQuery({
    queryKey: ['manufacturerComparison', manufacturers, filters.dateFrom, filters.dateTo],
    queryFn: () =>
      api.compareManufacturers(
        manufacturers,
        filters.dateFrom || undefined,
        filters.dateTo || undefined
      ),
    enabled: manufacturers.length > 0,
  })
}

export function useEventTypeDistribution() {
  const { filters } = useFilters()

  return useQuery({
    queryKey: ['eventTypeDistribution', filters.manufacturers, filters.productCodes, filters.dateFrom, filters.dateTo],
    queryFn: () =>
      api.getEventTypeDistribution({
        manufacturers: filters.manufacturers.length ? filters.manufacturers : undefined,
        productCodes: filters.productCodes.length ? filters.productCodes : undefined,
        dateFrom: filters.dateFrom || undefined,
        dateTo: filters.dateTo || undefined,
      }),
  })
}

export function useDatabaseStatus() {
  return useQuery({
    queryKey: ['databaseStatus'],
    queryFn: () => api.getDatabaseStatus(),
    staleTime: 60 * 1000, // 1 minute
  })
}

export function useDataQuality() {
  return useQuery({
    queryKey: ['dataQuality'],
    queryFn: () => api.getDataQuality(),
    staleTime: 5 * 60 * 1000, // 5 minutes
  })
}

export function useIngestionHistory() {
  return useQuery({
    queryKey: ['ingestionHistory'],
    queryFn: () => api.getIngestionHistory(),
    staleTime: 60 * 1000,
  })
}
