import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client'
import { useAdvancedFilters } from './useAdvancedFilters'

export function useTrends(groupBy: 'day' | 'month' | 'year' = 'month') {
  const { filters } = useAdvancedFilters()

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
  const { filters } = useAdvancedFilters()

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
  const { filters } = useAdvancedFilters()

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

export function useSafetySignals(lookbackMonths = 12, minThreshold = 10) {
  const { filters } = useAdvancedFilters()

  return useQuery({
    queryKey: ['safetySignals', filters.manufacturers, filters.productCodes, lookbackMonths, minThreshold],
    queryFn: () =>
      api.getSafetySignals({
        manufacturers: filters.manufacturers.length ? filters.manufacturers : undefined,
        productCodes: filters.productCodes.length ? filters.productCodes : undefined,
        lookbackMonths,
        minThreshold,
      }),
    staleTime: 5 * 60 * 1000, // 5 minutes
  })
}

export function useTextFrequency(sampleSize = 1000) {
  const { filters } = useAdvancedFilters()

  return useQuery({
    queryKey: ['textFrequency', filters.manufacturers, filters.productCodes, filters.eventTypes, filters.dateFrom, filters.dateTo, sampleSize],
    queryFn: () =>
      api.getTextFrequency({
        manufacturers: filters.manufacturers.length ? filters.manufacturers : undefined,
        productCodes: filters.productCodes.length ? filters.productCodes : undefined,
        eventTypes: filters.eventTypes.length ? filters.eventTypes : undefined,
        dateFrom: filters.dateFrom || undefined,
        dateTo: filters.dateTo || undefined,
        sampleSize,
      }),
    staleTime: 5 * 60 * 1000, // 5 minutes
  })
}
