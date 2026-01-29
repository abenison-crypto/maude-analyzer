import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../api/client'
import type {
  SignalRequest,
  SignalMethod,
  DrillDownLevel,
  TimeComparisonConfig,
} from '../types/signals'

interface UseAdvancedSignalsOptions {
  methods: SignalMethod[]
  timeConfig: TimeComparisonConfig
  level: DrillDownLevel
  parentValue?: string | null
  productCodes?: string[]
  eventTypes?: string[]
  minEvents?: number
  limit?: number
  enabled?: boolean
}

export function useAdvancedSignals({
  methods,
  timeConfig,
  level,
  parentValue,
  productCodes,
  eventTypes,
  minEvents = 10,
  limit = 20,
  enabled = true,
}: UseAdvancedSignalsOptions) {
  const request: SignalRequest = {
    methods,
    time_config: timeConfig,
    level,
    parent_value: parentValue,
    product_codes: productCodes,
    event_types: eventTypes,
    min_events: minEvents,
    limit,
  }

  return useQuery({
    queryKey: [
      'advancedSignals',
      methods,
      timeConfig,
      level,
      parentValue,
      productCodes,
      eventTypes,
      minEvents,
      limit,
    ],
    queryFn: () => api.postAdvancedSignals(request),
    enabled,
    staleTime: 5 * 60 * 1000, // 5 minutes
  })
}

// Mutation hook for on-demand signal detection
export function useSignalDetection() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (request: SignalRequest) => api.postAdvancedSignals(request),
    onSuccess: (data, variables) => {
      // Cache the result
      queryClient.setQueryData(
        [
          'advancedSignals',
          variables.methods,
          variables.time_config,
          variables.level,
          variables.parent_value,
          variables.product_codes,
          variables.event_types,
          variables.min_events,
          variables.limit,
        ],
        data
      )
    },
  })
}

// Helper hook for drill-down navigation
export function useDrillDown(initialLevel: DrillDownLevel = 'manufacturer') {
  const drillDown = (
    currentRequest: SignalRequest,
    entity: string,
    childLevel: DrillDownLevel
  ): SignalRequest => {
    return {
      ...currentRequest,
      level: childLevel,
      parent_value: entity,
    }
  }

  const drillUp = (
    currentRequest: SignalRequest,
    path: Array<{ level: DrillDownLevel; value: string }>
  ): SignalRequest => {
    if (path.length <= 1) {
      return {
        ...currentRequest,
        level: initialLevel,
        parent_value: null,
      }
    }

    const parentPath = path.slice(0, -1)
    const parent = parentPath[parentPath.length - 1]

    return {
      ...currentRequest,
      level: parent.level,
      parent_value: parentPath.length > 1 ? parentPath[parentPath.length - 2].value : null,
    }
  }

  const goToLevel = (
    currentRequest: SignalRequest,
    path: Array<{ level: DrillDownLevel; value: string }>,
    targetIndex: number
  ): SignalRequest => {
    if (targetIndex < 0) {
      return {
        ...currentRequest,
        level: initialLevel,
        parent_value: null,
      }
    }

    const targetPath = path.slice(0, targetIndex + 1)
    const target = targetPath[targetPath.length - 1]

    return {
      ...currentRequest,
      level: getChildLevel(target.level),
      parent_value: target.value,
    }
  }

  return { drillDown, drillUp, goToLevel }
}

function getChildLevel(level: DrillDownLevel): DrillDownLevel {
  const hierarchy: Record<DrillDownLevel, DrillDownLevel> = {
    manufacturer: 'brand',
    brand: 'generic',
    generic: 'model',
    model: 'model', // No child
  }
  return hierarchy[level]
}
