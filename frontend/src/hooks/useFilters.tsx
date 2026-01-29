/**
 * Legacy Filter Hook - Compatibility Layer
 *
 * This file maintains backward compatibility with components that still
 * use the old useFilters hook. All functionality is now provided by
 * useAdvancedFilters.
 *
 * @deprecated Use useAdvancedFilters instead for new code
 */

import { ReactNode } from 'react'
import {
  AdvancedFilterProvider,
  useFiltersCompat,
} from './useAdvancedFilters'

// Re-export the old Filters interface for backward compatibility
export interface Filters {
  manufacturers: string[]
  productCodes: string[]
  eventTypes: string[]
  dateFrom: string
  dateTo: string
  searchText: string
}

/**
 * @deprecated Use AdvancedFilterProvider instead
 */
export function FilterProvider({ children }: { children: ReactNode }) {
  return <AdvancedFilterProvider>{children}</AdvancedFilterProvider>
}

/**
 * @deprecated Use useAdvancedFilters instead
 *
 * Provides the same interface as the old useFilters hook for backward
 * compatibility with existing components.
 */
export function useFilters() {
  return useFiltersCompat()
}
