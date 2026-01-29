/**
 * Advanced Filter Hook
 *
 * Extended filter context with support for:
 * - Device filters (brand names, generic names, model numbers, etc.)
 * - Filter presets (save/load filter combinations)
 * - Filter visibility configuration
 * - Filter history for undo/redo
 * - URL persistence for all filters
 */

import {
  createContext,
  useContext,
  useState,
  useCallback,
  useEffect,
  ReactNode,
  useMemo,
} from 'react'
import { useSearchParams } from 'react-router-dom'
import {
  FilterValues,
  FilterVisibilityConfig,
  FilterPreset,
  FilterHistoryEntry,
  defaultFilterValues,
  defaultVisibilityConfig,
  countActiveFilters,
  hasActiveFilters as checkHasActiveFilters,
  filtersToURLParams,
  urlParamsToFilters,
  STORAGE_KEYS,
  MAX_HISTORY_ENTRIES,
  builtInPresets,
  FilterCategory,
} from '../types/filters'
import { api } from '../api/client'

// =============================================================================
// CONTEXT DEFINITION
// =============================================================================

interface AdvancedFilterContextValue {
  // Current filter values
  filters: FilterValues

  // Filter setters
  setFilter: <K extends keyof FilterValues>(key: K, value: FilterValues[K]) => void
  setFilters: (updates: Partial<FilterValues>) => void
  clearFilters: () => void

  // Computed properties
  hasActiveFilters: boolean
  activeFilterCount: number

  // Visibility config
  visibilityConfig: FilterVisibilityConfig
  setVisibilityConfig: (config: FilterVisibilityConfig) => void
  toggleFilterVisibility: (filterId: keyof FilterValues) => void
  toggleSection: (section: FilterCategory) => void

  // Presets
  presets: FilterPreset[]
  loadingPresets: boolean
  savePreset: (name: string, description?: string) => Promise<FilterPreset>
  loadPreset: (presetId: string) => void
  deletePreset: (presetId: string) => Promise<void>
  refreshPresets: () => Promise<void>

  // History
  history: FilterHistoryEntry[]
  historyIndex: number
  undo: () => void
  redo: () => void
  canUndo: boolean
  canRedo: boolean
}

const AdvancedFilterContext = createContext<AdvancedFilterContextValue | undefined>(undefined)

// =============================================================================
// STORAGE HELPERS
// =============================================================================

function loadFromStorage<T>(key: string, defaultValue: T): T {
  try {
    const stored = localStorage.getItem(key)
    if (stored) {
      const parsed = JSON.parse(stored)
      // Handle Set conversion for visibilityConfig
      if (key === STORAGE_KEYS.VISIBILITY_CONFIG && parsed.visibleFilters) {
        parsed.visibleFilters = new Set(parsed.visibleFilters)
      }
      return parsed
    }
  } catch (e) {
    console.warn(`Failed to load ${key} from localStorage:`, e)
  }
  return defaultValue
}

function saveToStorage<T>(key: string, value: T): void {
  try {
    // Handle Set serialization
    let toStore = value
    if (key === STORAGE_KEYS.VISIBILITY_CONFIG && value && typeof value === 'object') {
      const config = value as unknown as FilterVisibilityConfig
      toStore = {
        ...config,
        visibleFilters: Array.from(config.visibleFilters),
      } as unknown as T
    }
    localStorage.setItem(key, JSON.stringify(toStore))
  } catch (e) {
    console.warn(`Failed to save ${key} to localStorage:`, e)
  }
}

// =============================================================================
// PROVIDER COMPONENT
// =============================================================================

export function AdvancedFilterProvider({ children }: { children: ReactNode }) {
  const [searchParams, setSearchParams] = useSearchParams()

  // Initialize filters from URL params
  const getInitialFilters = (): FilterValues => {
    return urlParamsToFilters(searchParams)
  }

  const [filters, setFiltersState] = useState<FilterValues>(getInitialFilters)

  // Visibility configuration (persisted to localStorage)
  const [visibilityConfig, setVisibilityConfigState] = useState<FilterVisibilityConfig>(() =>
    loadFromStorage(STORAGE_KEYS.VISIBILITY_CONFIG, defaultVisibilityConfig)
  )

  // Presets
  const [presets, setPresets] = useState<FilterPreset[]>(builtInPresets)
  const [loadingPresets, setLoadingPresets] = useState(false)

  // History for undo/redo
  const [history, setHistory] = useState<FilterHistoryEntry[]>([])
  const [historyIndex, setHistoryIndex] = useState(-1)

  // ==========================================================================
  // URL SYNC
  // ==========================================================================

  const updateURL = useCallback((newFilters: FilterValues) => {
    setSearchParams(filtersToURLParams(newFilters))
  }, [setSearchParams])

  // ==========================================================================
  // FILTER SETTERS
  // ==========================================================================

  const addToHistory = useCallback((newFilters: FilterValues) => {
    const entry: FilterHistoryEntry = {
      id: crypto.randomUUID(),
      filters: { ...newFilters },
      timestamp: new Date().toISOString(),
    }

    setHistory((prev) => {
      // Remove any "future" entries if we're not at the end
      const truncated = prev.slice(0, historyIndex + 1)
      const updated = [...truncated, entry]
      // Limit history size
      return updated.slice(-MAX_HISTORY_ENTRIES)
    })
    setHistoryIndex((prev) => Math.min(prev + 1, MAX_HISTORY_ENTRIES - 1))
  }, [historyIndex])

  const setFilter = useCallback(<K extends keyof FilterValues>(
    key: K,
    value: FilterValues[K]
  ) => {
    const newFilters = { ...filters, [key]: value }
    setFiltersState(newFilters)
    updateURL(newFilters)
    addToHistory(newFilters)
  }, [filters, updateURL, addToHistory])

  const setFilters = useCallback((updates: Partial<FilterValues>) => {
    const newFilters = { ...filters, ...updates }
    setFiltersState(newFilters)
    updateURL(newFilters)
    addToHistory(newFilters)
  }, [filters, updateURL, addToHistory])

  const clearFilters = useCallback(() => {
    setFiltersState(defaultFilterValues)
    setSearchParams(new URLSearchParams())
    addToHistory(defaultFilterValues)
  }, [setSearchParams, addToHistory])

  // ==========================================================================
  // COMPUTED PROPERTIES
  // ==========================================================================

  const hasActiveFilters = useMemo(() => checkHasActiveFilters(filters), [filters])
  const activeFilterCount = useMemo(() => countActiveFilters(filters), [filters])

  // ==========================================================================
  // VISIBILITY CONFIG
  // ==========================================================================

  const setVisibilityConfig = useCallback((config: FilterVisibilityConfig) => {
    setVisibilityConfigState(config)
    saveToStorage(STORAGE_KEYS.VISIBILITY_CONFIG, config)
  }, [])

  const toggleFilterVisibility = useCallback((filterId: keyof FilterValues) => {
    setVisibilityConfigState((prev) => {
      const newVisible = new Set(prev.visibleFilters)
      if (newVisible.has(filterId)) {
        newVisible.delete(filterId)
      } else {
        newVisible.add(filterId)
      }
      const newConfig = { ...prev, visibleFilters: newVisible }
      saveToStorage(STORAGE_KEYS.VISIBILITY_CONFIG, newConfig)
      return newConfig
    })
  }, [])

  const toggleSection = useCallback((section: FilterCategory) => {
    setVisibilityConfigState((prev) => {
      const newConfig = {
        ...prev,
        collapsedSections: {
          ...prev.collapsedSections,
          [section]: !prev.collapsedSections[section],
        },
      }
      saveToStorage(STORAGE_KEYS.VISIBILITY_CONFIG, newConfig)
      return newConfig
    })
  }, [])

  // ==========================================================================
  // PRESETS
  // ==========================================================================

  const refreshPresets = useCallback(async () => {
    setLoadingPresets(true)
    try {
      const fetchedPresets = await api.getPresets()
      setPresets(fetchedPresets)
    } catch (error) {
      console.error('Failed to fetch presets:', error)
      // Fall back to built-in presets
      setPresets(builtInPresets)
    } finally {
      setLoadingPresets(false)
    }
  }, [])

  // Load presets on mount
  useEffect(() => {
    refreshPresets()
  }, [refreshPresets])

  const savePreset = useCallback(async (name: string, description?: string): Promise<FilterPreset> => {
    const preset = await api.createPreset(name, description, filters)
    await refreshPresets()
    return preset
  }, [filters, refreshPresets])

  const loadPreset = useCallback((presetId: string) => {
    const preset = presets.find((p) => p.id === presetId)
    if (preset) {
      const newFilters = { ...defaultFilterValues, ...preset.filters }
      setFiltersState(newFilters)
      updateURL(newFilters)
      addToHistory(newFilters)
    }
  }, [presets, updateURL, addToHistory])

  const deletePreset = useCallback(async (presetId: string): Promise<void> => {
    await api.deletePreset(presetId)
    await refreshPresets()
  }, [refreshPresets])

  // ==========================================================================
  // HISTORY (UNDO/REDO)
  // ==========================================================================

  const canUndo = historyIndex > 0
  const canRedo = historyIndex < history.length - 1

  const undo = useCallback(() => {
    if (canUndo) {
      const newIndex = historyIndex - 1
      const entry = history[newIndex]
      setHistoryIndex(newIndex)
      setFiltersState(entry.filters)
      updateURL(entry.filters)
    }
  }, [canUndo, historyIndex, history, updateURL])

  const redo = useCallback(() => {
    if (canRedo) {
      const newIndex = historyIndex + 1
      const entry = history[newIndex]
      setHistoryIndex(newIndex)
      setFiltersState(entry.filters)
      updateURL(entry.filters)
    }
  }, [canRedo, historyIndex, history, updateURL])

  // ==========================================================================
  // CONTEXT VALUE
  // ==========================================================================

  const value: AdvancedFilterContextValue = {
    filters,
    setFilter,
    setFilters,
    clearFilters,
    hasActiveFilters,
    activeFilterCount,
    visibilityConfig,
    setVisibilityConfig,
    toggleFilterVisibility,
    toggleSection,
    presets,
    loadingPresets,
    savePreset,
    loadPreset,
    deletePreset,
    refreshPresets,
    history,
    historyIndex,
    undo,
    redo,
    canUndo,
    canRedo,
  }

  return (
    <AdvancedFilterContext.Provider value={value}>
      {children}
    </AdvancedFilterContext.Provider>
  )
}

// =============================================================================
// HOOK
// =============================================================================

export function useAdvancedFilters() {
  const context = useContext(AdvancedFilterContext)
  if (!context) {
    throw new Error('useAdvancedFilters must be used within an AdvancedFilterProvider')
  }
  return context
}

// =============================================================================
// COMPATIBILITY HOOK
// =============================================================================

/**
 * Compatibility hook that provides the same interface as the old useFilters
 * for components that haven't been updated yet.
 */
export function useFiltersCompat() {
  const ctx = useAdvancedFilters()

  return {
    filters: ctx.filters,
    setManufacturers: (manufacturers: string[]) => ctx.setFilter('manufacturers', manufacturers),
    setProductCodes: (productCodes: string[]) => ctx.setFilter('productCodes', productCodes),
    setEventTypes: (eventTypes: string[]) => ctx.setFilter('eventTypes', eventTypes),
    setDateRange: (dateFrom: string, dateTo: string) => ctx.setFilters({ dateFrom, dateTo }),
    setSearchText: (searchText: string) => ctx.setFilter('searchText', searchText),
    clearFilters: ctx.clearFilters,
    hasActiveFilters: ctx.hasActiveFilters,
  }
}
