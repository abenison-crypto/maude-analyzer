import { useState, useMemo } from 'react'
import { AlertTriangle, TrendingUp, Calendar, Settings, Filter } from 'lucide-react'
import { useAdvancedSignals } from '../../hooks/useAdvancedSignals'
import { useAdvancedFilters } from '../../hooks/useAdvancedFilters'
import SignalMethodSelector from './SignalMethodSelector'
import TimePeriodConfigurator from './TimePeriodConfigurator'
import SignalBreadcrumb from './SignalBreadcrumb'
import SignalTreeTable from './SignalTreeTable'
import SignalHelpPanel from './SignalHelpPanel'
import DataLagWarning from './DataLagWarning'
import type {
  SignalMethod,
  DrillDownLevel,
  TimeComparisonConfig,
  TimeComparisonMode,
} from '../../types/signals'

interface BreadcrumbItem {
  level: DrillDownLevel
  value: string
}

const getNextLevel = (level: DrillDownLevel): DrillDownLevel => {
  const hierarchy: Record<DrillDownLevel, DrillDownLevel> = {
    manufacturer: 'brand',
    brand: 'generic',
    generic: 'model',
    model: 'model',
  }
  return hierarchy[level]
}

export default function AdvancedSignalsPanel() {
  // Global filters
  const { filters, hasActiveFilters, activeFilterCount } = useAdvancedFilters()

  // State
  const [methods, setMethods] = useState<SignalMethod[]>(['zscore'])
  const [timeConfig, setTimeConfig] = useState<TimeComparisonConfig>({
    mode: 'lookback' as TimeComparisonMode,
    lookback_months: 12,
  })
  const [level, setLevel] = useState<DrillDownLevel>('manufacturer')
  const [parentValue, setParentValue] = useState<string | null>(null)
  const [path, setPath] = useState<BreadcrumbItem[]>([])
  const [minEvents, setMinEvents] = useState(10)
  const [showSettings, setShowSettings] = useState(false)
  const [showHelp, setShowHelp] = useState(false)

  // Combine product codes from both filter fields
  const productCodes = useMemo(() => {
    const codes = [
      ...filters.productCodes,
      ...filters.deviceProductCodes,
    ]
    return codes.length > 0 ? codes : undefined
  }, [filters.productCodes, filters.deviceProductCodes])

  // Event types from filters
  const eventTypes = useMemo(() => {
    return filters.eventTypes.length > 0 ? filters.eventTypes : undefined
  }, [filters.eventTypes])

  // Query
  const { data, isLoading, error } = useAdvancedSignals({
    methods,
    timeConfig,
    level,
    parentValue,
    productCodes,
    eventTypes,
    minEvents,
    limit: 20,
    enabled: true,
  })

  // Handlers
  const handleDrillDown = (entity: string, childLevel: DrillDownLevel) => {
    setPath([...path, { level, value: entity }])
    setLevel(childLevel)
    setParentValue(entity)
  }

  const handleNavigate = (index: number) => {
    if (index < 0) {
      // Go to root
      setPath([])
      setLevel('manufacturer')
      setParentValue(null)
    } else {
      // Navigate to specific level
      const newPath = path.slice(0, index + 1)
      const target = newPath[newPath.length - 1]
      setPath(newPath)
      setLevel(getNextLevel(target.level))
      setParentValue(target.value)
    }
  }

  // Summary stats
  const summary = useMemo(() => {
    if (!data) {
      return { high: 0, elevated: 0, total: 0 }
    }
    return {
      high: data.high_signal_count,
      elevated: data.elevated_signal_count,
      total: data.total_entities_analyzed,
    }
  }, [data])

  // Time period display
  const timePeriodLabel = useMemo(() => {
    if (!data?.time_info) return ''
    const { analysis_start, analysis_end, comparison_start, comparison_end } = data.time_info
    if (comparison_start && comparison_end) {
      return `${analysis_start} to ${analysis_end} vs ${comparison_start} to ${comparison_end}`
    }
    return `${analysis_start} to ${analysis_end}`
  }, [data])

  return (
    <div className="space-y-6">
      {/* Header with Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-red-50 rounded-lg p-4 border border-red-200">
          <div className="flex items-center gap-2">
            <AlertTriangle className="w-5 h-5 text-red-600" />
            <span className="font-medium text-red-800">High Signals</span>
          </div>
          <p className="text-2xl font-bold text-red-900 mt-2">{summary.high}</p>
          <p className="text-sm text-red-600">Immediate attention</p>
        </div>
        <div className="bg-yellow-50 rounded-lg p-4 border border-yellow-200">
          <div className="flex items-center gap-2">
            <TrendingUp className="w-5 h-5 text-yellow-600" />
            <span className="font-medium text-yellow-800">Elevated Signals</span>
          </div>
          <p className="text-2xl font-bold text-yellow-900 mt-2">{summary.elevated}</p>
          <p className="text-sm text-yellow-600">Worth monitoring</p>
        </div>
        <div className="bg-blue-50 rounded-lg p-4 border border-blue-200">
          <div className="flex items-center gap-2">
            <Calendar className="w-5 h-5 text-blue-600" />
            <span className="font-medium text-blue-800">Analysis Period</span>
          </div>
          <p className="text-lg font-bold text-blue-900 mt-2">
            {timeConfig.lookback_months || 12} months
          </p>
          <p className="text-sm text-blue-600 truncate" title={timePeriodLabel}>
            {data?.time_info?.mode || 'Lookback'}
          </p>
        </div>
        <div className={`rounded-lg p-4 border ${hasActiveFilters ? 'bg-purple-50 border-purple-200' : 'bg-gray-50 border-gray-200'}`}>
          <div className="flex items-center gap-2">
            {hasActiveFilters && <Filter className="w-4 h-4 text-purple-600" />}
            <span className={`font-medium ${hasActiveFilters ? 'text-purple-800' : 'text-gray-800'}`}>
              {hasActiveFilters ? 'Filtered Analysis' : 'Entities Analyzed'}
            </span>
          </div>
          <p className={`text-2xl font-bold mt-2 ${hasActiveFilters ? 'text-purple-900' : 'text-gray-900'}`}>
            {summary.total}
          </p>
          <p className={`text-sm ${hasActiveFilters ? 'text-purple-600' : 'text-gray-600'}`}>
            {hasActiveFilters
              ? `${activeFilterCount} filter${activeFilterCount !== 1 ? 's' : ''} applied`
              : `${methods.length} method${methods.length !== 1 ? 's' : ''}`}
          </p>
        </div>
      </div>

      {/* Help Panel */}
      <SignalHelpPanel isOpen={showHelp} onToggle={() => setShowHelp(!showHelp)} />

      {/* Data Lag Warning */}
      <DataLagWarning
        dataCompleteness={data?.data_completeness}
        analysisEndDate={data?.time_info?.analysis_end?.toString()}
      />

      {/* Settings Panel */}
      <div className="bg-white rounded-lg shadow">
        <button
          onClick={() => setShowSettings(!showSettings)}
          className="w-full flex items-center justify-between p-4 hover:bg-gray-50"
        >
          <div className="flex items-center gap-2">
            <Settings className="w-5 h-5 text-gray-500" />
            <span className="font-medium text-gray-900">Detection Settings</span>
          </div>
          <span className="text-sm text-gray-500">
            {showSettings ? 'Hide' : 'Show'} configuration
          </span>
        </button>

        {showSettings && (
          <div className="border-t p-4 space-y-6">
            {/* Detection Methods */}
            <div>
              <h3 className="text-sm font-semibold text-gray-900 mb-3">Detection Methods</h3>
              <SignalMethodSelector selectedMethods={methods} onChange={setMethods} />
            </div>

            {/* Time Configuration */}
            <div>
              <h3 className="text-sm font-semibold text-gray-900 mb-3">Time Period</h3>
              <TimePeriodConfigurator
                config={timeConfig}
                onChange={setTimeConfig}
                selectedMethods={methods}
              />
            </div>

            {/* Advanced Options */}
            <div className="flex flex-wrap gap-4 pt-4 border-t">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Minimum Events
                </label>
                <select
                  value={minEvents}
                  onChange={(e) => setMinEvents(parseInt(e.target.value, 10))}
                  className="px-3 py-2 border border-gray-300 rounded-md text-sm"
                >
                  <option value={5}>5+</option>
                  <option value={10}>10+</option>
                  <option value={25}>25+</option>
                  <option value={50}>50+</option>
                  <option value={100}>100+</option>
                </select>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Breadcrumb Navigation */}
      <div className="bg-white rounded-lg shadow px-4 py-3">
        <SignalBreadcrumb path={path} currentLevel={level} onNavigate={handleNavigate} />
      </div>

      {/* Error State */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 text-red-800">
          <p className="font-medium">Error loading signals</p>
          <p className="text-sm mt-1">{(error as Error).message}</p>
        </div>
      )}

      {/* Data Note */}
      {data?.data_note && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-blue-800 text-sm">
          {data.data_note}
        </div>
      )}

      {/* Results Table */}
      <SignalTreeTable
        signals={data?.signals || []}
        methods={methods}
        isLoading={isLoading}
        onDrillDown={handleDrillDown}
      />

    </div>
  )
}
