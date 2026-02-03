import { Info } from 'lucide-react'
import { type TimeComparisonConfig, type TimeComparisonMode, type SignalMethod, TIME_MODE_LABELS } from '../../types/signals'

interface TimePeriodConfiguratorProps {
  config: TimeComparisonConfig
  onChange: (config: TimeComparisonConfig) => void
  selectedMethods?: SignalMethod[]
}

const LOOKBACK_OPTIONS = [6, 12, 24, 36, 48, 60]
const ROLLING_WINDOW_OPTIONS = [3, 6, 12]
const CURRENT_YEAR = new Date().getFullYear()
const YEARS = Array.from({ length: 10 }, (_, i) => CURRENT_YEAR - i)

// Smart defaults based on signal detection method
const METHOD_DEFAULTS: Record<SignalMethod, { lookback: number; rollingWindow?: number; recommendation: string }> = {
  zscore: { lookback: 24, recommendation: 'Z-Score works best with 24+ months for a stable baseline' },
  prr: { lookback: 12, recommendation: 'PRR needs sufficient events; 12 months is usually adequate' },
  ror: { lookback: 12, recommendation: 'ROR needs sufficient events; 12 months is usually adequate' },
  ebgm: { lookback: 12, recommendation: 'EBGM can work with smaller datasets due to Bayesian shrinkage' },
  cusum: { lookback: 24, recommendation: 'CUSUM detects drift; longer periods (24+ months) improve sensitivity' },
  yoy: { lookback: 12, recommendation: 'YoY compares full years; ensure both periods are complete' },
  pop: { lookback: 12, recommendation: 'Period comparisons work best with equal-length periods' },
  rolling: { lookback: 12, rollingWindow: 3, recommendation: 'Rolling uses a 3-month window by default; 12 months provides context' },
}

export default function TimePeriodConfigurator({ config, onChange, selectedMethods = [] }: TimePeriodConfiguratorProps) {
  const modes: TimeComparisonMode[] = ['lookback', 'custom', 'yoy', 'rolling']

  // Get the recommended lookback based on selected methods
  const getRecommendedLookback = (): number => {
    if (selectedMethods.length === 0) return 12
    const lookbacks = selectedMethods.map((m) => METHOD_DEFAULTS[m]?.lookback || 12)
    return Math.max(...lookbacks)
  }

  // Get recommendation text for display
  const getRecommendation = (): string | null => {
    if (selectedMethods.length === 0) return null
    // Show recommendation for the method requiring the longest lookback
    const maxMethod = selectedMethods.reduce((max, m) =>
      (METHOD_DEFAULTS[m]?.lookback || 12) > (METHOD_DEFAULTS[max]?.lookback || 12) ? m : max
    )
    return METHOD_DEFAULTS[maxMethod]?.recommendation || null
  }

  const handleModeChange = (mode: TimeComparisonMode) => {
    const newConfig: TimeComparisonConfig = { ...config, mode }
    const recommended = getRecommendedLookback()

    // Set default values for the mode using smart defaults
    if (mode === 'lookback') {
      newConfig.lookback_months = config.lookback_months || recommended
    } else if (mode === 'yoy') {
      newConfig.current_year = config.current_year || CURRENT_YEAR - 1
      newConfig.comparison_year = config.comparison_year || CURRENT_YEAR - 2
    } else if (mode === 'rolling') {
      newConfig.rolling_window_months = config.rolling_window_months || 3
      newConfig.lookback_months = config.lookback_months || recommended
    }

    onChange(newConfig)
  }

  // Apply smart defaults when methods change
  const applySmartDefaults = () => {
    const recommended = getRecommendedLookback()
    onChange({
      ...config,
      lookback_months: recommended,
    })
  }

  const recommendation = getRecommendation()
  const recommendedLookback = getRecommendedLookback()
  const isUsingRecommended = config.lookback_months === recommendedLookback

  return (
    <div className="space-y-4">
      {/* Smart defaults recommendation */}
      {recommendation && !isUsingRecommended && (
        <div className="flex items-start gap-2 p-2 bg-blue-50 border border-blue-100 rounded text-xs">
          <Info className="w-4 h-4 text-blue-500 flex-shrink-0 mt-0.5" />
          <div className="flex-1">
            <p className="text-blue-800">{recommendation}</p>
            <button
              onClick={applySmartDefaults}
              className="mt-1 text-blue-600 hover:text-blue-800 font-medium"
            >
              Apply recommended ({recommendedLookback} months)
            </button>
          </div>
        </div>
      )}

      {/* Mode Tabs */}
      <div className="flex border-b border-gray-200">
        {modes.map((mode) => (
          <button
            key={mode}
            onClick={() => handleModeChange(mode)}
            className={`px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors ${
              config.mode === mode
                ? 'border-blue-500 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            {TIME_MODE_LABELS[mode]}
          </button>
        ))}
      </div>

      {/* Mode-specific Controls */}
      <div className="pt-2">
        {config.mode === 'lookback' && (
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Analyze events from the last:
              </label>
              <div className="flex flex-wrap gap-2">
                {LOOKBACK_OPTIONS.map((months) => (
                  <button
                    key={months}
                    onClick={() => onChange({ ...config, lookback_months: months })}
                    className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                      config.lookback_months === months
                        ? 'bg-blue-100 text-blue-800 border-2 border-blue-300'
                        : 'bg-gray-100 text-gray-700 hover:bg-gray-200 border-2 border-transparent'
                    }`}
                  >
                    {months} months
                  </button>
                ))}
              </div>
            </div>

            {/* Comparison month selector for Z-Score method */}
            {selectedMethods.includes('zscore') && (
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Compare month (for Z-Score):
                </label>
                <div className="flex items-center gap-3">
                  <input
                    type="month"
                    value={config.comparison_month || ''}
                    onChange={(e) =>
                      onChange({ ...config, comparison_month: e.target.value || undefined })
                    }
                    className="px-3 py-2 border border-gray-300 rounded-md text-sm"
                  />
                  <span className="text-xs text-gray-500">
                    {config.comparison_month
                      ? 'Z-score will be calculated for this month'
                      : 'Leave empty to use the latest month'}
                  </span>
                  {config.comparison_month && (
                    <button
                      onClick={() => onChange({ ...config, comparison_month: undefined })}
                      className="text-xs text-gray-500 hover:text-gray-700 underline"
                    >
                      Clear
                    </button>
                  )}
                </div>
              </div>
            )}
          </div>
        )}

        {config.mode === 'custom' && (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="space-y-2">
              <label className="block text-sm font-medium text-gray-700">Period A (Current)</label>
              <div className="flex gap-2">
                <input
                  type="date"
                  value={config.period_a?.start_date || ''}
                  onChange={(e) =>
                    onChange({
                      ...config,
                      period_a: {
                        start_date: e.target.value,
                        end_date: config.period_a?.end_date || '',
                      },
                    })
                  }
                  className="flex-1 px-3 py-2 border border-gray-300 rounded-md text-sm"
                />
                <span className="text-gray-500 self-center">to</span>
                <input
                  type="date"
                  value={config.period_a?.end_date || ''}
                  onChange={(e) =>
                    onChange({
                      ...config,
                      period_a: {
                        start_date: config.period_a?.start_date || '',
                        end_date: e.target.value,
                      },
                    })
                  }
                  className="flex-1 px-3 py-2 border border-gray-300 rounded-md text-sm"
                />
              </div>
            </div>
            <div className="space-y-2">
              <label className="block text-sm font-medium text-gray-700">Period B (Comparison)</label>
              <div className="flex gap-2">
                <input
                  type="date"
                  value={config.period_b?.start_date || ''}
                  onChange={(e) =>
                    onChange({
                      ...config,
                      period_b: {
                        start_date: e.target.value,
                        end_date: config.period_b?.end_date || '',
                      },
                    })
                  }
                  className="flex-1 px-3 py-2 border border-gray-300 rounded-md text-sm"
                />
                <span className="text-gray-500 self-center">to</span>
                <input
                  type="date"
                  value={config.period_b?.end_date || ''}
                  onChange={(e) =>
                    onChange({
                      ...config,
                      period_b: {
                        start_date: config.period_b?.start_date || '',
                        end_date: e.target.value,
                      },
                    })
                  }
                  className="flex-1 px-3 py-2 border border-gray-300 rounded-md text-sm"
                />
              </div>
            </div>
          </div>
        )}

        {config.mode === 'yoy' && (
          <div className="flex flex-wrap gap-4 items-end">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Current Year</label>
              <select
                value={config.current_year || CURRENT_YEAR - 1}
                onChange={(e) =>
                  onChange({ ...config, current_year: parseInt(e.target.value, 10) })
                }
                className="px-3 py-2 border border-gray-300 rounded-md text-sm"
              >
                {YEARS.map((year) => (
                  <option key={year} value={year}>
                    {year}
                  </option>
                ))}
              </select>
            </div>
            <span className="text-gray-500 pb-2">vs</span>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Comparison Year</label>
              <select
                value={config.comparison_year || CURRENT_YEAR - 2}
                onChange={(e) =>
                  onChange({ ...config, comparison_year: parseInt(e.target.value, 10) })
                }
                className="px-3 py-2 border border-gray-300 rounded-md text-sm"
              >
                {YEARS.map((year) => (
                  <option key={year} value={year}>
                    {year}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Quarter (optional)</label>
              <select
                value={config.quarter || ''}
                onChange={(e) =>
                  onChange({
                    ...config,
                    quarter: e.target.value ? (parseInt(e.target.value, 10) as 1 | 2 | 3 | 4) : undefined,
                  })
                }
                className="px-3 py-2 border border-gray-300 rounded-md text-sm"
              >
                <option value="">Full Year</option>
                <option value="1">Q1 (Jan-Mar)</option>
                <option value="2">Q2 (Apr-Jun)</option>
                <option value="3">Q3 (Jul-Sep)</option>
                <option value="4">Q4 (Oct-Dec)</option>
              </select>
            </div>
          </div>
        )}

        {config.mode === 'rolling' && (
          <div className="flex flex-wrap gap-4 items-end">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Rolling Window</label>
              <div className="flex gap-2">
                {ROLLING_WINDOW_OPTIONS.map((months) => (
                  <button
                    key={months}
                    onClick={() => onChange({ ...config, rolling_window_months: months })}
                    className={`px-4 py-2 rounded-md text-sm font-medium transition-colors ${
                      config.rolling_window_months === months
                        ? 'bg-blue-100 text-blue-800 border-2 border-blue-300'
                        : 'bg-gray-100 text-gray-700 hover:bg-gray-200 border-2 border-transparent'
                    }`}
                  >
                    {months}-month
                  </button>
                ))}
              </div>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Analysis Period</label>
              <select
                value={config.lookback_months || 12}
                onChange={(e) =>
                  onChange({ ...config, lookback_months: parseInt(e.target.value, 10) })
                }
                className="px-3 py-2 border border-gray-300 rounded-md text-sm"
              >
                {LOOKBACK_OPTIONS.map((months) => (
                  <option key={months} value={months}>
                    Last {months} months
                  </option>
                ))}
              </select>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
