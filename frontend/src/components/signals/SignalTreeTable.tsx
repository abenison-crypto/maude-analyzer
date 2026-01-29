import { useState } from 'react'
import { ChevronRight, ChevronDown, AlertTriangle, TrendingUp, Minus, Info } from 'lucide-react'
import type {
  SignalResult,
  SignalMethod,
  DrillDownLevel,
  MethodResult,
  SignalStrength,
  ZScoreDetails,
  PRRDetails,
  EBGMDetails,
  CUSUMDetails,
  YoYDetails,
  RollingDetails,
} from '../../types/signals'
import { MethodInfoIcon } from './MethodInfoTooltip'
import { ThresholdIndicatorCompact } from './ThresholdIndicator'
import SignalRowDetail from './SignalRowDetail'

interface SignalTreeTableProps {
  signals: SignalResult[]
  methods: SignalMethod[]
  isLoading: boolean
  onDrillDown: (entity: string, childLevel: DrillDownLevel) => void
}

const SIGNAL_STYLES = {
  high: {
    bg: 'bg-red-100',
    text: 'text-red-800',
    border: 'border-red-200',
    icon: AlertTriangle,
    label: 'High',
  },
  elevated: {
    bg: 'bg-yellow-100',
    text: 'text-yellow-800',
    border: 'border-yellow-200',
    icon: TrendingUp,
    label: 'Elevated',
  },
  normal: {
    bg: 'bg-gray-100',
    text: 'text-gray-800',
    border: 'border-gray-200',
    icon: Minus,
    label: 'Normal',
  },
}

const METHOD_LABELS: Record<SignalMethod, string> = {
  zscore: 'Z-Score',
  prr: 'PRR',
  ror: 'ROR',
  ebgm: 'EBGM',
  cusum: 'CUSUM',
  yoy: 'YoY %',
  pop: 'PoP %',
  rolling: 'Rolling',
}

/**
 * Get a human-readable explanation of why the signal was triggered
 */
function getDetailExplanation(result: MethodResult): string | null {
  if (!result.details || result.value === null) return null

  const details = result.details

  switch (result.method) {
    case 'zscore': {
      const d = details as ZScoreDetails
      return `${d.latest_month} this month (avg: ${Math.round(d.avg_monthly)})`
    }
    case 'prr': {
      const d = details as PRRDetails
      return `${d.a} deaths vs ${d.c} others`
    }
    case 'ror': {
      const d = details as PRRDetails
      if (d.b > 0) {
        const ratio = ((d.a / d.b) / (d.c / d.d)).toFixed(1)
        return `Odds ${ratio}x higher`
      }
      return null
    }
    case 'ebgm': {
      const d = details as EBGMDetails
      return `Obs: ${d.observed}, Exp: ${Math.round(d.expected)}`
    }
    case 'cusum': {
      const d = details as CUSUMDetails
      const pctOver = result.value > d.control_limit
        ? `+${Math.round(((result.value - d.control_limit) / d.control_limit) * 100)}% over`
        : ''
      return `Limit: ${d.control_limit} ${pctOver}`
    }
    case 'yoy':
    case 'pop': {
      const d = details as YoYDetails
      if (d.current_period && d.comparison_period) {
        return `${d.current_period} vs ${d.comparison_period} prior`
      }
      return null
    }
    case 'rolling': {
      const d = details as RollingDetails
      return `${d.latest} vs avg ${Math.round(d.rolling_avg)}`
    }
    default:
      return null
  }
}

/**
 * Get the trigger method for the overall signal classification
 */
function getTriggerMethod(methodResults: MethodResult[], signalType: SignalStrength): MethodResult | null {
  if (signalType === 'normal') return null

  // Find the method that triggered the highest signal
  const highTrigger = methodResults.find(
    (r) => r.is_signal && r.signal_strength === 'high'
  )
  if (highTrigger) return highTrigger

  const elevatedTrigger = methodResults.find(
    (r) => r.is_signal && r.signal_strength === 'elevated'
  )
  return elevatedTrigger || null
}

export default function SignalTreeTable({
  signals,
  methods,
  isLoading,
  onDrillDown,
}: SignalTreeTableProps) {
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set())

  const toggleRow = (entity: string) => {
    setExpandedRows((prev) => {
      const next = new Set(prev)
      if (next.has(entity)) {
        next.delete(entity)
      } else {
        next.add(entity)
      }
      return next
    })
  }

  if (isLoading) {
    return (
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <div className="p-6 animate-pulse space-y-4">
          <div className="h-4 bg-gray-200 rounded w-1/4" />
          <div className="space-y-3">
            {Array.from({ length: 5 }).map((_, i) => (
              <div key={i} className="h-12 bg-gray-200 rounded" />
            ))}
          </div>
        </div>
      </div>
    )
  }

  if (signals.length === 0) {
    return (
      <div className="bg-white rounded-lg shadow p-8 text-center text-gray-500">
        <Info className="w-12 h-12 mx-auto mb-4 text-gray-300" />
        <p>No signals detected with current parameters</p>
        <p className="text-sm mt-2">Try adjusting the time period or minimum events threshold</p>
      </div>
    )
  }

  return (
    <div className="bg-white rounded-lg shadow overflow-hidden">
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="w-8 px-2 py-3" />
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Signal
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Entity
              </th>
              <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                Events
              </th>
              <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                Deaths
              </th>
              {/* Dynamic method columns with info icons */}
              {methods.map((method) => (
                <th
                  key={method}
                  className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider"
                >
                  <div className="inline-flex items-center gap-1">
                    {METHOD_LABELS[method]}
                    <MethodInfoIcon method={method} />
                  </div>
                </th>
              ))}
              <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">
                Drill
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {signals.map((signal) => {
              const style = SIGNAL_STYLES[signal.signal_type]
              const Icon = style.icon
              const isExpanded = expandedRows.has(signal.entity)
              const triggerMethod = getTriggerMethod(signal.method_results, signal.signal_type)

              return (
                <>
                  <tr
                    key={signal.entity}
                    className={`hover:bg-gray-50 cursor-pointer ${isExpanded ? 'bg-blue-50' : ''}`}
                    onClick={() => toggleRow(signal.entity)}
                  >
                    {/* Expand/Collapse Toggle */}
                    <td className="px-2 py-3 text-center">
                      <button
                        onClick={(e) => {
                          e.stopPropagation()
                          toggleRow(signal.entity)
                        }}
                        className="p-1 text-gray-400 hover:text-gray-600"
                      >
                        {isExpanded ? (
                          <ChevronDown className="w-4 h-4" />
                        ) : (
                          <ChevronRight className="w-4 h-4" />
                        )}
                      </button>
                    </td>

                    {/* Signal Badge with trigger info */}
                    <td className="px-4 py-3 whitespace-nowrap">
                      <div className="group relative inline-block">
                        <span
                          className={`inline-flex items-center gap-1 px-2 py-1 text-xs font-semibold rounded-full ${style.bg} ${style.text}`}
                        >
                          <Icon className="w-3 h-3" />
                          {style.label}
                        </span>
                        {/* Trigger tooltip */}
                        {triggerMethod && (
                          <div className="absolute hidden group-hover:block z-10 w-48 p-2 bg-gray-900 text-white text-xs rounded shadow-lg -top-8 left-0">
                            Triggered by: {METHOD_LABELS[triggerMethod.method]} (
                            {triggerMethod.value?.toFixed(2)}
                            {triggerMethod.method === 'yoy' || triggerMethod.method === 'pop' ? '%' : ''})
                            <div className="absolute bottom-0 left-4 transform translate-y-1/2 rotate-45 w-2 h-2 bg-gray-900" />
                          </div>
                        )}
                      </div>
                    </td>

                    {/* Entity Name */}
                    <td className="px-4 py-3">
                      <div className="text-sm text-gray-900 max-w-xs truncate" title={signal.entity}>
                        {signal.entity}
                      </div>
                    </td>

                    {/* Events */}
                    <td className="px-4 py-3 text-right text-sm text-gray-600">
                      {signal.total_events.toLocaleString()}
                    </td>

                    {/* Deaths */}
                    <td className="px-4 py-3 text-right">
                      <span
                        className={`text-sm ${
                          signal.deaths > 0 ? 'text-red-600 font-medium' : 'text-gray-600'
                        }`}
                      >
                        {signal.deaths.toLocaleString()}
                      </span>
                    </td>

                    {/* Method Results with detail explanations */}
                    {methods.map((method) => {
                      const result = signal.method_results.find((r) => r.method === method)
                      const detailExplanation = result ? getDetailExplanation(result) : null

                      return (
                        <td key={method} className="px-4 py-3 text-right">
                          {result?.value != null ? (
                            <div className="inline-flex flex-col items-end gap-0.5">
                              {/* Main value */}
                              <span
                                className={`text-sm font-medium ${
                                  result.signal_strength === 'high'
                                    ? 'text-red-600'
                                    : result.signal_strength === 'elevated'
                                    ? 'text-yellow-600'
                                    : 'text-gray-600'
                                }`}
                              >
                                {method === 'yoy' || method === 'pop'
                                  ? `${result.value > 0 ? '+' : ''}${result.value}%`
                                  : result.value.toFixed(2)}
                              </span>

                              {/* Confidence interval */}
                              {(result.lower_ci != null || result.upper_ci != null) && (
                                <span className="text-xs text-gray-400">
                                  [{result.lower_ci?.toFixed(1)}
                                  {result.upper_ci != null ? `-${result.upper_ci.toFixed(1)}` : ''}]
                                </span>
                              )}

                              {/* Detail explanation */}
                              {detailExplanation && (
                                <span className="text-[10px] text-gray-400">
                                  {detailExplanation}
                                </span>
                              )}

                              {/* Mini threshold indicator */}
                              {result.is_signal && (
                                <ThresholdIndicatorCompact
                                  method={method}
                                  value={result.value}
                                  signalStrength={result.signal_strength}
                                />
                              )}
                            </div>
                          ) : (
                            <span className="text-sm text-gray-400">-</span>
                          )}
                        </td>
                      )
                    })}

                    {/* Drill Down Button */}
                    <td className="px-4 py-3 text-center">
                      {signal.has_children && signal.child_level ? (
                        <button
                          onClick={(e) => {
                            e.stopPropagation()
                            onDrillDown(signal.entity, signal.child_level!)
                          }}
                          className="inline-flex items-center gap-1 px-2 py-1 text-xs font-medium text-blue-600 hover:text-blue-800 hover:bg-blue-50 rounded"
                        >
                          <ChevronRight className="w-4 h-4" />
                          Drill
                        </button>
                      ) : (
                        <span className="text-gray-300">-</span>
                      )}
                    </td>
                  </tr>

                  {/* Expanded Detail Row */}
                  {isExpanded && (
                    <tr key={`${signal.entity}-detail`}>
                      <td colSpan={6 + methods.length} className="px-4 py-4 bg-gray-50">
                        <SignalRowDetail signal={signal} methods={methods} />
                      </td>
                    </tr>
                  )}
                </>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
