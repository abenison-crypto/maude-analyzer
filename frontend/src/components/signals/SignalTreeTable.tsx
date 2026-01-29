import { ChevronRight, AlertTriangle, TrendingUp, Minus, Info } from 'lucide-react'
import type { SignalResult, SignalMethod, DrillDownLevel } from '../../types/signals'

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

export default function SignalTreeTable({
  signals,
  methods,
  isLoading,
  onDrillDown,
}: SignalTreeTableProps) {
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
              {/* Dynamic method columns */}
              {methods.map((method) => (
                <th
                  key={method}
                  className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider"
                >
                  {METHOD_LABELS[method]}
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

              return (
                <tr key={signal.entity} className="hover:bg-gray-50">
                  {/* Signal Badge */}
                  <td className="px-4 py-3 whitespace-nowrap">
                    <span
                      className={`inline-flex items-center gap-1 px-2 py-1 text-xs font-semibold rounded-full ${style.bg} ${style.text}`}
                    >
                      <Icon className="w-3 h-3" />
                      {style.label}
                    </span>
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

                  {/* Method Results */}
                  {methods.map((method) => {
                    const result = signal.method_results.find((r) => r.method === method)
                    return (
                      <td key={method} className="px-4 py-3 text-right">
                        {result?.value != null ? (
                          <div className="inline-flex flex-col items-end">
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
                            {(result.lower_ci != null || result.upper_ci != null) && (
                              <span className="text-xs text-gray-400">
                                [{result.lower_ci?.toFixed(1)}-{result.upper_ci?.toFixed(1)}]
                              </span>
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
                        onClick={() => onDrillDown(signal.entity, signal.child_level!)}
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
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
