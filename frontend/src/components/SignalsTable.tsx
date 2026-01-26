import { AlertTriangle, TrendingUp, Minus } from 'lucide-react'

interface Signal {
  manufacturer: string
  avg_monthly: number
  std_monthly: number
  total_events: number
  total_deaths: number
  latest_month: number
  z_score: number
  signal_type: 'high' | 'elevated' | 'normal'
}

interface SignalsTableProps {
  signals: Signal[]
  isLoading: boolean
  lookbackMonths: number
}

const SIGNAL_STYLES = {
  high: {
    bg: 'bg-red-100',
    text: 'text-red-800',
    icon: AlertTriangle,
    label: 'High',
  },
  elevated: {
    bg: 'bg-yellow-100',
    text: 'text-yellow-800',
    icon: TrendingUp,
    label: 'Elevated',
  },
  normal: {
    bg: 'bg-gray-100',
    text: 'text-gray-800',
    icon: Minus,
    label: 'Normal',
  },
}

export default function SignalsTable({ signals, isLoading, lookbackMonths }: SignalsTableProps) {
  if (isLoading) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="animate-pulse space-y-4">
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

  const highSignals = signals.filter((s) => s.signal_type === 'high')
  const elevatedSignals = signals.filter((s) => s.signal_type === 'elevated')

  return (
    <div className="space-y-6">
      {/* Summary */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-red-50 rounded-lg p-4 border border-red-200">
          <div className="flex items-center gap-2">
            <AlertTriangle className="w-5 h-5 text-red-600" />
            <span className="font-medium text-red-800">High Signals</span>
          </div>
          <p className="text-2xl font-bold text-red-900 mt-2">{highSignals.length}</p>
          <p className="text-sm text-red-600">Z-score &gt; 2.0</p>
        </div>
        <div className="bg-yellow-50 rounded-lg p-4 border border-yellow-200">
          <div className="flex items-center gap-2">
            <TrendingUp className="w-5 h-5 text-yellow-600" />
            <span className="font-medium text-yellow-800">Elevated Signals</span>
          </div>
          <p className="text-2xl font-bold text-yellow-900 mt-2">{elevatedSignals.length}</p>
          <p className="text-sm text-yellow-600">Z-score 1.0 - 2.0</p>
        </div>
        <div className="bg-blue-50 rounded-lg p-4 border border-blue-200">
          <div className="flex items-center gap-2">
            <span className="font-medium text-blue-800">Analysis Period</span>
          </div>
          <p className="text-2xl font-bold text-blue-900 mt-2">{lookbackMonths} months</p>
          <p className="text-sm text-blue-600">Lookback window</p>
        </div>
      </div>

      {/* Signals Table */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <div className="p-4 border-b">
          <h3 className="text-lg font-semibold text-gray-900">Safety Signal Detection</h3>
          <p className="text-sm text-gray-600 mt-1">
            Manufacturers with unusual increases in adverse events based on z-score analysis
          </p>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Signal
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Manufacturer
                </th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                  Avg/Month
                </th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                  Latest Month
                </th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                  Z-Score
                </th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                  Total Events
                </th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                  Deaths
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {signals.length === 0 ? (
                <tr>
                  <td colSpan={7} className="px-4 py-8 text-center text-gray-500">
                    No signals detected with current parameters
                  </td>
                </tr>
              ) : (
                signals.map((signal) => {
                  const style = SIGNAL_STYLES[signal.signal_type]
                  const Icon = style.icon
                  return (
                    <tr key={signal.manufacturer} className="hover:bg-gray-50">
                      <td className="px-4 py-3">
                        <span
                          className={`inline-flex items-center gap-1 px-2 py-1 text-xs font-semibold rounded-full ${style.bg} ${style.text}`}
                        >
                          <Icon className="w-3 h-3" />
                          {style.label}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-sm text-gray-900 max-w-xs truncate">
                        {signal.manufacturer}
                      </td>
                      <td className="px-4 py-3 text-sm text-gray-600 text-right">
                        {signal.avg_monthly?.toFixed(1) || 'N/A'}
                      </td>
                      <td className="px-4 py-3 text-sm text-gray-900 text-right font-medium">
                        {signal.latest_month?.toLocaleString() || 'N/A'}
                      </td>
                      <td className="px-4 py-3 text-right">
                        <span
                          className={`font-medium ${
                            signal.z_score > 2
                              ? 'text-red-600'
                              : signal.z_score > 1
                              ? 'text-yellow-600'
                              : 'text-gray-600'
                          }`}
                        >
                          {signal.z_score?.toFixed(2) || 'N/A'}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-sm text-gray-600 text-right">
                        {signal.total_events?.toLocaleString() || 'N/A'}
                      </td>
                      <td className="px-4 py-3 text-right">
                        <span className={signal.total_deaths > 0 ? 'text-red-600 font-medium' : 'text-gray-600'}>
                          {signal.total_deaths?.toLocaleString() || '0'}
                        </span>
                      </td>
                    </tr>
                  )
                })
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
