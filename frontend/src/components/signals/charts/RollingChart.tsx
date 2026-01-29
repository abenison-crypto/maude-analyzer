import {
  ComposedChart,
  Line,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
} from 'recharts'
import type { RollingDetails } from '../../../types/signals'

interface RollingChartProps {
  details: RollingDetails
  value: number
  isSignal: boolean
}

export default function RollingChart({ details, value, isSignal }: RollingChartProps) {
  const { rolling_avg, rolling_std, latest, window_months, monthly_series } = details

  // Use real series if available, otherwise generate placeholder
  const data = monthly_series?.length
    ? monthly_series.map((point, index) => ({
        month: formatMonth(point.month),
        count: point.count,
        upperBand: rolling_avg + rolling_std,
        lowerBand: Math.max(0, rolling_avg - rolling_std),
        rollingAvg: rolling_avg,
        isLatest: index === monthly_series.length - 1,
      }))
    : generatePlaceholderData(rolling_avg, rolling_std, latest, window_months)

  return (
    <div className="w-full">
      <div className="text-xs font-medium text-gray-600 mb-2">
        Rolling {window_months}-Month Average Comparison
      </div>

      <ResponsiveContainer width="100%" height={140}>
        <ComposedChart data={data} margin={{ top: 5, right: 5, bottom: 5, left: 5 }}>
          <defs>
            <linearGradient id="rollingBand" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#9ca3af" stopOpacity={0.3} />
              <stop offset="100%" stopColor="#9ca3af" stopOpacity={0.1} />
            </linearGradient>
          </defs>

          <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
          <XAxis
            dataKey="month"
            tick={{ fontSize: 9, fill: '#9ca3af' }}
            tickLine={false}
            axisLine={{ stroke: '#e5e7eb' }}
          />
          <YAxis
            tick={{ fontSize: 9, fill: '#9ca3af' }}
            tickLine={false}
            axisLine={{ stroke: '#e5e7eb' }}
            width={30}
          />
          <Tooltip
            contentStyle={{
              fontSize: 11,
              padding: '4px 8px',
              borderRadius: 4,
              border: '1px solid #e5e7eb',
            }}
            formatter={(val: number, name: string) => {
              if (name === 'count') return [val.toLocaleString(), 'Events']
              if (name === 'rollingAvg') return [val.toFixed(1), 'Rolling Avg']
              return [val, name]
            }}
          />

          {/* Confidence band (rolling avg +/- std) */}
          <Area
            dataKey="upperBand"
            stroke="none"
            fill="url(#rollingBand)"
            type="monotone"
          />
          <Area
            dataKey="lowerBand"
            stroke="none"
            fill="#fff"
            type="monotone"
          />

          {/* Rolling average line */}
          <Line
            type="monotone"
            dataKey="rollingAvg"
            stroke="#9ca3af"
            strokeWidth={1}
            strokeDasharray="5 5"
            dot={false}
          />

          {/* Actual values */}
          <Line
            type="monotone"
            dataKey="count"
            stroke="#3b82f6"
            strokeWidth={2}
            dot={(props) => {
              const { cx, cy, payload } = props
              if (payload.isLatest) {
                return (
                  <circle
                    cx={cx}
                    cy={cy}
                    r={6}
                    fill={isSignal ? '#ef4444' : '#3b82f6'}
                    stroke="#fff"
                    strokeWidth={2}
                  />
                )
              }
              return <circle cx={cx} cy={cy} r={2} fill="#3b82f6" />
            }}
          />
        </ComposedChart>
      </ResponsiveContainer>

      <div className="flex justify-between text-[10px] text-gray-500 mt-1">
        <span>
          Rolling avg: {rolling_avg.toFixed(0)} | Latest: {latest} |{' '}
          {window_months}-month window
        </span>
        <span>
          Deviation:{' '}
          <span className={isSignal ? 'text-red-600 font-medium' : ''}>
            {value.toFixed(2)} std
          </span>
        </span>
      </div>
    </div>
  )
}

function formatMonth(dateStr: string): string {
  try {
    const date = new Date(dateStr)
    return date.toLocaleDateString('en-US', { month: 'short', year: '2-digit' })
  } catch {
    return dateStr
  }
}

function generatePlaceholderData(
  avg: number,
  std: number,
  latest: number,
  windowMonths: number
) {
  const months = []
  const now = new Date()
  const numPoints = Math.max(12, windowMonths + 3)

  for (let i = numPoints - 1; i >= 0; i--) {
    const d = new Date(now)
    d.setMonth(d.getMonth() - i)
    const month = d.toLocaleDateString('en-US', { month: 'short', year: '2-digit' })

    // For the last month, use actual latest value
    const count =
      i === 0 ? latest : Math.max(0, Math.round(avg + (Math.random() - 0.5) * std * 1.5))

    months.push({
      month,
      count,
      upperBand: avg + std,
      lowerBand: Math.max(0, avg - std),
      rollingAvg: avg,
      isLatest: i === 0,
    })
  }

  return months
}
