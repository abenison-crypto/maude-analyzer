import {
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  ReferenceLine,
  Area,
  ComposedChart,
  ResponsiveContainer,
  Tooltip,
} from 'recharts'
import type { ZScoreDetails } from '../../../types/signals'

interface ZScoreChartProps {
  details: ZScoreDetails
  value: number
  isSignal: boolean
}

export default function ZScoreChart({ details, value, isSignal }: ZScoreChartProps) {
  const { avg_monthly, std_monthly, latest_month, monthly_series } = details

  // If we have monthly series data, use it; otherwise generate placeholder
  const data = monthly_series?.length
    ? monthly_series.map((point, index) => ({
        month: formatMonth(point.month),
        count: point.count,
        upperBand: avg_monthly + std_monthly,
        lowerBand: Math.max(0, avg_monthly - std_monthly),
        isLatest: index === monthly_series.length - 1,
      }))
    : generatePlaceholderData(avg_monthly, std_monthly, latest_month)

  return (
    <div className="w-full">
      <div className="text-xs font-medium text-gray-600 mb-2">Monthly Events vs Historical Average</div>
      <ResponsiveContainer width="100%" height={120}>
        <ComposedChart data={data} margin={{ top: 5, right: 5, bottom: 5, left: 5 }}>
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
            formatter={(val: number) => [val.toLocaleString(), 'Events']}
          />

          {/* Standard deviation band */}
          <Area
            dataKey="upperBand"
            stroke="none"
            fill="#e5e7eb"
            fillOpacity={0.5}
            type="monotone"
          />
          <Area
            dataKey="lowerBand"
            stroke="none"
            fill="#fff"
            fillOpacity={1}
            type="monotone"
          />

          {/* Mean reference line */}
          <ReferenceLine
            y={avg_monthly}
            stroke="#9ca3af"
            strokeDasharray="5 5"
            label={{
              value: `Avg: ${Math.round(avg_monthly)}`,
              position: 'right',
              fontSize: 9,
              fill: '#9ca3af',
            }}
          />

          {/* Actual values */}
          <Line
            type="monotone"
            dataKey="count"
            stroke="#3b82f6"
            strokeWidth={2}
            dot={(props) => {
              const { cx, cy, payload } = props
              if (payload.isLatest && isSignal) {
                return (
                  <circle
                    cx={cx}
                    cy={cy}
                    r={5}
                    fill="#ef4444"
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
        <span>Gray band = mean +/- 1 std</span>
        <span>
          Z-Score: <span className={isSignal ? 'text-red-600 font-medium' : ''}>{value.toFixed(2)}</span>
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

function generatePlaceholderData(avg: number, std: number, latest: number) {
  // Generate 12 months of synthetic data for visualization
  const months = []
  const now = new Date()

  for (let i = 11; i >= 0; i--) {
    const d = new Date(now)
    d.setMonth(d.getMonth() - i)
    const month = d.toLocaleDateString('en-US', { month: 'short', year: '2-digit' })

    // For the last month, use actual latest value; otherwise generate around average
    const count = i === 0 ? latest : Math.max(0, Math.round(avg + (Math.random() - 0.5) * std * 2))

    months.push({
      month,
      count,
      upperBand: avg + std,
      lowerBand: Math.max(0, avg - std),
      isLatest: i === 0,
    })
  }

  return months
}
