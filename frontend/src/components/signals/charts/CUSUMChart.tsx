import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
} from 'recharts'
import type { CUSUMDetails } from '../../../types/signals'

interface CUSUMChartProps {
  details: CUSUMDetails
  value: number
  isSignal: boolean
  lookbackMonths?: number
}

export default function CUSUMChart({ details, value, isSignal, lookbackMonths }: CUSUMChartProps) {
  const { mean, std, control_limit, cusum_series } = details

  // Use real series if available, otherwise generate placeholder
  const data = cusum_series?.length
    ? cusum_series.map((point) => ({
        month: formatMonth(point.month),
        cusum: point.cusum,
        aboveLimit: point.cusum > control_limit,
      }))
    : generatePlaceholderCusum(value, control_limit, lookbackMonths || 12)

  // Find the point where limit was first exceeded (if any)
  const exceedIndex = data.findIndex((d) => d.cusum > control_limit)

  return (
    <div className="w-full">
      <div className="text-xs font-medium text-gray-600 mb-2">
        CUSUM Drift Detection Chart
      </div>

      <ResponsiveContainer width="100%" height={140}>
        <AreaChart data={data} margin={{ top: 5, right: 5, bottom: 5, left: 5 }}>
          <defs>
            <linearGradient id="cusumGradient" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={isSignal ? '#ef4444' : '#3b82f6'} stopOpacity={0.4} />
              <stop offset="100%" stopColor={isSignal ? '#ef4444' : '#3b82f6'} stopOpacity={0.1} />
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
            domain={[0, Math.max(value * 1.2, control_limit * 1.5)]}
          />
          <Tooltip
            contentStyle={{
              fontSize: 11,
              padding: '4px 8px',
              borderRadius: 4,
              border: '1px solid #e5e7eb',
            }}
            formatter={(val: number) => [val.toFixed(2), 'CUSUM']}
          />

          {/* Control limit line */}
          <ReferenceLine
            y={control_limit}
            stroke="#ef4444"
            strokeWidth={2}
            strokeDasharray="5 5"
            label={{
              value: `Limit: ${control_limit}`,
              position: 'right',
              fontSize: 9,
              fill: '#ef4444',
            }}
          />

          {/* CUSUM area */}
          <Area
            type="monotone"
            dataKey="cusum"
            stroke={isSignal ? '#ef4444' : '#3b82f6'}
            strokeWidth={2}
            fill="url(#cusumGradient)"
            dot={(props) => {
              const { cx, cy, index } = props
              // Highlight the point where limit was exceeded
              if (exceedIndex >= 0 && index === exceedIndex) {
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
              return <circle cx={0} cy={0} r={0} fill="transparent" />
            }}
          />
        </AreaChart>
      </ResponsiveContainer>

      <div className="flex justify-between text-[10px] text-gray-500 mt-1">
        <span>
          Mean: {mean.toFixed(1)}, Std: {std.toFixed(1)}
        </span>
        <span>
          Max CUSUM:{' '}
          <span className={isSignal ? 'text-red-600 font-medium' : ''}>{value.toFixed(2)}</span>
          {isSignal && <span className="text-red-600"> (limit exceeded!)</span>}
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

function generatePlaceholderCusum(finalValue: number, controlLimit: number, numMonths: number = 12) {
  // Generate a plausible CUSUM path ending at finalValue
  const months = []
  const now = new Date()

  for (let i = numMonths - 1; i >= 0; i--) {
    const d = new Date(now)
    d.setMonth(d.getMonth() - i)
    const month = d.toLocaleDateString('en-US', { month: 'short', year: '2-digit' })

    // Interpolate CUSUM value with some noise
    const progress = (numMonths - i) / numMonths
    const baseValue = finalValue * progress
    const noise = (Math.random() - 0.3) * 0.5 // Slight upward bias
    const cusum = Math.max(0, baseValue + noise)

    months.push({
      month,
      cusum,
      aboveLimit: cusum > controlLimit,
    })
  }

  return months
}
