import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { useTrends } from '../hooks/useAnalytics'

interface TrendChartProps {
  groupBy?: 'day' | 'month' | 'year'
  showDeaths?: boolean
  showInjuries?: boolean
  showMalfunctions?: boolean
}

export default function TrendChart({
  groupBy = 'month',
  showDeaths = true,
  showInjuries = true,
  showMalfunctions = true,
}: TrendChartProps) {
  const { data: trends, isLoading } = useTrends(groupBy)

  if (isLoading) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="h-80 flex items-center justify-center">
          <div className="animate-pulse text-gray-400">Loading trends...</div>
        </div>
      </div>
    )
  }

  if (!trends || trends.length === 0) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="h-80 flex items-center justify-center text-gray-500">
          No trend data available
        </div>
      </div>
    )
  }

  // Format data for the chart
  const chartData = trends.map((point) => ({
    ...point,
    period: point.period ? formatDate(point.period, groupBy) : 'Unknown',
  }))

  function formatDate(dateStr: string, groupBy: string): string {
    const date = new Date(dateStr)
    if (groupBy === 'year') {
      return date.getFullYear().toString()
    } else if (groupBy === 'month') {
      return date.toLocaleDateString('en-US', { year: 'numeric', month: 'short' })
    } else {
      return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })
    }
  }

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h3 className="text-lg font-semibold text-gray-900 mb-4">Event Trends Over Time</h3>
      <ResponsiveContainer width="100%" height={350}>
        <LineChart data={chartData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="period" tick={{ fontSize: 12 }} />
          <YAxis tick={{ fontSize: 12 }} tickFormatter={(value) => value.toLocaleString()} />
          <Tooltip
            formatter={(value: number) => value.toLocaleString()}
            labelStyle={{ fontWeight: 'bold' }}
          />
          <Legend />
          <Line
            type="monotone"
            dataKey="total"
            stroke="#3B82F6"
            strokeWidth={2}
            name="Total"
            dot={false}
          />
          {showDeaths && (
            <Line
              type="monotone"
              dataKey="deaths"
              stroke="#EF4444"
              strokeWidth={2}
              name="Deaths"
              dot={false}
            />
          )}
          {showInjuries && (
            <Line
              type="monotone"
              dataKey="injuries"
              stroke="#F97316"
              strokeWidth={2}
              name="Injuries"
              dot={false}
            />
          )}
          {showMalfunctions && (
            <Line
              type="monotone"
              dataKey="malfunctions"
              stroke="#EAB308"
              strokeWidth={2}
              name="Malfunctions"
              dot={false}
            />
          )}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
