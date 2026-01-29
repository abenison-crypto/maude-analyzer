import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  Cell,
} from 'recharts'
import type { YoYDetails } from '../../../types/signals'

interface YoYChartProps {
  details: YoYDetails
  value: number
  isSignal: boolean
  method: 'yoy' | 'pop'
}

export default function YoYChart({ details, value, isSignal, method }: YoYChartProps) {
  const { current_period, comparison_period } = details

  const data = [
    {
      name: method === 'yoy' ? 'Prior Year' : 'Prior Period',
      value: comparison_period || 0,
      fill: '#9ca3af',
    },
    {
      name: method === 'yoy' ? 'Current Year' : 'Current Period',
      value: current_period || 0,
      fill: isSignal ? '#ef4444' : '#3b82f6',
    },
  ]

  const maxValue = Math.max(current_period || 0, comparison_period || 0) * 1.2
  const changeLabel = value > 0 ? `+${value}%` : `${value}%`
  const methodLabel = method === 'yoy' ? 'Year-over-Year' : 'Period-over-Period'

  return (
    <div className="w-full">
      <div className="text-xs font-medium text-gray-600 mb-2">
        {methodLabel} Comparison
      </div>

      <div className="flex gap-4">
        {/* Bar chart */}
        <div className="flex-1">
          <ResponsiveContainer width="100%" height={120}>
            <BarChart data={data} margin={{ top: 20, right: 30, bottom: 5, left: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" vertical={false} />
              <XAxis
                dataKey="name"
                tick={{ fontSize: 10, fill: '#6b7280' }}
                tickLine={false}
                axisLine={{ stroke: '#e5e7eb' }}
              />
              <YAxis
                tick={{ fontSize: 9, fill: '#9ca3af' }}
                tickLine={false}
                axisLine={{ stroke: '#e5e7eb' }}
                width={40}
                domain={[0, maxValue]}
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
              <Bar dataKey="value" radius={[4, 4, 0, 0]} barSize={60}>
                {data.map((entry, index) => (
                  <Cell key={index} fill={entry.fill} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Summary */}
        <div className="w-32 text-xs space-y-2">
          <div className="bg-gray-50 rounded p-2">
            <div className="text-gray-500">
              {method === 'yoy' ? 'Prior Year' : 'Prior Period'}
            </div>
            <div className="text-lg font-medium">{(comparison_period || 0).toLocaleString()}</div>
          </div>

          <div className={`rounded p-2 ${isSignal ? 'bg-red-50' : 'bg-blue-50'}`}>
            <div className={isSignal ? 'text-red-600' : 'text-blue-600'}>
              {method === 'yoy' ? 'Current Year' : 'Current Period'}
            </div>
            <div className={`text-lg font-medium ${isSignal ? 'text-red-700' : 'text-blue-700'}`}>
              {(current_period || 0).toLocaleString()}
            </div>
          </div>

          <div className="border-t border-gray-200 pt-2">
            <div className="flex justify-between items-center">
              <span className="text-gray-500">Change:</span>
              <span
                className={`text-lg font-semibold ${
                  isSignal
                    ? 'text-red-600'
                    : value > 0
                    ? 'text-orange-600'
                    : value < 0
                    ? 'text-green-600'
                    : ''
                }`}
              >
                {changeLabel}
              </span>
            </div>
          </div>
        </div>
      </div>

      <div className="text-[10px] text-gray-500 mt-1">
        {value > 0
          ? `Events increased by ${value}% compared to the ${method === 'yoy' ? 'same period last year' : 'comparison period'}`
          : value < 0
          ? `Events decreased by ${Math.abs(value)}%`
          : 'No change in event count'}
      </div>
    </div>
  )
}
