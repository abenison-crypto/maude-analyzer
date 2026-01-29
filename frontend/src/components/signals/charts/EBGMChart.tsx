import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  Cell,
  ReferenceLine,
} from 'recharts'
import type { EBGMDetails } from '../../../types/signals'

interface EBGMChartProps {
  details: EBGMDetails
  value: number
  lowerCi: number | null | undefined
  isSignal: boolean
}

export default function EBGMChart({ details, value, lowerCi, isSignal }: EBGMChartProps) {
  const { observed, expected, rr } = details

  const data = [
    { name: 'Expected', value: expected, fill: '#9ca3af' },
    { name: 'Observed', value: observed, fill: isSignal ? '#ef4444' : '#f97316' },
  ]

  const maxValue = Math.max(observed, expected) * 1.2

  return (
    <div className="w-full">
      <div className="text-xs font-medium text-gray-600 mb-2">
        Observed vs Expected Deaths (EBGM)
      </div>

      <div className="flex gap-4">
        {/* Horizontal bar chart */}
        <div className="flex-1">
          <ResponsiveContainer width="100%" height={100}>
            <BarChart
              data={data}
              layout="vertical"
              margin={{ top: 5, right: 30, bottom: 5, left: 60 }}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" horizontal={false} />
              <XAxis
                type="number"
                domain={[0, maxValue]}
                tick={{ fontSize: 9, fill: '#9ca3af' }}
              />
              <YAxis
                type="category"
                dataKey="name"
                tick={{ fontSize: 10, fill: '#6b7280' }}
                width={60}
              />
              <Tooltip
                contentStyle={{
                  fontSize: 11,
                  padding: '4px 8px',
                  borderRadius: 4,
                  border: '1px solid #e5e7eb',
                }}
                formatter={(val: number) => [val.toFixed(1), 'Count']}
              />

              {/* Reference line at expected */}
              <ReferenceLine
                x={expected}
                stroke="#6b7280"
                strokeDasharray="5 5"
              />

              <Bar dataKey="value" radius={[0, 4, 4, 0]}>
                {data.map((entry, index) => (
                  <Cell key={index} fill={entry.fill} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Summary metrics */}
        <div className="w-36 text-xs space-y-2">
          <div className="bg-gray-50 rounded p-2">
            <div className="text-gray-500">Expected (under independence)</div>
            <div className="text-lg font-medium">{expected.toFixed(1)}</div>
          </div>

          <div className={`rounded p-2 ${isSignal ? 'bg-red-50' : 'bg-orange-50'}`}>
            <div className={isSignal ? 'text-red-600' : 'text-orange-600'}>Observed</div>
            <div className={`text-lg font-medium ${isSignal ? 'text-red-700' : 'text-orange-700'}`}>
              {observed}
            </div>
          </div>

          <div className="border-t border-gray-200 pt-2 space-y-1">
            <div className="flex justify-between">
              <span className="text-gray-500">RR:</span>
              <span>{rr.toFixed(2)}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">EBGM:</span>
              <span className={isSignal ? 'text-red-600 font-semibold' : 'font-medium'}>
                {value.toFixed(2)}
              </span>
            </div>
            {lowerCi != null && (
              <div className="flex justify-between">
                <span className="text-gray-500">EB05:</span>
                <span className={lowerCi >= 1 ? 'text-red-600' : ''}>{lowerCi.toFixed(2)}</span>
              </div>
            )}
          </div>
        </div>
      </div>

      <div className="text-[10px] text-gray-500 mt-1">
        Signal requires EBGM {'>='} 2.0 AND EB05 {'>='} 1.0 (95% confidence lower bound)
      </div>
    </div>
  )
}
