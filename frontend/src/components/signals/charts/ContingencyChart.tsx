import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  Legend,
  Cell,
} from 'recharts'
import type { PRRDetails } from '../../../types/signals'

interface ContingencyChartProps {
  details: PRRDetails
  method: 'prr' | 'ror'
  value: number
  isSignal: boolean
}

export default function ContingencyChart({ details, method, value, isSignal }: ContingencyChartProps) {
  const { a, b, c, d } = details

  // Calculate percentages for the 2x2 table
  const entityTotal = a + b
  const otherTotal = c + d
  const entityDeathPct = entityTotal > 0 ? (a / entityTotal) * 100 : 0
  const otherDeathPct = otherTotal > 0 ? (c / otherTotal) * 100 : 0

  const data = [
    {
      name: 'This Entity',
      deaths: a,
      other: b,
      deathPct: entityDeathPct,
    },
    {
      name: 'All Others',
      deaths: c,
      other: d,
      deathPct: otherDeathPct,
    },
  ]

  const methodLabel = method === 'prr' ? 'PRR' : 'ROR'

  return (
    <div className="w-full">
      <div className="text-xs font-medium text-gray-600 mb-2">
        2x2 Contingency Table: Deaths vs Other Events
      </div>

      <div className="flex gap-4">
        {/* Bar chart */}
        <div className="flex-1">
          <ResponsiveContainer width="100%" height={140}>
            <BarChart data={data} layout="vertical" margin={{ top: 5, right: 30, bottom: 5, left: 60 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" horizontal={false} />
              <XAxis type="number" tick={{ fontSize: 9, fill: '#9ca3af' }} />
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
                formatter={(val: number, name: string) => [val.toLocaleString(), name]}
              />
              <Legend
                wrapperStyle={{ fontSize: 10 }}
                iconSize={8}
              />
              <Bar dataKey="deaths" name="Deaths" stackId="a">
                {data.map((_, index) => (
                  <Cell key={index} fill={index === 0 && isSignal ? '#ef4444' : '#f87171'} />
                ))}
              </Bar>
              <Bar dataKey="other" name="Other Events" stackId="a" fill="#93c5fd" />
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Summary table */}
        <div className="w-40 text-xs">
          <table className="w-full border-collapse">
            <thead>
              <tr>
                <th className="border border-gray-200 bg-gray-50 p-1"></th>
                <th className="border border-gray-200 bg-gray-50 p-1 text-center">Deaths</th>
                <th className="border border-gray-200 bg-gray-50 p-1 text-center">Other</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td className="border border-gray-200 p-1 font-medium bg-blue-50">Entity</td>
                <td className={`border border-gray-200 p-1 text-center ${isSignal ? 'bg-red-50 text-red-700 font-medium' : ''}`}>
                  {a.toLocaleString()}
                </td>
                <td className="border border-gray-200 p-1 text-center">{b.toLocaleString()}</td>
              </tr>
              <tr>
                <td className="border border-gray-200 p-1 font-medium">Others</td>
                <td className="border border-gray-200 p-1 text-center">{c.toLocaleString()}</td>
                <td className="border border-gray-200 p-1 text-center">{d.toLocaleString()}</td>
              </tr>
            </tbody>
          </table>

          <div className="mt-2 space-y-1">
            <div className="flex justify-between">
              <span className="text-gray-500">Entity death %:</span>
              <span className={isSignal ? 'text-red-600 font-medium' : ''}>{entityDeathPct.toFixed(1)}%</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500">Others death %:</span>
              <span>{otherDeathPct.toFixed(1)}%</span>
            </div>
            <div className="flex justify-between border-t border-gray-200 pt-1 mt-1">
              <span className="text-gray-500">{methodLabel}:</span>
              <span className={isSignal ? 'text-red-600 font-semibold' : 'font-medium'}>{value.toFixed(2)}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
