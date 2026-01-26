import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'

interface TextFrequencyItem {
  term: string
  count: number
  percentage: number
}

interface TextFrequencyProps {
  data: TextFrequencyItem[]
  isLoading: boolean
}

export default function TextFrequency({ data, isLoading }: TextFrequencyProps) {
  if (isLoading) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="animate-pulse space-y-4">
          <div className="h-4 bg-gray-200 rounded w-1/4" />
          <div className="h-80 bg-gray-200 rounded" />
        </div>
      </div>
    )
  }

  // Take top 20 for the chart
  const chartData = data.slice(0, 20)

  return (
    <div className="space-y-6">
      {/* Bar Chart */}
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Top Terms in Event Narratives</h3>
        {chartData.length > 0 ? (
          <ResponsiveContainer width="100%" height={500}>
            <BarChart
              data={chartData}
              layout="vertical"
              margin={{ left: 100, right: 20, top: 10, bottom: 10 }}
            >
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis type="number" tickFormatter={(v) => v.toLocaleString()} />
              <YAxis type="category" dataKey="term" width={90} tick={{ fontSize: 12 }} />
              <Tooltip
                formatter={(value: number) => [
                  value.toLocaleString(),
                  'Occurrences',
                ]}
              />
              <Bar dataKey="count" fill="#3B82F6" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <div className="h-80 flex items-center justify-center text-gray-500">
            No text data available for the selected filters
          </div>
        )}
      </div>

      {/* Full Table */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <div className="p-4 border-b">
          <h3 className="text-lg font-semibold text-gray-900">All Terms</h3>
          <p className="text-sm text-gray-600 mt-1">
            Frequency analysis of terms in adverse event narrative text
          </p>
        </div>
        <div className="overflow-x-auto max-h-96">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50 sticky top-0">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Rank
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Term
                </th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                  Count
                </th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">
                  Percentage
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                  Distribution
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {data.length === 0 ? (
                <tr>
                  <td colSpan={5} className="px-4 py-8 text-center text-gray-500">
                    No terms found
                  </td>
                </tr>
              ) : (
                data.map((item, index) => {
                  const maxPercentage = data[0]?.percentage || 1
                  const barWidth = (item.percentage / maxPercentage) * 100
                  return (
                    <tr key={item.term} className="hover:bg-gray-50">
                      <td className="px-4 py-2 text-sm text-gray-500">{index + 1}</td>
                      <td className="px-4 py-2 text-sm text-gray-900 font-medium">{item.term}</td>
                      <td className="px-4 py-2 text-sm text-gray-600 text-right">
                        {item.count.toLocaleString()}
                      </td>
                      <td className="px-4 py-2 text-sm text-gray-600 text-right">{item.percentage}%</td>
                      <td className="px-4 py-2 w-48">
                        <div className="bg-gray-200 rounded-full h-2">
                          <div
                            className="bg-blue-500 rounded-full h-2"
                            style={{ width: `${barWidth}%` }}
                          />
                        </div>
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
