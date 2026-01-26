import { useState } from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts'
import FilterBar from '../components/FilterBar'
import TrendChart from '../components/TrendChart'
import { useManufacturerComparison, useEventTypeDistribution } from '../hooks/useAnalytics'
import { useManufacturers } from '../hooks/useEvents'

const TABS = [
  { id: 'trends', label: 'Trends' },
  { id: 'compare', label: 'Compare' },
  { id: 'distribution', label: 'Distribution' },
]

const COLORS = ['#EF4444', '#F97316', '#EAB308', '#6B7280', '#3B82F6']

export default function AnalyzePage() {
  const [activeTab, setActiveTab] = useState('trends')
  const [groupBy, setGroupBy] = useState<'month' | 'year'>('year')
  const [selectedMfrs, setSelectedMfrs] = useState<string[]>([])
  const [mfrSearch, setMfrSearch] = useState('')

  const { data: manufacturers } = useManufacturers(mfrSearch)
  const { data: comparison, isLoading: comparisonLoading } = useManufacturerComparison(selectedMfrs)
  const { data: distribution, isLoading: distributionLoading } = useEventTypeDistribution()

  const handleAddMfr = (name: string) => {
    if (!selectedMfrs.includes(name) && selectedMfrs.length < 10) {
      setSelectedMfrs([...selectedMfrs, name])
    }
    setMfrSearch('')
  }

  const handleRemoveMfr = (name: string) => {
    setSelectedMfrs(selectedMfrs.filter((m) => m !== name))
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Analyze</h1>
        <p className="text-gray-600 mt-1">
          Analyze trends, compare manufacturers, and explore event patterns
        </p>
      </div>

      {/* Filters */}
      <FilterBar />

      {/* Tabs */}
      <div className="border-b border-gray-200">
        <nav className="-mb-px flex space-x-8">
          {TABS.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`py-4 px-1 border-b-2 font-medium text-sm ${
                activeTab === tab.id
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </nav>
      </div>

      {/* Tab Content */}
      <div className="mt-6">
        {activeTab === 'trends' && (
          <div className="space-y-4">
            <div className="flex justify-end">
              <select
                value={groupBy}
                onChange={(e) => setGroupBy(e.target.value as 'month' | 'year')}
                className="px-3 py-2 border border-gray-300 rounded-md text-sm"
              >
                <option value="month">By Month</option>
                <option value="year">By Year</option>
              </select>
            </div>
            <TrendChart groupBy={groupBy} />
          </div>
        )}

        {activeTab === 'compare' && (
          <div className="space-y-4">
            {/* Manufacturer Selection */}
            <div className="bg-white rounded-lg shadow p-4">
              <h3 className="font-medium text-gray-900 mb-3">Select Manufacturers to Compare</h3>
              <div className="flex gap-2 mb-3">
                <input
                  type="text"
                  value={mfrSearch}
                  onChange={(e) => setMfrSearch(e.target.value)}
                  placeholder="Search manufacturers..."
                  className="flex-1 px-3 py-2 border border-gray-300 rounded-md"
                />
              </div>
              {mfrSearch && manufacturers && manufacturers.length > 0 && (
                <div className="border border-gray-200 rounded-md max-h-48 overflow-auto mb-3">
                  {manufacturers.slice(0, 10).map((mfr) => (
                    <button
                      key={mfr.name}
                      onClick={() => handleAddMfr(mfr.name)}
                      disabled={selectedMfrs.includes(mfr.name)}
                      className="w-full text-left px-3 py-2 hover:bg-gray-100 text-sm disabled:opacity-50"
                    >
                      {mfr.name} ({mfr.count.toLocaleString()})
                    </button>
                  ))}
                </div>
              )}
              <div className="flex flex-wrap gap-2">
                {selectedMfrs.map((mfr) => (
                  <span
                    key={mfr}
                    className="inline-flex items-center gap-1 px-2 py-1 bg-blue-100 text-blue-800 text-sm rounded"
                  >
                    {mfr.substring(0, 30)}...
                    <button onClick={() => handleRemoveMfr(mfr)} className="hover:text-blue-600">
                      &times;
                    </button>
                  </span>
                ))}
              </div>
            </div>

            {/* Comparison Chart */}
            {selectedMfrs.length > 0 && (
              <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-lg font-semibold text-gray-900 mb-4">Manufacturer Comparison</h3>
                {comparisonLoading ? (
                  <div className="h-80 flex items-center justify-center">
                    <div className="animate-pulse text-gray-400">Loading comparison...</div>
                  </div>
                ) : comparison && comparison.length > 0 ? (
                  <ResponsiveContainer width="100%" height={400}>
                    <BarChart data={comparison} layout="vertical" margin={{ left: 150 }}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis type="number" tickFormatter={(v) => v.toLocaleString()} />
                      <YAxis type="category" dataKey="manufacturer" width={140} tick={{ fontSize: 12 }} />
                      <Tooltip formatter={(value: number) => value.toLocaleString()} />
                      <Legend />
                      <Bar dataKey="deaths" fill="#EF4444" name="Deaths" />
                      <Bar dataKey="injuries" fill="#F97316" name="Injuries" />
                      <Bar dataKey="malfunctions" fill="#EAB308" name="Malfunctions" />
                    </BarChart>
                  </ResponsiveContainer>
                ) : (
                  <p className="text-gray-500 text-center py-8">No data available for selected manufacturers</p>
                )}
              </div>
            )}

            {selectedMfrs.length === 0 && (
              <div className="bg-gray-50 rounded-lg p-8 text-center text-gray-500">
                Select at least one manufacturer to compare
              </div>
            )}
          </div>
        )}

        {activeTab === 'distribution' && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Pie Chart */}
            <div className="bg-white rounded-lg shadow p-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">Event Type Distribution</h3>
              {distributionLoading ? (
                <div className="h-80 flex items-center justify-center">
                  <div className="animate-pulse text-gray-400">Loading...</div>
                </div>
              ) : distribution && distribution.length > 0 ? (
                <ResponsiveContainer width="100%" height={300}>
                  <PieChart>
                    <Pie
                      data={distribution}
                      dataKey="count"
                      nameKey="type"
                      cx="50%"
                      cy="50%"
                      outerRadius={100}
                      label={({ type, percentage }) => `${type} (${percentage}%)`}
                    >
                      {distribution.map((_, index) => (
                        <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip formatter={(value: number) => value.toLocaleString()} />
                  </PieChart>
                </ResponsiveContainer>
              ) : (
                <p className="text-gray-500 text-center py-8">No data available</p>
              )}
            </div>

            {/* Distribution Table */}
            <div className="bg-white rounded-lg shadow p-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">Event Counts</h3>
              {distribution && distribution.length > 0 ? (
                <table className="min-w-full">
                  <thead>
                    <tr className="border-b">
                      <th className="text-left py-2 text-sm font-medium text-gray-500">Event Type</th>
                      <th className="text-right py-2 text-sm font-medium text-gray-500">Count</th>
                      <th className="text-right py-2 text-sm font-medium text-gray-500">Percentage</th>
                    </tr>
                  </thead>
                  <tbody>
                    {distribution.map((item, index) => (
                      <tr key={item.type} className="border-b">
                        <td className="py-3 flex items-center gap-2">
                          <span
                            className="w-3 h-3 rounded-full"
                            style={{ backgroundColor: COLORS[index % COLORS.length] }}
                          />
                          {item.type}
                        </td>
                        <td className="py-3 text-right font-medium">{item.count.toLocaleString()}</td>
                        <td className="py-3 text-right text-gray-500">{item.percentage}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <p className="text-gray-500 text-center py-8">No data available</p>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
