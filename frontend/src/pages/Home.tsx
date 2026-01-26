import { Link } from 'react-router-dom'
import { Search, BarChart3, Skull, TrendingUp } from 'lucide-react'
import KPICards from '../components/KPICards'
import TrendChart from '../components/TrendChart'
import { useDatabaseStatus } from '../hooks/useAnalytics'

export default function HomePage() {
  const { data: status } = useDatabaseStatus()

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-gray-900">MAUDE Analyzer Dashboard</h1>
        <p className="text-gray-600 mt-1">
          FDA Medical Device Adverse Event Reports
          {status && (
            <span className="ml-2 text-sm">
              | {status.date_range_start?.slice(0, 4)} - {status.date_range_end?.slice(0, 4)}
            </span>
          )}
        </p>
      </div>

      {/* Data Quality Alert */}
      {status && status.manufacturer_coverage_pct < 50 && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
          <div className="flex items-start gap-3">
            <div className="text-yellow-600">
              <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
              </svg>
            </div>
            <div>
              <h3 className="text-sm font-medium text-yellow-800">Data Quality Notice</h3>
              <p className="text-sm text-yellow-700 mt-1">
                Only {status.manufacturer_coverage_pct.toFixed(1)}% of events have manufacturer data linked.
                Some events may not have linked device information.
              </p>
            </div>
          </div>
        </div>
      )}

      {/* KPI Cards */}
      <KPICards />

      {/* Quick Actions */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <Link
          to="/explore?event_types=D"
          className="bg-white rounded-lg shadow p-4 hover:shadow-md transition-shadow flex items-center gap-4"
        >
          <div className="p-3 bg-red-100 rounded-lg">
            <Skull className="w-6 h-6 text-red-600" />
          </div>
          <div>
            <h3 className="font-medium text-gray-900">Search Deaths</h3>
            <p className="text-sm text-gray-500">View death events</p>
          </div>
        </Link>

        <Link
          to="/explore"
          className="bg-white rounded-lg shadow p-4 hover:shadow-md transition-shadow flex items-center gap-4"
        >
          <div className="p-3 bg-blue-100 rounded-lg">
            <Search className="w-6 h-6 text-blue-600" />
          </div>
          <div>
            <h3 className="font-medium text-gray-900">Browse Latest</h3>
            <p className="text-sm text-gray-500">Explore recent events</p>
          </div>
        </Link>

        <Link
          to="/analyze"
          className="bg-white rounded-lg shadow p-4 hover:shadow-md transition-shadow flex items-center gap-4"
        >
          <div className="p-3 bg-green-100 rounded-lg">
            <BarChart3 className="w-6 h-6 text-green-600" />
          </div>
          <div>
            <h3 className="font-medium text-gray-900">Compare Manufacturers</h3>
            <p className="text-sm text-gray-500">Analyze by manufacturer</p>
          </div>
        </Link>

        <Link
          to="/analyze"
          className="bg-white rounded-lg shadow p-4 hover:shadow-md transition-shadow flex items-center gap-4"
        >
          <div className="p-3 bg-purple-100 rounded-lg">
            <TrendingUp className="w-6 h-6 text-purple-600" />
          </div>
          <div>
            <h3 className="font-medium text-gray-900">View Trends</h3>
            <p className="text-sm text-gray-500">Time series analysis</p>
          </div>
        </Link>
      </div>

      {/* Trend Chart */}
      <TrendChart groupBy="year" />

      {/* Database Info */}
      {status && (
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold text-gray-900 mb-4">Database Status</h3>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
            <div>
              <span className="text-gray-500">Total Events</span>
              <p className="font-medium text-gray-900">{status.total_events.toLocaleString()}</p>
            </div>
            <div>
              <span className="text-gray-500">Total Devices</span>
              <p className="font-medium text-gray-900">{status.total_devices.toLocaleString()}</p>
            </div>
            <div>
              <span className="text-gray-500">Total Patients</span>
              <p className="font-medium text-gray-900">{status.total_patients.toLocaleString()}</p>
            </div>
            <div>
              <span className="text-gray-500">Last Updated</span>
              <p className="font-medium text-gray-900">
                {status.last_refresh ? new Date(status.last_refresh).toLocaleDateString() : 'N/A'}
              </p>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
