import { Database, RefreshCw, FileText, CheckCircle, XCircle } from 'lucide-react'
import { useDatabaseStatus, useDataQuality, useIngestionHistory } from '../hooks/useAnalytics'

export default function AdminPage() {
  const { data: status, isLoading: statusLoading } = useDatabaseStatus()
  const { data: quality, isLoading: qualityLoading } = useDataQuality()
  const { data: history, isLoading: historyLoading } = useIngestionHistory()

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold text-gray-900">Admin</h1>
        <p className="text-gray-600 mt-1">Database status, data quality, and management</p>
      </div>

      {/* Database Status */}
      <div className="bg-white rounded-lg shadow p-6">
        <div className="flex items-center gap-2 mb-4">
          <Database className="w-5 h-5 text-blue-600" />
          <h2 className="text-lg font-semibold text-gray-900">Database Status</h2>
        </div>

        {statusLoading ? (
          <div className="animate-pulse space-y-3">
            <div className="h-4 bg-gray-200 rounded w-1/3" />
            <div className="h-4 bg-gray-200 rounded w-1/2" />
          </div>
        ) : status ? (
          <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
            <div>
              <span className="text-sm text-gray-500">Total Events</span>
              <p className="text-2xl font-bold text-gray-900">{status.total_events.toLocaleString()}</p>
            </div>
            <div>
              <span className="text-sm text-gray-500">Total Devices</span>
              <p className="text-2xl font-bold text-gray-900">{status.total_devices.toLocaleString()}</p>
            </div>
            <div>
              <span className="text-sm text-gray-500">Total Patients</span>
              <p className="text-2xl font-bold text-gray-900">{status.total_patients.toLocaleString()}</p>
            </div>
            <div>
              <span className="text-sm text-gray-500">Manufacturer Coverage</span>
              <p className="text-2xl font-bold text-gray-900">{status.manufacturer_coverage_pct.toFixed(1)}%</p>
            </div>
            <div className="col-span-2">
              <span className="text-sm text-gray-500">Date Range</span>
              <p className="text-lg font-medium text-gray-900">
                {status.date_range_start} to {status.date_range_end}
              </p>
            </div>
            <div className="col-span-2">
              <span className="text-sm text-gray-500">Last Refresh</span>
              <p className="text-lg font-medium text-gray-900">
                {status.last_refresh ? new Date(status.last_refresh).toLocaleString() : 'Never'}
              </p>
            </div>
          </div>
        ) : (
          <p className="text-gray-500">Unable to load database status</p>
        )}
      </div>

      {/* Data Quality */}
      <div className="bg-white rounded-lg shadow p-6">
        <div className="flex items-center gap-2 mb-4">
          <CheckCircle className="w-5 h-5 text-green-600" />
          <h2 className="text-lg font-semibold text-gray-900">Data Quality</h2>
        </div>

        {qualityLoading ? (
          <div className="animate-pulse space-y-3">
            <div className="h-4 bg-gray-200 rounded w-full" />
            <div className="h-4 bg-gray-200 rounded w-3/4" />
          </div>
        ) : quality ? (
          <div className="space-y-6">
            {/* Field Completeness */}
            <div>
              <h3 className="text-sm font-medium text-gray-700 mb-3">Field Completeness</h3>
              <div className="space-y-2">
                {quality.field_completeness.map((field) => (
                  <div key={field.field} className="flex items-center gap-3">
                    <span className="w-40 text-sm text-gray-600">{field.field}</span>
                    <div className="flex-1 bg-gray-200 rounded-full h-2">
                      <div
                        className={`h-2 rounded-full ${
                          field.percentage > 90 ? 'bg-green-500' : field.percentage > 50 ? 'bg-yellow-500' : 'bg-red-500'
                        }`}
                        style={{ width: `${field.percentage}%` }}
                      />
                    </div>
                    <span className="w-16 text-sm text-gray-600 text-right">{field.percentage.toFixed(1)}%</span>
                  </div>
                ))}
              </div>
            </div>

            {/* Orphan Analysis */}
            <div>
              <h3 className="text-sm font-medium text-gray-700 mb-3">Orphan Analysis</h3>
              <div className="grid grid-cols-2 gap-4">
                <div className="bg-gray-50 rounded-lg p-4">
                  <span className="text-sm text-gray-500">Orphaned Devices</span>
                  <p className="text-xl font-bold text-gray-900">
                    {quality.orphan_analysis.orphaned_devices.toLocaleString()}
                  </p>
                </div>
                <div className="bg-gray-50 rounded-lg p-4">
                  <span className="text-sm text-gray-500">Events Without Devices</span>
                  <p className="text-xl font-bold text-gray-900">
                    {quality.orphan_analysis.events_without_devices.toLocaleString()}
                  </p>
                </div>
              </div>
            </div>
          </div>
        ) : (
          <p className="text-gray-500">Unable to load data quality</p>
        )}
      </div>

      {/* Ingestion History */}
      <div className="bg-white rounded-lg shadow p-6">
        <div className="flex items-center gap-2 mb-4">
          <FileText className="w-5 h-5 text-purple-600" />
          <h2 className="text-lg font-semibold text-gray-900">Recent Ingestion History</h2>
        </div>

        {historyLoading ? (
          <div className="animate-pulse space-y-3">
            {[1, 2, 3].map((i) => (
              <div key={i} className="h-10 bg-gray-200 rounded" />
            ))}
          </div>
        ) : history && history.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead>
                <tr>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">File</th>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Type</th>
                  <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase">Loaded</th>
                  <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase">Errors</th>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                  <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase">Completed</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {history.slice(0, 20).map((entry) => (
                  <tr key={entry.id}>
                    <td className="px-3 py-2 text-sm text-gray-900">{entry.file_name}</td>
                    <td className="px-3 py-2 text-sm text-gray-600">{entry.file_type}</td>
                    <td className="px-3 py-2 text-sm text-gray-600 text-right">
                      {entry.records_loaded.toLocaleString()}
                    </td>
                    <td className="px-3 py-2 text-sm text-right">
                      {entry.records_errors > 0 ? (
                        <span className="text-red-600">{entry.records_errors.toLocaleString()}</span>
                      ) : (
                        <span className="text-gray-400">0</span>
                      )}
                    </td>
                    <td className="px-3 py-2">
                      {entry.status === 'COMPLETED' ? (
                        <span className="inline-flex items-center gap-1 text-green-600 text-sm">
                          <CheckCircle className="w-4 h-4" />
                          OK
                        </span>
                      ) : (
                        <span className="inline-flex items-center gap-1 text-yellow-600 text-sm">
                          <XCircle className="w-4 h-4" />
                          {entry.status}
                        </span>
                      )}
                    </td>
                    <td className="px-3 py-2 text-sm text-gray-600">
                      {entry.completed_at ? new Date(entry.completed_at).toLocaleString() : 'N/A'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="text-gray-500">No ingestion history available</p>
        )}
      </div>

      {/* Refresh Note */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <div className="flex items-start gap-3">
          <RefreshCw className="w-5 h-5 text-blue-600 mt-0.5" />
          <div>
            <h3 className="text-sm font-medium text-blue-800">Data Refresh</h3>
            <p className="text-sm text-blue-700 mt-1">
              To refresh data from FDA, run: <code className="bg-blue-100 px-1 rounded">python scripts/weekly_refresh.py</code>
            </p>
          </div>
        </div>
      </div>
    </div>
  )
}
