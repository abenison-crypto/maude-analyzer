import { useState } from 'react'
import { ChevronLeft, ChevronRight, ExternalLink } from 'lucide-react'
import { useEvents, useEventDetail } from '../hooks/useEvents'
import { OUTCOME_BADGES, getEventTypeDisplay } from '../constants/schema'

interface EventDetailModalProps {
  mdrReportKey: string
  onClose: () => void
}

function EventDetailModal({ mdrReportKey, onClose }: EventDetailModalProps) {
  const { data: event, isLoading } = useEventDetail(mdrReportKey)

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
      <div className="bg-white rounded-lg shadow-xl max-w-4xl w-full max-h-[90vh] overflow-hidden">
        <div className="p-4 border-b flex justify-between items-center">
          <h2 className="text-lg font-semibold">Event Details</h2>
          <button onClick={onClose} className="text-gray-500 hover:text-gray-700">
            &times;
          </button>
        </div>
        <div className="p-4 overflow-y-auto max-h-[calc(90vh-120px)]">
          {isLoading ? (
            <div className="animate-pulse space-y-4">
              <div className="h-4 bg-gray-200 rounded w-3/4" />
              <div className="h-4 bg-gray-200 rounded w-1/2" />
            </div>
          ) : event ? (
            <div className="space-y-6">
              {/* Basic Info */}
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="text-sm font-medium text-gray-500">Report Number</label>
                  <p className="text-gray-900">{event.report_number || 'N/A'}</p>
                </div>
                <div>
                  <label className="text-sm font-medium text-gray-500">MDR Report Key</label>
                  <p className="text-gray-900">{event.mdr_report_key}</p>
                </div>
                <div>
                  <label className="text-sm font-medium text-gray-500">Date Received</label>
                  <p className="text-gray-900">{event.date_received || 'N/A'}</p>
                </div>
                <div>
                  <label className="text-sm font-medium text-gray-500">Date of Event</label>
                  <p className="text-gray-900">{event.date_of_event || 'N/A'}</p>
                </div>
                <div>
                  <label className="text-sm font-medium text-gray-500">Manufacturer</label>
                  <p className="text-gray-900">{event.manufacturer || event.manufacturer_name || 'N/A'}</p>
                </div>
                <div>
                  <label className="text-sm font-medium text-gray-500">Product Code</label>
                  <p className="text-gray-900">{event.product_code || 'N/A'}</p>
                </div>
              </div>

              {/* Devices */}
              {event.devices && event.devices.length > 0 && (
                <div>
                  <h3 className="text-sm font-medium text-gray-500 mb-2">Devices</h3>
                  <div className="space-y-2">
                    {event.devices.map((device, i) => (
                      <div key={i} className="bg-gray-50 p-3 rounded-lg">
                        <p className="font-medium">{device.brand_name || 'Unknown Device'}</p>
                        <p className="text-sm text-gray-600">{device.generic_name}</p>
                        {device.model_number && (
                          <p className="text-sm text-gray-500">Model: {device.model_number}</p>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Narratives */}
              {event.narratives && event.narratives.length > 0 && (
                <div>
                  <h3 className="text-sm font-medium text-gray-500 mb-2">Event Narratives</h3>
                  <div className="space-y-3">
                    {event.narratives.map((narrative, i) => (
                      <div key={i} className="bg-gray-50 p-3 rounded-lg">
                        <p className="text-xs font-medium text-gray-500 mb-1">
                          {narrative.type === 'D' ? 'Description' : narrative.type}
                        </p>
                        <p className="text-sm text-gray-700 whitespace-pre-wrap">{narrative.text}</p>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Patients */}
              {event.patients && event.patients.length > 0 && (
                <div>
                  <h3 className="text-sm font-medium text-gray-500 mb-2">Patient Information</h3>
                  <div className="space-y-2">
                    {event.patients.map((patient, i) => (
                      <div key={i} className="bg-gray-50 p-3 rounded-lg">
                        <p className="text-sm">
                          Patient {patient.sequence || i + 1}: {patient.sex || 'Unknown'}, Age: {patient.age || 'Unknown'}
                        </p>
                        <div className="flex gap-2 mt-1">
                          {OUTCOME_BADGES.map(
                            (badge) =>
                              patient.outcomes[badge.key] && (
                                <span
                                  key={badge.key}
                                  className={`text-xs px-2 py-1 ${badge.colorClass} rounded`}
                                >
                                  {badge.label}
                                </span>
                              )
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          ) : (
            <p className="text-gray-500">Event not found</p>
          )}
        </div>
      </div>
    </div>
  )
}

export default function EventsTable() {
  const [page, setPage] = useState(1)
  const [pageSize] = useState(25)
  const [selectedEvent, setSelectedEvent] = useState<string | null>(null)

  const { data, isLoading } = useEvents(page, pageSize)

  const events = data?.events || []
  const pagination = data?.pagination

  return (
    <div className="bg-white rounded-lg shadow">
      {/* Header */}
      <div className="p-4 border-b">
        <div className="flex items-center justify-between">
          <h2 className="text-lg font-semibold text-gray-900">Events</h2>
          {pagination && (
            <span className="text-sm text-gray-500">
              Showing {((page - 1) * pageSize + 1).toLocaleString()} - {Math.min(page * pageSize, pagination.total).toLocaleString()} of {pagination.total.toLocaleString()}
            </span>
          )}
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Report #
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Date
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Type
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Manufacturer
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Product Code
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {isLoading ? (
              Array.from({ length: 5 }).map((_, i) => (
                <tr key={i}>
                  {Array.from({ length: 6 }).map((_, j) => (
                    <td key={j} className="px-4 py-3">
                      <div className="h-4 bg-gray-200 animate-pulse rounded" />
                    </td>
                  ))}
                </tr>
              ))
            ) : events.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-4 py-8 text-center text-gray-500">
                  No events found matching your filters
                </td>
              </tr>
            ) : (
              events.map((event) => {
                const typeInfo = getEventTypeDisplay(event.event_type || '')
                return (
                  <tr key={event.mdr_report_key} className="hover:bg-gray-50">
                    <td className="px-4 py-3 text-sm text-gray-900">{event.report_number || event.mdr_report_key}</td>
                    <td className="px-4 py-3 text-sm text-gray-600">{event.date_received || 'N/A'}</td>
                    <td className="px-4 py-3">
                      <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${typeInfo.color}`}>
                        {typeInfo.label}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600 max-w-xs truncate">
                      {event.manufacturer || 'N/A'}
                    </td>
                    <td className="px-4 py-3 text-sm text-gray-600">{event.product_code || 'N/A'}</td>
                    <td className="px-4 py-3">
                      <button
                        onClick={() => setSelectedEvent(event.mdr_report_key)}
                        className="text-blue-600 hover:text-blue-800"
                      >
                        <ExternalLink className="w-4 h-4" />
                      </button>
                    </td>
                  </tr>
                )
              })
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {pagination && pagination.total_pages > 1 && (
        <div className="px-4 py-3 border-t flex items-center justify-between">
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page === 1}
            className="flex items-center gap-1 px-3 py-1 rounded-md bg-gray-100 text-gray-700 disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-200"
          >
            <ChevronLeft className="w-4 h-4" />
            Previous
          </button>
          <span className="text-sm text-gray-600">
            Page {page} of {pagination.total_pages.toLocaleString()}
          </span>
          <button
            onClick={() => setPage((p) => Math.min(pagination.total_pages, p + 1))}
            disabled={page === pagination.total_pages}
            className="flex items-center gap-1 px-3 py-1 rounded-md bg-gray-100 text-gray-700 disabled:opacity-50 disabled:cursor-not-allowed hover:bg-gray-200"
          >
            Next
            <ChevronRight className="w-4 h-4" />
          </button>
        </div>
      )}

      {/* Detail Modal */}
      {selectedEvent && (
        <EventDetailModal mdrReportKey={selectedEvent} onClose={() => setSelectedEvent(null)} />
      )}
    </div>
  )
}
