import { AlertTriangle, X, Info } from 'lucide-react'
import { useState } from 'react'
import type { DataCompleteness } from '../../types/signals'

interface DataLagWarningProps {
  dataCompleteness?: DataCompleteness | null
  analysisEndDate?: string
  onIncludeRecent?: () => void
}

export default function DataLagWarning({
  dataCompleteness,
  analysisEndDate,
  onIncludeRecent,
}: DataLagWarningProps) {
  const [isDismissed, setIsDismissed] = useState(false)

  if (isDismissed) return null

  // If no completeness info, show a general warning about data lag
  if (!dataCompleteness) {
    return (
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-3 flex items-start gap-3">
        <Info className="w-5 h-5 text-blue-500 flex-shrink-0 mt-0.5" />
        <div className="flex-1">
          <p className="text-sm text-blue-800">
            <strong>Note:</strong> FDA MAUDE data typically has a 2-3 month reporting lag. Recent
            months may have incomplete data.
          </p>
        </div>
        <button
          onClick={() => setIsDismissed(true)}
          className="text-blue-400 hover:text-blue-600"
          aria-label="Dismiss"
        >
          <X className="w-4 h-4" />
        </button>
      </div>
    )
  }

  const { last_complete_month, incomplete_months, estimated_lag_months } = dataCompleteness

  // If analysis includes incomplete months, show warning
  const hasIncompleteData =
    incomplete_months.length > 0 && analysisEndDate && incomplete_months.some((m) => m <= analysisEndDate)

  if (!hasIncompleteData) {
    return null
  }

  return (
    <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-3 flex items-start gap-3">
      <AlertTriangle className="w-5 h-5 text-yellow-600 flex-shrink-0 mt-0.5" />
      <div className="flex-1">
        <p className="text-sm text-yellow-800 font-medium">Data Completeness Warning</p>
        <p className="text-sm text-yellow-700 mt-1">
          The last {estimated_lag_months} months may have incomplete data due to reporting delays.
          Your analysis ends at{' '}
          <strong>{formatMonth(analysisEndDate || last_complete_month)}</strong>.
          {incomplete_months.length > 0 && (
            <>
              {' '}
              Potentially incomplete:{' '}
              <span className="font-medium">{incomplete_months.map(formatMonth).join(', ')}</span>
            </>
          )}
        </p>
        <div className="mt-2 flex gap-2">
          {onIncludeRecent && (
            <button
              onClick={onIncludeRecent}
              className="text-xs px-2 py-1 bg-yellow-100 hover:bg-yellow-200 text-yellow-800 rounded transition-colors"
            >
              Include recent data anyway
            </button>
          )}
          <button
            onClick={() => setIsDismissed(true)}
            className="text-xs px-2 py-1 text-yellow-600 hover:text-yellow-800"
          >
            Dismiss
          </button>
        </div>
      </div>
    </div>
  )
}

function formatMonth(dateStr: string): string {
  try {
    const date = new Date(dateStr + '-01')
    return date.toLocaleDateString('en-US', { month: 'short', year: 'numeric' })
  } catch {
    return dateStr
  }
}

/**
 * Inline version for compact display
 */
export function DataLagBadge({ estimatedLagMonths = 2 }: { estimatedLagMonths?: number }) {
  return (
    <span
      className="inline-flex items-center gap-1 text-xs text-yellow-700 bg-yellow-100 px-2 py-0.5 rounded"
      title={`Data may be incomplete for the last ${estimatedLagMonths} months due to reporting delays`}
    >
      <AlertTriangle className="w-3 h-3" />
      ~{estimatedLagMonths}mo lag
    </span>
  )
}
