import { useState, useRef, useEffect } from 'react'
import { Info } from 'lucide-react'
import type { SignalMethod } from '../../types/signals'
import { METHOD_DOCS, THRESHOLD_REFERENCE } from '../../constants/signalDocumentation'

interface MethodInfoTooltipProps {
  method: SignalMethod
  compact?: boolean
}

export default function MethodInfoTooltip({ method, compact = false }: MethodInfoTooltipProps) {
  const [isOpen, setIsOpen] = useState(false)
  const tooltipRef = useRef<HTMLDivElement>(null)
  const buttonRef = useRef<HTMLButtonElement>(null)

  const doc = METHOD_DOCS[method]
  const threshold = THRESHOLD_REFERENCE.find((t) => t.method === method)

  // Close tooltip when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (
        tooltipRef.current &&
        !tooltipRef.current.contains(event.target as Node) &&
        buttonRef.current &&
        !buttonRef.current.contains(event.target as Node)
      ) {
        setIsOpen(false)
      }
    }

    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside)
      return () => document.removeEventListener('mousedown', handleClickOutside)
    }
  }, [isOpen])

  // Close on escape
  useEffect(() => {
    function handleEscape(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        setIsOpen(false)
      }
    }

    if (isOpen) {
      document.addEventListener('keydown', handleEscape)
      return () => document.removeEventListener('keydown', handleEscape)
    }
  }, [isOpen])

  return (
    <div className="relative inline-block">
      <button
        ref={buttonRef}
        onClick={(e) => {
          e.stopPropagation()
          setIsOpen(!isOpen)
        }}
        className="p-0.5 text-gray-400 hover:text-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500 rounded"
        aria-label={`Information about ${doc.name}`}
      >
        <Info className={compact ? 'w-3 h-3' : 'w-4 h-4'} />
      </button>

      {isOpen && (
        <div
          ref={tooltipRef}
          className="absolute z-50 w-80 bg-white rounded-lg shadow-lg border border-gray-200 p-4 text-left"
          style={{
            top: '100%',
            left: '50%',
            transform: 'translateX(-50%)',
            marginTop: '8px',
          }}
        >
          {/* Arrow */}
          <div
            className="absolute w-3 h-3 bg-white border-l border-t border-gray-200 transform -rotate-45"
            style={{
              top: '-7px',
              left: '50%',
              marginLeft: '-6px',
            }}
          />

          {/* Content */}
          <div className="space-y-3">
            <div>
              <h4 className="font-semibold text-gray-900">{doc.name}</h4>
              <p className="text-xs text-gray-500">{doc.shortDescription}</p>
            </div>

            <div>
              <h5 className="text-xs font-medium text-gray-700 uppercase tracking-wide">
                When to Use
              </h5>
              <p className="text-sm text-gray-600 mt-1">{doc.whenToUse}</p>
            </div>

            {threshold && (
              <div>
                <h5 className="text-xs font-medium text-gray-700 uppercase tracking-wide">
                  Thresholds
                </h5>
                <div className="mt-1 space-y-1 text-xs">
                  <div className="flex items-center gap-2">
                    <span className="inline-block w-2 h-2 bg-red-500 rounded-full" />
                    <span className="text-gray-700">
                      <strong>High:</strong> {threshold.highCondition}
                    </span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="inline-block w-2 h-2 bg-yellow-500 rounded-full" />
                    <span className="text-gray-700">
                      <strong>Elevated:</strong> {threshold.elevatedCondition}
                    </span>
                  </div>
                </div>
              </div>
            )}

            <div>
              <h5 className="text-xs font-medium text-gray-700 uppercase tracking-wide">
                Interpretation
              </h5>
              <p className="text-sm text-gray-600 mt-1">{doc.interpretation}</p>
            </div>

            <div className="pt-2 border-t border-gray-100">
              <p className="text-xs text-gray-500">
                <strong>Recommended lookback:</strong> {doc.recommendedLookback}
              </p>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

/**
 * Inline version for use in table headers
 */
export function MethodInfoIcon({ method }: { method: SignalMethod }) {
  return <MethodInfoTooltip method={method} compact />
}
