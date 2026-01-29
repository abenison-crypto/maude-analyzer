import type { SignalMethod, SignalStrength } from '../../types/signals'

interface ThresholdIndicatorProps {
  method: SignalMethod
  value: number
  signalStrength: SignalStrength
}

interface ThresholdConfig {
  min: number
  elevatedAt: number
  highAt: number
  max: number
  isPercentage?: boolean
}

const THRESHOLD_CONFIGS: Record<SignalMethod, ThresholdConfig> = {
  zscore: { min: -1, elevatedAt: 1, highAt: 2, max: 5 },
  prr: { min: 0, elevatedAt: 2, highAt: 3, max: 6 },
  ror: { min: 0, elevatedAt: 2, highAt: 3, max: 6 },
  ebgm: { min: 0, elevatedAt: 2, highAt: 3, max: 6 },
  cusum: { min: 0, elevatedAt: 3, highAt: 5, max: 8 },
  yoy: { min: -50, elevatedAt: 50, highAt: 100, max: 300, isPercentage: true },
  pop: { min: -50, elevatedAt: 50, highAt: 100, max: 300, isPercentage: true },
  rolling: { min: -1, elevatedAt: 1, highAt: 2, max: 5 },
}

export default function ThresholdIndicator({
  method,
  value,
  signalStrength,
}: ThresholdIndicatorProps) {
  const config = THRESHOLD_CONFIGS[method]

  // Calculate position as percentage
  const range = config.max - config.min
  const clampedValue = Math.min(Math.max(value, config.min), config.max)
  const position = ((clampedValue - config.min) / range) * 100

  // Calculate zone widths
  const normalWidth = ((config.elevatedAt - config.min) / range) * 100
  const elevatedWidth = ((config.highAt - config.elevatedAt) / range) * 100
  const highWidth = 100 - normalWidth - elevatedWidth

  // Marker color based on signal strength
  const markerColors: Record<SignalStrength, string> = {
    normal: 'bg-gray-700',
    elevated: 'bg-yellow-600',
    high: 'bg-red-600',
  }

  return (
    <div className="w-full">
      {/* Gauge bar */}
      <div className="relative h-1.5 rounded-full overflow-hidden flex">
        <div className="bg-gray-200" style={{ width: `${normalWidth}%` }} />
        <div className="bg-yellow-200" style={{ width: `${elevatedWidth}%` }} />
        <div className="bg-red-200" style={{ width: `${highWidth}%` }} />
      </div>

      {/* Marker */}
      <div className="relative h-0">
        <div
          className={`absolute -top-1.5 w-1.5 h-3 rounded-sm ${markerColors[signalStrength]} transform -translate-x-1/2`}
          style={{ left: `${position}%` }}
        />
      </div>

      {/* Labels */}
      <div className="flex justify-between text-[9px] text-gray-400 mt-1">
        <span>{config.min}{config.isPercentage ? '%' : ''}</span>
        <span>{config.elevatedAt}{config.isPercentage ? '%' : ''}</span>
        <span>{config.highAt}{config.isPercentage ? '%' : ''}</span>
      </div>
    </div>
  )
}

/**
 * Compact inline version for use in table cells
 */
export function ThresholdIndicatorCompact({
  method,
  value,
  signalStrength,
}: ThresholdIndicatorProps) {
  const config = THRESHOLD_CONFIGS[method]

  const range = config.max - config.min
  const clampedValue = Math.min(Math.max(value, config.min), config.max)
  const position = ((clampedValue - config.min) / range) * 100

  const normalWidth = ((config.elevatedAt - config.min) / range) * 100
  const elevatedWidth = ((config.highAt - config.elevatedAt) / range) * 100
  const highWidth = 100 - normalWidth - elevatedWidth

  const markerColors: Record<SignalStrength, string> = {
    normal: 'bg-gray-600',
    elevated: 'bg-yellow-500',
    high: 'bg-red-500',
  }

  return (
    <div className="w-16 inline-block align-middle">
      <div className="relative h-1 rounded-full overflow-hidden flex">
        <div className="bg-gray-200" style={{ width: `${normalWidth}%` }} />
        <div className="bg-yellow-200" style={{ width: `${elevatedWidth}%` }} />
        <div className="bg-red-200" style={{ width: `${highWidth}%` }} />
      </div>
      <div className="relative h-0">
        <div
          className={`absolute -top-1 w-1 h-2 rounded-sm ${markerColors[signalStrength]} transform -translate-x-1/2`}
          style={{ left: `${position}%` }}
        />
      </div>
    </div>
  )
}
