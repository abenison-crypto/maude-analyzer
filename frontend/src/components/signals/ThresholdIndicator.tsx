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

  const suffix = config.isPercentage ? '%' : ''

  return (
    <div className="w-full pt-4">
      {/* Current value label above marker */}
      <div className="relative h-0">
        <div
          className="absolute -top-4 text-[9px] font-semibold text-gray-700 transform -translate-x-1/2 whitespace-nowrap"
          style={{ left: `${position}%` }}
        >
          {value.toFixed(1)}{suffix}
        </div>
      </div>

      {/* Gauge bar with threshold markers */}
      <div className="relative h-1.5 rounded-full overflow-hidden flex">
        <div className="bg-gray-200" style={{ width: `${normalWidth}%` }} />
        <div className="bg-yellow-200" style={{ width: `${elevatedWidth}%` }} />
        <div className="bg-red-200" style={{ width: `${highWidth}%` }} />

        {/* Threshold boundary markers */}
        <div
          className="absolute top-0 h-full w-0.5 bg-yellow-500"
          style={{ left: `${normalWidth}%` }}
        />
        <div
          className="absolute top-0 h-full w-0.5 bg-red-500"
          style={{ left: `${normalWidth + elevatedWidth}%` }}
        />
      </div>

      {/* Marker */}
      <div className="relative h-0">
        <div
          className={`absolute -top-1.5 w-1.5 h-3 rounded-sm ${markerColors[signalStrength]} transform -translate-x-1/2`}
          style={{ left: `${position}%` }}
        />
      </div>

      {/* Labels positioned at actual zone boundaries */}
      <div className="relative text-[9px] text-gray-400 mt-1 h-3">
        <span className="absolute left-0">{config.min}{suffix}</span>
        <span
          className="absolute transform -translate-x-1/2"
          style={{ left: `${normalWidth}%` }}
        >
          {config.elevatedAt}{suffix}
        </span>
        <span
          className="absolute transform -translate-x-1/2"
          style={{ left: `${normalWidth + elevatedWidth}%` }}
        >
          {config.highAt}{suffix}
        </span>
        <span className="absolute right-0">{config.max}{suffix}</span>
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

        {/* Threshold boundary markers */}
        <div
          className="absolute top-0 h-full w-px bg-yellow-500"
          style={{ left: `${normalWidth}%` }}
        />
        <div
          className="absolute top-0 h-full w-px bg-red-500"
          style={{ left: `${normalWidth + elevatedWidth}%` }}
        />
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
