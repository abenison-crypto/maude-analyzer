import { useState } from 'react'
import type {
  SignalResult,
  SignalMethod,
  MethodResult,
  ZScoreDetails,
  PRRDetails,
  EBGMDetails,
  CUSUMDetails,
  YoYDetails,
  RollingDetails,
} from '../../types/signals'
import { METHOD_DOCS } from '../../constants/signalDocumentation'
import ZScoreChart from './charts/ZScoreChart'
import ContingencyChart from './charts/ContingencyChart'
import EBGMChart from './charts/EBGMChart'
import CUSUMChart from './charts/CUSUMChart'
import YoYChart from './charts/YoYChart'
import RollingChart from './charts/RollingChart'
import ThresholdIndicator from './ThresholdIndicator'

interface SignalRowDetailProps {
  signal: SignalResult
  methods: SignalMethod[]
}

const METHOD_LABELS: Record<SignalMethod, string> = {
  zscore: 'Z-Score',
  prr: 'PRR',
  ror: 'ROR',
  ebgm: 'EBGM',
  cusum: 'CUSUM',
  yoy: 'Year-over-Year',
  pop: 'Period-over-Period',
  rolling: 'Rolling Average',
}

export default function SignalRowDetail({ signal, methods }: SignalRowDetailProps) {
  // Filter to methods that have results with values
  const availableMethods = methods.filter((method) => {
    const result = signal.method_results.find((r) => r.method === method)
    return result?.value != null
  })

  const [activeMethod, setActiveMethod] = useState<SignalMethod>(
    availableMethods[0] || methods[0]
  )

  const activeResult = signal.method_results.find((r) => r.method === activeMethod)

  if (availableMethods.length === 0) {
    return (
      <div className="text-sm text-gray-500 py-4 text-center">
        No detailed data available for the selected methods.
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Method tabs */}
      <div className="flex gap-1 border-b border-gray-200 pb-2">
        {availableMethods.map((method) => {
          const result = signal.method_results.find((r) => r.method === method)
          const isActive = method === activeMethod

          return (
            <button
              key={method}
              onClick={() => setActiveMethod(method)}
              className={`px-3 py-1.5 text-xs font-medium rounded-t transition-colors ${
                isActive
                  ? 'bg-white text-blue-600 border border-b-0 border-gray-200 -mb-[1px]'
                  : 'text-gray-500 hover:text-gray-700 hover:bg-gray-100'
              }`}
            >
              {METHOD_LABELS[method]}
              {result?.is_signal && (
                <span
                  className={`ml-1 inline-block w-2 h-2 rounded-full ${
                    result.signal_strength === 'high' ? 'bg-red-500' : 'bg-yellow-500'
                  }`}
                />
              )}
            </button>
          )
        })}
      </div>

      {/* Active method content */}
      {activeResult && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          {/* Chart visualization */}
          <div className="lg:col-span-2 bg-white rounded-lg border border-gray-200 p-4">
            {renderChart(activeMethod, activeResult)}
          </div>

          {/* Method explanation & threshold */}
          <div className="space-y-4">
            {/* Threshold indicator */}
            <div className="bg-white rounded-lg border border-gray-200 p-4">
              <h4 className="text-xs font-medium text-gray-700 uppercase tracking-wide mb-3">
                Threshold Position
              </h4>
              <ThresholdIndicator
                method={activeMethod}
                value={activeResult.value!}
                signalStrength={activeResult.signal_strength}
              />
            </div>

            {/* Classification explanation */}
            <div className="bg-white rounded-lg border border-gray-200 p-4">
              <h4 className="text-xs font-medium text-gray-700 uppercase tracking-wide mb-2">
                Why This Classification
              </h4>
              <div
                className={`text-sm p-2 rounded ${
                  activeResult.signal_strength === 'high'
                    ? 'bg-red-50 text-red-800'
                    : activeResult.signal_strength === 'elevated'
                    ? 'bg-yellow-50 text-yellow-800'
                    : 'bg-gray-50 text-gray-700'
                }`}
              >
                {getClassificationExplanation(activeMethod, activeResult)}
              </div>
            </div>

            {/* Method summary */}
            <div className="bg-gray-50 rounded-lg p-3 text-xs text-gray-600">
              <h4 className="font-medium text-gray-700 mb-1">
                About {METHOD_LABELS[activeMethod]}
              </h4>
              <p>{METHOD_DOCS[activeMethod].shortDescription}</p>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function renderChart(method: SignalMethod, result: MethodResult) {
  if (result.value === null || !result.details) {
    return (
      <div className="text-sm text-gray-500 py-8 text-center">
        No visualization data available for this method.
      </div>
    )
  }

  switch (method) {
    case 'zscore':
      return (
        <ZScoreChart
          details={result.details as ZScoreDetails}
          value={result.value}
          isSignal={result.is_signal}
        />
      )

    case 'prr':
      return (
        <ContingencyChart
          details={result.details as PRRDetails}
          method="prr"
          value={result.value}
          isSignal={result.is_signal}
        />
      )

    case 'ror':
      return (
        <ContingencyChart
          details={result.details as PRRDetails}
          method="ror"
          value={result.value}
          isSignal={result.is_signal}
        />
      )

    case 'ebgm':
      return (
        <EBGMChart
          details={result.details as EBGMDetails}
          value={result.value}
          lowerCi={result.lower_ci}
          isSignal={result.is_signal}
        />
      )

    case 'cusum':
      return (
        <CUSUMChart
          details={result.details as CUSUMDetails}
          value={result.value}
          isSignal={result.is_signal}
        />
      )

    case 'yoy':
      return (
        <YoYChart
          details={result.details as YoYDetails}
          value={result.value}
          isSignal={result.is_signal}
          method="yoy"
        />
      )

    case 'pop':
      return (
        <YoYChart
          details={result.details as YoYDetails}
          value={result.value}
          isSignal={result.is_signal}
          method="pop"
        />
      )

    case 'rolling':
      return (
        <RollingChart
          details={result.details as RollingDetails}
          value={result.value}
          isSignal={result.is_signal}
        />
      )

    default:
      return (
        <div className="text-sm text-gray-500 py-8 text-center">
          Visualization not available for this method.
        </div>
      )
  }
}

function getClassificationExplanation(method: SignalMethod, result: MethodResult): string {
  const doc = METHOD_DOCS[method]
  const value = result.value!
  const strength = result.signal_strength

  if (strength === 'normal') {
    return `Value of ${formatValue(method, value)} is within normal range. ${doc.thresholds.normal} is considered normal.`
  }

  const thresholdUsed = strength === 'high' ? doc.thresholds.high : doc.thresholds.elevated

  let explanation = `Value of ${formatValue(method, value)} exceeds the ${strength} threshold (${thresholdUsed}).`

  // Add CI context for disproportionality methods
  if (['prr', 'ror', 'ebgm'].includes(method) && result.lower_ci != null) {
    const ciLabel = method === 'ebgm' ? 'EB05' : 'Lower CI'
    explanation += ` The ${ciLabel} of ${result.lower_ci.toFixed(2)} ${
      result.lower_ci >= 1 ? 'confirms statistical significance (>= 1.0)' : 'suggests limited statistical confidence (< 1.0)'
    }.`
  }

  return explanation
}

function formatValue(method: SignalMethod, value: number): string {
  if (method === 'yoy' || method === 'pop') {
    return `${value > 0 ? '+' : ''}${value}%`
  }
  return value.toFixed(2)
}
