import { useState } from 'react'
import { HelpCircle, ChevronDown, ChevronRight, Lightbulb, BookOpen, Target } from 'lucide-react'
import type { SignalMethod } from '../../types/signals'
import { METHOD_DOCS, THRESHOLD_REFERENCE, METHOD_SELECTION_GUIDE } from '../../constants/signalDocumentation'

interface SignalHelpPanelProps {
  isOpen: boolean
  onToggle: () => void
}

type HelpSection = 'guide' | 'methods' | 'thresholds'

export default function SignalHelpPanel({ isOpen, onToggle }: SignalHelpPanelProps) {
  const [activeSection, setActiveSection] = useState<HelpSection>('guide')
  const [expandedMethod, setExpandedMethod] = useState<SignalMethod | null>(null)

  return (
    <div className="bg-white rounded-lg shadow border border-gray-200">
      {/* Header */}
      <button
        onClick={onToggle}
        className="w-full flex items-center justify-between p-4 hover:bg-gray-50 transition-colors"
      >
        <div className="flex items-center gap-2">
          <HelpCircle className="w-5 h-5 text-blue-500" />
          <span className="font-medium text-gray-900">Signal Detection Help</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-sm text-gray-500">
            {isOpen ? 'Hide' : 'Show'} documentation
          </span>
          {isOpen ? (
            <ChevronDown className="w-5 h-5 text-gray-400" />
          ) : (
            <ChevronRight className="w-5 h-5 text-gray-400" />
          )}
        </div>
      </button>

      {/* Content */}
      {isOpen && (
        <div className="border-t border-gray-200">
          {/* Section tabs */}
          <div className="flex border-b border-gray-200 px-4">
            <button
              onClick={() => setActiveSection('guide')}
              className={`flex items-center gap-1.5 px-3 py-2 text-sm font-medium border-b-2 -mb-px transition-colors ${
                activeSection === 'guide'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              <Lightbulb className="w-4 h-4" />
              Selection Guide
            </button>
            <button
              onClick={() => setActiveSection('methods')}
              className={`flex items-center gap-1.5 px-3 py-2 text-sm font-medium border-b-2 -mb-px transition-colors ${
                activeSection === 'methods'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              <BookOpen className="w-4 h-4" />
              Method Details
            </button>
            <button
              onClick={() => setActiveSection('thresholds')}
              className={`flex items-center gap-1.5 px-3 py-2 text-sm font-medium border-b-2 -mb-px transition-colors ${
                activeSection === 'thresholds'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              <Target className="w-4 h-4" />
              Thresholds
            </button>
          </div>

          {/* Section content */}
          <div className="p-4 max-h-96 overflow-y-auto">
            {activeSection === 'guide' && <SelectionGuide />}
            {activeSection === 'methods' && (
              <MethodDetails
                expandedMethod={expandedMethod}
                onToggleMethod={(m) => setExpandedMethod(expandedMethod === m ? null : m)}
              />
            )}
            {activeSection === 'thresholds' && <ThresholdReference />}
          </div>
        </div>
      )}
    </div>
  )
}

function SelectionGuide() {
  return (
    <div className="space-y-6">
      {/* Quick recommendations */}
      <div>
        <h4 className="text-sm font-semibold text-gray-900 mb-3">Quick Recommendations</h4>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          {Object.entries(METHOD_SELECTION_GUIDE.quickRecommendations).map(([key, rec]) => (
            <div
              key={key}
              className="p-3 bg-gray-50 rounded-lg border border-gray-200"
            >
              <div className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">
                {key === 'initial' ? 'Getting Started' : key === 'comprehensive' ? 'Full Analysis' : 'FDA Style'}
              </div>
              <div className="flex flex-wrap gap-1 mb-2">
                {rec.methods.map((m) => (
                  <span
                    key={m}
                    className="px-2 py-0.5 bg-blue-100 text-blue-800 text-xs rounded"
                  >
                    {METHOD_DOCS[m].name}
                  </span>
                ))}
              </div>
              <p className="text-xs text-gray-600">{rec.reason}</p>
            </div>
          ))}
        </div>
      </div>

      {/* Decision flowchart */}
      <div>
        <h4 className="text-sm font-semibold text-gray-900 mb-3">Which Method Should I Use?</h4>
        <div className="space-y-4">
          {METHOD_SELECTION_GUIDE.questions.map((q, qi) => (
            <div key={qi} className="bg-blue-50 rounded-lg p-3">
              <div className="text-sm font-medium text-blue-900 mb-2">{q.question}</div>
              <div className="space-y-2">
                {q.options.map((opt, oi) => (
                  <div
                    key={oi}
                    className="flex items-start gap-2 text-xs"
                  >
                    <ChevronRight className="w-4 h-4 text-blue-500 flex-shrink-0 mt-0.5" />
                    <div>
                      <span className="font-medium text-blue-800">{opt.answer}</span>
                      <span className="text-blue-700"> - Use </span>
                      <span className="font-medium text-blue-800">
                        {opt.methods.map((m) => METHOD_DOCS[m].name).join(', ')}
                      </span>
                      <p className="text-blue-600 mt-0.5">{opt.explanation}</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

function MethodDetails({
  expandedMethod,
  onToggleMethod,
}: {
  expandedMethod: SignalMethod | null
  onToggleMethod: (method: SignalMethod) => void
}) {
  const methods = Object.keys(METHOD_DOCS) as SignalMethod[]

  return (
    <div className="space-y-2">
      {methods.map((method) => {
        const doc = METHOD_DOCS[method]
        const isExpanded = expandedMethod === method

        return (
          <div key={method} className="border border-gray-200 rounded-lg overflow-hidden">
            <button
              onClick={() => onToggleMethod(method)}
              className="w-full flex items-center justify-between p-3 hover:bg-gray-50 text-left"
            >
              <div>
                <span className="font-medium text-gray-900">{doc.name}</span>
                <span className="text-gray-500 text-sm ml-2">- {doc.shortDescription}</span>
              </div>
              {isExpanded ? (
                <ChevronDown className="w-5 h-5 text-gray-400" />
              ) : (
                <ChevronRight className="w-5 h-5 text-gray-400" />
              )}
            </button>

            {isExpanded && (
              <div className="p-4 bg-gray-50 border-t border-gray-200 space-y-3 text-sm">
                <div>
                  <h5 className="font-medium text-gray-700">When to Use</h5>
                  <p className="text-gray-600 mt-1">{doc.whenToUse}</p>
                </div>

                <div>
                  <h5 className="font-medium text-gray-700">How It Works</h5>
                  <p className="text-gray-600 mt-1">{doc.howItWorks}</p>
                </div>

                <div>
                  <h5 className="font-medium text-gray-700">Interpretation</h5>
                  <p className="text-gray-600 mt-1">{doc.interpretation}</p>
                </div>

                <div className="flex gap-4 pt-2 border-t border-gray-200">
                  <div>
                    <span className="text-xs font-medium text-gray-500">RECOMMENDED LOOKBACK</span>
                    <p className="text-gray-700">{doc.recommendedLookback}</p>
                  </div>
                </div>

                <div className="text-xs text-gray-500 italic">
                  <strong>Limitations:</strong> {doc.limitations}
                </div>
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}

function ThresholdReference() {
  return (
    <div>
      <p className="text-sm text-gray-600 mb-4">
        Signals are classified based on these thresholds. Methods with confidence intervals (PRR, ROR, EBGM)
        also require the lower bound to be significant.
      </p>

      <div className="overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200">
              <th className="text-left py-2 px-3 font-medium text-gray-700">Method</th>
              <th className="text-left py-2 px-3 font-medium text-gray-700">Metric</th>
              <th className="text-center py-2 px-3">
                <span className="inline-flex items-center gap-1">
                  <span className="w-2 h-2 bg-red-500 rounded-full" />
                  <span className="font-medium text-red-700">High</span>
                </span>
              </th>
              <th className="text-center py-2 px-3">
                <span className="inline-flex items-center gap-1">
                  <span className="w-2 h-2 bg-yellow-500 rounded-full" />
                  <span className="font-medium text-yellow-700">Elevated</span>
                </span>
              </th>
            </tr>
          </thead>
          <tbody>
            {THRESHOLD_REFERENCE.map((row) => (
              <tr key={row.method} className="border-b border-gray-100">
                <td className="py-2 px-3 font-medium text-gray-900">
                  {METHOD_DOCS[row.method].name}
                </td>
                <td className="py-2 px-3 text-gray-600">{row.metric}</td>
                <td className="py-2 px-3 text-center text-red-700 font-mono text-xs">
                  {row.highCondition}
                </td>
                <td className="py-2 px-3 text-center text-yellow-700 font-mono text-xs">
                  {row.elevatedCondition}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="mt-4 p-3 bg-gray-50 rounded text-xs text-gray-600">
        <strong>Note:</strong> For PRR, ROR, and EBGM, signals also require the lower confidence interval
        (or EB05 for EBGM) to be &gt;= 1.0, ensuring statistical significance. Additionally, PRR/ROR require
        at least 3 target events.
      </div>
    </div>
  )
}
