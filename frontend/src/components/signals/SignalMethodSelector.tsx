import { SIGNAL_METHODS, type SignalMethod } from '../../types/signals'
import MethodInfoTooltip from './MethodInfoTooltip'

interface SignalMethodSelectorProps {
  selectedMethods: SignalMethod[]
  onChange: (methods: SignalMethod[]) => void
}

export default function SignalMethodSelector({ selectedMethods, onChange }: SignalMethodSelectorProps) {
  const timeMethods = SIGNAL_METHODS.filter((m) => m.category === 'time')
  const disproportionalityMethods = SIGNAL_METHODS.filter((m) => m.category === 'disproportionality')

  const toggleMethod = (method: SignalMethod) => {
    if (selectedMethods.includes(method)) {
      // Don't allow removing all methods
      if (selectedMethods.length > 1) {
        onChange(selectedMethods.filter((m) => m !== method))
      }
    } else {
      onChange([...selectedMethods, method])
    }
  }

  const selectAll = (category: 'time' | 'disproportionality') => {
    const categoryMethods = SIGNAL_METHODS.filter((m) => m.category === category).map((m) => m.id)
    const otherMethods = selectedMethods.filter(
      (m) => !categoryMethods.includes(m)
    )
    onChange([...otherMethods, ...categoryMethods])
  }

  const clearCategory = (category: 'time' | 'disproportionality') => {
    const categoryMethods = SIGNAL_METHODS.filter((m) => m.category === category).map((m) => m.id)
    const remaining = selectedMethods.filter((m) => !categoryMethods.includes(m))
    // Ensure at least one method remains
    if (remaining.length > 0) {
      onChange(remaining)
    }
  }

  return (
    <div className="space-y-4">
      {/* Time-Based Methods */}
      <div>
        <div className="flex items-center justify-between mb-2">
          <h4 className="text-sm font-medium text-gray-700">Time-Based Methods</h4>
          <div className="space-x-2">
            <button
              onClick={() => selectAll('time')}
              className="text-xs text-blue-600 hover:text-blue-800"
            >
              Select All
            </button>
            <button
              onClick={() => clearCategory('time')}
              className="text-xs text-gray-500 hover:text-gray-700"
            >
              Clear
            </button>
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          {timeMethods.map((method) => (
            <div key={method.id} className="flex items-center gap-1">
              <button
                onClick={() => toggleMethod(method.id)}
                className={`px-3 py-1.5 rounded-full text-sm font-medium transition-colors ${
                  selectedMethods.includes(method.id)
                    ? 'bg-blue-100 text-blue-800 border-2 border-blue-300'
                    : 'bg-gray-100 text-gray-600 border-2 border-transparent hover:bg-gray-200'
                }`}
                title={method.description}
              >
                {method.label}
              </button>
              <MethodInfoTooltip method={method.id} compact />
            </div>
          ))}
        </div>
      </div>

      {/* Disproportionality Methods */}
      <div>
        <div className="flex items-center justify-between mb-2">
          <h4 className="text-sm font-medium text-gray-700">Disproportionality Methods</h4>
          <div className="space-x-2">
            <button
              onClick={() => selectAll('disproportionality')}
              className="text-xs text-blue-600 hover:text-blue-800"
            >
              Select All
            </button>
            <button
              onClick={() => clearCategory('disproportionality')}
              className="text-xs text-gray-500 hover:text-gray-700"
            >
              Clear
            </button>
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          {disproportionalityMethods.map((method) => (
            <div key={method.id} className="flex items-center gap-1">
              <button
                onClick={() => toggleMethod(method.id)}
                className={`px-3 py-1.5 rounded-full text-sm font-medium transition-colors ${
                  selectedMethods.includes(method.id)
                    ? 'bg-purple-100 text-purple-800 border-2 border-purple-300'
                    : 'bg-gray-100 text-gray-600 border-2 border-transparent hover:bg-gray-200'
                }`}
                title={method.description}
              >
                {method.label}
              </button>
              <MethodInfoTooltip method={method.id} compact />
            </div>
          ))}
        </div>
      </div>

      {/* Selected Methods Summary */}
      <div className="text-xs text-gray-500">
        {selectedMethods.length} method{selectedMethods.length !== 1 ? 's' : ''} selected
      </div>
    </div>
  )
}
