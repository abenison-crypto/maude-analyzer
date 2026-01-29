/**
 * EventTypeFilter Component
 *
 * Toggle buttons for event type selection
 */

import { EVENT_TYPE_FILTER_OPTIONS, EVENT_TYPE_COLORS } from '../../../constants/filterRegistry'

interface EventTypeFilterProps {
  label: string
  values: string[]
  onChange: (values: string[]) => void
}

export default function EventTypeFilter({
  label,
  values,
  onChange,
}: EventTypeFilterProps) {
  const toggleType = (type: string) => {
    if (values.includes(type)) {
      onChange(values.filter((t) => t !== type))
    } else {
      onChange([...values, type])
    }
  }

  return (
    <div>
      <label className="block text-sm font-medium text-gray-700 mb-1">{label}</label>
      <div className="flex flex-wrap gap-2">
        {EVENT_TYPE_FILTER_OPTIONS.map((type) => {
          const isSelected = values.includes(type.value)
          const colors = EVENT_TYPE_COLORS[type.value]

          return (
            <button
              key={type.value}
              type="button"
              onClick={() => toggleType(type.value)}
              className={`px-3 py-1.5 rounded-md text-sm font-medium transition-colors border ${
                isSelected
                  ? `${colors.bg} ${colors.text} ${colors.border}`
                  : 'bg-gray-50 text-gray-600 border-gray-200 hover:bg-gray-100'
              }`}
            >
              {type.label}
            </button>
          )
        })}
      </div>
    </div>
  )
}
