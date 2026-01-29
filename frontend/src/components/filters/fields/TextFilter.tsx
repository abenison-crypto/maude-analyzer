/**
 * TextFilter Component
 *
 * Text search input with optional icon
 */

import { Search, X } from 'lucide-react'

interface TextFilterProps {
  label: string
  placeholder?: string
  value: string
  onChange: (value: string) => void
  showSearchIcon?: boolean
}

export default function TextFilter({
  label,
  placeholder = 'Search...',
  value,
  onChange,
  showSearchIcon = true,
}: TextFilterProps) {
  return (
    <div>
      <label className="block text-sm font-medium text-gray-700 mb-1">{label}</label>
      <div className="relative">
        {showSearchIcon && (
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
        )}
        <input
          type="text"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
          className={`w-full py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm ${
            showSearchIcon ? 'pl-9 pr-8' : 'px-3 pr-8'
          }`}
        />
        {value && (
          <button
            type="button"
            onClick={() => onChange('')}
            className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
          >
            <X className="w-4 h-4" />
          </button>
        )}
      </div>
    </div>
  )
}
