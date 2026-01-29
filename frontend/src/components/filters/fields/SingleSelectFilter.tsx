/**
 * SingleSelectFilter Component
 *
 * Single-select dropdown for options like Y/N/Any
 */

import { ChevronDown } from 'lucide-react'

interface Option {
  value: string
  label: string
}

interface SingleSelectFilterProps {
  label: string
  value: string
  onChange: (value: string) => void
  options: Option[]
}

export default function SingleSelectFilter({
  label,
  value,
  onChange,
  options,
}: SingleSelectFilterProps) {
  return (
    <div>
      <label className="block text-sm font-medium text-gray-700 mb-1">{label}</label>
      <div className="relative">
        <select
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className="w-full px-3 py-2 pr-8 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm appearance-none bg-white"
        >
          {options.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
        <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 pointer-events-none" />
      </div>
    </div>
  )
}
