/**
 * MultiSelectFilter Component
 *
 * Multi-select filter field with:
 * - Debounced autocomplete search
 * - Removable value chips
 * - Dropdown with search results
 */

import { useState, useRef, useEffect, useCallback } from 'react'
import { X, ChevronDown, Loader2 } from 'lucide-react'
import type { AutocompleteItem } from '../../../api/client'

interface MultiSelectFilterProps {
  label: string
  placeholder?: string
  values: string[]
  onChange: (values: string[]) => void
  fetchOptions: (search: string) => Promise<AutocompleteItem[]>
  debounceMs?: number
}

export default function MultiSelectFilter({
  label,
  placeholder = 'Search...',
  values,
  onChange,
  fetchOptions,
  debounceMs = 300,
}: MultiSelectFilterProps) {
  const [search, setSearch] = useState('')
  const [options, setOptions] = useState<AutocompleteItem[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [isOpen, setIsOpen] = useState(false)
  const containerRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)
  const debounceRef = useRef<NodeJS.Timeout>()

  // Close dropdown when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
        setIsOpen(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  // Debounced search
  const handleSearch = useCallback(async (term: string) => {
    if (debounceRef.current) {
      clearTimeout(debounceRef.current)
    }

    debounceRef.current = setTimeout(async () => {
      setIsLoading(true)
      try {
        const results = await fetchOptions(term)
        setOptions(results)
      } catch (error) {
        console.error('Failed to fetch options:', error)
        setOptions([])
      } finally {
        setIsLoading(false)
      }
    }, debounceMs)
  }, [fetchOptions, debounceMs])

  // Initial load and search changes
  useEffect(() => {
    if (isOpen) {
      handleSearch(search)
    }
  }, [search, isOpen, handleSearch])

  const handleSelect = (value: string) => {
    if (!values.includes(value)) {
      onChange([...values, value])
    }
    setSearch('')
    setIsOpen(false)
  }

  const handleRemove = (value: string) => {
    onChange(values.filter((v) => v !== value))
  }

  const handleInputFocus = () => {
    setIsOpen(true)
  }

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSearch(e.target.value)
    if (!isOpen) setIsOpen(true)
  }

  // Filter out already-selected values from options
  const filteredOptions = options.filter((opt) => !values.includes(opt.value))

  return (
    <div className="relative" ref={containerRef}>
      <label className="block text-sm font-medium text-gray-700 mb-1">{label}</label>

      {/* Selected values as chips */}
      {values.length > 0 && (
        <div className="flex flex-wrap gap-1 mb-2">
          {values.map((value) => (
            <span
              key={value}
              className="inline-flex items-center gap-1 px-2 py-0.5 bg-blue-100 text-blue-800 text-xs rounded-full"
            >
              <span className="truncate max-w-[150px]" title={value}>
                {value}
              </span>
              <button
                type="button"
                onClick={() => handleRemove(value)}
                className="hover:text-blue-600 flex-shrink-0"
              >
                <X className="w-3 h-3" />
              </button>
            </span>
          ))}
        </div>
      )}

      {/* Input with dropdown trigger */}
      <div className="relative">
        <input
          ref={inputRef}
          type="text"
          value={search}
          onChange={handleInputChange}
          onFocus={handleInputFocus}
          placeholder={placeholder}
          className="w-full px-3 py-2 pr-8 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
        />
        <button
          type="button"
          onClick={() => setIsOpen(!isOpen)}
          className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
        >
          {isLoading ? (
            <Loader2 className="w-4 h-4 animate-spin" />
          ) : (
            <ChevronDown className={`w-4 h-4 transition-transform ${isOpen ? 'rotate-180' : ''}`} />
          )}
        </button>
      </div>

      {/* Dropdown */}
      {isOpen && (
        <div className="absolute z-20 w-full mt-1 bg-white border border-gray-300 rounded-md shadow-lg max-h-60 overflow-auto">
          {isLoading && filteredOptions.length === 0 ? (
            <div className="px-3 py-2 text-sm text-gray-500">Loading...</div>
          ) : filteredOptions.length === 0 ? (
            <div className="px-3 py-2 text-sm text-gray-500">
              {search ? 'No results found' : 'Type to search'}
            </div>
          ) : (
            filteredOptions.slice(0, 15).map((option) => (
              <button
                key={option.value}
                type="button"
                onClick={() => handleSelect(option.value)}
                className="w-full text-left px-3 py-2 hover:bg-gray-100 text-sm flex justify-between items-center"
              >
                <span className="truncate">{option.label}</span>
                {option.count !== undefined && (
                  <span className="text-gray-400 text-xs ml-2">
                    ({option.count.toLocaleString()})
                  </span>
                )}
              </button>
            ))
          )}
        </div>
      )}
    </div>
  )
}
