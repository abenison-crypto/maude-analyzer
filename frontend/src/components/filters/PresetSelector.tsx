/**
 * PresetSelector Component
 *
 * Dropdown to save and load filter presets
 */

import { useState, useRef, useEffect } from 'react'
import { Bookmark, ChevronDown, Plus, Trash2, Loader2 } from 'lucide-react'
import { useAdvancedFilters } from '../../hooks/useAdvancedFilters'

export default function PresetSelector() {
  const {
    presets,
    loadingPresets,
    savePreset,
    loadPreset,
    deletePreset,
    hasActiveFilters,
  } = useAdvancedFilters()

  const [isOpen, setIsOpen] = useState(false)
  const [showSaveDialog, setShowSaveDialog] = useState(false)
  const [newPresetName, setNewPresetName] = useState('')
  const [newPresetDescription, setNewPresetDescription] = useState('')
  const [isSaving, setIsSaving] = useState(false)
  const containerRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
        setIsOpen(false)
        setShowSaveDialog(false)
      }
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  const handleSave = async () => {
    if (!newPresetName.trim()) return

    setIsSaving(true)
    try {
      await savePreset(newPresetName.trim(), newPresetDescription.trim() || undefined)
      setNewPresetName('')
      setNewPresetDescription('')
      setShowSaveDialog(false)
      setIsOpen(false)
    } catch (error) {
      console.error('Failed to save preset:', error)
    } finally {
      setIsSaving(false)
    }
  }

  const handleLoad = (presetId: string) => {
    loadPreset(presetId)
    setIsOpen(false)
  }

  const handleDelete = async (e: React.MouseEvent, presetId: string) => {
    e.stopPropagation()
    if (confirm('Delete this preset?')) {
      try {
        await deletePreset(presetId)
      } catch (error) {
        console.error('Failed to delete preset:', error)
      }
    }
  }

  const builtInPresets = presets.filter((p) => p.isBuiltIn)
  const userPresets = presets.filter((p) => !p.isBuiltIn)

  return (
    <div className="relative" ref={containerRef}>
      <button
        type="button"
        onClick={() => setIsOpen(!isOpen)}
        className="flex items-center gap-1 px-3 py-2 text-sm text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-md transition-colors"
      >
        <Bookmark className="w-4 h-4" />
        <span>Presets</span>
        <ChevronDown className={`w-3 h-3 transition-transform ${isOpen ? 'rotate-180' : ''}`} />
      </button>

      {isOpen && (
        <div className="absolute right-0 mt-1 w-72 bg-white border border-gray-200 rounded-lg shadow-lg z-30">
          {showSaveDialog ? (
            <div className="p-3 space-y-3">
              <h3 className="font-medium text-gray-900">Save Current Filters</h3>
              <input
                type="text"
                value={newPresetName}
                onChange={(e) => setNewPresetName(e.target.value)}
                placeholder="Preset name"
                className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                autoFocus
              />
              <input
                type="text"
                value={newPresetDescription}
                onChange={(e) => setNewPresetDescription(e.target.value)}
                placeholder="Description (optional)"
                className="w-full px-3 py-2 border border-gray-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <div className="flex gap-2 justify-end">
                <button
                  type="button"
                  onClick={() => setShowSaveDialog(false)}
                  className="px-3 py-1.5 text-sm text-gray-600 hover:text-gray-900"
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={handleSave}
                  disabled={!newPresetName.trim() || isSaving}
                  className="px-3 py-1.5 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50"
                >
                  {isSaving ? <Loader2 className="w-4 h-4 animate-spin" /> : 'Save'}
                </button>
              </div>
            </div>
          ) : (
            <>
              {/* Save new preset button */}
              {hasActiveFilters && (
                <button
                  type="button"
                  onClick={() => setShowSaveDialog(true)}
                  className="w-full flex items-center gap-2 px-3 py-2.5 text-left text-sm text-blue-600 hover:bg-blue-50 border-b"
                >
                  <Plus className="w-4 h-4" />
                  Save current filters as preset
                </button>
              )}

              {/* Loading state */}
              {loadingPresets && (
                <div className="px-3 py-4 text-center text-gray-500">
                  <Loader2 className="w-4 h-4 animate-spin mx-auto" />
                </div>
              )}

              {/* Built-in presets */}
              {builtInPresets.length > 0 && (
                <div>
                  <div className="px-3 py-1.5 text-xs font-medium text-gray-500 uppercase bg-gray-50">
                    Built-in
                  </div>
                  {builtInPresets.map((preset) => (
                    <button
                      key={preset.id}
                      type="button"
                      onClick={() => handleLoad(preset.id)}
                      className="w-full text-left px-3 py-2 hover:bg-gray-100 text-sm"
                    >
                      <div className="font-medium text-gray-900">{preset.name}</div>
                      {preset.description && (
                        <div className="text-xs text-gray-500">{preset.description}</div>
                      )}
                    </button>
                  ))}
                </div>
              )}

              {/* User presets */}
              {userPresets.length > 0 && (
                <div>
                  <div className="px-3 py-1.5 text-xs font-medium text-gray-500 uppercase bg-gray-50">
                    Your Presets
                  </div>
                  {userPresets.map((preset) => (
                    <button
                      key={preset.id}
                      type="button"
                      onClick={() => handleLoad(preset.id)}
                      className="w-full text-left px-3 py-2 hover:bg-gray-100 text-sm group flex justify-between items-start"
                    >
                      <div>
                        <div className="font-medium text-gray-900">{preset.name}</div>
                        {preset.description && (
                          <div className="text-xs text-gray-500">{preset.description}</div>
                        )}
                      </div>
                      <button
                        type="button"
                        onClick={(e) => handleDelete(e, preset.id)}
                        className="opacity-0 group-hover:opacity-100 p-1 text-gray-400 hover:text-red-500 transition-opacity"
                      >
                        <Trash2 className="w-3.5 h-3.5" />
                      </button>
                    </button>
                  ))}
                </div>
              )}

              {/* Empty state */}
              {!loadingPresets && presets.length === 0 && (
                <div className="px-3 py-4 text-center text-sm text-gray-500">
                  No presets available
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  )
}
