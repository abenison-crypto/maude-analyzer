import { ChevronRight, Home } from 'lucide-react'
import { type DrillDownLevel, LEVEL_LABELS } from '../../types/signals'

interface BreadcrumbItem {
  level: DrillDownLevel
  value: string
}

interface SignalBreadcrumbProps {
  path: BreadcrumbItem[]
  currentLevel: DrillDownLevel
  onNavigate: (index: number) => void
}

export default function SignalBreadcrumb({ path, currentLevel, onNavigate }: SignalBreadcrumbProps) {
  return (
    <nav className="flex items-center space-x-1 text-sm">
      {/* Home / Root level */}
      <button
        onClick={() => onNavigate(-1)}
        className={`flex items-center gap-1 px-2 py-1 rounded hover:bg-gray-100 ${
          path.length === 0 ? 'text-gray-900 font-medium' : 'text-blue-600 hover:text-blue-800'
        }`}
      >
        <Home className="w-4 h-4" />
        <span>Manufacturers</span>
      </button>

      {/* Path items */}
      {path.map((item, index) => (
        <div key={`${item.level}-${item.value}`} className="flex items-center">
          <ChevronRight className="w-4 h-4 text-gray-400" />
          <button
            onClick={() => onNavigate(index)}
            className={`px-2 py-1 rounded max-w-[200px] truncate ${
              index === path.length - 1
                ? 'text-gray-900 font-medium bg-gray-100'
                : 'text-blue-600 hover:text-blue-800 hover:bg-gray-100'
            }`}
            title={item.value}
          >
            {item.value}
          </button>
        </div>
      ))}

      {/* Current level indicator */}
      {path.length > 0 && (
        <div className="flex items-center text-gray-500 ml-2">
          <span className="text-xs bg-gray-200 px-2 py-0.5 rounded">
            Showing: {LEVEL_LABELS[currentLevel]}
          </span>
        </div>
      )}
    </nav>
  )
}
