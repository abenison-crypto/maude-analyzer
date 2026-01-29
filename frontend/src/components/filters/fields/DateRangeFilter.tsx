/**
 * DateRangeFilter Component
 *
 * Date range picker with from/to inputs
 */

interface DateRangeFilterProps {
  label: string
  dateFrom: string
  dateTo: string
  onChange: (from: string, to: string) => void
}

export default function DateRangeFilter({
  label,
  dateFrom,
  dateTo,
  onChange,
}: DateRangeFilterProps) {
  return (
    <div>
      <label className="block text-sm font-medium text-gray-700 mb-1">{label}</label>
      <div className="flex gap-2">
        <input
          type="date"
          value={dateFrom}
          onChange={(e) => onChange(e.target.value, dateTo)}
          className="flex-1 px-2 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
          placeholder="From"
        />
        <span className="flex items-center text-gray-400 text-sm">to</span>
        <input
          type="date"
          value={dateTo}
          onChange={(e) => onChange(dateFrom, e.target.value)}
          className="flex-1 px-2 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 text-sm"
          placeholder="To"
        />
      </div>
    </div>
  )
}
