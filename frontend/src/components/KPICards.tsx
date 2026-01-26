import { Skull, AlertTriangle, Wrench, HelpCircle } from 'lucide-react'
import { useEventStats } from '../hooks/useEvents'

interface KPICardProps {
  title: string
  value: number
  icon: React.ReactNode
  color: string
  loading?: boolean
}

function KPICard({ title, value, icon, color, loading }: KPICardProps) {
  return (
    <div className="bg-white rounded-lg shadow p-6">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm font-medium text-gray-600">{title}</p>
          {loading ? (
            <div className="h-8 w-24 bg-gray-200 animate-pulse rounded mt-1" />
          ) : (
            <p className={`text-2xl font-bold ${color}`}>{value.toLocaleString()}</p>
          )}
        </div>
        <div className={`p-3 rounded-full ${color.replace('text-', 'bg-').replace('-700', '-100')}`}>
          {icon}
        </div>
      </div>
    </div>
  )
}

export default function KPICards() {
  const { data: stats, isLoading } = useEventStats()

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
      <KPICard
        title="Total Events"
        value={stats?.total || 0}
        icon={<AlertTriangle className="w-6 h-6 text-blue-700" />}
        color="text-blue-700"
        loading={isLoading}
      />
      <KPICard
        title="Deaths"
        value={stats?.deaths || 0}
        icon={<Skull className="w-6 h-6 text-red-700" />}
        color="text-red-700"
        loading={isLoading}
      />
      <KPICard
        title="Injuries"
        value={stats?.injuries || 0}
        icon={<AlertTriangle className="w-6 h-6 text-orange-700" />}
        color="text-orange-700"
        loading={isLoading}
      />
      <KPICard
        title="Malfunctions"
        value={stats?.malfunctions || 0}
        icon={<Wrench className="w-6 h-6 text-yellow-700" />}
        color="text-yellow-700"
        loading={isLoading}
      />
      <KPICard
        title="Other"
        value={stats?.other || 0}
        icon={<HelpCircle className="w-6 h-6 text-gray-700" />}
        color="text-gray-700"
        loading={isLoading}
      />
    </div>
  )
}
