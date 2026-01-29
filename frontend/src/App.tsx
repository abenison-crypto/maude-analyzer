import { Routes, Route, NavLink } from 'react-router-dom'
import { Home, Search, BarChart3, Settings } from 'lucide-react'
import { AdvancedFilterProvider } from './hooks/useAdvancedFilters'
import HomePage from './pages/Home'
import ExplorePage from './pages/Explore'
import AnalyzePage from './pages/Analyze'
import AdminPage from './pages/Admin'

function App() {
  return (
    <AdvancedFilterProvider>
      <div className="min-h-screen bg-gray-50">
        {/* Navigation */}
        <nav className="bg-white shadow-sm border-b">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div className="flex justify-between h-16">
              <div className="flex">
                <div className="flex-shrink-0 flex items-center">
                  <span className="text-xl font-bold text-blue-600">MAUDE Analyzer</span>
                </div>
                <div className="hidden sm:ml-8 sm:flex sm:space-x-4">
                  <NavLink
                    to="/"
                    className={({ isActive }) =>
                      `inline-flex items-center px-3 py-2 text-sm font-medium rounded-md ${
                        isActive
                          ? 'bg-blue-100 text-blue-700'
                          : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'
                      }`
                    }
                  >
                    <Home className="w-4 h-4 mr-2" />
                    Home
                  </NavLink>
                  <NavLink
                    to="/explore"
                    className={({ isActive }) =>
                      `inline-flex items-center px-3 py-2 text-sm font-medium rounded-md ${
                        isActive
                          ? 'bg-blue-100 text-blue-700'
                          : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'
                      }`
                    }
                  >
                    <Search className="w-4 h-4 mr-2" />
                    Explore
                  </NavLink>
                  <NavLink
                    to="/analyze"
                    className={({ isActive }) =>
                      `inline-flex items-center px-3 py-2 text-sm font-medium rounded-md ${
                        isActive
                          ? 'bg-blue-100 text-blue-700'
                          : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'
                      }`
                    }
                  >
                    <BarChart3 className="w-4 h-4 mr-2" />
                    Analyze
                  </NavLink>
                  <NavLink
                    to="/admin"
                    className={({ isActive }) =>
                      `inline-flex items-center px-3 py-2 text-sm font-medium rounded-md ${
                        isActive
                          ? 'bg-blue-100 text-blue-700'
                          : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900'
                      }`
                    }
                  >
                    <Settings className="w-4 h-4 mr-2" />
                    Admin
                  </NavLink>
                </div>
              </div>
            </div>
          </div>
        </nav>

        {/* Main Content */}
        <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <Routes>
            <Route path="/" element={<HomePage />} />
            <Route path="/explore" element={<ExplorePage />} />
            <Route path="/analyze" element={<AnalyzePage />} />
            <Route path="/admin" element={<AdminPage />} />
          </Routes>
        </main>
      </div>
    </AdvancedFilterProvider>
  )
}

export default App
