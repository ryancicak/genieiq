import { Routes, Route, Navigate } from 'react-router-dom';
import { useState, useEffect } from 'react';
import SpaceSelector from './pages/SpaceSelector';
import SpaceDetail from './pages/SpaceDetail';
import AdminDashboard from './pages/AdminDashboard';
import Header from './components/Header';
import { getCurrentUser, getHealth } from './api/client';

function App() {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  const [health, setHealth] = useState(null);

  useEffect(() => {
    async function checkAuth() {
      try {
        const me = await getCurrentUser();
        setUser(me);
        try {
          const h = await getHealth();
          setHealth(h);
        } catch {
          setHealth(null);
        }
      } catch (error) {
        // Not authenticated (or backend unavailable)
        setUser(null);
        setHealth(null);
      } finally {
        setLoading(false);
      }
    }
    checkAuth();
  }, []);

  if (loading) {
    return (
      <div style={{ 
        display: 'flex', 
        alignItems: 'center', 
        justifyContent: 'center', 
        height: '100vh',
        color: 'var(--text-secondary)'
      }}>
        Loading...
      </div>
    );
  }

  return (
    <div className="app">
      <Header user={user} health={health} />
      <main className="main">
        <Routes>
          <Route path="/" element={<SpaceSelector user={user} health={health} />} />
          <Route path="/space/:id" element={<SpaceDetail />} />
          <Route path="/admin" element={
            user?.isAdmin ? <AdminDashboard /> : <Navigate to="/" replace />
          } />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
    </div>
  );
}

export default App;

