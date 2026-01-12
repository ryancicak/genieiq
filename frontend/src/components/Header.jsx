import { Link, useLocation } from 'react-router-dom';
import './Header.css';

function Header({ user, health }) {
  const location = useLocation();
  const isAdmin = location.pathname === '/admin';
  const db = health?.database;
  const dbMode = db?.mode || (db?.status === 'healthy' && db?.host ? 'lakebase' : null);
  const dbLabel =
    dbMode === 'lakebase' ? 'Lakebase' :
    dbMode === 'in-memory' ? 'In-memory' :
    null;

  const initials = (() => {
    const email = (user?.email || '').trim();
    if (!email) return '';
    const localPart = email.split('@')[0] || '';
    const parts = localPart
      .split(/[._-]+/)
      .map((p) => p.trim())
      .filter(Boolean);

    if (parts.length >= 2) {
      return `${parts[0][0]}${parts[1][0]}`.toUpperCase();
    }
    if (parts.length === 1) {
      const p = parts[0];
      return (p.length >= 2 ? `${p[0]}${p[1]}` : p[0]).toUpperCase();
    }
    return email.slice(0, 2).toUpperCase();
  })();

  return (
    <header className="header">
      <div className="header-container">
        <div className="logo-section">
          <Link to="/" className="logo">
            GenieIQ
          </Link>
          <div className="logo-divider"></div>
          <span className="logo-slogan">For Better Answers</span>
        </div>

        <div className="header-right">
          {user?.isAdmin && dbLabel && (
            <div
              className={`storage-pill ${dbMode === 'lakebase' ? 'ok' : 'warn'}`}
              title={dbMode === 'lakebase'
                ? `Storage: Lakebase${db?.host ? ` (${db.host})` : ''}`
                : 'Storage: In-memory (history resets if the app restarts)'}
            >
              Storage: {dbLabel}
            </div>
          )}
          {user?.isAdmin && (
            <Link 
              to={isAdmin ? '/' : '/admin'} 
              className="btn btn-secondary"
            >
              {isAdmin ? 'My Spaces' : 'Admin'}
            </Link>
          )}
          
          {user && (
            <div className="user-avatar" title={user.email}>
              {initials}
            </div>
          )}
        </div>
      </div>
    </header>
  );
}

export default Header;

