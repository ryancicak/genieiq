import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import StatCard from '../components/StatCard';
import StatusBadge from '../components/StatusBadge';
import { getAdminDashboard, startScanAllJob, getScanAllJob } from '../api/client';
import './AdminDashboard.css';

function AdminDashboard() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [scanJob, setScanJob] = useState(null);
  const [error, setError] = useState(null);

  async function loadDashboard() {
    setLoading(true);
    try {
      const result = await getAdminDashboard();
      setData(result);
      setError(null);
    } catch (err) {
      console.error('Failed to load admin dashboard:', err);
      setData(null);
      setError(err?.message || 'Failed to load admin dashboard');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadDashboard();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleScanAll = async () => {
    try {
      const resp = await startScanAllJob({ concurrency: 4, delayMs: 150 });
      setScanJob(resp?.job || null);
    } catch (err) {
      console.error('Scan failed:', err);
      setError(err?.message || 'Scan failed');
    }
  };

  useEffect(() => {
    if (!scanJob?.id) return;
    let cancelled = false;

    const tick = async () => {
      try {
        const resp = await getScanAllJob(scanJob.id);
        if (!cancelled && resp?.job) setScanJob(resp.job);
        if (!cancelled && (resp?.job?.status === 'completed' || resp?.job?.status === 'failed')) {
          await loadDashboard();
        }
      } catch {
        // ignore polling errors
      }
    };

    tick();
    const t = setInterval(tick, 2000);
    return () => {
      cancelled = true;
      clearInterval(t);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [scanJob?.id]);

  const stats = data?.stats;
  const needsAttention = data?.needsAttention || [];
  const allSpaces = data?.allSpaces || [];
  const note = data?.note;

  return (
    <div className="container admin-dashboard">
      <Link to="/" className="back-link">← Back to spaces</Link>

      <header className="page-header">
        <h1>Admin Dashboard</h1>
        <p className="page-subtitle">Org-wide visibility across Genie spaces</p>
      </header>

      {error && (
        <div className="alert alert-error" role="status" aria-live="polite" style={{ marginBottom: 16 }}>
          <div>
            <div className="alert-title">Couldn’t load admin dashboard</div>
            <div className="alert-body">{error}</div>
          </div>
          <div className="alert-actions">
            <button type="button" className="btn btn-secondary" onClick={loadDashboard} disabled={loading}>
              Retry
            </button>
          </div>
        </div>
      )}

      {!loading && note && (
        <div className="alert" role="status" aria-live="polite" style={{ marginBottom: 16 }}>
          <div>
            <div className="alert-title">History storage</div>
            <div className="alert-body">{note}</div>
          </div>
        </div>
      )}

      {!loading && scanJob?.status && (
        <div className="section-subtitle" style={{ marginBottom: 12 }}>
          Scan job: <strong>{scanJob.status}</strong>
          {typeof scanJob.total === 'number' ? ` · ${scanJob.completed}/${scanJob.total}` : ''}
          {scanJob.errors ? ` · errors: ${scanJob.errors}` : ''}
          {scanJob.lastScannedSpace?.name ? ` · last: ${scanJob.lastScannedSpace.name}` : ''}
          {scanJob.lastError ? ` · last error: ${scanJob.lastError}` : ''}
        </div>
      )}

      <div className="stats-grid" aria-busy={loading ? 'true' : 'false'}>
        {loading ? (
          Array.from({ length: 4 }).map((_, idx) => (
            <div key={idx} className="card" aria-hidden="true" style={{ padding: 18 }}>
              <div className="skeleton skeleton-text sm" style={{ width: 96, marginBottom: 10 }} />
              <div className="skeleton skeleton-text lg" style={{ width: 56 }} />
            </div>
          ))
        ) : (
          <>
            <StatCard value={stats?.totalSpaces ?? 0} label="Total Spaces" index={0} />
            <StatCard value={stats?.avgScore ?? 0} label="Avg Score" index={1} />
            <StatCard value={stats?.criticalCount ?? 0} label="Critical Issues" index={2} />
            <StatCard value={stats?.warehouseAttentionCount ?? 0} label="Warehouse needs attention" index={3} />
          </>
        )}
      </div>

      {!loading && needsAttention.length > 0 && (
        <section className="section">
          <h2 className="section-title">
            <span className="alert-icon">◉</span>
            Needs Attention
          </h2>
          <div className="attention-list">
            {needsAttention.map((space) => (
              <Link to={`/space/${space.id}`} key={space.id} className="attention-row">
                <span className="attention-score">{space.totalScore}</span>
                <div className="attention-info">
                  <span className="attention-name">{space.name}</span>
                  <span className="attention-owner">{space.owner}</span>
                </div>
                <StatusBadge status={space.status} />
              </Link>
            ))}
          </div>
        </section>
      )}

      <section className="section">
        <div className="section-header">
          <h2 className="section-title">All Spaces</h2>
          <button 
            className="btn btn-secondary"
            onClick={handleScanAll}
            disabled={loading || scanJob?.status === 'running' || scanJob?.status === 'queued'}
          >
              {scanJob?.status === 'running' || scanJob?.status === 'queued'
                ? `Scanning... ${scanJob.completed ?? 0}/${scanJob.total ?? '-'}`
                : 'Scan All (safe)'}
          </button>
        </div>
        
        <div className="table-container">
          <div className="table-scroll">
          <table className="spaces-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Score</th>
                <th>Owner</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                Array.from({ length: 8 }).map((_, idx) => (
                  <tr key={idx} aria-hidden="true">
                    <td><div className="skeleton skeleton-text" style={{ width: `${55 + (idx % 4) * 10}%` }} /></td>
                    <td><div className="skeleton skeleton-text sm" style={{ width: 42 }} /></td>
                    <td><div className="skeleton skeleton-text sm" style={{ width: `${40 + (idx % 3) * 12}%` }} /></td>
                    <td><div className="skeleton skeleton-pill" style={{ width: 82 }} /></td>
                  </tr>
                ))
              ) : (
                allSpaces.map((space) => (
                  <tr key={space.id}>
                    <td>
                      <Link to={`/space/${space.id}`} className="space-link">
                        {space.name}
                      </Link>
                    </td>
                    <td className="score-cell">{space.totalScore}</td>
                    <td className="owner-cell">{space.owner}</td>
                    <td><StatusBadge status={space.status} /></td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
          </div>
        </div>
      </section>
    </div>
  );
}

export default AdminDashboard;

