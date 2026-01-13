import { useMemo, useState, useEffect } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import SpaceCard from '../components/SpaceCard';
import { getSpaces, getAllSpacesPage, getAllSpacesPageNew, getNewSpacesFeed, getSpacesStarred, getAllSpacesPageStarred, findAllSpaceByName, setSpaceStar, startScanAllJob, getScanAllJob } from '../api/client';
import './SpaceSelector.css';

function SpaceSelector({ user, health }) {
  const navigate = useNavigate();
  const [spaces, setSpaces] = useState([]);
  const [query, setQuery] = useState('');
  const [sort, setSort] = useState('score_desc');
  const [page, setPage] = useState(1); // used for scored mode
  const pageSize = 12;
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [scanJob, setScanJob] = useState(null);
  const [scanJobError, setScanJobError] = useState(null);
  const [finding, setFinding] = useState(false);
  const [findError, setFindError] = useState(null);
  const [mode, setMode] = useState('scored'); // 'scored' | 'all'
  const [starredOnly, setStarredOnly] = useState(false);
  const [newOnly, setNewOnly] = useState(false);
  const [newDays, setNewDays] = useState(7);
  const [newFeed, setNewFeed] = useState([]);
  const [newFeedLoading, setNewFeedLoading] = useState(false);
  const [total, setTotal] = useState(0);
  const [allTokens, setAllTokens] = useState([null]); // cursor stack for 'all' mode
  const [allIndex, setAllIndex] = useState(0);
  const [allNextToken, setAllNextToken] = useState(null);

  async function loadSpaces() {
    setLoading(true);
    try {
      if (mode === 'all') {
        const pageToken = allTokens[allIndex] || null;
        const data = starredOnly
          ? await getAllSpacesPageStarred({ pageToken, pageSize: 50 })
          : (newOnly ? await getAllSpacesPageNew({ pageToken, pageSize: 50, days: newDays }) : await getAllSpacesPage({ pageToken, pageSize: 50 }));
        setSpaces(data.spaces || []);
        setAllNextToken(data.nextPageToken || null);
        setTotal(0);
      } else {
        const data = starredOnly
          ? await getSpacesStarred({ page, pageSize, q: query.trim(), sort })
          : await getSpaces({ page, pageSize, q: query.trim(), sort });
        setSpaces(data.spaces || []);
        setTotal(data.total || 0);
      }
      setError(null);
    } catch (err) {
      console.error('Failed to load spaces:', err);
      setSpaces([]);
      setError(err?.message || 'Failed to load spaces');
    } finally {
      setLoading(false);
    }
  }

  const toggleStar = async (spaceId, nextStarred) => {
    // Optimistic update
    setSpaces((prev) => prev.map((s) => (s.id === spaceId ? { ...s, starred: nextStarred } : s)));
    try {
      await setSpaceStar(spaceId, nextStarred);
    } catch (e) {
      // Revert on failure
      setSpaces((prev) => prev.map((s) => (s.id === spaceId ? { ...s, starred: !nextStarred } : s)));
    }
  };

  const startSafeScanAll = async () => {
    setScanJobError(null);
    try {
      const resp = await startScanAllJob({ concurrency: 2, delayMs: 250 });
      setScanJob(resp?.job || null);
    } catch (e) {
      setScanJobError(e?.message || 'Failed to start scan job');
    }
  };

  const findGlobally = async () => {
    const q = query.trim();
    if (!q) return;
    setFindError(null);
    setFinding(true);
    try {
      const resp = await findAllSpaceByName(q, { maxPages: 30, pageSize: 200 });
      if (resp?.found?.id) {
        navigate(`/spaces/${resp.found.id}`);
      } else {
        setFindError(`No space found matching “${q}”.`);
      }
    } catch (e) {
      setFindError(e?.message || 'Failed to find space');
    } finally {
      setFinding(false);
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
          // Refresh list once job ends so Lakebase-backed scores appear.
          loadSpaces();
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

  const totalPages = mode === 'scored' ? Math.max(1, Math.ceil((total || 0) / pageSize)) : 1;
  const safePage = Math.min(page, totalPages);

  // Scored mode: server-side search/sort/pagination (debounced)
  useEffect(() => {
    if (mode !== 'scored') return;
    const t = setTimeout(() => {
      loadSpaces();
    }, 150);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, page, sort, query, starredOnly]);

  // All-spaces mode: only refetch when paging (not when typing search/sort)
  useEffect(() => {
    if (mode !== 'all') return;
    loadSpaces();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, allIndex, starredOnly, newOnly, newDays]);

  // Reset paging when mode changes
  useEffect(() => {
    setError(null);
    setSpaces([]);
    if (mode === 'scored') {
      setPage(1);
    } else {
      setAllTokens([null]);
      setAllIndex(0);
      setAllNextToken(null);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode]);

  // If Lakebase is missing/unhealthy, default to "All spaces" so the app isn't a blank screen.
  useEffect(() => {
    const db = health?.database;
    const lakebaseOk = db?.mode === 'lakebase' && db?.status === 'healthy';
    if (!lakebaseOk && mode === 'scored' && !loading && !error && (spaces?.length || 0) === 0 && (total || 0) === 0) {
      setMode('all');
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [health?.database?.status, health?.database?.mode, mode, loading, error, total, spaces?.length]);

  useEffect(() => {
    if (mode === 'scored') setPage(1);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [starredOnly]);

  useEffect(() => {
    if (starredOnly) setNewOnly(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [starredOnly]);

  useEffect(() => {
    if (mode !== 'all') return;
    if (starredOnly) return; // starred view already curated; keep it quiet
    let cancelled = false;
    (async () => {
      setNewFeedLoading(true);
      try {
        const resp = await getNewSpacesFeed({ days: newDays, limit: 10 });
        if (!cancelled) setNewFeed(Array.isArray(resp?.spaces) ? resp.spaces : []);
      } catch {
        if (!cancelled) setNewFeed([]);
      } finally {
        if (!cancelled) setNewFeedLoading(false);
      }
    })();
    return () => { cancelled = true; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, starredOnly, newDays]);

  // In "All spaces" mode, search/sort apply to the currently loaded page (fast + predictable).
  const displayedSpaces = useMemo(() => {
    if (mode !== 'all') return spaces;
    const q = query.trim().toLowerCase();
    let result = Array.isArray(spaces) ? spaces : [];
    // In "All spaces" mode, the backend applies starred_only if requested, but keep a guard
    // in case older cached pages lack it.
    if (starredOnly) result = result.filter((s) => Boolean(s?.starred));
    if (q) result = result.filter((s) => String(s?.name || '').toLowerCase().includes(q));

    const sorted = [...result];
    sorted.sort((a, b) => {
      if (sort === 'name_asc') return String(a?.name || '').localeCompare(String(b?.name || ''));
      if (sort === 'scanned_desc') {
        const at = a?.scannedAt ? Date.parse(a.scannedAt) : 0;
        const bt = b?.scannedAt ? Date.parse(b.scannedAt) : 0;
        return bt - at;
      }
      if (sort === 'score_asc') return (Number(a?.totalScore ?? -1)) - (Number(b?.totalScore ?? -1));
      // default score_desc
      return (Number(b?.totalScore ?? -1)) - (Number(a?.totalScore ?? -1));
    });
    return sorted;
  }, [mode, spaces, query, sort]);

  return (
    <div className="container space-selector">
      <header className="page-header">
        <h1>Your Spaces</h1>
        <p className="page-subtitle">
          {mode === 'scored'
            ? 'Fast view from history (Lakebase). Run scans to populate scores.'
            : 'All Genie spaces (paged). Search/sort applies to the current page.'}
        </p>

        {health?.database?.status === 'unhealthy' && (
          <div className="alert alert-error" role="status" aria-live="polite" style={{ marginTop: 12 }}>
            <div>
              <div className="alert-title">Lakebase history is unavailable</div>
              <div className="alert-body">
                Your Lakebase database is missing or unreachable. You can still use GenieIQ via “All spaces”, but scores/history won’t persist until Lakebase is restored.
                <br />
                <br />
                First-time deploy: run <strong>./deploy.sh</strong> to provision/connect Lakebase (or use <strong>scripts/Deploy_GenieIQ.command</strong> on macOS).
                {Array.isArray(health?.database?.failures) && health.database.failures.some((f) => String(f?.error || '').includes('role') && String(f?.error || '').includes('does not exist')) && (
                  <>
                    <br />
                    <br />
                    <strong>Fix (common):</strong> grant the GenieIQ app service principal access to this Lakebase instance.
                    <br />
                    In Databricks: Compute → Lakebase Postgres → your instance → Permissions → Add principal.
                    <br />
                    Service principal client id: <strong>{health?.servicePrincipalClientId || '(unknown)'}</strong>
                  </>
                )}
              </div>
            </div>
            <div className="alert-actions">
              <button type="button" className="btn btn-secondary" onClick={() => setMode('all')} disabled={loading}>
                Open All spaces
              </button>
            </div>
          </div>
        )}
        <div className="space-controls" role="search">
          <div className="mode-tabs" role="tablist" aria-label="Spaces mode">
            <button
              type="button"
              role="tab"
              aria-selected={mode === 'scored'}
              className={`mode-tab ${mode === 'scored' ? 'active' : ''}`}
              onClick={() => setMode('scored')}
              disabled={loading}
            >
              Scored
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={mode === 'all'}
              className={`mode-tab ${mode === 'all' ? 'active' : ''}`}
              onClick={() => setMode('all')}
              disabled={loading}
            >
              All spaces
            </button>
          </div>

          <label className="mine-toggle" title="Show only spaces you’ve starred">
            <input
              type="checkbox"
              checked={starredOnly}
              onChange={(e) => setStarredOnly(e.target.checked)}
              disabled={loading}
            />
            <span>Starred by me</span>
          </label>

          {mode === 'all' && (
            <label className="mine-toggle" title="Show only newly discovered spaces (based on first seen time)">
              <input
                type="checkbox"
                checked={newOnly}
                onChange={(e) => setNewOnly(e.target.checked)}
                disabled={loading || starredOnly}
              />
              <span>New ({newDays}d)</span>
            </label>
          )}

          <input
            className="space-search control"
            type="search"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder={mode === 'all' ? 'Search this page…' : 'Search spaces by name…'}
            aria-label="Search spaces by name"
            autoComplete="off"
            disabled={loading}
            onKeyDown={(e) => {
              if (mode === 'all' && (e.metaKey || e.ctrlKey) && e.key === 'Enter') {
                e.preventDefault();
                findGlobally();
              }
            }}
          />
          <select
            className="space-sort control"
            value={sort}
            onChange={(e) => setSort(e.target.value)}
            aria-label="Sort spaces"
            disabled={loading}
          >
            <option value="score_desc">Score (High → Low)</option>
            <option value="score_asc">Score (Low → High)</option>
            <option value="name_asc">Name (A → Z)</option>
            <option value="scanned_desc">Last scanned (Newest)</option>
          </select>
          {query.trim().length > 0 && (
            <button
              type="button"
              className="space-clear btn btn-secondary"
              onClick={() => setQuery('')}
              aria-label="Clear search"
              disabled={loading}
            >
              Clear
            </button>
          )}
          {mode === 'all' && query.trim().length > 0 && (
            <button
              type="button"
              className="btn btn-secondary"
              onClick={findGlobally}
              disabled={loading || finding}
              title="Find across all pages (Cmd/Ctrl+Enter in the search box)"
            >
              {finding ? 'Finding…' : 'Find globally'}
            </button>
          )}
          {user?.isAdmin && (
            <button
              type="button"
              className="btn btn-secondary"
              onClick={startSafeScanAll}
              disabled={loading || scanJob?.status === 'running' || scanJob?.status === 'queued'}
              title="Scans spaces with small concurrency and pacing to avoid rate limits"
            >
              {scanJob?.status === 'running' || scanJob?.status === 'queued'
                ? `Scanning... ${scanJob.completed ?? 0}/${scanJob.total ?? '-'}`
                : 'Scan All (safe)'}
            </button>
          )}
        </div>

        {mode === 'all' && findError && (
          <div className="alert alert-error" role="status" aria-live="polite" style={{ marginTop: 10 }}>
            <div className="alert-body">{findError}</div>
          </div>
        )}

        <div className="spaces-meta-row">
          <div className="space-count" aria-live="polite">
            {mode === 'scored'
              ? `${spaces.length} shown · ${total} total`
              : `${spaces.length} shown`}
          </div>
        </div>

        {user?.isAdmin && (scanJob?.status || scanJobError) && (
          <div className="section-subtitle" style={{ marginTop: 10 }}>
            {scanJobError
              ? `Scan job error: ${scanJobError}`
              : (
                <>
                  Scan job: <strong>{scanJob.status}</strong>
                  {typeof scanJob.total === 'number' ? ` · ${scanJob.completed}/${scanJob.total}` : ''}
                  {scanJob.errors ? ` · errors: ${scanJob.errors}` : ''}
                  {scanJob.lastScannedSpace?.name ? ` · last: ${scanJob.lastScannedSpace.name}` : ''}
                </>
              )
            }
          </div>
        )}
      </header>

      {error && (
        <div className="alert alert-error" role="status" aria-live="polite">
          <div>
            <div className="alert-title">Couldn’t load spaces</div>
            <div className="alert-body">{error}</div>
          </div>
          <div className="alert-actions">
            <button type="button" className="btn btn-secondary" onClick={loadSpaces}>
              Retry
            </button>
          </div>
        </div>
      )}

      {!loading && mode === 'all' && !starredOnly && (
        <section className="card new-spaces-card">
          <div className="new-spaces-header">
            <div>
              <div className="new-spaces-title">New spaces</div>
              <div className="new-spaces-subtitle">Recently discovered (last {newDays} days)</div>
            </div>
          </div>

          <div className="new-spaces-list" aria-busy={newFeedLoading ? 'true' : 'false'}>
            {newFeedLoading ? (
              Array.from({ length: 4 }).map((_, idx) => (
                <div key={idx} className="new-space-row" aria-hidden="true">
                  <div className="skeleton skeleton-text lg" style={{ width: `${58 + (idx % 3) * 10}%` }} />
                  <div className="skeleton skeleton-pill" style={{ width: 76 }} />
                </div>
              ))
            ) : (
              (Array.isArray(newFeed) ? newFeed : []).slice(0, 8).map((s) => (
                <Link key={s.id} to={`/space/${s.id}`} className="new-space-row">
                  <span className="new-space-name">{s.name}</span>
                  <span className="new-space-meta">just added</span>
                </Link>
              ))
            )}
            {!newFeedLoading && (!newFeed || newFeed.length === 0) && (
              <div className="new-space-empty">No new spaces detected yet. As you page through “All spaces”, this will populate.</div>
            )}
          </div>
        </section>
      )}

      <div className="space-grid">
        {loading
          ? Array.from({ length: pageSize }).map((_, idx) => (
              <div
                key={idx}
                className={`space-card card animate-fade-in stagger-${(idx % 6) + 1}`}
                aria-hidden="true"
              >
                <div className="skeleton skeleton-score-md" />
                <div className="space-card-info">
                  <div className="skeleton skeleton-text lg" style={{ width: `${62 + (idx % 4) * 8}%` }} />
                  <div className="skeleton skeleton-pill" style={{ width: `${92 + (idx % 3) * 14}px` }} />
                </div>
                <div className="skeleton skeleton-text sm" style={{ width: '55%' }} />
              </div>
            ))
          : (mode === 'all' ? displayedSpaces : spaces).map((space, index) => (
              <SpaceCard
                key={space.id}
                space={space}
                index={index}
                newThresholdDays={newDays}
                onToggleStar={(nextStarred) => toggleStar(space.id, nextStarred)}
              />
            ))}
      </div>

      {!loading && mode === 'scored' && total > pageSize && (
        <div className="space-pagination" role="navigation" aria-label="Spaces pagination">
          <div className="space-pagination-meta">
            Page <span className="space-pagination-strong">{safePage}</span> of <span className="space-pagination-strong">{totalPages}</span>
          </div>
          <div className="space-pagination-controls">
            <button
              type="button"
              className="btn btn-secondary"
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={safePage <= 1}
            >
              Prev
            </button>
            <div className="space-pagination-page">{safePage}</div>
            <button
              type="button"
              className="btn btn-secondary"
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={safePage >= totalPages}
            >
              Next
            </button>
          </div>
        </div>
      )}

      {!loading && mode === 'all' && (
        <div className="space-pagination" role="navigation" aria-label="All spaces pagination">
          <div className="space-pagination-meta">
            Page <span className="space-pagination-strong">{allIndex + 1}</span>
            {query.trim() ? (
              <>
                {' '}· Showing <span className="space-pagination-strong">{displayedSpaces.length}</span> matches on this page
              </>
            ) : null}
          </div>
          <div className="space-pagination-controls">
            <button
              type="button"
              className="btn btn-secondary"
              onClick={() => setAllIndex((i) => Math.max(0, i - 1))}
              disabled={allIndex <= 0 || loading}
            >
              Prev
            </button>
            <button
              type="button"
              className="btn btn-secondary"
              onClick={() => {
                if (!allNextToken) return;
                setAllTokens((toks) => {
                  const next = toks.slice(0, allIndex + 1);
                  next.push(allNextToken);
                  return next;
                });
                setAllIndex((i) => i + 1);
              }}
              disabled={!allNextToken || loading}
            >
              Next
            </button>
          </div>
        </div>
      )}

      {!loading && spaces.length === 0 && (
        <div className="empty-state">
          <p>{mode === 'scored' ? 'No scored spaces yet.' : 'No spaces found on this page.'}</p>
          {mode === 'scored' && (
            <p className="empty-hint">Run “Scan All (safe)” (admin) or open a space and click “Scan Again” to create history.</p>
          )}
        </div>
      )}
    </div>
  );
}

export default SpaceSelector;

