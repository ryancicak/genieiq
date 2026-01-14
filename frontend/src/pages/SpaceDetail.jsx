import { useState, useEffect } from 'react';
import { useParams, Link } from 'react-router-dom';
import ScoreDisplay from '../components/ScoreDisplay';
import MaturityBadge from '../components/MaturityBadge';
import ProgressBar from '../components/ProgressBar';
import RecommendationRow from '../components/RecommendationRow';
import ScoreHistoryModal from '../components/ScoreHistoryModal';
import { getSpace, scanSpace, generateFixNotebook, getSpaceHistory, setSpaceStar } from '../api/client';
import './SpaceDetail.css';

function SpaceDetail() {
  const { id } = useParams();
  const [space, setSpace] = useState(null);
  const [loading, setLoading] = useState(true);
  const [scanning, setScanning] = useState(false);
  const [generatingNotebook, setGeneratingNotebook] = useState(false);
  const [notebookInfo, setNotebookInfo] = useState(null);
  const [notebookError, setNotebookError] = useState(null);
  const [scoreDelta, setScoreDelta] = useState(null);
  const [error, setError] = useState(null);
  const [autoScanEnabled, setAutoScanEnabled] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState(null);
  const [historyPoints, setHistoryPoints] = useState([]);

  async function loadSpace() {
    setLoading(true);
    try {
      const data = await getSpace(id);
      setSpace(data);
      setError(null);
    } catch (err) {
      console.error('Failed to load space:', err);
      setSpace(null);
      setError(err?.message || 'Failed to load space');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadSpace();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [id]);

  async function loadHistory() {
    if (!id) return;
    setHistoryLoading(true);
    setHistoryError(null);
    try {
      const data = await getSpaceHistory(id, { days: 30, limit: 180 });
      setHistoryPoints(Array.isArray(data?.history) ? data.history : []);
    } catch (err) {
      setHistoryPoints([]);
      setHistoryError(err?.message || 'Failed to load history');
    } finally {
      setHistoryLoading(false);
    }
  }

  useEffect(() => {
    if (!autoScanEnabled) return;

    const interval = setInterval(() => {
      // Avoid scanning in the background (and avoid stacking scans).
      if (typeof document !== 'undefined' && document.visibilityState !== 'visible') return;
      if (scanning || generatingNotebook) return;
      handleRescan();
    }, 60_000);

    return () => clearInterval(interval);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoScanEnabled, id, scanning, generatingNotebook]);

  const handleRescan = async () => {
    setScanning(true);
    try {
      const result = await scanSpace(id);
      setSpace(result);
      setScoreDelta(result.scoreDelta || 0);
      setError(null);
      if (historyOpen) loadHistory();
    } catch (err) {
      console.error('Rescan failed:', err);
      setError(err?.message || 'Rescan failed');
    } finally {
      setScanning(false);
    }
  };

  const toggleStar = async () => {
    if (!space?.id) return;
    const nextStarred = !Boolean(space.starred);
    setSpace((prev) => (prev ? { ...prev, starred: nextStarred } : prev));
    try {
      await setSpaceStar(space.id, nextStarred);
    } catch {
      setSpace((prev) => (prev ? { ...prev, starred: !nextStarred } : prev));
    }
  };

  const handleGenerateNotebook = async () => {
    setGeneratingNotebook(true);
    try {
      const result = await generateFixNotebook(id);
      setNotebookInfo(result);
      setNotebookError(null);
    } catch (err) {
      console.error('Notebook generation failed:', err);
      setNotebookError(err?.message || 'Couldn’t create a fix notebook right now.');
    } finally {
      setGeneratingNotebook(false);
    }
  };

  if (loading) {
    return (
      <div className="container space-detail">
        <Link to="/" className="back-link">← Back to spaces</Link>
        <div className="score-hero" aria-hidden="true">
          <div className="skeleton skeleton-score-lg" />
          <div className="skeleton skeleton-text lg" style={{ width: '52%', maxWidth: 520 }} />
          <div className="skeleton skeleton-pill" style={{ width: 140 }} />
        </div>
        <div className="detail-content" aria-hidden="true">
          <section className="breakdown-section card">
            <div className="skeleton skeleton-text lg" style={{ width: 220, marginBottom: 16 }} />
            <div className="breakdown-list">
              {Array.from({ length: 4 }).map((_, idx) => (
                <div key={idx} className="skeleton skeleton-text" style={{ height: 42, borderRadius: 12 }} />
              ))}
            </div>
          </section>
          <section className="next-steps-section">
            <div className="skeleton skeleton-text lg" style={{ width: 160, marginBottom: 16 }} />
            <div className="next-steps-list">
              {Array.from({ length: 3 }).map((_, idx) => (
                <div key={idx} className="skeleton" style={{ height: 64, borderRadius: 12 }} />
              ))}
            </div>
          </section>
        </div>
      </div>
    );
  }

  if (!space) {
    return (
      <div className="container">
        <Link to="/" className="back-link">← Back to spaces</Link>
        <div className="alert alert-error" role="status" aria-live="polite" style={{ marginTop: 12 }}>
          <div>
            <div className="alert-title">Couldn’t load this space</div>
            <div className="alert-body">{error || 'Space not found'}</div>
          </div>
          <div className="alert-actions">
            <button type="button" className="btn btn-secondary" onClick={loadSpace}>
              Retry
            </button>
          </div>
        </div>
      </div>
    );
  }

  const accessWarning = space?.accessWarning;
  const breakdownItems = Object.values(space.breakdown || {});
  const findings = Array.isArray(space.findings) ? space.findings : [];
  const passedFindings = findings.filter(f => f.passed);
  const failedFindings = findings.filter(f => !f.passed);

  const categoryOrder = ['foundation', 'dataSetup', 'sqlAssets', 'optimization'];
  const categoryNameByKey = {
    foundation: 'Foundation',
    dataSetup: 'Data Setup',
    sqlAssets: 'SQL Assets',
    optimization: 'Optimization'
  };

  const nextStepsByCategory = (() => {
    const steps = Array.isArray(space.nextSteps) ? space.nextSteps : [];
    const map = new Map();
    for (const step of steps) {
      const key = step.category || (findings.find(f => f.id === step.id)?.category) || 'other';
      const name = step.categoryName || categoryNameByKey[key] || key;
      if (!map.has(key)) map.set(key, { key, name, items: [] });
      map.get(key).items.push(step);
    }
    // preserve rubric order first, then anything else
    const ordered = [];
    for (const k of categoryOrder) {
      if (map.has(k)) ordered.push(map.get(k));
    }
    for (const [k, v] of map.entries()) {
      if (!categoryOrder.includes(k)) ordered.push(v);
    }
    return ordered;
  })();

  const didWellByCategory = (() => {
    const map = new Map();
    for (const f of passedFindings) {
      const key = f.category || 'other';
      const name = f.categoryName || categoryNameByKey[key] || key;
      if (!map.has(key)) map.set(key, { key, name, items: [] });
      map.get(key).items.push(f);
    }
    const ordered = [];
    for (const k of categoryOrder) {
      if (map.has(k)) ordered.push(map.get(k));
    }
    for (const [k, v] of map.entries()) {
      if (!categoryOrder.includes(k)) ordered.push(v);
    }
    return ordered;
  })();

  return (
    <div className="container space-detail">
      <div className="space-detail-sticky">
        <div className="space-detail-sticky-row">
          <Link to="/" className="back-link">← Back</Link>
          <div className="space-detail-sticky-right">
            <label className="auto-scan-toggle" title="Runs a full scan every 60 seconds (only while this tab is visible)">
              <input
                type="checkbox"
                checked={autoScanEnabled}
                onChange={(e) => setAutoScanEnabled(e.target.checked)}
                disabled={scanning}
              />
              <span>Auto-scan (60s)</span>
            </label>
            <div className="action-buttons action-buttons-compact">
              <button
                className="btn btn-secondary"
                onClick={handleGenerateNotebook}
                disabled={generatingNotebook || scanning}
              >
                {generatingNotebook ? 'Creating…' : 'Create Fix Notebook'}
              </button>
              <button
                className="btn btn-secondary"
                onClick={handleRescan}
                disabled={scanning}
              >
                {scanning ? 'Scanning...' : 'Scan Again'}
              </button>
            </div>
          </div>
        </div>

        {notebookInfo?.notebookUrl && (
          <div className="action-note action-note-center" role="status" aria-live="polite">
            Notebook ready ·{' '}
            <a href={notebookInfo.notebookUrl} target="_blank" rel="noreferrer">Open</a>
            {notebookInfo?.notebookPath && (
              <>
                {' '}· <code className="mono">{notebookInfo.notebookPath}</code>
              </>
            )}
          </div>
        )}
        {notebookError && (
          <div className="action-note action-note-error action-note-center" role="status" aria-live="polite">
            Couldn’t create the fix notebook. Please try again.
          </div>
        )}
      </div>

      {accessWarning?.message && (
        <div className="alert alert-error" role="status" aria-live="polite" style={{ marginTop: 12 }}>
          <div>
            <div className="alert-title">Limited access to this space</div>
            <div className="alert-body">{accessWarning.message}</div>
          </div>
        </div>
      )}

      {error && (
        <div className="alert alert-error" role="status" aria-live="polite" style={{ marginTop: 12 }}>
          <div>
            <div className="alert-title">Something went wrong</div>
            <div className="alert-body">{error}</div>
          </div>
          <div className="alert-actions">
            <button type="button" className="btn btn-secondary" onClick={loadSpace} disabled={scanning}>
              Refresh
            </button>
          </div>
        </div>
      )}

      <div className="score-hero">
        <button
          type="button"
          className="score-history-trigger"
          onClick={() => {
            setHistoryOpen(true);
            loadHistory();
          }}
          title="View score history"
        >
          <ScoreDisplay 
            score={space.totalScore} 
            size="large" 
            delta={scoreDelta}
          />
        </button>
        
        <div className="space-name-row">
          <h2 className="space-name">{space.name}</h2>
          <button
            type="button"
            className={`space-detail-star ${space.starred ? 'is-starred' : ''}`}
            onClick={toggleStar}
            title={space.starred ? 'Starred' : 'Star this space'}
            aria-label={space.starred ? 'Unstar space' : 'Star space'}
          >
            {space.starred ? '★' : '☆'}
          </button>
        </div>
        <MaturityBadge level={space.maturityLevel} />
      </div>

      <ScoreHistoryModal
        open={historyOpen}
        spaceName={space?.name}
        points={historyPoints}
        loading={historyLoading}
        error={historyError}
        onClose={() => setHistoryOpen(false)}
      />

      <div className="detail-content">
        <section className="breakdown-section card">
          <h3>Category Breakdown</h3>
          <div className="breakdown-list">
            {breakdownItems.map((item, index) => (
              <ProgressBar
                key={item.name}
                label={item.name}
                score={item.score}
                maxScore={item.maxPoints}
                index={index}
              />
            ))}
          </div>
        </section>

        {didWellByCategory.length > 0 && (
          <section className="card">
            <h3>What you’re doing well</h3>
            <p className="section-subtitle">These criteria are already passing and contributing to your score.</p>
            <div className="category-groups">
              {didWellByCategory.map(group => (
                <div key={group.key} className="category-group">
                  <div className="category-group-title">{group.name}</div>
                  <ul className="criteria-list">
                    {group.items.slice(0, 4).map(item => (
                      <li key={item.id} className="criteria-item">
                        <span className="criteria-name">{item.name}</span>
                        <span className="criteria-points">+{item.points}</span>
                      </li>
                    ))}
                    {group.items.length > 4 && (
                      <li className="criteria-more">+{group.items.length - 4} more</li>
                    )}
                  </ul>
                </div>
              ))}
            </div>
          </section>
        )}

        <section className="card">
          <h3>How your score is calculated</h3>
          <p className="section-subtitle">Your score is the sum of all passing criteria (up to 100 points).</p>
          <div className="scoring-details">
            {categoryOrder.map((categoryKey) => {
              const label = categoryNameByKey[categoryKey] || categoryKey;
              const catFindings = findings.filter(f => f.category === categoryKey);
              if (catFindings.length === 0) return null;
              const passed = catFindings.filter(f => f.passed).length;
              return (
                <details key={categoryKey} className="scoring-category" open>
                  <summary className="scoring-summary">
                    <span className="scoring-summary-title">{label}</span>
                    <span className="scoring-summary-meta">{passed}/{catFindings.length} passing</span>
                  </summary>
                  <ul className="criteria-list">
                    {catFindings.map((f) => (
                      <li key={f.id} className={`criteria-item ${f.passed ? 'pass' : 'fail'}`}>
                        <span className="criteria-name-wrap">
                          <span className="criteria-name">{f.name}</span>
                          {(f.id === 'sample_questions_5' || f.id === 'sample_questions_10') && !f.passed && space?.raw?.debug && (
                            <span className="criteria-debug">
                              Detected sample questions: {space.raw.debug.sampleQuestionsCount ?? '-'} · source: {space.raw.debug.sampleQuestionsSource ?? '-'}
                              {space.raw.debug.genieGetOk !== null ? ` · genie_get ok: ${String(space.raw.debug.genieGetOk)}` : ''}
                              {space.raw.debug.genieGetError ? ` · genie_get error: ${space.raw.debug.genieGetError}` : ''}
                              {Array.isArray(space.raw.debug.sampleQuestionCandidates) && space.raw.debug.sampleQuestionCandidates.length > 0 ? ` · candidates: ${space.raw.debug.sampleQuestionCandidates.map(c => c.path).join(', ')}` : ''}
                              {space.raw.debug.configSampleQuestionsCount !== undefined ? ` · config.sample_questions: ${space.raw.debug.configSampleQuestionsCount}` : ''}
                              {space.raw.debug.userTokenPresent !== undefined ? ` · user token: ${String(space.raw.debug.userTokenPresent)}` : ''}
                              {space.raw.debug.serializedHasConfig !== undefined ? ` · serialized has config: ${String(space.raw.debug.serializedHasConfig)}` : ''}
                            </span>
                          )}
                        </span>
                        <span className="criteria-points">{f.passed ? `+${f.points}` : `${f.points}`}</span>
                      </li>
                    ))}
                  </ul>
                </details>
              );
            })}
          </div>
        </section>

        {nextStepsByCategory.length > 0 && (
          <section className="next-steps-section">
            <h3>Next Steps</h3>
            <p className="section-subtitle">Highest-impact improvements, grouped by category.</p>
            <div className="category-groups">
              {nextStepsByCategory.map(group => (
                <div key={group.key} className="category-group">
                  <div className="category-group-title">{group.name}</div>
                  <div className="next-steps-list">
                    {group.items.map((step, index) => (
                      <RecommendationRow key={step.id} step={step} index={index} />
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </section>
        )}
      </div>

    </div>
  );
}

export default SpaceDetail;

