import { useEffect, useMemo } from 'react';
import './ScoreHistoryModal.css';

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function fmtDateLabel(iso) {
  try {
    const d = new Date(iso);
    return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
  } catch {
    return '';
  }
}

function computeStats(points) {
  const scores = points.map((p) => Number(p.total_score ?? 0)).filter((n) => Number.isFinite(n));
  const current = scores[0] ?? 0;
  const previous = scores[1] ?? null;
  const delta = previous == null ? null : current - previous;
  const deltaPct = previous && previous !== 0 ? (delta / previous) * 100 : null;
  const peak = scores.length ? Math.max(...scores) : 0;
  const low = scores.length ? Math.min(...scores) : 0;
  const avg = scores.length ? Math.round(scores.reduce((a, b) => a + b, 0) / scores.length) : 0;
  return { current, previous, delta, deltaPct, peak, low, avg };
}

function reduceToDaily(pointsDesc) {
  // pointsDesc is newest->oldest. Keep latest scan per day.
  const byDay = new Map();
  for (const p of pointsDesc) {
    const day = p.scan_date || (p.scanned_at ? String(p.scanned_at).slice(0, 10) : null);
    if (!day) continue;
    if (!byDay.has(day)) byDay.set(day, p);
  }
  // Return oldest->newest for plotting left-to-right
  return Array.from(byDay.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([, p]) => p);
}

function buildAreaPath(values, w, h, padding = 10) {
  const pts = values.map((v) => clamp(Number(v ?? 0), 0, 100));
  if (pts.length === 0) return { line: '', area: '' };
  const innerW = w - padding * 2;
  const innerH = h - padding * 2;
  // Special-case single point so we don't draw a misleading triangle.
  const xy = (() => {
    if (pts.length === 1) {
      const y = padding + innerH * (1 - pts[0] / 100);
      return [
        { x: padding, y },
        { x: padding + innerW, y }
      ];
    }
    const step = innerW / (pts.length - 1);
    return pts.map((v, i) => {
      const x = padding + i * step;
      const y = padding + innerH * (1 - v / 100);
      return { x, y };
    });
  })();
  const line = `M ${xy.map((p) => `${p.x.toFixed(1)} ${p.y.toFixed(1)}`).join(' L ')}`;
  const area = `${line} L ${(padding + innerW).toFixed(1)} ${(padding + innerH).toFixed(1)} L ${padding.toFixed(1)} ${(padding + innerH).toFixed(1)} Z`;
  return { line, area, xy };
}

export default function ScoreHistoryModal({
  open,
  spaceName,
  points = [],
  loading = false,
  error = null,
  onClose
}) {
  useEffect(() => {
    if (!open) return;
    const onKey = (e) => {
      if (e.key === 'Escape') onClose?.();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, onClose]);

  const stats = useMemo(() => computeStats(points), [points]);
  const daily = useMemo(() => reduceToDaily(points), [points]);
  const chart = useMemo(() => buildAreaPath(daily.map((p) => p.total_score), 980, 300, 18), [daily]);

  if (!open) return null;

  return (
    <div className="shm-overlay" role="dialog" aria-modal="true" aria-label="Score history">
      <div className="shm-backdrop" onClick={onClose} />
      <div className="shm-modal">
        <div className="shm-header">
          <div>
            <div className="shm-title">Score History</div>
            <div className="shm-subtitle">{spaceName || 'Genie Space'}</div>
          </div>
          <button type="button" className="shm-close" onClick={onClose} aria-label="Close">
            ×
          </button>
        </div>

        <div className="shm-body">
          {error && (
            <div className="alert alert-error" role="status" aria-live="polite">
              <div>
                <div className="alert-title">Couldn’t load history</div>
                <div className="alert-body">{String(error)}</div>
              </div>
            </div>
          )}

          <div className="shm-cards">
            <div className="shm-card">
              <div className="shm-kicker">Current score</div>
              <div className="shm-score">{loading ? '-' : stats.current}</div>
              <div className={`shm-delta ${stats.delta != null && stats.delta < 0 ? 'neg' : 'pos'}`}>
                {loading || stats.delta == null ? ' ' : (
                  <>
                    {stats.delta > 0 ? `+${stats.delta}` : `${stats.delta}`} {stats.deltaPct != null ? `(${stats.deltaPct.toFixed(1)}%)` : ''}
                  </>
                )}
              </div>
            </div>

            <div className="shm-card">
              <div className="shm-kicker">30-day average</div>
              <div className="shm-score">{loading ? '-' : stats.avg}</div>
              <div className="shm-note">across scans</div>
            </div>

            <div className="shm-card accent">
              <div className="shm-kicker">Peak score</div>
              <div className="shm-score">{loading ? '-' : stats.peak}</div>
              <div className="shm-note">highest recorded</div>
            </div>

            <div className="shm-card">
              <div className="shm-kicker">Low point</div>
              <div className="shm-score">{loading ? '-' : stats.low}</div>
              <div className="shm-note">needs improvement</div>
            </div>
          </div>

          <div className="shm-chart card">
            <div className="shm-chart-head">
              <div className="shm-chart-title">30-Day Trend</div>
              <div className="shm-chart-sub">Daily score progression</div>
            </div>

            <div className="shm-chart-wrap" aria-label="Score trend chart">
              <svg viewBox="0 0 980 300" className="shm-svg" preserveAspectRatio="none">
                <defs>
                  <linearGradient id="shmFill" x1="0" x2="0" y1="0" y2="1">
                    <stop offset="0%" stopColor="rgba(34, 211, 238, 0.28)" />
                    <stop offset="100%" stopColor="rgba(34, 211, 238, 0.00)" />
                  </linearGradient>
                </defs>

                {/* Bands */}
                {[75, 50, 25].map((v) => {
                  const y = 18 + (300 - 36) * (1 - v / 100);
                  return <line key={v} x1="18" x2="962" y1={y} y2={y} className={`shm-band shm-band-${v}`} />;
                })}

                {/* Trend */}
                {!loading && chart.area && <path d={chart.area} fill="url(#shmFill)" />}
                {!loading && chart.line && <path d={chart.line} className="shm-line" fill="none" />}
                {!loading && chart.xy?.length ? (
                  <circle
                    cx={chart.xy[chart.xy.length - 1].x}
                    cy={chart.xy[chart.xy.length - 1].y}
                    r="3.5"
                    fill="rgba(34, 211, 238, 0.95)"
                  />
                ) : null}
              </svg>

              <div className="shm-xlabels">
                {daily.length > 1 ? (
                  <>
                    <span>{fmtDateLabel(daily[0]?.scanned_at)}</span>
                    <span>{fmtDateLabel(daily[Math.floor(daily.length / 2)]?.scanned_at)}</span>
                    <span>{fmtDateLabel(daily[daily.length - 1]?.scanned_at)}</span>
                  </>
                ) : (
                  <span>{daily[0]?.scanned_at ? fmtDateLabel(daily[0]?.scanned_at) : ''}</span>
                )}
              </div>
            </div>

            <div className="shm-legend">
              <span className="shm-pill ok">Optimized (75+)</span>
              <span className="shm-pill mid">Maturing (50–74)</span>
              <span className="shm-pill bad">Needs Work (&lt;50)</span>
              <span className="shm-pill line">Score Trend</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

