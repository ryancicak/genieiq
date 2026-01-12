import { useEffect, useMemo, useState } from 'react';
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

function fmtDateTime(iso) {
  try {
    const d = new Date(iso);
    return d.toLocaleString(undefined, {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
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

function reduceToScans(pointsDesc) {
  // pointsDesc is newest->oldest. Return oldest->newest for plotting left-to-right.
  const pts = (Array.isArray(pointsDesc) ? pointsDesc : [])
    .filter((p) => p && p.scanned_at)
    .slice()
    .sort((a, b) => Date.parse(a.scanned_at) - Date.parse(b.scanned_at));
  return pts;
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
  const [hoveredIdx, setHoveredIdx] = useState(null);

  useEffect(() => {
    if (!open) return;
    const onKey = (e) => {
      if (e.key === 'Escape') onClose?.();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, onClose]);

  const stats = useMemo(() => computeStats(points), [points]);
  const scans = useMemo(() => reduceToScans(points), [points]);
  const chart = useMemo(() => buildAreaPath(scans.map((p) => p.total_score), 980, 300, 18), [scans]);

  const pointXY = useMemo(() => {
    if (!chart?.xy?.length) return [];
    if (scans.length <= 1) return [{ x: chart.xy[0].x, y: chart.xy[0].y }];
    return chart.xy.slice(0, scans.length);
  }, [chart, scans.length]);

  const hovered = hoveredIdx == null ? null : scans[hoveredIdx];
  const hoveredPos = hoveredIdx == null ? null : pointXY[hoveredIdx];
  const hoveredScore = hovered ? Number(hovered.total_score ?? 0) : null;
  const hoveredPrev = hoveredIdx != null && hoveredIdx > 0 ? scans[hoveredIdx - 1] : null;
  const hoveredDelta = hoveredPrev ? (hoveredScore - Number(hoveredPrev.total_score ?? 0)) : null;

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
              <div className="shm-chart-title">Last scans (30 days)</div>
              <div className="shm-chart-sub">Score changes across scans</div>
            </div>

            <div className="shm-chart-stage" aria-label="Score trend chart" onMouseLeave={() => setHoveredIdx(null)}>
              <div className="shm-chart-surface">
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
                {!loading && pointXY.length ? (
                  <>
                    {pointXY.map((p, i) => (
                      <g key={i}>
                        {/* hit area */}
                        <circle
                          cx={p.x}
                          cy={p.y}
                          r="10"
                          className="shm-point-hit"
                          onMouseEnter={() => setHoveredIdx(i)}
                        />
                        {/* visible dot */}
                        <circle
                          cx={p.x}
                          cy={p.y}
                          r={hoveredIdx === i ? '4.5' : '3.0'}
                          className={`shm-point ${hoveredIdx === i ? 'active' : ''}`}
                        />
                      </g>
                    ))}
                  </>
                ) : null}
                </svg>
              </div>

              {!loading && hovered && hoveredPos && (
                <div
                  className="shm-tooltip"
                  style={{
                    left: `${(hoveredPos.x / 980) * 100}%`,
                    top: `${(hoveredPos.y / 300) * 100}%`
                  }}
                >
                  <div className="shm-tooltip-title">{fmtDateTime(hovered.scanned_at)}</div>
                  <div className="shm-tooltip-row">
                    <span>Score</span>
                    <strong>{hoveredScore}</strong>
                  </div>
                  {hoveredDelta != null && (
                    <div className="shm-tooltip-row">
                      <span>Change</span>
                      <strong>{hoveredDelta > 0 ? `+${hoveredDelta}` : `${hoveredDelta}`}</strong>
                    </div>
                  )}
                </div>
              )}

              <div className="shm-xlabels">
                {scans.length > 1 ? (
                  <>
                    <span>{fmtDateLabel(scans[0]?.scanned_at)}</span>
                    <span>{fmtDateLabel(scans[Math.floor(scans.length / 2)]?.scanned_at)}</span>
                    <span>{fmtDateLabel(scans[scans.length - 1]?.scanned_at)}</span>
                  </>
                ) : (
                  <span>{scans[0]?.scanned_at ? fmtDateLabel(scans[0]?.scanned_at) : ''}</span>
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

