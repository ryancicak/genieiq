/**
 * Admin Routes
 */

const { Router } = require('express');
const { requireAdmin } = require('../middleware/auth.js');
const { scanSpace } = require('../services/scanner.js');
const { getStatus } = require('../config/scoring-rubric.js');
const lakebase = require('../services/lakebase.js');
const crypto = require('crypto');

const router = Router();
router.use(requireAdmin);

// In-memory background scan jobs (best-effort). Good enough for Databricks Apps single-instance.
const scanJobs = new Map(); // jobId -> job state

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function publicJobState(job) {
  if (!job) return null;
  const { id, status, startedAt, finishedAt, total, completed, errors, lastError, lastScannedSpace } = job;
  return { id, status, startedAt, finishedAt, total, completed, errors, lastError, lastScannedSpace };
}

async function runScanAllJob({ job, databricksClient, options }) {
  job.status = 'running';
  job.startedAt = new Date().toISOString();
  job.completed = 0;
  job.errors = 0;
  job.lastError = null;
  job.lastScannedSpace = null;

  const concurrency = Math.max(1, Math.min(10, Number(options.concurrency || 2)));
  const delayMs = Math.max(0, Math.min(5_000, Number(options.delayMs || 250)));
  const limit = options.limit ? Math.max(1, Number(options.limit)) : null;

  // Fetch full space list (DatabricksClient.listGenieSpaces is paginated)
  const listResp = await databricksClient.listGenieSpaces();
  const spaces = listResp?.spaces || listResp?.rooms || [];
  const ids = (Array.isArray(spaces) ? spaces : [])
    .map((s) => s?.id || s?.space_id || s?.room_id)
    .filter(Boolean);

  const queue = limit ? ids.slice(0, limit) : ids;
  job.total = queue.length;

  let cursor = 0;
  const nextId = () => {
    if (cursor >= queue.length) return null;
    const id = queue[cursor];
    cursor += 1;
    return id;
  };

  const worker = async () => {
    while (true) {
      if (job.status !== 'running') return;
      const spaceId = nextId();
      if (!spaceId) return;

      try {
        const result = await scanSpace(databricksClient, spaceId, { forceRefreshUc: false });
        result.scannedBy = job.requestedBy || 'admin';
        result.scanDuration = null;
        job.lastScannedSpace = { id: result.id, name: result.name, score: result.totalScore };

        try {
          // Use the requesting admin's proxy token (PGPASSWORD-style) for Lakebase Postgres auth.
          result._dbToken = job.dbToken || null;
          await lakebase.saveScanResult(result);
        } catch (e) {
          // Non-fatal: history persistence optional
        }
      } catch (e) {
        job.errors += 1;
        job.lastError = String(e?.message || e).slice(0, 200);
      } finally {
        job.completed += 1;
      }

      if (delayMs) await sleep(delayMs);
    }
  };

  await Promise.all(Array.from({ length: concurrency }, worker));

  if (job.status === 'running') {
    job.status = 'completed';
    job.finishedAt = new Date().toISOString();
  }
}

// GET /api/admin/dashboard
router.get('/dashboard', async (req, res) => {
  try {
    // Fast dashboard: read from Lakebase latest_scores (no live scanning).
    const dbPool = await lakebase.getPool({ userEmail: req.user?.email, token: req.userToken });
    if (!dbPool) {
      return res.json({
        stats: { totalSpaces: 0, avgScore: 0, criticalCount: 0, warehouseAttentionCount: 0, lastScanTime: null },
        needsAttention: [],
        allSpaces: [],
        note: 'Lakebase not configured; run scans to populate history.'
      });
    }

    const statsRes = await dbPool.query(`
      SELECT
        COUNT(*)::int AS total_spaces,
        COALESCE(ROUND(AVG(total_score))::int, 0) AS avg_score,
        COUNT(*) FILTER (WHERE total_score < 40)::int AS critical_count,
        COUNT(*) FILTER (
          WHERE EXISTS (
            SELECT 1
            FROM jsonb_array_elements(COALESCE(findings, '[]'::jsonb)) f
            WHERE (f->>'id') IN ('dedicated_warehouse', 'warehouse_size_latency')
              AND COALESCE((f->>'passed')::boolean, false) = false
          )
        )::int AS warehouse_attention_count,
        MAX(scanned_at) AS last_scan_time
      FROM latest_scores
    `);

    const s = statsRes.rows?.[0] || {};
    const stats = {
      totalSpaces: s.total_spaces ?? 0,
      avgScore: s.avg_score ?? 0,
      criticalCount: s.critical_count ?? 0,
      warehouseAttentionCount: s.warehouse_attention_count ?? 0,
      lastScanTime: s.last_scan_time ?? null
    };

    const needsRes = await dbPool.query(`
      SELECT space_id, space_name, owner_email, total_score, maturity_level
      FROM latest_scores
      WHERE total_score < 50
      ORDER BY total_score ASC NULLS LAST, scanned_at DESC
      LIMIT 5
    `);

    const needsAttention = (needsRes.rows || []).map((r) => ({
      id: r.space_id,
      name: r.space_name,
      owner: r.owner_email,
      totalScore: r.total_score,
      maturityLevel: r.maturity_level,
      status: getStatus(r.total_score)
    }));

    const allRes = await dbPool.query(`
      SELECT space_id, space_name, owner_email, total_score, maturity_level
      FROM latest_scores
      ORDER BY total_score DESC NULLS LAST, scanned_at DESC
      LIMIT 200
    `);

    const allSpaces = (allRes.rows || []).map((r) => ({
      id: r.space_id,
      name: r.space_name,
      owner: r.owner_email,
      totalScore: r.total_score,
      maturityLevel: r.maturity_level,
      status: getStatus(r.total_score)
    }));

    res.json({ stats, needsAttention, allSpaces });
  } catch (error) {
    console.error('Error getting admin dashboard:', error);
    res.status(500).json({ error: 'Failed to load dashboard', details: error.message });
  }
});

// GET /api/admin/leaderboard
router.get('/leaderboard', async (req, res) => {
  try {
    const dbPool = await lakebase.getPool({ userEmail: req.user?.email, token: req.userToken });
    if (!dbPool) return res.json({ top: [], bottom: [] });

    const topRes = await dbPool.query(`
      SELECT space_id as id, space_name as name, owner_email as owner, total_score as "totalScore", maturity_level as "maturityLevel"
      FROM latest_scores
      ORDER BY total_score DESC NULLS LAST, scanned_at DESC
      LIMIT 5
    `);
    const bottomRes = await dbPool.query(`
      SELECT space_id as id, space_name as name, owner_email as owner, total_score as "totalScore", maturity_level as "maturityLevel"
      FROM latest_scores
      ORDER BY total_score ASC NULLS LAST, scanned_at DESC
      LIMIT 5
    `);

    res.json({
      top: topRes.rows || [],
      bottom: bottomRes.rows || []
    });
  } catch (error) {
    console.error('Error getting leaderboard:', error);
    res.status(500).json({ error: 'Failed to load leaderboard' });
  }
});

// POST /api/admin/scan-all-job
// Kicks off a paced background scan over all Genie spaces and returns a job id + progress endpoint.
router.post('/scan-all-job', async (req, res) => {
  try {
    const jobId = crypto.randomUUID();
    const options = req.body && typeof req.body === 'object' ? req.body : {};

    const job = {
      id: jobId,
      status: 'queued',
      requestedBy: req.user?.email || 'admin',
      dbToken: req.userToken || null,
      startedAt: null,
      finishedAt: null,
      total: 0,
      completed: 0,
      errors: 0,
      lastError: null,
      lastScannedSpace: null
    };
    scanJobs.set(jobId, job);

    // Fire-and-forget background worker
    Promise.resolve()
      .then(() => runScanAllJob({ job, databricksClient: req.databricks, options }))
      .catch((e) => {
        job.status = 'failed';
        job.finishedAt = new Date().toISOString();
        job.lastError = String(e?.message || e).slice(0, 200);
      });

    res.json({ success: true, job: publicJobState(job) });
  } catch (error) {
    console.error('Error starting scan job:', error);
    res.status(500).json({ error: 'Failed to start scan job', details: error.message });
  }
});

// GET /api/admin/scan-all-job/:id
router.get('/scan-all-job/:id', async (req, res) => {
  const job = scanJobs.get(String(req.params.id));
  if (!job) return res.status(404).json({ error: 'Job not found' });
  res.json({ job: publicJobState(job) });
});

// GET /api/admin/alerts
router.get('/alerts', async (req, res) => {
  try {
    const dbPool = await lakebase.getPool({ userEmail: req.user?.email, token: req.userToken });
    if (!dbPool) return res.json({ alerts: [] });

    // Alert heuristic from history: critical score OR non-serverless (shared warehouse behavior approximated).
    const resAlerts = await dbPool.query(`
      SELECT space_id, space_name, owner_email, total_score, maturity_level, is_serverless
      FROM latest_scores
      WHERE total_score < 40 OR (is_serverless = false)
      ORDER BY total_score ASC NULLS LAST, scanned_at DESC
      LIMIT 50
    `);

    const alerts = (resAlerts.rows || []).map((r) => ({
      spaceId: r.space_id,
      spaceName: r.space_name,
      owner: r.owner_email,
      score: r.total_score,
      status: getStatus(r.total_score),
      issues: []
    }));

    res.json({ alerts });
  } catch (error) {
    console.error('Error getting alerts:', error);
    res.status(500).json({ error: 'Failed to load alerts' });
  }
});

module.exports = router;
