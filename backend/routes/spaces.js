/**
 * Spaces Routes
 */

const { Router } = require('express');
const { scanSpace, scanAllSpaces } = require('../services/scanner.js');
const lakebase = require('../services/lakebase.js');
const { buildFixNotebook, safeSlug, isoForPath } = require('../services/notebookGenerator.js');

const router = Router();
const scanCache = new Map();

// GET /api/spaces - List scored spaces (Lakebase-backed) with server-side pagination
router.get('/', async (req, res) => {
  try {
    const scannedByMe = String(req.query.scanned_by_me || '').toLowerCase() === 'true'; // legacy
    const starredOnly = String(req.query.starred_only || '').toLowerCase() === 'true';
    const q = String(req.query.q || '').trim();
    const sort = String(req.query.sort || 'scanned_desc');
    const pageSize = Math.max(1, Math.min(50, Number(req.query.page_size || 12)));
    const page = Math.max(1, Number(req.query.page || 1));
    const offset = (page - 1) * pageSize;

    const [total, rows] = await Promise.all([
      lakebase.getLatestScoresCount({
        query: q,
        userEmail: req.user?.email,
        token: req.userToken,
        scannedByEmail: scannedByMe ? req.user?.email : null,
        starredOnly,
        starredByEmail: starredOnly ? req.user?.email : null
      }),
      lakebase.getLatestScoresPage({
        query: q,
        sort,
        limit: pageSize,
        offset,
        userEmail: req.user?.email,
        token: req.userToken,
        scannedByEmail: scannedByMe ? req.user?.email : null,
        starredOnly,
        starredByEmail: starredOnly ? req.user?.email : null
      })
    ]);

    const ids = (Array.isArray(rows) ? rows : []).map((r) => String(r.space_id)).filter(Boolean);
    const starMap = await lakebase.getStarMapForSpaceIds({ spaceIds: ids, userEmail: req.user?.email, token: req.userToken });

    const spaces = (Array.isArray(rows) ? rows : []).map((r) => ({
      id: r.space_id,
      name: r.space_name,
      description: r.space_description || '',
      owner: r.owner_email || null,
      warehouse: r.warehouse_id
        ? { id: r.warehouse_id, name: r.warehouse_name || null, type: r.warehouse_type || null, serverless: r.is_serverless ?? null }
        : null,
      totalScore: r.total_score ?? null,
      maturityLevel: r.maturity_level ?? null,
      scannedAt: r.scanned_at ?? null,
      scannedBy: r.scanned_by ?? null,
      starred: starMap.get(String(r.space_id)) || false
    }));

    res.json({ spaces, page, pageSize, total });
  } catch (error) {
    console.error('Error listing spaces:', error);
    res.status(500).json({ error: 'Failed to list spaces', details: error.message });
  }
});

// GET /api/spaces/all - List ALL Genie spaces with cursor pagination (no full scan)
router.get('/all', async (req, res) => {
  try {
    const scannedByMe = String(req.query.scanned_by_me || '').toLowerCase() === 'true'; // legacy
    const starredOnly = String(req.query.starred_only || '').toLowerCase() === 'true';
    const newOnly = String(req.query.new_only || '').toLowerCase() === 'true';
    const newDays = req.query.new_days != null ? Number(req.query.new_days) : 7;
    const pageToken = req.query.page_token ? String(req.query.page_token) : null;
    const pageSize = Math.max(1, Math.min(200, Number(req.query.page_size || 50)));

    const resp = await req.databricks.listGenieSpacesPage({ pageToken, pageSize });
    const spaces = resp?.spaces || resp?.rooms || [];
    const nextPageToken = resp?.next_page_token || resp?.nextPageToken || null;

    const ids = (Array.isArray(spaces) ? spaces : [])
      .map((s) => s?.id || s?.space_id || s?.room_id)
      .filter(Boolean);

    // Record "first seen" timestamps (fast single upsert per page).
    try {
      await lakebase.upsertSpacesSeen({
        spaces: (Array.isArray(spaces) ? spaces : []).map((s) => ({
          id: s?.id || s?.space_id || s?.room_id,
          name: s?.name || s?.title || null
        })),
        userEmail: req.user?.email,
        token: req.userToken
      });
    } catch {
      // ignore
    }

    let latest = [];
    try {
      latest = await lakebase.getLatestScoresBySpaceIds(ids, { userEmail: req.user?.email, token: req.userToken });
    } catch {
      latest = [];
    }

    const latestById = new Map();
    for (const r of Array.isArray(latest) ? latest : []) {
      const sid = r?.space_id || r?.id || r?.spaceId;
      if (!sid) continue;
      latestById.set(String(sid), r);
    }

    let seenRows = [];
    try {
      seenRows = await lakebase.getSpacesSeenByIds({ spaceIds: ids, userEmail: req.user?.email, token: req.userToken });
    } catch {
      seenRows = [];
    }
    const seenById = new Map((Array.isArray(seenRows) ? seenRows : []).map((r) => [String(r.space_id), r]));

    const out = (Array.isArray(spaces) ? spaces : []).map((s) => {
      const id = s?.id || s?.space_id || s?.room_id;
      const name = s?.name || s?.title || 'Untitled';
      const row = id ? (latestById.get(String(id)) || scanCache.get(String(id)) || null) : null;
      const seen = id ? seenById.get(String(id)) : null;

      return {
        id,
        name,
        description: s?.description || '',
        // Genie list endpoints often omit owner/creator; prefer Lakebase if available.
        owner: s?.owner || s?.creator_email || s?.creator || row?.owner_email || null,
        warehouse: s?.warehouse_id
          ? { id: s.warehouse_id, name: row?.warehouse_name || null, type: row?.warehouse_type || null, serverless: row?.is_serverless ?? null }
          : null,
        totalScore: row?.total_score ?? row?.totalScore ?? null,
        maturityLevel: row?.maturity_level ?? row?.maturityLevel ?? null,
        scannedAt: row?.scanned_at ?? row?.scannedAt ?? null,
        scannedBy: row?.scanned_by ?? row?.scannedBy ?? null,
        starred: false,
        firstSeenAt: seen?.first_seen_at || null
      };
    });

    const starIds = out.map((s) => String(s.id)).filter(Boolean);
    const starMap = await lakebase.getStarMapForSpaceIds({ spaceIds: starIds, userEmail: req.user?.email, token: req.userToken });
    for (const s of out) {
      s.starred = starMap.get(String(s.id)) || false;
    }

    let filtered = out;
    if (scannedByMe) {
      filtered = filtered.filter((s) => String(s?.scannedBy || '').toLowerCase() === String(req.user?.email || '').toLowerCase());
    }
    if (starredOnly) {
      filtered = filtered.filter((s) => Boolean(s.starred));
    }
    if (newOnly) {
      const cutoff = Date.now() - Math.max(1, Number(newDays) || 7) * 24 * 60 * 60 * 1000;
      filtered = filtered.filter((s) => {
        if (!s.firstSeenAt) return false;
        const t = Date.parse(s.firstSeenAt);
        return Number.isFinite(t) && t >= cutoff;
      });
    }
    res.json({ spaces: filtered, pageSize, nextPageToken });
  } catch (error) {
    console.error('Error listing all spaces:', error);
    res.status(500).json({ error: 'Failed to list all spaces', details: error.message });
  }
});

// GET /api/spaces/new?days=7&limit=10 - "New spaces recently added" (based on first_seen_at)
router.get('/new', async (req, res) => {
  try {
    const days = req.query.days != null ? Number(req.query.days) : 7;
    const limit = req.query.limit != null ? Number(req.query.limit) : 10;
    const rows = await lakebase.getNewSpaces({ days, limit, userEmail: req.user?.email, token: req.userToken });

    // Attach latest score if available (best-effort), so the list feels richer.
    const ids = (rows || []).map((r) => String(r.space_id)).filter(Boolean);
    let latest = [];
    try {
      latest = await lakebase.getLatestScoresBySpaceIds(ids, { userEmail: req.user?.email, token: req.userToken });
    } catch {
      latest = [];
    }
    const latestById = new Map();
    for (const r of Array.isArray(latest) ? latest : []) {
      const sid = r?.space_id || r?.id || r?.spaceId;
      if (!sid) continue;
      latestById.set(String(sid), r);
    }

    const starMap = await lakebase.getStarMapForSpaceIds({ spaceIds: ids, userEmail: req.user?.email, token: req.userToken });

    const spaces = (rows || []).map((r) => {
      const sid = String(r.space_id);
      const row = latestById.get(sid) || null;
      return {
        id: sid,
        name: r.last_name || sid,
        firstSeenAt: r.first_seen_at || null,
        starred: starMap.get(sid) || false,
        totalScore: row?.total_score ?? row?.totalScore ?? null,
        maturityLevel: row?.maturity_level ?? row?.maturityLevel ?? null,
        scannedAt: row?.scanned_at ?? row?.scannedAt ?? null
      };
    });

    res.json({ days: Math.max(1, Number(days) || 7), spaces });
  } catch (error) {
    res.json({ days: 7, spaces: [] });
  }
});

// PUT /api/spaces/:id/star  { starred: true|false }
router.put('/:id/star', async (req, res) => {
  try {
    const userEmail = req.user?.email;
    if (!userEmail) return res.status(401).json({ error: 'Not authenticated' });
    const spaceId = String(req.params.id);
    const starred = Boolean(req.body?.starred);
    const result = await lakebase.setSpaceStar({ spaceId, userEmail, token: req.userToken, starred });
    res.json(result);
  } catch (error) {
    res.status(500).json({ error: 'Failed to update star', details: error.message });
  }
});

// GET /api/spaces/:id - Get specific space
router.get('/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { refresh } = req.query;
    
    if (!refresh && scanCache.has(id)) {
      const cached = scanCache.get(id);
      try {
        const starMap = await lakebase.getStarMapForSpaceIds({ spaceIds: [id], userEmail: req.user?.email, token: req.userToken });
        return res.json({ ...cached, starred: starMap.get(String(id)) || false });
      } catch {
        return res.json({ ...cached, starred: false });
      }
    }
    
    const forceRefreshUc = String(refresh || '').toLowerCase() === 'true';
    const startTime = Date.now();
    const result = await scanSpace(req.databricks, id, { forceRefreshUc });
    result.scanDuration = Date.now() - startTime;
    result.scannedBy = req.user?.email || 'view';

    // Best-effort: persist the computed score so it shows up in the fast "Scored" list.
    try {
      result._dbToken = req.userToken;
      await lakebase.saveScanResult(result);
    } catch {
      // ignore
    }
    scanCache.set(id, result);
    try {
      const starMap = await lakebase.getStarMapForSpaceIds({ spaceIds: [id], userEmail: req.user?.email, token: req.userToken });
      result.starred = starMap.get(String(id)) || false;
    } catch {
      result.starred = false;
    }

    res.json(result);
  } catch (error) {
    console.error(`Error getting space ${req.params.id}:`, error);
    res.status(500).json({ error: 'Failed to get space details', details: error.message });
  }
});

// POST /api/spaces/:id/scan - Fresh scan
router.post('/:id/scan', async (req, res) => {
  try {
    const { id } = req.params;
    const startTime = Date.now();
    
    // User-triggered scan: force-refresh UC metadata so recent description edits are reflected immediately.
    const result = await scanSpace(req.databricks, id, { forceRefreshUc: true });
    result.scanDuration = Date.now() - startTime;
    result.scannedBy = req.user?.email || 'manual';
    
    let previousScore = scanCache.get(id)?.totalScore || null;
    const scoreDelta = previousScore !== null ? result.totalScore - previousScore : null;
    
    try {
      result._dbToken = req.userToken;
      await lakebase.saveScanResult(result);
    } catch (e) {
      console.warn('Could not save to Lakebase:', e.message);
    }
    
    scanCache.set(id, result);
    try {
      const starMap = await lakebase.getStarMapForSpaceIds({ spaceIds: [id], userEmail: req.user?.email, token: req.userToken });
      result.starred = starMap.get(String(id)) || false;
    } catch {
      result.starred = false;
    }
    
    res.json({ ...result, scoreDelta, previousScore });
  } catch (error) {
    console.error(`Error scanning space ${req.params.id}:`, error);
    res.status(500).json({ error: 'Failed to scan space', details: error.message });
  }
});

// GET /api/spaces/:id/history
router.get('/:id/history', async (req, res) => {
  try {
    const limit = req.query.limit != null ? Number(req.query.limit) : 90;
    const days = req.query.days != null ? Number(req.query.days) : 30;
    const history = await lakebase.getScoreHistory(req.params.id, {
      limit,
      days,
      userEmail: req.user?.email,
      token: req.userToken
    });
    res.json({ spaceId: req.params.id, history });
  } catch (error) {
    res.json({ spaceId: req.params.id, history: [] });
  }
});

// POST /api/spaces/:id/fix-notebook - Generate a remediation notebook for all failing items
router.post('/:id/fix-notebook', async (req, res) => {
  try {
    const { id } = req.params;
    const scan = await scanSpace(req.databricks, id, { forceRefreshUc: true });

    const userEmail = req.user?.email || 'unknown';
    const filename = `${safeSlug(scan.name)}-fixes-${isoForPath()}.ipynb`;
    const preferredDir = `/Users/${userEmail}/GenieIQ Fixes`;
    const fallbackDir = `/Shared/GenieIQ Fixes/${userEmail}`;

    const notebookJson = buildFixNotebook({
      scanResult: scan,
      workspaceHost: process.env.DATABRICKS_HOST
    });

    const importNotebookToDir = async (dir) => {
      const notebookPath = `${dir}/${filename}`;
      await req.databricks.workspaceMkdirs(dir);
      await req.databricks.workspaceImport({
        path: notebookPath,
        contentBase64: Buffer.from(notebookJson, 'utf8').toString('base64'),
        format: 'JUPYTER',
        overwrite: true
      });
      return notebookPath;
    };

    // Ensure folder exists then import notebook.
    // Prefer per-user location, but fall back to /Shared if the app lacks permissions in /Users/<email>.
    let notebookPath;
    try {
      notebookPath = await importNotebookToDir(preferredDir);
    } catch (e) {
      console.warn('Could not create notebook in user folder, falling back to /Shared:', e.message);
      notebookPath = await importNotebookToDir(fallbackDir);
    }

    const host = process.env.DATABRICKS_HOST
      ? (process.env.DATABRICKS_HOST.startsWith('http') ? process.env.DATABRICKS_HOST : `https://${process.env.DATABRICKS_HOST}`)
      : null;
    const url = host ? `${host}/#workspace${notebookPath}` : null;

    res.json({
      success: true,
      notebookPath,
      notebookUrl: url
    });
  } catch (error) {
    console.error('Error generating fix notebook:', error);
    res.status(500).json({
      error: 'Failed to generate fix notebook',
      details: error.message
    });
  }
});

module.exports = router;
