/**
 * Lakebase (PostgreSQL) Client
 * 
 * Connects to Databricks Lakebase Provisioned for storing audit results.
 * Uses OAuth token authentication for Databricks Lakebase.
 */

const pg = require('pg');
const { Pool } = pg;
const crypto = require('crypto');

let pool = null;
let useInMemory = false;
let lastLakebaseFailures = null;
let inMemoryStore = {
  audit_results: [],
  space_stars: [], // { space_id, user_email, starred_at }
  spaces_seen: [] // { space_id, first_seen_at, last_seen_at, last_name }
};

// Per-user pool cache for Postgres OAuth auth (token-as-password).
// Map<key, { pool: Pool, expiry: number }>
const poolCache = new Map();

function decodeJwtClaims(token) {
  try {
    const parts = String(token || '').split('.');
    if (parts.length < 2) return null;
    const payload = parts[1].replace(/-/g, '+').replace(/_/g, '/');
    const padded = payload + '='.repeat((4 - (payload.length % 4)) % 4);
    const json = Buffer.from(padded, 'base64').toString('utf8');
    return JSON.parse(json);
  } catch {
    return null;
  }
}

function tokenHasDbScope(claims) {
  const scope = String(claims?.scope || '').toLowerCase();
  if (!scope) return false;
  const parts = scope.split(/\s+/g).filter(Boolean);
  return parts.includes('database') || parts.includes('lakebase');
}

function identityFromClaims(claims) {
  // Best-effort: Databricks identity login expects the username to match the token identity.
  // For user tokens, `sub` is typically the user email.
  // For service principal tokens, `sub` may be absent and `client_id` is present.
  return (
    claims?.sub ||
    claims?.email ||
    claims?.user ||
    claims?.client_id ||
    null
  );
}

async function getAppClientCredentialsToken({ scope = 'all-apis' } = {}) {
  const clientId = process.env.DATABRICKS_CLIENT_ID;
  const clientSecret = process.env.DATABRICKS_CLIENT_SECRET;
  const hostEnv = process.env.DATABRICKS_HOST;
  if (!clientId || !clientSecret || !hostEnv) return null;
  const baseUrl = hostEnv.startsWith('http') ? hostEnv : `https://${hostEnv}`;
  const tokenUrl = `${baseUrl}/oidc/v1/token`;
  try {
    const response = await fetch(tokenUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'client_credentials',
        client_id: clientId,
        client_secret: clientSecret,
        scope
      })
    });
    if (!response.ok) return null;
    const data = await response.json().catch(() => ({}));
    return data?.access_token || null;
  } catch {
    return null;
  }
}

async function getDatabaseCredentialToken({ instanceName }) {
  const hostEnv = process.env.DATABRICKS_HOST;
  if (!hostEnv || !instanceName) return null;
  const baseUrl = hostEnv.startsWith('http') ? hostEnv : `https://${hostEnv}`;
  const accessToken = await getAppClientCredentialsToken({ scope: 'all-apis' });
  if (!accessToken) return null;
  try {
    const response = await fetch(`${baseUrl}/api/2.0/database/credentials`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${accessToken}`
      },
      body: JSON.stringify({
        instance_names: [instanceName],
        request_id: crypto.randomUUID()
      })
    });
    if (!response.ok) return null;
    const data = await response.json().catch(() => ({}));
    return data?.token || null;
  } catch {
    return null;
  }
}

function hashToken(token) {
  try {
    return crypto.createHash('sha256').update(String(token || '')).digest('hex').slice(0, 16);
  } catch {
    return null;
  }
}

function normalizeInMemoryLatestRow(r) {
  if (!r || typeof r !== 'object') return null;
  // In-memory records store the raw scan result shape (camelCase). Normalize to
  // the same snake_case shape returned by the `latest_scores` SQL view.
  const spaceId = r.space_id || r.spaceId || r.id;
  if (!spaceId) return null;

  return {
    space_id: String(spaceId),
    space_name: r.space_name || r.spaceName || r.name || '',
    owner_email: r.owner_email || r.owner || null,
    total_score: r.total_score ?? r.totalScore ?? null,
    maturity_level: r.maturity_level || r.maturityLevel || null,
    scanned_at: r.scanned_at || r.scannedAt || null,
    scanned_by: r.scanned_by || r.scannedBy || null,
    warehouse_id: r.warehouse_id ?? r.warehouse?.id ?? null,
    warehouse_name: r.warehouse_name ?? r.warehouse?.name ?? null,
    warehouse_type: r.warehouse_type ?? r.warehouse?.type ?? null,
    is_serverless: r.is_serverless ?? r.warehouse?.serverless ?? null,
    // Optional fields (not all callers use these)
    space_description: r.space_description || r.description || ''
  };
}

// SQL Setup Script
const SETUP_SQL = `
-- Needed for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Main audit results table
CREATE TABLE IF NOT EXISTS audit_results (
  id                  SERIAL PRIMARY KEY,
  scan_id             UUID NOT NULL DEFAULT gen_random_uuid(),
  space_id            VARCHAR(255) NOT NULL,
  space_name          VARCHAR(255) NOT NULL,
  space_description   TEXT,
  owner_email         VARCHAR(255),
  total_score         INTEGER NOT NULL,
  maturity_level      VARCHAR(50) NOT NULL,
  foundation_score    INTEGER,
  foundation_max      INTEGER DEFAULT 30,
  data_setup_score    INTEGER,
  data_setup_max      INTEGER DEFAULT 25,
  sql_assets_score    INTEGER,
  sql_assets_max      INTEGER DEFAULT 25,
  optimization_score  INTEGER,
  optimization_max    INTEGER DEFAULT 20,
  breakdown           JSONB,
  findings            JSONB,
  next_steps          JSONB,
  warehouse_id        VARCHAR(255),
  warehouse_name      VARCHAR(255),
  warehouse_type      VARCHAR(50),
  is_serverless       BOOLEAN,
  scanned_at          TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  scanned_by          VARCHAR(255),
  scan_duration_ms    INTEGER,
  CONSTRAINT valid_score CHECK (total_score >= 0 AND total_score <= 100)
);

CREATE INDEX IF NOT EXISTS idx_audit_space_id ON audit_results(space_id);
CREATE INDEX IF NOT EXISTS idx_audit_scanned_at ON audit_results(scanned_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_score ON audit_results(total_score);

-- Per-user starred spaces (favorites)
CREATE TABLE IF NOT EXISTS space_stars (
  space_id    VARCHAR(255) NOT NULL,
  user_email  VARCHAR(255) NOT NULL,
  starred_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(space_id, user_email)
);
CREATE INDEX IF NOT EXISTS idx_space_stars_user ON space_stars(user_email);

-- Space discovery registry (to power "New spaces" UX when Genie API doesn't expose created_at)
CREATE TABLE IF NOT EXISTS spaces_seen (
  space_id      VARCHAR(255) PRIMARY KEY,
  first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  last_seen_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  last_name     VARCHAR(255)
);
CREATE INDEX IF NOT EXISTS idx_spaces_seen_first ON spaces_seen(first_seen_at DESC);

CREATE OR REPLACE VIEW latest_scores AS
SELECT DISTINCT ON (space_id) *
FROM audit_results
ORDER BY space_id, scanned_at DESC;

CREATE OR REPLACE VIEW org_stats AS
SELECT 
  COUNT(DISTINCT space_id) as total_spaces,
  ROUND(AVG(total_score)) as avg_score,
  COUNT(*) FILTER (WHERE total_score < 40) as critical_count,
  COUNT(*) FILTER (WHERE is_serverless = false) as shared_warehouse_count,
  MAX(scanned_at) as last_scan_time
FROM latest_scores;
`;

// Cache successful schema init per database to avoid repeated CREATEs on every request.
// Map<dbKey, expiryMs>
const schemaReadyCache = new Map();
// Map<dbKey, Promise>
const schemaInitInflight = new Map();

function dbKeyFromEnv() {
  return `${process.env.LAKEBASE_HOST || 'nohost'}::${process.env.LAKEBASE_DATABASE || 'postgres'}`;
}

async function ensureSchema(dbPool) {
  if (!dbPool) return;
  const key = dbKeyFromEnv();
  const cached = schemaReadyCache.get(key);
  if (cached && Date.now() < cached) return;

  if (schemaInitInflight.has(key)) {
    await schemaInitInflight.get(key);
    return;
  }

  const p = (async () => {
    // Quick check: does the view exist?
    const viewRes = await dbPool.query(
      `SELECT EXISTS (
         SELECT FROM information_schema.views
         WHERE table_name = 'latest_scores'
       ) AS exists;`
    );
    const ok = Boolean(viewRes?.rows?.[0]?.exists);
    if (!ok) {
      // Create table + views (idempotent).
      await dbPool.query(SETUP_SQL);
    }

    // Lightweight migrations (safe + idempotent) to keep older DBs forward-compatible.
    await dbPool.query(`
      ALTER TABLE audit_results ADD COLUMN IF NOT EXISTS space_description TEXT;
      ALTER TABLE audit_results ADD COLUMN IF NOT EXISTS breakdown JSONB;
      ALTER TABLE audit_results ADD COLUMN IF NOT EXISTS scanned_by VARCHAR(255);
    `);

    await dbPool.query(`
      CREATE TABLE IF NOT EXISTS space_stars (
        space_id    VARCHAR(255) NOT NULL,
        user_email  VARCHAR(255) NOT NULL,
        starred_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(space_id, user_email)
      );
      CREATE INDEX IF NOT EXISTS idx_space_stars_user ON space_stars(user_email);
    `);

    await dbPool.query(`
      CREATE TABLE IF NOT EXISTS spaces_seen (
        space_id      VARCHAR(255) PRIMARY KEY,
        first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        last_seen_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        last_name     VARCHAR(255)
      );
      CREATE INDEX IF NOT EXISTS idx_spaces_seen_first ON spaces_seen(first_seen_at DESC);
    `);

    // Ensure views are up to date (they reference audit_results columns via *).
    await dbPool.query(`
      CREATE OR REPLACE VIEW latest_scores AS
      SELECT DISTINCT ON (space_id) *
      FROM audit_results
      ORDER BY space_id, scanned_at DESC;

      CREATE OR REPLACE VIEW org_stats AS
      SELECT 
        COUNT(DISTINCT space_id) as total_spaces,
        ROUND(AVG(total_score)) as avg_score,
        COUNT(*) FILTER (WHERE total_score < 40) as critical_count,
        COUNT(*) FILTER (WHERE is_serverless = false) as shared_warehouse_count,
        MAX(scanned_at) as last_scan_time
      FROM latest_scores;
    `);

    schemaReadyCache.set(key, Date.now() + 5 * 60 * 1000);
  })()
    .finally(() => {
      schemaInitInflight.delete(key);
    });

  schemaInitInflight.set(key, p);
  await p;
}

async function upsertSpacesSeen({ spaces = [], userEmail = null, token = null } = {}) {
  const list = (Array.isArray(spaces) ? spaces : [])
    .map((s) => ({ id: s?.id != null ? String(s.id) : null, name: s?.name != null ? String(s.name) : null }))
    .filter((s) => s.id);
  if (!list.length) return { count: 0 };

  if (useInMemory) {
    const now = new Date().toISOString();
    const map = new Map((inMemoryStore.spaces_seen || []).map((r) => [String(r.space_id), r]));
    for (const s of list) {
      const existing = map.get(s.id);
      if (existing) {
        existing.last_seen_at = now;
        if (s.name) existing.last_name = s.name;
      } else {
        map.set(s.id, { space_id: s.id, first_seen_at: now, last_seen_at: now, last_name: s.name || null });
      }
    }
    inMemoryStore.spaces_seen = Array.from(map.values());
    return { count: list.length };
  }

  let dbPool = null;
  try {
    dbPool = await getPool({ userEmail, token });
    if (!dbPool) return { count: 0 };
    await ensureSchema(dbPool);
  } catch {
    return { count: 0 };
  }

  const values = [];
  const params = [];
  let i = 1;
  for (const s of list) {
    values.push(`($${i++}::text, $${i++}::text)`);
    params.push(s.id, s.name || null);
  }

  try {
    await dbPool.query(
      `
      INSERT INTO spaces_seen(space_id, last_name)
      VALUES ${values.join(',')}
      ON CONFLICT (space_id)
      DO UPDATE SET
        last_seen_at = CURRENT_TIMESTAMP,
        last_name = COALESCE(EXCLUDED.last_name, spaces_seen.last_name)
      `,
      params
    );
  } catch {
    return { count: 0 };
  }

  return { count: list.length };
}

async function getSpacesSeenByIds({ spaceIds = [], userEmail = null, token = null } = {}) {
  const ids = (Array.isArray(spaceIds) ? spaceIds : []).map(String).filter(Boolean);
  if (!ids.length) return [];

  if (useInMemory) {
    const set = new Set(ids);
    return (inMemoryStore.spaces_seen || []).filter((r) => set.has(String(r.space_id)));
  }

  try {
    const dbPool = await getPool({ userEmail, token });
    if (!dbPool) return [];
    await ensureSchema(dbPool);
    const res = await dbPool.query(
      `SELECT space_id, first_seen_at, last_seen_at, last_name FROM spaces_seen WHERE space_id = ANY($1::text[])`,
      [ids]
    );
    return res.rows || [];
  } catch {
    return [];
  }
}

async function getNewSpaces({ days = 7, limit = 10, userEmail = null, token = null } = {}) {
  const lim = Math.max(1, Math.min(100, Number(limit) || 10));
  const d = Math.max(1, Math.min(3650, Number(days) || 7));

  if (useInMemory) {
    const cutoff = Date.now() - d * 24 * 60 * 60 * 1000;
    return (inMemoryStore.spaces_seen || [])
      .filter((r) => (new Date(r.first_seen_at).getTime() >= cutoff))
      .sort((a, b) => new Date(b.first_seen_at) - new Date(a.first_seen_at))
      .slice(0, lim);
  }

  try {
    const dbPool = await getPool({ userEmail, token });
    if (!dbPool) return [];
    await ensureSchema(dbPool);
    const cutoff = new Date(Date.now() - d * 24 * 60 * 60 * 1000).toISOString();
    const res = await dbPool.query(
      `
      SELECT space_id, first_seen_at, last_seen_at, last_name
      FROM spaces_seen
      WHERE first_seen_at >= $1::timestamptz
      ORDER BY first_seen_at DESC
      LIMIT $2
      `,
      [cutoff, lim]
    );
    return res.rows || [];
  } catch {
    return [];
  }
}

async function setSpaceStar({ spaceId, userEmail, token, starred }) {
  if (!spaceId) throw new Error('Missing spaceId');
  if (!userEmail) throw new Error('Missing userEmail');
  const on = Boolean(starred);

  if (useInMemory) {
    const me = String(userEmail).toLowerCase();
    const sid = String(spaceId);
    inMemoryStore.space_stars = (inMemoryStore.space_stars || []).filter(
      (r) => !(String(r.space_id) === sid && String(r.user_email).toLowerCase() === me)
    );
    if (on) {
      inMemoryStore.space_stars.push({ space_id: sid, user_email: userEmail, starred_at: new Date().toISOString() });
    }
    return { spaceId: sid, starred: on };
  }

  let dbPool = null;
  try {
    dbPool = await getPool({ userEmail, token });
    if (!dbPool) throw new Error('Lakebase unavailable');
    await ensureSchema(dbPool);
  } catch {
    // best-effort: fall back to in-memory behavior (non-persistent)
    const sid = String(spaceId);
    const on2 = Boolean(starred);
    const me = String(userEmail).toLowerCase();
    inMemoryStore.space_stars = (inMemoryStore.space_stars || []).filter(
      (r) => !(String(r.space_id) === sid && String(r.user_email).toLowerCase() === me)
    );
    if (on2) {
      inMemoryStore.space_stars.push({ space_id: sid, user_email: userEmail, starred_at: new Date().toISOString() });
    }
    return { spaceId: sid, starred: on2 };
  }
  if (on) {
    await dbPool.query(
      `INSERT INTO space_stars(space_id, user_email) VALUES ($1, $2)
       ON CONFLICT (space_id, user_email) DO UPDATE SET starred_at = CURRENT_TIMESTAMP`,
      [String(spaceId), String(userEmail)]
    );
  } else {
    await dbPool.query(
      `DELETE FROM space_stars WHERE space_id = $1 AND user_email = $2`,
      [String(spaceId), String(userEmail)]
    );
  }
  return { spaceId: String(spaceId), starred: on };
}

async function getStarMapForSpaceIds({ spaceIds = [], userEmail = null, token = null } = {}) {
  if (!userEmail) return new Map();
  const ids = (Array.isArray(spaceIds) ? spaceIds : []).map(String).filter(Boolean);
  if (!ids.length) return new Map();

  if (useInMemory) {
    const me = String(userEmail).toLowerCase();
    const set = new Set(
      (inMemoryStore.space_stars || [])
        .filter((r) => String(r?.user_email || '').toLowerCase() === me)
        .map((r) => String(r.space_id))
    );
    const map = new Map();
    for (const id of ids) map.set(id, set.has(id));
    return map;
  }

  try {
    const dbPool = await getPool({ userEmail, token });
    if (!dbPool) return new Map();
    await ensureSchema(dbPool);
    const res = await dbPool.query(
      `SELECT space_id FROM space_stars WHERE user_email = $1 AND space_id = ANY($2::text[])`,
      [String(userEmail), ids]
    );
    const set = new Set((res.rows || []).map((r) => String(r.space_id)));
    const map = new Map();
    for (const id of ids) map.set(id, set.has(id));
    return map;
  } catch {
    return new Map();
  }
}

/**
 * Get a fresh OAuth token for Lakebase authentication
 * In Databricks Apps, the token is available via environment variable
 */
async function getOAuthToken() {
  // Preferred: explicitly-provided Lakebase OAuth token (so we don't overload DATABRICKS_TOKEN).
  if (process.env.LAKEBASE_TOKEN) {
    return process.env.LAKEBASE_TOKEN;
  }

  // In Databricks Apps, DATABRICKS_TOKEN may be injected.
  // Even if its `scope` claim doesn't include "database", it can still be valid for
  // Databricks identity login to Lakebase, so accept it as a candidate.
  if (process.env.DATABRICKS_TOKEN) {
    return process.env.DATABRICKS_TOKEN;
  }

  // In Databricks Apps, prefer service-principal client-credentials token if configured.
  // This makes Lakebase persistence work even when DATABRICKS_TOKEN is not injected.
  try {
    const clientId = process.env.DATABRICKS_CLIENT_ID;
    const clientSecret = process.env.DATABRICKS_CLIENT_SECRET;
    const host = process.env.DATABRICKS_HOST;
    if (clientId && clientSecret && host) {
      const baseUrl = host.startsWith('http') ? host : `https://${host}`;
      const tokenUrl = `${baseUrl}/oidc/v1/token`;
      // DB auth needs DB-specific scopes.
      for (const scope of ['database', 'lakebase']) {
        const response = await fetch(tokenUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body: new URLSearchParams({
            grant_type: 'client_credentials',
            client_id: clientId,
            client_secret: clientSecret,
            scope
          })
        });
        if (response.ok) {
          const data = await response.json().catch(() => ({}));
          if (data?.access_token) return data.access_token;
        }
      }
    }
  } catch {
    // ignore
  }
  
  // Try to get token from the Databricks CLI credential file
  // This is for local development
  try {
    const fs = require('fs');
    const path = require('path');
    const configPath = path.join(process.env.HOME || '', '.databrickscfg');
    
    if (fs.existsSync(configPath)) {
      const config = fs.readFileSync(configPath, 'utf-8');
      const tokenMatch = config.match(/token\s*=\s*(.+)/);
      if (tokenMatch) {
        return tokenMatch[1].trim();
      }
    }
  } catch (e) {
    // Ignore errors
  }
  
  return null;
}

async function tryCreatePool({ host, database, user, token }) {
  if (!host || !database || !user || !token) return null;

  const claims = decodeJwtClaims(token);
  const expMs = claims?.exp ? (Number(claims.exp) * 1000) : null;
  const expiry = expMs ? expMs : (Date.now() + 55 * 60 * 1000);
  const cacheKey = `${user}::${database}::${hashToken(token) || 'tok'}`;

  const cached = poolCache.get(cacheKey);
  if (cached?.pool && Date.now() < cached.expiry - 60_000) {
    return cached.pool;
  }

  const config = {
    host,
    port: parseInt(process.env.LAKEBASE_PORT || '5432'),
    database,
    user,
    password: token,
    ssl: { rejectUnauthorized: false },
    max: 10,
    connectionTimeoutMillis: 10_000
  };

  const p = new Pool(config);
  try {
    // Validate auth once per token/user combo (then cached).
    await p.query('SELECT 1');
    p.on('error', (err) => console.error('Lakebase pool error:', err.message));
    poolCache.set(cacheKey, { pool: p, expiry });
    return { pool: p, error: null };
  } catch (e) {
    try { await p.end(); } catch {}
    const msg = e?.message ? String(e.message) : String(e);
    const code = e?.code ? String(e.code) : null;
    return { pool: null, error: code ? `${code}: ${msg}` : msg };
  }
}

/**
 * Get or create a connection pool
 */
async function getPool({ userEmail = null, token = null } = {}) {
  // NOTE: don't permanently block DB connection attempts once in-memory fallback was used.
  // We can still retry DB auth using LAKEBASE_TOKEN or service principal credentials.
  if (useInMemory && !token && !process.env.LAKEBASE_TOKEN && !process.env.DATABRICKS_CLIENT_ID) return null;

  const host = process.env.LAKEBASE_HOST;
    
  if (!host) {
    console.warn('⚠️ LAKEBASE_HOST not set. Using In-Memory Storage.');
    useInMemory = true;
    return null;
  }

  const database = process.env.LAKEBASE_DATABASE || 'postgres';

  const candidates = [];
  if (token) candidates.push({ token, kind: 'request' });
  if (process.env.LAKEBASE_TOKEN) candidates.push({ token: process.env.LAKEBASE_TOKEN, kind: 'lakebase_env' });
  if (process.env.DATABRICKS_TOKEN) candidates.push({ token: process.env.DATABRICKS_TOKEN, kind: 'databricks_env' });

  // Add service-principal DB credential token as last resort.
  // This matches the "Get OAuth token (1 hr lifetime) for PGPASSWORD" flow and tends to work consistently.
  try {
    const instanceName = process.env.LAKEBASE_INSTANCE;
    if (instanceName) {
      const dbCredToken = await getDatabaseCredentialToken({ instanceName });
      if (dbCredToken) candidates.push({ token: dbCredToken, kind: 'sp:db-credential' });
    }
  } catch {
    // ignore
  }

  if (candidates.length === 0) {
    console.warn('⚠️ No Databricks token found. Using In-Memory Storage.');
    return null;
  }

  const failures = [];
  for (const c of candidates) {
    const claims = decodeJwtClaims(c.token);
    const ident = identityFromClaims(claims);
    const user =
      (c.kind === 'request'
        ? (userEmail || process.env.DATABRICKS_USER || ident)
        : (ident || userEmail || process.env.DATABRICKS_USER || process.env.USER_EMAIL)) ||
      'databricks_user';

    const attempt = await tryCreatePool({ host, database, user, token: c.token });
    if (attempt?.pool) {
      lastLakebaseFailures = null;
      console.log(`🔌 Connecting to Lakebase: ${host}:${process.env.LAKEBASE_PORT || '5432'}/${database} as ${c.kind === 'request' ? 'user' : 'env-user'} (${c.kind})`);
      return attempt.pool;
    }
    failures.push({ kind: c.kind, user, error: attempt?.error || 'unknown' });
  }

  if (failures.length) {
    const summarized = failures.slice(0, 6).map((f) => ({
      kind: f.kind,
      user: f.user,
      error: String(f.error).slice(0, 180)
    }));
    lastLakebaseFailures = summarized;
    console.warn('⚠️ Lakebase connection failed for all credential candidates:', summarized);
  }
  return null;
}

/**
 * Initialize Database (Auto-Migration)
 */
async function initializeDatabase() {
  try {
    const dbPool = await getPool();
    if (!dbPool) return { status: 'in-memory' };

    console.log('🔄 Checking Database Schema...');
    
    // Test connection
    await dbPool.query('SELECT 1');
    console.log('✅ Lakebase connection successful');
    
    // Check if table exists
    const res = await dbPool.query(`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = 'audit_results'
      );
    `);

    if (!res.rows[0].exists) {
      console.log('🛠 Creating Database Tables...');
      await dbPool.query(SETUP_SQL);
      console.log('✅ Database Initialized Successfully');
    } else {
      console.log('✅ Database Schema Verified');
    }
    return { status: 'connected', host: process.env.LAKEBASE_HOST };

  } catch (err) {
    console.error('❌ Database connection failed:', err.message);
    console.warn('⚠️ Lakebase unavailable right now; will retry on next request');
    return { status: 'fallback', error: err.message };
  }
}

/**
 * Save a scan result to Lakebase
 */
async function saveScanResult(result) {
  if (useInMemory) {
    const record = {
      ...result,
      scanned_at: new Date(),
      findings: result.findings,
      next_steps: result.nextSteps,
      breakdown: result.breakdown
    };
    inMemoryStore.audit_results.push(record);
    return record;
  }

  const dbPool = await getPool({ userEmail: result?.scannedBy, token: result?._dbToken });
  await ensureSchema(dbPool);
  const query = `
    INSERT INTO audit_results (
      space_id, space_name, space_description, owner_email,
      total_score, maturity_level,
      foundation_score, data_setup_score, sql_assets_score, optimization_score,
      breakdown,
      findings, next_steps,
      warehouse_id, warehouse_name, warehouse_type, is_serverless,
      scanned_by, scan_duration_ms
    ) VALUES (
      $1, $2, $3, $4,
      $5, $6,
      $7, $8, $9, $10,
      $11,
      $12, $13,
      $14, $15, $16, $17,
      $18, $19
    )
    RETURNING id, scan_id, scanned_at
  `;

  const breakdown = result.breakdown || {};
  const values = [
    result.id,
    result.name,
    result.description || null,
    result.owner,
    result.totalScore,
    result.maturityLevel,
    breakdown.foundation?.score || 0,
    breakdown.dataSetup?.score || 0,
    breakdown.sqlAssets?.score || 0,
    breakdown.optimization?.score || 0,
    JSON.stringify(breakdown || {}),
    JSON.stringify(result.findings || []),
    JSON.stringify(result.nextSteps || []),
    result.warehouse?.id,
    result.warehouse?.name,
    result.warehouse?.type,
    result.warehouse?.serverless,
    result.scannedBy || 'manual',
    result.scanDuration
  ];

  const res = await dbPool.query(query, values);
  return res.rows[0];
}

/**
 * Get all latest scores
 */
async function getAllLatestScores() {
  if (useInMemory) {
    const map = new Map();
    inMemoryStore.audit_results.forEach((r) => {
      const row = normalizeInMemoryLatestRow(r);
      if (!row?.space_id) return;
      map.set(row.space_id, row);
    });
    return Array.from(map.values());
  }

  const dbPool = await getPool();
  try {
    await ensureSchema(dbPool);
    const res = await dbPool.query('SELECT * FROM latest_scores');
    return res.rows;
  } catch {
    return [];
  }
}

function normalizeSort(sort) {
  const s = String(sort || '').toLowerCase();
  if (['score_desc', 'score_asc', 'scanned_desc', 'name_asc'].includes(s)) return s;
  return 'scanned_desc';
}

async function getLatestScoresCount({ query = '', userEmail = null, token = null, scannedByEmail = null, starredOnly = false, starredByEmail = null } = {}) {
  const q = String(query || '').trim();

  if (useInMemory) {
    const all = await getAllLatestScores();
    let rows = all;
    if (scannedByEmail) {
      const me = String(scannedByEmail).toLowerCase();
      rows = rows.filter(r => String(r?.scanned_by || '').toLowerCase() === me);
    }
    if (starredOnly && starredByEmail) {
      const me = String(starredByEmail).toLowerCase();
      const starredSet = new Set(
        (inMemoryStore.space_stars || [])
          .filter((r) => String(r?.user_email || '').toLowerCase() === me)
          .map((r) => String(r.space_id))
      );
      rows = rows.filter((r) => starredSet.has(String(r?.space_id)));
    }
    if (!q) return rows.length;
    const qq = q.toLowerCase();
    return rows.filter(r => String(r?.space_name || '').toLowerCase().includes(qq)).length;
  }

  const dbPool = await getPool({ userEmail, token });
  if (!dbPool) return 0;
  try {
    await ensureSchema(dbPool);
  } catch {
    return 0;
  }
  const where = [];
  const args = [];
  if (q) {
    args.push(`%${q}%`);
    where.push(`space_name ILIKE $${args.length}`);
  }
  if (scannedByEmail) {
    args.push(String(scannedByEmail));
    where.push(`scanned_by = $${args.length}`);
  }
  if (starredOnly && starredByEmail) {
    args.push(String(starredByEmail));
    where.push(`EXISTS (SELECT 1 FROM space_stars st WHERE st.space_id = latest_scores.space_id AND st.user_email = $${args.length})`);
  }

  const sql = `SELECT COUNT(*)::int AS count FROM latest_scores${where.length ? ` WHERE ${where.join(' AND ')}` : ''}`;
  const res = await dbPool.query(sql, args);
  return res.rows?.[0]?.count ?? 0;
}

async function getLatestScoresPage({ query = '', sort = 'scanned_desc', limit = 12, offset = 0, userEmail = null, token = null, scannedByEmail = null, starredOnly = false, starredByEmail = null } = {}) {
  const q = String(query || '').trim();
  const s = normalizeSort(sort);
  const lim = Math.max(1, Math.min(200, Number(limit) || 12));
  const off = Math.max(0, Number(offset) || 0);

  const sortSql =
    s === 'score_desc' ? 'total_score DESC NULLS LAST, scanned_at DESC' :
    s === 'score_asc' ? 'total_score ASC NULLS LAST, scanned_at DESC' :
    s === 'name_asc' ? 'space_name ASC NULLS LAST' :
    /* scanned_desc */ 'scanned_at DESC NULLS LAST';

  if (useInMemory) {
    const all = await getAllLatestScores();
    let filtered = all;
    if (scannedByEmail) {
      const me = String(scannedByEmail).toLowerCase();
      filtered = filtered.filter(r => String(r?.scanned_by || '').toLowerCase() === me);
    }
    if (starredOnly && starredByEmail) {
      const me = String(starredByEmail).toLowerCase();
      const starredSet = new Set(
        (inMemoryStore.space_stars || [])
          .filter((r) => String(r?.user_email || '').toLowerCase() === me)
          .map((r) => String(r.space_id))
      );
      filtered = filtered.filter((r) => starredSet.has(String(r?.space_id)));
    }
    if (q) filtered = filtered.filter(r => String(r?.space_name || '').toLowerCase().includes(q.toLowerCase()));

    const sorted = [...filtered];
    sorted.sort((a, b) => {
      if (s === 'name_asc') return String(a?.space_name || '').localeCompare(String(b?.space_name || ''));
      if (s === 'score_asc') return (Number(a?.total_score ?? -1)) - (Number(b?.total_score ?? -1));
      if (s === 'score_desc') return (Number(b?.total_score ?? -1)) - (Number(a?.total_score ?? -1));
      const at = a?.scanned_at || 0;
      const bt = b?.scanned_at || 0;
      return Date.parse(bt) - Date.parse(at);
    });

    return sorted.slice(off, off + lim);
  }

  const dbPool = await getPool({ userEmail, token });
  if (!dbPool) return [];
  try {
    await ensureSchema(dbPool);
  } catch {
    return [];
  }
  const where = [];
  const args = [];
  if (q) {
    args.push(`%${q}%`);
    where.push(`space_name ILIKE $${args.length}`);
  }
  if (scannedByEmail) {
    args.push(String(scannedByEmail));
    where.push(`scanned_by = $${args.length}`);
  }
  if (starredOnly && starredByEmail) {
    args.push(String(starredByEmail));
    where.push(`EXISTS (SELECT 1 FROM space_stars st WHERE st.space_id = latest_scores.space_id AND st.user_email = $${args.length})`);
  }
  args.push(lim);
  args.push(off);

  const res = await dbPool.query(
    `SELECT * FROM latest_scores${where.length ? ` WHERE ${where.join(' AND ')}` : ''} ORDER BY ${sortSql} LIMIT $${args.length - 1} OFFSET $${args.length}`,
    args
  );
  return res.rows || [];
}

async function getLatestScoresBySpaceIds(spaceIds, { userEmail = null, token = null } = {}) {
  const ids = Array.isArray(spaceIds) ? spaceIds.map(String).filter(Boolean) : [];
  if (ids.length === 0) return [];

  if (useInMemory) {
    const all = await getAllLatestScores();
    const set = new Set(ids);
    return all.filter(r => set.has(String(r?.space_id)));
  }

  const dbPool = await getPool({ userEmail, token });
  if (!dbPool) return [];
  try {
    await ensureSchema(dbPool);
  } catch {
    return [];
  }
  const res = await dbPool.query(
    'SELECT * FROM latest_scores WHERE space_id = ANY($1::text[])',
    [ids]
  );
  return res.rows || [];
}

/**
 * Check health
 */
async function healthCheck({ userEmail = null, token = null } = {}) {
  if (useInMemory) return { status: 'healthy', mode: 'in-memory' };
  
  try {
    const dbPool = await getPool({ userEmail, token });
    if (!dbPool) {
      return {
        status: 'unhealthy',
        mode: 'lakebase',
        host: process.env.LAKEBASE_HOST,
        error: 'Lakebase login failed',
        failures: lastLakebaseFailures || []
      };
    }
    await dbPool.query('SELECT 1');
    return { status: 'healthy', mode: 'lakebase', host: process.env.LAKEBASE_HOST };
  } catch (error) {
    return {
      status: 'unhealthy',
      mode: 'lakebase',
      host: process.env.LAKEBASE_HOST,
      error: error.message,
      failures: lastLakebaseFailures || []
    };
  }
}

function toIso(d) {
  try {
    return d instanceof Date ? d.toISOString() : new Date(d).toISOString();
  } catch {
    return null;
  }
}

async function getScoreHistory(spaceId, opts = {}) {
  if (!spaceId) return [];
  const limit = Math.max(1, Math.min(500, Number(opts?.limit ?? 30)));
  const days = opts?.days != null ? Math.max(1, Math.min(3650, Number(opts.days))) : null;
  const userEmail = opts?.userEmail ?? null;
  const token = opts?.token ?? null;

  if (useInMemory) {
    const cutoff = days ? new Date(Date.now() - days * 24 * 60 * 60 * 1000) : null;
    const rows = (inMemoryStore.audit_results || [])
      .filter(r => (r.space_id || r.id) === spaceId)
      .filter(r => (cutoff ? (new Date(r.scanned_at) >= cutoff) : true))
      .sort((a, b) => new Date(b.scanned_at) - new Date(a.scanned_at))
      .slice(0, limit);

    // Output shape mirrors Lakebase: include timestamps + category breakdown fields.
    return rows.map((r, idx) => {
      const score = r.total_score ?? r.totalScore ?? 0;
      const prev = rows[idx + 1];
      const prevScore = prev ? (prev.total_score ?? prev.totalScore ?? 0) : null;
      const breakdown = r.breakdown || {};
      return {
        scanned_at: toIso(r.scanned_at),
        scan_date: (r.scanned_at ? String(toIso(r.scanned_at)).slice(0, 10) : null),
        total_score: score,
        maturity_level: r.maturity_level ?? r.maturityLevel ?? null,
        foundation_score: r.foundation_score ?? breakdown?.foundation?.score ?? null,
        data_setup_score: r.data_setup_score ?? breakdown?.dataSetup?.score ?? null,
        sql_assets_score: r.sql_assets_score ?? breakdown?.sqlAssets?.score ?? null,
        optimization_score: r.optimization_score ?? breakdown?.optimization?.score ?? null,
        scanned_by: r.scanned_by ?? r.scannedBy ?? null,
        score_delta: prevScore === null ? null : (score - prevScore)
      };
    });
  }

  // Backed by Lakebase
  const dbPool = await getPool({ userEmail, token });
  await ensureSchema(dbPool);

  const where = ['space_id = $1'];
  const args = [String(spaceId)];
  if (days) {
    const cutoff = new Date(Date.now() - days * 24 * 60 * 60 * 1000);
    args.push(cutoff.toISOString());
    where.push(`scanned_at >= $${args.length}::timestamptz`);
  }
  args.push(limit);
  const limIdx = args.length;

  const res = await dbPool.query(
    `
    SELECT
      scanned_at,
      DATE(scanned_at) as scan_date,
      total_score,
      maturity_level,
      foundation_score,
      data_setup_score,
      sql_assets_score,
      optimization_score,
      scanned_by,
      total_score - LEAD(total_score) OVER (ORDER BY scanned_at DESC) as score_delta
    FROM audit_results
    WHERE ${where.join(' AND ')}
    ORDER BY scanned_at DESC
    LIMIT $${limIdx}
    `,
    args
  );
  return res.rows || [];
}

async function getLatestScore(spaceId) {
  const hist = await getScoreHistory(spaceId, { limit: 1 });
  return hist[0] || null;
}

async function getPreviousScore(spaceId) {
  const hist = await getScoreHistory(spaceId, { limit: 2 });
  return hist[1] || null;
}

// TODO: implement these when/if Admin dashboard switches to Lakebase-backed stats.
async function getOrgStats() { return { total_spaces: 0, avg_score: 0 }; }
async function getSpacesNeedingAttention() { return []; }
async function closePool() { if (pool) await pool.end(); }

module.exports = {
  initializeDatabase,
  getPool,
  saveScanResult,
  getAllLatestScores,
  getLatestScoresCount,
  getLatestScoresPage,
  getLatestScoresBySpaceIds,
  setSpaceStar,
  getStarMapForSpaceIds,
  upsertSpacesSeen,
  getSpacesSeenByIds,
  getNewSpaces,
  healthCheck,
  getLatestScore,
  getPreviousScore,
  getScoreHistory,
  getOrgStats,
  getSpacesNeedingAttention,
  closePool
};
