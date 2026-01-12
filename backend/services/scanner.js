/**
 * Genie Space Scanner
 */

const { calculateScore, getStatus } = require('../config/scoring-rubric.js');

// Cache Unity Catalog table metadata to avoid repeated UC calls during org-wide scans.
// Map<tableFullName, { value: any, expiry: number }>
const ucTableCache = new Map();
const UC_CACHE_TTL_MS = 5 * 60 * 1000;

async function getUcTableMetadata(databricksClient, fullName, { forceRefresh = false } = {}) {
  if (!fullName) return null;
  const key = String(fullName).trim();
  if (!key) return null;

  const cached = ucTableCache.get(key);
  if (!forceRefresh && cached?.value && Date.now() < cached.expiry) {
    return cached.value;
  }

  try {
    const meta = await databricksClient.getTable(key);
    ucTableCache.set(key, { value: meta, expiry: Date.now() + UC_CACHE_TTL_MS });
    return meta;
  } catch (e) {
    // Cache failures briefly to avoid hammering.
    ucTableCache.set(key, { value: null, expiry: Date.now() + 30_000 });
    return null;
  }
}

async function enrichTablesWithUcMetadata(databricksClient, tableNames, { forceRefresh = false } = {}) {
  const names = Array.isArray(tableNames) ? tableNames.filter(Boolean) : [];
  const results = await Promise.all(
    names.map(async (fullName) => {
      const meta = await getUcTableMetadata(databricksClient, fullName, { forceRefresh });
      const columns = Array.isArray(meta?.columns)
        ? meta.columns
            .map((c) => ({
              name: c?.name,
              description: (c?.comment || '').trim()
            }))
            .filter((c) => c.name)
        : [];

      return {
        full_name: String(fullName),
        name: String(fullName).split('.').pop(),
        description: (meta?.comment || '').trim(),
        columns
      };
    })
  );

  return results;
}

function parseSerializedSpace(serialized) {
  if (!serialized) return null;
  if (typeof serialized === 'object') return serialized;
  if (typeof serialized !== 'string') return null;
  try {
    return JSON.parse(serialized);
  } catch {
    return null;
  }
}

function extractTextInstructionsFromSerialized(serializedObj) {
  if (!serializedObj || typeof serializedObj !== 'object') return '';
  const textInstr = serializedObj?.instructions?.text_instructions;
  if (!Array.isArray(textInstr) || textInstr.length === 0) return '';
  const parts = [];
  for (const item of textInstr) {
    if (!item) continue;
    const content = item.content;
    if (Array.isArray(content)) {
      for (const line of content) {
        if (typeof line === 'string' && line.trim()) parts.push(line.trim());
      }
      continue;
    }
    if (typeof content === 'string' && content.trim()) {
      parts.push(content.trim());
    }
  }
  return parts.join('\n');
}

function extractTablesFromSerialized(serializedObj) {
  if (!serializedObj || typeof serializedObj !== 'object') return [];
  const tables = serializedObj?.data_sources?.tables;
  if (!Array.isArray(tables)) return [];
  const out = [];
  const seen = new Set();
  for (const t of tables) {
    const ident = t?.identifier || t?.full_name || t?.name;
    if (!ident) continue;
    const s = String(ident).trim();
    if (!s || seen.has(s)) continue;
    seen.add(s);
    out.push(s);
  }
  return out;
}

function extractJoinsFromSerialized(serializedObj) {
  if (!serializedObj || typeof serializedObj !== 'object') return [];
  // Genie currently stores join specs under `instructions.join_specs` in serialized_space.
  // Keep a fallback to a top-level `join_specs` just in case the shape changes.
  const joinSpecs =
    (Array.isArray(serializedObj?.instructions?.join_specs) ? serializedObj.instructions.join_specs : null) ||
    (Array.isArray(serializedObj?.join_specs) ? serializedObj.join_specs : null) ||
    null;
  if (!Array.isArray(joinSpecs)) return [];
  return joinSpecs.filter(Boolean);
}

function extractSampleQuestionsFromSerialized(serializedObj) {
  if (!serializedObj || typeof serializedObj !== 'object') return [];

  // Preferred (per Databricks Agent): serialized_space.config.sample_questions
  const cfg = serializedObj?.config;
  const cfgSq = cfg?.sample_questions;
  if (Array.isArray(cfgSq) && cfgSq.length) {
    const out = [];
    const seen = new Set();
    for (const item of cfgSq) {
      if (!item || typeof item !== 'object') continue;
      const q = item.question;
      const push = (s) => {
        const t = String(s || '').trim();
        if (!t || seen.has(t)) return;
        seen.add(t);
        out.push(t);
      };
      if (Array.isArray(q)) q.forEach(push);
      else if (typeof q === 'string') push(q);
      else if (item.text) push(item.text);
    }
    return out;
  }

  // Fallback: question+SQL pairs under `instructions.example_question_sqls`.
  // In this workspace, those represent "Trusted SQL Queries" (Configure > SQL queries & functions).
  // We treat *question-only* entries (no SQL) as "Sample Questions" for the UI.
  const ex = serializedObj?.instructions?.example_question_sqls;
  if (!Array.isArray(ex)) return [];

  const out = [];
  const seen = new Set();
  for (const item of ex) {
    if (!item || typeof item !== 'object') continue;
    const sql = item.sql;
    const sqlText = Array.isArray(sql) ? sql.join('').trim() : (typeof sql === 'string' ? sql.trim() : '');
    if (sqlText) {
      // Has SQL => treat as trusted query, not a plain sample question.
      continue;
    }
    const q = item.question;
    const push = (s) => {
      const t = String(s || '').trim();
      if (!t || seen.has(t)) return;
      seen.add(t);
      out.push(t);
    };
    if (Array.isArray(q)) {
      q.forEach(push);
    } else if (typeof q === 'string') {
      push(q);
    }
  }
  return out;
}

function extractTrustedSqlQueriesFromSerialized(serializedObj) {
  if (!serializedObj || typeof serializedObj !== 'object') return [];
  const ex = serializedObj?.instructions?.example_question_sqls;
  if (!Array.isArray(ex)) return [];

  const out = [];
  const seen = new Set();

  for (const item of ex) {
    if (!item || typeof item !== 'object') continue;
    const sql = item.sql;
    const sqlText = Array.isArray(sql) ? sql.join('').trim() : (typeof sql === 'string' ? sql.trim() : '');
    if (!sqlText) continue;

    const q = item.question;
    const qText = Array.isArray(q) ? String(q[0] || '').trim() : (typeof q === 'string' ? q.trim() : '');
    const key = `${qText}::${sqlText}`;
    if (!key.trim() || seen.has(key)) continue;
    seen.add(key);

    out.push({
      question: qText || null,
      sql: sqlText
    });
  }

  return out;
}

function extractSampleQuestionsFromAny(payload) {
  // Best-effort: capture "Settings → Sample questions" from richer Genie payloads (e.g. /genie/spaces/get).
  const out = [];
  const seen = new Set();

  const push = (s) => {
    const t = String(s || '').trim();
    if (!t || seen.has(t)) return;
    seen.add(t);
    out.push(t);
  };

  const keyLooksLikeSampleQuestions = (keyHint) => {
    const k = String(keyHint || '').toLowerCase();
    // Accept a few common variants:
    // - sample_questions
    // - settings.sample_questions
    // - sampleQuestions
    // - sample prompts (some UIs call them prompts)
    // - starter_questions / suggested_questions (some UIs call them “starter” questions)
    // - recommended_questions
    return (
      k.includes('sample_questions') ||
      (k.includes('sample') && (k.includes('question') || k.includes('questions') || k.includes('prompt') || k.includes('prompts')))
      || (k.includes('starter') && (k.includes('question') || k.includes('questions')))
      || (k.includes('suggest') && (k.includes('question') || k.includes('questions')))
      || (k.includes('recommend') && (k.includes('question') || k.includes('questions')))
    );
  };

  const walk = (x, keyHint = '') => {
    if (!x) return;
    if (Array.isArray(x)) {
      const allStrings = x.every((v) => typeof v === 'string');
      if (allStrings && keyLooksLikeSampleQuestions(keyHint)) {
        x.forEach(push);
        return;
      }
      for (const v of x) {
        if (v && typeof v === 'object') {
          const q =
            v.question ||
            v.sample_question ||
            v.sampleQuestion ||
            v.questions ||
            v.text ||
            v.prompt ||
            v.title ||
            null;
          if (q && keyLooksLikeSampleQuestions(keyHint)) {
            if (Array.isArray(q)) q.forEach(push);
            else push(q);
          }
        }
        walk(v, keyHint);
      }
      return;
    }
    if (typeof x === 'object') {
      for (const [k, v] of Object.entries(x)) {
        const hint = `${keyHint}.${String(k).toLowerCase()}`;
        // If this *object itself* looks like a sample question entry, capture it.
        if (keyLooksLikeSampleQuestions(hint) && v && typeof v === 'string') push(v);
        walk(v, hint);
      }
    }
  };

  walk(payload, '');
  return out;
}

function collectSampleQuestionCandidatePaths(payload, max = 12) {
  const candidates = [];
  const seen = new Set();
  const needle = /(sample(_|\s)?questions?|starter(_|\s)?questions?|suggest(ed)?(_|\s)?questions?|recommend(ed)?(_|\s)?questions?)/i;

  const push = (path, value) => {
    if (candidates.length >= max) return;
    const key = String(path || '');
    if (!key || seen.has(key)) return;
    seen.add(key);
    const type = Array.isArray(value) ? `array(len=${value.length})` : typeof value;
    candidates.push({ path: key, type });
  };

  const walk = (x, path = '') => {
    if (!x) return;
    if (Array.isArray(x)) {
      for (let i = 0; i < Math.min(x.length, 50); i++) {
        walk(x[i], `${path}[${i}]`);
      }
      return;
    }
    if (typeof x === 'object') {
      for (const [k, v] of Object.entries(x)) {
        const p = path ? `${path}.${k}` : k;
        if (needle.test(String(k))) push(p, v);
        walk(v, p);
      }
    }
  };

  walk(payload, '');
  return candidates;
}

function extractSqlExpressionsFromSerialized(serializedObj) {
  if (!serializedObj || typeof serializedObj !== 'object') return [];
  const snippets = serializedObj?.instructions?.sql_snippets;
  if (!snippets || typeof snippets !== 'object') return [];

  // Genie organizes these into buckets; for scoring we just need a count.
  const buckets = ['expressions', 'measures', 'filters', 'dimensions'];
  const out = [];
  const seen = new Set();

  const push = (x) => {
    if (!x || typeof x !== 'object') return;
    const id = x.id ? String(x.id) : '';
    const name = x.display_name ? String(x.display_name) : '';
    const sql = Array.isArray(x.sql) ? x.sql.join('').trim() : (typeof x.sql === 'string' ? x.sql.trim() : '');
    const key = id || `${name}::${sql}`;
    if (!key || seen.has(key)) return;
    seen.add(key);
    out.push({
      id,
      name,
      sql
    });
  };

  for (const b of buckets) {
    const arr = snippets[b];
    if (Array.isArray(arr)) arr.forEach(push);
  }

  return out;
}

function extractTablesFromAny(payload) {
  const out = [];
  const seen = new Set();
  const push = (v) => {
    const s = String(v || '').trim();
    if (!s) return;
    if (seen.has(s)) return;
    seen.add(s);
    out.push(s);
  };

  const maybeExtractFromObject = (obj) => {
    if (!obj || typeof obj !== 'object') return;
    const directKeys = ['full_name', 'fullName', 'table_name', 'tableName', 'identifier', 'name'];
    for (const k of directKeys) {
      if (typeof obj[k] === 'string') push(obj[k]);
    }
  };

  const walk = (x) => {
    if (!x) return;
    if (typeof x === 'string') {
      // Heuristic: UC-style full name (catalog.schema.table) or schema.table
      if (x.includes('.')) push(x);
      return;
    }
    if (Array.isArray(x)) {
      for (const i of x) walk(i);
      return;
    }
    if (typeof x === 'object') {
      maybeExtractFromObject(x);
      for (const v of Object.values(x)) walk(v);
    }
  };

  walk(payload);
  return out;
}

function extractGenieTableDetailsFromAny(payload) {
  // Best-effort extractor for Genie “Data & descriptions” metadata.
  // We don’t know the exact export schema across workspaces, so we look for table-shaped objects.
  const detailsByName = new Map(); // full_name -> { full_name, description, columns: [{name, description}] }

  const pushDetail = (fullName, description, columns) => {
    const key = String(fullName || '').trim();
    if (!key) return;
    const existing = detailsByName.get(key) || { full_name: key, description: '', columns: [] };

    const desc = String(description || '').trim();
    if (desc) existing.description = desc;

    const cols = Array.isArray(columns) ? columns : [];
    if (cols.length) {
      const byCol = new Map(existing.columns.map((c) => [c.name, c]));
      for (const c of cols) {
        if (!c?.name) continue;
        const name = String(c.name).trim();
        if (!name) continue;
        const cd = String(c.description || '').trim();
        if (!cd) continue;
        byCol.set(name, { name, description: cd });
      }
      existing.columns = Array.from(byCol.values());
    }

    detailsByName.set(key, existing);
  };

  const extractFromTableLike = (t) => {
    if (!t || typeof t !== 'object') return;
    const fullName =
      t.identifier ||
      t.full_name ||
      t.fullName ||
      t.table_name ||
      t.tableName ||
      t.name ||
      null;
    if (!fullName) return;

    const normalizeDesc = (d) => {
      if (!d) return '';
      if (Array.isArray(d)) {
        return d.map((x) => String(x || '').trim()).filter(Boolean).join('\n');
      }
      return String(d).trim();
    };

    const description = normalizeDesc(
      t.description ||
        t.comment ||
        t.table_description ||
        t.tableDescription ||
        t.doc ||
        null
    );

    const columns = [];
    // Common shapes:
    // - column_configs: [{column_name, description/comment/...}]
    // - columns: [{name, description/comment/...}]
    const cc = Array.isArray(t.column_configs) ? t.column_configs : [];
    for (const c of cc) {
      const name = c?.column_name || c?.name;
      const cd = normalizeDesc(c?.description || c?.comment || c?.column_description || c?.columnDescription);
      if (name && cd) columns.push({ name: String(name), description: cd });
    }
    const cols = Array.isArray(t.columns) ? t.columns : [];
    for (const c of cols) {
      const name = c?.name || c?.column_name;
      const cd = normalizeDesc(c?.description || c?.comment || c?.column_description || c?.columnDescription);
      if (name && cd) columns.push({ name: String(name), description: cd });
    }

    pushDetail(fullName, description, columns);
  };

  const walk = (x) => {
    if (!x) return;
    if (Array.isArray(x)) {
      for (const i of x) walk(i);
      return;
    }
    if (typeof x === 'object') {
      // If it looks like a table object, capture it.
      if ('identifier' in x || 'full_name' in x || 'fullName' in x || 'column_configs' in x || 'columns' in x) {
        extractFromTableLike(x);
      }
      for (const v of Object.values(x)) walk(v);
    }
  };

  walk(payload);
  return detailsByName;
}

function mergeGenieTableDetailsIntoSpace(space, detailsMap) {
  if (!space || typeof space !== 'object') return;
  if (!(detailsMap instanceof Map) || detailsMap.size === 0) return;
  if (!(space._genieTableDetailsByName instanceof Map)) {
    space._genieTableDetailsByName = new Map();
  }
  for (const [k, v] of detailsMap.entries()) {
    if (!k || !v) continue;
    const existing = space._genieTableDetailsByName.get(k) || { full_name: k, description: '', columns: [] };
    const merged = { ...existing };
    if (v.description) merged.description = v.description;
    if (Array.isArray(v.columns) && v.columns.length) {
      const byName = new Map((merged.columns || []).map((c) => [c.name, c]));
      for (const c of v.columns) {
        if (!c?.name) continue;
        const name = String(c.name).trim();
        if (!name) continue;
        const desc = String(c.description || '').trim();
        if (!desc) continue;
        byName.set(name, { name, description: desc });
      }
      merged.columns = Array.from(byName.values());
    }
    space._genieTableDetailsByName.set(k, merged);
  }
}

function extractOwnerEmailFromAny(payload) {
  // Best-effort: find a string email in common owner/creator fields.
  const candidates = new Set();
  const push = (v) => {
    const s = String(v || '').trim();
    if (s.includes('@') && s.length <= 320) candidates.add(s);
  };

  const walk = (x) => {
    if (!x) return;
    if (typeof x === 'string') {
      // Avoid collecting random strings; only accept obvious email strings.
      if (x.includes('@')) push(x);
      return;
    }
    if (Array.isArray(x)) {
      for (const i of x) walk(i);
      return;
    }
    if (typeof x === 'object') {
      for (const [k, v] of Object.entries(x)) {
        const key = String(k || '').toLowerCase();
        if (
          key === 'owner' ||
          key === 'owner_email' ||
          key === 'owneremail' ||
          key === 'creator' ||
          key === 'creator_email' ||
          key === 'creatoremail' ||
          key === 'created_by' ||
          key === 'createdby'
        ) {
          if (typeof v === 'string') push(v);
          if (v && typeof v === 'object') {
            if (typeof v.email === 'string') push(v.email);
            if (typeof v.userName === 'string') push(v.userName);
          }
        }
        walk(v);
      }
    }
  };

  walk(payload);
  // Prefer the first candidate deterministically.
  return Array.from(candidates.values())[0] || null;
}

async function tryEnrichSpaceFromExport(databricksClient, space) {
  if (!space?.id) return space;
  try {
    const exported = await databricksClient.exportGenieSpace(space.id);
    // Response could be JSON or {raw: "..."}; try to parse if needed.
    let obj = exported;
    if (exported && typeof exported === 'object' && typeof exported.raw === 'string') {
      try { obj = JSON.parse(exported.raw); } catch { obj = exported; }
    }

    // Prefer obvious fields if present, else fall back to recursive extraction.
    const rawTables =
      obj?.tables ||
      obj?.table_identifiers ||
      obj?.data_sources ||
      obj?.space?.tables ||
      obj?.space?.data_sources ||
      null;
    const tables = rawTables ? extractTablesFromAny(rawTables) : extractTablesFromAny(obj);

    if (tables.length && (!Array.isArray(space.tables) || space.tables.length === 0)) {
      space.tables = tables;
    }

    // Capture Genie “Data & descriptions” details (best effort) for scoring.
    try {
      mergeGenieTableDetailsIntoSpace(space, extractGenieTableDetailsFromAny(obj));
    } catch {
      // ignore
    }

    // Best-effort owner enrichment for "Mine only"
    if (!space.owner && !space.creator_email && !space.creator) {
      const ownerEmail = extractOwnerEmailFromAny(obj);
      if (ownerEmail) space.creator_email = ownerEmail;
    }
  } catch (e) {
    // Non-fatal: export permissions may be tighter than basic space read.
    console.warn(`Could not export/enrich space ${space.id}:`, e.message);
  }
  return space;
}

async function tryEnrichSpaceFromGenieGet(databricksClient, space) {
  if (!space?.id) return space;
  try {
    const details = await databricksClient.getGenieSpaceDetails(space.id);
    try {
      mergeGenieTableDetailsIntoSpace(space, extractGenieTableDetailsFromAny(details));
    } catch {
      // ignore
    }
    try {
      const sq = extractSampleQuestionsFromAny(details);
      if (sq.length) space._genieSampleQuestions = sq;
    } catch {
      // ignore
    }
    try {
      space._debugSampleQuestionCandidates = collectSampleQuestionCandidatePaths(details);
    } catch {
      // ignore
    }
    // Best-effort owner enrichment for "Mine only"
    if (!space.owner && !space.creator_email && !space.creator) {
      const ownerEmail = extractOwnerEmailFromAny(details);
      if (ownerEmail) space.creator_email = ownerEmail;
    }
    space._debugGenieGet = { ok: true };
  } catch (e) {
    // Non-fatal: this endpoint may be permissioned differently across workspaces.
    console.warn(`Could not fetch /genie/spaces/get for ${space.id}:`, e.message);
    space._debugGenieGet = { ok: false, error: e?.message || String(e) };
  }
  return space;
}

async function scanSpace(databricksClient, spaceId, opts = {}) {
  try {
    // Use serialized_space when available so we can score instructions/tables accurately.
    const space = await databricksClient.getGenieSpaceWithSerialized(spaceId);
    // Genie “Data & descriptions” for tables may be stored inside serialized_space itself in some workspaces.
    try {
      const serializedObj = parseSerializedSpace(space?.serialized_space);
      if (serializedObj) {
        mergeGenieTableDetailsIntoSpace(space, extractGenieTableDetailsFromAny(serializedObj));
      }
    } catch {
      // ignore
    }
    await tryEnrichSpaceFromExport(databricksClient, space);
    await tryEnrichSpaceFromGenieGet(databricksClient, space);
    
    let warehouse = null;
    if (space.warehouse_id) {
      try {
        warehouse = await databricksClient.getWarehouse(space.warehouse_id);
      } catch (e) {
        console.warn(`Could not fetch warehouse ${space.warehouse_id}:`, e.message);
      }
    }

    const normalizedSpace = normalizeSpaceData(space, warehouse);
    // Non-sensitive: help diagnose why user-specific settings (like Sample questions) aren’t visible.
    try {
      normalizedSpace.debug = normalizedSpace.debug || {};
      normalizedSpace.debug.userTokenPresent = Boolean(databricksClient?.userToken);
      normalizedSpace.debug.serializedHasConfig = Boolean(parseSerializedSpace(space?.serialized_space)?.config);
    } catch {
      // ignore
    }

    // Enrich tables with metadata so description-based criteria can be scored.
    // Prefer Genie “Data & descriptions” (from export) when available; fall back to UC comments.
    try {
      const tableNames = Array.isArray(normalizedSpace.tables) ? normalizedSpace.tables : [];
      const uc = await enrichTablesWithUcMetadata(databricksClient, tableNames, {
        forceRefresh: opts.forceRefreshUc === true
      });

      const genieMap =
        space && typeof space === 'object' && space._genieTableDetailsByName instanceof Map
          ? space._genieTableDetailsByName
          : null;

      // Merge: if Genie provides descriptions, prefer them over UC.
      normalizedSpace.tables = uc.map((t) => {
        const g = genieMap?.get(t.full_name);
        if (!g) return t;
        const merged = { ...t };
        if (g.description) merged.description = g.description;
        if (Array.isArray(g.columns) && g.columns.length) {
          const byName = new Map((merged.columns || []).map((c) => [c.name, c]));
          for (const c of g.columns) {
            if (!c?.name) continue;
            const name = String(c.name).trim();
            if (!name) continue;
            const desc = String(c.description || '').trim();
            if (!desc) continue;
            byName.set(name, { name, description: desc });
          }
          merged.columns = Array.from(byName.values());
        }
        return merged;
      });
    } catch (e) {
      // Non-fatal: if UC metadata cannot be fetched, description-based criteria will remain failing.
      normalizedSpace.tables = (Array.isArray(normalizedSpace.tables) ? normalizedSpace.tables : []).map((t) => ({
        full_name: String(t),
        name: String(t).split('.').pop(),
        description: '',
        columns: []
      }));
    }

    const scoreResult = calculateScore(normalizedSpace);
    
    return {
      id: space.id || spaceId,
      name: space.name || space.title || 'Unnamed Space',
      description: space.description,
      owner: space.owner || space.creator_email || space.creator,
      warehouse: warehouse ? {
        id: warehouse.id,
        name: warehouse.name,
        type: warehouse.warehouse_type,
        serverless: warehouse.enable_serverless_compute
      } : null,
      ...scoreResult,
      status: getStatus(scoreResult.totalScore),
      scannedAt: new Date().toISOString(),
      raw: normalizedSpace
    };
  } catch (error) {
    console.error(`Error scanning space ${spaceId}:`, error);
    throw error;
  }
}

async function scanAllSpaces(databricksClient, ownerFilter = null) {
  try {
    const response = await databricksClient.listGenieSpaces();
    const spaces = response.spaces || response.rooms || [];
    
    console.log(`📊 Found ${spaces.length} Genie spaces`);
    
    const filteredSpaces = ownerFilter
      ? spaces.filter(s => s.owner === ownerFilter || s.creator_email === ownerFilter || s.creator === ownerFilter)
      : spaces;
    
    const results = await Promise.all(
      filteredSpaces.map(async (space) => {
        const spaceId = space.id || space.space_id || space.room_id;
        try {
          // Org-wide scans should not force-refresh UC metadata for every table.
          return await scanSpace(databricksClient, spaceId, { forceRefreshUc: false });
        } catch (error) {
          console.error(`Failed to scan space ${spaceId}:`, error.message);
          return {
            id: spaceId,
            name: space.name || space.title || 'Unknown',
            error: error.message,
            status: 'error',
            totalScore: 0,
            maturityLevel: 'emerging'
          };
        }
      })
    );
    
    return results;
  } catch (error) {
    console.error('Error scanning all spaces:', error);
    throw error;
  }
}

function normalizeSpaceData(space, warehouse) {
  const serializedObj = parseSerializedSpace(space?.serialized_space);
  const serializedInstructions = extractTextInstructionsFromSerialized(serializedObj);
  const serializedTables = extractTablesFromSerialized(serializedObj);
  const serializedJoins = extractJoinsFromSerialized(serializedObj);
  const serializedSampleQuestions = extractSampleQuestionsFromSerialized(serializedObj);
  const serializedSqlQueries = extractTrustedSqlQueriesFromSerialized(serializedObj);
  const serializedSqlExpressions = extractSqlExpressionsFromSerialized(serializedObj);
  const genieSampleQuestions = Array.isArray(space?._genieSampleQuestions) ? space._genieSampleQuestions : [];
  const genieGetOk = space?._debugGenieGet?.ok;
  const genieGetErr = space?._debugGenieGet?.error;
  const sqCandidates = Array.isArray(space?._debugSampleQuestionCandidates) ? space._debugSampleQuestionCandidates : [];
  const cfgSampleCount = Array.isArray(serializedObj?.config?.sample_questions) ? serializedObj.config.sample_questions.length : 0;

  return {
    id: space.id,
    name: space.name || space.title,
    description: space.description || '',
    // Prefer serialized_space instructions when present (this is what the Genie UI edits).
    instructions: serializedInstructions || space.instructions || space.system_prompt || space.curated_instructions || '',
    warehouse: warehouse ? {
      id: warehouse.id,
      name: warehouse.name,
      warehouse_type: warehouse.warehouse_type,
      enable_serverless_compute: warehouse.enable_serverless_compute,
      cluster_size: warehouse.cluster_size
    } : null,
    // Prefer Genie Settings → Sample questions when available; otherwise fall back.
    sampleQuestions: genieSampleQuestions.length
      ? genieSampleQuestions
      : (serializedSampleQuestions.length
        ? serializedSampleQuestions
        : (space.sample_questions || space.sample_prompts || space.examples || [])),
    // Non-sensitive debug breadcrumbs to help diagnose why scoring didn’t change.
    debug: {
      sampleQuestionsCount: genieSampleQuestions.length ? genieSampleQuestions.length : serializedSampleQuestions.length,
      sampleQuestionsSource: genieSampleQuestions.length
        ? 'genie_get'
        : (serializedSampleQuestions.length
          ? (cfgSampleCount ? 'serialized_space.config.sample_questions' : 'serialized_space')
          : 'none'),
      genieGetOk: genieGetOk === undefined ? null : Boolean(genieGetOk),
      genieGetError: genieGetErr ? String(genieGetErr).slice(0, 120) : null,
      sampleQuestionCandidates: sqCandidates.slice(0, 8),
      configSampleQuestionsCount: cfgSampleCount
    },
    // Prefer serialized_space SQL snippets when available.
    sqlExpressions: serializedSqlExpressions.length
      ? serializedSqlExpressions
      : (space.sql_expressions || space.curated_questions || []),
    // Prefer trusted query definitions from serialized_space when available.
    sqlQueries: serializedSqlQueries.length ? serializedSqlQueries : (space.sql_queries || space.trusted_queries || []),
    // Prefer serialized_space joins (authoritative) when available.
    joins: serializedJoins.length ? serializedJoins : (space.joins || space.table_relationships || []),
    // Prefer serialized_space tables (authoritative) when available.
    tables: serializedTables.length ? serializedTables : (space.tables || space.table_identifiers || space.data_sources || []),
    feedbackEnabled: space.feedback_enabled !== false,
    owner: space.owner || space.creator_email || space.creator,
    createdAt: space.created_at || space.create_time,
    updatedAt: space.updated_at || space.update_time
  };
}

function calculateOrgStats(scanResults) {
  const validResults = scanResults.filter(r => !r.error);
  
  if (validResults.length === 0) {
    return {
      totalSpaces: 0,
      avgScore: 0,
      criticalCount: 0,
      sharedWarehouseCount: 0,
      maturityDistribution: { optimized: 0, maturing: 0, developing: 0, emerging: 0 }
    };
  }
  
  // Count spaces using a warehouse that is referenced by multiple spaces.
  // This is a closer interpretation of "shared warehouse" than "non-serverless".
  const warehouseUsage = new Map(); // warehouseId -> number of spaces
  validResults.forEach(r => {
    const wid = r.warehouse?.id;
    if (!wid) return;
    warehouseUsage.set(wid, (warehouseUsage.get(wid) || 0) + 1);
  });
  const sharedWarehouseSpaceCount = validResults.filter(r => {
    const wid = r.warehouse?.id;
    if (!wid) return false;
    return (warehouseUsage.get(wid) || 0) > 1;
  }).length;

  return {
    totalSpaces: validResults.length,
    avgScore: Math.round(validResults.reduce((sum, r) => sum + r.totalScore, 0) / validResults.length),
    criticalCount: validResults.filter(r => r.totalScore < 40).length,
    sharedWarehouseCount: sharedWarehouseSpaceCount,
    maturityDistribution: {
      optimized: validResults.filter(r => r.maturityLevel === 'optimized').length,
      maturing: validResults.filter(r => r.maturityLevel === 'maturing').length,
      developing: validResults.filter(r => r.maturityLevel === 'developing').length,
      emerging: validResults.filter(r => r.maturityLevel === 'emerging').length
    }
  };
}

module.exports = { scanSpace, scanAllSpaces, calculateOrgStats };
