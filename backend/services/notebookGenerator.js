/**
 * Notebook generator for "Fix it for me" notebooks.
 *
 * Generates a single Jupyter notebook containing all failing criteria for a space,
 * grouped by category, with a guided remediation plan.
 */

function safeSlug(input) {
  const s = String(input || '').trim().toLowerCase();
  const cleaned = s
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+/, '')
    .replace(/-+$/, '');
  return cleaned || 'space';
}

function isoForPath(d = new Date()) {
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}${pad(d.getUTCMonth() + 1)}${pad(d.getUTCDate())}-${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}Z`;
}

function toLines(text) {
  return String(text || '')
    .split('\n')
    .map((l) => (l.endsWith('\n') ? l : `${l}\n`));
}

function extractTableNames(rawTables) {
  if (!Array.isArray(rawTables)) return [];
  const names = [];
  for (const t of rawTables) {
    if (!t) continue;
    if (typeof t === 'string') {
      names.push(t);
      continue;
    }
    if (typeof t === 'object') {
      const n =
        t.full_name ||
        t.fullName ||
        t.name ||
        t.table_name ||
        t.tableName ||
        t.identifier ||
        null;
      if (n) names.push(String(n));
    }
  }
  // de-dupe preserving order
  const seen = new Set();
  return names.filter((n) => {
    const key = n.trim();
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function assistantPromptForCriterion({ spaceName, spaceId, criterion, tables, tablesPlaceholder = false }) {
  const header = [
    'You are helping improve a Databricks Genie space.',
    '',
    `Space: ${spaceName}`,
    `Space ID: ${spaceId}`,
    '',
    `Failing criterion: ${criterion.name} (+${criterion.points})`,
    criterion.recommendation ? `Suggestion: ${criterion.recommendation}` : '',
    '',
    tablesPlaceholder
      ? '__TABLES_SECTION__'
      : (tables?.length ? `Tables in this space:\n${tables.map(t => `- ${t}`).join('\n')}` : 'Tables in this space: (unknown)'),
    '',
    'Please provide a concrete, step-by-step fix. Prefer safe, incremental changes.'
  ]
    .filter(Boolean)
    .join('\n');

  switch (criterion.id) {
    case 'has_join':
      return [
        header,
        '',
        'Goal: Define at least one join between two tables in this Genie space.',
        '',
        'What I need from you:',
        '- If known: what each table represents (entity) and the “grain” (one row per what?)',
        '- The primary key (PK) of dimension/lookup tables and the foreign keys (FK) in fact/event tables',
        '',
        'Step 1 - propose 1-3 join candidates',
        '- Use naming conventions (id, *_id) plus table semantics.',
        '',
        'Step 2 - for EACH candidate, include ALL of the following:',
        '- Left table + right table (recommended direction: fact/event -> dimension)',
        '- Join keys (left_key = right_key)',
        '- Join type (start with LEFT JOIN unless you are certain INNER is correct)',
        '- Relationship/cardinality: 1:1, 1:many, many:1, or many:many',
        '- Expected uniqueness (which side should be unique?) and how you’d validate it (example SQL)',
        '- What will break if cardinality is wrong (duplicate rows / inflated metrics)',
        '',
        'Step 3 - guidance to avoid common join mistakes',
        '- If both sides can repeat the key => many:many. Suggest an intermediate bridge table or pre-aggregation.',
        '- If joining a fact table to another fact table, call it out as high-risk and propose a safer modeling step.',
        '',
        'Step 4 - how to apply it in Genie (make it concrete)',
        '- Tell me exactly what to enter in Genie’s Join UI: left table, right table, keys, and join type.',
        '- Then propose a short addition to the space’s Text Instructions explaining the correct grain and preferred join path.',
        '',
        'If anything is ambiguous, ask me 2–3 targeted questions instead of guessing.'
      ].join('\n');
    case 'has_instructions':
    case 'lean_instructions':
      return [
        header,
        '',
        'Goal: Provide concise Genie “Text instructions” (<500 chars) that improve accuracy.',
        'Write the exact instruction text. Keep it short and practical. Mention how to handle ambiguity and how to reference tables.'
      ].join('\n');
    case 'sample_questions_5':
    case 'sample_questions_10':
      return [
        header,
        '',
        'Goal: Generate high-quality sample questions for this Genie space.',
        'Return a numbered list of questions that cover common business intents and require joins/filters/aggregations.',
        'Avoid questions that are too vague. Include at least 2 that require a join.',
        '',
        'IMPORTANT:',
        '- These are “Sample questions” shown in the Genie UI (Settings → Sample questions).',
        '- Return at least 5 questions (or 10 if aiming for the 10+ criterion).',
        '- Do NOT return SQL here. Just the natural-language questions.'
      ].join('\n');
    case 'sql_expressions_3':
    case 'sql_expressions_5':
      return [
        header,
        '',
        'Goal: Propose useful SQL expressions (calculated metrics) for this space.',
        'Return 3–5 expressions with names, definitions, and example usage.',
        '',
        'For EACH expression, label it clearly as one of:',
        '- Measure (aggregated metric like SUM/AVG/COUNT)',
        '- Dimension (derived grouping key like bucket, category, date grain)',
        '- Filter (boolean predicate / reusable WHERE condition)',
        '',
        'Also specify:',
        '- Expected grain (what it aggregates by, if any)',
        '- Where it should be used (SELECT vs GROUP BY vs WHERE/HAVING)',
        '',
        'IMPORTANT (Genie requirement):',
        '- Genie SQL Expressions must be unambiguous about where columns come from.',
        '- For every column referenced, specify the SOURCE TABLE (from the list above) and use fully-qualified references, e.g. `schema.table`.`column`.',
        '- If an expression depends on a join, say so explicitly and name the join keys.',
        '',
        'Deliverable format (repeat per expression):',
        '- Name:',
        '- Type: Measure | Dimension | Filter',
        '- Source table(s) + columns used:',
        '- Expression SQL (copy/paste-ready):',
        '- Instruction text (1–2 sentences for Genie UI explaining what it means + when to use it):',
        '- Example usage query snippet:'
      ].join('\n');
    case 'sql_queries_3':
    case 'sql_queries_5':
      return [
        header,
        '',
        'Goal: Propose trusted SQL queries for common patterns in this space.',
        'Return 3–5 query entries suitable for Genie “Configure → SQL queries & functions”.',
        '',
        'For EACH query, include:',
        '- Display name (short, readable)',
        '- User-facing question (natural language)',
        '- SQL (copy/paste-ready; fully qualify table/column references)',
        '- Notes on joins/cardinality if applicable'
      ].join('\n');
    case 'table_descriptions':
      return [
        header,
        '',
        'Goal: Add table descriptions INSIDE the Genie space (Configure → Data & descriptions).',
        '',
        'Steps:',
        '1) Open the Genie space → Configure → Data & descriptions.',
        '2) For each table, add a 1–3 sentence description that explains:',
        '   - what one row represents (grain),',
        '   - key entities/metrics,',
        '   - important filters (e.g., time zone, status meanings).',
        '3) Save, then rescan in GenieIQ.',
        '',
        'Deliverable:',
        '- Provide suggested descriptions for each table listed above (one per table).',
        '',
        'If table semantics are unclear, ask 1–3 targeted questions instead of guessing.'
      ].join('\n');
    case 'column_descriptions':
      return [
        header,
        '',
        'Goal: Add key column descriptions INSIDE the Genie space (Configure → Data & descriptions).',
        '',
        'Steps:',
        '1) Open Configure → Data & descriptions.',
        '2) For each table, add short, high-signal descriptions for key columns:',
        '   - primary/foreign keys (what they reference),',
        '   - timestamps (timezone + meaning),',
        '   - status/category fields (allowed values + meaning),',
        '   - core metrics (units + calculation notes).',
        '3) Save, then rescan in GenieIQ.',
        '',
        'Deliverable:',
        '- Propose descriptions for the most important columns in each table (start with id/*_id, dates, status, and metrics).',
        '',
        'If you need the column list, ask me to paste it (or tell me to run a DESCRIBE).'
      ].join('\n');
    default:
      return header;
  }
}

function buildFixNotebook({ scanResult, workspaceHost }) {
  const spaceId = scanResult?.id || '';
  const spaceName = scanResult?.name || 'Genie Space';
  const score = scanResult?.totalScore ?? 0;
  const maturityLabel = scanResult?.maturityLabel || scanResult?.maturityLevel || '';

  const findings = Array.isArray(scanResult?.findings) ? scanResult.findings : [];
  const failing = findings.filter((f) => !f.passed);
  const raw = scanResult?.raw || {};
  const tableNames = extractTableNames(raw.tables);

  // Group failing items by category
  const groups = new Map(); // key -> { key, name, items: [] }
  for (const f of failing) {
    const key = f.category || 'other';
    const name = f.categoryName || key;
    if (!groups.has(key)) groups.set(key, { key, name, items: [] });
    groups.get(key).items.push({
      id: f.id,
      name: f.name,
      points: f.points,
      recommendation: f.recommendation,
      category: f.category,
      categoryName: f.categoryName
    });
  }

  const categoryOrder = ['foundation', 'dataSetup', 'sqlAssets', 'optimization'];
  const orderedGroups = [];
  for (const k of categoryOrder) if (groups.has(k)) orderedGroups.push(groups.get(k));
  for (const [k, v] of groups.entries()) if (!categoryOrder.includes(k)) orderedGroups.push(v);

  const now = new Date().toISOString();
  const dbHost = workspaceHost || process.env.DATABRICKS_HOST || '';
  const workspaceUrl = dbHost
    ? (dbHost.startsWith('http') ? dbHost : `https://${dbHost}`)
    : '';

  const title = `GenieIQ Fix Notebook - ${spaceName}`;

  const introMd = [
    `# ${title}`,
    '',
    `Generated: **${now}**`,
    '',
    `Space: **${spaceName}**`,
    `- Space ID: \`${spaceId}\``,
    `- Current score: **${score} / 100**${maturityLabel ? ` (${maturityLabel})` : ''}`,
    '',
    '## How to use this notebook',
    '',
    '1. Review the failing items and suggested fixes below.',
    '2. Apply the changes in the Genie UI (recommended for safety), or implement API calls in the “Apply” section if you’re comfortable.',
    '3. Re-run the verification cell to see your updated score.',
    ''
  ].join('\n');

  const planMdLines = [];
  planMdLines.push('## Failing items (grouped)');
  planMdLines.push('');
  if (orderedGroups.length === 0) {
    planMdLines.push('✅ No failing criteria found. Your space is in great shape!');
  } else {
    for (const g of orderedGroups) {
      planMdLines.push(`### ${g.name}`);
      planMdLines.push('');
      for (const item of g.items) {
        planMdLines.push(`- [ ] **${item.name}** (+${item.points})`);
        if (item.recommendation) {
          planMdLines.push(`  - Suggestion: ${item.recommendation}`);
        }
      }
      planMdLines.push('');
    }
  }

  const guidedFixCells = [];
  let addedInstructionsGenerator = false;
  if (orderedGroups.length === 0) {
    guidedFixCells.push({
      cell_type: 'markdown',
      metadata: {},
      source: toLines('No failing criteria found. Nothing to fix.\n')
    });
  } else {
    for (const group of orderedGroups) {
      guidedFixCells.push({
        cell_type: 'markdown',
        metadata: {},
        source: toLines(`### ${group.name}\n`)
      });

      for (const item of group.items) {
        const isInstructionsCriterion = item.id === 'has_instructions' || item.id === 'lean_instructions';
        // Collapse the two instruction criteria into a single guided section to reduce clutter.
        if (isInstructionsCriterion && addedInstructionsGenerator) {
          continue;
        }

        const assistantOnlyCriterion = item.id === 'table_descriptions';

        const md = [
          isInstructionsCriterion
            ? '#### Text Instructions (covers “Text Instructions Defined” + “Instructions are Lean”)'
            : `#### ${item.name} (+${item.points})`,
          '',
          isInstructionsCriterion
            ? 'Generate concise, context-aware Text Instructions (≤ 500 chars) based on the actual tables in this space.'
            : (item.recommendation ? `Suggestion: ${item.recommendation}` : ''),
          ''
        ]
          .filter(Boolean)
          .join('\n');

        if (!assistantOnlyCriterion) {
          guidedFixCells.push({
            cell_type: 'markdown',
            metadata: {},
            source: toLines(md)
          });
        }

        // Criterion-specific helper cells
        // For `has_join`, we rely primarily on the Assistant prompt (cleaner + often more useful),
        // so we intentionally do not add an extra heuristic join-suggestion code cell here.

        if (!addedInstructionsGenerator && (item.id === 'has_instructions' || item.id === 'lean_instructions')) {
          addedInstructionsGenerator = true;
          guidedFixCells.push({
            cell_type: 'code',
            metadata: {},
            execution_count: null,
            outputs: [],
            source: toLines(
              [
                '# Generate context-aware Genie text instructions (kept under 500 chars)',
                '',
                'def analyze_table_context(table_name):',
                '    """Analyze table to understand purpose, key columns, and basic characteristics (best effort)."""',
                '    try:',
                '        meta = get_uc_table(table_name)',
                '        columns = meta.get("columns") or []',
                '',
                '        col_info = []',
                '        for c in columns:',
                '            col_name = c.get("name")',
                '            col_type = c.get("type_name", "unknown")',
                '            col_comment = c.get("comment", "") or ""',
                '            if col_name:',
                '                col_info.append({',
                '                    "name": col_name,',
                '                    "type": col_type,',
                '                    "comment": col_comment',
                '                })',
                '',
                '        table_comment = meta.get("comment", "") or ""',
                '',
                '        # Optional: sample a few rows to infer basic characteristics (won\'t fail the cell)',
                '        sample_data = []',
                '        try:',
                '            sample_query = f"SELECT * FROM {table_name} LIMIT 3"',
                '            result = spark.sql(sample_query).collect()',
                '            sample_data = [row.asDict() for row in result]',
                '        except Exception:',
                '            sample_data = []',
                '',
                '        return {',
                '            "name": table_name,',
                '            "short_name": table_name.split(".")[-1],',
                '            "columns": col_info,',
                '            "comment": table_comment,',
                '            "sample_data": sample_data',
                '        }',
                '    except Exception:',
                '        return None',
                '',
                'def generate_smart_instructions():',
                '    """Generate intelligent, context-aware Genie instructions (<= 500 chars)."""',
                '    if not TABLES:',
                '        return "Answer questions using available data. Always show SQL. Ask clarifying questions when ambiguous."',
                '',
                '    table_contexts = []',
                '    for table in TABLES:',
                '        ctx = analyze_table_context(table)',
                '        if ctx:',
                '            table_contexts.append(ctx)',
                '',
                '    if not table_contexts:',
                '        return "Answer questions using available tables. Always show SQL. When ambiguous, ask clarifying questions."',
                '',
                '    instructions_parts = []',
                '    sep = ", "',
                '',
                '    # 1) Scope: list tables (short names)',
                '    short_names = [t["short_name"] for t in table_contexts]',
                '    if len(short_names) == 1:',
                '        instructions_parts.append(f"Answer questions about {short_names[0]} data.")',
                '    else:',
                '        instructions_parts.append(f"Answer questions using: {sep.join(short_names)}.")',
                '',
                '    # 2) Join hint (if we can find common key-ish columns)',
                '    common_cols = set()',
                '    join_cols = []',
                '    if len(table_contexts) > 1:',
                '        cols_by_table = {t["short_name"]: [c["name"].lower() for c in t["columns"] if c.get("name")] for t in table_contexts}',
                '        common_cols = set(next(iter(cols_by_table.values())) or [])',
                '        for cols in cols_by_table.values():',
                '            common_cols &= set(cols or [])',
                '        if common_cols:',
                '            join_cols = [c for c in sorted(common_cols) if "id" in c]',
                '            if join_cols:',
                '                instructions_parts.append(f"Join tables using: {sep.join(join_cols[:3])}.")',
                '',
                '    # 3) Best practices',
                '    instructions_parts.append("Always show SQL.")',
                '    instructions_parts.append("Ask clarifying questions when ambiguous.")',
                '',
                '    instructions = " ".join(instructions_parts)',
                '',
                '    # Ensure <= 500 chars',
                '    if len(instructions) > 500:',
                '        essential = f"Answer questions using {sep.join(short_names)}. Always show SQL. Ask clarifying questions when ambiguous."',
                '        if join_cols:',
                '            hint = f" Join using {join_cols[0]}."',
                '            if len(essential + hint) <= 500:',
                '                essential += hint',
                '        instructions = essential',
                '',
                '    return instructions',
                '',
                'print("Analyzing tables and generating context-aware instructions...\\n")',
                'INSTRUCTIONS = generate_smart_instructions()',
                '',
                'print("=" * 70)',
                'print("GENERATED GENIE TEXT INSTRUCTIONS")',
                'print("=" * 70)',
                'print(INSTRUCTIONS)',
                'print("=" * 70)',
                'print(f"\\nLength: {len(INSTRUCTIONS)} characters (max: 500)")',
                'print(f"Status: {\'✓ Valid\' if len(INSTRUCTIONS) <= 500 else \'✗ Too long\'}")',
                '',
                'print(f"\\n📊 Tables used: {len(TABLES)}")',
                'for t in TABLES:',
                '    print(f"  - {t}")',
                '',
                'print("\\n💡 Next steps:")',
                'print("  1. Copy the instructions above into: Configure > Instructions > Text")',
                'print("  2. Customize for business context (fiscal year, jargon, formatting rules, etc.)")',
                ''
              ].join('\n')
            )
          });
        }

        // For sample question criteria, we rely on the Assistant prompt (cleaner + more tailored),
        // so we intentionally do not add a separate static template cell.

        // Assistant prompt cell (copy/paste into Databricks Assistant)
        // Skip for instruction criteria to keep the notebook simple and avoid duplicating guidance.
        if (!isInstructionsCriterion) {
          const prompt = assistantPromptForCriterion({
            spaceName,
            spaceId,
            criterion: item,
            tables: tableNames,
            tablesPlaceholder: true
          });
          guidedFixCells.push({
            cell_type: 'code',
            metadata: {},
            execution_count: null,
            outputs: [],
            source: toLines(
              [
                '# Databricks Assistant prompt (copy/paste into Assistant)',
                `PROMPT_TEMPLATE = ${JSON.stringify(prompt)}`,
                '',
                'def _tables_section():',
                '    if TABLES:',
                '        return "Tables in this space:\\n" + "\\n".join([f"- {t}" for t in TABLES])',
                '    return "Tables in this space: (unknown)"',
                '',
                'PROMPT = PROMPT_TEMPLATE.replace("__TABLES_SECTION__", _tables_section())',
                'print(PROMPT)',
                ''
              ].join('\n')
            )
          });
        }
      }
    }
  }

  const notebook = {
    nbformat: 4,
    nbformat_minor: 5,
    metadata: {
      kernelspec: { name: 'python3', display_name: 'Python 3', language: 'python' },
      language_info: { name: 'python', version: '3' }
    },
    cells: [
      {
        cell_type: 'markdown',
        metadata: {},
        source: introMd.split('\n').map((l) => `${l}\n`)
      },
      {
        cell_type: 'code',
        metadata: {},
        execution_count: null,
        outputs: [],
        source: toLines(
          [
            '# Setup (no secrets embedded)',
            'import json',
            'import re',
            'import textwrap',
            'from datetime import datetime',
            'import urllib.parse',
            '',
            'try:',
            '    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()',
            '    WORKSPACE_HOST = ctx.apiUrl().get()',
            '    WORKSPACE_TOKEN = ctx.apiToken().get()',
            'except Exception as e:',
            '    WORKSPACE_HOST = None',
            '    WORKSPACE_TOKEN = None',
            '',
            `SPACE_ID = ${JSON.stringify(spaceId)}`,
            `SPACE_NAME = ${JSON.stringify(spaceName)}`,
            `TABLES = ${JSON.stringify(tableNames, null, 2)}`,
            '',
            'print("Workspace:", WORKSPACE_HOST)',
            'print("Space:", SPACE_NAME, "(", SPACE_ID, ")")',
            '',
            '# Read any existing widget values (if you already ran this notebook once).',
            'RAW_TABLES_WIDGET = ""',
            'RAW_EXPORT_WIDGET = ""',
            'try:',
            '    RAW_TABLES_WIDGET = dbutils.widgets.get("genieiq_tables")',
            'except Exception:',
            '    RAW_TABLES_WIDGET = ""',
            'try:',
            '    RAW_EXPORT_WIDGET = dbutils.widgets.get("genieiq_export_json")',
            'except Exception:',
            '    RAW_EXPORT_WIDGET = ""',
            '',
            '# If you already pasted table names into the widget, honor that first (override).',
            'if RAW_TABLES_WIDGET and RAW_TABLES_WIDGET.strip():',
            '    parts = [p.strip() for p in re.split(r"[\\n,]+", RAW_TABLES_WIDGET) if p.strip()]',
            '    TABLES = [p for p in parts if 1 <= p.count(".") <= 2]',
            '    if TABLES:',
            '        print("Loaded tables from widget override:", len(TABLES))',
            '',
            '# Helper to call Databricks REST API (optional)',
            'def dbx_api(method, path, json_body=None, params=None):',
            '    if not WORKSPACE_HOST or not WORKSPACE_TOKEN:',
            '        raise RuntimeError("No workspace context available. Attach this notebook to a Databricks compute.")',
            '    import requests',
            '    url = f"{WORKSPACE_HOST}/api/2.0{path}"',
            '    headers = {"Authorization": f"Bearer {WORKSPACE_TOKEN}"}',
            '    r = requests.request(method, url, headers=headers, json=json_body, params=params)',
            '    if r.status_code >= 400:',
            '        raise RuntimeError(f"{r.status_code}: {r.text[:300]}")',
            '    if not r.text:',
            '        return {}',
            '    try:',
            '        return r.json()',
            '    except Exception:',
            '        return {"raw": r.text}',
            '',
            'def collect_table_names(obj):',
            '    # Heuristic fallback: collect UC-ish table names from any JSON payload',
            '    out = set()',
            '    def walk(x):',
            '        if isinstance(x, dict):',
            '            for k, v in x.items():',
            '                if isinstance(v, str) and any(s in k.lower() for s in ["table", "identifier", "full_name", "fullname"]):',
            '                    walk(v)',
            '                else:',
            '                    walk(v)',
            '        elif isinstance(x, list):',
            '            for v in x[:400]:',
            '                walk(v)',
            '        elif isinstance(x, str):',
            '            s = x.strip()',
            '            if s.count(".") >= 1 and " " not in s and "/" not in s:',
            '                out.add(s)',
            '    walk(obj)',
            '    return sorted(out)',
            '',
            'def extract_tables_from_serialized_space(serialized_space):',
            '    """Extract table identifiers from Genie serialized_space (JSON string or dict)."""',
            '    try:',
            '        space_config = serialized_space',
            '        if isinstance(serialized_space, str):',
            '            space_config = json.loads(serialized_space)',
            '        if not isinstance(space_config, dict):',
            '            return []',
            '        tables = (space_config.get("data_sources") or {}).get("tables") or []',
            '        out = []',
            '        seen = set()',
            '        for t in tables:',
            '            if not isinstance(t, dict):',
            '                continue',
            '            ident = t.get("identifier") or t.get("full_name") or t.get("name")',
            '            if not ident:',
            '                continue',
            '            ident = str(ident).strip()',
            '            if not ident or ident in seen:',
            '                continue',
            '            seen.add(ident)',
            '            out.append(ident)',
            '        return out',
            '    except Exception as e:',
            '        return []',
            '',
            'def detect_tables_from_genie_space():',
            '    """Best-effort: prefer include_serialized_space, then fall back to heuristics."""',
            '    # 1) Prefer serialized_space (most reliable when available)',
            '    try:',
            '        resp = dbx_api("GET", f"/genie/spaces/{SPACE_ID}", params={"include_serialized_space": "true"})',
            '        serialized_space = None',
            '        if isinstance(resp, dict):',
            '            serialized_space = resp.get("serialized_space")',
            '        if serialized_space:',
            '            tables = extract_tables_from_serialized_space(serialized_space)',
            '            if tables:',
            '                print("Successfully retrieved serialized space configuration")',
            '                return tables',
            '    except Exception:',
            '        pass',
            '',
            '    # 2) Fallback: scrape whatever the API returns',
            '    try:',
            '        resp = dbx_api("GET", f"/genie/spaces/{SPACE_ID}")',
            '        tables = collect_table_names(resp)',
            '        if tables:',
            '            return tables',
            '    except Exception:',
            '        pass',
            '',
            '    return []',
            '',
            'def extract_tables_from_export(obj):',
            '    out = []',
            '    seen = set()',
            '    def push(v):',
            '        if not v: return',
            '        s = str(v).strip()',
            '        if not s: return',
            '        if s in seen: return',
            '        # Prefer UC-style (catalog.schema.table) but allow schema.table',
            '        if s.count(".") < 1: return',
            '        seen.add(s)',
            '        out.append(s)',
            '    def walk(x):',
            '        if x is None: return',
            '        if isinstance(x, str):',
            '            push(x)',
            '            return',
            '        if isinstance(x, list):',
            '            for i in x: walk(i)',
            '            return',
            '        if isinstance(x, dict):',
            '            for k in ("full_name","fullName","table_name","tableName","identifier","name"):',
            '                if k in x and isinstance(x[k], str): push(x[k])',
            '            for v in x.values(): walk(v)',
            '    walk(obj)',
            '    return out',
            '',
            'if not TABLES:',
            '    auto_tables = detect_tables_from_genie_space()',
            '    if auto_tables:',
            '        TABLES = auto_tables',
            '        print("Auto-detected tables from Genie space payload:", len(TABLES))',
            '',
            '# Try Genie Export API (often contains the configured tables/joins)',
            'if not TABLES:',
            '    try:',
            '        export_payload = dbx_api("GET", "/genie/spaces/export", params={"space_id": SPACE_ID})',
            '        if isinstance(export_payload, dict) and isinstance(export_payload.get("raw"), str):',
            '            try:',
            '                export_payload = json.loads(export_payload["raw"])',
            '            except Exception:',
            '                pass',
            '        # Some exports may include serialized-space-like objects',
            '        TABLES = extract_tables_from_serialized_space(export_payload) or extract_tables_from_export(export_payload)',
            '        if TABLES:',
            '            print("Auto-detected tables from Genie Export:", len(TABLES))',
            '    except Exception:',
            '        pass',
            '',
            '# Option A (recommended): paste Genie Export JSON (or just the tables section) here',
            'if RAW_EXPORT_WIDGET and RAW_EXPORT_WIDGET.strip() and not TABLES:',
            '    try:',
            '        export_obj = json.loads(RAW_EXPORT_WIDGET)',
            '    except Exception:',
            '        export_obj = None',
            '    if export_obj is not None:',
            '        TABLES = extract_tables_from_serialized_space(export_obj) or extract_tables_from_export(export_obj)',
            '        if TABLES:',
            '            print("Loaded tables from export JSON widget:", len(TABLES))',
            '',
            '# (Re)create widgets at the end so the UI reflects what we detected.',
            'try:',
            '    for w in ["genieiq_tables", "genieiq_export_json"]:',
            '        try:',
            '            dbutils.widgets.remove(w)',
            '        except Exception:',
            '            pass',
            '    tables_default = RAW_TABLES_WIDGET.strip() if (RAW_TABLES_WIDGET and RAW_TABLES_WIDGET.strip()) else ("\\n".join(TABLES) if TABLES else "")',
            '    dbutils.widgets.text("genieiq_tables", tables_default, "GenieIQ tables (catalog.schema.table)")',
            '    # Only show the export widget if we still don’t have tables (keeps the notebook UI cleaner).',
            '    if not TABLES:',
            '        dbutils.widgets.text("genieiq_export_json", RAW_EXPORT_WIDGET or "", "GenieIQ: paste Genie Export JSON (optional)")',
            'except Exception:',
            '    pass',
            '',
            'print("Tables in space:", len(TABLES))',
            'if not TABLES:',
            '    print("Could not auto-detect tables from the Genie API payload.")',
            '    print("Tip: widgets appear at the top of the notebook.")',
            '    print("If your space has tables configured, use the Genie UI Export and paste the JSON into genieiq_export_json, or paste table names into genieiq_tables, then re-run this cell.")',
            '',
            'def get_uc_table(full_name: str):',
            '    enc = urllib.parse.quote(full_name, safe="")',
            '    return dbx_api("GET", f"/unity-catalog/tables/{enc}")',
            '',
            'def get_columns_for_tables(tables):',
            '    cols = {}',
            '    for t in tables:',
            '        try:',
            '            meta = get_uc_table(t)',
            '            columns = meta.get("columns") or []',
            '            cols[t] = [c.get("name") for c in columns if c.get("name")]',
            '        except Exception as e:',
            '            cols[t] = []',
            '    return cols',
            '',
            'def suggest_join_pairs(columns_by_table):',
            '    # Heuristic: look for shared key-like column names (id, *_id) across tables',
            '    tables = list(columns_by_table.keys())',
            '    suggestions = []',
            '    for i in range(len(tables)):',
            '        for j in range(i + 1, len(tables)):',
            '            a, b = tables[i], tables[j]',
            '            ca = set([c.lower() for c in columns_by_table[a] if c])',
            '            cb = set([c.lower() for c in columns_by_table[b] if c])',
            '            common = ca & cb',
            '            keyish = [c for c in common if c == "id" or c.endswith("_id")]',
            '            if keyish:',
            '                # pick the most specific key (longest name)',
            '                k = sorted(keyish, key=lambda x: len(x), reverse=True)[0]',
            '                suggestions.append((a, b, k))',
            '    return suggestions',
            ''
          ].join('\n')
        )
      },
      {
        cell_type: 'markdown',
        metadata: {},
        source: planMdLines.map((l) => `${l}\n`)
      },
      {
        cell_type: 'markdown',
        metadata: {},
        source: toLines(
          [
            '## Guided fixes (one section per failing item)',
            '',
            'Each failing criterion below has:',
            '- a short explanation,',
            '- a practical next action, and',
            '- an “Assistant prompt” you can paste into Databricks Assistant for tailored help.',
            ''
          ].join('\n')
        )
      },
      ...guidedFixCells,
      {
        cell_type: 'markdown',
        metadata: {},
        source: toLines(
          [
            '## Apply fixes (guided)',
            '',
            'For v1, this notebook is **deliberately conservative**: it gives you a clear plan and suggested content, and you apply it in the Genie UI.',
            '',
            'If/when we confirm stable write APIs for Genie spaces, we can upgrade this notebook to apply changes automatically.',
            '',
            `- Open your space in the UI: ${workspaceUrl ? `${workspaceUrl}/genie` : '(workspace host not configured in app env)'}`,
            ''
          ].join('\n')
        )
      },
      {
        cell_type: 'code',
        metadata: {},
        execution_count: null,
        outputs: [],
        source: toLines(
          [
            '# Optional: fetch current Genie space JSON (read-only)',
            '# Note: This call requires the right scopes/permissions.',
            '',
            'try:',
            '    space = dbx_api("GET", f"/genie/spaces/{SPACE_ID}")',
            '    print("Fetched space:", space.get("name") or space.get("title"))',
            'except Exception as e:',
            '    print("Could not fetch space via API:", e)',
            ''
          ].join('\n')
        )
      },
      {
        cell_type: 'markdown',
        metadata: {},
        source: toLines(
          [
            '## Verify (rescan in GenieIQ)',
            '',
            'After applying changes, go back to the GenieIQ app and click “Scan Again” for this space to see the score delta.',
            ''
          ].join('\n')
        )
      }
    ]
  };

  return JSON.stringify(notebook, null, 2);
}

module.exports = { buildFixNotebook, safeSlug, isoForPath };

