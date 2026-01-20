#!/bin/bash
set -euo pipefail

# GenieIQ local setup helper (safe + repo-contained).
# Creates/updates .env based on env.example and optional Lakebase OAuth token paste.

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$ROOT_DIR"

ENV_FILE="$ROOT_DIR/.env"
EXAMPLE_FILE="$ROOT_DIR/env.example"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

echo ""
echo -e "${CYAN}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║${NC}   ${BOLD}GenieIQ - Local Setup${NC}                                 ${CYAN}║${NC}"
echo -e "${CYAN}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""

if [ ! -f "$EXAMPLE_FILE" ]; then
  echo -e "${RED}✗ Missing env.example${NC}"
  exit 1
fi

if [ ! -f "$ENV_FILE" ]; then
  cp "$EXAMPLE_FILE" "$ENV_FILE"
  echo -e "${GREEN}✓ Created .env from env.example${NC}"
else
  echo -e "${GREEN}✓ Using existing .env${NC}"
fi

backup_env() {
  cp "$ENV_FILE" "$ENV_FILE.bak.$(date +%Y%m%d_%H%M%S)"
}

set_kv() {
  local key="$1"
  local val="$2"
  if grep -qE "^${key}=" "$ENV_FILE"; then
    # macOS sed needs -i ''.
    sed -i '' -E "s|^${key}=.*|${key}=${val}|g" "$ENV_FILE"
  else
    echo "${key}=${val}" >> "$ENV_FILE"
  fi
}

prompt() {
  local label="$1"
  local varname="$2"
  local example="$3"
  local secret="${4:-false}"
  local current=""
  current="$(grep -E "^${varname}=" "$ENV_FILE" | tail -1 | cut -d'=' -f2- || true)"
  if [ "$secret" = "true" ]; then
    echo ""
    echo -e "${BOLD}${label}${NC}"
    if [ -n "$example" ]; then echo -e "  e.g. ${example}"; fi
    if [ -n "$current" ]; then echo -e "  (currently set; press Enter to keep)"; fi
    read -r -s -p "  Paste value: " v
    echo ""
  else
    echo ""
    echo -e "${BOLD}${label}${NC}"
    if [ -n "$example" ]; then echo -e "  e.g. ${example}"; fi
    if [ -n "$current" ]; then echo -e "  current: ${current}"; fi
    read -r -p "  Enter value (or press Enter to skip/keep): " v
  fi
  if [ -z "${v:-}" ]; then
    return 0
  fi
  backup_env
  set_kv "$varname" "$v"
}

echo -e "${BOLD}Step 1 - Databricks workspace access${NC}"
prompt "Databricks host (used for Genie + UC APIs)" "DATABRICKS_HOST" "https://e2-demo-field-eng.cloud.databricks.com"
prompt "Databricks PAT token (local dev; optional if you only use the deployed App)" "DATABRICKS_TOKEN" "dapixxx" true

echo ""
echo -e "${BOLD}Step 2 - Lakebase (optional but recommended)${NC}"
echo "If you want history/starred/new-spaces to persist locally, fill these in."
echo "If you skip, GenieIQ still runs (in-memory storage)."

prompt "Lakebase host (from the Lakebase instance connection string)" "LAKEBASE_HOST" "instance-xxxxx.database.cloud.databricks.com"

# Most Lakebase instances use the default dbname `databricks_postgres`. Set it automatically to reduce confusion.
CURRENT_LAKEBASE_HOST="$(grep -E '^LAKEBASE_HOST=' "$ENV_FILE" | tail -1 | cut -d'=' -f2- | tr -d '\"' || true)"
CURRENT_LAKEBASE_DB="$(grep -E '^LAKEBASE_DATABASE=' "$ENV_FILE" | tail -1 | cut -d'=' -f2- | tr -d '\"' || true)"
if [ -n "${CURRENT_LAKEBASE_HOST:-}" ] && [ -z "${CURRENT_LAKEBASE_DB:-}" ]; then
  backup_env
  set_kv "LAKEBASE_DATABASE" "databricks_postgres"
fi

echo ""
echo -e "${YELLOW}Note:${NC} Lakebase Postgres usually uses the default dbname ${BOLD}databricks_postgres${NC}."
read -r -p "Advanced: override LAKEBASE_DATABASE? [y/N]: " OVERRIDE_DB
OVERRIDE_DB="${OVERRIDE_DB:-N}"
if [[ "$OVERRIDE_DB" =~ ^[Yy]$ ]]; then
  prompt "Lakebase database name (from psql: dbname=...)" "LAKEBASE_DATABASE" "databricks_postgres"
fi
prompt "Your Databricks user email (used as the Postgres username)" "DATABRICKS_USER" "ryan.cicak@databricks.com"

echo ""
echo -e "${BOLD}Lakebase OAuth token (recommended)${NC}"
echo "Lakebase Postgres typically requires an OAuth token for PGPASSWORD."
echo "Get it from the Lakebase instance UI:"
echo "  Credentials → Get OAuth token (1 hr lifetime) for PGPASSWORD"
echo ""
prompt "Paste Lakebase OAuth token (1 hour lifetime)" "LAKEBASE_TOKEN" "eyJraWQiOi..." true

echo ""
echo -e "${GREEN}✓ Setup complete${NC}"
echo ""
echo "Next (local dev):"
echo "  1) Install deps:   npm run install:all"
echo "  2) Run locally:    npm run dev"
echo ""
echo "Tip: If Lakebase auth fails, your LAKEBASE_TOKEN likely expired. Paste a fresh one and retry."
echo ""

# ----------------------------------------------------------------------------
# Optional: Deploy as a Databricks App (customer-first default)
# ----------------------------------------------------------------------------
echo -e "${BOLD}Optional - Deploy GenieIQ as a Databricks App${NC}"
echo "This will run ./deploy.sh and deploy GenieIQ into the workspace you entered above."
echo "It may open a browser for Databricks CLI auth if you are not already authenticated."
echo ""
read -r -p "Deploy now? [Y/n]: " DO_DEPLOY
DO_DEPLOY="${DO_DEPLOY:-Y}"

if [[ "$DO_DEPLOY" =~ ^[Yy]$ ]]; then
  if ! command -v databricks &> /dev/null; then
    echo -e "${RED}✗ Databricks CLI not found. Install it first:${NC}"
    echo "  brew install databricks/tap/databricks"
    exit 1
  fi

  # Read DATABRICKS_HOST from .env to ensure we use the final value (kept/updated).
  DBX_HOST="$(grep -E '^DATABRICKS_HOST=' "$ENV_FILE" | tail -1 | cut -d'=' -f2- | tr -d '"' || true)"
  if [ -z "${DBX_HOST:-}" ]; then
    echo -e "${RED}✗ Missing DATABRICKS_HOST in .env${NC}"
    exit 1
  fi
  # Normalize: remove trailing slash
  DBX_HOST="${DBX_HOST%/}"

  # If the user provided Lakebase connection info in setup, pass it through so deploy.sh
  # can wire the app to an existing Lakebase instance even if provisioning APIs are blocked/quota is hit.
  EXISTING_LAKEBASE_HOST="$(grep -E '^LAKEBASE_HOST=' "$ENV_FILE" | tail -1 | cut -d'=' -f2- | tr -d '\"' || true)"
  EXISTING_LAKEBASE_DATABASE="$(grep -E '^LAKEBASE_DATABASE=' "$ENV_FILE" | tail -1 | cut -d'=' -f2- | tr -d '\"' || true)"
  EXISTING_LAKEBASE_HOST="${EXISTING_LAKEBASE_HOST%/}"

  echo ""
  echo -e "${GREEN}→ Deploying to ${DBX_HOST}${NC}"
  echo ""
  # Use a stable default profile so repeated runs behave predictably.
  TARGET_DATABRICKS_HOST="$DBX_HOST" \
    DATABRICKS_CONFIG_PROFILE="${DATABRICKS_CONFIG_PROFILE:-genieiq}" \
    EXISTING_LAKEBASE_HOST="${EXISTING_LAKEBASE_HOST:-}" \
    EXISTING_LAKEBASE_DATABASE="${EXISTING_LAKEBASE_DATABASE:-}" \
    ./deploy.sh
else
  echo "Skipped deploy."
  echo "To deploy later: ./deploy.sh"
fi

# ----------------------------------------------------------------------------
# Optional: Grant Unity Catalog read access to the GenieIQ App service principal
# ----------------------------------------------------------------------------
echo ""
echo -e "${BOLD}Optional - Unity Catalog access for the GenieIQ app${NC}"
echo "Some Genie APIs fail if the calling identity lacks UC permissions on any table in a space."
echo "To avoid \"Failed to fetch tables for the space\" errors, you can grant the GenieIQ app service principal"
echo "least-privilege read access to specific UC schema(s)."
echo ""
read -r -p "Grant UC read access to the GenieIQ app now? [y/N]: " DO_GRANTS
DO_GRANTS="${DO_GRANTS:-N}"

if [[ "$DO_GRANTS" =~ ^[Yy]$ ]]; then
  # Pick a reliable CLI binary (many machines have multiple versions).
  DBX_BIN="/opt/homebrew/bin/databricks"
  if [ ! -x "$DBX_BIN" ]; then
    if command -v databricks &> /dev/null; then
      DBX_BIN="$(command -v databricks)"
    else
      echo -e "${RED}✗ Databricks CLI not found. Install it first:${NC}"
      echo "  brew install databricks/tap/databricks"
      exit 1
    fi
  fi

  PROFILE="${DATABRICKS_CONFIG_PROFILE:-genieiq}"
  APP_NAME="${GENIEIQ_APP_NAME:-genieiq}"

  echo ""
  echo -e "${BOLD}Detecting GenieIQ app service principal...${NC}"
  echo "  profile: ${PROFILE}"
  echo "  app:     ${APP_NAME}"

  APP_SP_ID="$("$DBX_BIN" --profile "$PROFILE" apps get "$APP_NAME" --output json 2>/dev/null | python3 -c 'import sys, json
try:
  o = json.load(sys.stdin)
except Exception:
  o = {}
print(o.get("service_principal_client_id") or "")
' || true)"
  if [ -z "${APP_SP_ID:-}" ]; then
    echo -e "${RED}✗ Could not detect the app service principal client id.${NC}"
    echo "Make sure the app exists in this workspace and your CLI profile is authenticated."
    echo "Tip: set GENIEIQ_APP_NAME=<name> and/or DATABRICKS_CONFIG_PROFILE=<profile> and rerun."
    exit 1
  fi

  echo -e "${GREEN}✓ App service principal client id: ${APP_SP_ID}${NC}"
  echo ""
  echo -e "${BOLD}How broad should the app's UC read access be?${NC}"
  echo "  1) Specific schema(s) (recommended)"
  echo "  2) Broad read across ALL catalogs/schemas you can administer (simple, but very permissive)"
  echo ""
  read -r -p "Choose [1/2] (default 1): " GRANT_MODE
  GRANT_MODE="${GRANT_MODE:-1}"

  apply_catalog_grant() {
    local catalog="$1"
    "$DBX_BIN" --profile "$PROFILE" grants update CATALOG "$catalog" \
      --json "{\"changes\":[{\"principal\":\"${APP_SP_ID}\",\"add\":[\"USE_CATALOG\"]}]}" \
      --output json >/dev/null 2>&1 || return 1
    return 0
  }

  apply_schema_grant() {
    local full_schema="$1" # catalog.schema
    "$DBX_BIN" --profile "$PROFILE" grants update SCHEMA "$full_schema" \
      --json "{\"changes\":[{\"principal\":\"${APP_SP_ID}\",\"add\":[\"USE_SCHEMA\",\"SELECT\"]}]}" \
      --output json >/dev/null 2>&1 || return 1
    return 0
  }

  if [ "$GRANT_MODE" = "2" ]; then
    echo ""
    echo -e "${YELLOW}${BOLD}Warning:${NC} This grants the GenieIQ app broad read access across your Unity Catalog."
    echo -e "${YELLOW}This is convenient, but it may exceed least-privilege security standards.${NC}"
    echo ""
    read -r -p "Proceed with broad read grants? [y/N]: " CONFIRM_BROAD
    CONFIRM_BROAD="${CONFIRM_BROAD:-N}"
    if [[ ! "$CONFIRM_BROAD" =~ ^[Yy]$ ]]; then
      echo "Skipped UC grants."
    else
      echo ""
      echo -e "${BOLD}Discovering catalogs and schemas...${NC}"
      # Best-effort: list catalogs + schemas. Requires metastore admin to see everything.
      CATALOGS_JSON="$("$DBX_BIN" --profile "$PROFILE" catalogs list --max-results 0 --include-browse --include-unbound --output json 2>/dev/null || echo "[]")"
      CATALOGS="$(python3 -c 'import sys, json
try:
  o = json.load(sys.stdin)
except Exception:
  o = []
if isinstance(o, dict):
  o = o.get("catalogs") or []
if not isinstance(o, list):
  o = []
for c in o:
  n = (c.get("name") or c.get("full_name") or "").strip()
  t = (c.get("catalog_type") or "").upper()
  if not n:
    continue
  # Skip system catalogs that are usually not grant-manageable.
  if t in ("SYSTEM_CATALOG",):
    continue
  if n.lower() in ("system",):
    continue
  print(n)
' <<<"$CATALOGS_JSON")"

      if [ -z "${CATALOGS:-}" ]; then
        echo -e "${YELLOW}  ! No catalogs found or insufficient privileges to list catalogs.${NC}"
        echo -e "${YELLOW}  Tip: run as a metastore admin, or use mode 1 and list schema(s) explicitly.${NC}"
      else
        echo ""
        echo -e "${BOLD}Applying broad UC grants...${NC}"
        while IFS= read -r catalog; do
          [ -z "${catalog:-}" ] && continue
          echo -e "${CYAN}→ Catalog: ${catalog}${NC}"
          if ! apply_catalog_grant "$catalog"; then
            echo -e "${YELLOW}  ! Could not grant USE_CATALOG on ${catalog}. Continuing...${NC}"
            continue
          fi

          SCHEMAS_JSON="$("$DBX_BIN" --profile "$PROFILE" schemas list "$catalog" --max-results 0 --include-browse --output json 2>/dev/null || echo "[]")"
          SCHEMAS="$(python3 -c 'import sys, json
cat = sys.argv[1]
try:
  o = json.load(sys.stdin)
except Exception:
  o = []
if isinstance(o, dict):
  o = o.get("schemas") or []
if not isinstance(o, list):
  o = []
for s in o:
  n = (s.get("name") or "").strip()
  if not n:
    continue
  # Skip system-like schemas.
  if n.lower() in ("information_schema",):
    continue
  print(f"{cat}.{n}")
' "$catalog" <<<"$SCHEMAS_JSON")"

          if [ -z "${SCHEMAS:-}" ]; then
            echo -e "${YELLOW}  ! No schemas found in ${catalog} (or insufficient privilege to list).${NC}"
            continue
          fi

          while IFS= read -r full_schema; do
            [ -z "${full_schema:-}" ] && continue
            if ! apply_schema_grant "$full_schema"; then
              echo -e "${YELLOW}  ! Could not grant USE_SCHEMA/SELECT on ${full_schema}. Continuing...${NC}"
            fi
          done <<<"$SCHEMAS"
        done <<<"$CATALOGS"

        echo ""
        echo -e "${GREEN}✓ Broad UC grants completed.${NC}"
        echo "If some catalogs/schemas were skipped, it usually means your identity isn't allowed to manage grants there."
      fi
    fi
  else
    echo ""
    echo -e "${BOLD}Which UC schema(s) should GenieIQ be able to read?${NC}"
    echo "Enter one per line in the form: catalog.schema"
    echo "Example:"
    echo "  cicaktest_catalog.default"
    echo ""
    echo "Press Enter on a blank line to finish."

    SCHEMAS=()
    fail_log="$(mktemp -t genieiq-perm-fail.XXXXXX)"
    while true; do
      read -r -p "schema> " s
      s="$(echo "${s:-}" | tr -d '\"' | xargs || true)"
      if [ -z "${s:-}" ]; then
        break
      fi
      if ! echo "$s" | grep -qE '^[^.]+\.[^.]+$'; then
        echo -e "${YELLOW}  ! Invalid format. Expected catalog.schema${NC}"
        continue
      fi
      SCHEMAS+=("$s")
    done

    if [ "${#SCHEMAS[@]}" -eq 0 ]; then
      echo -e "${YELLOW}No schemas provided. Skipping UC grants.${NC}"
    else
      echo ""
      echo -e "${BOLD}Applying UC grants (least privilege)${NC}"
      echo "This will grant:"
      echo "  - USE_CATALOG on each catalog"
      echo "  - USE_SCHEMA + SELECT on each schema (inherits to all tables)"
      echo ""

      for full in "${SCHEMAS[@]}"; do
        CATALOG="${full%%.*}"
        SCHEMA="${full#*.}"

        echo -e "${CYAN}→ ${CATALOG}.${SCHEMA}${NC}"

        if ! apply_catalog_grant "$CATALOG"; then
          echo -e "${YELLOW}  ! Could not grant USE_CATALOG on ${CATALOG}. Continuing...${NC}"
        fi

        if ! apply_schema_grant "${CATALOG}.${SCHEMA}"; then
          echo -e "${YELLOW}  ! Could not grant USE_SCHEMA/SELECT on ${CATALOG}.${SCHEMA}. Continuing...${NC}"
        fi
      done

      echo ""
      echo -e "${GREEN}✓ Done.${NC}"
      echo "If a space references tables outside these schema(s), you may still see table-access errors until you grant access there too."
    fi
  fi
fi

# ----------------------------------------------------------------------------
# Optional: Grant Genie space edit access to the GenieIQ App service principal
# ----------------------------------------------------------------------------
echo ""
echo -e "${BOLD}Optional - Genie space permissions for the GenieIQ app${NC}"
echo "GenieIQ may call APIs that require the app service principal to have edit access to Genie spaces."
echo "If you want the app to be able to read serialized settings and operate across spaces without manual sharing,"
echo "you can grant the app service principal a permission level on Genie spaces."
echo ""
read -r -p "Grant Genie space permissions to the GenieIQ app now? [y/N]: " DO_GENIE_PERMS
DO_GENIE_PERMS="${DO_GENIE_PERMS:-N}"

if [[ "$DO_GENIE_PERMS" =~ ^[Yy]$ ]]; then
  DBX_BIN="/opt/homebrew/bin/databricks"
  if [ ! -x "$DBX_BIN" ]; then
    if command -v databricks &> /dev/null; then
      DBX_BIN="$(command -v databricks)"
    else
      echo -e "${RED}✗ Databricks CLI not found. Install it first:${NC}"
      echo "  brew install databricks/tap/databricks"
      exit 1
    fi
  fi

  PROFILE="${DATABRICKS_CONFIG_PROFILE:-genieiq}"
  APP_NAME="${GENIEIQ_APP_NAME:-genieiq}"

  APP_SP_ID="$("$DBX_BIN" --profile "$PROFILE" apps get "$APP_NAME" --output json 2>/dev/null | python3 -c 'import sys, json
try:
  o = json.load(sys.stdin)
except Exception:
  o = {}
print(o.get("service_principal_client_id") or "")
' || true)"
  if [ -z "${APP_SP_ID:-}" ]; then
    echo -e "${RED}✗ Could not detect the app service principal client id.${NC}"
    echo "Make sure the app exists in this workspace and your CLI profile is authenticated."
    exit 1
  fi

  echo ""
  echo -e "${BOLD}Choose permission level to grant the app on Genie spaces:${NC}"
  echo "  - CAN_EDIT  (recommended): can edit spaces"
  echo "  - CAN_MANAGE (very permissive): can edit + change permissions + view other users' conversations"
  echo ""
  read -r -p "Permission level [CAN_EDIT/CAN_MANAGE] (default CAN_EDIT): " GENIE_LEVEL
  GENIE_LEVEL="${GENIE_LEVEL:-CAN_EDIT}"
  if [[ "$GENIE_LEVEL" != "CAN_EDIT" && "$GENIE_LEVEL" != "CAN_MANAGE" ]]; then
    echo -e "${YELLOW}  ! Invalid choice. Using CAN_EDIT.${NC}"
    GENIE_LEVEL="CAN_EDIT"
  fi

  echo ""
  echo -e "${YELLOW}${BOLD}Warning:${NC} This modifies permissions on Genie spaces for service principal ${APP_SP_ID}."
  echo -e "${YELLOW}Proceed only if you want the GenieIQ app to have broad access in this workspace.${NC}"
  echo ""
  read -r -p "Proceed? [y/N]: " CONFIRM_GENIE
  CONFIRM_GENIE="${CONFIRM_GENIE:-N}"
  if [[ ! "$CONFIRM_GENIE" =~ ^[Yy]$ ]]; then
    echo "Skipped Genie permissions."
  else
    echo ""
    read -r -p "How many spaces to update? Enter a number or 'all' (default: all): " LIMIT_SPACES
    LIMIT_SPACES="${LIMIT_SPACES:-all}"
    if [[ "$LIMIT_SPACES" != "all" ]] && ! echo "$LIMIT_SPACES" | grep -qE '^[0-9]+$'; then
      echo -e "${YELLOW}  ! Invalid value. Using 'all'.${NC}"
      LIMIT_SPACES="all"
    fi

    echo ""
    echo -e "${BOLD}Updating Genie spaces...${NC}"
    echo "App principal: ${APP_SP_ID}"
    echo "Permission:    ${GENIE_LEVEL}"
    echo "Limit:         ${LIMIT_SPACES}"
    echo ""

    page_token=""
    updated=0
    failed=0
    fail_log=""
    if fail_log="$(mktemp -t genieiq-perm-fail.XXXXXX 2>/dev/null)"; then
      : > "$fail_log"
    else
      fail_log="/tmp/genieiq-perm-fail.$$"
      : > "$fail_log"
    fi

    while true; do
      qs="page_size=200"
      if [ -n "${page_token:-}" ]; then
        qs="${qs}&page_token=${page_token}"
      fi

      RESP="$("$DBX_BIN" --profile "$PROFILE" api get "/api/2.0/genie/spaces?${qs}" --output json 2>/dev/null || echo '{}')"

      IDS="$(python3 -c 'import sys, json
try:
  o = json.load(sys.stdin)
except Exception:
  o = {}
spaces = o.get("spaces") or o.get("rooms") or []
for s in spaces or []:
  sid = s.get("id") or s.get("space_id") or s.get("room_id")
  if sid:
    print(sid)
' <<<"$RESP")"

      NEXT="$(python3 -c 'import sys, json
try:
  o = json.load(sys.stdin)
except Exception:
  o = {}
print(o.get("next_page_token") or o.get("nextPageToken") or "")
' <<<"$RESP")"

      if [ -z "${IDS:-}" ]; then
        break
      fi

      while IFS= read -r sid; do
        [ -z "${sid:-}" ] && continue

        # Apply permission (idempotent).
        if update_out="$("$DBX_BIN" --profile "$PROFILE" permissions update genie "$sid" \
          --json "{\"access_control_list\":[{\"service_principal_name\":\"${APP_SP_ID}\",\"permission_level\":\"${GENIE_LEVEL}\"}]}" \
          --output json 2>&1)"; then
          updated=$((updated + 1))
        else
          failed=$((failed + 1))
          reason="unknown"
          if echo "$update_out" | grep -qi "PERMISSION_DENIED\|Permission denied\|403"; then
            reason="no_permission_to_manage_space"
          elif echo "$update_out" | grep -qi "NOT_FOUND\|not found\|404"; then
            reason="space_not_found"
          elif echo "$update_out" | grep -qi "rate limit\|429"; then
            reason="rate_limited"
          fi
          if [ -n "${fail_log:-}" ]; then
            echo "${reason}|${sid}|${update_out}" >> "$fail_log"
          fi
        fi

        if [ "$((updated + failed))" -eq 1 ] || [ $(( (updated + failed) % 50 )) -eq 0 ]; then
          echo "  progress: $((updated + failed)) updated (ok=${updated}, failed=${failed})"
        fi

        if [ "$LIMIT_SPACES" != "all" ] && [ "$((updated + failed))" -ge "$LIMIT_SPACES" ]; then
          NEXT=""
          break
        fi
      done <<<"$IDS"

      if [ -z "${NEXT:-}" ]; then
        break
      fi
      page_token="$NEXT"
    done

    echo ""
    echo -e "${GREEN}✓ Genie permissions update complete.${NC}"
    echo "Updated: ${updated}"
    echo "Failed:  ${failed}"
    if [ "$failed" -gt 0 ] && [ -n "${fail_log:-}" ] && [ -f "$fail_log" ]; then
      echo ""
      echo -e "${YELLOW}Top failure reasons:${NC}"
      awk -F'|' '{c[$1]++} END{for (r in c) printf("%s\t%s\n", c[r], r)}' "$fail_log" \
        | sort -nr | head -5 | awk '{print "  - " $2 " (" $1 ")"}'
      echo ""
      echo -e "${YELLOW}Sample errors:${NC}"
      awk -F'|' 'NR<=3 {print "  - " $1 ": " $3}' "$fail_log"
      echo ""
      echo "Tip: If most failures are permission-related, run this step as a workspace admin or a space owner."
      echo "You can also re-run with a smaller limit to validate permissions before scanning everything."
    fi
    if [ -n "${fail_log:-}" ]; then
      rm -f "$fail_log" >/dev/null 2>&1 || true
    fi
  fi
fi
