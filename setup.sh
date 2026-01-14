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

  APP_SP_ID="$("$DBX_BIN" --profile "$PROFILE" apps get "$APP_NAME" --output json 2>/dev/null | python3 -c 'import sys,json\ntry:\n  o=json.load(sys.stdin)\nexcept Exception:\n  o={}\nprint(o.get(\"service_principal_client_id\") or \"\")' || true)"
  if [ -z "${APP_SP_ID:-}" ]; then
    echo -e "${RED}✗ Could not detect the app service principal client id.${NC}"
    echo "Make sure the app exists in this workspace and your CLI profile is authenticated."
    echo "Tip: set GENIEIQ_APP_NAME=<name> and/or DATABRICKS_CONFIG_PROFILE=<profile> and rerun."
    exit 1
  fi

  echo -e "${GREEN}✓ App service principal client id: ${APP_SP_ID}${NC}"
  echo ""
  echo -e "${BOLD}Which UC schema(s) should GenieIQ be able to read?${NC}"
  echo "Enter one per line in the form: catalog.schema"
  echo "Example:"
  echo "  cicaktest_catalog.default"
  echo ""
  echo "Press Enter on a blank line to finish."

  SCHEMAS=()
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

      # 1) Catalog: USE_CATALOG
      "$DBX_BIN" --profile "$PROFILE" grants update CATALOG "$CATALOG" \
        --json "{\"changes\":[{\"principal\":\"${APP_SP_ID}\",\"add\":[\"USE_CATALOG\"]}]}" \
        --output json >/dev/null 2>&1 || {
          echo -e "${YELLOW}  ! Could not grant USE_CATALOG on ${CATALOG}. Continuing...${NC}"
        }

      # 2) Schema: USE_SCHEMA + SELECT (inherited to tables)
      "$DBX_BIN" --profile "$PROFILE" grants update SCHEMA "${CATALOG}.${SCHEMA}" \
        --json "{\"changes\":[{\"principal\":\"${APP_SP_ID}\",\"add\":[\"USE_SCHEMA\",\"SELECT\"]}]}" \
        --output json >/dev/null 2>&1 || {
          echo -e "${YELLOW}  ! Could not grant USE_SCHEMA/SELECT on ${CATALOG}.${SCHEMA}. Continuing...${NC}"
        }
    done

    echo ""
    echo -e "${GREEN}✓ Done.${NC}"
    echo "If a space references tables outside these schema(s), you may still see table-access errors until you grant access there too."
  fi
fi
