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
