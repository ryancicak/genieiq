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
prompt "Lakebase database name" "LAKEBASE_DATABASE" "databricks_postgres"
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
echo "Next:"
echo "  1) Install deps:   npm run install:all"
echo "  2) Run locally:    npm run dev"
echo ""
echo "Tip: If Lakebase auth fails, your LAKEBASE_TOKEN likely expired. Paste a fresh one and retry."
echo ""
