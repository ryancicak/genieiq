#!/bin/bash

# GenieIQ Deploy Script
# FULLY AUTOMATED deployment to Databricks Apps with Lakebase

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Config
APP_NAME="genieiq"
LAKEBASE_INSTANCE="genieiq-db"
WORKSPACE_PATH="/Workspace/Users/\${USER_EMAIL}/genieiq-deploy"
SKIP_LAKEBASE="${SKIP_LAKEBASE:-0}"

# Prefer the real Databricks CLI binary to avoid the wrapper output that breaks JSON parsing.
DBX_BIN="$(command -v databricks || true)"
if [ -x "/opt/homebrew/bin/databricks" ]; then
  DBX_BIN="/opt/homebrew/bin/databricks"
fi

# Use a stable profile by default so auth + subsequent commands are consistent.
# setup.sh can set DATABRICKS_CONFIG_PROFILE, otherwise we use "genieiq".
PROFILE="${DATABRICKS_CONFIG_PROFILE:-genieiq}"

dbx() {
  "$DBX_BIN" --profile "$PROFILE" "$@"
}

json_value() {
  # Reads mixed output from stdin, extracts the first JSON object, then prints a field.
  # Usage: json_value '<python_expr_using_obj>'
  # Example: json_value 'obj.get("userName")'
  #
  # IMPORTANT: This function must read JSON from stdin. Do NOT use `python3 -` with a heredoc,
  # because that consumes stdin for the program source and leaves no stdin for the JSON payload.
  python3 -c 'import sys, json, re
expr = sys.argv[1] if len(sys.argv) > 1 else ""
raw = sys.stdin.read()
m = re.search(r"(\{[\s\S]*\})", raw)
if not m:
  sys.exit(0)
obj = json.loads(m.group(1))
val = eval(expr, {"obj": obj})
if val is None:
  sys.exit(0)
if isinstance(val, (list, dict)):
  print(json.dumps(val))
else:
  print(str(val))' "$@"
}

# Script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo ""
echo -e "${CYAN}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║${NC}   ${BOLD}GenieIQ - Fully Automated Deployment${NC}                 ${CYAN}║${NC}"
echo -e "${CYAN}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""

# ============================================================================
# STEP 1: Check Dependencies
# ============================================================================
echo -e "${BOLD}[1/7] Checking dependencies...${NC}"

if ! command -v databricks &> /dev/null; then
    echo -e "${RED}✗ Databricks CLI not found${NC}"
    echo ""
    echo "Install it with:"
    echo "  brew install databricks/tap/databricks"
    echo ""
    exit 1
fi

CLI_VERSION=$(databricks --version 2>&1 | head -1)
echo -e "${GREEN}  ✓ Databricks CLI ${CLI_VERSION}${NC}"

# Check for database command (required for Lakebase)
if ! databricks database --help &> /dev/null; then
    echo -e "${YELLOW}  ! CLI version too old for Lakebase. Upgrading...${NC}"
    brew upgrade databricks/tap/databricks
fi

if ! command -v node &> /dev/null; then
    echo -e "${RED}✗ Node.js not found${NC}"
    exit 1
fi
echo -e "${GREEN}  ✓ Node.js $(node --version)${NC}"

# ============================================================================
# STEP 2: Databricks Authentication
# ============================================================================
echo ""
echo -e "${BOLD}[2/7] Checking Databricks authentication...${NC}"

DESIRED_HOST="${TARGET_DATABRICKS_HOST:-}"
DESIRED_HOST="${DESIRED_HOST%/}"

CURRENT_HOST="$(dbx auth describe --output json 2>/dev/null | json_value 'obj.get("details", {}).get("host") or obj.get("details", {}).get("configuration", {}).get("host", {}).get("value")')"
CURRENT_HOST="${CURRENT_HOST%/}"

if [ -n "${DESIRED_HOST}" ] && [ -n "${CURRENT_HOST}" ] && [ "${CURRENT_HOST}" != "${DESIRED_HOST}" ]; then
    echo -e "${YELLOW}  ! Databricks CLI is authenticated to a different workspace:${NC}"
    echo -e "${YELLOW}    current: ${CURRENT_HOST}${NC}"
    echo -e "${YELLOW}    desired: ${DESIRED_HOST}${NC}"
    echo "  Re-authenticating to the desired workspace..."
    "$DBX_BIN" auth login --host "${DESIRED_HOST}" --profile "${PROFILE}"
fi

if ! dbx current-user me &> /dev/null; then
    echo -e "${YELLOW}  ! Not authenticated${NC}"
    WORKSPACE_URL="${DESIRED_HOST}"
    if [ -z "${WORKSPACE_URL}" ]; then
        read -p "  Enter your Databricks workspace URL: " WORKSPACE_URL
        WORKSPACE_URL="${WORKSPACE_URL%/}"
    fi
    "$DBX_BIN" auth login --host "$WORKSPACE_URL" --profile "${PROFILE}"

    # Validate login really completed (the CLI may create/update a profile even if the browser flow wasn't finished).
    echo "  Verifying authentication..."
    AUTH_OK=0
    for i in {1..15}; do
        if dbx current-user me &> /dev/null; then
            AUTH_OK=1
            break
        fi
        sleep 2
    done
    if [ "$AUTH_OK" != "1" ]; then
        echo -e "${RED}✗ Authentication was not completed.${NC}"
        echo "  Please finish the browser login flow, then verify with:"
        echo "    $DBX_BIN --profile \"$PROFILE\" current-user me"
        echo ""
        echo "  Then rerun:"
        echo "    TARGET_DATABRICKS_HOST=\"$WORKSPACE_URL\" DATABRICKS_CONFIG_PROFILE=\"$PROFILE\" ./deploy.sh"
        exit 1
    fi
fi

USER_EMAIL="$(dbx current-user me --output json 2>/dev/null | json_value 'obj.get("userName")')"
WORKSPACE_HOST="$(dbx auth describe --output json 2>/dev/null | json_value 'obj.get("details", {}).get("host") or obj.get("details", {}).get("configuration", {}).get("host", {}).get("value")')"

if [ -z "${USER_EMAIL}" ] || [ -z "${WORKSPACE_HOST}" ]; then
    echo -e "${RED}✗ Could not determine your user email or workspace host from the Databricks CLI.${NC}"
    echo -e "${RED}  USER_EMAIL='${USER_EMAIL}' WORKSPACE_HOST='${WORKSPACE_HOST}'${NC}"
    echo ""
    echo "Fix:"
    echo "  1) Ensure you can run: $DBX_BIN --profile \"$PROFILE\" current-user me"
    echo "  2) Then rerun: ./deploy.sh"
    exit 1
fi

echo -e "${GREEN}  ✓ Authenticated as ${USER_EMAIL}${NC}"
echo -e "${GREEN}  ✓ Workspace: ${WORKSPACE_HOST}${NC}"

# ============================================================================
# STEP 3: Setup Lakebase Instance (optional)
# ============================================================================
echo ""
echo -e "${BOLD}[3/7] Setting up Lakebase database (optional)...${NC}"

LAKEBASE_HOST=""
LAKEBASE_DATABASE_NAME=""
LAKEBASE_STATE=""
INSTANCE_JSON=""

if [ "$SKIP_LAKEBASE" = "1" ]; then
    echo -e "${YELLOW}  ! SKIP_LAKEBASE=1 set - deploying without Lakebase (in-memory storage).${NC}"
else
    # If the caller already knows the Lakebase connection details (common when instance quota is hit),
    # use them directly and skip provisioning via the Databricks database APIs.
    if [ -n "${EXISTING_LAKEBASE_HOST:-}" ]; then
        LAKEBASE_HOST="${EXISTING_LAKEBASE_HOST}"
        LAKEBASE_DATABASE_NAME="${EXISTING_LAKEBASE_DATABASE:-databricks_postgres}"
        echo -e "${GREEN}  ✓ Using existing Lakebase connection (skipping provisioning)${NC}"
        echo -e "${GREEN}  ✓ Host: ${LAKEBASE_HOST}${NC}"
        echo -e "${GREEN}  ✓ Database: ${LAKEBASE_DATABASE_NAME}${NC}"
    else
    ERR_FILE="$(mktemp)"
    if ! INSTANCE_JSON=$(dbx database get-database-instance "$LAKEBASE_INSTANCE" --output json 2>"$ERR_FILE"); then
        ERR_MSG="$(cat "$ERR_FILE" || true)"
        # If the instance simply doesn't exist, we'll create it. Otherwise, skip Lakebase.
        if echo "$ERR_MSG" | grep -qiE "does not exist|RESOURCE_DOES_NOT_EXIST|NOT_FOUND"; then
            INSTANCE_JSON=""
        else
            echo -e "${YELLOW}  ! Could not manage Lakebase instances via the Databricks CLI in this workspace.${NC}"
            echo -e "${YELLOW}    This does NOT necessarily mean your Lakebase database doesn't exist.${NC}"
            echo -e "${YELLOW}    If you already have a Lakebase host/dbname, rerun with:${NC}"
            echo -e "${YELLOW}      EXISTING_LAKEBASE_HOST=... EXISTING_LAKEBASE_DATABASE=... ./deploy.sh${NC}"
            echo -e "${YELLOW}    Proceeding without Lakebase (in-memory storage).${NC}"
            SKIP_LAKEBASE="1"
        fi
    fi
    rm -f "$ERR_FILE" 2>/dev/null || true

    if [ "$SKIP_LAKEBASE" != "1" ] && [ -z "$INSTANCE_JSON" ]; then
        echo "  Creating Lakebase instance '$LAKEBASE_INSTANCE'..."
        if ! dbx database create-database-instance "$LAKEBASE_INSTANCE" \
            --capacity CU_1 \
            --enable-pg-native-login \
            --no-wait; then
            echo -e "${YELLOW}  ! Could not create Lakebase instance. Proceeding without Lakebase (in-memory storage).${NC}"
            SKIP_LAKEBASE="1"
        else
            echo "  Waiting for instance to start..."
            for i in {1..60}; do
                STATE=$(dbx database get-database-instance "$LAKEBASE_INSTANCE" --output json 2>/dev/null | json_value 'obj.get("state")')
                if [ "$STATE" = "AVAILABLE" ]; then
                    break
                fi
                sleep 5
            done
            INSTANCE_JSON=$(dbx database get-database-instance "$LAKEBASE_INSTANCE" --output json 2>/dev/null || echo "")
        fi
    fi

    if [ "$SKIP_LAKEBASE" != "1" ] && [ -n "$INSTANCE_JSON" ]; then
        LAKEBASE_HOST=$(echo "$INSTANCE_JSON" | grep -o '"read_write_dns":"[^"]*"' | cut -d'"' -f4)
        LAKEBASE_STATE=$(echo "$INSTANCE_JSON" | grep -o '"state":"[^"]*"' | cut -d'"' -f4)
        LAKEBASE_DATABASE_NAME="databricks_postgres"

        if [ "$LAKEBASE_STATE" != "AVAILABLE" ]; then
            echo -e "${YELLOW}  ! Lakebase is $LAKEBASE_STATE, waiting...${NC}"
            for i in {1..60}; do
                STATE=$(dbx database get-database-instance "$LAKEBASE_INSTANCE" --output json 2>/dev/null | json_value 'obj.get("state")')
                if [ "$STATE" = "AVAILABLE" ]; then
                    break
                fi
                sleep 5
            done
        fi

        echo -e "${GREEN}  ✓ Lakebase instance: $LAKEBASE_INSTANCE${NC}"
        echo -e "${GREEN}  ✓ Host: $LAKEBASE_HOST${NC}"
    fi
    fi
fi

# ============================================================================
# STEP 4: Install Dependencies & Build
# ============================================================================
echo ""
echo -e "${BOLD}[4/7] Installing dependencies...${NC}"

npm install --silent 2>/dev/null || npm install
echo -e "${GREEN}  ✓ Backend dependencies${NC}"

cd frontend
npm install --silent 2>/dev/null || npm install
echo -e "${GREEN}  ✓ Frontend dependencies${NC}"

echo ""
echo -e "${BOLD}[5/7] Building frontend...${NC}"
npm run build
cd ..
echo -e "${GREEN}  ✓ Frontend built${NC}"

# ============================================================================
# STEP 5: Bundle for Deployment
# ============================================================================
echo ""
echo -e "${BOLD}[6/7] Preparing deployment package...${NC}"

# Create a clean deployment directory INSIDE the repo so everything stays under ~/Documents/genieiq
DEPLOY_VERSION="v$(date +%Y%m%d%H%M%S)"
DEPLOY_ROOT="$SCRIPT_DIR/.tmp/deploy-bundles"
DEPLOY_DIR="$DEPLOY_ROOT/$DEPLOY_VERSION"
mkdir -p "$DEPLOY_DIR"

# Copy only what's needed
cp -r backend "$DEPLOY_DIR/"
cp -r frontend/dist "$DEPLOY_DIR/frontend/"
mkdir -p "$DEPLOY_DIR/frontend/dist"
# Use a dedicated deploy package manifest that won't trigger frontend builds in Databricks Apps
cp package-deploy.json "$DEPLOY_DIR/package.json"
cp package-lock.json "$DEPLOY_DIR/" 2>/dev/null || true
# Generate app.yaml in the deploy bundle so env vars are baked in (Databricks CLI `apps deploy` does not accept --env).
cat > "$DEPLOY_DIR/app.yaml" <<YAML
command:
  - "node"
  - "backend/server.js"

env:
  - name: NODE_ENV
    value: production
  - name: ADMIN_EMAILS
    value: "${USER_EMAIL}"
YAML

if [ -n "${LAKEBASE_HOST}" ]; then
  cat >> "$DEPLOY_DIR/app.yaml" <<YAML
  - name: LAKEBASE_HOST
    value: "${LAKEBASE_HOST}"
  - name: LAKEBASE_DATABASE
    value: "${LAKEBASE_DATABASE_NAME:-databricks_postgres}"
  - name: LAKEBASE_INSTANCE
    value: "${LAKEBASE_INSTANCE}"
  - name: DATABRICKS_USER
    value: "${USER_EMAIL}"
YAML
fi

# Install production dependencies in deploy dir
cd "$DEPLOY_DIR"
npm install --omit=dev --silent 2>/dev/null || npm install --omit=dev
cd "$SCRIPT_DIR"

echo -e "${GREEN}  ✓ Deployment package ready${NC}"
echo -e "${GREEN}  ✓ Saved locally at: ${DEPLOY_DIR}${NC}"

# ============================================================================
# STEP 6: Upload to Workspace
# ============================================================================
echo ""
echo -e "${BOLD}[7/7] Deploying to Databricks...${NC}"

# Construct workspace path
WORKSPACE_DEPLOY_PATH="/Workspace/Users/${USER_EMAIL}/genieiq-${DEPLOY_VERSION}"

echo "  Uploading to ${WORKSPACE_DEPLOY_PATH}..."

# Upload the deployment directory
dbx workspace import-dir "$DEPLOY_DIR" "$WORKSPACE_DEPLOY_PATH" --overwrite

echo -e "${GREEN}  ✓ Files uploaded${NC}"

# Deploy/Update the app (env is defined in app.yaml in the uploaded source)
echo "  Deploying app..."

# Check if app exists
if dbx apps get "$APP_NAME" &> /dev/null; then
    # Update existing app
    dbx apps deploy "$APP_NAME" \
        --source-code-path "$WORKSPACE_DEPLOY_PATH"
else
    # Create new app
    dbx apps create "$APP_NAME" \
        --description "GenieIQ - For Better Answers"
    
    dbx apps deploy "$APP_NAME" \
        --source-code-path "$WORKSPACE_DEPLOY_PATH"
fi

# Keep the deploy bundle on disk (under .tmp/) so your local folder is fully self-contained.
# Set CLEANUP_DEPLOY_BUNDLE=1 if you want to delete it after a successful deploy.
if [ "${CLEANUP_DEPLOY_BUNDLE}" = "1" ]; then
    rm -rf "$DEPLOY_DIR"
fi

echo -e "${GREEN}  ✓ App deployed${NC}"

# Get app URL
APP_INFO=$(databricks apps get "$APP_NAME" --output json 2>/dev/null)
APP_URL=$(echo "$APP_INFO" | grep -o '"url":"[^"]*"' | head -1 | cut -d'"' -f4)

if [ -z "$APP_URL" ]; then
    APP_URL="${WORKSPACE_HOST}/apps/${APP_NAME}"
fi

echo ""
echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  ${GREEN}${BOLD}🎉 GenieIQ is live!${NC}"
echo ""
echo -e "  ${BOLD}App URL:${NC}      ${APP_URL}"
echo -e "  ${BOLD}Lakebase:${NC}     ${LAKEBASE_INSTANCE}"
echo -e "  ${BOLD}Database:${NC}     PostgreSQL 16 @ ${LAKEBASE_HOST}"
echo ""
echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Offer to open in browser
read -p "Open in browser? [Y/n] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]] || [[ -z $REPLY ]]; then
    if command -v open &> /dev/null; then
        open "$APP_URL"
    elif command -v xdg-open &> /dev/null; then
        xdg-open "$APP_URL"
    fi
fi
