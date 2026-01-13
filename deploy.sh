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

if ! databricks current-user me &> /dev/null; then
    echo -e "${YELLOW}  ! Not authenticated${NC}"
    read -p "  Enter your Databricks workspace URL: " WORKSPACE_URL
    WORKSPACE_URL="${WORKSPACE_URL%/}"
    databricks auth login --host "$WORKSPACE_URL"
fi

USER_EMAIL=$(databricks current-user me --output json 2>/dev/null | grep -o '"userName":"[^"]*"' | cut -d'"' -f4)
WORKSPACE_HOST=$(databricks auth describe --output json 2>/dev/null | grep -o '"host":"[^"]*"' | head -1 | cut -d'"' -f4)

echo -e "${GREEN}  ✓ Authenticated as ${USER_EMAIL}${NC}"
echo -e "${GREEN}  ✓ Workspace: ${WORKSPACE_HOST}${NC}"

# ============================================================================
# STEP 3: Setup Lakebase Instance (optional)
# ============================================================================
echo ""
echo -e "${BOLD}[3/7] Setting up Lakebase database (optional)...${NC}"

LAKEBASE_HOST=""
LAKEBASE_STATE=""
INSTANCE_JSON=""

if [ "$SKIP_LAKEBASE" = "1" ]; then
    echo -e "${YELLOW}  ! SKIP_LAKEBASE=1 set - deploying without Lakebase (in-memory storage).${NC}"
else
    ERR_FILE="$(mktemp)"
    if ! INSTANCE_JSON=$(databricks database get-database-instance "$LAKEBASE_INSTANCE" --output json 2>"$ERR_FILE"); then
        ERR_MSG="$(cat "$ERR_FILE" || true)"
        # If the instance simply doesn't exist, we'll create it. Otherwise, skip Lakebase.
        if echo "$ERR_MSG" | grep -qiE "does not exist|RESOURCE_DOES_NOT_EXIST|NOT_FOUND"; then
            INSTANCE_JSON=""
        else
            echo -e "${YELLOW}  ! Lakebase not available in this workspace or you lack permissions.${NC}"
            echo -e "${YELLOW}    Proceeding without Lakebase (in-memory storage).${NC}"
            SKIP_LAKEBASE="1"
        fi
    fi
    rm -f "$ERR_FILE" 2>/dev/null || true

    if [ "$SKIP_LAKEBASE" != "1" ] && [ -z "$INSTANCE_JSON" ]; then
        echo "  Creating Lakebase instance '$LAKEBASE_INSTANCE'..."
        if ! databricks database create-database-instance "$LAKEBASE_INSTANCE" \
            --capacity CU_1 \
            --enable-pg-native-login \
            --no-wait; then
            echo -e "${YELLOW}  ! Could not create Lakebase instance. Proceeding without Lakebase (in-memory storage).${NC}"
            SKIP_LAKEBASE="1"
        else
            echo "  Waiting for instance to start..."
            for i in {1..60}; do
                STATE=$(databricks database get-database-instance "$LAKEBASE_INSTANCE" --output json 2>/dev/null | grep -o '"state":"[^"]*"' | cut -d'"' -f4)
                if [ "$STATE" = "AVAILABLE" ]; then
                    break
                fi
                sleep 5
            done
            INSTANCE_JSON=$(databricks database get-database-instance "$LAKEBASE_INSTANCE" --output json 2>/dev/null || echo "")
        fi
    fi

    if [ "$SKIP_LAKEBASE" != "1" ] && [ -n "$INSTANCE_JSON" ]; then
        LAKEBASE_HOST=$(echo "$INSTANCE_JSON" | grep -o '"read_write_dns":"[^"]*"' | cut -d'"' -f4)
        LAKEBASE_STATE=$(echo "$INSTANCE_JSON" | grep -o '"state":"[^"]*"' | cut -d'"' -f4)

        if [ "$LAKEBASE_STATE" != "AVAILABLE" ]; then
            echo -e "${YELLOW}  ! Lakebase is $LAKEBASE_STATE, waiting...${NC}"
            for i in {1..60}; do
                STATE=$(databricks database get-database-instance "$LAKEBASE_INSTANCE" --output json 2>/dev/null | grep -o '"state":"[^"]*"' | cut -d'"' -f4)
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
cp app.yaml "$DEPLOY_DIR/"

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
databricks workspace import-dir "$DEPLOY_DIR" "$WORKSPACE_DEPLOY_PATH" --overwrite

echo -e "${GREEN}  ✓ Files uploaded${NC}"

# Deploy/Update the app with environment variables
echo "  Deploying app with Lakebase configuration..."

# Build env args (Lakebase is optional)
ENV_ARGS=(--env "NODE_ENV=production")
if [ -n "${LAKEBASE_HOST}" ]; then
    ENV_ARGS+=(--env "LAKEBASE_HOST=$LAKEBASE_HOST")
    # Lakebase provisioned Postgres default database name is typically `databricks_postgres`.
    ENV_ARGS+=(--env "LAKEBASE_DATABASE=databricks_postgres")
    ENV_ARGS+=(--env "LAKEBASE_INSTANCE=$LAKEBASE_INSTANCE")
    ENV_ARGS+=(--env "DATABRICKS_USER=$USER_EMAIL")
fi

# Check if app exists
if databricks apps get "$APP_NAME" &> /dev/null; then
    # Update existing app
    databricks apps deploy "$APP_NAME" \
        --source-code-path "$WORKSPACE_DEPLOY_PATH" \
        "${ENV_ARGS[@]}"
else
    # Create new app
    databricks apps create "$APP_NAME" \
        --description "GenieIQ - For Better Answers" 
    
    databricks apps deploy "$APP_NAME" \
        --source-code-path "$WORKSPACE_DEPLOY_PATH" \
        "${ENV_ARGS[@]}"
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
