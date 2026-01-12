#!/bin/bash
set -euo pipefail

# Finder-friendly double-click script (macOS).
# Runs the fully automated deploy from inside this repo folder.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$ROOT_DIR"

echo ""
echo "GenieIQ - Deploy to Databricks Apps"
echo "Repo: $ROOT_DIR"
echo ""
echo "Tip: if you use multiple Databricks CLI profiles, set it first in this Terminal:"
echo "  export DATABRICKS_CONFIG_PROFILE=your-profile"
echo ""
echo "Tip: if the target workspace doesn’t have Lakebase enabled, you can deploy without it:"
echo "  export SKIP_LAKEBASE=1"
echo ""

./deploy.sh

echo ""
echo "Done. Press Enter to close."
read -r

