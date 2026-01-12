#!/bin/bash
set -euo pipefail

# Finder-friendly double-click script (macOS).
# Runs local setup from inside this repo folder.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$ROOT_DIR"

echo ""
echo "GenieIQ - Local Setup"
echo "Repo: $ROOT_DIR"
echo ""

chmod +x ./setup.sh 2>/dev/null || true
./setup.sh

echo ""
echo "Done. Press Enter to close."
read -r

