#!/bin/bash
set -euo pipefail

# Finder-friendly double-click script (macOS).
# Creates a local timestamped backup INSIDE this repo folder.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$ROOT_DIR"

echo ""
echo "GenieIQ - Local Backup"
echo "Repo: $ROOT_DIR"
echo ""

./scripts/backup_local.sh

echo ""
echo "Done. Press Enter to close."
read -r

