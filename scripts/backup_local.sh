#!/bin/bash
set -euo pipefail

# Creates a timestamped local backup INSIDE ~/Documents/genieiq
# Default behavior backs up everything needed to reproduce + redeploy, while excluding heavy deps.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"
BACKUP_DIR="$ROOT_DIR/backups"

TS="$(date +%Y%m%d_%H%M%S)"
OUT="$BACKUP_DIR/genieiq_backup_${TS}.tar.gz"

mkdir -p "$BACKUP_DIR"

echo "Backing up: $ROOT_DIR"
echo "Output:     $OUT"
echo ""

# Exclusions:
# - node_modules: reproducible via npm install
# - deploy bundles: reproducible via ./deploy.sh, and may contain their own node_modules
tar \
  --exclude="./backups" \
  --exclude="./node_modules" \
  --exclude="./frontend/node_modules" \
  --exclude="./.tmp/deploy-bundles" \
  --exclude="./.tmp/deploy-upload" \
  -czf "$OUT" \
  -C "$ROOT_DIR" .

echo "Backup created:"
echo "  $OUT"
