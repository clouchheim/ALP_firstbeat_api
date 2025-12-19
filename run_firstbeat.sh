#!/usr/bin/env bash
set -euo pipefail

echo "=== Starting Firstbeat pipeline ==="

# Ensure we are running from the repo root
REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$REPO_ROOT"

echo "Working directory: $(pwd)"

# -------------------------
# Run Python data pull
# -------------------------
echo "Running Firstbeat API Python script..."
python get_firstbeat_api.py

# -------------------------
# Optional: sanity check CSV
# -------------------------
if [[ ! -f firstbeat_data.csv ]]; then
  echo "ERROR: firstbeat_data.csv was not created"
  exit 1
fi

# -------------------------
# Run R processing + upload
# -------------------------
echo "Running R Smartabase upload script..."
Rscript firstbeat_api.R

echo "=== Firstbeat pipeline completed successfully ==="
