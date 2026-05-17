#!/usr/bin/env bash
# Export Hive dashboard CSVs on an interval until Ctrl+C.
#
# Usage:
#   bash scripts/export-hive-dashboard-loop.sh
#
# Optional:
#   EXPORT_INTERVAL_SECONDS=120   default: 120

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"
wiki_pulse_platform_init

ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$ROOT"

INTERVAL="${EXPORT_INTERVAL_SECONDS:-120}"

echo "Exporting Hive -> CSV every ${INTERVAL}s (Ctrl+C to stop)."
while true; do
  echo ""
  echo "Exporting at $(date)"
  if ! bash scripts/export-hive-dashboard-data.sh; then
    echo "Export failed; retrying in ${INTERVAL}s..."
  fi
  sleep "$INTERVAL"
done
