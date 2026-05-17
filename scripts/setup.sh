#!/usr/bin/env bash
# One-time setup: HDFS repair, Kafka topic, static lookup, Hive tables.
#
# Usage:
#   bash scripts/setup.sh
#   SKIP_HDFS_REPAIR_CHECK=1 bash scripts/setup.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"
wiki_pulse_platform_init

ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$ROOT"

if ! docker inspect cs523bdt-lab >/dev/null 2>&1; then
  echo "ERROR: cs523bdt-lab container not found. Start the course Docker stack first."
  exit 1
fi

docker exec cs523bdt-lab bash -lc "hdfs dfsadmin -safemode leave" >/dev/null 2>&1 || true

if [[ "${SKIP_HDFS_REPAIR_CHECK:-0}" != "1" ]]; then
  bash scripts/repair-hdfs-state.sh --auto
fi

bash scripts/create-project-topic.sh
bash scripts/upload-static-wiki-lookup.sh
bash scripts/create-hive-tables.sh

echo "Setup complete."
