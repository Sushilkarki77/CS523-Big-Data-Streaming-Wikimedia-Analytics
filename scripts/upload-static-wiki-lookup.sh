#!/usr/bin/env bash
# Upload the static wiki metadata lookup CSV to HDFS for the Spark SQL bonus join.
#
# Usage:
#   bash scripts/upload-static-wiki-lookup.sh
#
# Optional environment variables:
#   STATIC_WIKI_LOOKUP_HDFS_PATH default: /tmp/wiki-pulse/static/wiki_project_lookup.csv

set -euo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SRC="${ROOT}/static-data/wiki_project_lookup.csv"
DEST="${STATIC_WIKI_LOOKUP_HDFS_PATH:-/tmp/wiki-pulse/static/wiki_project_lookup.csv}"
DEST_DIR="$(dirname "$DEST")"
CONTAINER_TMP="/tmp/wiki_project_lookup.csv"

if [[ ! -f "$SRC" ]]; then
  echo "ERROR: static lookup file not found: ${SRC}"
  exit 1
fi

if ! docker inspect cs523bdt-lab >/dev/null 2>&1; then
  echo "ERROR: cs523bdt-lab container not found. Start your Docker stack first."
  exit 1
fi

echo "Copying static lookup into cs523bdt-lab..."
docker exec -i cs523bdt-lab bash -lc "cat > '${CONTAINER_TMP}'" < "$SRC"

echo "Uploading static lookup to HDFS: ${DEST}"
docker exec cs523bdt-lab bash -lc "hdfs dfs -mkdir -p '${DEST_DIR}' && hdfs dfs -put -f '${CONTAINER_TMP}' '${DEST}' && hdfs dfs -ls '${DEST}'"

echo "OK — Spark can read: hdfs://localhost:9000${DEST}"
