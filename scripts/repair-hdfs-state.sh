#!/usr/bin/env bash
# Repair generated HDFS state when local Hadoop has stale or missing blocks.
# Only touches wiki-pulse checkpoints and Hive warehouse output.
#
# Usage:
#   bash scripts/repair-hdfs-state.sh --check
#   bash scripts/repair-hdfs-state.sh --auto
#   bash scripts/repair-hdfs-state.sh --reset

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"
wiki_pulse_platform_init

MODE="${1:---check}"
HIVE_DATABASE="${HIVE_DATABASE:-wiki_pulse}"
CHECKPOINT_DIR="${SPARK_CHECKPOINT_DIR:-hdfs://localhost:9000/tmp/wiki-pulse/checkpoints/hive}"
WAREHOUSE_DIR="${HIVE_WAREHOUSE_DIR:-hdfs://localhost:9000/user/hive/warehouse/${HIVE_DATABASE}.db}"

case "$MODE" in
  --check|--auto|--reset) ;;
  *)
    echo "Usage: bash scripts/repair-hdfs-state.sh [--check|--auto|--reset]"
    exit 2
    ;;
esac

if ! docker inspect cs523bdt-lab >/dev/null 2>&1; then
  echo "ERROR: cs523bdt-lab container not found. Start your Docker stack first."
  exit 1
fi

hdfs_path_without_scheme() {
  local path="$1"
  path="${path#hdfs://localhost:9000}"
  path="${path#hdfs://127.0.0.1:9000}"
  echo "$path"
}

CHECKPOINT_PATH="$(hdfs_path_without_scheme "$CHECKPOINT_DIR")"
WAREHOUSE_PATH="$(hdfs_path_without_scheme "$WAREHOUSE_DIR")"

docker exec cs523bdt-lab bash -lc "hdfs dfsadmin -safemode leave" >/dev/null 2>&1 || true

path_has_missing_blocks() {
  local path="$1"
  local output

  if ! docker exec cs523bdt-lab bash -lc "hdfs dfs -test -e '$path'" >/dev/null 2>&1; then
    return 1
  fi

  output="$(docker exec cs523bdt-lab bash -lc "hdfs fsck '$path' -files -blocks" 2>&1 || true)"
  if [[ "$output" == *"CORRUPT"* || "$output" == *"MISSING"* || "$output" == *"BlockMissingException"* || "$output" == *"Could not obtain block"* ]]; then
    echo "$output"
    return 0
  fi

  return 1
}

reset_pipeline_state() {
  echo "Resetting wiki-pulse HDFS/Hive state..."
  docker exec cs523bdt-lab bash -lc "hive -e 'DROP DATABASE IF EXISTS ${HIVE_DATABASE} CASCADE;'" >/dev/null 2>&1 || true
  docker exec cs523bdt-lab bash -lc "hdfs dfs -rm -r -skipTrash '$CHECKPOINT_PATH' '$WAREHOUSE_PATH'" >/dev/null 2>&1 || true
  echo "Reset complete. Run bash scripts/setup.sh before restarting Spark."
}

if [[ "$MODE" == "--reset" ]]; then
  reset_pipeline_state
  exit 0
fi

needs_reset=0

echo "Checking generated HDFS state for missing blocks..."
if path_has_missing_blocks "$CHECKPOINT_PATH"; then
  echo "Detected missing/corrupt blocks in Spark checkpoint path: $CHECKPOINT_PATH"
  needs_reset=1
fi

if path_has_missing_blocks "$WAREHOUSE_PATH"; then
  echo "Detected missing/corrupt blocks in Hive warehouse path: $WAREHOUSE_PATH"
  needs_reset=1
fi

if [[ "$needs_reset" -eq 0 ]]; then
  echo "Generated HDFS state looks readable."
  exit 0
fi

if [[ "$MODE" == "--auto" ]]; then
  reset_pipeline_state
  exit 0
fi

echo ""
echo "Repair needed. Run:"
echo "  bash scripts/repair-hdfs-state.sh --reset"
exit 1
