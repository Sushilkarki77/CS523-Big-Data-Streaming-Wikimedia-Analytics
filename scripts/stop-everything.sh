#!/usr/bin/env bash
# Stop all Wiki Pulse processes started manually or by start.sh.
#
# Usage:
#   bash scripts/stop-everything.sh              # stop all processes (default)
#   bash scripts/stop-everything.sh --pids-only  # only .run/ background PIDs (same as stop.sh)
#   bash scripts/stop-everything.sh --containers   # also stop course Docker containers

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"
wiki_pulse_platform_init

ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$ROOT"

stop_course_containers() {
  echo "Stopping course Docker containers..."
  docker stop kafka-server zookeeper-server cs523bdt-lab hive-metastore-db >/dev/null 2>&1 || true
}

PIDS_ONLY=0
STOP_CONTAINERS=0
for arg in "$@"; do
  case "$arg" in
    --pids-only) PIDS_ONLY=1 ;;
    --containers) STOP_CONTAINERS=1 ;;
    *)
      echo "Usage: bash scripts/stop-everything.sh [--pids-only] [--containers]"
      exit 2
      ;;
  esac
done

if [[ "$PIDS_ONLY" -eq 1 ]]; then
  bash scripts/stop.sh
  if [[ "$STOP_CONTAINERS" -eq 1 ]]; then
    echo ""
    echo "== Stopping course Docker stack =="
    stop_course_containers
  fi
  exit 0
fi

stop_by_pattern() {
  local label="$1"
  local pattern="$2"

  if command -v pkill >/dev/null 2>&1; then
    echo "Stopping ${label}..."
    pkill -f "$pattern" >/dev/null 2>&1 || true
  else
    echo "WARN: pkill not found; skip ${label}. Stop it with Ctrl+C if still running."
  fi
}

stop_lab_processes() {
  if ! docker inspect cs523bdt-lab >/dev/null 2>&1; then
    return
  fi

  echo "Stopping Spark/Hive processes inside cs523bdt-lab..."
  docker exec cs523bdt-lab bash -lc "
    pkill -f 'spark-shell' >/dev/null 2>&1 || true
    pkill -f 'org.apache.spark.deploy.SparkSubmit' >/dev/null 2>&1 || true
    pkill -f 'org.apache.hadoop.hive.cli.CliDriver' >/dev/null 2>&1 || true
  " >/dev/null 2>&1 || true
}

stop_labeled_containers() {
  local ids
  ids="$(docker ps --filter 'label=wiki-pulse.project=final-project' --format '{{.ID}}' 2>/dev/null || true)"

  if [[ -n "$ids" ]]; then
    echo "Stopping Wiki Pulse Docker helper containers..."
    docker stop $ids >/dev/null 2>&1 || true
  fi
}

echo "== Stopping PID-managed background processes =="
bash scripts/stop.sh || true

echo ""
echo "== Stopping manually started project processes =="
stop_by_pattern "producer wrapper" "scripts/run-producer-docker.sh"
stop_by_pattern "Spark wrapper" "scripts/run-spark-streaming-hive.sh"
stop_by_pattern "Hive exporter loop" "scripts/export-hive-dashboard-loop.sh"
stop_by_pattern "Hive exporter loop" "scripts/export-hive-dashboard-data.sh"
stop_by_pattern "dashboard API" "node --watch src/server.js"
stop_by_pattern "React/Vite dashboard" "vite --host 0.0.0.0"

echo ""
echo "== Stopping container-side project processes =="
stop_labeled_containers
stop_lab_processes

if [[ "$STOP_CONTAINERS" -eq 1 ]]; then
  echo ""
  echo "== Stopping course Docker stack =="
  stop_course_containers
else
  echo ""
  echo "Course Docker containers were left running."
  echo "To stop them too, run:"
  echo "  bash scripts/stop-everything.sh --containers"
fi

echo ""
echo "Done."
