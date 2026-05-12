#!/usr/bin/env bash
# Start the full manual demo flow in the background:
#   Wikimedia -> Producer -> Kafka -> Spark/Hive (+ HDFS static join) -> CSV export -> Node API -> React UI
#
# Usage:
#   bash scripts/start-demo.sh
#
# Logs/PIDs are written under .demo/.

set -euo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DEMO_DIR="${ROOT}/.demo"
LOG_DIR="${DEMO_DIR}/logs"
PID_DIR="${DEMO_DIR}/pids"
EXPORT_INTERVAL_SECONDS="${EXPORT_INTERVAL_SECONDS:-60}"

mkdir -p "$LOG_DIR" "$PID_DIR"

required_container() {
  local name="$1"
  if ! docker inspect "$name" >/dev/null 2>&1; then
    echo "ERROR: required container '${name}' was not found. Start the course Docker stack first."
    exit 1
  fi
}

is_pid_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1
}

start_background() {
  local name="$1"
  local command="$2"
  local pid_file="${PID_DIR}/${name}.pid"
  local log_file="${LOG_DIR}/${name}.log"

  if [[ -f "$pid_file" ]]; then
    local old_pid
    old_pid="$(<"$pid_file")"
    if is_pid_running "$old_pid"; then
      echo "SKIP: ${name} already appears to be running (pid ${old_pid}). Log: ${log_file}"
      return
    fi
  fi

  echo "Starting ${name}..."
  bash -lc "$command" >"$log_file" 2>&1 &
  local pid=$!
  echo "$pid" >"$pid_file"
  echo "  pid=${pid}"
  echo "  log=${log_file}"
}

echo "== Checking required Docker containers =="
required_container kafka-server
required_container cs523bdt-lab
required_container zookeeper-server
required_container hive-metastore-db

echo ""
echo "== One-time setup =="
bash scripts/create-project-topic.sh
bash scripts/upload-static-wiki-lookup.sh
bash scripts/create-hive-tables.sh

echo ""
echo "== Installing dashboard dependencies if needed =="
if [[ ! -d dashboard-react/backend/node_modules ]]; then
  (cd dashboard-react/backend && npm install)
else
  echo "Backend dependencies already installed."
fi

if [[ ! -d dashboard-react/frontend/node_modules ]]; then
  (cd dashboard-react/frontend && npm install)
else
  echo "Frontend dependencies already installed."
fi

echo ""
echo "== Starting long-running demo processes =="
start_background "producer" "cd '$ROOT' && bash scripts/run-producer-docker.sh"
start_background "spark-hive" "cd '$ROOT' && bash scripts/run-spark-streaming-hive.sh"
start_background "hive-exporter" "cd '$ROOT' && while true; do bash scripts/export-hive-dashboard-data.sh; sleep '${EXPORT_INTERVAL_SECONDS}'; done"
start_background "dashboard-api" "cd '$ROOT/dashboard-react/backend' && npm run dev"
start_background "dashboard-ui" "cd '$ROOT/dashboard-react/frontend' && npm run dev"

echo ""
echo "Demo is starting. Give Spark/Hive and the exporter a minute or two to produce fresh rows."
echo ""
echo "Open:"
echo "  React dashboard: http://localhost:5173"
echo "  Node API health: http://localhost:4000/api/health"
echo ""
echo "Logs:"
echo "  ${LOG_DIR}"
echo ""
echo "Stop demo processes with:"
echo "  bash scripts/stop-demo.sh"
