#!/usr/bin/env bash
# Stop processes started by scripts/start.sh.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN_DIR="${ROOT}/.run"
PID_DIR="${RUN_DIR}/pids"

if [[ ! -d "$PID_DIR" ]]; then
  echo "No PID directory found: ${PID_DIR}"
  exit 0
fi

stop_pid_file() {
  local pid_file="$1"
  local name
  name="$(basename "$pid_file" .pid)"
  local pid
  pid="$(<"$pid_file")"

  if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    echo "Stopping ${name} (pid ${pid})..."
    kill "$pid" >/dev/null 2>&1 || true
  else
    echo "${name} is not running."
  fi

  rm -f "$pid_file"
}

for pid_file in "$PID_DIR"/*.pid; do
  [[ -e "$pid_file" ]] || continue
  stop_pid_file "$pid_file"
done

echo ""
echo "Stopped background host processes."
echo "For full cleanup (manual terminals + Spark in lab): bash scripts/stop-everything.sh"
