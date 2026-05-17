#!/usr/bin/env bash
# Quick host checks before running the pipeline (macOS, Linux, or Windows Git Bash).
#
# Usage:
#   bash scripts/check-prerequisites.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"
wiki_pulse_platform_init

ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$ROOT"

OS="$(uname -s 2>/dev/null || echo unknown)"
ARCH="$(uname -m 2>/dev/null || echo unknown)"

echo "== Wiki Pulse prerequisites =="
echo "  OS: ${OS} (${ARCH})"
echo "  Repo: ${ROOT}"
echo ""

fail=0

if ! command -v docker >/dev/null 2>&1; then
  echo "FAIL: docker not found in PATH"
  fail=1
else
  echo "OK: docker $(docker --version 2>/dev/null | head -1)"
  if ! docker info >/dev/null 2>&1; then
    echo "FAIL: docker daemon not running (start Docker Desktop on macOS/Windows)"
    fail=1
  else
    echo "OK: docker daemon reachable"
  fi
fi

if ! command -v bash >/dev/null 2>&1; then
  echo "FAIL: bash not found"
  fail=1
else
  echo "OK: bash $(bash --version 2>/dev/null | head -1)"
fi

for c in kafka-server zookeeper-server cs523bdt-lab hive-metastore-db; do
  if docker inspect "$c" >/dev/null 2>&1; then
    echo "OK: container ${c}"
  else
    echo "WARN: container ${c} not running"
    fail=1
  fi
done

if command -v node >/dev/null 2>&1; then
  echo "OK: node $(node --version 2>/dev/null)"
else
  echo "WARN: node not found (needed for dashboard; start.sh can still run producer/Spark)"
fi

if [[ -f "${ROOT}/.env" ]]; then
  echo "OK: .env present"
else
  echo "NOTE: no .env — scripts use defaults (copy .env.example if needed)"
fi

case "$OS" in
  Darwin)
    echo ""
    echo "macOS: use bash scripts/run-producer-docker.sh (recommended)."
    echo "      Native Python needs 3.11 and optional: sudo sh -c 'grep -q kafka-server /etc/hosts || echo \"127.0.0.1 kafka-server\" >> /etc/hosts'"
    ;;
  MINGW* | MSYS* | CYGWIN* | *_NT*)
    echo ""
    echo "Windows: run scripts from Git Bash; MSYS path conversion is disabled in scripts."
    ;;
esac

echo ""
if [[ "$fail" -eq 0 ]]; then
  echo "Ready. Next: bash scripts/setup.sh && bash scripts/start.sh"
  exit 0
fi

echo "Fix the items above, then re-run this script."
exit 1
