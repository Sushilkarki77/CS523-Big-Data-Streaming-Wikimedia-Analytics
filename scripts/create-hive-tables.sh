#!/usr/bin/env bash
# Create Phase 4 Hive database/tables inside cs523bdt-lab using Hive CLI.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"
wiki_pulse_platform_init

ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$ROOT"

SQL_SRC="${ROOT}/sql/hive/create_wiki_pulse_tables.sql"
SQL_DEST_DIR="/tmp/final-project/sql/hive"
SQL_DEST="${SQL_DEST_DIR}/create_wiki_pulse_tables.sql"

if [[ ! -f "${SQL_SRC}" ]]; then
  echo "ERROR: SQL file not found: ${SQL_SRC}"
  exit 1
fi

if ! docker inspect cs523bdt-lab >/dev/null 2>&1; then
  echo "ERROR: cs523bdt-lab container not found. Start your Docker stack first."
  exit 1
fi

echo "Copying Hive DDL into cs523bdt-lab: ${SQL_DEST}"
docker exec -i cs523bdt-lab bash -lc "mkdir -p '${SQL_DEST_DIR}' && cat > '${SQL_DEST}'" < "${SQL_SRC}"

echo "Creating Hive database and tables..."
docker exec cs523bdt-lab bash -lc "hive -f '${SQL_DEST}'"

echo ""
echo "Hive tables:"
docker exec cs523bdt-lab bash -lc "hive -e 'USE wiki_pulse; SHOW TABLES;'"
