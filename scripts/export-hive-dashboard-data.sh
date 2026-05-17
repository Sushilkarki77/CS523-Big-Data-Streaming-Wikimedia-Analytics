#!/usr/bin/env bash
# Export latest Hive summary rows to CSV snapshots consumed by the Node dashboard API.
#
# Prerequisites (run in other terminals first):
#   bash scripts/run-producer-docker.sh
#   bash scripts/run-spark-streaming-hive.sh
#
# One-time export (from repo root; may take 1–2 minutes, little console output until done):
#   bash scripts/export-hive-dashboard-data.sh
#
# Continuous export for a live dashboard (Ctrl+C to stop):
#   bash scripts/export-hive-dashboard-loop.sh
#
# On success you should see:
#   Exported dashboard CSV snapshots:
#     .../dashboard-react/backend/data/throughput_latest.csv
#     .../dashboard-react/backend/data/by_wiki_latest.csv
#     .../dashboard-react/backend/data/project_family_latest.csv
#
# Then refresh http://localhost:5173 (backend: npm run dev in dashboard-react/backend).
# Hive errors (if any): dashboard-react/backend/data/.hive-errors/*.err
#
# Optional environment variables:
#   DASHBOARD_DATA_DIR      default: dashboard-react/backend/data
#   HIVE_DATABASE           default: wiki_pulse
#   THROUGHPUT_LIMIT        default: 100
#   TOP_WIKI_LIMIT          default: 25
#   PROJECT_FAMILY_LIMIT    default: 25
#   HIVE_EXPORT_ERROR_DIR   default: dashboard-react/backend/data/.hive-errors

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib/common.sh
source "${SCRIPT_DIR}/lib/common.sh"
wiki_pulse_platform_init

ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$ROOT"

if ! docker inspect cs523bdt-lab >/dev/null 2>&1; then
  echo "ERROR: cs523bdt-lab container not found. Start your Docker stack first."
  exit 1
fi

DATA_DIR="${DASHBOARD_DATA_DIR:-${ROOT}/dashboard-react/backend/data}"
HIVE_DATABASE="${HIVE_DATABASE:-wiki_pulse}"
THROUGHPUT_LIMIT="${THROUGHPUT_LIMIT:-100}"
TOP_WIKI_LIMIT="${TOP_WIKI_LIMIT:-25}"
PROJECT_FAMILY_LIMIT="${PROJECT_FAMILY_LIMIT:-25}"
ERROR_DIR="${HIVE_EXPORT_ERROR_DIR:-${DATA_DIR}/.hive-errors}"

mkdir -p "$DATA_DIR"
mkdir -p "$ERROR_DIR"

THROUGHPUT_TMP="${DATA_DIR}/throughput_latest.csv.tmp"
BY_WIKI_TMP="${DATA_DIR}/by_wiki_latest.csv.tmp"
PROJECT_FAMILY_TMP="${DATA_DIR}/project_family_latest.csv.tmp"
THROUGHPUT_OUT="${DATA_DIR}/throughput_latest.csv"
BY_WIKI_OUT="${DATA_DIR}/by_wiki_latest.csv"
PROJECT_FAMILY_OUT="${DATA_DIR}/project_family_latest.csv"

cleanup_tmp_files() {
  rm -f "$THROUGHPUT_TMP" "$BY_WIKI_TMP" "$PROJECT_FAMILY_TMP"
}

trap cleanup_tmp_files ERR INT TERM

print_hive_failure() {
  local label="$1"
  local log_file="$2"

  echo "ERROR: Hive export query failed for ${label}." >&2
  echo "Hive error log: ${log_file}" >&2

  if grep -Eq "BlockMissingException|Could not obtain block|No live nodes contain block" "$log_file"; then
    echo "" >&2
    echo "Detected missing HDFS blocks in generated pipeline data." >&2
    echo "Stop Spark/exporter, then run:" >&2
    echo "  bash scripts/repair-hdfs-state.sh --reset" >&2
    echo "  bash scripts/setup.sh" >&2
    echo "  bash scripts/run-spark-streaming-hive.sh" >&2
  fi
}

run_hive_csv_query() {
  local label="$1"
  local query="$2"
  local log_file="${ERROR_DIR}/${label}.err"

  if ! printf "%s\n" "$query" | docker exec -i cs523bdt-lab bash -lc '
    tmp_sql="$(mktemp /tmp/wiki-pulse-query.XXXXXX.sql)"
    cat > "$tmp_sql"
    hive -S -f "$tmp_sql"
    status=$?
    rm -f "$tmp_sql"
    exit "$status"
  ' 2>"$log_file" | sed '/^[[:space:]]*$/d'; then
    print_hive_failure "$label" "$log_file"
    cleanup_tmp_files
    exit 1
  fi

  rm -f "$log_file"
}

# --- Hive export queries (read-only; see docs/hive-dashboard-export.md) ---
# Each query: inner SELECT = newest LIMIT rows from wiki_pulse.* ;
# outer SELECT concat_ws = one CSV data line per row; timestamps space→T for ISO parsing.
# Shell adds CSV header + writes *.csv.tmp then mv to throughput_latest.csv etc.

# Table: wiki_pulse_throughput → throughput_latest.csv (throughput chart, bot/human totals)
THROUGHPUT_QUERY="
SET hive.cli.print.header=false;
SELECT concat_ws(',',
  regexp_replace(CAST(window_start AS STRING), ' ', 'T'),
  regexp_replace(CAST(window_end AS STRING), ' ', 'T'),
  CAST(edit_count AS STRING),
  CAST(bot_edit_count AS STRING),
  regexp_replace(CAST(batch_written_at AS STRING), ' ', 'T')
)
FROM (
  SELECT window_start, window_end, edit_count, bot_edit_count, batch_written_at
  FROM ${HIVE_DATABASE}.wiki_pulse_throughput
  ORDER BY batch_written_at DESC
  LIMIT ${THROUGHPUT_LIMIT}
) t;
"

# Table: wiki_pulse_by_wiki → by_wiki_latest.csv (top wikis bar chart)
BY_WIKI_QUERY="
SET hive.cli.print.header=false;
SELECT concat_ws(',',
  regexp_replace(CAST(window_start AS STRING), ' ', 'T'),
  regexp_replace(CAST(window_end AS STRING), ' ', 'T'),
  wiki,
  CAST(edit_count AS STRING),
  regexp_replace(CAST(batch_written_at AS STRING), ' ', 'T')
)
FROM (
  SELECT window_start, window_end, wiki, edit_count, batch_written_at
  FROM ${HIVE_DATABASE}.wiki_pulse_by_wiki
  ORDER BY batch_written_at DESC, edit_count DESC
  LIMIT ${TOP_WIKI_LIMIT}
) t;
"

# Table: wiki_pulse_by_project_family → project_family_latest.csv (bonus join chart)
PROJECT_FAMILY_QUERY="
SET hive.cli.print.header=false;
SELECT concat_ws(',',
  regexp_replace(CAST(window_start AS STRING), ' ', 'T'),
  regexp_replace(CAST(window_end AS STRING), ' ', 'T'),
  project_family,
  CAST(edit_count AS STRING),
  regexp_replace(CAST(batch_written_at AS STRING), ' ', 'T')
)
FROM (
  SELECT window_start, window_end, project_family, edit_count, batch_written_at
  FROM ${HIVE_DATABASE}.wiki_pulse_by_project_family
  ORDER BY batch_written_at DESC, edit_count DESC
  LIMIT ${PROJECT_FAMILY_LIMIT}
) t;
"

{
  echo "window_start,window_end,edit_count,bot_edit_count,batch_written_at"
  run_hive_csv_query "throughput" "$THROUGHPUT_QUERY"
} > "$THROUGHPUT_TMP"

{
  echo "window_start,window_end,wiki,edit_count,batch_written_at"
  run_hive_csv_query "by_wiki" "$BY_WIKI_QUERY"
} > "$BY_WIKI_TMP"

{
  echo "window_start,window_end,project_family,edit_count,batch_written_at"
  run_hive_csv_query "project_family" "$PROJECT_FAMILY_QUERY"
} > "$PROJECT_FAMILY_TMP"

mv "$THROUGHPUT_TMP" "$THROUGHPUT_OUT"
mv "$BY_WIKI_TMP" "$BY_WIKI_OUT"
mv "$PROJECT_FAMILY_TMP" "$PROJECT_FAMILY_OUT"

echo "Exported dashboard CSV snapshots:"
echo "  ${THROUGHPUT_OUT}"
echo "  ${BY_WIKI_OUT}"
echo "  ${PROJECT_FAMILY_OUT}"
echo ""
echo "Refresh the dashboard: http://localhost:5173  (API: http://localhost:4000/api/dashboard)"
