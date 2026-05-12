#!/usr/bin/env bash
# Export latest Hive summary rows to CSV snapshots consumed by the Node dashboard API.
#
# Usage:
#   bash scripts/export-hive-dashboard-data.sh
#
# Optional environment variables:
#   DASHBOARD_DATA_DIR      default: dashboard-react/backend/data
#   HIVE_DATABASE           default: wiki_pulse
#   THROUGHPUT_LIMIT        default: 100
#   TOP_WIKI_LIMIT          default: 25

set -euo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if ! docker inspect cs523bdt-lab >/dev/null 2>&1; then
  echo "ERROR: cs523bdt-lab container not found. Start your Docker stack first."
  exit 1
fi

DATA_DIR="${DASHBOARD_DATA_DIR:-${ROOT}/dashboard-react/backend/data}"
HIVE_DATABASE="${HIVE_DATABASE:-wiki_pulse}"
THROUGHPUT_LIMIT="${THROUGHPUT_LIMIT:-100}"
TOP_WIKI_LIMIT="${TOP_WIKI_LIMIT:-25}"

mkdir -p "$DATA_DIR"

THROUGHPUT_TMP="${DATA_DIR}/throughput_latest.csv.tmp"
BY_WIKI_TMP="${DATA_DIR}/by_wiki_latest.csv.tmp"
THROUGHPUT_OUT="${DATA_DIR}/throughput_latest.csv"
BY_WIKI_OUT="${DATA_DIR}/by_wiki_latest.csv"

run_hive_csv_query() {
  local query="$1"
  docker exec cs523bdt-lab bash -lc "hive -S -e \"${query}\"" 2>/dev/null | sed '/^[[:space:]]*$/d'
}

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

{
  echo "window_start,window_end,edit_count,bot_edit_count,batch_written_at"
  run_hive_csv_query "$THROUGHPUT_QUERY"
} > "$THROUGHPUT_TMP"

{
  echo "window_start,window_end,wiki,edit_count,batch_written_at"
  run_hive_csv_query "$BY_WIKI_QUERY"
} > "$BY_WIKI_TMP"

mv "$THROUGHPUT_TMP" "$THROUGHPUT_OUT"
mv "$BY_WIKI_TMP" "$BY_WIKI_OUT"

echo "Exported dashboard CSV snapshots:"
echo "  ${THROUGHPUT_OUT}"
echo "  ${BY_WIKI_OUT}"
