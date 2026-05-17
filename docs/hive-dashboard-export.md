# Hive → dashboard export

How aggregated Hive tables become CSV snapshots the React dashboard can read. Spark writes to Hive in [`spark-streaming-flow.md`](spark-streaming-flow.md); this document covers the **export** step only.

---

## Overview

```text
Spark (running)  →  Hive tables (Parquet on HDFS, database wiki_pulse)
                           │
                           ▼  export-hive-dashboard-data.sh  (Hive CLI queries)
                           ▼
         dashboard-react/backend/data/*.csv
                           │
                           ▼  Node Express API (reads CSV from disk)
                           ▼
                    React dashboard (http://localhost:5173)
```

The dashboard **does not query Hive directly**. Hive remains the source of truth; the exporter copies the **latest useful rows** into CSV files the API serves as JSON (no HiveServer2/JDBC in the hot path).

---

## Scripts

| Script | Behavior |
|--------|----------|
| `scripts/export-hive-dashboard-data.sh` | **One-shot:** run three Hive queries, write three CSV files, exit. |
| `scripts/export-hive-dashboard-loop.sh` | **Loop:** call the one-shot script, sleep, repeat until **Ctrl+C**. |

**Run (from repo root):**

```bash
# After producer + Spark have written at least one batch (~1–2 minutes)
bash scripts/export-hive-dashboard-data.sh

# Live demo (recommended)
bash scripts/export-hive-dashboard-loop.sh

# Faster refresh (default interval is 120 seconds)
EXPORT_INTERVAL_SECONDS=60 bash scripts/export-hive-dashboard-loop.sh
```

---

## What one export does

Each run of `export-hive-dashboard-data.sh`:

1. **Runs Hive SQL** inside container `cs523bdt-lab` against database `wiki_pulse`.
2. **Selects the newest rows** from each summary table (ordered by `batch_written_at`):
   - `wiki_pulse_throughput` — window totals and bot counts (up to **100** rows, `THROUGHPUT_LIMIT`)
   - `wiki_pulse_by_wiki` — edits per wiki (up to **25** rows, `TOP_WIKI_LIMIT`)
   - `wiki_pulse_by_project_family` — edits per project family after the HDFS lookup join (up to **25** rows, `PROJECT_FAMILY_LIMIT`)
3. **Formats rows as CSV** (header row + comma-separated values; timestamps normalized for parsing).
4. **Writes atomically:** data goes to `*.csv.tmp` first, then `mv` to the final filename so the API never reads a half-written file.

**Output files:**

```text
dashboard-react/backend/data/throughput_latest.csv
dashboard-react/backend/data/by_wiki_latest.csv
dashboard-react/backend/data/project_family_latest.csv
```

On failure, Hive stderr is saved under `dashboard-react/backend/data/.hive-errors/` (e.g. `throughput.err`). Common cause: missing HDFS blocks — see README troubleshooting (`repair-hdfs-state.sh`).

A single export often takes **1–2 minutes** because Hive runs MapReduce-style jobs for these queries.

---

## Export Hive queries (what the SQL does)

Defined in `scripts/export-hive-dashboard-data.sh` (`THROUGHPUT_QUERY`, `BY_WIKI_QUERY`, `PROJECT_FAMILY_QUERY`). Each query is **read-only**: it does not insert, update, or re-aggregate data — Spark already wrote summary rows to Hive.

### Shared SQL pattern

Every query uses the same two-layer shape:

```sql
SET hive.cli.print.header=false;
SELECT concat_ws(',',  /* column1 */, /* column2 */, ... )
FROM (
  SELECT /* columns */
  FROM wiki_pulse.<table>
  ORDER BY batch_written_at DESC [, edit_count DESC]
  LIMIT <N>
) t;
```

| Piece | Purpose |
|-------|---------|
| Inner `SELECT … ORDER BY … LIMIT` | Pick the **newest** snapshot rows Spark wrote (`batch_written_at` = processing time when the micro-batch landed in Hive). |
| `ORDER BY batch_written_at DESC` | Most recently written batches first. |
| `ORDER BY …, edit_count DESC` | For ranking tables, prefer higher counts when batch times tie. |
| `LIMIT` | Cap rows per export (`THROUGHPUT_LIMIT` / `TOP_WIKI_LIMIT` / `PROJECT_FAMILY_LIMIT`). |
| Outer `concat_ws(',', …)` | Emit **one comma-separated line per row** (body lines for the CSV). |
| `CAST(… AS STRING)` | Hive prints numbers/timestamps as strings suitable for CSV. |
| `regexp_replace(CAST(timestamp AS STRING), ' ', 'T')` | Turn `2026-05-14 12:30:00` into `2026-05-14T12:30:00` so the Node API parses dates reliably. |
| `SET hive.cli.print.header=false` | Suppress Hive’s column header in query output; the shell script writes the CSV header line itself. |

The shell script prepends a header row (e.g. `window_start,window_end,edit_count,…`) and redirects Hive stdout into `*.csv.tmp`, then renames to `*.csv`.

### Query 1 — `THROUGHPUT_QUERY` → `throughput_latest.csv`

**Hive table:** `wiki_pulse.wiki_pulse_throughput`

**Selects (per row):**

| Column | Meaning |
|--------|---------|
| `window_start` | Event-time start of the Spark tumbling window |
| `window_end` | Event-time end of the window |
| `edit_count` | Total edits in that window |
| `bot_edit_count` | Edits where `bot=true` |
| `batch_written_at` | When Spark appended this snapshot row |

**Sort / limit:** `ORDER BY batch_written_at DESC` then `LIMIT 100` (default).

**Dashboard use:** Throughput line chart; summary cards for total events and bot vs human counts.

**Example CSV line:**

```text
2026-05-14T12:00:00,2026-05-14T12:05:00,42,3,2026-05-14T12:05:30
```

### Query 2 — `BY_WIKI_QUERY` → `by_wiki_latest.csv`

**Hive table:** `wiki_pulse.wiki_pulse_by_wiki`

**Selects (per row):**

| Column | Meaning |
|--------|---------|
| `window_start`, `window_end` | Same window semantics as throughput |
| `wiki` | Wikimedia project id (e.g. `enwiki`) |
| `edit_count` | Edits for that wiki in the window |
| `batch_written_at` | When Spark wrote this row |

**Sort / limit:** `ORDER BY batch_written_at DESC, edit_count DESC` then `LIMIT 25` (default) — newest batches first, then busiest wikis.

**Dashboard use:** Top-wikis bar chart; “top wiki” metric on the summary row.

**Example CSV line:**

```text
2026-05-14T12:00:00,2026-05-14T12:05:00,enwiki,18,2026-05-14T12:05:30
```

### Query 3 — `PROJECT_FAMILY_QUERY` → `project_family_latest.csv`

**Hive table:** `wiki_pulse.wiki_pulse_by_project_family`

**Selects (per row):**

| Column | Meaning |
|--------|---------|
| `window_start`, `window_end` | Same window semantics |
| `project_family` | From Spark’s broadcast join to `wiki_project_lookup.csv` on HDFS (e.g. `Wikipedia`, `Wikidata`) |
| `edit_count` | Edits in that family for the window |
| `batch_written_at` | When Spark wrote this row |

**Sort / limit:** `ORDER BY batch_written_at DESC, edit_count DESC` then `LIMIT 25` (default).

**Dashboard use:** Project Families bonus chart.

**Example CSV line:**

```text
2026-05-14T12:00:00,2026-05-14T12:05:00,Wikipedia,25,2026-05-14T12:05:30
```

### What these queries do *not* do

- **No new aggregation** — counts and windows were computed in Spark; Hive only reads stored Parquet rows.
- **No full table dump** — only the latest **N** rows per table per export run.
- **No direct UI access** — output is files on disk; Express reads them on the next API request.

For table DDL and metric definitions, see [`sink-spec.md`](sink-spec.md).

---

## What the export loop does

`export-hive-dashboard-loop.sh` is a thin wrapper:

```bash
while true; do
  bash scripts/export-hive-dashboard-data.sh   # or log failure and continue
  sleep "${EXPORT_INTERVAL_SECONDS:-120}"
done
```

So it **re-refreshes the CSV snapshots on a timer** (default every **120 seconds**) while you keep the terminal open.

**Why use the loop?**

- Spark **keeps appending** new batches to Hive while the producer and streaming job run.
- The React app polls the API on an interval (e.g. every 15s via `VITE_REFRESH_MS`).
- Without the loop, you would run `export-hive-dashboard-data.sh` manually after each Spark batch to see updated charts.

The loop **does not** run Spark, write to Kafka, or change Hive data — it only **reads** Hive and updates files on the host.

---

## What must be running

| Process | Role |
|---------|------|
| Producer (`run-producer-docker.sh`) | Feeds Kafka so Spark has events |
| Spark Hive job (`run-spark-streaming-hive.sh`) | Writes/updates the three Hive tables |
| **Export loop** | Copies Hive → CSV for the UI |
| Node backend + React frontend | Serves CSV as JSON and renders charts |

If `SELECT COUNT(*)` on Hive tables is **0**, exports succeed but CSVs stay empty (or stale). Fix upstream (producer + Spark), not the exporter alone.

---

## Timing and UI refresh

- **First useful export:** usually **1–2 minutes** after Spark logs show at least one batch appended.
- **Ongoing updates:** each successful loop iteration replaces the three CSVs; refresh http://localhost:5173 or wait for the frontend auto-refresh.
- **API check:** `curl -s http://localhost:4000/api/dashboard`

If CSVs are missing, the API falls back to `sample-data/` so the UI still loads (static demo data).

---

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `EXPORT_INTERVAL_SECONDS` | `120` | Sleep between loop iterations |
| `DASHBOARD_DATA_DIR` | `dashboard-react/backend/data` | Output directory |
| `HIVE_DATABASE` | `wiki_pulse` | Hive database name |
| `THROUGHPUT_LIMIT` | `100` | Max throughput rows per export |
| `TOP_WIKI_LIMIT` | `25` | Max by-wiki rows per export |
| `PROJECT_FAMILY_LIMIT` | `25` | Max project-family rows per export |

---

## Related documentation

| Topic | Document |
|-------|----------|
| **Hive → CSV → JSON → React charts** | [`hive-to-react-charts.md`](hive-to-react-charts.md) |
| Runbook | [`README.md`](../README.md) |
| Dashboard API/UI | [`dashboard-react/README.md`](../dashboard-react/README.md) |
| Hive table schemas | [`sink-spec.md`](sink-spec.md) |
| System diagram | [`architecture.md`](architecture.md) |
