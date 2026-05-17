# React dashboard

```text
Hive tables -> CSV snapshot export -> Node API -> React charts
```

Hive is the source of truth. The exporter snapshots Hive query results into CSV files that the Node API serves as JSON (no HiveServer2/JDBC).

**Export (Hive → CSV):** [docs/hive-dashboard-export.md](../docs/hive-dashboard-export.md).  
**Full path to charts (Hive → CSV → Node JSON → React):** [docs/hive-to-react-charts.md](../docs/hive-to-react-charts.md).

## How export works

```text
wiki_pulse Hive tables  →  export-hive-dashboard-data.sh  →  backend/data/*.csv  →  Express  →  React
```

- **`export-hive-dashboard-data.sh`** — runs three Hive queries in `cs523bdt-lab`, exports the latest rows (limits: 100 throughput, 25 by-wiki, 25 project-family), writes CSVs atomically via `*.tmp` then rename.
- **`export-hive-dashboard-loop.sh`** — runs the one-shot script every **120s** (override with `EXPORT_INTERVAL_SECONDS`) until Ctrl+C.

The loop does **not** produce Kafka or Spark data; it only refreshes CSVs so charts track new Hive batches. Run it while producer and `run-spark-streaming-hive.sh` are active. First useful export: ~1–2 minutes after Spark appends a batch.

## Files

| Path | Purpose |
|------|---------|
| `backend/` | Express API (reads CSV snapshots) |
| `frontend/` | Vite React app |
| `backend/data/` | Generated CSVs (gitignored) |
| `../scripts/export-hive-dashboard-data.sh` | One-shot Hive → CSV export |
| `../scripts/export-hive-dashboard-loop.sh` | Continuous export (default every 120s) |

## Install

```bash
cd dashboard-react/backend && npm install
cd ../frontend && npm install
```

## Export Hive snapshots

From the **repo root** (not `dashboard-react/`):

One shot:

```bash
bash scripts/export-hive-dashboard-data.sh
```

Continuous (recommended for live UI):

```bash
bash scripts/export-hive-dashboard-loop.sh
```

Faster refresh:

```bash
EXPORT_INTERVAL_SECONDS=60 bash scripts/export-hive-dashboard-loop.sh
```

Hive errors (if any): `backend/data/.hive-errors/*.err`

Generated files:

```text
dashboard-react/backend/data/throughput_latest.csv
dashboard-react/backend/data/by_wiki_latest.csv
dashboard-react/backend/data/project_family_latest.csv
```

## Hive → chart data (no aggregation in Node/React)

1. **Spark** writes pre-aggregated rows to Hive.
2. **Export script** snapshots latest rows into `backend/data/*.csv`.
3. **`server.js`** reads CSVs, normalizes types, filters to the latest `batch_written_at` for wiki/family charts, builds `summary` for metric cards → JSON at `/api/dashboard`.
4. **`main.jsx`** fetches JSON and passes arrays to Recharts (`edit_count`, `wiki`, `project_family`, etc.).

Details: [docs/hive-to-react-charts.md](../docs/hive-to-react-charts.md).

## Run backend

```bash
cd dashboard-react/backend
npm run dev
```

API:

```text
http://localhost:4000/api/health
http://localhost:4000/api/dashboard
```

If CSVs are missing, the API falls back to `sample-data/`.

## Run frontend

```bash
cd dashboard-react/frontend
npm run dev
```

Open http://localhost:5173

Optional:

```bash
VITE_API_BASE_URL=http://localhost:4000 VITE_REFRESH_MS=15000 npm run dev
```

## Run order

See **[README.md](../README.md)**. Short version:

1. `bash scripts/run-producer-docker.sh`
2. `bash scripts/run-spark-streaming-hive.sh`
3. `bash scripts/export-hive-dashboard-loop.sh`
4. `cd dashboard-react/backend && npm run dev`
5. `cd dashboard-react/frontend && npm run dev`

The Project Families chart uses Spark’s broadcast join to the static HDFS lookup.
