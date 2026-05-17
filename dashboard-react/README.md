# React dashboard

```text
Hive tables -> CSV snapshot export -> Node API -> React charts
```

Hive is the source of truth. The exporter snapshots Hive query results into CSV files that the Node API serves as JSON (no HiveServer2/JDBC).

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

One shot:

```bash
bash scripts/export-hive-dashboard-data.sh
```

Continuous (recommended):

```bash
bash scripts/export-hive-dashboard-loop.sh
```

Or set interval:

```bash
EXPORT_INTERVAL_SECONDS=60 bash scripts/export-hive-dashboard-loop.sh
```

Generated files:

```text
dashboard-react/backend/data/throughput_latest.csv
dashboard-react/backend/data/by_wiki_latest.csv
dashboard-react/backend/data/project_family_latest.csv
```

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
