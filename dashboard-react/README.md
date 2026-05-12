# React dashboard

Custom dashboard for the final project:

```text
Hive tables -> CSV snapshot export -> Node API -> React charts
```

This avoids HiveServer2/JDBC. Hive remains the source of truth; the exporter periodically snapshots Hive query results into CSV files that the Node API serves as JSON.

## Files

| Path | Purpose |
|------|---------|
| `backend/` | Express API that reads CSV files and exposes JSON endpoints. |
| `frontend/` | Vite React app with metric cards and charts. |
| `backend/data/` | Generated CSV snapshots from Hive. |
| `../scripts/export-hive-dashboard-data.sh` | Hive CLI exporter for dashboard CSV snapshots. |

## Install

```bash
cd dashboard-react/backend
npm install

cd ../frontend
npm install
```

## Export Hive snapshots

Run once:

```bash
bash scripts/export-hive-dashboard-data.sh
```

Run continuously while demoing:

```bash
while true; do
  bash scripts/export-hive-dashboard-data.sh
  sleep 30
done
```

Generated files:

```text
dashboard-react/backend/data/throughput_latest.csv
dashboard-react/backend/data/by_wiki_latest.csv
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
http://localhost:4000/api/throughput
http://localhost:4000/api/top-wikis
```

If exported CSVs do not exist yet, the API falls back to `sample-data/` so the UI can still load.

## Run frontend

```bash
cd dashboard-react/frontend
npm run dev
```

Open the Vite URL, usually:

```text
http://localhost:5173
```

Optional frontend variables:

```bash
VITE_API_BASE_URL=http://localhost:4000 VITE_REFRESH_MS=15000 npm run dev
```

## Demo run order

1. `bash scripts/run-producer-docker.sh`
2. `bash scripts/run-spark-streaming-hive.sh`
3. `while true; do bash scripts/export-hive-dashboard-data.sh; sleep 30; done`
4. `cd dashboard-react/backend && npm run dev`
5. `cd dashboard-react/frontend && npm run dev`
