# Wiki Pulse — Big Data Technologies final project

**Wikimedia EventStreams → Kafka → Spark Structured Streaming → Hive (Parquet on HDFS) → React dashboard**

## Quick start

1. Start the course Docker stack (`kafka-server`, `zookeeper-server`, `cs523bdt-lab`, `hive-metastore-db`).
2. From this directory:

```bash
bash scripts/start.sh
```

3. Open http://localhost:5173 (API: http://localhost:4000/api/health).

Stop background processes:

```bash
bash scripts/stop.sh
```

Stop everything (including manual terminals):

```bash
bash scripts/stop-everything.sh
```

## Prerequisites

- Docker Desktop / Engine (macOS, Windows, or Linux)
- Node.js + npm (for dashboard; `start.sh` runs `npm install` if needed)
- Internet (Wikimedia SSE + first Spark run downloads Kafka connector JARs)
- Bash in your terminal (`bash scripts/...`)

Check your machine:

```bash
bash scripts/check-prerequisites.sh
```

Optional: `cp .env.example .env` — topic and Kafka settings.

### macOS

The same commands work in **Terminal** or iTerm. Recommended:

- Use **`bash scripts/run-producer-docker.sh`** for Kafka ingest (no `/etc/hosts` change).
- **Apple Silicon**: course images may run under emulation; first Spark start can take longer.
- **Native Python** (optional): use **Python 3.11** (`brew install python@3.11`). `kafka-python` is unreliable on 3.12+. If you run the producer on the host, add `127.0.0.1 kafka-server` to `/etc/hosts`.

### Windows

Use **Git Bash** from the repo root. Scripts disable MSYS path rewriting automatically when needed (paths with spaces in the project folder are supported).

## Project layout

| Path | Role |
|------|------|
| `producer/` | Wikimedia SSE → Kafka (`wikimedia_kafka_producer.py` + modules) |
| `spark-streaming/wiki_recentchange_hive/` | Spark streaming job (assembled at runtime) |
| `scripts/` | Setup, run, export, repair, stop |
| `sql/hive/` | Hive DDL |
| `static-data/` | Wiki → project_family lookup (bonus join) |
| `dashboard-react/` | Node API + React UI |
| `docs/` | Architecture, contracts, deep dives |

## Start the pipeline (step by step)

Requires the course Docker stack: `kafka-server`, `zookeeper-server`, `cs523bdt-lab`, `hive-metastore-db`. Run all commands from this directory.

### 0. Optional check

```bash
bash scripts/check-prerequisites.sh
```

### 1. Clean slate (fresh run or after dropping Hive)

Stop Spark/producer first (Ctrl+C or `bash scripts/stop-everything.sh`), then:

```bash
docker exec cs523bdt-lab bash -lc "hdfs dfsadmin -safemode leave"
bash scripts/repair-hdfs-state.sh --reset
bash scripts/setup.sh
```

`setup.sh` creates the Kafka topic, uploads the wiki lookup to HDFS, and creates Hive tables. Skip this step if you already ran `setup.sh` and HDFS is healthy.

**Drop Hive only (manual):**

```bash
docker exec cs523bdt-lab bash -lc "hive -e 'DROP DATABASE IF EXISTS wiki_pulse CASCADE;'"
bash scripts/setup.sh
```

### 2. Terminal 1 — Producer (Kafka)

```bash
bash scripts/run-producer-docker.sh
```

Leave running. Wait for `Connected to Kafka` and rising publish counts. Quick test (20 messages then exit):

```bash
bash scripts/run-producer-docker.sh 20
```

### 3. Terminal 2 — Spark → Hive

```bash
bash scripts/run-spark-streaming-hive.sh
```

First run may take 1–2 minutes (Kafka connector download). Wait for `Batch X -> wiki_pulse... appended`. Leave running.

Start the **producer before or with** Spark (`SPARK_STARTING_OFFSETS=latest` reads only new messages).

### 4. Terminal 3 — Export (dashboard data)

After Spark has written at least one batch (~1–2 minutes):

```bash
bash scripts/export-hive-dashboard-loop.sh
```

One-shot export: `bash scripts/export-hive-dashboard-data.sh`

The export loop **does not run Spark** — it repeatedly runs Hive queries inside `cs523bdt-lab`, copies the latest rows from the three `wiki_pulse` tables into CSV files under `dashboard-react/backend/data/`, and the Node API reads those files for the React UI. Default interval: **120 seconds**. See [Hive → dashboard export](#hive--dashboard-export).

### 5. Terminals 4 & 5 — Dashboard (optional)

```bash
cd dashboard-react/backend && npm install && npm run dev   # http://localhost:4000
cd dashboard-react/frontend && npm install && npm run dev  # http://localhost:5173
```

Refresh http://localhost:5173 after each successful export.

### Minimal path (core pipeline only)

| Step | Command |
|------|---------|
| 1 | `bash scripts/setup.sh` |
| 2 | `bash scripts/run-producer-docker.sh` |
| 3 | `bash scripts/run-spark-streaming-hive.sh` |

Enough for live data in the three Hive tables. Add steps 4–5 (or `bash scripts/start.sh`) for the UI.

### Verify

```bash
docker exec kafka-server kafka-topics --bootstrap-server localhost:9092 --describe --topic bdt-wikimedia-recentchange
docker exec cs523bdt-lab bash -lc "hive -e 'USE wiki_pulse; SHOW TABLES;'"
docker exec cs523bdt-lab bash -lc "hive -e 'SELECT COUNT(*) FROM wiki_pulse.wiki_pulse_throughput;'"
ls -la dashboard-react/backend/data/
curl -s http://localhost:4000/api/health
```

See [Check Hive database and tables](#check-hive-database-and-tables) for schema and sample queries.

### Stop

```bash
bash scripts/stop-everything.sh
```

Or Ctrl+C in each terminal. Background mode: `bash scripts/stop.sh` (logs under `.run/logs/`).

## Common commands

```bash
bash scripts/check-prerequisites.sh
bash scripts/setup.sh
bash scripts/run-producer-docker.sh
bash scripts/run-spark-streaming-hive.sh
bash scripts/export-hive-dashboard-loop.sh
bash scripts/export-hive-dashboard-data.sh
bash scripts/repair-hdfs-state.sh --reset
```

## Check Hive database and tables

Run from the repo root with `cs523bdt-lab` running. DDL lives in `sql/hive/create_wiki_pulse_tables.sql`.

### Database and table list

```bash
docker exec cs523bdt-lab bash -lc "hive -e 'SHOW DATABASES LIKE \"wiki_pulse\";'"
docker exec cs523bdt-lab bash -lc "hive -e 'USE wiki_pulse; SHOW TABLES;'"
```

Expected tables: `wiki_pulse_throughput`, `wiki_pulse_by_wiki`, `wiki_pulse_by_project_family`.

### Table schema

```bash
docker exec cs523bdt-lab bash -lc "hive -e 'DESCRIBE wiki_pulse.wiki_pulse_throughput;'"
docker exec cs523bdt-lab bash -lc "hive -e 'DESCRIBE wiki_pulse.wiki_pulse_by_wiki;'"
docker exec cs523bdt-lab bash -lc "hive -e 'DESCRIBE wiki_pulse.wiki_pulse_by_project_family;'"
```

Optional (storage path and format):

```bash
docker exec cs523bdt-lab bash -lc "hive -e 'DESCRIBE FORMATTED wiki_pulse.wiki_pulse_throughput;'"
```

### Row counts and sample data

After the producer and Spark Hive job have run for 1–2 minutes:

```bash
docker exec cs523bdt-lab bash -lc "hive -e 'SELECT COUNT(*) FROM wiki_pulse.wiki_pulse_throughput;'"
docker exec cs523bdt-lab bash -lc "hive -e 'SELECT COUNT(*) FROM wiki_pulse.wiki_pulse_by_wiki;'"
docker exec cs523bdt-lab bash -lc "hive -e 'SELECT COUNT(*) FROM wiki_pulse.wiki_pulse_by_project_family;'"
```

```bash
docker exec cs523bdt-lab bash -lc "hive -e 'SELECT * FROM wiki_pulse.wiki_pulse_throughput ORDER BY batch_written_at DESC LIMIT 5;'"
docker exec cs523bdt-lab bash -lc "hive -e 'SELECT * FROM wiki_pulse.wiki_pulse_by_wiki ORDER BY batch_written_at DESC, edit_count DESC LIMIT 10;'"
docker exec cs523bdt-lab bash -lc "hive -e 'SELECT * FROM wiki_pulse.wiki_pulse_by_project_family ORDER BY batch_written_at DESC, edit_count DESC LIMIT 10;'"
```

If `SHOW TABLES` is empty, recreate tables: `bash scripts/setup.sh`, then restart producer and Spark.

## Hive → dashboard export

The dashboard does not query Hive directly. The exporter is the bridge from warehouse tables to files the API can serve.

```text
Spark → Hive (Parquet)  →  export scripts  →  backend/data/*.csv  →  Node API  →  React
```

| Script | Purpose |
|--------|---------|
| `scripts/export-hive-dashboard-data.sh` | One-shot: three Hive queries → three CSV snapshots |
| `scripts/export-hive-dashboard-loop.sh` | Repeat the one-shot export every `EXPORT_INTERVAL_SECONDS` (default **120**) until Ctrl+C |

**Each export** reads the newest rows from `wiki_pulse_throughput`, `wiki_pulse_by_wiki`, and `wiki_pulse_by_project_family`, then writes:

- `dashboard-react/backend/data/throughput_latest.csv`
- `dashboard-react/backend/data/by_wiki_latest.csv`
- `dashboard-react/backend/data/project_family_latest.csv`

Writes use temporary `*.csv.tmp` files and rename on success so the API never reads partial files. One export often takes **1–2 minutes** (Hive MapReduce).

**Keep running while demoing:** producer + Spark Hive job + export loop + dashboard backend/frontend. If Hive `COUNT(*)` is 0, fix producer/Spark first — the exporter only copies existing Hive data.

```bash
bash scripts/export-hive-dashboard-loop.sh
EXPORT_INTERVAL_SECONDS=60 bash scripts/export-hive-dashboard-loop.sh   # faster refresh
```

Deep dive: [docs/hive-dashboard-export.md](docs/hive-dashboard-export.md) (includes [what each export SQL query does](docs/hive-dashboard-export.md#export-hive-queries-what-the-sql-does)).  
Hive → React charts: [docs/hive-to-react-charts.md](docs/hive-to-react-charts.md).

## Documentation

| Doc | Purpose |
|-----|---------|
| [docs/architecture.md](docs/architecture.md) | System diagram and layers |
| [docs/kafka-producer-flow.md](docs/kafka-producer-flow.md) | **Producer:** SSE → Kafka (with examples) |
| [docs/spark-streaming-flow.md](docs/spark-streaming-flow.md) | **Spark:** Kafka → Hive (with examples) |
| [docs/hive-dashboard-export.md](docs/hive-dashboard-export.md) | **Export:** Hive → CSV → dashboard API |
| [docs/hive-to-react-charts.md](docs/hive-to-react-charts.md) | **Charts:** CSV → Node JSON → React/Recharts |
| [docs/kafka-message-contract.md](docs/kafka-message-contract.md) | Kafka JSON schema |
| [docs/sink-spec.md](docs/sink-spec.md) | Hive table contract |
| [docs/run-spark-streaming-hive.md](docs/run-spark-streaming-hive.md) | Spark launcher (detailed) |
| [docs/producer-wikimedia-kafka.md](docs/producer-wikimedia-kafka.md) | Producer details |
| [spark-streaming/README.md](spark-streaming/README.md) | Spark jobs overview |
| [dashboard-react/README.md](dashboard-react/README.md) | Dashboard API/UI |
| [docs/archive/](docs/archive/) | Planning notes (optional) |

## Troubleshooting

| Problem | Fix |
|---------|-----|
| HDFS safe mode | `docker exec cs523bdt-lab bash -lc "hdfs dfsadmin -safemode leave"` |
| `BlockMissingException` / export fails | `bash scripts/repair-hdfs-state.sh --reset` then `bash scripts/setup.sh`; restart producer + Spark |
| Export slow (1–2 min) | Normal for Hive MapReduce |
| Dashboard empty | Producer + Spark running? Run export once; `curl localhost:4000/api/dashboard` |
| No Kafka messages | Start producer first; or `SPARK_STARTING_OFFSETS=earliest` on Spark restart |

## Optional dev tools

| Script | Purpose |
|--------|---------|
| `scripts/dev/verify-producer.sh` | Publish N messages and consume back |
| `scripts/dev/run-spark-streaming-console.sh` | Spark metrics to console only (no Hive) |
| `scripts/dev/phase0-verify.sh` | Course stack health check |

Background logs from `start.sh`: `.run/logs/`
