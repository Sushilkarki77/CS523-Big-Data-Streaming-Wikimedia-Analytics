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

## Common commands

```bash
bash scripts/check-prerequisites.sh          # Docker + containers (optional)
bash scripts/setup.sh                        # topic + HDFS lookup + Hive tables
bash scripts/run-producer-docker.sh          # Terminal 1
bash scripts/run-spark-streaming-hive.sh     # Terminal 2
bash scripts/export-hive-dashboard-loop.sh   # Terminal 3
bash scripts/export-hive-dashboard-data.sh   # one-shot export
bash scripts/repair-hdfs-state.sh --reset    # fix corrupt HDFS/Hive state
```

## Manual run (five terminals)

### Fresh setup (once per session)

```bash
bash scripts/stop-everything.sh
docker exec cs523bdt-lab bash -lc "hdfs dfsadmin -safemode leave"
bash scripts/repair-hdfs-state.sh --reset
bash scripts/setup.sh
```

### Terminals

| # | Command | Notes |
|---|---------|--------|
| 1 | `bash scripts/run-producer-docker.sh` | Streams to Kafka |
| 2 | `bash scripts/run-spark-streaming-hive.sh` | Wait for `Batch X -> wiki_pulse... appended` |
| 3 | `bash scripts/export-hive-dashboard-loop.sh` | ~1–2 min per export |
| 4 | `cd dashboard-react/backend && npm run dev` | API on :4000 |
| 5 | `cd dashboard-react/frontend && npm run dev` | UI on :5173 |

Refresh http://localhost:5173 after each successful export.

### Smoke checks

```bash
docker exec kafka-server kafka-topics --bootstrap-server localhost:9092 --describe --topic bdt-wikimedia-recentchange
ls -la dashboard-react/backend/data/
curl -s http://localhost:4000/api/health
```

## Query Hive tables

```bash
docker exec cs523bdt-lab bash -lc "hive -e 'USE wiki_pulse; SHOW TABLES;'"
docker exec cs523bdt-lab bash -lc "hive -e 'SELECT * FROM wiki_pulse.wiki_pulse_throughput ORDER BY batch_written_at DESC LIMIT 5;'"
docker exec cs523bdt-lab bash -lc "hive -e 'SELECT * FROM wiki_pulse.wiki_pulse_by_wiki ORDER BY batch_written_at DESC, edit_count DESC LIMIT 10;'"
docker exec cs523bdt-lab bash -lc "hive -e 'SELECT * FROM wiki_pulse.wiki_pulse_by_project_family ORDER BY batch_written_at DESC, edit_count DESC LIMIT 10;'"
```

## Documentation

| Doc | Purpose |
|-----|---------|
| [docs/architecture.md](docs/architecture.md) | System diagram and layers |
| [docs/kafka-message-contract.md](docs/kafka-message-contract.md) | Kafka JSON schema |
| [docs/sink-spec.md](docs/sink-spec.md) | Hive table contract |
| [docs/run-spark-streaming-hive.md](docs/run-spark-streaming-hive.md) | Spark launcher and job flow |
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
