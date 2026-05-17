# Final project — implementation plan

This document is the **master plan** for building the end-to-end pipeline required by the Big Data Technologies final project. Infrastructure specifics (containers, ports, Kafka bootstrap) live in **[phase0-inventory.md](./phase0-inventory.md)**.

**Two teammates working in parallel?** See **[team-parallel-plan.md](./team-parallel-plan.md)** (roles, weekly split, RACI, conflict rules).

---

## Objectives (course alignment)

Build a pipeline that covers:

| Layer | Technology |
|-------|------------|
| Ingestion | Apache Kafka (producer from a public real-time or simulated source) |
| Processing | Apache Spark **Structured Streaming** |
| Storage | **HBase** *or* **Hive** |
| Presentation | Live or frequently updating **dashboard** |
| Bonus | **Spark SQL** join of streaming data with **static data on HDFS** |

---

## Rubric mapping

| Part | Points | What we will implement |
|------|--------|-------------------------|
| 1 | 3 | Producer (**Python or Java**) → Kafka topic |
| 2 | 3 | Spark Structured Streaming from Kafka → transforms (windows, filters, aggregates, anomalies, moving averages, etc.) |
| 3 | 2 | Sink processed stream to **HBase** *or* **Hive** |
| 4 | 2 | Dashboard (e.g. Streamlit, Grafana, ELK, Tableau, or custom web app) fed from storage or query path |
| 5 (bonus) | +2 | Load static dataset from **HDFS**, join to stream via **Spark SQL**, enrich before sink |

---

## Current architecture

```text
Wikimedia EventStreams recentchange
        |
        v
Python producer -> Kafka topic bdt-wikimedia-recentchange
        |
        v
Spark Structured Streaming in cs523bdt-lab
        |
        +-- reads static wiki lookup CSV from HDFS (bonus)
        |   hdfs://localhost:9000/tmp/wiki-pulse/static/wiki_project_lookup.csv
        |
        v
Hive-readable Parquet summary tables
        |
        v
Hive CLI export -> CSV snapshots
        |
        v
Node/Express API -> React dashboard
```

---

## Phase summary

| Phase | Focus | Exit criteria |
|-------|--------|----------------|
| **0** (done) | Inventory Docker stack, verify Kafka | Documented in [phase0-inventory.md](./phase0-inventory.md); `scripts/phase0-verify.sh` passes |
| **1** (done) | Choose stream + message contract | Topic **`bdt-wikimedia-recentchange`**; see [kafka-message-contract.md](./kafka-message-contract.md) and [README.md](../README.md) |
| **2** (done) | Kafka producer (Part 1) | `producer/wikimedia_kafka_producer.py`; `scripts/verify-producer.sh`; README Phase 2 |
| **3** (done) | Spark Structured Streaming (Part 2) | `readStream` from Kafka, explicit schema, checkpoint dir, event-time windows, watermarks, throughput/per-wiki aggregates |
| **4** (done) | Persistence (Part 3) | Processed results written to Hive-readable Parquet tables and queryable with Hive CLI |
| **5** (done) | Dashboard (Part 4) | React dashboard updates from Node API backed by Hive-exported CSV snapshots |
| **6** (done) | Bonus: Spark SQL + HDFS | Static wiki lookup CSV on HDFS; Spark broadcast join; enriched project-family aggregate in Hive/dashboard |
| **7** | Deliverables | Public GitHub repo, README end-to-end steps, ≤20 min video (Microsoft Streams) |

---

## Phase 1 — Data source and Kafka contract (complete)

**Implemented**

- Source: **Wikimedia EventStreams** `recentchange` (`https://stream.wikimedia.org/v2/stream/recentchange`).
- Topic: **`bdt-wikimedia-recentchange`** (4 partitions, RF=1); create with `scripts/create-project-topic.sh`.
- Contract and sample: **[kafka-message-contract.md](./kafka-message-contract.md)**, **[sample-kafka-message.json](./sample-kafka-message.json)**.
- README: **[README.md](../README.md)** — includes **where to see output** (topic describe, console consumer, smoke script).

**Tasks for you**

- Copy `.env.example` → `.env` locally (gitignored).
- Run `bash scripts/create-project-topic.sh` and optional `bash scripts/send-sample-contract-message.sh` to verify.

---

## Phase 2 — Producer implementation (Part 1) (complete)

**Implemented**

- **`producer/wikimedia_kafka_producer.py`** — Wikimedia SSE → JSON contract → Kafka (`kafka-python`, reconnect loop, `--limit` for tests).
- **`scripts/verify-producer.sh`** — optional Docker smoke test on `kafka-server` network (default `--limit` 5).
- **`scripts/run-producer-docker.sh`** — run the same producer in Docker **without** a default limit (continuous stream until stopped); `.env` loaded via mounted repo + `load_dotenv()` (avoids Windows **`--env-file`** issues when paths contain spaces).
- **`producer/requirements.txt`**, README Phase 2 run instructions.

**Exit criteria**

- Run producer + `kafka-console-consumer` to confirm sustained JSON lines (see README).

---

## Phase 3 — Spark Structured Streaming (Part 2) (complete)

**Goals**

- Streaming job consumes project topic; applies non-trivial transformations.

**Starter implementation**

- **`spark-streaming/wiki_recentchange_console.scala`** — Scala `spark-shell` Structured Streaming job that reads Kafka, parses the contract schema, applies event-time watermarks, and prints throughput plus per-wiki window aggregates.
- **`scripts/run-spark-streaming-console.sh`** — copies the Scala script into `cs523bdt-lab` and runs it with the Spark Kafka connector package.
- **`spark-streaming/README.md`** — Phase 3 runbook and environment overrides.

**Tasks**

- Configure Kafka subscription, `startingOffsets`, **`checkpointLocation`** (HDFS or durable path in lab).
- Parse with `from_json` + schema; use **watermarks** if event-time windows are used.
- Choose at least one: **windowed aggregation**, **filtering**, **anomaly rule**, **moving average**, or join to small static frame.

**Exit criteria**

- Batches run in Spark UI; restart recovery works via checkpoint.

---

## Phase 4 — Sink to Hive (Part 3) (complete)

**Goals**

- Persist streaming outputs for dashboard consumption.

**Decision**

- Use **Hive** for Phase 4.
- Keep the Hive sink simple: three non-partitioned summary tables matching `docs/sink-spec.md`.
- HBase remains unnecessary unless we later need key-value lookups.

**Tasks**

- Use Hive CLI inside `cs523bdt-lab`; HiveServer2 is not required for the implemented flow.
- Implement Spark write path compatible with course image versions (Spark 3.1.2, Hive 3.1.2, HBase 2.2.0).

**Starter implementation**

- **`sql/hive/create_wiki_pulse_tables.sql`** — creates the `wiki_pulse` database plus three non-partitioned Parquet Hive tables.
- **`scripts/create-hive-tables.sh`** — runs the DDL with Hive CLI inside `cs523bdt-lab`.
- **`spark-streaming/wiki_recentchange_hive/`** — writes the same Spark aggregates to Hive (fragments concatenated at runtime).
- **`scripts/run-spark-streaming-hive.sh`** — runs the Hive writer. HiveServer2 is not required for CLI verification.

**Exit criteria**

- Spot-check rows via Hive CLI inside `cs523bdt-lab`.

---

## Phase 5 — Dashboard (Part 4) (complete)

**Goals**

- Visualize insights that match Spark outputs.

**Tasks**

- Pick tooling (Streamlit/Grafana/ELK/Tableau/custom).
- Define refresh strategy (polling JDBC, query API, or exported aggregates).

**Starter implementation**

- **`scripts/export-hive-dashboard-data.sh`** — exports latest Hive rows into CSV snapshots.
- **`dashboard-react/backend/`** — Node/Express API that reads CSV snapshots and serves JSON.
- **`dashboard-react/frontend/`** — React/Vite dashboard with metric cards and charts.
- Refresh strategy: keep the exporter running periodically; React polls the Node API.

**Exit criteria**

- Charts update while producer + Spark job run.

---

## Phase 6 — Bonus (Part 5) (complete)

**Goals**

- `spark.read` static file from **HDFS**; **Spark SQL** join with streaming pipeline before sink.

**Tasks**

- Document HDFS path and upload commands (`hdfs dfs -put`).
- Use broadcast join when reference data is small.

**Starter implementation**

- **`static-data/wiki_project_lookup.csv`** — static wiki metadata keyed by `wiki`.
- **`scripts/upload-static-wiki-lookup.sh`** — uploads the lookup to HDFS at `/tmp/wiki-pulse/static/wiki_project_lookup.csv`.
- **`spark-streaming/wiki_recentchange_hive/`** — reads the HDFS CSV, broadcasts it, joins streaming events by `wiki`, and writes `wiki_pulse_by_project_family`.
- **`dashboard-react/frontend/`** — displays the bonus project-family aggregate.

**Exit criteria**

- Enriched columns visible in storage or dashboard.

---

## Phase 7 — Submission polish

**Goals**

- Reproducible repo and demo video.

**Tasks**

- README: start order (Docker → topic → producer → Spark → sink → dashboard).
- Record walkthrough: **source → Kafka → Spark → storage → dashboard**; all team members visible and participating.

---

## Repository layout (target)

```text
docs/
  phase0-inventory.md
  implementation-plan.md      ← this file
producer/                     # Phase 2
spark-streaming/              # Phase 3–6
sql/hive/                     # Hive DDL
static-data/                  # Bonus HDFS CSV source
dashboard-react/              # Phase 5 Node API + React dashboard
scripts/
  phase0-verify.sh
.env.example
README.md                     # Phase 7 — full runbook
.gitignore
```

---

## Remaining submission steps

1. Confirm `bash scripts/start-demo.sh` works from a clean repo with the course Docker stack already running.
2. Record a demo walkthrough: source → Kafka → Spark → HDFS static join → Hive → CSV export → Node API → React dashboard.
3. Push final code to the public GitHub repo.
4. Submit the GitHub link and Microsoft Streams video.

---

## Related documentation

- [phase0-inventory.md](./phase0-inventory.md) — containers, ports, Kafka bootstrap, metastore DB
- [current-end-to-end-flow.md](./current-end-to-end-flow.md) — implemented runtime flow and demo startup
- [source-data-and-metrics.md](./source-data-and-metrics.md) — source fields, Spark metrics, and bonus enrichment
- [sink-spec.md](./sink-spec.md) — Hive table contract
- [../dashboard-react/README.md](../dashboard-react/README.md) — Node/React dashboard runbook
- Course PDF — rubric and deliverables (GitHub + Microsoft Streams video)
