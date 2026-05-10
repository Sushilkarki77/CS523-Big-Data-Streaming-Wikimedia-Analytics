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

## Target architecture

```text
Public API (or replay)
        │
        ▼
   Kafka producer ──► Kafka topic
                            │
                            ▼
              Spark Structured Streaming (checkpointed)
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
      [optional] Spark SQL          Static CSV on HDFS
      join / enrichment                    │
              └─────────────┬─────────────┘
                            ▼
                    HBase  OR  Hive
                            ▼
                      Dashboard
```

---

## Phase summary

| Phase | Focus | Exit criteria |
|-------|--------|----------------|
| **0** (done) | Inventory Docker stack, verify Kafka | Documented in [phase0-inventory.md](./phase0-inventory.md); `scripts/phase0-verify.sh` passes |
| **1** (done) | Choose stream + message contract | Topic **`bdt-wikimedia-recentchange`**; see [kafka-message-contract.md](./kafka-message-contract.md) and [README.md](../README.md) |
| **2** (done) | Kafka producer (Part 1) | `producer/wikimedia_kafka_producer.py`; `scripts/verify-producer.sh`; README Phase 2 |
| **3** | Spark Structured Streaming (Part 2) | `readStream` from Kafka, explicit schema, checkpoint dir, meaningful windowed/aggregate logic |
| **4** | Persistence (Part 3) | Processed results written to HBase **or** Hive; readable by downstream query path |
| **5** | Dashboard (Part 4) | Dashboard updates with metrics tied to Spark logic |
| **6** | Bonus: Spark SQL + HDFS | Static file on HDFS; broadcast/small-file join; enriched fields in sink |
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
- **`scripts/verify-producer.sh`** — optional Docker-based verification on `kafka-server` network.
- **`producer/requirements.txt`**, README Phase 2 run instructions.

**Exit criteria**

- Run producer + `kafka-console-consumer` to confirm sustained JSON lines (see README).

---

## Phase 3 — Spark Structured Streaming (Part 2)

**Goals**

- Streaming job consumes project topic; applies non-trivial transformations.

**Tasks**

- Configure Kafka subscription, `startingOffsets`, **`checkpointLocation`** (HDFS or durable path in lab).
- Parse with `from_json` + schema; use **watermarks** if event-time windows are used.
- Choose at least one: **windowed aggregation**, **filtering**, **anomaly rule**, **moving average**, or join to small static frame.

**Exit criteria**

- Batches run in Spark UI; restart recovery works via checkpoint.

---

## Phase 4 — Sink to HBase or Hive (Part 3)

**Goals**

- Persist streaming outputs for dashboard consumption.

**Decision**

- **HBase**: row-key design for key lookups (dashboard-friendly).
- **Hive**: partitioned table for appended window summaries (SQL/BI-friendly).

**Tasks**

- Start required daemons in `cs523bdt-lab` (HiveServer2 / HBase) per lab instructions if not already running.
- Implement Spark write path compatible with course image versions (Spark 3.1.2, Hive 3.1.2, HBase 2.2.0).

**Exit criteria**

- Spot-check rows via HBase shell / Beeline or equivalent.

---

## Phase 5 — Dashboard (Part 4)

**Goals**

- Visualize insights that match Spark outputs.

**Tasks**

- Pick tooling (Streamlit/Grafana/ELK/Tableau/custom).
- Define refresh strategy (polling JDBC, query API, or exported aggregates).

**Exit criteria**

- Charts update while producer + Spark job run.

---

## Phase 6 — Bonus (Part 5)

**Goals**

- `spark.read` static file from **HDFS**; **Spark SQL** join with streaming pipeline before sink.

**Tasks**

- Document HDFS path and upload commands (`hdfs dfs -put`).
- Use broadcast join when reference data is small.

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
sql/                          # Hive DDL or HBase notes
dashboard/                    # Phase 5
scripts/
  phase0-verify.sh
.env.example
README.md                     # Phase 7 — full runbook
.gitignore
```

---

## Immediate next steps (after this document)

1. Finalize **data source** and **topic name**.
2. Add **`127.0.0.1 kafka-server`** to Windows hosts if producers run on the host.
3. Implement **Phase 1–2**: topic + producer skeleton → live traffic into Kafka.

---

## Related documentation

- [phase0-inventory.md](./phase0-inventory.md) — containers, ports, Kafka bootstrap, metastore DB
- Course PDF — rubric and deliverables (GitHub + Microsoft Streams video)
