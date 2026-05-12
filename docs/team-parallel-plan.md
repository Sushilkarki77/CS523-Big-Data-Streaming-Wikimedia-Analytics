# Two-person parallel work plan

Use this when **two teammates** work at the same time on the final pipeline. This document now reflects the implemented split: Sushil owns producer/Spark/Hive, and Sudipto can consume the Node/React dashboard or the Hive-exported CSV/API outputs.

**Concrete split (Sushil → Hive + Sudipto → viz):** see **[two-developer-plan-sushil-viz.md](./two-developer-plan-sushil-viz.md)** and **[sink-spec.md](./sink-spec.md)**.

**Shared handshake:** freeze **`docs/kafka-message-contract.md`**, raw topic **`bdt-wikimedia-recentchange`**, and **`schema_version`** (`1.0`) unless both agree to change them.

---

## Roles at a glance

| Track | Primary owner | Repo areas (suggested) |
|-------|----------------|-------------------------|
| **Ingestion & streaming** | Person A | `producer/`, `spark-streaming/` (create when adding Spark job), checkpoint paths documented in README |
| **Storage & visualization** | Person B | `dashboard-react/`, `sample-data/`, dashboard polish, demo story |

---

## Person A — Kafka producer & Spark Structured Streaming

| Responsibility | Notes |
|----------------|--------|
| Producer reliability | Reconnect, backoff, logging; env via `.env` / `.env.example` |
| Spark streaming job | Kafka subscribe, `from_json` + explicit schema, watermarks if event-time windows |
| Checkpoints | Path on HDFS (or agreed durable path in lab); document in README |
| Bonus | Static CSV on HDFS + Spark SQL/DataFrame broadcast join — implemented via `static-data/wiki_project_lookup.csv` |

**Definition of done:** Live JSON on the raw topic; Spark job runs in **`cs523bdt-lab`** and produces **aggregates** (console or sink) matching whatever Person B expects for storage/dashboard.

---

## Person B — Dashboard and demo polish

| Responsibility | Notes |
|----------------|--------|
| Sink choice | **Hive** summary tables — implemented as non-partitioned Parquet-backed Hive tables |
| Schema for downstream | **`docs/sink-spec.md`**: table/column names, types, no-partition choice, CSV/API refresh path |
| Dashboard | Custom React app with Node/Express API reading Hive-exported CSV snapshots |
| Runbook | `README.md` and `dashboard-react/README.md` |

**Parallel unblock:** Person B can still build/test dashboard changes against `sample-data/`; the live path uses CSV snapshots in `dashboard-react/backend/data/`.

**Definition of done:** Hive tables are populated from Spark, CSV snapshots update from Hive, and dashboard charts update from the Node API.

---

## Dependency timeline

```text
Frozen contract + topic  →  A: producer + Spark  →  agreed aggregate shape (sink-spec)
                                        ↓
                              B: sink + dashboard (mock OK early)
```

| Week | Person A | Person B |
|------|-----------|-----------|
| **1** | Producer stable; Spark reads Kafka + parses + windows (console OK) | Sink DDL / table design; **`docs/sink-spec.md`**; dashboard shell + mock data |
| **2** | Spark writes to Hive per sink-spec | Wire dashboard to Node API / CSV snapshots |
| **3** | Bonus join + checkpoint hardening | Polish visuals; README “storage + UI” sections |

Adjust dates to your course deadline.

---

## Conflict avoidance

| Topic | Rule |
|-------|------|
| **Git** | Small PRs; shared branch `main` or `develop`; avoid long-lived divergent branches |
| **Config** | Single source of truth: **`.env.example`**; never commit **`.env`** |
| **Kafka topics** | One raw topic by default; new topics (e.g. `...-aggregates`) must be documented in README |
| **Schema changes** | Bump **`schema_version`** in JSON + update Spark schema + notify Person B |

---

## RACI summary

| Deliverable | Primary |
|-------------|---------|
| Stream → Kafka | A |
| Spark streaming + checkpoints | A |
| Hive design + DDL | A |
| Dashboard + demo story | B |
| Bonus static HDFS join | A |
| End-to-end integration test | Both |
| Final README + video outline | Both (split sections, joint demo) |

---

## If one person finishes early

- **A free:** Help B with API payloads, CSV export cadence, or troubleshooting Spark/Hive writes.
- **B free:** Improve React visuals, dashboard copy, screenshots, or record draft demo clips.

---

## Related docs

- **[implementation-plan.md](./implementation-plan.md)** — phased rubric plan  
- **[kafka-message-contract.md](./kafka-message-contract.md)** — producer JSON contract  
- **[phase0-inventory.md](./phase0-inventory.md)** — Docker services and Kafka bootstrap  
