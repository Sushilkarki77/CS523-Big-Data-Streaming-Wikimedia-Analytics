# Two-person parallel work plan

Use this when **two teammates** work at the same time on the final pipeline. It minimizes merge conflicts and blocking dependencies.

**Shared handshake:** freeze **`docs/kafka-message-contract.md`**, raw topic **`bdt-wikimedia-recentchange`**, and **`schema_version`** (`1.0`) unless both agree to change them.

---

## Roles at a glance

| Track | Primary owner | Repo areas (suggested) |
|-------|----------------|-------------------------|
| **Ingestion & streaming** | Person A | `producer/`, `spark-streaming/` (create when adding Spark job), checkpoint paths documented in README |
| **Storage & visualization** | Person B | `dashboard/`, `sql/` (Hive DDL / HBase notes), optional `docs/sink-spec.md` |

---

## Person A — Kafka producer & Spark Structured Streaming

| Responsibility | Notes |
|----------------|--------|
| Producer reliability | Reconnect, backoff, logging; env via `.env` / `.env.example` |
| Spark streaming job | Kafka subscribe, `from_json` + explicit schema, watermarks if event-time windows |
| Checkpoints | Path on HDFS (or agreed durable path in lab); document in README |
| Bonus (optional) | Static CSV on HDFS + Spark SQL join — often fits this track after base job works |

**Definition of done:** Live JSON on the raw topic; Spark job runs in **`cs523bdt-lab`** and produces **aggregates** (console or sink) matching whatever Person B expects for storage/dashboard.

---

## Person B — Sink (Hive or HBase) & dashboard

| Responsibility | Notes |
|----------------|--------|
| Sink choice | **Hive** (partitioned summaries) vs **HBase** (key-value lookups) — decide once as a team |
| Schema for downstream | Short **`docs/sink-spec.md`**: table/column names, types, partition keys, refresh expectations |
| Dashboard | Streamlit, Grafana, ELK, Tableau, or custom; polling / JDBC / agreed query path |
| Runbook | How to start metastore DB, Hive/HBase daemons in lab, and dashboard |

**Parallel unblock:** If Spark → sink is not ready, Person B can build the dashboard against **mock CSV** or **manual inserts** that match the agreed **aggregate schema**.

**Definition of done:** Tables (or HBase rows) populated from Spark or verified manually; dashboard updates from that layer.

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
| **2** | Spark writes to Hive/HBase per sink-spec | Wire dashboard to real queries |
| **3** | Bonus join; checkpoint hardening | Polish visuals; README “storage + UI” sections |

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
| Hive/HBase design + DDL | B |
| Dashboard + demo story | B |
| End-to-end integration test | Both |
| Final README + video outline | Both (split sections, joint demo) |

---

## If one person finishes early

- **A free:** Help B with JDBC/Hive queries, sink column alignment, or troubleshooting Spark writes.
- **B free:** Manual Parquet/Hive load from sample aggregates, improve **`docs/phase0-inventory.md`** troubleshooting, or record draft demo clips.

---

## Related docs

- **[implementation-plan.md](./implementation-plan.md)** — phased rubric plan  
- **[kafka-message-contract.md](./kafka-message-contract.md)** — producer JSON contract  
- **[phase0-inventory.md](./phase0-inventory.md)** — Docker services and Kafka bootstrap  
