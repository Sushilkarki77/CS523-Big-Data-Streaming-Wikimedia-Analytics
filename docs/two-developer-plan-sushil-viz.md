# Two-developer plan — Sushil (source → Hive) + Sudipto (Hive → visualization)

This document is the **working agreement** for splitting and demoing the current final pipeline:

- **Sushil** owns **ingestion through persistence**: public stream → **Kafka** → **Spark Structured Streaming** → **Hive**.
- **Sudipto** owns **consumption for insights**: consume Hive-exported CSV/API data → **React dashboard / charts**.

Everything else (Docker lab, message contract, producer) stays as documented in **`kafka-message-contract.md`**, **`phase0-inventory.md`**, and **`implementation-plan.md`**.

---

## Shared rules (both developers)

| Rule | Detail |
|------|--------|
| **Contract freeze** | Raw Kafka JSON follows **`docs/kafka-message-contract.md`** (`schema_version` `1.0`). Breaking changes require **both** devs notified and Spark + viz updated together. |
| **Integration handshake** | **`docs/sink-spec.md`** is the **single source of truth** for **Hive database name, table names, column names, types, and no-partition choice**. CSV/API payloads should match that schema. |
| **Environment** | **`cs523bdt-lab`** for Spark + Hive CLI; **`kafka-server`** for Kafka; **`hive-metastore-db`** Postgres backs metastore (see **`phase0-inventory.md`**). |
| **Git / repo** | Sushil: `producer/`, `spark-streaming/`, `sql/`, `static-data/`, `scripts/`. **Sudipto:** `dashboard-react/`, dashboard styling, demo story. Small PRs; avoid editing the same large file simultaneously without coordination. |

---

## Interface: what lands in Hive (must be agreed early)

**Sudipto** can build final charts from the Node API / CSV snapshots once the Hive table shapes are fixed.

### Recommended minimum (dashboard-friendly)

**Table A — time-series throughput** (for line charts)

| Column | Type | Example | Notes |
|--------|------|---------|--------|
| `window_start` | `TIMESTAMP` | start of 5-minute bucket | Spark window start |
| `window_end` | `TIMESTAMP` | end of bucket | Optional if redundant |
| `edit_count` | `BIGINT` | total edits in window | Global across all wikis |
| `bot_edit_count` | `BIGINT` | optional | For bot vs human charts |
| `batch_written_at` | `TIMESTAMP` | when Spark wrote row | Debugging / latest snapshot selection |

Partitioning: none in the current implementation.

**Table B — rankings** (for bar / donut charts), e.g. refresh every window or every N minutes

| Column | Type | Notes |
|--------|------|--------|
| `window_start` | `TIMESTAMP` | aligns with Table A windows |
| `wiki` | `STRING` | `meta.domain` style domain |
| `edit_count` | `BIGINT` | counts per wiki in that window |
| `batch_written_at` | `TIMESTAMP` | latest snapshot selection |

Sudipto may use **latest `batch_written_at`** for “top wikis right now,” or recent throughput rows for trends.

**Table C — project-family counts** (bonus HDFS static join)

| Column | Type | Notes |
|--------|------|-------|
| `window_start` | `TIMESTAMP` | aligns with Table A windows |
| `window_end` | `TIMESTAMP` | end of bucket |
| `project_family` | `STRING` | joined from `static-data/wiki_project_lookup.csv` |
| `edit_count` | `BIGINT` | counts per family in that window |
| `batch_written_at` | `TIMESTAMP` | latest snapshot selection |

**Deliverable:** Keep **`docs/sink-spec.md`**, `sql/hive/create_wiki_pulse_tables.sql`, and dashboard CSV/API fields aligned.

---

## Sushil — responsibilities and task list

### Scope

Source → **Kafka producer** → **Spark Structured Streaming** → **HDFS static CSV join** → **Hive-readable Parquet tables**.

### Phase A — Confirm upstream (short)

| # | Task | Done when |
|---|------|-----------|
| A1 | Kafka + ZooKeeper up; topic **`bdt-wikimedia-recentchange`** exists | `kafka-topics --describe` shows healthy leader |
| A2 | Producer publishes continuously | `kafka-console-consumer` shows steady JSON |
| A3 | Align with **Sudipto** on **`sink-spec`** column names | **`docs/sink-spec.md`** reviewed together |

### Phase B — Spark streaming → aggregates

| # | Task | Done when |
|---|------|-----------|
| B1 | Create `spark-streaming/` job (Scala or PySpark per course preference) | Reads Kafka with `subscribe` / bootstrap **`kafka-server:9092`** inside lab |
| B2 | Parse JSON with explicit schema matching contract | No silent drops; bad rows to log or DLQ optional |
| B3 | **Watermark** on `event_time`; **window** (e.g. 5 minutes) | `groupBy` window + optional `wiki` for Table B |
| B4 | **Checkpoint** on HDFS (`hdfs://localhost:9000/...`) | Restart job; processing resumes |
| B5 | Output **DataFrame** matching Table A / Table B columns | Unit-test or `show()` in lab |

### Phase C — Hive sink

| # | Task | Done when |
|---|------|-----------|
| C1 | Hive metastore reachable; create **database** | `SHOW DATABASES` in Beeline/spark-sql |
| C2 | Run DDL from **`sql/`** (Sudipto can author DDL; Sushil executes or vice versa per repo rule) | Tables visible in Hive |
| C3 | Spark streaming **foreachBatch** or **complete mode** write to Hive | Rows appear when producer runs |
| C4 | Document **write mode** (append), **no-partition Hive choice**, and **refresh cadence** in README | Sudipto can schedule viz refresh |

### Phase D — Integration handoff

| # | Task | Done when |
|---|------|-----------|
| D1 | Export CSV snapshots with `scripts/export-hive-dashboard-data.sh` | `dashboard-react/backend/data/*.csv` update |
| D2 | Joint test: producer + Spark + Hive + API + React | Row counts move over 15–30 min demo |

### Bonus (course +2)

| # | Task |
|---|------|
| E1 | Static CSV on **HDFS**; **broadcast join** in Spark before Hive write — implemented |
| E2 | Extra Hive table `wiki_pulse_by_project_family` documented in **`sink-spec`** |

---

## Sudipto — responsibilities and task list

### Scope

Assume **Hive holds authoritative aggregates** produced by Sushil. The dashboard consumes CSV snapshots exported from Hive via the Node API; it does **not** read Kafka directly.

### Phase A — Schema and connectivity

| # | Task | Done when |
|---|------|-----------|
| T1 | Review **`docs/sink-spec.md`** with Sushil; request changes **before** heavy Spark work | Locked column list |
| T2 | Use stack: **Node/Express + React/Vite + Recharts** | Tool installs documented |
| T3 | Consume `http://localhost:4000/api/dashboard` from React | Test API returns rows |

### Phase B — Dashboard build (parallel while Sushil finishes Spark)

| # | Task | Done when |
|---|------|-----------|
| T4 | **Mock dashboard** using `sample-data/` fallback matching **`sink-spec`** | Charts render with fake data |
| T5 | **Chart 1 — Throughput:** line chart from Table A (`window_start`, `edit_count`) | Updates when new rows appear |
| T6 | **Chart 2 — Top wikis:** bar chart from Table B (latest window or sliding) | Clear labels (`wiki`) |
| T7 | **Chart 3 — Project family bonus:** use `wiki_pulse_by_project_family` | Matches Spark HDFS CSV join |

### Phase C — Polish and demo

| # | Task | Done when |
|---|------|-----------|
| T8 | Auto-refresh or manual refresh interval documented | README dashboard section |
| T9 | Screenshots / script for video | Aligns with rubric “live or frequently updating” |

---

## Joint milestones (calendar blocks — adjust to deadline)

| Milestone | Goal |
|-----------|------|
| **M1 — Schema freeze** | **`docs/sink-spec.md`** approved by both |
| **M2 — First Hive rows** | Sushil: Spark writes ≥ 1 batch; Sudipto: `SELECT COUNT(*)` works |
| **M3 — Wire viz** | Sudipto switches mocks → live Hive |
| **M4 — End-to-end demo** | Producer + Spark + Hive + dashboard in one run |
| **M5 — Submission** | README, GitHub link, video |

Suggested pacing if **~2–3 weeks** remain:

| Week | Sushil | Sudipto |
|------|--------|---------|
| **1** | Spark read Kafka + window aggregates (console) | Mock dashboard + JDBC/spark-sql probe |
| **2** | Hive DDL + streaming sink | Replace mocks with Hive queries |
| **3** | Bonus join + stability | Polish charts + refresh + README viz section |

---

## Risks and mitigations

| Risk | Mitigation |
|------|------------|
| Hive tables or HDFS paths not ready | Run `scripts/upload-static-wiki-lookup.sh` and `scripts/create-hive-tables.sh`; verify with Hive CLI before demo |
| Schema drift | Only change **`sink-spec`** with paired PR |
| Sudipto blocked on Hive access | Sushil exports **nightly Parquet** or CSV snapshot to shared path as fallback |
| Kafka advertised host (`kafka-server`) | Producer/Spark run **inside lab container** or hosts file on Windows host |

---

## Related documents

| Doc | Role |
|-----|------|
| **`docs/sink-spec.md`** | **Hive tables + columns — fill together** |
| **`docs/kafka-message-contract.md`** | Raw JSON fields |
| **`docs/team-parallel-plan.md`** | Generic two-person RACI |
| **`docs/implementation-plan.md`** | Full course phases |
