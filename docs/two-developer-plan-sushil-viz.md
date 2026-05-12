# Two-developer plan — Sushil (source → Hive) + Sudipto (Hive → visualization)

This document is the **working agreement** for splitting the final pipeline when:

- **Sushil** owns **ingestion through persistence**: public stream → **Kafka** → **Spark Structured Streaming** → **Hive**.
- **Sudipto** owns **consumption for insights**: read **from Hive** (or exported Parquet/CSV from Hive) → **dashboard / charts**.

Everything else (Docker lab, message contract, producer) stays as documented in **`kafka-message-contract.md`**, **`phase0-inventory.md`**, and **`implementation-plan.md`**.

---

## Shared rules (both developers)

| Rule | Detail |
|------|--------|
| **Contract freeze** | Raw Kafka JSON follows **`docs/kafka-message-contract.md`** (`schema_version` `1.0`). Breaking changes require **both** devs notified and Spark + viz updated together. |
| **Integration handshake** | **`docs/sink-spec.md`** is the **single source of truth** for **Hive database name, table names, column names, types, partitions**. Sushil proposes first revision; **Sudipto** confirms viz feasibility before Sushil locks writes. |
| **Environment** | **`cs523bdt-lab`** for Spark + Hive CLI; **`kafka-server`** for Kafka; **`hive-metastore-db`** Postgres backs metastore (see **`phase0-inventory.md`**). |
| **Git / repo** | Sushil: `producer/`, `spark-streaming/` (create when adding jobs). **Sudipto:** `dashboard/`, `sql/` (DDL). Small PRs; avoid editing the same large file simultaneously without coordination. |

---

## Interface: what lands in Hive (must be agreed early)

**Sudipto** cannot build final charts without knowing **exact** query shapes. Agree on **one or two** Hive tables that Spark **appends** on a schedule.

### Recommended minimum (dashboard-friendly)

**Table A — time-series throughput** (for line charts)

| Column | Type | Example | Notes |
|--------|------|---------|--------|
| `window_start` | `TIMESTAMP` | start of 5-minute bucket | Spark window start |
| `window_end` | `TIMESTAMP` | end of bucket | Optional if redundant |
| `edit_count` | `BIGINT` | total edits in window | Global across all wikis |
| `bot_edit_count` | `BIGINT` | optional | For bot vs human charts |
| `ingest_batch_ts` | `TIMESTAMP` | when Spark wrote row | Debugging |

Partition (optional): `dt` = `DATE` derived from `window_start` for pruning.

**Table B — rankings** (for bar / donut charts), e.g. refresh every window or every N minutes

| Column | Type | Notes |
|--------|------|--------|
| `window_start` | `TIMESTAMP` | aligns with Table A windows |
| `wiki` | `STRING` | `meta.domain` style domain |
| `edit_count` | `BIGINT` | counts per wiki in that window |
| `rank` | `INT` | optional top-N helper |

Sudipto may query **latest window only** for “top wikis right now,” or **last 24 buckets** for trends.

**Deliverable:** Fill in **`docs/sink-spec.md`** with real database name (`wikidata_analytics` or similar), **exact** `CREATE TABLE` stubs, and **location** (`hdfs://.../warehouse/...`) once known.

---

## Sushil — responsibilities and task list

### Scope

Source → **Kafka producer** (already in repo) → **Spark Structured Streaming** → **Hive** (managed tables or external Parquet + Hive DDL).

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
| D1 | Export sample query file **`sql/sample_queries.sql`** for Sudipto | Example `SELECT` for charts 1–3 |
| D2 | Joint test: producer + Spark + Hive + Sudipto reads | Row counts move over 15–30 min demo |

### Optional bonus (course +2)

| # | Task |
|---|------|
| E1 | Static CSV on **HDFS**; **broadcast join** in Spark before Hive write |
| E2 | Extra Hive columns from enrichment documented in **`sink-spec`** |

---

## Sudipto — responsibilities and task list

### Scope

Assume **Hive holds authoritative aggregates** produced by Sushil. Sudipto’s visualization does **not** read Kafka directly unless course explicitly requires it.

### Phase A — Schema and connectivity

| # | Task | Done when |
|---|------|-----------|
| T1 | Review **`docs/sink-spec.md`** with Sushil; request changes **before** heavy Spark work | Locked column list |
| T2 | Choose stack: **Streamlit**, **Grafana**, **Superset**, **Tableau**, etc. | Tool installs documented |
| T3 | Connect to Hive: **HiveServer2 / JDBC**, **Spark Thrift**, or **periodic CSV export** from agreed path | Test query returns rows |

### Phase B — Dashboard build (parallel while Sushil finishes Spark)

| # | Task | Done when |
|---|------|-----------|
| T4 | **Mock dashboard** using static CSV matching **`sink-spec`** | Charts render with fake data |
| T5 | **Chart 1 — Throughput:** line chart from Table A (`window_start`, `edit_count`) | Updates when new rows appear |
| T6 | **Chart 2 — Top wikis:** bar chart from Table B (latest window or sliding) | Clear labels (`wiki`) |
| T7 | **Chart 3 — Bot vs human or event mix:** use `bot_edit_count` / totals or extra columns | Matches Spark logic |

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
| Hive/HBase services not started in lab | Both follow instructor steps; Sushil verifies `SHOW TABLES` before promising sink |
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
