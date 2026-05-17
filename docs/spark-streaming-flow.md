# Spark Structured Streaming flow

How messages on Kafka become **Hive tables** on HDFS. This starts where the [Kafka producer flow](kafka-producer-flow.md) ends.

**Run (with producer already running):**

```bash
bash scripts/run-spark-streaming-hive.sh
```

---

## Overview

```text
Kafka topic: bdt-wikimedia-recentchange
        │
        ▼  03_kafka_parse.scala   — readStream, parse JSON, watermark, join lookup
        ▼  04_aggregates.scala     — 5-minute windows, counts
        ▼  05_writes.scala        — append Parquet to Hive table paths
        │
        ▼
Hive (wiki_pulse) on HDFS — 3 tables
```

Spark **subscribes** to Kafka; it does not read Wikimedia HTTP. The producer must be running (or the topic must already have data if using `SPARK_STARTING_OFFSETS=earliest`).

---

## Launcher vs Scala job

| Layer | What | Role |
|-------|------|------|
| Shell | `scripts/run-spark-streaming-hive.sh` | Repair HDFS, create Hive tables, upload lookup CSV, **concatenate** Scala files, start `spark-shell` |
| Config | `01_imports_config.scala` | Read env vars (Kafka, windows, HDFS paths) |
| Lookup | `02_lookup.scala` | Load static wiki → `project_family` CSV from HDFS |
| Ingest | `03_kafka_parse.scala` | Kafka → parsed, watermarked, enriched events |
| Aggregate | `04_aggregates.scala` | Windowed counts (3 streams) |
| Write | `05_writes.scala` | Three streaming queries → Parquet append |

At runtime, the five `.scala` files are merged into one script inside `cs523bdt-lab` and executed with `spark-shell -i` (see [run-spark-streaming-hive.md](run-spark-streaming-hive.md)).

---

## Step-by-step

### 0. Prerequisites (shell script, before Spark)

`run-spark-streaming-hive.sh` runs:

1. `repair-hdfs-state.sh --auto` — fix corrupt checkpoints/warehouse if needed  
2. `create-hive-tables.sh` — register tables in Hive metastore  
3. `upload-static-wiki-lookup.sh` — CSV on HDFS for the join  

### 1. Subscribe to Kafka (`03_kafka_parse.scala`)

```scala
spark.readStream
  .format("kafka")
  .option("subscribe", topic)              // bdt-wikimedia-recentchange
  .option("startingOffsets", "latest")     // default: only new messages after start
```

Every **~30 seconds** (trigger), Spark pulls **new** records since the last checkpoint offset.

Parse `value` as JSON → filter `schema_version = 1.0` → `event_ts` from `event_time` → **watermark** (default 10 min late data).

### 2. Enrich with static lookup (`02_lookup` + `03`)

Broadcast-join `wiki` to `wiki_project_lookup.csv` → add `project_family` (default `"Other"` if unknown).

**Output stream:** `enrichedEvents` — one row per edit.

### 3. Aggregate in event-time windows (`04_aggregates.scala`)

Default window: **5 minutes** on `event_ts`.

| Stream | Group by | Metrics |
|--------|----------|---------|
| `throughput` | window | `edit_count`, `bot_edit_count` |
| `byWiki` | window + `wiki` | `edit_count` |
| `byProjectFamily` | window + `project_family` | `edit_count` |

Spark adds `window_start` / `window_end` from `window(event_ts, "5 minutes")`.

### 4. Write to Hive (`05_writes.scala`)

Three independent `writeStream` jobs. Each micro-batch:

1. Adds `batch_written_at` (when Spark wrote the row)  
2. **Appends** Parquet files to the table’s HDFS folder  
3. Logs e.g. `Batch 5 -> wiki_pulse.wiki_pulse_throughput: appended 2 rows`  

Hive metastore points at those paths; Hive CLI can `SELECT` without HiveServer2.

**Checkpoints** (offsets + query state) live under:

```text
hdfs://localhost:9000/tmp/wiki-pulse/checkpoints/hive/throughput
hdfs://localhost:9000/tmp/wiki-pulse/checkpoints/hive/by-wiki
hdfs://localhost:9000/tmp/wiki-pulse/checkpoints/hive/by-project-family
```

---

## Data example: in → out

### A. One message **in** from Kafka (value JSON)

What the producer wrote (same as [kafka-producer-flow.md](kafka-producer-flow.md)):

```json
{
  "event_time": "2026-05-16T14:05:12.500Z",
  "ingest_time": "2026-05-16T14:05:13.047Z",
  "schema_version": "1.0",
  "wiki": "en.wikipedia.org",
  "title": "Apache Kafka",
  "namespace_id": 0,
  "event_type": "edit",
  "user": "ExampleUser",
  "bot": false,
  "minor": true,
  "comment": "ce",
  "meta_uri": "https://en.wikipedia.org/wiki/Apache_Kafka",
  "source": "wikimedia.eventstreams.recentchange"
}
```

Spark parses this into columns, sets `event_ts`, joins lookup → `project_family: "Wikipedia"`.

Many such rows in the same 5-minute window are **counted together**.

### B. What gets **written** to Hive (after aggregation)

**Table `wiki_pulse.wiki_pulse_throughput`** (one row per window update in a batch):

| window_start | window_end | edit_count | bot_edit_count | batch_written_at |
|--------------|------------|------------|----------------|------------------|
| 2026-05-16 14:05:00 | 2026-05-16 14:10:00 | 847 | 23 | 2026-05-16 14:06:30 |

**Table `wiki_pulse.wiki_pulse_by_wiki`** (example rows):

| window_start | window_end | wiki | edit_count | batch_written_at |
|--------------|------------|------|------------|------------------|
| 2026-05-16 14:05:00 | 2026-05-16 14:10:00 | en.wikipedia.org | 312 | 2026-05-16 14:06:30 |
| 2026-05-16 14:05:00 | 2026-05-16 14:10:00 | de.wikipedia.org | 89 | 2026-05-16 14:06:30 |

**Table `wiki_pulse.wiki_pulse_by_project_family`**:

| window_start | window_end | project_family | edit_count | batch_written_at |
|--------------|------------|----------------|------------|------------------|
| 2026-05-16 14:05:00 | 2026-05-16 14:10:00 | Wikipedia | 520 | 2026-05-16 14:06:30 |
| 2026-05-16 14:05:00 | 2026-05-16 14:10:00 | Wiktionary | 41 | 2026-05-16 14:06:30 |

Writes are **append-only**; the same window may appear again in a later batch with updated counts until the window is finalized.

Physical files: Parquet under `/user/hive/warehouse/wiki_pulse.db/<table_name>/` on HDFS.

### C. What Spark does **not** write

- Raw per-edit rows (only **aggregates** go to Hive)  
- Individual `title`, `user`, `comment` (dropped after counting; not in Hive schema)

---

## Three Hive tables (output)

| Hive table | Contents |
|------------|----------|
| `wiki_pulse.wiki_pulse_throughput` | Total edits + bot edits per 5-min window |
| `wiki_pulse.wiki_pulse_by_wiki` | Edits per wiki per window |
| `wiki_pulse.wiki_pulse_by_project_family` | Edits per project family per window (bonus) |

DDL: `sql/hive/create_wiki_pulse_tables.sql`

---

## Configuration (from shell → env → Scala)

| Variable | Default | Purpose |
|----------|---------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka-server:9092` | Broker |
| `KAFKA_TOPIC_RAW` | `bdt-wikimedia-recentchange` | Topic |
| `SPARK_STARTING_OFFSETS` | `latest` | Only new data after job start |
| `SPARK_WINDOW_DURATION` | `5 minutes` | Aggregation window |
| `SPARK_WATERMARK_DELAY` | `10 minutes` | Late-event allowance |
| `SPARK_TRIGGER_INTERVAL` | `30 seconds` | Micro-batch frequency |
| `SPARK_CHECKPOINT_DIR` | `.../checkpoints/hive` | Fault tolerance |
| `HIVE_*_PATH` | warehouse paths | Where Parquet is appended |
| `STATIC_WIKI_LOOKUP_PATH` | HDFS CSV | Wiki metadata join |

---

## End-to-end with producer

```text
Terminal 1:  bash scripts/run-producer-docker.sh
             → Kafka topic fills with contract JSON

Terminal 2:  bash scripts/run-spark-streaming-hive.sh
             → read new offsets every 30s
             → aggregate
             → append to 3 Hive tables

Terminal 3:  bash scripts/export-hive-dashboard-data.sh
             → Hive SQL → CSV → dashboard
```

**Order matters** with `startingOffsets=latest`: start producer before or with Spark.

---

## Verify

```bash
# Hive row counts growing
docker exec cs523bdt-lab bash -lc \
  "hive -e 'SELECT COUNT(*) FROM wiki_pulse.wiki_pulse_throughput;'"

# Recent throughput rows
docker exec cs523bdt-lab bash -lc \
  "hive -e 'SELECT * FROM wiki_pulse.wiki_pulse_throughput ORDER BY batch_written_at DESC LIMIT 5;'"
```

Spark terminal should show `Batch N -> wiki_pulse... appended X rows`.

---

## Related docs

| Doc | Content |
|-----|---------|
| [kafka-producer-flow.md](kafka-producer-flow.md) | SSE → Kafka |
| [run-spark-streaming-hive.md](run-spark-streaming-hive.md) | Launcher details |
| [kafka-message-contract.md](kafka-message-contract.md) | Input JSON schema |
| [sink-spec.md](sink-spec.md) | Hive table contract |
| [architecture.md](architecture.md) | Full pipeline |
