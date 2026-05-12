# Spark Structured Streaming - Phases 3 and 4

This folder contains the Spark jobs for the final project. They consume the raw Kafka topic produced by `producer/wikimedia_kafka_producer.py`, parse the JSON contract from `docs/kafka-message-contract.md`, and produce live aggregates.

## Job

| File | Purpose |
|------|---------|
| `wiki_recentchange_console.scala` | Scala `spark-shell` script for Phase 3 console validation. |
| `wiki_recentchange_hive.scala` | Scala `spark-shell` script for Phase 4 Hive persistence. |

## What it computes

- **Throughput:** total edit/event count per event-time window.
- **Bot count:** count of records where `bot=true` in the same window.
- **Per-wiki counts:** counts by `wiki` per event-time window, sorted by newest window and highest count in each micro-batch.

Defaults:

| Setting | Default |
|---------|---------|
| Kafka bootstrap | `kafka-server:9092` |
| Kafka topic | `bdt-wikimedia-recentchange` |
| Starting offsets | `latest` |
| Window duration | `5 minutes` |
| Watermark delay | `10 minutes` |
| Trigger interval | `30 seconds` |
| Shuffle partitions | `4` |
| Checkpoint root | `hdfs://localhost:9000/tmp/wiki-pulse/checkpoints/console` |

The Hive job uses `hdfs://localhost:9000/tmp/wiki-pulse/checkpoints/hive` by default.

## Phase 3: Run console output

Start the producer first in another terminal:

```bash
bash scripts/run-producer-docker.sh
```

Then run the Spark console job:

```bash
bash scripts/run-spark-streaming-console.sh
```

Stop with **Ctrl+C**.

## Phase 4: Run Hive output

Create the Hive database and tables:

```bash
bash scripts/create-hive-tables.sh
```

Start the producer in another terminal:

```bash
bash scripts/run-producer-docker.sh
```

Then run the Spark Hive job:

```bash
bash scripts/run-spark-streaming-hive.sh
```

The Hive job writes append-style snapshot rows into:

- `wiki_pulse.wiki_pulse_throughput`
- `wiki_pulse.wiki_pulse_by_wiki`

Read the tables with Hive CLI:

```bash
docker exec cs523bdt-lab bash -lc 'hive -e "SELECT * FROM wiki_pulse.wiki_pulse_throughput ORDER BY batch_written_at DESC LIMIT 10;"'
docker exec cs523bdt-lab bash -lc 'hive -e "SELECT * FROM wiki_pulse.wiki_pulse_by_wiki ORDER BY batch_written_at DESC, edit_count DESC LIMIT 10;"'
```

## Useful overrides

Read existing records from the topic instead of only new records:

```bash
SPARK_STARTING_OFFSETS=earliest bash scripts/run-spark-streaming-console.sh
```

Use shorter windows for a quick demo:

```bash
SPARK_WINDOW_DURATION="1 minute" SPARK_TRIGGER_INTERVAL="15 seconds" bash scripts/run-spark-streaming-console.sh
```

Use more shuffle partitions only if the lab container has enough CPU:

```bash
SPARK_SHUFFLE_PARTITIONS=8 bash scripts/run-spark-streaming-console.sh
```

Reset the console checkpoints if you want to replay offsets:

```bash
docker exec cs523bdt-lab hdfs dfs -rm -r -f /tmp/wiki-pulse/checkpoints/console
```

Reset the Hive job checkpoints if you want to restart the Hive writer from a fresh offset:

```bash
docker exec cs523bdt-lab hdfs dfs -rm -r -f /tmp/wiki-pulse/checkpoints/hive
```

## Notes

- The lab image has Spark 3.1.2 and Scala 2.12. The Kafka source is added at runtime with `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2`.
- HiveServer2 is not required for Phase 4 verification; use Hive CLI inside `cs523bdt-lab`.
