# Spark Structured Streaming

Spark jobs consume the raw Kafka topic from `producer/wikimedia_kafka_producer.py`, parse the JSON contract in `docs/kafka-message-contract.md`, and compute live aggregates.

## Jobs

| Path | Purpose |
|------|---------|
| `wiki_recentchange_hive/` | **Production pipeline** — Hive Parquet writes; assembled at runtime by `scripts/run-spark-streaming-hive.sh` |
| `dev/wiki_recentchange_console.scala` | Optional console validation (no Hive); see `dev/README.md` |

## Metrics

- **Throughput:** event count per event-time window
- **Bot count:** records with `bot=true` per window
- **Per-wiki counts:** by `wiki` per window
- **Project families (bonus):** broadcast join to `static-data/wiki_project_lookup.csv` on HDFS

## Run the Hive job

```bash
bash scripts/run-producer-docker.sh          # terminal 1
bash scripts/run-spark-streaming-hive.sh     # terminal 2
```

Setup: `bash scripts/setup.sh` (topic, lookup upload, Hive DDL).

Full launcher walkthrough: [`docs/run-spark-streaming-hive.md`](../docs/run-spark-streaming-hive.md).

Hive tables:

- `wiki_pulse.wiki_pulse_throughput`
- `wiki_pulse.wiki_pulse_by_wiki`
- `wiki_pulse.wiki_pulse_by_project_family`

## Run the console job (optional)

```bash
bash scripts/dev/run-spark-streaming-console.sh
```

## Defaults (Hive job)

| Setting | Default |
|---------|---------|
| Kafka bootstrap | `kafka-server:9092` |
| Kafka topic | `bdt-wikimedia-recentchange` |
| Starting offsets | `latest` |
| Window | `5 minutes` |
| Watermark | `10 minutes` |
| Trigger | `30 seconds` |
| Checkpoint | `hdfs://localhost:9000/tmp/wiki-pulse/checkpoints/hive` |
| Static lookup | `hdfs://localhost:9000/tmp/wiki-pulse/static/wiki_project_lookup.csv` |

Override via environment variables (see `scripts/run-spark-streaming-hive.sh`).

## Reset checkpoints

```bash
bash scripts/repair-hdfs-state.sh --reset
bash scripts/setup.sh
```

Or manually:

```bash
docker exec cs523bdt-lab hdfs dfs -rm -r -f /tmp/wiki-pulse/checkpoints/hive
```

## Notes

- Lab image: Spark 3.1.2, Scala 2.12; Kafka connector via `--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2`
- Hive CLI inside `cs523bdt-lab` is enough for verification (no HiveServer2 required)
