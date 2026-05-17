# Current end-to-end flow

This is the implemented demo flow as of the current project state.

## Runtime pipeline

```text
Wikimedia EventStreams recentchange
        |
        v
producer/wikimedia_kafka_producer.py
        |
        v
Kafka topic: bdt-wikimedia-recentchange
        |
        v
Spark Structured Streaming in cs523bdt-lab
        |
        +-- static HDFS CSV lookup:
        |   /tmp/wiki-pulse/static/wiki_project_lookup.csv
        |
        v
Hive-readable Parquet summary tables
        |
        v
scripts/export-hive-dashboard-data.sh
        |
        v
dashboard-react/backend/data/*.csv
        |
        v
Node/Express API
        |
        v
React dashboard
```

## One-command project startup

After the course Docker stack is already running:

```bash
bash scripts/start-demo.sh
```

The script starts the long-running project processes in the background and writes logs under `.demo/logs/`.

Stop them with:

```bash
bash scripts/stop-demo.sh
```

## Main services and scripts

| Layer | File / command |
|-------|----------------|
| Kafka topic setup | `scripts/create-project-topic.sh` |
| Producer | `scripts/run-producer-docker.sh` |
| Static HDFS lookup upload | `scripts/upload-static-wiki-lookup.sh` |
| Hive table creation | `scripts/create-hive-tables.sh` |
| Spark console validation | `scripts/run-spark-streaming-console.sh` |
| Spark Hive writer + bonus join | `scripts/run-spark-streaming-hive.sh` |
| Hive to CSV snapshots | `scripts/export-hive-dashboard-data.sh` |
| Node API | `dashboard-react/backend` |
| React dashboard | `dashboard-react/frontend` |

## Hive outputs

Database: `wiki_pulse`

| Table | Purpose |
|-------|---------|
| `wiki_pulse_throughput` | Event throughput and bot count per time window |
| `wiki_pulse_by_wiki` | Top Wikimedia projects per time window |
| `wiki_pulse_by_project_family` | Bonus output from the HDFS static CSV join |

## Dashboard outputs

The React dashboard displays:

- latest event count
- bot count and human count
- top Wikimedia project
- top project family from the bonus join
- throughput line chart
- top wiki bar chart
- project-family bonus chart

## Bonus requirement

The bonus is implemented by:

1. Storing `static-data/wiki_project_lookup.csv` in HDFS.
2. Reading it in Spark from `hdfs://localhost:9000/tmp/wiki-pulse/static/wiki_project_lookup.csv`.
3. Broadcasting it and joining stream records on `wiki`.
4. Writing the enriched aggregate to `wiki_pulse_by_project_family`.
5. Exporting that table to `project_family_latest.csv` for the dashboard.
