# Big Data Technologies — final project pipeline

End-to-end pipeline: **public stream → Kafka → Spark Structured Streaming → Hive/HBase → dashboard** (see `docs/implementation-plan.md`).

## Prerequisites

- Docker stack running: `kafka-server`, `zookeeper-server`, `cs523bdt-lab`, `hive-metastore-db` (see `docs/phase0-inventory.md`).
- On **Windows**, if you run Kafka clients on the host using hostname `kafka-server`, add to `C:\Windows\System32\drivers\etc\hosts`:

  ```text
  127.0.0.1 kafka-server
  ```

## Phase 1 — Data source and Kafka contract (done)

| Item | Detail |
|------|--------|
| Source | Wikimedia EventStreams `recentchange` |
| Topic | `bdt-wikimedia-recentchange` |
| Contract | `docs/kafka-message-contract.md` |
| Sample JSON | `docs/sample-kafka-message.json` |

### Configuration

```bash
cp .env.example .env
# Edit .env if you change the topic name (default matches Phase 1).
```

### Create the topic (idempotent)

```bash
bash scripts/create-project-topic.sh
```

### Where to see output

Phase 1 wires **topic + documentation**. You can confirm everything in these places:

1. **Topic exists** — list and describe:

   ```bash
   docker exec kafka-server kafka-topics --bootstrap-server localhost:9092 --list
   docker exec kafka-server kafka-topics --bootstrap-server localhost:9092 --describe --topic bdt-wikimedia-recentchange
   ```

2. **One test message on the wire** — send a sample line matching the contract, then read it back:

   ```bash
   bash scripts/send-sample-contract-message.sh
   docker exec kafka-server kafka-console-consumer --bootstrap-server localhost:9092 \
     --topic bdt-wikimedia-recentchange --from-beginning --max-messages 3 --timeout-ms 20000
   ```

   You should see at least the smoke-test JSON (and any earlier samples). **Ctrl+C** is not needed when `--max-messages` is set; the consumer exits after reading that many messages.

3. **Continuous live traffic** — arrives after **Phase 2** when the Wikimedia SSE producer runs; use the same `kafka-console-consumer` **without** `--max-messages` to watch the stream.

### Troubleshooting: `TimeoutException` / `Processed a total of 0 messages`

`kafka-console-consumer --max-messages N` **waits until it has read N records**. If the topic has **fewer than N** messages (often **zero**), it keeps polling until **`--timeout-ms`** expires, then exits with a timeout and **0 processed**.

**Fix:** Produce data first, or lower `--max-messages` to what actually exists.

```bash
# Put at least one message on the topic, then consume
bash scripts/send-sample-contract-message.sh
docker exec kafka-server kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic bdt-wikimedia-recentchange \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 20000
```

**Note:** `LEADER_NOT_AVAILABLE` right after creating a topic or restarting Kafka usually clears after a short wait; run `kafka-topics --describe` and confirm `Leader` is not `-1`.

## Phase 2 — Wikimedia → Kafka producer (done)

| Item | Detail |
|------|--------|
| Script | `producer/wikimedia_kafka_producer.py` |
| Dependencies | `producer/requirements.txt` (`kafka-python`, `python-dotenv`) |

### Run on your machine (requires hosts entry + `.env`)

From the **project root**, after `cp .env.example .env` and `127.0.0.1 kafka-server` in your hosts file:

```bash
pip install -r producer/requirements.txt
python producer/wikimedia_kafka_producer.py --limit 10
```

**Python version:** use **3.11.x**. The package **`kafka-python`** does **not** work reliably on **Python 3.12, 3.13, or 3.14** — you may see:

`ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`

**Fix (pick one):**

1. **Install Python 3.11** from [python.org](https://www.python.org/downloads/), then create a venv and install deps:
   ```powershell
   py -3.11 -m venv .venv
   .venv\Scripts\activate
   python -m pip install -r producer/requirements.txt
   python producer/wikimedia_kafka_producer.py --limit 10
   ```
2. **Skip local Python** and use **`bash scripts/verify-producer.sh`** (Docker runs Python 3.11 — no `kafka-python` on your Windows Python needed).

- Omit `--limit` to stream continuously (**Ctrl+C** to stop).
- Ensure **`KAFKA_BOOTSTRAP_SERVERS=kafka-server:9092`** in `.env` when using the hostname fix.

### Docker scripts (no local Python 3.11)

Both use a **Python 3.11** image on the **`kafka-server` Docker network** (no Windows hosts entry needed for **`kafka-server:9092`**).

| Script | Command | Behavior |
|--------|---------|----------|
| Smoke test | `bash scripts/verify-producer.sh` | Publishes **5** messages (default), then consumes **5** via `kafka-console-consumer`. |
| Smoke test (custom **N**) | `bash scripts/verify-producer.sh 500` | **`--limit`** and consumer **`max-messages`** = **N**. |
| Continuous producer | `bash scripts/run-producer-docker.sh` | No default limit — streams until **Ctrl+C**. |
| Limited run in Docker | `bash scripts/run-producer-docker.sh 100` | Same as **`--limit 100`** for testing. |

**`.env` in Docker:** the repo is mounted at **`/app`**. The producer’s **`load_dotenv()`** reads **`.env`** from the working directory inside the container. **`run-producer-docker.sh`** does not use **`docker --env-file`** with a host path (that breaks on **Docker Desktop for Windows** when the project path contains **spaces**). If **`.env`** is absent, the continuous-run script passes default **`-e`** values for Kafka and the stream URL.

Details: **`docs/producer-wikimedia-kafka.md`** (Running with Docker).

### Where to see Kafka messages

In another terminal:

```bash
docker exec kafka-server kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic bdt-wikimedia-recentchange
```

You should see **JSON objects** (one per line) while `wikimedia_kafka_producer.py` is running.

## Phase 3 — Spark Structured Streaming (starter)

The first Spark job consumes the raw Kafka topic, parses the JSON contract, and prints live window aggregates to the console.

Start the producer first:

```bash
bash scripts/run-producer-docker.sh
```

Then run Spark inside `cs523bdt-lab`:

```bash
bash scripts/run-spark-streaming-console.sh
```

The job uses:

- Kafka topic `bdt-wikimedia-recentchange`
- Spark Kafka package `org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2`
- Event-time windows from `event_time`
- Watermarking and checkpointing at `hdfs://localhost:9000/tmp/wiki-pulse/checkpoints/console`
- Console output for throughput and per-wiki aggregates

See `spark-streaming/README.md` for overrides such as `SPARK_STARTING_OFFSETS=earliest` and shorter demo windows.

## Phase 4 — Spark to Hive (starter)

HiveServer2 is not required for this phase. The project uses Hive CLI inside `cs523bdt-lab` to create and query simple non-partitioned Hive tables.

Create the Hive database and tables:

```bash
bash scripts/create-hive-tables.sh
```

Upload the static lookup CSV to HDFS for the bonus Spark join:

```bash
bash scripts/upload-static-wiki-lookup.sh
```

Run the Hive-writing Spark job:

```bash
bash scripts/run-spark-streaming-hive.sh
```

Query the latest rows:

```bash
docker exec cs523bdt-lab bash -lc 'hive -e "SELECT * FROM wiki_pulse.wiki_pulse_throughput ORDER BY batch_written_at DESC LIMIT 10;"'
docker exec cs523bdt-lab bash -lc 'hive -e "SELECT * FROM wiki_pulse.wiki_pulse_by_wiki ORDER BY batch_written_at DESC, edit_count DESC LIMIT 10;"'
docker exec cs523bdt-lab bash -lc 'hive -e "SELECT * FROM wiki_pulse.wiki_pulse_by_project_family ORDER BY batch_written_at DESC, edit_count DESC LIMIT 10;"'
```

The Hive schema lives in `sql/hive/create_wiki_pulse_tables.sql`.
The bonus static lookup lives in `static-data/wiki_project_lookup.csv` and is uploaded to HDFS at `/tmp/wiki-pulse/static/wiki_project_lookup.csv`.

## Phase 5 — React dashboard (starter)

The custom dashboard uses Hive as the source, exports latest Hive query results to CSV snapshots, serves them through a Node API, and renders charts in React.

Export Hive data to dashboard CSVs:

```bash
bash scripts/export-hive-dashboard-data.sh
```

Run the API:

```bash
cd dashboard-react/backend
npm install
npm run dev
```

Run the React app:

```bash
cd dashboard-react/frontend
npm install
npm run dev
```

For a live demo, keep the exporter running in a loop:

```bash
while true; do bash scripts/export-hive-dashboard-data.sh; sleep 30; done
```

See `dashboard-react/README.md` for the full run order.

## Documentation index

| Doc | Purpose |
|-----|---------|
| `docs/implementation-plan.md` | Full phased plan |
| `docs/two-developer-plan-sushil-viz.md` | **Sushil:** source→Kafka→Spark→Hive · **Sudipto:** Hive→dashboard |
| `docs/sink-spec.md` | Hive table/column contract (fill together) |
| `sample-data/` | **CSV mocks** matching sink-spec (`README.md` inside) — for Sudipto before Hive is live |
| `docs/team-parallel-plan.md` | Generic two-person roles, RACI |
| `docs/phase0-inventory.md` | Docker ports and Kafka bootstrap |
| `docs/kafka-message-contract.md` | Phase 1 topic and JSON schema |
| `docs/source-data-and-metrics.md` | Explains what Wikimedia data we read and which Spark metrics we output |
| `docs/producer-wikimedia-kafka.md` | **`wikimedia_kafka_producer.py`** functions and flow diagram |
| `spark-streaming/README.md` | Phase 3 console job and Phase 4 Hive job |
| `sql/hive/create_wiki_pulse_tables.sql` | Hive DDL for Phase 4 summary tables |
| `dashboard-react/README.md` | Phase 5 React dashboard and Node API |
| `static-data/wiki_project_lookup.csv` | Static HDFS CSV lookup used for the bonus Spark join |
