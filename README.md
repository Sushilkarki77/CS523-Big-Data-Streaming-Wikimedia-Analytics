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

Use **Python 3.11** if possible — `kafka-python` 2.0.x can fail to import on **Python 3.12** (`kafka.vendor.six`). If you only have 3.12, rely on **`scripts/verify-producer.sh`** (Docker uses Python 3.11) or create a 3.11 venv.

- Omit `--limit` to stream continuously (**Ctrl+C** to stop).
- Ensure **`KAFKA_BOOTSTRAP_SERVERS=kafka-server:9092`** in `.env` when using the hostname fix.

### Verify without local Python (Docker)

Uses a throwaway Python container **on the same Docker network as Kafka** (no hosts file needed):

```bash
bash scripts/verify-producer.sh 5
```

This publishes **5** messages from EventStreams, then prints **5** lines from `kafka-console-consumer`.

### Where to see Kafka messages

In another terminal:

```bash
docker exec kafka-server kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic bdt-wikimedia-recentchange
```

You should see **JSON objects** (one per line) while `wikimedia_kafka_producer.py` is running.

## Documentation index

| Doc | Purpose |
|-----|---------|
| `docs/implementation-plan.md` | Full phased plan |
| `docs/team-parallel-plan.md` | Two-person roles, weekly split, RACI |
| `docs/phase0-inventory.md` | Docker ports and Kafka bootstrap |
| `docs/kafka-message-contract.md` | Phase 1 topic and JSON schema |
