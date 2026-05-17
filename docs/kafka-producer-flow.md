# Kafka producer flow

How live Wikipedia edits reach the Kafka topic **`bdt-wikimedia-recentchange`**. This stops at Kafka; Spark consumes the topic in [`spark-streaming-flow.md`](spark-streaming-flow.md).

---

## Overview

```text
Wikimedia EventStreams (HTTP SSE, live)
        │
        ▼  sse.py          — read "data:" lines, parse JSON
        ▼  contract.py     — map to project JSON contract
        ▼  kafka_sink.py   — JSON → bytes, connect to broker
        ▼  runner.py        — loop, send, reconnect on errors
        │
        ▼
Kafka topic: bdt-wikimedia-recentchange
```

**Kafka does not read HTTP.** The Python producer is the bridge from the internet to the topic.

**Run:**

```bash
bash scripts/create-project-topic.sh   # once
bash scripts/run-producer-docker.sh    # stream until Ctrl+C
bash scripts/run-producer-docker.sh 10 # test: 10 messages then exit
```

---

## Launcher vs Python code

| Layer | File | Role |
|-------|------|------|
| Shell | `scripts/run-producer-docker.sh` | Start Python 3.11 in Docker on the same network as `kafka-server` |
| Entry | `producer/wikimedia_kafka_producer.py` | CLI (`--limit`, `--stream-url`), load `.env` |
| Loop | `producer/runner.py` | SSE → contract → `producer.send()` |
| Input | `producer/sse.py` | Live HTTP SSE stream |
| Transform | `producer/contract.py` | Wikimedia shape → stable contract |
| Kafka | `producer/kafka_sink.py` | Create `KafkaProducer` |
| Config | `producer/config.py` | Env vars, logging |

---

## Step-by-step

### 1. Open one long-lived HTTP stream (`sse.py`)

- **URL:** `https://stream.wikimedia.org/v2/stream/recentchange` (or `EVENTSTREAMS_URL` in `.env`)
- **Method:** One `GET` with `Accept: text/event-stream`
- **Behavior:** Connection stays open; Wikimedia pushes new lines as edits happen (not one request per edit)

### 2. Parse each SSE line

Keep lines that start with `data:` and parse the JSON after `data: `.

### 3. Map to contract (`contract.py`)

Wikimedia sends a large, nested JSON object. The producer outputs a **small, fixed schema** for Spark ([`kafka-message-contract.md`](kafka-message-contract.md)).

### 4. Publish to Kafka (`runner.py` + `kafka_sink.py`)

```python
producer.send(topic, key=wiki, value=msg)
fut.get(timeout=30)   # wait until broker acknowledges
```

| Kafka field | Value |
|-------------|--------|
| **Topic** | `KAFKA_TOPIC_RAW` → `bdt-wikimedia-recentchange` |
| **Key** | Wiki domain, e.g. `en.wikipedia.org` |
| **Value** | Contract JSON (UTF-8 bytes) |

Messages **stay on the topic** after Spark reads them (retention deletes them later, not consumption).

---

## Data example: in → out

### A. Raw line from Wikimedia (SSE)

What arrives over the wire (one line of many):

```text
data: {"title":"Apache Kafka","type":"edit","user":"ExampleUser","bot":false,"minor":true,"comment":"ce","namespace":0,"timestamp":1715868312500,"meta":{"domain":"en.wikipedia.org","uri":"https://en.wikipedia.org/wiki/Apache_Kafka","dt":"2026-05-16T14:05:12.500Z"}}
```

After `sse.py`, this is a Python **dict** (`raw`) with fields like `title`, `meta`, `type`, `bot`, etc.

### B. After `map_to_contract(raw)` — what we send to Kafka

```json
{
  "event_time": "2026-05-16T14:05:12.500Z",
  "ingest_time": "2026-05-16T14:05:13.047Z",
  "source": "wikimedia.eventstreams.recentchange",
  "schema_version": "1.0",
  "wiki": "en.wikipedia.org",
  "title": "Apache Kafka",
  "namespace_id": 0,
  "event_type": "edit",
  "user": "ExampleUser",
  "bot": false,
  "minor": true,
  "comment": "ce",
  "meta_uri": "https://en.wikipedia.org/wiki/Apache_Kafka"
}
```

| Contract field | Typical source in Wikimedia `raw` |
|----------------|-----------------------------------|
| `wiki` | `meta.domain` |
| `event_time` | `meta.dt` or `timestamp` |
| `ingest_time` | now (UTC) when producer sends |
| `event_type` | `type` |
| `title`, `user`, `bot`, `minor`, `comment` | same names on `raw` |
| `meta_uri` | `meta.uri` |
| `source`, `schema_version` | constants |

### C. What is stored on Kafka

One **record** per edit:

```text
Topic:     bdt-wikimedia-recentchange
Partition: 0–3 (from hash of key)
Key:       en.wikipedia.org
Value:     <JSON above as UTF-8 bytes>
Offset:    12345 (broker-assigned, per partition)
```

Spark later reads `value`, parses JSON, and uses `event_time` for 5-minute windows.

---

## Field mapping (quick reference)

```text
Wikimedia raw                    Kafka contract
─────────────────────────────────────────────────
meta.domain                  →   wiki
meta.dt / timestamp          →   event_time
(now)                        →   ingest_time
type                         →   event_type
title, user, bot, minor, …   →   same
meta.uri                     →   meta_uri
(constants)                  →   source, schema_version = "1.0"
```

---

## Configuration (`.env`)

| Variable | Example | Purpose |
|----------|---------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka-server:9092` | Broker (inside Docker) |
| `KAFKA_TOPIC_RAW` | `bdt-wikimedia-recentchange` | Topic name |
| `EVENTSTREAMS_URL` | Wikimedia recentchange URL | SSE source |

---

## Reliability

| Situation | Behavior |
|-----------|----------|
| Wikimedia disconnects | `runner.py` logs, sleeps, opens a **new** SSE connection |
| Kafka down | Retry with backoff, reconnect |
| `--limit N` | Stop after N successful publishes (testing) |
| Live stream | Same HTTP connection receives new `data:` lines continuously |

---

## Verify it works

```bash
# Publish 5 messages and exit
bash scripts/run-producer-docker.sh 5

# Read back from Kafka (inside kafka-server)
docker exec kafka-server kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic bdt-wikimedia-recentchange \
  --from-beginning \
  --max-messages 3
```

You should see contract JSON with recent `ingest_time` values.

---

## Related docs

| Doc | Content |
|-----|---------|
| [`kafka-message-contract.md`](kafka-message-contract.md) | Full schema |
| [`producer-wikimedia-kafka.md`](producer-wikimedia-kafka.md) | Module reference |
| [`architecture.md`](architecture.md) | Full pipeline including Spark and dashboard |
