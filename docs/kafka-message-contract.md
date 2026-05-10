# Kafka message contract — Phase 1

## Data source (chosen)

| Field | Value |
|-------|--------|
| Name | **Wikimedia EventStreams — `recentchange`** |
| Protocol | HTTP **Server-Sent Events** (SSE), not WebSocket |
| Public URL | `https://stream.wikimedia.org/v2/stream/recentchange` |
| Documentation | [EventStreams on Wikitech](https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams) |

This stream emits high-volume, near-real-time edit events across Wikimedia wikis. No API key is required. Use polite client behavior: reconnect on disconnect, avoid extra parallel connections.

---

## Kafka topic

| Property | Value |
|----------|--------|
| Topic name | **`bdt-wikimedia-recentchange`** |
| Partitions | **4** |
| Replication factor | **1** (matches single-broker lab setup) |
| Key | Optional UTF-8 string (e.g. `wiki` or `wiki:title`) for co-location by wiki |

Environment variable: **`KAFKA_TOPIC_RAW`** — must match this topic name in `.env`.

---

## Message format

Each Kafka record **value** is a single JSON object (UTF-8). Recommended **key**: wiki domain string.

### Required fields (producer Phase 2)

| Field | Type | Description |
|-------|------|-------------|
| `event_time` | string | Event time in **ISO-8601** UTC (e.g. `2026-05-07T18:30:00.000Z`). Used for Spark event-time windows and watermarks. Prefer the timestamp from the Wikimedia payload when present; otherwise set from server metadata or ingestion time. |
| `ingest_time` | string | When your producer parsed/sent the record (ISO-8601 UTC). |
| `source` | string | Constant: `wikimedia.eventstreams.recentchange`. |
| `schema_version` | string | Contract version, e.g. `1.0`. |
| `wiki` | string | Wiki domain, e.g. `en.wikipedia.org` (from EventStreams `meta.domain`). |
| `title` | string | Page title. |
| `namespace_id` | integer | MediaWiki namespace id. |
| `event_type` | string | EventStreams `type` (e.g. `edit`, `categorize`, `log`). |
| `user` | string or null | Editor username or null if not present. |
| `bot` | boolean | Whether the change is flagged as a bot edit when provided. |
| `minor` | boolean | Minor edit flag when provided. |

### Optional fields

| Field | Type | Description |
|-------|------|-------------|
| `comment` | string or null | Edit summary (may be long; producer may truncate for Kafka size). |
| `meta_uri` | string or null | `meta.uri` from EventStreams for traceability. |

### Spark note

Structured Streaming should parse this JSON with an explicit schema aligned to **`schema_version` `1.0`**. If you evolve the schema later, bump `schema_version` and handle in Spark.

---

## Sample record

See [sample-kafka-message.json](./sample-kafka-message.json) (pretty-printed). For `kafka-console-producer`, send **one JSON object per line** with no line breaks inside the object.

---

## Phase 1 exit checklist

- [x] Source and topic documented (this file).
- [x] Topic created on the lab broker (see `scripts/create-project-topic.sh`).
- [x] `.env.example` updated; copy to `.env` locally (do not commit `.env`).

Phase **2** producer: **`producer/wikimedia_kafka_producer.py`** maps live SSE JSON → this contract.
