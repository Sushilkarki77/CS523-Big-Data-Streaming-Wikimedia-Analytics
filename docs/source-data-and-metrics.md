# Source data and output metrics

This document explains what the project reads from Wikimedia and what we produce from it with Kafka and Spark.

## High-level flow

```text
Wikimedia recentchange stream
        |
        v
Python Kafka producer
        |
        v
Kafka topic: bdt-wikimedia-recentchange
        |
        v
Spark Structured Streaming
        |
        v
Hive-readable Parquet tables
        |
        v
CSV snapshots -> Node API -> React dashboard
```

## What we read

The source is Wikimedia EventStreams, specifically the public `recentchange` stream:

```text
https://stream.wikimedia.org/v2/stream/recentchange
```

This stream emits near-real-time events whenever public activity happens on Wikimedia projects. That includes activity from Wikipedia, Wikidata, Wikimedia Commons, Wiktionary, and other Wikimedia sites.

Examples of source activity:

- A Wikipedia page is edited.
- A new page is created.
- A page is moved.
- A Wikidata item is updated.
- A bot performs an automated update.
- A log or category-related event happens.

In simple terms, each event means:

```text
Something changed on a public Wikimedia project.
```

## What a Wikimedia project means

A Wikimedia project is one public website in the Wikimedia ecosystem.

Examples:

- `en.wikipedia.org` means English Wikipedia.
- `commons.wikimedia.org` means Wikimedia Commons.
- `www.wikidata.org` means Wikidata.
- `en.wiktionary.org` means English Wiktionary.
- `meta.wikimedia.org` means Wikimedia Meta-Wiki.

In our Kafka messages, this value is stored in the `wiki` field.

## What the producer keeps

The raw Wikimedia event contains many fields. Our producer maps it into a smaller JSON contract that downstream Spark code can rely on.

Important fields we keep:

- `event_time`: when the Wikimedia event happened.
- `ingest_time`: when our producer handled the event.
- `wiki`: which Wikimedia project emitted the event.
- `title`: page or item title.
- `namespace_id`: MediaWiki namespace id.
- `event_type`: event category, such as edit, log, or categorize.
- `user`: public username/IP value when present.
- `bot`: whether the event is marked as bot activity.
- `minor`: whether it is marked as a minor edit.
- `comment`: public edit/log summary when present.
- `schema_version`: our contract version, currently `1.0`.

Simplified Kafka message example:

```json
{
  "event_time": "2026-05-12T00:28:10.123Z",
  "ingest_time": "2026-05-12T00:28:11.002Z",
  "source": "wikimedia.eventstreams.recentchange",
  "schema_version": "1.0",
  "wiki": "en.wikipedia.org",
  "title": "Apache Kafka",
  "namespace_id": 0,
  "event_type": "edit",
  "user": "ExampleUser",
  "bot": false,
  "minor": true,
  "comment": "Fix typo",
  "meta_uri": "https://en.wikipedia.org/wiki/Apache_Kafka"
}
```

## What Spark produces

Spark reads the Kafka topic continuously and turns raw event records into time-windowed metrics. The console job prints these metrics for Phase 3 validation; the Hive job writes the same metrics, plus the bonus enrichment metric, into Hive-readable Parquet table locations.

## Metric 1: Throughput over time

Purpose: show how much Wikimedia activity is happening per time window.

Output columns:

- `window_start`: start of the event-time window.
- `window_end`: end of the event-time window.
- `edit_count`: total events in that window.
- `bot_edit_count`: events in that window where `bot=true`.
- `batch_written_at`: when Spark processed/wrote the batch.

Example output:

```text
window_start          window_end            edit_count  bot_edit_count
2026-05-12 00:28:00   2026-05-12 00:29:00   1205        732
```

Dashboard idea:

- Line chart of `edit_count` over time.
- Bot vs total activity chart using `bot_edit_count` and `edit_count`.

## Metric 2: Top Wikimedia projects

Purpose: show which Wikimedia projects are most active in each time window.

Output columns:

- `window_start`: start of the event-time window.
- `window_end`: end of the event-time window.
- `wiki`: Wikimedia project domain.
- `edit_count`: number of events from that project in the window.
- `batch_written_at`: when Spark wrote this snapshot row to Hive.

Example output:

```text
window_start          wiki                    edit_count  batch_written_at
2026-05-12 00:28:00   commons.wikimedia.org   458         2026-05-12 00:28:50
2026-05-12 00:28:00   www.wikidata.org        172         2026-05-12 00:28:50
2026-05-12 00:28:00   en.wikipedia.org        81          2026-05-12 00:28:50
```

Dashboard idea:

- Bar chart of top Wikimedia projects by activity.
- Latest-window ranking of most active projects.

## Bonus enrichment: project family lookup

For the bonus requirement, Spark also reads a static CSV from HDFS:

```text
hdfs://localhost:9000/tmp/wiki-pulse/static/wiki_project_lookup.csv
```

Source file in the repo:

```text
static-data/wiki_project_lookup.csv
```

The CSV maps `wiki` domains to metadata:

- `project_family`: Wikipedia, Wikidata, Commons, Wiktionary, etc.
- `language`: language code or `multilingual`.
- `region`: broad region label for demo-friendly grouping.

Spark broadcasts this static lookup and joins it with the streaming events on `wiki`.

## Metric 3: Project families

Purpose: show the result of the static HDFS CSV join by grouping enriched stream records.

Output columns:

- `window_start`: start of the event-time window.
- `window_end`: end of the event-time window.
- `project_family`: value from the static CSV lookup, or `Other` when not matched.
- `edit_count`: number of events for that family in the window.
- `batch_written_at`: when Spark wrote this snapshot row to Hive.

Example output:

```text
window_start          project_family  edit_count  batch_written_at
2026-05-12 00:28:00   Wikipedia       720         2026-05-12 00:28:50
2026-05-12 00:28:00   Commons         286         2026-05-12 00:28:50
2026-05-12 00:28:00   Wikidata        172         2026-05-12 00:28:50
```

## Why these metrics are useful

These metrics answer simple, demo-friendly questions:

- How active is Wikimedia right now?
- Is activity increasing or decreasing over time?
- Which Wikimedia projects are generating the most events?
- Which high-level project family is most active after enrichment?
- How much of the activity is from bots?

They also align with the course pipeline:

- Kafka stores the raw stream.
- Spark performs streaming transformations, aggregations, and the bonus static-data join.
- Hive will store processed summary tables.
- The exporter snapshots Hive rows into CSV files.
- The Node API reads those CSV snapshots.
- The React dashboard polls the Node API.

## Current implementation

Relevant files:

- `producer/wikimedia_kafka_producer.py`: reads Wikimedia SSE and writes Kafka JSON.
- `docs/kafka-message-contract.md`: defines the Kafka message schema.
- `spark-streaming/dev/wiki_recentchange_console.scala`: optional console validation (no Hive).
- `spark-streaming/wiki_recentchange_hive/`: writes Hive metrics and performs the bonus HDFS CSV join (Scala fragments assembled by `scripts/run-spark-streaming-hive.sh`).
- `static-data/wiki_project_lookup.csv`: static lookup uploaded to HDFS for the bonus join.
- `sql/hive/create_wiki_pulse_tables.sql`: Hive table DDL.
- `scripts/export-hive-dashboard-data.sh`: exports Hive results to dashboard CSV snapshots.
- `dashboard-react/backend/`: Node API reading CSV snapshots.
- `dashboard-react/frontend/`: React dashboard.
- `docs/sink-spec.md`: Hive table contract.
