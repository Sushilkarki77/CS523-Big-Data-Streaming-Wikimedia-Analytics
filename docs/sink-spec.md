# Sink specification — Hive (draft for Sushil + Sudipto)

**Status:** Draft — simple non-partitioned Hive approach selected for Phase 4.

Update this file whenever **table names**, **columns**, or refresh expectations change.

---

## Hive database

| Property | Value |
|----------|--------|
| Database name | `wiki_pulse` |
| Location root | Hive default warehouse location |
| Partitioning | None for the first implementation |

---

## Table 1 — Throughput time series

**Purpose:** Line charts (edits per minute / per 5-minute window).

**Table name:** `wiki_pulse_throughput`

| Column | Spark/Hive type | Nullable | Description |
|--------|-----------------|----------|-------------|
| `window_start` | `TIMESTAMP` | N | Start of tumbling window (event time) |
| `window_end` | `TIMESTAMP` | Y | End of window (optional) |
| `edit_count` | `BIGINT` | N | Total edits in window |
| `bot_edit_count` | `BIGINT` | Y | Edits with `bot=true` |
| `batch_written_at` | `TIMESTAMP` | N | Processing-time when Spark wrote the row |

**Partition:** none. Keep this table simple for Phase 4; dashboard queries can sort/filter by `window_start`.

**DDL file:** `sql/hive/wiki_pulse_throughput.sql` *(create when finalized)*

---

## Table 2 — Per-wiki counts (rankings)

**Purpose:** Bar charts — top wikis per window.

**Table name:** `wiki_pulse_by_wiki`

| Column | Spark/Hive type | Nullable | Description |
|--------|-----------------|----------|-------------|
| `window_start` | `TIMESTAMP` | N | Aligns with Table 1 |
| `window_end` | `TIMESTAMP` | Y | End of tumbling window |
| `wiki` | `STRING` | N | Domain, e.g. `en.wikipedia.org` |
| `edit_count` | `BIGINT` | N | Edits for that wiki in window |
| `batch_written_at` | `TIMESTAMP` | N | Processing-time when Spark wrote this snapshot row |

**Partition:** none. Keep this table simple for Phase 4; dashboard queries can filter by latest `window_start`.

**DDL file:** `sql/hive/wiki_pulse_by_wiki.sql` *(create when finalized)*

---

## Sample queries for visualization (Sudipto)

```sql
-- Latest throughput rows (adjust database/table names)
-- SELECT * FROM wiki_pulse.wiki_pulse_throughput ORDER BY window_start DESC LIMIT 48;

-- Top wikis in latest written batch
-- SELECT wiki, edit_count FROM wiki_pulse.wiki_pulse_by_wiki WHERE batch_written_at = (SELECT MAX(batch_written_at) FROM wiki_pulse.wiki_pulse_by_wiki) ORDER BY edit_count DESC LIMIT 15;
```

These queries assume the simple non-partitioned Hive tables above.

---

## Sample CSVs (mock Hive rows for Sudipto)

Until Spark writes to Hive, use the files in **`sample-data/`**:

- **`sample-data/wiki_pulse_throughput_sample.csv`** — same columns as Table 1  
- **`sample-data/wiki_pulse_by_wiki_sample.csv`** — same columns as Table 2  

See **`sample-data/README.md`** for chart hints and a small **pandas** load example.

---

## Sign-off

| Developer | Role | Date | Notes |
|-----------|------|------|--------|
| Sushil | Spark → Hive | | |
| Sudipto | Hive → Viz | | |
