# Wiki recentchange → Hive (Spark shell fragments)

Phase 4 job is split into ordered fragments for easier review. At runtime, `scripts/run-spark-streaming-hive.sh` concatenates them into a single file inside `cs523bdt-lab` and runs `spark-shell -i ...` on that combined script.

| Order | File | Contents |
|-------|------|------------|
| 1 | `01_imports_config.scala` | Imports, Spark session tuning, env-based paths, startup banner |
| 2 | `02_lookup.scala` | Static HDFS CSV lookup (`wikiLookup`) |
| 3 | `03_kafka_parse.scala` | Kafka stream, JSON schema, parse, watermark, enrichment join |
| 4 | `04_aggregates.scala` | Windowed aggregates for throughput, by-wiki, by project family |
| 5 | `05_writes.scala` | `foreachBatch` Parquet writes to Hive warehouse paths + `awaitAnyTermination` |

Do not reorder files without checking dependencies: each stage uses `val`s from earlier fragments.
