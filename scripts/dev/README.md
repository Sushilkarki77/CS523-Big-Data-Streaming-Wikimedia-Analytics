# Development and smoke-test scripts

Optional helpers — **not used by `start.sh`**.

| Script | Purpose |
|--------|---------|
| `verify-producer.sh` | Publish N messages and consume back from Kafka |
| `run-spark-streaming-console.sh` | Spark metrics to stdout only (no Hive) |
| `phase0-verify.sh` | Course Docker/Kafka health check (`phase0-healthcheck` topic) |
| `send-sample-contract-message.sh` | One-off sample JSON to the project topic |
