#!/usr/bin/env bash
# Creates the Phase 1 project topic on the Kafka broker container (idempotent).
# Requires: Docker, running container named kafka-server.

set -euo pipefail
export MSYS_NO_PATHCONV=1

TOPIC="${KAFKA_TOPIC_RAW:-bdt-wikimedia-recentchange}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_INTERNAL:-localhost:9092}"

docker exec kafka-server kafka-topics --bootstrap-server "$BOOTSTRAP" \
  --create --topic "$TOPIC" --partitions 4 --replication-factor 1 --if-not-exists

echo "Topic ready: $TOPIC"
docker exec kafka-server kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"
