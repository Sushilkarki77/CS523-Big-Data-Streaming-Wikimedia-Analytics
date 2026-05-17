#!/usr/bin/env bash
# Sends one JSON message matching docs/kafka-message-contract.md (single line).
# Use this to verify consumers before the real producer (Phase 2) runs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=../lib/common.sh
source "${SCRIPT_DIR}/../lib/common.sh"
wiki_pulse_platform_init

TOPIC="${KAFKA_TOPIC_RAW:-bdt-wikimedia-recentchange}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_INTERNAL:-localhost:9092}"

# Minified JSON — one object per line for Kafka
MSG='{"event_time":"2026-05-07T18:30:00.000Z","ingest_time":"2026-05-07T18:30:05.000Z","source":"wikimedia.eventstreams.recentchange","schema_version":"1.0","wiki":"en.wikipedia.org","title":"Apache Kafka","namespace_id":0,"event_type":"edit","user":"Phase1SmokeTest","bot":false,"minor":true,"comment":"contract verification","meta_uri":null}'

docker exec kafka-server sh -c "echo '$MSG' | kafka-console-producer --bootstrap-server $BOOTSTRAP --topic $TOPIC"

echo "Sent 1 message to topic: $TOPIC"
echo "Tail with:"
echo "  docker exec kafka-server kafka-console-consumer --bootstrap-server $BOOTSTRAP --topic $TOPIC --from-beginning --max-messages 1 --timeout-ms 15000"
