#!/usr/bin/env bash
# Runs the Wikimedia→Kafka producer with --limit against Docker Kafka (same network as kafka-server).
# Prerequisites: Docker running, kafka-server container up; optional .env with KAFKA_* (passed into container).

set -euo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

NET="$(docker inspect kafka-server --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' 2>/dev/null || true)"
if [[ -z "${NET}" ]]; then
  echo "ERROR: kafka-server container not found. Start your Docker stack first."
  exit 1
fi

LIMIT="${1:-5}"

echo "Using Docker network: ${NET}"
echo "Publishing ${LIMIT} messages then consuming up to ${LIMIT} from topic..."

docker run --rm \
  -v "${ROOT}:/app" -w /app \
  --network "${NET}" \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka-server:9092 \
  -e KAFKA_TOPIC_RAW="${KAFKA_TOPIC_RAW:-bdt-wikimedia-recentchange}" \
  -e EVENTSTREAMS_URL="${EVENTSTREAMS_URL:-https://stream.wikimedia.org/v2/stream/recentchange}" \
  python:3.11-slim-bookworm \
  bash -c "pip install -q -r producer/requirements.txt && python producer/wikimedia_kafka_producer.py --limit ${LIMIT}"

echo ""
echo "Consuming from Kafka (inside kafka-server container):"
docker exec kafka-server kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic "${KAFKA_TOPIC_RAW:-bdt-wikimedia-recentchange}" \
  --from-beginning \
  --max-messages "${LIMIT}" \
  --timeout-ms 120000

echo ""
echo "OK — If you saw JSON lines above, Phase 2 producer path works."
