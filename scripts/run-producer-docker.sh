#!/usr/bin/env bash
# Run Wikimedia→Kafka producer inside Docker on the same network as kafka-server.
#
# Usage:
#   bash scripts/run-producer-docker.sh           # stream forever (Ctrl+C to stop)
#   bash scripts/run-producer-docker.sh 500       # stop after 500 messages (testing)
#
# Prerequisites: kafka-server container running. Copy .env.example → .env (optional but recommended).

set -euo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

NET="$(docker inspect kafka-server --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' 2>/dev/null || true)"
if [[ -z "${NET}" ]]; then
  echo "ERROR: kafka-server container not found. Start your Docker stack first."
  exit 1
fi

LIMIT="${1:-}"
if [[ -n "${LIMIT}" ]] && ! [[ "${LIMIT}" =~ ^[0-9]+$ ]]; then
  echo "ERROR: optional first argument must be a non-negative integer (message limit)."
  exit 1
fi

# Do not use `docker --env-file` with host paths: Docker Desktop on Windows fails when the path
# contains spaces (e.g. "New folder"). The repo is mounted at /app; python-dotenv loads `.env` from
# cwd (/app) inside the container automatically.
ENV_ARGS=()
if [[ ! -f "${ROOT}/.env" ]]; then
  ENV_ARGS=(
    -e KAFKA_BOOTSTRAP_SERVERS=kafka-server:9092
    -e KAFKA_TOPIC_RAW="${KAFKA_TOPIC_RAW:-bdt-wikimedia-recentchange}"
    -e EVENTSTREAMS_URL="${EVENTSTREAMS_URL:-https://stream.wikimedia.org/v2/stream/recentchange}"
  )
fi

if [[ -n "${LIMIT}" ]]; then
  echo "Using Docker network: ${NET} — publishing ${LIMIT} messages then exiting."
  INNER="pip install -q -r producer/requirements.txt && exec python producer/wikimedia_kafka_producer.py --limit ${LIMIT}"
else
  echo "Using Docker network: ${NET} — streaming until Ctrl+C (no --limit)."
  INNER="pip install -q -r producer/requirements.txt && exec python producer/wikimedia_kafka_producer.py"
fi

DOCKER_TTY=(-i)
if [[ -t 0 && -t 1 ]]; then
  DOCKER_TTY=(-it)
fi

exec docker run --rm "${DOCKER_TTY[@]}" \
  --label wiki-pulse.project=final-project \
  --label wiki-pulse.component=producer \
  -v "${ROOT}:/app" -w /app \
  --network "${NET}" \
  "${ENV_ARGS[@]}" \
  python:3.11-slim-bookworm \
  bash -c "${INNER}"
