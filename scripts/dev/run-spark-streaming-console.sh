#!/usr/bin/env bash
# Run the Phase 3 Spark Structured Streaming console job inside cs523bdt-lab.
#
# Usage:
#   bash scripts/dev/run-spark-streaming-console.sh
#
# Optional environment variables:
#   KAFKA_BOOTSTRAP_SERVERS  default: kafka-server:9092
#   KAFKA_TOPIC_RAW          default: bdt-wikimedia-recentchange
#   SPARK_STARTING_OFFSETS   default: latest
#   SPARK_CHECKPOINT_DIR     default: hdfs://localhost:9000/tmp/wiki-pulse/checkpoints/console
#   SPARK_WINDOW_DURATION    default: 5 minutes
#   SPARK_WATERMARK_DELAY    default: 10 minutes
#   SPARK_TRIGGER_INTERVAL   default: 30 seconds
#   SPARK_SHUFFLE_PARTITIONS default: 4

set -euo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

if [[ -f "${ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT}/.env"
  set +a
fi

if ! docker inspect cs523bdt-lab >/dev/null 2>&1; then
  echo "ERROR: cs523bdt-lab container not found. Start your Docker stack first."
  exit 1
fi

if ! docker inspect kafka-server >/dev/null 2>&1; then
  echo "ERROR: kafka-server container not found. Start your Docker stack first."
  exit 1
fi

APP_SRC="${ROOT}/spark-streaming/dev/wiki_recentchange_console.scala"
APP_DEST_DIR="/tmp/final-project/spark-streaming"
APP_DEST="${APP_DEST_DIR}/wiki_recentchange_console.scala"

if [[ ! -f "${APP_SRC}" ]]; then
  echo "ERROR: Spark script not found: ${APP_SRC}"
  exit 1
fi

KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka-server:9092}"
KAFKA_TOPIC_RAW="${KAFKA_TOPIC_RAW:-bdt-wikimedia-recentchange}"
SPARK_STARTING_OFFSETS="${SPARK_STARTING_OFFSETS:-latest}"
SPARK_CHECKPOINT_DIR="${SPARK_CHECKPOINT_DIR:-hdfs://localhost:9000/tmp/wiki-pulse/checkpoints/console}"
SPARK_WINDOW_DURATION="${SPARK_WINDOW_DURATION:-5 minutes}"
SPARK_WATERMARK_DELAY="${SPARK_WATERMARK_DELAY:-10 minutes}"
SPARK_TRIGGER_INTERVAL="${SPARK_TRIGGER_INTERVAL:-30 seconds}"
SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-4}"
SPARK_KAFKA_PACKAGE="${SPARK_KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2}"

echo "Copying Spark job into cs523bdt-lab: ${APP_DEST}"
docker exec -i cs523bdt-lab bash -lc "mkdir -p '${APP_DEST_DIR}' && cat > '${APP_DEST}'" < "${APP_SRC}"

echo "Starting Spark Structured Streaming console job..."
echo "  topic: ${KAFKA_TOPIC_RAW}"
echo "  bootstrap: ${KAFKA_BOOTSTRAP_SERVERS}"
echo "  startingOffsets: ${SPARK_STARTING_OFFSETS}"
echo "  checkpoint: ${SPARK_CHECKPOINT_DIR}"

DOCKER_TTY=(-i)
if [[ -t 0 && -t 1 ]]; then
  DOCKER_TTY=(-it)
fi

exec docker exec "${DOCKER_TTY[@]}" \
  -e KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS}" \
  -e KAFKA_TOPIC_RAW="${KAFKA_TOPIC_RAW}" \
  -e SPARK_STARTING_OFFSETS="${SPARK_STARTING_OFFSETS}" \
  -e SPARK_CHECKPOINT_DIR="${SPARK_CHECKPOINT_DIR}" \
  -e SPARK_WINDOW_DURATION="${SPARK_WINDOW_DURATION}" \
  -e SPARK_WATERMARK_DELAY="${SPARK_WATERMARK_DELAY}" \
  -e SPARK_TRIGGER_INTERVAL="${SPARK_TRIGGER_INTERVAL}" \
  -e SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS}" \
  cs523bdt-lab bash -lc \
  "spark-shell --master local[2] --packages '${SPARK_KAFKA_PACKAGE}' --conf spark.sql.session.timeZone=UTC -i '${APP_DEST}'"
