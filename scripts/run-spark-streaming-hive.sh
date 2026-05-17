#!/usr/bin/env bash
# Run the Phase 4 Spark Structured Streaming -> Hive job inside cs523bdt-lab.
#
# Usage:
#   bash scripts/run-spark-streaming-hive.sh
#
# Optional environment variables:
#   KAFKA_BOOTSTRAP_SERVERS  default: kafka-server:9092
#   KAFKA_TOPIC_RAW          default: bdt-wikimedia-recentchange
#   SPARK_STARTING_OFFSETS   default: latest
#   SPARK_CHECKPOINT_DIR     default: hdfs://localhost:9000/tmp/wiki-pulse/checkpoints/hive
#   SPARK_WINDOW_DURATION    default: 5 minutes
#   SPARK_WATERMARK_DELAY    default: 10 minutes
#   SPARK_TRIGGER_INTERVAL   default: 30 seconds
#   SPARK_SHUFFLE_PARTITIONS default: 4
#   HIVE_DATABASE            default: wiki_pulse
#   HIVE_THROUGHPUT_PATH     default: hdfs://localhost:9000/user/hive/warehouse/wiki_pulse.db/wiki_pulse_throughput
#   HIVE_BY_WIKI_PATH        default: hdfs://localhost:9000/user/hive/warehouse/wiki_pulse.db/wiki_pulse_by_wiki
#   HIVE_BY_PROJECT_FAMILY_PATH default: hdfs://localhost:9000/user/hive/warehouse/wiki_pulse.db/wiki_pulse_by_project_family
#   STATIC_WIKI_LOOKUP_PATH  default: hdfs://localhost:9000/tmp/wiki-pulse/static/wiki_project_lookup.csv
#   SKIP_HDFS_REPAIR_CHECK   default: false; set to 1 to skip auto-repair

set -euo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
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

HIVE_JOB_DIR="${ROOT}/spark-streaming/wiki_recentchange_hive"
APP_DEST_DIR="/tmp/final-project/spark-streaming"
APP_DEST="${APP_DEST_DIR}/wiki_recentchange_hive.scala"
HIVE_PARTS=(
  01_imports_config.scala
  02_lookup.scala
  03_kafka_parse.scala
  04_aggregates.scala
  05_writes.scala
)

for part in "${HIVE_PARTS[@]}"; do
  fragment="${HIVE_JOB_DIR}/${part}"
  if [[ ! -f "${fragment}" ]]; then
    echo "ERROR: Spark Hive job fragment not found: ${fragment}"
    exit 1
  fi
done

KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka-server:9092}"
KAFKA_TOPIC_RAW="${KAFKA_TOPIC_RAW:-bdt-wikimedia-recentchange}"
SPARK_STARTING_OFFSETS="${SPARK_STARTING_OFFSETS:-latest}"
SPARK_CHECKPOINT_DIR="${SPARK_CHECKPOINT_DIR:-hdfs://localhost:9000/tmp/wiki-pulse/checkpoints/hive}"
SPARK_WINDOW_DURATION="${SPARK_WINDOW_DURATION:-5 minutes}"
SPARK_WATERMARK_DELAY="${SPARK_WATERMARK_DELAY:-10 minutes}"
SPARK_TRIGGER_INTERVAL="${SPARK_TRIGGER_INTERVAL:-30 seconds}"
SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-4}"
HIVE_DATABASE="${HIVE_DATABASE:-wiki_pulse}"
HIVE_THROUGHPUT_PATH="${HIVE_THROUGHPUT_PATH:-hdfs://localhost:9000/user/hive/warehouse/wiki_pulse.db/wiki_pulse_throughput}"
HIVE_BY_WIKI_PATH="${HIVE_BY_WIKI_PATH:-hdfs://localhost:9000/user/hive/warehouse/wiki_pulse.db/wiki_pulse_by_wiki}"
HIVE_BY_PROJECT_FAMILY_PATH="${HIVE_BY_PROJECT_FAMILY_PATH:-hdfs://localhost:9000/user/hive/warehouse/wiki_pulse.db/wiki_pulse_by_project_family}"
STATIC_WIKI_LOOKUP_PATH="${STATIC_WIKI_LOOKUP_PATH:-hdfs://localhost:9000/tmp/wiki-pulse/static/wiki_project_lookup.csv}"
SPARK_KAFKA_PACKAGE="${SPARK_KAFKA_PACKAGE:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2}"

if [[ "${SKIP_HDFS_REPAIR_CHECK:-0}" != "1" ]]; then
  echo "Checking generated HDFS state before Spark starts..."
  bash scripts/repair-hdfs-state.sh --auto
fi

echo "Ensuring Hive tables exist..."
bash scripts/create-hive-tables.sh

echo "Ensuring static wiki lookup exists on HDFS..."
bash scripts/upload-static-wiki-lookup.sh

echo "Assembling Spark Hive job from ${HIVE_JOB_DIR} into cs523bdt-lab: ${APP_DEST}"
{
  for part in "${HIVE_PARTS[@]}"; do
    cat "${HIVE_JOB_DIR}/${part}"
    printf '\n'
  done
} | docker exec -i cs523bdt-lab bash -lc "mkdir -p '${APP_DEST_DIR}' && cat > '${APP_DEST}'"

echo "Starting Spark Structured Streaming Hive job..."
echo "  topic: ${KAFKA_TOPIC_RAW}"
echo "  bootstrap: ${KAFKA_BOOTSTRAP_SERVERS}"
echo "  startingOffsets: ${SPARK_STARTING_OFFSETS}"
echo "  checkpoint: ${SPARK_CHECKPOINT_DIR}"
echo "  hive database: ${HIVE_DATABASE}"
echo "  throughput path: ${HIVE_THROUGHPUT_PATH}"
echo "  by-wiki path: ${HIVE_BY_WIKI_PATH}"
echo "  by-project-family path: ${HIVE_BY_PROJECT_FAMILY_PATH}"
echo "  static lookup path: ${STATIC_WIKI_LOOKUP_PATH}"

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
  -e HIVE_DATABASE="${HIVE_DATABASE}" \
  -e HIVE_THROUGHPUT_PATH="${HIVE_THROUGHPUT_PATH}" \
  -e HIVE_BY_WIKI_PATH="${HIVE_BY_WIKI_PATH}" \
  -e HIVE_BY_PROJECT_FAMILY_PATH="${HIVE_BY_PROJECT_FAMILY_PATH}" \
  -e STATIC_WIKI_LOOKUP_PATH="${STATIC_WIKI_LOOKUP_PATH}" \
  cs523bdt-lab bash -lc \
  "spark-shell --master local[2] --packages '${SPARK_KAFKA_PACKAGE}' --conf spark.sql.session.timeZone=UTC -i '${APP_DEST}'"
