#!/usr/bin/env bash
# Phase 0 regression checks. Run from Git Bash on Windows.
# Uses MSYS_NO_PATHCONV so Docker exec paths are not rewritten.

set -euo pipefail
export MSYS_NO_PATHCONV=1

echo "== Docker containers (expect kafka-server, zookeeper-server, cs523bdt-lab, hive-metastore-db) =="
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Ports}}\t{{.Status}}"

echo ""
echo "== Kafka: list topics =="
docker exec kafka-server kafka-topics --bootstrap-server localhost:9092 --list || true

echo ""
echo "== Kafka: ensure phase0-healthcheck topic exists =="
docker exec kafka-server kafka-topics --bootstrap-server localhost:9092 \
  --create --topic phase0-healthcheck --partitions 3 --replication-factor 1 --if-not-exists

echo ""
echo "== Kafka: produce one message =="
docker exec kafka-server sh -c 'echo "phase0-verify-$(date +%s)" | kafka-console-producer --bootstrap-server localhost:9092 --topic phase0-healthcheck'

echo ""
echo "== Kafka: consume latest (non-blocking tail) =="
docker exec kafka-server kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic phase0-healthcheck --from-beginning --max-messages 1 --timeout-ms 15000

echo ""
echo "== Host TCP check to Kafka port =="
if bash -c 'echo >/dev/tcp/127.0.0.1/9092' 2>/dev/null; then
  echo "OK: 127.0.0.1:9092 accepts TCP"
else
  echo "WARN: could not open TCP to 127.0.0.1:9092 (firewall or Kafka down)"
fi

echo ""
echo "== HDFS / YARN HTTP probe =="
curl -s -o /dev/null -w "9870 NameNode HTTP: %{http_code}\n" --connect-timeout 3 http://127.0.0.1:9870/ || true
curl -s -o /dev/null -w "8088 YARN RM HTTP: %{http_code}\n" --connect-timeout 3 http://127.0.0.1:8088/ || true

echo ""
echo "Phase 0 script finished. See docs/archive/phase0-inventory.md for advertised listener / hosts notes."
