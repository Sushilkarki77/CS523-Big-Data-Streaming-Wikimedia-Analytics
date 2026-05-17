#!/usr/bin/env python3
"""
Read Wikimedia EventStreams (SSE recentchange) and publish JSON to Kafka
per docs/kafka-message-contract.md.

Modules:
  config.py      — env vars and logging
  contract.py    — raw event → Kafka JSON contract
  sse.py         — EventStreams HTTP reader
  kafka_sink.py  — KafkaProducer factory
  runner.py      — reconnect loop and publish
"""
from __future__ import annotations

import argparse
import sys

try:
    from kafka.errors import KafkaError  # noqa: F401 — import check
except ImportError:
    print("Install dependencies: pip install -r producer/requirements.txt", file=sys.stderr)
    raise

from config import configure_logging, kafka_settings, log, stream_url_from_env
from runner import run


def main() -> int:
    configure_logging()

    p = argparse.ArgumentParser(description="Wikimedia EventStreams → Kafka producer")
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Stop after publishing N messages (for testing). Default: run forever.",
    )
    p.add_argument(
        "--stream-url",
        default=stream_url_from_env(),
        help="EventStreams SSE URL",
    )
    args = p.parse_args()

    bootstrap, topic = kafka_settings()
    if not bootstrap or not topic:
        log.error("Set KAFKA_BOOTSTRAP_SERVERS and KAFKA_TOPIC_RAW (e.g. via .env)")
        return 1

    return run(args.stream_url, topic, bootstrap, args.limit)


if __name__ == "__main__":
    sys.exit(main())
