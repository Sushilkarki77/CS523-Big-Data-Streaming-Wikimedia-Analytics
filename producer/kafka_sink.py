"""Kafka producer factory."""

from __future__ import annotations

import json

from kafka import KafkaProducer


def make_producer(bootstrap: str) -> KafkaProducer:
    servers = [s.strip() for s in bootstrap.split(",") if s.strip()]
    return KafkaProducer(
        bootstrap_servers=servers,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=10,
        request_timeout_ms=60_000,
        linger_ms=5,
    )
