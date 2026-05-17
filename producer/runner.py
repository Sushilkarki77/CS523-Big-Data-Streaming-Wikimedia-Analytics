"""Main ingest loop: SSE → contract → Kafka with reconnect/backoff."""

from __future__ import annotations

import time
import urllib.error
from typing import Optional

from kafka.errors import KafkaError

from config import log
from contract import map_to_contract
from kafka_sink import make_producer
from sse import iter_sse_events


def run(stream_url: str, topic: str, bootstrap: str, limit: Optional[int]) -> int:
    produced = 0
    backoff = 2.0
    max_backoff = 60.0

    while True:
        try:
            producer = make_producer(bootstrap)
        except KafkaError as e:
            log.error("Kafka connection failed: %s", e)
            time.sleep(min(backoff, max_backoff))
            backoff = min(backoff * 2, max_backoff)
            continue

        backoff = 2.0
        log.info("Connected to Kafka at %s; topic=%s", bootstrap, topic)

        try:
            for raw in iter_sse_events(stream_url):
                msg = map_to_contract(raw)
                key = msg.get("wiki") or ""
                fut = producer.send(topic, key=key, value=msg)
                fut.get(timeout=30)
                produced += 1
                if produced % 500 == 0:
                    log.info("Published %s messages", produced)
                if limit is not None and produced >= limit:
                    producer.flush()
                    log.info("Reached --limit=%s; exiting.", limit)
                    return 0
        except urllib.error.HTTPError as e:
            log.warning("HTTP error from EventStreams: %s; reconnecting…", e)
        except urllib.error.URLError as e:
            log.warning("Network error reading EventStreams: %s; reconnecting…", e)
        except KafkaError as e:
            log.error("Kafka error: %s; reconnecting producer…", e)
        except Exception as e:
            log.exception("Unexpected error: %s; reconnecting…", e)
        finally:
            try:
                producer.flush(timeout=10)
                producer.close(timeout=10)
            except Exception:
                pass

        time.sleep(min(backoff, max_backoff))
        backoff = min(backoff * 2, max_backoff)
