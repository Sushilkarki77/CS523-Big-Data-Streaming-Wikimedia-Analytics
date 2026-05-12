#!/usr/bin/env python3
"""
Read Wikimedia EventStreams (SSE recentchange) and publish JSON to Kafka per docs/kafka-message-contract.md.
"""
from __future__ import annotations

import argparse
import os
import json
import logging
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional

from dotenv import load_dotenv

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    print("Install dependencies: pip install -r producer/requirements.txt", file=sys.stderr)
    raise

load_dotenv()

DEFAULT_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
SOURCE_TAG = "wikimedia.eventstreams.recentchange"
SCHEMA_VERSION = "1.0"
COMMENT_MAX_LEN = 2048

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("wikimedia_kafka_producer")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def event_time_iso(ev: Dict[str, Any]) -> str:
    meta = ev.get("meta") if isinstance(ev.get("meta"), dict) else {}
    dt = meta.get("dt")
    if isinstance(dt, str) and dt.strip():
        s = dt.strip().replace(" ", "T")
        if s.endswith("Z"):
            return s
        if "+" in s:
            return s.split("+", 1)[0] + "Z"
        return s + "Z" if "T" in s else utc_now_iso()

    ts = ev.get("timestamp")
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    if isinstance(ts, str) and ts.isdigit():
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    return utc_now_iso()


def map_to_contract(raw: Dict[str, Any]) -> Dict[str, Any]:
    meta = raw.get("meta") if isinstance(raw.get("meta"), dict) else {}
    wiki = meta.get("domain")
    if not isinstance(wiki, str) or not wiki:
        wiki = "unknown"

    title = raw.get("title")
    if not isinstance(title, str):
        title = ""

    ns = raw.get("namespace")
    try:
        namespace_id = int(ns) if ns is not None else 0
    except (TypeError, ValueError):
        namespace_id = 0

    et = raw.get("type")
    event_type = et if isinstance(et, str) else "unknown"

    user = raw.get("user")
    if user is not None and not isinstance(user, str):
        user = str(user)

    bot = bool(raw.get("bot")) if raw.get("bot") is not None else False
    minor = bool(raw.get("minor")) if raw.get("minor") is not None else False

    comment = raw.get("comment")
    if isinstance(comment, str) and len(comment) > COMMENT_MAX_LEN:
        comment = comment[:COMMENT_MAX_LEN] + "…"
    elif comment is not None and not isinstance(comment, str):
        comment = str(comment)

    meta_uri = meta.get("uri")
    if meta_uri is not None and not isinstance(meta_uri, str):
        meta_uri = str(meta_uri)

    return {
        "event_time": event_time_iso(raw),
        "ingest_time": utc_now_iso(),
        "source": SOURCE_TAG,
        "schema_version": SCHEMA_VERSION,
        "wiki": wiki,
        "title": title,
        "namespace_id": namespace_id,
        "event_type": event_type,
        "user": user,
        "bot": bot,
        "minor": minor,
        "comment": comment if isinstance(comment, str) else None,
        "meta_uri": meta_uri if isinstance(meta_uri, str) else None,
    }


def iter_sse_events(stream_url: str) -> Iterator[Dict[str, Any]]:
    req = urllib.request.Request(
        stream_url,
        headers={
            "Accept": "text/event-stream",
            "User-Agent": "BDT-FinalProject/1.0 (educational; kafka producer)",
        },
        method="GET",
    )
    buf = b""
    with urllib.request.urlopen(req, timeout=120) as resp:
        while True:
            chunk = resp.read(8192)
            if not chunk:
                break
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                try:
                    text = line.decode("utf-8", errors="replace").strip()
                except Exception:
                    continue
                if not text.startswith("data:"):
                    continue
                payload = text[5:].strip()
                if not payload or payload == "[DONE]":
                    continue
                try:
                    obj = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict):
                    yield obj


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


def main() -> int:
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
        default=os.environ.get("EVENTSTREAMS_URL", DEFAULT_STREAM_URL),
        help="EventStreams SSE URL",
    )
    args = p.parse_args()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    topic = os.environ.get("KAFKA_TOPIC_RAW", "").strip()
    if not bootstrap or not topic:
        log.error("Set KAFKA_BOOTSTRAP_SERVERS and KAFKA_TOPIC_RAW (e.g. via .env)")
        return 1

    return run(args.stream_url, topic, bootstrap, args.limit)


if __name__ == "__main__":
    sys.exit(main())
