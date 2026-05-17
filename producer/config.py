"""Environment defaults and logging setup."""

from __future__ import annotations

import logging
import os

from dotenv import load_dotenv

load_dotenv()

DEFAULT_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
SOURCE_TAG = "wikimedia.eventstreams.recentchange"
SCHEMA_VERSION = "1.0"
COMMENT_MAX_LEN = 2048

log = logging.getLogger("wikimedia_kafka_producer")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )


def kafka_settings() -> tuple[str, str]:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "").strip()
    topic = os.environ.get("KAFKA_TOPIC_RAW", "").strip()
    return bootstrap, topic


def stream_url_from_env() -> str:
    return os.environ.get("EVENTSTREAMS_URL", DEFAULT_STREAM_URL)
