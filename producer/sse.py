"""Read Wikimedia EventStreams over HTTP Server-Sent Events."""

from __future__ import annotations

import json
import urllib.request
from typing import Any, Dict, Iterator


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
