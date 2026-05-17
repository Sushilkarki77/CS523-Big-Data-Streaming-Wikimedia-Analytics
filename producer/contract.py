"""Map raw Wikimedia EventStreams JSON to docs/kafka-message-contract.md."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

from config import COMMENT_MAX_LEN, SCHEMA_VERSION, SOURCE_TAG


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
