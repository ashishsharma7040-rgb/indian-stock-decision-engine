from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from typing import Any, Iterator

try:
    import redis
except ImportError:  # pragma: no cover - optional dependency
    redis = None


REDIS_URL = os.getenv("REDIS_URL", "")
TICK_CHANNEL = os.getenv("REDIS_TICK_CHANNEL", "market:ticks")
TICK_HASH = os.getenv("REDIS_TICK_HASH", "market:latest_ticks")
_CLIENT: Any | None = None
_CLIENT_LOCK = threading.RLock()


def enabled() -> bool:
    return bool(REDIS_URL and redis is not None)


def client() -> Any | None:
    global _CLIENT
    if not enabled():
        return None
    with _CLIENT_LOCK:
        if _CLIENT is None:
            _CLIENT = redis.Redis.from_url(
                REDIS_URL,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                health_check_interval=30,
            )
        return _CLIENT


def status() -> dict[str, Any]:
    if not REDIS_URL:
        return {"enabled": False, "configured": False, "reason": "REDIS_URL not set"}
    if redis is None:
        return {"enabled": False, "configured": True, "reason": "redis package not installed"}
    try:
        r = client()
        assert r is not None
        pong = r.ping()
        return {"enabled": True, "configured": True, "ping": bool(pong), "channel": TICK_CHANNEL, "hash": TICK_HASH}
    except Exception as exc:  # pragma: no cover - network dependent
        return {"enabled": False, "configured": True, "reason": str(exc), "channel": TICK_CHANNEL, "hash": TICK_HASH}


def publish_tick(tick: dict[str, Any]) -> None:
    r = client()
    if r is None:
        return
    payload = {**tick, "redis_updated_at": datetime.now(timezone.utc).isoformat()}
    symbol = str(payload.get("symbol") or "").upper()
    if not symbol:
        return
    raw = json.dumps(payload, separators=(",", ":"), default=str)
    r.hset(TICK_HASH, symbol, raw)
    r.publish(TICK_CHANNEL, raw)


def latest_ticks(symbols: list[str] | None = None) -> dict[str, dict[str, Any]]:
    r = client()
    if r is None:
        return {}
    if symbols:
        wanted = [symbol.upper() for symbol in symbols]
        values = r.hmget(TICK_HASH, wanted)
        pairs = zip(wanted, values)
    else:
        pairs = r.hgetall(TICK_HASH).items()
    ticks: dict[str, dict[str, Any]] = {}
    for symbol, raw in pairs:
        if not raw:
            continue
        try:
            ticks[symbol.upper()] = json.loads(raw)
        except json.JSONDecodeError:
            continue
    return ticks


def subscribe_ticks() -> Iterator[dict[str, Any]]:
    r = client()
    if r is None:
        return iter(())
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(TICK_CHANNEL)
    for message in pubsub.listen():
        if message.get("type") != "message":
            continue
        try:
            yield json.loads(message.get("data") or "{}")
        except json.JSONDecodeError:
            continue
