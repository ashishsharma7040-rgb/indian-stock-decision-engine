from __future__ import annotations

import os
import signal
import time
import json
import urllib.request

from live_feed import live_feed
from redis_state import status as redis_status


RUNNING = True


def _stop(_: int, __: object) -> None:
    global RUNNING
    RUNNING = False


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    symbols = [item.strip().upper() for item in os.getenv("FEED_SYMBOLS", "").split(",") if item.strip()]
    if not symbols:
        try:
            base_url = os.getenv("ENGINE_API_BASE", "http://127.0.0.1:8000").rstrip("/")
            with urllib.request.urlopen(f"{base_url}/api/v1/dashboard/focus?scan_limit=50", timeout=10) as response:
                focus = json.loads(response.read().decode("utf-8"))
            buckets = focus.get("focus", {})
            focus_symbols = [
                item.get("symbol")
                for bucket in ("triggered", "stalking", "watchlist")
                for item in buckets.get(bucket, [])
                if item.get("symbol")
            ]
            symbols = list(dict.fromkeys(str(item).upper() for item in focus_symbols))[:50]
        except Exception as exc:
            print(f"[feed_worker] focus fetch failed: {exc}, using fallback symbols")
    if not symbols:
        symbols = ["NIFTY", "RELIANCE", "HDFCBANK", "POLYCAB", "KPITTECH", "DIXON", "TATAPOWER", "SUZLON"]
    print({"worker": "feed_worker", "redis": redis_status(), "symbols": symbols})
    live_feed.start()
    while RUNNING:
        status = live_feed.status()
        if status.get("feed_open"):
            live_feed.subscribe(symbols)
            time.sleep(30)
        elif status.get("configured"):
            live_feed.start()
            time.sleep(10)
        else:
            print({"worker": "feed_worker", "status": status})
            time.sleep(60)


if __name__ == "__main__":
    main()
