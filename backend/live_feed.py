from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

try:
    import pyotp
except ImportError:  # pragma: no cover - optional until Shoonya is configured
    pyotp = None

try:
    from api_helper import ShoonyaApiPy as ShoonyaApiClass
except ImportError:  # pragma: no cover - depends on installed package variant
    try:
        from NorenRestApiPy.NorenApi import NorenApi as ShoonyaApiClass
    except ImportError:  # pragma: no cover
        ShoonyaApiClass = None


ORDER_METHODS = {
    "place_order",
    "modify_order",
    "cancel_order",
    "exit_order",
    "product_conversion",
    "position_product_conversion",
}


class LiveFeedManager:
    """Market-data-only Shoonya WebSocket bridge.

    This class deliberately exposes only quote/search/subscribe behavior. It never
    places, modifies, cancels, exits, or converts orders.
    """

    def __init__(self) -> None:
        self._api: Any = None
        self._lock = threading.RLock()
        self._started = False
        self._feed_open = False
        self._status = "not_configured"
        self._last_error: str | None = None
        self._latest: dict[str, dict[str, Any]] = {}
        self._token_to_symbol: dict[str, str] = {}
        self._symbol_to_token: dict[str, str] = {}
        self._subscribed_tokens: set[str] = set()

    @property
    def configured(self) -> bool:
        required = [
            "SHOONYA_USER_ID",
            "SHOONYA_PASSWORD",
            "SHOONYA_VENDOR_CODE",
            "SHOONYA_API_KEY",
            "SHOONYA_IMEI",
        ]
        has_twofa = bool(os.getenv("SHOONYA_TWOFA") or os.getenv("SHOONYA_TOTP_SECRET"))
        return all(os.getenv(key) for key in required) and has_twofa

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "provider": "shoonya",
                "mode": os.getenv("BROKER_MODE", "market_data_only"),
                "configured": self.configured,
                "sdk_available": ShoonyaApiClass is not None,
                "started": self._started,
                "feed_open": self._feed_open,
                "status": self._status,
                "last_error": self._last_error,
                "subscribed_symbols": sorted(self._symbol_to_token),
                "trading_enabled": os.getenv("ENABLE_TRADING", "false").lower() == "true",
            }

    def _twofa(self) -> str:
        manual = os.getenv("SHOONYA_TWOFA")
        if manual:
            return manual
        secret = os.getenv("SHOONYA_TOTP_SECRET")
        if not secret:
            raise RuntimeError("Set SHOONYA_TWOFA or SHOONYA_TOTP_SECRET")
        if pyotp is None:
            raise RuntimeError("pyotp is required for SHOONYA_TOTP_SECRET")
        return pyotp.TOTP(secret).now()

    def _disable_order_methods(self, api: Any) -> None:
        def disabled_order_method(*_: Any, **__: Any) -> None:
            raise RuntimeError("Trading is disabled. This app is market-data-only.")

        for name in ORDER_METHODS:
            if hasattr(api, name):
                setattr(api, name, disabled_order_method)

    def start(self) -> None:
        if os.getenv("ENABLE_TRADING", "false").lower() == "true":
            raise RuntimeError("ENABLE_TRADING must remain false for this market-data-only app")
        if not self.configured:
            self._status = "not_configured"
            return
        if ShoonyaApiClass is None:
            self._status = "sdk_missing"
            self._last_error = "Install NorenRestApiPy and pyotp"
            return
        with self._lock:
            if self._started:
                return
            self._started = True
            self._status = "starting"

        thread = threading.Thread(target=self._run, name="shoonya-live-feed", daemon=True)
        thread.start()

    def _run(self) -> None:
        try:
            api = ShoonyaApiClass()
            self._disable_order_methods(api)
            ret = api.login(
                userid=os.environ["SHOONYA_USER_ID"],
                password=os.environ["SHOONYA_PASSWORD"],
                twoFA=self._twofa(),
                vendor_code=os.environ["SHOONYA_VENDOR_CODE"],
                api_secret=os.environ["SHOONYA_API_KEY"],
                imei=os.environ["SHOONYA_IMEI"],
            )
            if not ret or ret.get("stat") != "Ok":
                raise RuntimeError(f"Shoonya login failed: {ret.get('emsg') if isinstance(ret, dict) else ret}")
            with self._lock:
                self._api = api
                self._status = "logged_in"

            api.start_websocket(
                subscribe_callback=self._on_tick,
                order_update_callback=self._ignore_order_update,
                socket_open_callback=self._on_open,
                socket_close_callback=self._on_close,
            )
            deadline = time.time() + 20
            while time.time() < deadline and not self._feed_open:
                time.sleep(0.2)
            if not self._feed_open:
                raise RuntimeError("Shoonya WebSocket did not open within 20 seconds")
        except Exception as exc:  # pragma: no cover - broker/network dependent
            with self._lock:
                self._status = "error"
                self._last_error = str(exc)
                self._started = False
                self._feed_open = False

    def _on_open(self) -> None:
        with self._lock:
            self._feed_open = True
            self._status = "live"

    def _on_close(self) -> None:
        with self._lock:
            self._feed_open = False
            self._status = "closed"
            self._started = False

    def _ignore_order_update(self, _: dict[str, Any]) -> None:
        return

    def _on_tick(self, tick: dict[str, Any]) -> None:
        token = str(tick.get("tk") or "")
        if not token:
            return
        with self._lock:
            symbol = self._token_to_symbol.get(token) or tick.get("ts") or token
            existing = self._latest.get(symbol, {})
            merged = {**existing, **tick}
            live = {
                "symbol": symbol,
                "exchange": tick.get("e") or merged.get("e") or "NSE",
                "token": token,
                "trading_symbol": tick.get("ts") or merged.get("ts"),
                "ltp": self._num(merged.get("lp")),
                "change_pct": self._num(merged.get("pc")),
                "open": self._num(merged.get("o")),
                "high": self._num(merged.get("h")),
                "low": self._num(merged.get("l")),
                "close": self._num(merged.get("c")),
                "avg_price": self._num(merged.get("ap")),
                "volume": self._int(merged.get("v")),
                "bid": self._num(merged.get("bp1")),
                "ask": self._num(merged.get("sp1")),
                "source": "Shoonya WebSocket",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            self._latest[symbol] = live

    def subscribe(self, symbols: list[str]) -> dict[str, Any]:
        self.start()
        if not symbols:
            return self.status()
        tokens: list[str] = []
        unresolved: list[str] = []
        with self._lock:
            api = self._api
        if api is None or not self._feed_open:
            return {**self.status(), "unresolved": symbols}

        for symbol in [item.upper().strip() for item in symbols if item.strip()]:
            token = self._symbol_to_token.get(symbol)
            if not token:
                token = self._resolve_token(api, symbol)
            if token:
                key = f"NSE|{token}"
                tokens.append(key)
                with self._lock:
                    self._subscribed_tokens.add(key)
            else:
                unresolved.append(symbol)
        if tokens:
            api.subscribe(tokens)
        return {**self.status(), "subscribed": tokens, "unresolved": unresolved}

    def _resolve_token(self, api: Any, symbol: str) -> str | None:
        ret = api.searchscrip(exchange="NSE", searchtext=symbol)
        values = ret.get("values", []) if isinstance(ret, dict) else []
        target = f"{symbol}-EQ"
        match = next((row for row in values if str(row.get("tsym", "")).upper() == target), None)
        if match is None:
            match = next((row for row in values if str(row.get("tsym", "")).upper().startswith(symbol)), None)
        if not match:
            return None
        token = str(match["token"])
        with self._lock:
            self._symbol_to_token[symbol] = token
            self._token_to_symbol[token] = symbol
        return token

    def snapshot(self, symbols: list[str] | None = None) -> dict[str, dict[str, Any]]:
        with self._lock:
            if not symbols:
                return dict(self._latest)
            wanted = {symbol.upper() for symbol in symbols}
            return {symbol: tick for symbol, tick in self._latest.items() if symbol.upper() in wanted}

    @staticmethod
    def _num(value: Any) -> float | None:
        try:
            return None if value in {None, ""} else float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _int(value: Any) -> int | None:
        try:
            return None if value in {None, ""} else int(float(value))
        except (TypeError, ValueError):
            return None


live_feed = LiveFeedManager()
