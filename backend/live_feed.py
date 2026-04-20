from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from typing import Any

import redis_state

try:
    import pyotp
except ImportError:  # pragma: no cover - optional until Shoonya is configured
    pyotp = None

ORDER_METHODS = {
    "place_order",
    "modify_order",
    "cancel_order",
    "exit_order",
    "product_conversion",
    "position_product_conversion",
}


def load_shoonya_api_class() -> Any:
    try:
        from api_helper import ShoonyaApiPy

        return ShoonyaApiPy
    except ImportError:
        from NorenRestApiPy.NorenApi import NorenApi

        class ShoonyaApiPy(NorenApi):
            def __init__(self) -> None:
                kwargs = {
                    "host": os.getenv("SHOONYA_HOST", "https://api.shoonya.com/NorenWClientTP"),
                    "websocket": os.getenv("SHOONYA_WEBSOCKET", "wss://api.shoonya.com/NorenWSTP/"),
                }
                try:
                    super().__init__(**kwargs)
                except TypeError:
                    # Older NorenRestApiPy builds can have a narrower constructor.
                    # The market-data methods still work after setting endpoints.
                    super().__init__()
                    self.host = kwargs["host"]
                    self.websocket = kwargs["websocket"]

        return ShoonyaApiPy


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
        self._sdk_available: bool | None = None
        self._runtime_twofa: str | None = None
        self._runtime_twofa_set_at: str | None = None
        self._latest: dict[str, dict[str, Any]] = {}
        self._token_to_symbol: dict[str, str] = {}
        self._symbol_to_token: dict[str, str] = {}
        self._subscribed_tokens: set[str] = set()

    def _env(self, *names: str) -> str | None:
        for name in names:
            value = os.getenv(name)
            if value:
                return value
        return None

    def _credential_values(self) -> dict[str, str | None]:
        return {
            "user_id": self._env("SHOONYA_USER_ID", "SHOONYA_UID", "SHOONYA_CLIENT_ID", "SHOONYA_USER"),
            "password": self._env("SHOONYA_PASSWORD", "SHOONYA_PWD"),
            "vendor_code": self._env("SHOONYA_VENDOR_CODE", "SHOONYA_VC"),
            "api_key": self._env("SHOONYA_API_KEY", "SHOONYA_APP_KEY", "SHOONYA_API_SECRET", "SHOONYA_SECRET_KEY"),
            "imei": self._env("SHOONYA_IMEI", "SHOONYA_DEVICE_ID"),
            "twofa": self._runtime_twofa or self._env("SHOONYA_TWOFA", "SHOONYA_OTP", "SHOONYA_TOTP"),
            "totp_secret": self._env("SHOONYA_TOTP_SECRET", "SHOONYA_TOTP_KEY", "SHOONYA_TOTP_TOKEN", "TOTP_SECRET", "TOTP_KEY"),
        }

    def missing_credentials(self) -> list[str]:
        values = self._credential_values()
        missing = [
            key
            for key in ("user_id", "password", "vendor_code", "api_key", "imei")
            if not values.get(key)
        ]
        if not values.get("twofa") and not values.get("totp_secret"):
            missing.append("twofa_or_totp_secret")
        return missing

    @property
    def configured(self) -> bool:
        return not self.missing_credentials()

    def status(self) -> dict[str, Any]:
        values = self._credential_values()
        accepted_env_names = {
            "user_id": ["SHOONYA_USER_ID", "SHOONYA_UID", "SHOONYA_CLIENT_ID", "SHOONYA_USER"],
            "password": ["SHOONYA_PASSWORD", "SHOONYA_PWD"],
            "vendor_code": ["SHOONYA_VENDOR_CODE", "SHOONYA_VC"],
            "api_key": ["SHOONYA_API_KEY", "SHOONYA_APP_KEY", "SHOONYA_API_SECRET", "SHOONYA_SECRET_KEY"],
            "imei": ["SHOONYA_IMEI", "SHOONYA_DEVICE_ID"],
            "twofa": ["SHOONYA_TWOFA", "SHOONYA_OTP", "SHOONYA_TOTP"],
            "totp_secret": ["SHOONYA_TOTP_SECRET", "SHOONYA_TOTP_KEY", "SHOONYA_TOTP_TOKEN", "TOTP_SECRET", "TOTP_KEY"],
        }
        with self._lock:
            return {
                "provider": "shoonya",
                "mode": os.getenv("BROKER_MODE", "market_data_only"),
                "host": os.getenv("SHOONYA_HOST", "https://api.shoonya.com/NorenWClientTP"),
                "websocket": os.getenv("SHOONYA_WEBSOCKET", "wss://api.shoonya.com/NorenWSTP/"),
                "configured": self.configured,
                "credential_flags": {key: bool(values.get(key)) for key in values},
                "accepted_env_names": accepted_env_names,
                "sdk_available": self._sdk_available,
                "missing_credentials": self.missing_credentials(),
                "started": self._started,
                "feed_open": self._feed_open,
                "status": self._status,
                "last_error": self._last_error,
                "runtime_twofa_set": bool(self._runtime_twofa),
                "runtime_twofa_set_at": self._runtime_twofa_set_at,
                "subscribed_symbols": sorted(self._symbol_to_token),
                "trading_enabled": os.getenv("ENABLE_TRADING", "false").lower() == "true",
                "config_message": (
                    "Configured; check last_error if status is error."
                    if self.configured
                    else f"Missing {', '.join(self.missing_credentials())}. Set the accepted Render env names."
                ),
            }

    def set_runtime_twofa(self, twofa: str) -> dict[str, Any]:
        clean = str(twofa or "").strip()
        if not clean:
            raise ValueError("OTP/TOTP cannot be empty")
        with self._lock:
            self._runtime_twofa = clean
            self._runtime_twofa_set_at = datetime.now(timezone.utc).isoformat()
            self._started = False
            self._feed_open = False
            self._status = "otp_received"
            self._last_error = None
        self.start()
        return self.status()

    def _twofa(self) -> str:
        values = self._credential_values()
        manual = values.get("twofa")
        if manual:
            return manual
        secret = values.get("totp_secret")
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
        with self._lock:
            if self._started:
                return
            self._started = True
            self._status = "starting"

        thread = threading.Thread(target=self._run, name="shoonya-live-feed", daemon=True)
        thread.start()

    def _run(self) -> None:
        try:
            ShoonyaApiClass = load_shoonya_api_class()
            with self._lock:
                self._sdk_available = True
            api = ShoonyaApiClass()
            self._disable_order_methods(api)
            ret = api.login(
                userid=self._credential_values()["user_id"],
                password=self._credential_values()["password"],
                twoFA=self._twofa(),
                vendor_code=self._credential_values()["vendor_code"],
                api_secret=self._credential_values()["api_key"],
                imei=self._credential_values()["imei"],
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
                self._sdk_available = False if "NorenRestApiPy" in str(exc) or "api_helper" in str(exc) else self._sdk_available
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
        redis_state.publish_tick(live)

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
        redis_ticks = redis_state.latest_ticks(symbols)
        with self._lock:
            if not symbols:
                return {**self._latest, **redis_ticks}
            wanted = {symbol.upper() for symbol in symbols}
            memory_ticks = {symbol: tick for symbol, tick in self._latest.items() if symbol.upper() in wanted}
            return {**memory_ticks, **redis_ticks}

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
