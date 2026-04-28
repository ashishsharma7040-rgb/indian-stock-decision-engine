from __future__ import annotations

import base64
import binascii
import hashlib
import json
import logging
import os
import queue
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

logger = logging.getLogger(__name__)


def load_shoonya_api_class() -> Any:
    try:
        from api_helper import ShoonyaApiPy

        return ShoonyaApiPy
    except ImportError:
        from NorenRestApiPy.NorenApi import NorenApi

        class ShoonyaApiPy(NorenApi):
            def __init__(self) -> None:
                kwargs = {
                    "host": os.getenv("SHOONYA_HOST", "https://api.shoonya.com/NorenWClientTP/"),
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
        self._last_login_attempt_at: str | None = None
        self._last_login_success_at: str | None = None
        self._last_login_response_stat: str | None = None
        self._last_login_error: str | None = None
        self._websocket_open_attempted = False
        self._websocket_opened_at: str | None = None
        self._websocket_last_close_reason: str | None = None
        self._last_endpoint_host: str | None = None
        self._last_endpoint_websocket: str | None = None
        self._totp_source: str | None = None
        self._runtime_twofa_last_used_at: str | None = None
        self._unresolved_symbols: set[str] = set()
        self._pending_symbols: set[str] = set()
        self._latest: dict[str, dict[str, Any]] = {}
        self._token_to_symbol: dict[str, str] = {}
        self._symbol_to_token: dict[str, str] = {}
        self._subscribed_tokens: set[str] = set()
        self._reconnect_thread: threading.Thread | None = None
        self._run_generation = 0
        self._last_login_password_mode: str | None = None
        self._attempted_password_modes: list[str] = []

    def _env(self, *names: str) -> str | None:
        for name in names:
            value = os.getenv(name)
            if value and value.strip():
                return value.strip()
        return None

    def _credential_values(self) -> dict[str, str | None]:
        return {
            "user_id": self._env("SHOONYA_USER_ID", "SHOONYA_UID", "SHOONYA_CLIENT_ID", "SHOONYA_USER"),
            "password": self._env("SHOONYA_PASSWORD", "SHOONYA_PWD"),
            "vendor_code": self._env("SHOONYA_VENDOR_CODE", "SHOONYA_VC"),
            "api_key": self._env("SHOONYA_API_KEY", "SHOONYA_APP_KEY", "SHOONYA_API_SECRET", "SHOONYA_SECRET_KEY"),
            "imei": self._env("SHOONYA_IMEI", "SHOONYA_DEVICE_ID"),
            "runtime_twofa": self._runtime_twofa,
            "twofa": self._runtime_twofa or self._env("SHOONYA_TWOFA", "SHOONYA_OTP", "SHOONYA_TOTP"),
            "totp_secret": self._env("SHOONYA_TOTP_SECRET", "SHOONYA_TOTP_KEY", "SHOONYA_TOTP_TOKEN", "TOTP_SECRET", "TOTP_KEY"),
        }

    @staticmethod
    def _looks_placeholder(value: str | None) -> bool:
        text = str(value or "").strip().lower()
        if not text:
            return False
        markers = ("changeme", "your_", "replace", "example", "sample", "test", "dummy", "xxxx", "placeholder")
        return any(marker in text for marker in markers)

    def _credential_diagnostics(self) -> dict[str, Any]:
        values = self._credential_values()
        diagnostics: dict[str, Any] = {}
        for key in ("user_id", "vendor_code", "api_key", "imei", "password"):
            raw = str(values.get(key) or "")
            diagnostics[key] = {
                "present": bool(raw),
                "length": len(raw.strip()),
                "contains_spaces": bool(raw and any(ch.isspace() for ch in raw)),
                "looks_placeholder": self._looks_placeholder(raw),
            }
        secret = values.get("totp_secret")
        diagnostics["totp_secret"] = {
            "present": bool(secret),
            "looks_placeholder": self._looks_placeholder(secret),
            "format_valid": False,
        }
        if secret:
            try:
                self._validate_totp_secret(secret)
                diagnostics["totp_secret"]["format_valid"] = True
            except Exception:
                diagnostics["totp_secret"]["format_valid"] = False
        runtime_twofa = str(values.get("runtime_twofa") or "")
        diagnostics["runtime_twofa"] = {
            "present": bool(runtime_twofa),
            "length": len(runtime_twofa.strip()),
            "numeric": runtime_twofa.isdigit() if runtime_twofa else False,
            "looks_placeholder": self._looks_placeholder(runtime_twofa),
        }
        diagnostics["password_mode_setting"] = str(os.getenv("SHOONYA_PASSWORD_MODE", "raw")).strip().lower() or "raw"
        return diagnostics

    def _runtime_twofa_age_seconds(self) -> int | None:
        if not self._runtime_twofa_set_at:
            return None
        try:
            return max(
                0,
                int(
                    (
                        datetime.now(timezone.utc)
                        - datetime.fromisoformat(str(self._runtime_twofa_set_at).replace("Z", "+00:00"))
                    ).total_seconds()
                ),
            )
        except Exception:
            return None

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
                "last_login_attempt_at": self._last_login_attempt_at,
                "last_login_success_at": self._last_login_success_at,
                "last_login_response_stat": self._last_login_response_stat,
                "last_login_error": self._last_login_error,
                "websocket_open_attempted": self._websocket_open_attempted,
                "websocket_opened_at": self._websocket_opened_at,
                "websocket_last_close_reason": self._websocket_last_close_reason,
                "last_endpoint_host": self._last_endpoint_host,
                "last_endpoint_websocket": self._last_endpoint_websocket,
                "totp_source": self._totp_source,
                "auto_reconnect": os.getenv("SHOONYA_AUTO_RECONNECT", "true").lower() in {"1", "true", "yes"},
                "reconnect_delay_seconds": int(os.getenv("SHOONYA_RECONNECT_SECONDS", "30")),
                "runtime_twofa_set": bool(self._runtime_twofa),
                "runtime_twofa_set_at": self._runtime_twofa_set_at,
                "runtime_twofa_last_used_at": self._runtime_twofa_last_used_at,
                "subscribed_symbols": sorted(self._symbol_to_token),
                "subscribed_count": len(self._symbol_to_token),
                "resolved_tokens_count": len(self._token_to_symbol),
                "unresolved_symbols": sorted(self._unresolved_symbols),
                "pending_subscription_symbols": sorted(self._pending_symbols),
                "pending_subscription_count": len(self._pending_symbols),
                "trading_enabled": os.getenv("ENABLE_TRADING", "false").lower() == "true",
                "credential_diagnostics": self._credential_diagnostics(),
                "runtime_twofa_age_seconds": self._runtime_twofa_age_seconds(),
                "last_login_password_mode": self._last_login_password_mode,
                "attempted_password_modes": list(self._attempted_password_modes),
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
            self._status = "sdk_loaded" if self._sdk_available else "not_configured"
            self._last_error = None
            self._last_login_error = None
        return self.status()

    def _validate_totp_secret(self, secret: str) -> str:
        normalized = "".join(str(secret or "").split()).upper()
        if not normalized:
            raise RuntimeError("Invalid SHOONYA_TOTP_SECRET format")
        padded = normalized + ("=" * ((8 - len(normalized) % 8) % 8))
        try:
            base64.b32decode(padded, casefold=True)
        except (binascii.Error, ValueError) as exc:
            raise RuntimeError("Invalid SHOONYA_TOTP_SECRET format") from exc
        return normalized

    def _twofa(self) -> str:
        values = self._credential_values()
        runtime_twofa = values.get("runtime_twofa")
        if runtime_twofa:
            with self._lock:
                self._totp_source = "runtime_twofa"
                self._runtime_twofa_last_used_at = datetime.now(timezone.utc).isoformat()
            logger.info("Shoonya login: using runtime OTP override from settings.")
            return runtime_twofa
        secret = values.get("totp_secret")
        if secret:
            if pyotp is None:
                raise RuntimeError("pyotp is required for SHOONYA_TOTP_SECRET")
            normalized_secret = self._validate_totp_secret(secret)
            with self._lock:
                self._totp_source = "totp_secret"
            logger.info("Shoonya login: generated TOTP from SHOONYA_TOTP_SECRET.")
            return pyotp.TOTP(normalized_secret).now()
        manual = values.get("twofa")
        if manual:
            with self._lock:
                self._totp_source = "manual_twofa"
            logger.info("Shoonya login: using runtime/manual twofa value.")
            return manual
        with self._lock:
            self._totp_source = "missing"
        raise RuntimeError("Set SHOONYA_TOTP_SECRET for automatic TOTP or SHOONYA_TWOFA for one manual OTP")

    @staticmethod
    def _normalize_http_endpoint(value: str | None) -> str:
        text = str(value or "").strip() or "https://api.shoonya.com/NorenWClientTP/"
        if not text.endswith("/"):
            text += "/"
        return text

    @staticmethod
    def _normalize_ws_endpoint(value: str | None) -> str:
        text = str(value or "").strip() or "wss://api.shoonya.com/NorenWSTP/"
        if not text.endswith("/"):
            text += "/"
        return text

    def _endpoint_candidates(self) -> list[tuple[str, str]]:
        configured_host = self._normalize_http_endpoint(os.getenv("SHOONYA_HOST"))
        configured_ws = self._normalize_ws_endpoint(os.getenv("SHOONYA_WEBSOCKET"))
        candidates = [
            (configured_host, configured_ws),
            ("https://api.shoonya.com/NorenWClientTP/", "wss://api.shoonya.com/NorenWSTP/"),
            ("https://shoonyatrade.finvasia.com/NorenWClientTP/", "wss://shoonyatrade.finvasia.com/NorenWSTP/"),
        ]
        unique: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for item in candidates:
            if item in seen:
                continue
            seen.add(item)
            unique.append(item)
        return unique

    def _password_attempts(self) -> list[tuple[str, str]]:
        values = self._credential_values()
        password = str(values.get("password") or "")
        mode = str(os.getenv("SHOONYA_PASSWORD_MODE", "raw")).strip().lower() or "raw"
        hashed = hashlib.sha256(password.encode("utf-8")).hexdigest() if password else ""
        options: list[tuple[str, str]] = []
        if mode == "sha256":
            options.append(("sha256", hashed))
        elif mode == "auto":
            options.append(("raw", password))
            if hashed and hashed != password:
                options.append(("sha256", hashed))
        else:
            options.append(("raw", password))
        unique: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for item in options:
            if item in seen:
                continue
            seen.add(item)
            unique.append(item)
        return unique

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
            with self._lock:
                self._status = "not_configured"
            return
        with self._lock:
            if self._started:
                return
            self._started = True
            self._status = "sdk_loaded"
            self._websocket_open_attempted = False
            self._websocket_opened_at = None
            self._websocket_last_close_reason = None
            self._run_generation += 1
            generation = self._run_generation

        thread = threading.Thread(target=self._run, args=(generation,), name="shoonya-live-feed", daemon=True)
        thread.start()

    def _run(self, generation: int) -> None:
        login_succeeded = False
        connection_ready = False
        used_runtime_twofa = False
        endpoint_errors: list[str] = []
        try:
            ShoonyaApiClass = load_shoonya_api_class()
            with self._lock:
                if generation != self._run_generation:
                    return
                self._sdk_available = True
                self._status = "sdk_loaded"
            with self._lock:
                if generation != self._run_generation:
                    return
                self._status = "login_attempting"
                self._last_login_attempt_at = datetime.now(timezone.utc).isoformat()
                self._last_login_response_stat = None
                self._last_login_error = None
                self._attempted_password_modes = []
                used_runtime_twofa = bool(self._runtime_twofa)
            logger.info("Shoonya login attempt started. Market-data-only mode is active.")
            for host, websocket in self._endpoint_candidates():
                for password_mode, password_value in self._password_attempts():
                    api = ShoonyaApiClass()
                    setattr(api, "host", host)
                    setattr(api, "websocket", websocket)
                    self._disable_order_methods(api)
                    with self._lock:
                        if generation != self._run_generation:
                            return
                        self._last_endpoint_host = host
                        self._last_endpoint_websocket = websocket
                        self._last_error = None
                        self._last_login_password_mode = password_mode
                        self._attempted_password_modes.append(password_mode)
                    logger.info("Shoonya attempting endpoint host=%s websocket=%s password_mode=%s", host, websocket, password_mode)
                    try:
                        try:
                            ret = self._call_with_timeout(
                                lambda: api.login(
                                    userid=self._credential_values()["user_id"],
                                    password=password_value,
                                    twoFA=self._twofa(),
                                    vendor_code=self._credential_values()["vendor_code"],
                                    api_secret=self._credential_values()["api_key"],
                                    imei=self._credential_values()["imei"],
                                ),
                                timeout_seconds=max(10, int(os.getenv("SHOONYA_LOGIN_TIMEOUT_SECONDS", "35"))),
                                operation_name="Shoonya login",
                            )
                        except json.JSONDecodeError as exc:
                            raise RuntimeError(
                                f"Shoonya returned an empty/non-JSON login response from {host} using password mode {password_mode}. Common causes: wrong password/API key, "
                                "expired or stale OTP, invalid TOTP secret, Shoonya server outage, or Render IP/network not allowed by Shoonya."
                            ) from exc
                        with self._lock:
                            if generation != self._run_generation:
                                return
                            self._last_login_response_stat = ret.get("stat") if isinstance(ret, dict) else None
                        if not ret or ret.get("stat") != "Ok":
                            raise RuntimeError(f"Shoonya login failed ({password_mode}): {ret.get('emsg') if isinstance(ret, dict) else ret}")
                        with self._lock:
                            if generation != self._run_generation:
                                return
                            self._api = api
                            self._status = "logged_in"
                            self._last_login_error = None
                            self._last_login_success_at = datetime.now(timezone.utc).isoformat()
                        login_succeeded = True
                        logger.info("Shoonya login succeeded. Opening WebSocket feed.")

                        with self._lock:
                            self._status = "websocket_starting"
                            self._websocket_open_attempted = True
                            self._websocket_last_close_reason = None
                        try:
                            self._call_with_timeout(
                                lambda: api.start_websocket(
                                    subscribe_callback=lambda tick: self._on_tick(generation, tick),
                                    order_update_callback=self._ignore_order_update,
                                    socket_open_callback=lambda *args: self._on_open(generation, *args),
                                    socket_close_callback=lambda *args: self._on_close(generation, *args),
                                    socket_error_callback=lambda *args: self._on_socket_error(generation, *args),
                                ),
                                timeout_seconds=max(10, int(os.getenv("SHOONYA_WEBSOCKET_START_TIMEOUT_SECONDS", "20"))),
                                operation_name="Shoonya websocket startup",
                            )
                        except TypeError:
                            self._call_with_timeout(
                                lambda: api.start_websocket(
                                    subscribe_callback=lambda tick: self._on_tick(generation, tick),
                                    order_update_callback=self._ignore_order_update,
                                    socket_open_callback=lambda *args: self._on_open(generation, *args),
                                    socket_close_callback=lambda *args: self._on_close(generation, *args),
                                ),
                                timeout_seconds=max(10, int(os.getenv("SHOONYA_WEBSOCKET_START_TIMEOUT_SECONDS", "20"))),
                                operation_name="Shoonya websocket startup",
                            )
                        deadline = time.time() + 20
                        while time.time() < deadline:
                            with self._lock:
                                if generation != self._run_generation:
                                    return
                                if self._feed_open:
                                    break
                            time.sleep(0.2)
                        if not self._feed_open:
                            with self._lock:
                                self._websocket_last_close_reason = "websocket did not open within 20 seconds"
                            raise RuntimeError("Shoonya WebSocket did not open within 20 seconds")
                        connection_ready = True
                        endpoint_errors.clear()
                        break
                    except Exception as endpoint_exc:
                        endpoint_errors.append(str(endpoint_exc))
                        logger.warning("Shoonya endpoint attempt failed for host=%s password_mode=%s: %s", host, password_mode, endpoint_exc)
                        for method_name in ("close_websocket", "close"):
                            method = getattr(api, method_name, None)
                            if callable(method):
                                try:
                                    method()
                                except Exception:
                                    pass
                        if "Shoonya login failed (" in str(endpoint_exc):
                            raise
                        continue
                if connection_ready:
                    break
            if not connection_ready and endpoint_errors:
                raise RuntimeError(" | ".join(endpoint_errors[-2:]))
        except Exception as exc:  # pragma: no cover - broker/network dependent
            logger.warning("Shoonya live feed failed: %s", exc)
            with self._lock:
                if generation != self._run_generation:
                    return
                self._sdk_available = False if "NorenRestApiPy" in str(exc) or "api_helper" in str(exc) else self._sdk_available
                self._status = "error"
                self._last_error = str(exc)
                if not login_succeeded:
                    self._last_login_error = str(exc)
                self._started = False
                self._feed_open = False
        finally:
            if used_runtime_twofa:
                with self._lock:
                    if generation == self._run_generation:
                        self._runtime_twofa = None
        self._schedule_reconnect()

    def _call_with_timeout(self, fn: Any, timeout_seconds: int, operation_name: str) -> Any:
        result_queue: "queue.Queue[tuple[str, Any]]" = queue.Queue(maxsize=1)

        def runner() -> None:
            try:
                result_queue.put(("ok", fn()))
            except Exception as exc:
                result_queue.put(("error", exc))

        thread = threading.Thread(
            target=runner,
            name=f"shoonya-{operation_name.lower().replace(' ', '-')}",
            daemon=True,
        )
        thread.start()
        thread.join(timeout_seconds)
        if thread.is_alive():
            raise RuntimeError(f"{operation_name} timed out after {timeout_seconds} seconds")
        status, value = result_queue.get_nowait()
        if status == "error":
            raise value
        return value

    def _schedule_reconnect(self) -> None:
        if os.getenv("SHOONYA_AUTO_RECONNECT", "true").lower() not in {"1", "true", "yes"}:
            return
        if not self.configured:
            return
        with self._lock:
            if self._feed_open or self._started:
                return
            if self._reconnect_thread and self._reconnect_thread.is_alive():
                return
            delay = max(5, int(os.getenv("SHOONYA_RECONNECT_SECONDS", "30")))

        def delayed_start() -> None:
            time.sleep(delay)
            with self._lock:
                if self._feed_open or self._started:
                    return
            self.start()

        thread = threading.Thread(target=delayed_start, name="shoonya-live-reconnect", daemon=True)
        with self._lock:
            self._reconnect_thread = thread
        thread.start()

    def _on_open(self, generation: int, *_: Any) -> None:
        with self._lock:
            if generation != self._run_generation:
                return
            self._feed_open = True
            self._status = "live"
            self._websocket_opened_at = datetime.now(timezone.utc).isoformat()
            self._last_error = None
        logger.info("Shoonya WebSocket feed is open.")
        pending = sorted(self._pending_symbols)
        if pending:
            threading.Thread(
                target=lambda: self.subscribe(pending),
                name="shoonya-pending-subscribe",
                daemon=True,
            ).start()

    def _on_close(self, generation: int, *args: Any) -> None:
        reason = "socket_close_callback"
        if args:
            try:
                reason = " | ".join(str(arg) for arg in args if arg not in (None, ""))
            except Exception:
                reason = "socket_close_callback"
        with self._lock:
            if generation != self._run_generation:
                return
            self._feed_open = False
            self._status = "error"
            self._started = False
            self._websocket_last_close_reason = reason
            self._last_error = self._last_error or f"Shoonya WebSocket closed: {reason}"
        logger.warning("Shoonya WebSocket feed closed: %s", reason)
        self._schedule_reconnect()

    def _on_socket_error(self, generation: int, *args: Any) -> None:
        reason = "socket_error_callback"
        if args:
            try:
                reason = " | ".join(str(arg) for arg in args if arg not in (None, ""))
            except Exception:
                reason = "socket_error_callback"
        with self._lock:
            if generation != self._run_generation:
                return
            self._last_error = f"Shoonya WebSocket error: {reason}"
            self._websocket_last_close_reason = reason
        logger.warning("Shoonya WebSocket error: %s", reason)

    def _ignore_order_update(self, _: dict[str, Any]) -> None:
        return

    def _on_tick(self, generation: int, tick: dict[str, Any]) -> None:
        token = str(tick.get("tk") or "")
        if not token:
            return
        with self._lock:
            if generation != self._run_generation:
                return
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
            with self._lock:
                self._pending_symbols.update(str(symbol or "").upper().strip() for symbol in symbols if str(symbol or "").strip())
            return {**self.status(), "pending": symbols, "unresolved": []}

        limited_symbols = []
        seen: set[str] = set()
        for raw_symbol in symbols:
            symbol = str(raw_symbol or "").upper().strip()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            limited_symbols.append(symbol)
            if len(limited_symbols) >= 60:
                break

        for symbol in limited_symbols:
            token = self._symbol_to_token.get(symbol)
            if not token:
                token = self._resolve_token(api, symbol)
            if token:
                key = f"NSE|{token}"
                tokens.append(key)
                with self._lock:
                    self._subscribed_tokens.add(key)
                    self._unresolved_symbols.discard(symbol)
                    self._pending_symbols.discard(symbol)
            else:
                unresolved.append(symbol)
                with self._lock:
                    self._unresolved_symbols.add(symbol)
                    self._pending_symbols.discard(symbol)
        if tokens:
            logger.info("Shoonya subscribe: subscribing %s symbols.", len(tokens))
            api.subscribe(tokens)
        return {**self.status(), "subscribed": tokens, "unresolved": unresolved, "pending": sorted(self._pending_symbols)}

    def _resolve_token(self, api: Any, symbol: str) -> str | None:
        target = f"{symbol}-EQ"
        search_terms = []
        for candidate in (symbol, target, f"{symbol} EQ"):
            if candidate not in search_terms:
                search_terms.append(candidate)
        for term in search_terms:
            try:
                ret = api.searchscrip(exchange="NSE", searchtext=term)
            except Exception as exc:
                logger.warning("Shoonya searchscrip failed for %s via %s: %s", symbol, term, exc)
                continue
            values = ret.get("values", []) if isinstance(ret, dict) else []
            match = next((row for row in values if str(row.get("tsym", "")).upper() == target), None)
            if match is None:
                match = next((row for row in values if str(row.get("tsym", "")).upper().startswith(symbol)), None)
            if not match:
                continue
            token = str(match["token"])
            logger.info("Shoonya token resolved for %s as %s (%s).", symbol, token, match.get("tsym"))
            with self._lock:
                self._symbol_to_token[symbol] = token
                self._token_to_symbol[token] = symbol
                self._unresolved_symbols.discard(symbol)
                self._pending_symbols.discard(symbol)
            return token
        logger.info("Shoonya token not resolved for %s.", symbol)
        return None

    def restart(self) -> dict[str, Any]:
        with self._lock:
            api = self._api
            self._api = None
            self._started = False
            self._feed_open = False
            self._status = "sdk_loaded" if self.configured else "not_configured"
            self._last_error = None
            self._last_login_error = None
            self._websocket_open_attempted = False
            self._websocket_opened_at = None
            self._websocket_last_close_reason = None
            self._subscribed_tokens.clear()
            self._token_to_symbol.clear()
            self._symbol_to_token.clear()
            self._latest.clear()
            self._unresolved_symbols.clear()
            self._pending_symbols.clear()
            self._run_generation += 1
        for method_name in ("close_websocket", "close"):
            method = getattr(api, method_name, None)
            if callable(method):
                try:
                    method()
                except Exception:
                    pass
        self.start()
        return self.status()

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
