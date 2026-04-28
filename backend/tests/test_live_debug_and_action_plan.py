from __future__ import annotations

from fastapi.testclient import TestClient

import app as app_module
from live_feed import LiveFeedManager
from scoring_engine import build_action_plan


def test_live_debug_endpoint_hides_secrets(monkeypatch) -> None:
    monkeypatch.setattr(app_module.live_feed, "missing_credentials", lambda: [])
    monkeypatch.setattr(app_module.live_feed, "start", lambda: None)
    monkeypatch.setattr(
        app_module.live_feed,
        "status",
        lambda: {
            "configured": True,
            "credential_flags": {"user_id": True, "password": True, "totp_secret": True},
            "status": "error",
            "started": True,
            "feed_open": False,
            "missing_credentials": [],
            "last_error": "OTP expired",
            "sdk_available": True,
            "last_login_attempt_at": "2026-04-26T09:59:00+00:00",
            "last_login_response_stat": "Not_Ok",
            "last_login_error": "OTP expired",
            "websocket_open_attempted": True,
            "websocket_opened_at": None,
            "websocket_last_close_reason": "socket timeout",
            "totp_source": "totp_secret",
            "trading_enabled": False,
            "subscribed_symbols": ["RELIANCE"],
            "subscribed_count": 1,
            "resolved_tokens_count": 1,
            "unresolved_symbols": ["TCS"],
            "runtime_twofa_set_at": "2026-04-26T10:00:00+00:00",
            "accepted_env_names": {"twofa": ["SHOONYA_TWOFA"], "totp_secret": ["SHOONYA_TOTP_SECRET"]},
            "config_message": "Configured; check last_error if status is error.",
        },
    )
    client = TestClient(app_module.app)
    payload = client.get("/api/live/debug").json()
    dump = str(payload)
    assert payload["configured"] is True
    assert "SHOONYA_TOTP_SECRET" in dump
    assert "OTP expired" in dump
    assert "SECRET123" not in dump
    assert "654321" not in dump
    assert "password" not in dump.lower()


def test_build_action_plan_wait_data_for_bad_quality() -> None:
    plan = build_action_plan(
        candidate=True,
        verdict="Candidate - monitor the trigger and position size from the entry plan",
        technical={
            "indicators": {"close": 100.0},
            "entry": {
                "buy_stop_trigger": 105.0,
                "breakout_level": 104.5,
                "stop": 96.0,
                "target_1": 114.0,
                "target_2": 120.0,
                "pullback": [99.0, 101.0],
                "aggressive": [103.0, 104.0],
                "position_sizing": {
                    "risk_capital": 1000.0,
                    "suggested_quantity": 25,
                    "approx_position_value": 2575.0,
                    "account_size": 100000.0,
                },
            },
        },
        market_result={"regime": "Risk-on", "master_switch": {"can_buy": True}},
        price_data_quality={"pass": False},
        failed_gates=["data_quality_or_completeness_failed"],
        risk_score=12,
    )
    assert plan["action"] == "WAIT_DATA"
    assert plan["confidence"] == "low"
    assert "trusted decision" in plan["reason_summary"]


def test_wait_data_detail_exposes_research_sections() -> None:
    payload = app_module.wait_data_stock_detail(
        "BEL",
        {"symbol": "BEL", "name": "Bharat Electronics", "sector": "Industrials", "industry": "Defence", "price": 300},
        ["Need 220+ valid real daily bars before this stock can be ranked."],
        "Need 220+ valid real daily bars before this stock can be ranked.",
        bars_available=120,
    )
    assert payload["action_plan"]["action"] == "WAIT_DATA"
    assert payload["data_quality_explanation"]["pass"] is False
    assert payload["research_summary"]["why_not_actionable"]
    assert payload["pro_research_sections"]["checklist_before_action"]


def test_runtime_twofa_overrides_totp_secret(monkeypatch) -> None:
    monkeypatch.setenv("SHOONYA_TOTP_SECRET", "JBSWY3DPEHPK3PXP")
    feed = LiveFeedManager()
    feed._runtime_twofa = "123456"
    assert feed._twofa() == "123456"
    assert feed.status()["totp_source"] == "runtime_twofa"


def test_live_debug_includes_pending_subscription_context(monkeypatch) -> None:
    monkeypatch.setattr(app_module.live_feed, "missing_credentials", lambda: [])
    monkeypatch.setattr(app_module.live_feed, "start", lambda: None)
    monkeypatch.setattr(
        app_module.live_feed,
        "status",
        lambda: {
            "configured": True,
            "credential_flags": {"user_id": True, "password": True, "totp_secret": True, "twofa": False},
            "credential_diagnostics": {
                "user_id": {"present": True, "length": 8, "contains_spaces": False, "looks_placeholder": False},
                "vendor_code": {"present": True, "length": 4, "contains_spaces": False, "looks_placeholder": False},
                "api_key": {"present": True, "length": 24, "contains_spaces": False, "looks_placeholder": False},
                "imei": {"present": True, "length": 12, "contains_spaces": False, "looks_placeholder": False},
                "totp_secret": {"present": True, "looks_placeholder": False, "format_valid": True},
                "runtime_twofa": {"present": False, "length": 0, "numeric": False, "looks_placeholder": False},
                "password_mode_setting": "raw",
            },
            "status": "websocket_starting",
            "started": True,
            "feed_open": False,
            "missing_credentials": [],
            "last_error": None,
            "sdk_available": True,
            "last_login_attempt_at": "2026-04-28T09:59:00+00:00",
            "last_login_success_at": "2026-04-28T10:00:00+00:00",
            "last_login_response_stat": "Ok",
            "last_login_error": None,
            "websocket_open_attempted": True,
            "websocket_opened_at": None,
            "websocket_last_close_reason": None,
            "totp_source": "totp_secret",
            "trading_enabled": False,
            "subscribed_symbols": [],
            "subscribed_count": 0,
            "resolved_tokens_count": 0,
            "unresolved_symbols": [],
            "pending_subscription_symbols": ["BEL", "SUZLON"],
            "pending_subscription_count": 2,
            "runtime_twofa_set_at": None,
            "runtime_twofa_last_used_at": None,
            "runtime_twofa_age_seconds": None,
            "last_login_password_mode": "raw",
            "attempted_password_modes": ["raw"],
            "accepted_env_names": {"twofa": ["SHOONYA_TWOFA"], "totp_secret": ["SHOONYA_TOTP_SECRET"]},
            "config_message": "Configured; check last_error if status is error.",
        },
    )
    client = TestClient(app_module.app)
    payload = client.get("/api/live/debug").json()
    assert payload["pending_subscription_symbols"] == ["BEL", "SUZLON"]
    assert "Pending symbols" in payload["next_step"] or "Pending" in payload["next_step"]
    assert payload["likely_blockers"]
