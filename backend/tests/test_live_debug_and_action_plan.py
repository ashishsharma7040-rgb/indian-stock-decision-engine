from __future__ import annotations

from fastapi.testclient import TestClient

import app as app_module
from scoring_engine import build_action_plan


def test_live_debug_endpoint_hides_secrets(monkeypatch) -> None:
    monkeypatch.setattr(app_module.live_feed, "missing_credentials", lambda: [])
    monkeypatch.setattr(app_module.live_feed, "start", lambda: None)
    monkeypatch.setattr(
        app_module.live_feed,
        "status",
        lambda: {
            "configured": True,
            "status": "error",
            "feed_open": False,
            "missing_credentials": [],
            "last_error": "OTP expired",
            "sdk_available": True,
            "trading_enabled": False,
            "subscribed_symbols": ["RELIANCE"],
            "subscribed_count": 1,
            "resolved_tokens_count": 1,
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
