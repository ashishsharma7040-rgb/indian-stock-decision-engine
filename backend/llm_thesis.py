from __future__ import annotations

import json
import os
import urllib.request
from typing import Any


def generate_premium_thesis(stock: dict[str, Any]) -> dict[str, Any]:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return {"enabled": False, "provider": "anthropic", "thesis": stock.get("explanation_json", {}).get("thesis", ""), "note": "ANTHROPIC_API_KEY is not configured"}
    prompt = {
        "symbol": stock.get("symbol"),
        "scores": {
            "weekly": stock.get("weekly_score"),
            "monthly": stock.get("monthly_score"),
            "business": stock.get("business_quality", {}).get("score"),
            "tailwind": stock.get("sector_tailwind", {}).get("score"),
            "events": stock.get("event_strength", {}).get("score"),
            "technical": stock.get("technical_strength", {}).get("score"),
            "risk": stock.get("risk_penalty", {}).get("score"),
        },
        "forensic": stock.get("business_quality", {}).get("forensic_quality"),
        "entry": stock.get("entry"),
        "risk_flags": stock.get("explanation_json", {}).get("risk_flags", []),
        "top_events": stock.get("event_strength", {}).get("events", [])[:4],
    }
    body = {
        "model": os.getenv("ANTHROPIC_MODEL", "claude-3-5-haiku-20241022"),
        "max_tokens": 700,
        "messages": [
            {
                "role": "user",
                "content": (
                    "Write a concise institutional-style stock research thesis from this structured JSON. "
                    "Do not make price predictions. Include setup quality, accounting/forensic risk, event trigger, "
                    "technical confirmation, and invalidation. JSON:\n"
                    + json.dumps(prompt, default=str)
                ),
            }
        ],
    }
    request = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "content-type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))
    text = "\n".join(part.get("text", "") for part in payload.get("content", []) if part.get("type") == "text").strip()
    return {"enabled": True, "provider": "anthropic", "thesis": text, "raw_model": body["model"]}
