from __future__ import annotations

import csv
import io
import re
from typing import Any, Callable


def first_number(value: str) -> float | None:
    match = re.search(r"-?\d+(?:\.\d+)?", value.replace(",", ""))
    return float(match.group(0)) if match else None


def normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def text_trend(value: str, allowed: set[str], default: str) -> str:
    lowered = value.lower()
    for item in allowed:
        if item in lowered:
            return item
    return default


def as_bool(value: str) -> bool:
    lowered = value.lower()
    return any(word in lowered for word in {"yes", "true", "diluted", "issued", "increase"})


def as_number(value: str) -> float | None:
    return first_number(value)


FIELD_MAP: list[tuple[str, tuple[str, ...], Callable[[str], Any]]] = [
    ("sales_cagr", ("sales cagr", "revenue cagr", "compounded sales growth", "sales growth"), as_number),
    ("profit_cagr", ("profit cagr", "pat cagr", "compounded profit growth", "profit growth"), as_number),
    ("roce", ("roce", "return on capital employed"), as_number),
    ("roe", ("roe", "return on equity"), as_number),
    ("debt_equity", ("debt equity", "debt to equity", "debt/equity"), as_number),
    ("cfo_pat", ("cfo pat", "cash flow pat", "cash from operations pat", "cash conversion"), as_number),
    ("pledge_percent", ("pledge", "pledged", "promoter pledge"), as_number),
    ("margin_trend_bps", ("margin trend bps", "opm change bps", "margin change bps"), as_number),
    ("pe", ("stock p e", "p e", "pe ratio", "price earning"), as_number),
    ("dilution_flag", ("dilution", "equity dilution", "share dilution"), as_bool),
    ("fcf_trend", ("fcf trend", "free cash flow trend"), lambda value: text_trend(value, {"positive", "improving", "volatile", "negative"}, "volatile")),
    ("promoter_holding_trend", ("promoter holding trend", "promoter trend"), lambda value: text_trend(value, {"rising", "stable", "flat", "falling"}, "stable")),
    ("net_income", ("net income", "pat latest", "profit after tax"), as_number),
    ("operating_cash_flow", ("operating cash flow", "cash flow from operations", "cfo latest"), as_number),
    ("cash_flow_investing", ("cash flow investing", "cash from investing", "cfi"), as_number),
    ("average_total_assets", ("average total assets", "avg total assets"), as_number),
    ("ebitda", ("ebitda", "operating profit"), as_number),
    ("receivables_growth", ("receivables growth", "trade receivables growth", "debtors growth"), as_number),
    ("revenue_growth", ("revenue growth latest", "sales growth latest"), as_number),
    ("cash_conversion_cycle_days", ("cash conversion cycle", "ccc days"), as_number),
    ("previous_cash_conversion_cycle_days", ("previous cash conversion cycle", "prior ccc days"), as_number),
    ("altman_z_score", ("altman z", "z score"), as_number),
    ("piotroski_f_score", ("piotroski", "f score"), as_number),
    ("beneish_m_score", ("beneish", "m score"), as_number),
    ("next_earnings_date", ("next earnings date", "earnings date", "result date"), lambda value: value.strip()),
]


def maybe_update(label: str, value: str, updates: dict[str, Any]) -> None:
    clean_label = normalize(label)
    if not clean_label or not str(value).strip():
        return
    for field, patterns, converter in FIELD_MAP:
        if field in updates:
            continue
        if any(pattern in clean_label for pattern in patterns):
            parsed = converter(value)
            if parsed is not None:
                updates[field] = parsed


def parse_fundamentals_csv(csv_text: str) -> dict[str, Any]:
    """Parse flexible Screener/manual CSV text into engine fundamental fields.

    Supports both two-column metric/value rows and table-style header/value rows.
    """
    rows = [row for row in csv.reader(io.StringIO(csv_text)) if any(cell.strip() for cell in row)]
    updates: dict[str, Any] = {}
    for row in rows:
        cells = [cell.strip() for cell in row]
        if len(cells) >= 2:
            maybe_update(cells[0], " ".join(cells[1:]), updates)

    for header, values in zip(rows, rows[1:]):
        for index, label in enumerate(header):
            if index < len(values):
                maybe_update(label, values[index], updates)
    return updates
