from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from nse_data_fetcher import nse_fetcher
from universe_store import UNIVERSE_STORE


async def validate_pipeline() -> int:
    exit_code = 0
    print(f"NSE adapter: {nse_fetcher.status()}")
    try:
        payload = nse_fetcher.bhavcopy_payload()
        print(f"OK bhavcopy: {len(payload.get('rows', []))} rows for {payload.get('as_of')}")
    except Exception as exc:
        exit_code = 1
        print(f"FAIL bhavcopy: {exc}")

    meta = UNIVERSE_STORE.meta()
    print(f"Universe cache: {meta.get('total')} symbols, {meta.get('priced')} priced")

    for symbol in ["RELIANCE", "TCS", "HDFCBANK"]:
        try:
            actions = await nse_fetcher.fetch_corporate_actions(symbol)
            print(f"OK corporate actions {symbol}: {len(actions)} parsed adjustment(s)")
        except Exception as exc:
            print(f"WARN corporate actions {symbol}: {exc}")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(asyncio.run(validate_pipeline()))
