from __future__ import annotations

from nse_data_fetcher import NSEDataFetcher, nse_fetcher


def test_nse_fetcher_imports_without_optional_network_calls() -> None:
    fetcher = NSEDataFetcher()
    status = fetcher.status()
    assert "nsefin_available" in status
    assert isinstance(nse_fetcher.status(), dict)
