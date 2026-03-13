from __future__ import annotations

from pathlib import Path

import pandas as pd

from tests._paths import runtime_root
from screeners.qullamaggie.earnings_data_collector import EarningsDataCollector
from screeners.qullamaggie import screener as qullamaggie_screener


def _reset_dir(path: Path) -> None:
    if path.exists():
        for child in sorted(path.rglob("*"), reverse=True):
            try:
                if child.is_file():
                    child.unlink()
                elif child.is_dir():
                    child.rmdir()
            except Exception:
                continue
    path.mkdir(parents=True, exist_ok=True)


def test_earnings_collector_uses_disk_cache_before_external_fetch(monkeypatch):
    cache_root = runtime_root("_test_runtime_earnings_cache_fresh")
    _reset_dir(cache_root)
    collector = EarningsDataCollector(cache_dir=str(cache_root), cache_duration=3600)
    symbol = "AAPL"
    cache_file = Path(cache_root) / "AAPL.csv"
    pd.DataFrame(
        [
            {
                "date": "2025-12-31",
                "eps_actual": 2.0,
                "eps_estimate": 1.7,
                "revenue_actual": 100.0,
                "revenue_estimate": 90.0,
                "data_source": "yahoo_fin_actual",
            },
            {
                "date": "2025-09-30",
                "eps_actual": 1.8,
                "eps_estimate": 1.6,
                "revenue_actual": 95.0,
                "revenue_estimate": 88.0,
                "data_source": "yahoo_fin_actual",
            },
            {
                "date": "2025-06-30",
                "eps_actual": 1.7,
                "eps_estimate": 1.5,
                "revenue_actual": 92.0,
                "revenue_estimate": 86.0,
                "data_source": "yahoo_fin_actual",
            },
            {
                "date": "2025-03-31",
                "eps_actual": 1.5,
                "eps_estimate": 1.4,
                "revenue_actual": 85.0,
                "revenue_estimate": 82.0,
                "data_source": "yahoo_fin_actual",
            },
        ]
    ).to_csv(cache_file, index=False)

    def _fail_external(_symbol: str):
        raise AssertionError("external call should not execute when fresh disk cache exists")

    monkeypatch.setattr(collector, "_fetch_yahoo_fin_earnings", _fail_external)
    monkeypatch.setattr(collector, "_fetch_yf_earnings", _fail_external)

    payload = collector.get_earnings_surprise(symbol)
    assert payload is not None
    assert payload.get("data_source") == "yahoo_fin_actual"
    assert payload.get("eps_surprise_pct", 0) > 0


def test_earnings_collector_writes_disk_cache_after_fetch(monkeypatch):
    cache_root = runtime_root("_test_runtime_earnings_cache_write")
    _reset_dir(cache_root)
    collector = EarningsDataCollector(cache_dir=str(cache_root), cache_duration=3600)
    symbol = "MSFT"

    source_frame = pd.DataFrame(
        [
            {
                "date": "2025-12-31",
                "eps_actual": 2.1,
                "eps_estimate": 1.9,
                "revenue_actual": 110.0,
                "revenue_estimate": 105.0,
                "data_source": "yfinance_actual",
            },
            {
                "date": "2025-09-30",
                "eps_actual": 1.9,
                "eps_estimate": 1.8,
                "revenue_actual": 100.0,
                "revenue_estimate": 98.0,
                "data_source": "yfinance_actual",
            },
            {
                "date": "2025-06-30",
                "eps_actual": 1.8,
                "eps_estimate": 1.7,
                "revenue_actual": 97.0,
                "revenue_estimate": 95.0,
                "data_source": "yfinance_actual",
            },
            {
                "date": "2025-03-31",
                "eps_actual": 1.6,
                "eps_estimate": 1.5,
                "revenue_actual": 92.0,
                "revenue_estimate": 90.0,
                "data_source": "yfinance_actual",
            },
        ]
    )

    monkeypatch.setattr(collector, "_fetch_yahoo_fin_earnings", lambda _symbol: None)
    monkeypatch.setattr(collector, "_fetch_yf_earnings", lambda _symbol: source_frame.copy())

    payload = collector.get_earnings_surprise(symbol)
    assert payload is not None

    cache_file = Path(cache_root) / "MSFT.csv"
    assert cache_file.exists()
    cached = pd.read_csv(cache_file)
    assert not cached.empty
    assert str(cached.iloc[0]["data_source"]) == "yfinance_actual"


def test_calculate_surprise_does_not_invent_missing_revenue_values():
    collector = EarningsDataCollector(cache_duration=3600)

    source_frame = pd.DataFrame(
        [
            {"date": "2025-12-31", "eps_actual": 2.0, "eps_estimate": 1.8, "revenue_actual": pd.NA, "revenue_estimate": pd.NA, "data_source": "yfinance_actual"},
            {"date": "2025-09-30", "eps_actual": 1.7, "eps_estimate": 1.6, "revenue_actual": pd.NA, "revenue_estimate": pd.NA, "data_source": "yfinance_actual"},
            {"date": "2025-06-30", "eps_actual": 1.6, "eps_estimate": 1.5, "revenue_actual": pd.NA, "revenue_estimate": pd.NA, "data_source": "yfinance_actual"},
            {"date": "2025-03-31", "eps_actual": 1.4, "eps_estimate": 1.3, "revenue_actual": pd.NA, "revenue_estimate": pd.NA, "data_source": "yfinance_actual"},
        ]
    )

    payload = collector._calculate_surprise(source_frame)
    assert payload is not None
    assert payload["data_quality"] == "actual"
    assert payload["revenue_actual"] is None
    assert payload["revenue_estimate"] is None
    assert payload["revenue_surprise_pct"] is None


def test_qullamaggie_kr_defaults_to_earnings_filter(monkeypatch):
    captured: dict[str, object] = {}
    sample_frame = pd.DataFrame(
        {
            "date": pd.date_range("2025-01-01", periods=80, freq="B").strftime("%Y-%m-%d"),
            "open": [100 + index for index in range(80)],
            "high": [101 + index for index in range(80)],
            "low": [99 + index for index in range(80)],
            "close": [100.5 + index for index in range(80)],
            "volume": [1_000_000] * 80,
            "symbol": ["005930"] * 80,
        }
    )

    monkeypatch.setattr(qullamaggie_screener, "_load_market_symbols", lambda market: ["005930"])
    monkeypatch.setattr(qullamaggie_screener, "load_local_ohlcv_frame", lambda market, symbol: sample_frame.copy())
    monkeypatch.setattr(
        qullamaggie_screener,
        "screen_episode_pivot_setup",
        lambda symbol, frame, enable_earnings_filter, market: {
            "symbol": symbol,
            "passed": True,
            "score": 5,
            "earnings_filter_enabled": captured.setdefault("earnings_filter_enabled", enable_earnings_filter),
            "market": captured.setdefault("market", market),
        },
    )
    monkeypatch.setattr(qullamaggie_screener, "save_screening_results", lambda **kwargs: {"csv": "dummy.csv", "json": "dummy.json"})
    monkeypatch.setattr(qullamaggie_screener, "track_new_tickers", lambda **kwargs: [])
    monkeypatch.setattr(qullamaggie_screener, "create_screener_summary", lambda **kwargs: None)

    result = qullamaggie_screener.run_qullamaggie_screening(setup_type="episode_pivot", market="kr")

    assert captured["earnings_filter_enabled"] is True
    assert captured["market"] == "kr"
    assert len(result["episode_pivot"]) == 1
