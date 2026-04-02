from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

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


def test_earnings_collector_refreshes_even_when_disk_cache_exists(monkeypatch):
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

    source_frame = pd.DataFrame(
        [
            {
                "date": "2026-03-31",
                "eps_actual": 2.6,
                "eps_estimate": 2.2,
                "revenue_actual": 120.0,
                "revenue_estimate": 110.0,
                "data_source": "yfinance_actual",
            },
            {
                "date": "2025-12-31",
                "eps_actual": 2.2,
                "eps_estimate": 2.0,
                "revenue_actual": 110.0,
                "revenue_estimate": 100.0,
                "data_source": "yfinance_actual",
            },
            {
                "date": "2025-09-30",
                "eps_actual": 1.9,
                "eps_estimate": 1.8,
                "revenue_actual": 100.0,
                "revenue_estimate": 96.0,
                "data_source": "yfinance_actual",
            },
            {
                "date": "2025-06-30",
                "eps_actual": 1.7,
                "eps_estimate": 1.6,
                "revenue_actual": 94.0,
                "revenue_estimate": 90.0,
                "data_source": "yfinance_actual",
            },
        ]
    )

    monkeypatch.setattr(collector, "_fetch_yahoo_fin_earnings", lambda _symbol: None)
    monkeypatch.setattr(collector, "_fetch_yf_earnings", lambda _symbol: source_frame.copy())

    payload = collector.get_earnings_surprise(symbol)
    assert payload is not None
    assert payload.get("data_source") == "yfinance_actual"
    assert payload.get("fetch_status") == "complete"
    cached = pd.read_csv(cache_file)
    assert str(cached.iloc[0]["date"]) == "2026-03-31"


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
    assert payload.get("fetch_status") == "complete"

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
    assert payload["fetch_status"] == "complete"
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
    monkeypatch.setattr(qullamaggie_screener, "load_local_ohlcv_frame", lambda market, symbol, **kwargs: sample_frame.copy())
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
    monkeypatch.setattr(
        qullamaggie_screener,
        "load_market_truth_snapshot",
        lambda *args, **kwargs: SimpleNamespace(
            market_alias="RISK_ON",
            market_alignment_score=82.0,
            breadth_support_score=78.0,
            rotation_support_score=86.0,
            leader_health_score=74.0,
            market_state="uptrend",
            breadth_state="broad_participation",
        ),
    )

    result = qullamaggie_screener.run_qullamaggie_screening(setup_type="episode_pivot", market="kr")

    assert captured["earnings_filter_enabled"] is True
    assert captured["market"] == "kr"
    assert len(result["episode_pivot"]) == 1


def test_earnings_collector_reuses_stale_cache_after_rate_limit(monkeypatch):
    cache_root = runtime_root("_test_runtime_earnings_cache_rate_limit")
    _reset_dir(cache_root)
    collector = EarningsDataCollector(cache_dir=str(cache_root), cache_duration=1)
    symbol = "AAPL"

    stale_frame = pd.DataFrame(
        [
            {
                "date": "2025-12-31",
                "eps_actual": 2.0,
                "eps_estimate": 1.7,
                "revenue_actual": 100.0,
                "revenue_estimate": 90.0,
                "data_source": "yfinance_actual",
            },
            {
                "date": "2025-09-30",
                "eps_actual": 1.8,
                "eps_estimate": 1.6,
                "revenue_actual": 95.0,
                "revenue_estimate": 88.0,
                "data_source": "yfinance_actual",
            },
            {
                "date": "2025-06-30",
                "eps_actual": 1.7,
                "eps_estimate": 1.5,
                "revenue_actual": 92.0,
                "revenue_estimate": 86.0,
                "data_source": "yfinance_actual",
            },
            {
                "date": "2025-03-31",
                "eps_actual": 1.5,
                "eps_estimate": 1.4,
                "revenue_actual": 85.0,
                "revenue_estimate": 82.0,
                "data_source": "yfinance_actual",
            },
        ]
    )

    cooldowns: list[float] = []

    monkeypatch.setattr(type(collector), "_load_disk_cache", lambda self, _symbol, fresh_only: None if fresh_only else stale_frame.copy())
    monkeypatch.setattr(type(collector), "_fetch_yahoo_fin_earnings", lambda self, _symbol: None)
    monkeypatch.setattr(type(collector), "_fetch_yf_earnings", lambda self, _symbol: (_ for _ in ()).throw(RuntimeError("HTTP Error 429: Too Many Requests")))
    import screeners.qullamaggie.earnings_data_collector as earnings_module
    monkeypatch.setattr(earnings_module, "extend_yahoo_cooldown", lambda source, seconds: cooldowns.append(seconds))

    payload = collector.get_earnings_surprise(symbol)

    assert payload is not None
    assert payload.get("data_source") == "yfinance_actual"
    assert payload.get("data_quality") == "stale_cache"
    assert payload.get("cache_fallback_status") == "rate_limited"
    assert payload.get("fetch_status") == "complete"
    assert cooldowns == []


def test_earnings_collector_returns_rate_limited_status_without_cache(monkeypatch):
    cache_root = runtime_root("_test_runtime_earnings_cache_rate_limit_no_cache")
    _reset_dir(cache_root)
    collector = EarningsDataCollector(cache_dir=str(cache_root), cache_duration=1)

    monkeypatch.setattr(type(collector), "_fetch_yahoo_fin_earnings", lambda self, _symbol: (None, None, None))
    monkeypatch.setattr(type(collector), "_fetch_yf_earnings", lambda self, _symbol: (None, "rate_limited", "rate limited"))

    payload = collector.get_earnings_surprise("AAPL")

    assert payload is not None
    assert payload["fetch_status"] == "rate_limited"
    assert payload["unavailable_reason"] == "rate limited"
    assert payload["meets_criteria"] is False


def test_earnings_collector_returns_soft_unavailable_status_without_cache(monkeypatch):
    cache_root = runtime_root("_test_runtime_earnings_cache_unavailable_no_cache")
    _reset_dir(cache_root)
    collector = EarningsDataCollector(cache_dir=str(cache_root), cache_duration=1)

    monkeypatch.setattr(type(collector), "_fetch_yahoo_fin_earnings", lambda self, _symbol: (None, None, None))
    monkeypatch.setattr(
        type(collector),
        "_fetch_yf_earnings",
        lambda self, _symbol: (None, "soft_unavailable", "earnings data unavailable"),
    )

    payload = collector.get_earnings_surprise("QQQ")

    assert payload is not None
    assert payload["fetch_status"] == "soft_unavailable"
    assert payload["unavailable_reason"] == "earnings data unavailable"
    assert payload["meets_criteria"] is False
