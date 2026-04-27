from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import threading
import time

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


def test_earnings_collector_serializes_live_fetch_per_symbol(monkeypatch):
    cache_root = runtime_root("_test_runtime_earnings_cache_parallel_symbol")
    _reset_dir(cache_root)
    collector = EarningsDataCollector(cache_dir=str(cache_root), cache_duration=3600)
    symbol = "AAPL"
    fetch_calls: list[str] = []
    fetch_lock = threading.Lock()
    results: list[dict[str, object] | None] = []

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

    import screeners.qullamaggie.earnings_data_collector as earnings_module

    monkeypatch.setattr(
        earnings_module,
        "get_security_profile",
        lambda _symbol, _market: {
            "earnings_expected": True,
            "preferred_provider_symbol": symbol,
            "provider_symbol": symbol,
            "earnings_anchor_symbol": symbol,
            "issuer_symbol": symbol,
        },
    )
    monkeypatch.setattr(type(collector), "_fetch_yahoo_fin_earnings", lambda self, _symbol: None)

    def _slow_fetch(self, _symbol):
        with fetch_lock:
            fetch_calls.append(_symbol)
        time.sleep(0.15)
        return source_frame.copy()

    monkeypatch.setattr(type(collector), "_fetch_yf_earnings", _slow_fetch)

    def _worker() -> None:
        results.append(collector.get_earnings_surprise(symbol))

    threads = [threading.Thread(target=_worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(fetch_calls) == 1
    assert len(results) == 2
    assert all(result is not None for result in results)
    assert all(result.get("fetch_status") == "complete" for result in results if result is not None)


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
    real_build_context = qullamaggie_screener._build_context

    def _build_context_with_ep_universe(*args, **kwargs):  # noqa: ANN002, ANN003
        context = real_build_context(*args, **kwargs)
        if "005930" in context.get("feature_map", {}):
            context["feature_map"]["005930"] = {
                **context["feature_map"]["005930"],
                "ep_universe_pass": True,
            }
        return context

    monkeypatch.setattr(qullamaggie_screener, "_build_context", _build_context_with_ep_universe)
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

    import screeners.qullamaggie.earnings_data_collector as earnings_module

    monkeypatch.setattr(
        earnings_module,
        "get_security_profile",
        lambda symbol, market: {
            "symbol": symbol,
            "provider_symbol": symbol,
            "preferred_provider_symbol": symbol,
            "earnings_expected": True,
            "fundamentals_expected": True,
            "earnings_skip_reason": "",
            "security_type": "EQUITY",
        },
    )
    monkeypatch.setattr(type(collector), "_fetch_yahoo_fin_earnings", lambda self, _symbol: (None, None, None))
    monkeypatch.setattr(
        type(collector),
        "_fetch_yf_earnings",
        lambda self, _symbol: (None, "soft_unavailable", "earnings data unavailable"),
    )

    payload = collector.get_earnings_surprise("AAPL")

    assert payload is not None
    assert payload["fetch_status"] == "soft_unavailable"
    assert payload["unavailable_reason"] == "earnings data unavailable"
    assert payload["meets_criteria"] is False


def test_earnings_collector_prefers_kr_metadata_provider_symbol(monkeypatch):
    collector = EarningsDataCollector(cache_duration=3600, market="kr")
    calls: list[str] = []

    history = pd.DataFrame(
        {
            "Earnings Date": ["2026-03-13", "2025-12-13", "2025-09-13", "2025-06-13"],
            "Reported EPS": [1.2, 1.1, 1.0, 0.9],
            "EPS Estimate": [1.0, 0.9, 0.8, 0.7],
        }
    )

    class _Ticker:
        def __init__(self, provider_symbol: str) -> None:
            calls.append(provider_symbol)
            self.earnings_history = history.copy()

    import screeners.qullamaggie.earnings_data_collector as earnings_module

    monkeypatch.setattr(earnings_module, "get_preferred_provider_symbol", lambda symbol, market: "005930.KS")
    monkeypatch.setattr(earnings_module, "wait_for_yahoo_request_slot", lambda *args, **kwargs: None)
    monkeypatch.setattr(earnings_module.yf, "Ticker", _Ticker)

    frame, status, reason = collector._fetch_yf_earnings("005930")

    assert frame is not None and not frame.empty
    assert status == "complete"
    assert reason is None
    assert calls == ["005930.KS"]
    assert str(frame.loc[0, "data_source"]) == "yfinance_actual:005930.KS"


def test_earnings_collector_falls_back_to_yfinance_earnings_dates(monkeypatch):
    collector = EarningsDataCollector(cache_dir=str(runtime_root("_test_runtime_earnings_dates_fallback")), cache_duration=3600)

    earnings_dates = pd.DataFrame(
        {
            "EPS Estimate": [1.1, 1.0, 0.9, 0.8],
            "Reported EPS": [1.3, 1.2, 1.0, 0.85],
            "Surprise(%)": [18.0, 20.0, 11.1, 6.25],
        },
        index=pd.to_datetime(["2026-03-12", "2025-12-12", "2025-09-12", "2025-06-12"]),
    )

    class _Ticker:
        def __init__(self, provider_symbol: str) -> None:
            self.provider_symbol = provider_symbol
            self.earnings_history = pd.DataFrame()

        def get_earnings_dates(self, limit: int = 8, offset: int = 1) -> pd.DataFrame:
            assert limit == 8
            assert offset == 1
            return earnings_dates.copy()

        @property
        def calendar(self):  # pragma: no cover - should not be used in this test
            raise AssertionError("calendar should not be used when earnings_dates resolved")

    import screeners.qullamaggie.earnings_data_collector as earnings_module

    monkeypatch.setattr(earnings_module, "wait_for_yahoo_request_slot", lambda *args, **kwargs: None)
    monkeypatch.setattr(earnings_module.yf, "Ticker", _Ticker)

    frame, status, reason = collector._fetch_yf_earnings("AAOI")

    assert frame is not None and not frame.empty
    assert status == "complete"
    assert reason is None
    assert str(frame.loc[0, "data_source"]) == "yfinance_earnings_dates:AAOI"


def test_earnings_collector_continues_yfinance_ladder_after_history_exception(monkeypatch):
    collector = EarningsDataCollector(cache_dir=str(runtime_root("_test_runtime_earnings_dates_after_history_error")), cache_duration=3600)

    earnings_dates = pd.DataFrame(
        {
            "EPS Estimate": [1.1, 1.0, 0.9, 0.8],
            "Reported EPS": [1.3, 1.2, 1.0, 0.85],
            "Surprise(%)": [18.0, 20.0, 11.1, 6.25],
        },
        index=pd.to_datetime(["2026-03-12", "2025-12-12", "2025-09-12", "2025-06-12"]),
    )

    class _Ticker:
        def __init__(self, provider_symbol: str) -> None:
            self.provider_symbol = provider_symbol

        @property
        def earnings_history(self):  # noqa: ANN201
            raise IndexError("list index out of range")

        def get_earnings_dates(self, limit: int = 8, offset: int = 1) -> pd.DataFrame:
            assert limit == 8
            assert offset == 1
            return earnings_dates.copy()

        @property
        def calendar(self):  # pragma: no cover - should not be used when earnings_dates resolved
            raise AssertionError("calendar should not be used when earnings_dates resolved")

    import screeners.qullamaggie.earnings_data_collector as earnings_module

    monkeypatch.setattr(earnings_module, "wait_for_yahoo_request_slot", lambda *args, **kwargs: None)
    monkeypatch.setattr(earnings_module.yf, "Ticker", _Ticker)

    frame, status, reason = collector._fetch_yf_earnings("AAOI")

    assert frame is not None and not frame.empty
    assert status == "complete"
    assert reason is None
    assert str(frame.loc[0, "data_source"]) == "yfinance_earnings_dates:AAOI"


def test_earnings_collector_returns_date_only_payload_from_calendar(monkeypatch):
    cache_root = runtime_root("_test_runtime_earnings_calendar_date_only")
    _reset_dir(cache_root)
    collector = EarningsDataCollector(cache_dir=str(cache_root), cache_duration=3600)

    class _Ticker:
        def __init__(self, provider_symbol: str) -> None:
            self.earnings_history = pd.DataFrame()

        def get_earnings_dates(self, limit: int = 8, offset: int = 1) -> pd.DataFrame:
            return pd.DataFrame()

        @property
        def calendar(self):
            return {"Earnings Date": [pd.Timestamp("2026-03-20")]}

    import screeners.qullamaggie.earnings_data_collector as earnings_module

    monkeypatch.setattr(type(collector), "_fetch_yahoo_fin_earnings", lambda self, _symbol: (None, None, None))
    monkeypatch.setattr(earnings_module, "wait_for_yahoo_request_slot", lambda *args, **kwargs: None)
    monkeypatch.setattr(earnings_module.yf, "Ticker", _Ticker)

    payload = collector.get_earnings_surprise("AAOI")

    assert payload is not None
    assert payload["fetch_status"] == "complete"
    assert payload["data_quality"] == "date_only"
    assert payload["earnings_date"] == "2026-03-20"


def test_earnings_collector_treats_empty_calendar_list_as_no_event_announced(monkeypatch):
    cache_root = runtime_root("_test_runtime_earnings_calendar_empty_list")
    _reset_dir(cache_root)
    collector = EarningsDataCollector(cache_dir=str(cache_root), cache_duration=3600, market="kr")

    class _Ticker:
        def __init__(self, provider_symbol: str) -> None:
            self.provider_symbol = provider_symbol
            self.earnings_history = pd.DataFrame()

        def get_earnings_dates(self, limit: int = 8, offset: int = 1):  # noqa: ANN202
            return None

        @property
        def calendar(self):
            return {"Earnings Date": []}

    import screeners.qullamaggie.earnings_data_collector as earnings_module

    monkeypatch.setattr(
        earnings_module,
        "get_security_profile",
        lambda symbol, market: {
            "symbol": symbol,
            "provider_symbol": "000040.KS",
            "preferred_provider_symbol": "000040.KS",
            "earnings_expected": True,
            "fundamentals_expected": True,
            "earnings_skip_reason": "",
        },
    )
    monkeypatch.setattr(earnings_module, "get_preferred_provider_symbol", lambda symbol, market: "000040.KS")
    monkeypatch.setattr(type(collector), "_fetch_yahoo_fin_earnings", lambda self, _symbol: (None, None, None))
    monkeypatch.setattr(type(collector), "_fetch_kr_fnguide_event", lambda self, _symbol: ("no_event_announced", "no event announced", None))
    monkeypatch.setattr(earnings_module, "wait_for_yahoo_request_slot", lambda *args, **kwargs: None)
    monkeypatch.setattr(earnings_module.yf, "Ticker", _Ticker)

    payload = collector.get_earnings_surprise("000040")

    assert payload is not None
    assert payload["fetch_status"] == "no_event_announced"
    assert payload["unavailable_reason"] == "no event announced"
    assert payload["data_source"] == "fnguide_earning_issue"


def test_earnings_collector_skips_ineligible_metadata_symbol(monkeypatch):
    collector = EarningsDataCollector(cache_duration=3600)

    import screeners.qullamaggie.earnings_data_collector as earnings_module

    monkeypatch.setattr(
        earnings_module,
        "get_security_profile",
        lambda symbol, market: {
            "symbol": symbol,
            "provider_symbol": symbol,
            "earnings_expected": False,
            "fundamentals_expected": False,
            "earnings_skip_reason": "etf",
            "security_type": "ETF",
        },
    )
    monkeypatch.setattr(type(collector), "_fetch_yahoo_fin_earnings", lambda self, _symbol: (_ for _ in ()).throw(AssertionError("should skip yahoo_fin")))
    monkeypatch.setattr(type(collector), "_fetch_yf_earnings", lambda self, _symbol: (_ for _ in ()).throw(AssertionError("should skip yfinance")))

    payload = collector.get_earnings_surprise("AAAU")

    assert payload is not None
    assert payload["fetch_status"] == "not_expected"
    assert payload["data_source"] == "metadata_skip"
    assert payload["unavailable_reason"] == "ineligible:etf"


def test_earnings_collector_downgrades_yahoo_fin_indexerror_when_yfinance_has_no_data(monkeypatch):
    cache_root = runtime_root("_test_runtime_yahoo_fin_indexerror_downgrade")
    _reset_dir(cache_root)
    collector = EarningsDataCollector(cache_dir=str(cache_root), cache_duration=3600, market="kr")

    import screeners.qullamaggie.earnings_data_collector as earnings_module

    monkeypatch.setattr(
        earnings_module,
        "get_security_profile",
        lambda symbol, market: {
            "symbol": symbol,
            "provider_symbol": "000040.KS",
            "preferred_provider_symbol": "000040.KS",
            "earnings_expected": True,
            "fundamentals_expected": True,
            "earnings_skip_reason": "",
        },
    )
    monkeypatch.setattr(earnings_module, "YAHOO_FIN_AVAILABLE", True)
    monkeypatch.setattr(earnings_module, "wait_for_yahoo_request_slot", lambda *args, **kwargs: None)
    monkeypatch.setattr(earnings_module.si, "get_earnings_history", lambda _symbol: (_ for _ in ()).throw(IndexError("list index out of range")))
    monkeypatch.setattr(type(collector), "_fetch_yf_earnings", lambda self, _symbol: (None, "soft_unavailable", "earnings data unavailable"))
    monkeypatch.setattr(type(collector), "_fetch_kr_fnguide_event", lambda self, _symbol: ("no_event_announced", "no event announced", None))

    payload = collector.get_earnings_surprise("000040")

    assert payload is not None
    assert payload["fetch_status"] == "no_event_announced"
    assert payload["unavailable_reason"] == "no event announced"


def test_earnings_collector_inherits_kr_anchor_event_for_preferred_share(monkeypatch):
    cache_root = runtime_root("_test_runtime_kr_anchor_inherit")
    _reset_dir(cache_root)
    collector = EarningsDataCollector(cache_dir=str(cache_root), cache_duration=3600, market="kr")

    anchor_frame = pd.DataFrame(
        [
            {
                "date": "2026-03-12",
                "eps_actual": 1.4,
                "eps_estimate": 1.2,
                "revenue_actual": pd.NA,
                "revenue_estimate": pd.NA,
                "data_source": "yahoo_fin_actual:000150.KS",
                "provider_symbol": "000150.KS",
            },
            {
                "date": "2025-12-12",
                "eps_actual": 1.2,
                "eps_estimate": 1.1,
                "revenue_actual": pd.NA,
                "revenue_estimate": pd.NA,
                "data_source": "yahoo_fin_actual:000150.KS",
                "provider_symbol": "000150.KS",
            },
            {
                "date": "2025-09-12",
                "eps_actual": 1.1,
                "eps_estimate": 1.0,
                "revenue_actual": pd.NA,
                "revenue_estimate": pd.NA,
                "data_source": "yahoo_fin_actual:000150.KS",
                "provider_symbol": "000150.KS",
            },
            {
                "date": "2025-06-12",
                "eps_actual": 1.0,
                "eps_estimate": 0.9,
                "revenue_actual": pd.NA,
                "revenue_estimate": pd.NA,
                "data_source": "yahoo_fin_actual:000150.KS",
                "provider_symbol": "000150.KS",
            },
        ]
    )

    import screeners.qullamaggie.earnings_data_collector as earnings_module

    monkeypatch.setattr(
        earnings_module,
        "get_security_profile",
        lambda symbol, market: {
            "symbol": symbol,
            "provider_symbol": "000155.KS",
            "preferred_provider_symbol": "000155.KS",
            "earnings_expected": True,
            "fundamentals_expected": True,
            "earnings_skip_reason": "",
            "share_class_type": "PREFERRED",
            "issuer_symbol": "000150",
            "earnings_anchor_symbol": "000150",
        },
    )
    monkeypatch.setattr(
        type(collector),
        "_fetch_yahoo_fin_earnings",
        lambda self, symbol: (anchor_frame.copy(), "complete", None)
        if symbol == "000150"
        else (None, "soft_unavailable", "earnings data unavailable"),
    )
    monkeypatch.setattr(
        type(collector),
        "_fetch_yf_earnings",
        lambda self, _symbol: (None, "soft_unavailable", "earnings data unavailable"),
    )

    payload = collector.get_earnings_surprise("000155")

    assert payload is not None
    assert payload["fetch_status"] == "complete"
    assert payload["earnings_date"] == "2026-03-12"
    assert payload["resolution_class"] == "inherited_event"
    diagnostics = collector.provider_diagnostics_rows()
    assert diagnostics[0]["symbol"] == "000155"
    assert diagnostics[0]["anchor_symbol"] == "000150"
    assert diagnostics[0]["resolution_class"] == "inherited_event"
    summary = collector.provider_diagnostics_summary()
    assert summary["counts"]["live_provider_attempts"] == 2
    assert summary["timings"]["provider_fetch_seconds"] >= 0.0


def test_earnings_collector_resolves_kr_event_proxy_from_fnguide(monkeypatch):
    cache_root = runtime_root("_test_runtime_kr_fnguide_event_proxy")
    _reset_dir(cache_root)
    collector = EarningsDataCollector(cache_dir=str(cache_root), cache_duration=3600, market="kr")

    class _Response:
        status_code = 200
        text = """
        <html>
        <table>
        <tr><th>Date of Scheduled Disclosure</th><td>2026/04/28</td></tr>
        </table>
        </html>
        """

        def raise_for_status(self) -> None:
            return None

    import screeners.qullamaggie.earnings_data_collector as earnings_module

    monkeypatch.setattr(
        earnings_module,
        "get_security_profile",
        lambda symbol, market: {
            "symbol": symbol,
            "provider_symbol": "000430.KS",
            "preferred_provider_symbol": "000430.KS",
            "earnings_expected": True,
            "fundamentals_expected": True,
            "earnings_skip_reason": "",
            "share_class_type": "COMMON",
            "issuer_symbol": "",
            "earnings_anchor_symbol": "",
        },
    )
    monkeypatch.setattr(type(collector), "_fetch_yahoo_fin_earnings", lambda self, _symbol: (None, "soft_unavailable", "earnings data unavailable"))
    monkeypatch.setattr(type(collector), "_fetch_yf_earnings", lambda self, _symbol: (None, "soft_unavailable", "earnings data unavailable"))
    monkeypatch.setattr(earnings_module, "requests", SimpleNamespace(get=lambda *args, **kwargs: _Response()), raising=False)

    payload = collector.get_earnings_surprise("000430")

    assert payload is not None
    assert payload["fetch_status"] == "complete"
    assert payload["data_quality"] == "event_proxy"
    assert payload["data_source"] == "fnguide_earning_issue"
    assert payload["earnings_date"] == "2026-04-28"
    assert payload["resolution_class"] == "exact_event"
    diagnostics = collector.provider_diagnostics_rows()
    assert diagnostics[0]["resolution_class"] == "exact_event"
    assert diagnostics[0]["resolved_source"] == "fnguide_earning_issue"


def test_earnings_collector_classifies_fnguide_undecided_as_normal_terminal_state(monkeypatch):
    cache_root = runtime_root("_test_runtime_kr_fnguide_undecided")
    _reset_dir(cache_root)
    collector = EarningsDataCollector(cache_dir=str(cache_root), cache_duration=3600, market="kr")

    class _Response:
        status_code = 200
        text = """
        <html>
        <table>
        <tr><th>Date of Scheduled Disclosure</th><td>Undecided</td></tr>
        </table>
        </html>
        """

        def raise_for_status(self) -> None:
            return None

    import screeners.qullamaggie.earnings_data_collector as earnings_module

    monkeypatch.setattr(
        earnings_module,
        "get_security_profile",
        lambda symbol, market: {
            "symbol": symbol,
            "provider_symbol": "000670.KS",
            "preferred_provider_symbol": "000670.KS",
            "earnings_expected": True,
            "fundamentals_expected": True,
            "earnings_skip_reason": "",
            "share_class_type": "COMMON",
            "issuer_symbol": "",
            "earnings_anchor_symbol": "",
        },
    )
    monkeypatch.setattr(type(collector), "_fetch_yahoo_fin_earnings", lambda self, _symbol: (None, "soft_unavailable", "earnings data unavailable"))
    monkeypatch.setattr(type(collector), "_fetch_yf_earnings", lambda self, _symbol: (None, "soft_unavailable", "earnings data unavailable"))
    monkeypatch.setattr(earnings_module, "requests", SimpleNamespace(get=lambda *args, **kwargs: _Response()), raising=False)

    payload = collector.get_earnings_surprise("000670")

    assert payload is not None
    assert payload["fetch_status"] == "scheduled_undecided"
    assert payload["data_source"] == "fnguide_earning_issue"
    assert payload["earnings_date"] is None
    assert payload["resolution_class"] == "scheduled_undecided"
    diagnostics = collector.provider_diagnostics_rows()
    assert diagnostics[0]["resolution_class"] == "scheduled_undecided"
    assert diagnostics[0]["terminal_status"] == "scheduled_undecided"
