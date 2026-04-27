from __future__ import annotations

import numpy as np
import pandas as pd

from screeners.tradingview import screener as tradingview_screener
from screeners.tradingview.screener import _average_traded_value
from tests._paths import runtime_root
from utils.runtime_context import RuntimeContext


def test_average_traded_value_uses_typical_price():
    frame = pd.DataFrame(
        {
            "high": [12.0, 15.0, 18.0],
            "low": [6.0, 9.0, 12.0],
            "close": [9.0, 12.0, 15.0],
            "volume": [100.0, 200.0, 300.0],
        }
    )

    result = _average_traded_value(frame, window=3)

    expected = (((12.0 + 6.0 + 9.0) / 3.0) * 100.0 + (((15.0 + 9.0 + 12.0) / 3.0) * 200.0) + (((18.0 + 12.0 + 15.0) / 3.0) * 300.0)) / 3.0
    assert result == expected


def _daily_frame(length: int = 140) -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=length)
    closes = np.linspace(50.0, 90.0, length)
    return pd.DataFrame(
        {
            "date": dates,
            "open": closes * 0.99,
            "high": closes * 1.02,
            "low": closes * 0.98,
            "close": closes,
            "volume": np.full(length, 2_000_000.0),
        }
    )


def test_run_tradingview_preset_screeners_uses_market_specific_presets(monkeypatch):
    output_root = runtime_root("_test_runtime_tradingview_presets")
    output_root.mkdir(parents=True, exist_ok=True)
    data_root = runtime_root("_test_runtime_tradingview_data")
    data_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(tradingview_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(tradingview_screener, "get_tradingview_results_dir", lambda market: str(output_root / market))
    monkeypatch.setattr(tradingview_screener, "get_market_data_dir", lambda market: str(data_root / market))
    monkeypatch.setattr(tradingview_screener, "load_local_ohlcv_frame", lambda market, symbol, **kwargs: _daily_frame())

    for market in ("us", "kr"):
        market_dir = data_root / market
        market_dir.mkdir(parents=True, exist_ok=True)
        (market_dir / "TEST.csv").write_text("date,symbol,close\n2025-01-02,TEST,1\n", encoding="utf-8")

    us_results = tradingview_screener.run_tradingview_preset_screeners(market="us")
    kr_results = tradingview_screener.run_tradingview_preset_screeners(market="kr")

    assert us_results
    assert kr_results
    assert all(key.startswith("us_") for key in us_results)
    assert all(key.startswith("kr_") for key in kr_results)
    assert {
        "us_breakout_rvol",
        "us_breakout_10m",
        "us_breakout_strength",
        "us_market_leader",
    }.issubset(set(us_results))
    assert set(kr_results) == {
        "kr_breakout_rvol",
        "kr_market_leader",
    }


def test_run_tradingview_preset_screeners_scopes_local_frames_to_benchmark_as_of(monkeypatch):
    output_root = runtime_root("_test_runtime_tradingview_benchmark_asof")
    output_root.mkdir(parents=True, exist_ok=True)
    data_root = runtime_root("_test_runtime_tradingview_benchmark_data")
    data_root.mkdir(parents=True, exist_ok=True)
    frame = _daily_frame()
    truncated_benchmark = frame.iloc[:-5].copy()
    expected_as_of = str(pd.Timestamp(truncated_benchmark["date"].iloc[-1]).date())
    observed_as_of: list[str | None] = []

    monkeypatch.setattr(tradingview_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(tradingview_screener, "get_tradingview_results_dir", lambda market: str(output_root / market))
    monkeypatch.setattr(tradingview_screener, "get_market_data_dir", lambda market: str(data_root / market))
    monkeypatch.setattr(
        tradingview_screener,
        "load_benchmark_data",
        lambda *args, **kwargs: ("SPY", truncated_benchmark.copy()),
    )

    def _capture_frame(market, symbol, **kwargs):  # noqa: ANN001, ANN202
        observed_as_of.append(kwargs.get("as_of"))
        return frame.copy()

    monkeypatch.setattr(tradingview_screener, "load_local_ohlcv_frame", _capture_frame)

    market_dir = data_root / "us"
    market_dir.mkdir(parents=True, exist_ok=True)
    (market_dir / "TEST.csv").write_text("date,symbol,close\n2025-01-02,TEST,1\n", encoding="utf-8")

    results = tradingview_screener.run_tradingview_preset_screeners(market="us")

    assert results
    assert observed_as_of
    assert set(observed_as_of) == {expected_as_of}


def test_run_tradingview_preset_screeners_preserves_explicit_runtime_as_of(monkeypatch):
    output_root = runtime_root("_test_runtime_tradingview_explicit_asof")
    output_root.mkdir(parents=True, exist_ok=True)
    data_root = runtime_root("_test_runtime_tradingview_explicit_data")
    data_root.mkdir(parents=True, exist_ok=True)
    frame = _daily_frame()
    explicit_as_of = str(pd.Timestamp(frame["date"].iloc[-8]).date())
    benchmark_latest = str(pd.Timestamp(frame["date"].iloc[-1]).date())
    assert explicit_as_of != benchmark_latest
    observed_as_of: list[str | None] = []
    runtime_context = RuntimeContext(market="us", as_of_date=explicit_as_of)

    monkeypatch.setattr(tradingview_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(tradingview_screener, "get_tradingview_results_dir", lambda market: str(output_root / market))
    monkeypatch.setattr(tradingview_screener, "get_market_data_dir", lambda market: str(data_root / market))
    monkeypatch.setattr(
        tradingview_screener,
        "load_benchmark_data",
        lambda *args, **kwargs: ("SPY", frame.copy()),
    )

    def _capture_frame(market, symbol, **kwargs):  # noqa: ANN001, ANN202
        observed_as_of.append(kwargs.get("as_of"))
        return frame.copy()

    monkeypatch.setattr(tradingview_screener, "load_local_ohlcv_frame", _capture_frame)

    market_dir = data_root / "us"
    market_dir.mkdir(parents=True, exist_ok=True)
    (market_dir / "TEST.csv").write_text("date,symbol,close\n2025-01-02,TEST,1\n", encoding="utf-8")

    results = tradingview_screener.run_tradingview_preset_screeners(
        market="us",
        runtime_context=runtime_context,
    )

    assert results
    assert runtime_context.as_of_date == explicit_as_of
    assert observed_as_of
    assert set(observed_as_of) == {explicit_as_of}
    freshness = runtime_context.runtime_state["data_freshness"]["stages"]["tradingview_presets"]
    assert freshness["counts"]["future_or_partial"] == 1
    assert freshness["mode"] == "explicit_replay"


def test_preset_definitions_keep_kr_at_two_and_us_at_least_four():
    us_presets = tradingview_screener._preset_definitions_for_market("us")
    kr_presets = tradingview_screener._preset_definitions_for_market("kr")

    assert len(us_presets) >= 4
    assert len(kr_presets) == 2
    assert {
        "us_breakout_rvol",
        "us_breakout_10m",
        "us_breakout_strength",
        "us_market_leader",
    }.issubset({preset.key for preset in us_presets})
    assert {preset.key for preset in kr_presets} == {
        "kr_breakout_rvol",
        "kr_market_leader",
    }
