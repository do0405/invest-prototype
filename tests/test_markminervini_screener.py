from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from screeners.markminervini import screener as mark_screener
from tests._paths import runtime_root
from utils.runtime_context import RuntimeContext
from utils.typing_utils import to_float_or_none


def _trend_template_frame() -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=240)
    closes = np.linspace(100.0, 126.0, 240)
    highs = closes * 1.01
    lows = closes * 0.99
    opens = np.concatenate(([closes[0]], closes[:-1]))
    volumes = np.full(240, 1_500_000.0)
    return pd.DataFrame(
        {
            "date": dates,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
            "symbol": ["AAA"] * len(dates),
        }
    )


def test_calculate_trend_template_uses_125pct_52w_low_threshold() -> None:
    result = mark_screener.calculate_trend_template(_trend_template_frame())

    assert result["bars"] >= 220
    assert result["cond5"] is True


def test_run_market_screening_does_not_require_recommended_rs_gate(monkeypatch) -> None:
    root = runtime_root("_test_runtime_markminervini_screener")
    root.mkdir(parents=True, exist_ok=True)
    results_dir = root / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    results_path = results_dir / "with_rs.csv"
    frame = _trend_template_frame()

    monkeypatch.setattr(mark_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(mark_screener, "get_markminervini_results_dir", lambda market: str(results_dir))
    monkeypatch.setattr(mark_screener, "get_markminervini_with_rs_path", lambda market: str(results_path))
    monkeypatch.setattr(mark_screener, "_list_symbols", lambda market: ["AAA"])
    monkeypatch.setattr(mark_screener, "load_local_ohlcv_frame", lambda market, symbol, **kwargs: frame.copy())
    monkeypatch.setattr(
        mark_screener,
        "_resolve_rs_scores",
        lambda market, symbols, **kwargs: (pd.Series({"AAA": 60.0}), "SPY"),
    )

    result = mark_screener.run_market_screening("us")

    assert not result.empty
    assert to_float_or_none(result.loc[0, "rs_score"]) == 60.0
    assert bool(result.loc[0, "cond8"]) is False
    assert Path(results_path).exists()


def test_run_market_screening_scopes_local_frames_to_benchmark_as_of(monkeypatch) -> None:
    root = runtime_root("_test_runtime_markminervini_benchmark_asof")
    root.mkdir(parents=True, exist_ok=True)
    results_dir = root / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    results_path = results_dir / "with_rs.csv"
    frame = _trend_template_frame()
    truncated_benchmark = frame.iloc[:-5].copy()
    expected_as_of = str(pd.Timestamp(truncated_benchmark["date"].iloc[-1]).date())
    observed_as_of: list[str | None] = []
    runtime_context = RuntimeContext(market="us")

    monkeypatch.setattr(mark_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(mark_screener, "get_markminervini_results_dir", lambda market: str(results_dir))
    monkeypatch.setattr(mark_screener, "get_markminervini_with_rs_path", lambda market: str(results_path))
    monkeypatch.setattr(mark_screener, "_list_symbols", lambda market: ["AAA"])
    monkeypatch.setattr(
        mark_screener,
        "load_benchmark_data",
        lambda *args, **kwargs: ("SPY", truncated_benchmark.copy()),
    )

    def _capture_frame(market, symbol, **kwargs):  # noqa: ANN001, ANN202
        observed_as_of.append(kwargs.get("as_of"))
        return frame.copy()

    monkeypatch.setattr(mark_screener, "load_local_ohlcv_frame", _capture_frame)
    monkeypatch.setattr(
        mark_screener,
        "calculate_rs_score",
        lambda *args, **kwargs: pd.Series({"AAA": 80.0}),
    )

    result = mark_screener.run_market_screening("us", runtime_context=runtime_context)

    assert not result.empty
    assert observed_as_of
    assert set(observed_as_of) == {expected_as_of}
    freshness = runtime_context.runtime_state["data_freshness"]["stages"]["markminervini_technical"]
    assert freshness["counts"]["future_or_partial"] == 1
    assert freshness["mode"] == "default_completed_session"
    assert freshness["examples"][0]["symbol"] == "AAA"


def test_run_market_screening_uses_ordered_frame_loader_for_rs_universe(monkeypatch) -> None:
    root = runtime_root("_test_runtime_markminervini_parallel_loader")
    root.mkdir(parents=True, exist_ok=True)
    results_dir = root / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    results_path = results_dir / "with_rs.csv"
    frame = _trend_template_frame()
    calls: list[tuple[str, tuple[str, ...]]] = []

    monkeypatch.setattr(mark_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(mark_screener, "get_markminervini_results_dir", lambda market: str(results_dir))
    monkeypatch.setattr(mark_screener, "get_markminervini_with_rs_path", lambda market: str(results_path))
    monkeypatch.setattr(mark_screener, "_list_symbols", lambda market: ["BBB", "AAA"])
    monkeypatch.setattr(
        mark_screener,
        "load_benchmark_data",
        lambda *args, **kwargs: ("SPY", frame.copy()),
    )

    def _ordered_loader(market, symbols, **kwargs):  # noqa: ANN001, ANN202
        calls.append((market, tuple(symbols)))
        return {symbol: frame.assign(symbol=symbol).copy() for symbol in symbols}

    monkeypatch.setattr(mark_screener, "load_local_ohlcv_frames_ordered", _ordered_loader)
    monkeypatch.setattr(
        mark_screener,
        "calculate_rs_score",
        lambda *args, **kwargs: pd.Series({"AAA": 80.0, "BBB": 75.0}),
    )

    result = mark_screener.run_market_screening("us")

    assert calls == [("us", ("BBB", "AAA"))]
    assert sorted(result["symbol"].tolist()) == ["AAA", "BBB"]
