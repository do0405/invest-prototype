from __future__ import annotations

import numpy as np
import pandas as pd

from screeners.weinstein_stage2 import screener as weinstein_screener
from screeners.weinstein_stage2.screener import (
    GroupContext,
    MarketContext,
    WeinsteinStage2Screener,
    WeinsteinStage2Analyzer,
)
from tests._paths import runtime_root


def _weekly_to_daily(
    closes: np.ndarray,
    volumes: np.ndarray,
    *,
    start: str = "2024-01-01",
    spread: float = 0.002,
) -> pd.DataFrame:
    rows: list[dict[str, float | pd.Timestamp]] = []
    week_start = pd.Timestamp(start)
    prev_close = float(closes[0])
    for close, volume in zip(closes, volumes):
        week_open = prev_close
        for day_index in range(5):
            date = week_start + pd.Timedelta(days=day_index)
            fraction = (day_index + 1) / 5.0
            day_close = week_open + (float(close) - week_open) * fraction
            day_open = week_open if day_index == 0 else float(rows[-1]["close"])
            rows.append(
                {
                    "date": date,
                    "open": day_open,
                    "high": max(day_open, day_close) * (1.0 + spread),
                    "low": min(day_open, day_close) * (1.0 - spread),
                    "close": day_close,
                    "volume": float(volume) / 5.0,
                }
            )
        prev_close = float(close)
        week_start += pd.Timedelta(days=7)
    return pd.DataFrame(rows)


def _benchmark_daily() -> pd.DataFrame:
    closes = np.linspace(100.0, 108.0, 60)
    volumes = np.linspace(1_000_000, 1_100_000, 60)
    return _weekly_to_daily(closes, volumes)


def _market_context() -> MarketContext:
    return MarketContext(
        benchmark_symbol="SPY",
        market_state="MARKET_STAGE2_FAVORABLE",
        breadth150_market=62.0,
        benchmark_close=108.0,
        ma30w=105.0,
        ma30w_slope_4w=0.002,
        market_score=100.0,
    )


def _group_context() -> GroupContext:
    return GroupContext(
        group_name="Tech",
        group_state="GROUP_STRONG",
        breadth150_group=68.0,
        group_mrs=5.0,
        group_rp_ma52_slope=0.01,
        group_score=100.0,
        data_available=True,
    )


def _stage1_base_closes() -> np.ndarray:
    return np.array(
        [
            80.0, 82.0, 85.0, 88.0, 92.0,
            95.0, 98.0, 100.0, 101.0, 100.0,
            99.0, 100.0, 101.0, 100.0, 99.0,
            100.0, 101.0, 100.0, 99.0, 100.0,
            101.0, 100.0, 101.0, 100.5, 101.2,
            101.0, 100.8, 101.4, 101.6, 101.8,
            102.0, 101.9, 101.8, 101.9, 101.95,
            101.8, 101.9, 101.85, 101.95, 101.7,
            101.8, 101.9, 101.7, 101.85, 101.95,
        ]
    )


def test_weekly_builder_uses_actual_last_trading_day() -> None:
    analyzer = WeinsteinStage2Analyzer()
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2026-01-05",
                    "2026-01-06",
                    "2026-01-07",
                    "2026-01-08",
                    "2026-01-12",
                    "2026-01-13",
                ]
            ),
            "open": [10, 11, 12, 13, 14, 15],
            "high": [11, 12, 13, 14, 15, 16],
            "low": [9, 10, 11, 12, 13, 14],
            "close": [10.5, 11.5, 12.5, 13.5, 14.5, 15.5],
            "volume": [100, 110, 120, 130, 140, 150],
        }
    )

    weekly = analyzer.build_weekly_bars(frame)

    assert len(weekly) == 2
    assert weekly.iloc[0]["session_count"] == 4
    assert str(pd.Timestamp(weekly.iloc[0]["bar_end_date"]).date()) == "2026-01-08"


def test_analyze_symbol_classifies_pre_stage2_high() -> None:
    analyzer = WeinsteinStage2Analyzer()
    closes = _stage1_base_closes()
    volumes = np.linspace(700_000, 500_000, len(closes))
    daily = _weekly_to_daily(closes, volumes)

    result = analyzer.analyze_symbol(
        symbol="AAA",
        market="us",
        daily_frame=daily,
        benchmark_symbol="SPY",
        benchmark_daily=_benchmark_daily(),
        market_context=_market_context(),
        group_context=_group_context(),
        exchange="NASDAQ",
        sector="Tech",
        industry_group="Software",
    )

    assert result["timing_state"] == "PRE_STAGE2_HIGH"
    assert result["stock_stage"] == "STAGE_1"
    assert result["percent_to_stage2"] is not None and result["percent_to_stage2"] <= 1.0
    assert result["priority_label"] == "P1"


def test_analyze_symbol_classifies_breakout_week_and_fresh_w1() -> None:
    analyzer = WeinsteinStage2Analyzer()
    base = _stage1_base_closes()

    breakout_closes = np.concatenate([base, [102.2, 104.8]])
    breakout_volumes = np.linspace(700_000, 500_000, len(breakout_closes))
    breakout_volumes[-1] = 2_600_000
    breakout_daily = _weekly_to_daily(breakout_closes, breakout_volumes)

    breakout_result = analyzer.analyze_symbol(
        symbol="BBB",
        market="us",
        daily_frame=breakout_daily,
        benchmark_symbol="SPY",
        benchmark_daily=_benchmark_daily(),
        market_context=_market_context(),
        group_context=_group_context(),
        exchange="NASDAQ",
        sector="Tech",
        industry_group="Software",
    )

    assert breakout_result["timing_state"] == "BREAKOUT_WEEK"
    assert breakout_result["stock_stage"] == "STAGE_2A"
    assert breakout_result["breakout_age_weeks"] == 0

    fresh_w1_closes = np.concatenate([base, [102.2, 104.8, 105.4]])
    fresh_w1_volumes = np.linspace(700_000, 500_000, len(fresh_w1_closes))
    fresh_w1_volumes[-2] = 2_600_000
    fresh_w1_volumes[-1] = 1_100_000
    fresh_w1_daily = _weekly_to_daily(fresh_w1_closes, fresh_w1_volumes)

    fresh_w1_result = analyzer.analyze_symbol(
        symbol="CCC",
        market="us",
        daily_frame=fresh_w1_daily,
        benchmark_symbol="SPY",
        benchmark_daily=_benchmark_daily(),
        market_context=_market_context(),
        group_context=_group_context(),
        exchange="NASDAQ",
        sector="Tech",
        industry_group="Software",
    )

    assert fresh_w1_result["timing_state"] == "FRESH_STAGE2_W1"
    assert fresh_w1_result["stock_stage"] == "STAGE_2A"
    assert fresh_w1_result["breakout_age_weeks"] == 1


def test_run_persists_pattern_excluded_and_included_outputs(monkeypatch) -> None:
    output_root = runtime_root("_test_runtime_weinstein_stage2_outputs")
    output_root.mkdir(parents=True, exist_ok=True)

    metadata = pd.DataFrame(
        [
            {"symbol": "AAA", "sector": "Tech", "industry": "Software", "exchange": "NASDAQ"},
            {"symbol": "BBB", "sector": "Tech", "industry": "Software", "exchange": "NASDAQ"},
        ]
    )
    frames = {
        "AAA": _weekly_to_daily(_stage1_base_closes(), np.linspace(700_000, 500_000, len(_stage1_base_closes()))),
        "BBB": _weekly_to_daily(
            np.concatenate([_stage1_base_closes(), [102.2, 104.8]]),
            np.concatenate([np.linspace(700_000, 500_000, len(_stage1_base_closes())), np.array([520_000.0, 2_600_000.0])]),
        ),
    }

    monkeypatch.setattr(weinstein_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(weinstein_screener, "get_weinstein_stage2_results_dir", lambda market: str(output_root))
    monkeypatch.setattr(WeinsteinStage2Screener, "_load_metadata", lambda self: metadata.copy())
    monkeypatch.setattr(WeinsteinStage2Screener, "_load_daily_frames", lambda self: {key: value.copy() for key, value in frames.items()})
    monkeypatch.setattr(
        weinstein_screener,
        "load_benchmark_data",
        lambda *args, **kwargs: ("SPY", _benchmark_daily()),
    )

    result = weinstein_screener.run_weinstein_stage2_screening(market="us")

    assert not result.empty
    assert "phase_bucket" in result.columns
    assert (output_root / "pattern_excluded_pool.csv").exists()
    assert (output_root / "pattern_included_candidates.csv").exists()
    assert (output_root / "actual_data_calibration.json").exists()
