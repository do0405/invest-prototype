from __future__ import annotations

import json
import numpy as np
import pandas as pd

from screeners import leader_core_bridge
from screeners.weinstein_stage2 import screener as weinstein_screener
from screeners.leader_core_bridge import build_industry_key
from screeners.weinstein_stage2.screener import (
    GroupContext,
    MarketContext,
    WeinsteinStage2Screener,
    WeinsteinStage2Analyzer,
)
from tests._paths import runtime_root
from utils.runtime_context import RuntimeContext


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
        industry_key=build_industry_key("Tech", "Software"),
        group_name="Tech",
        group_state="STRONG",
        breadth150_group=68.0,
        group_mrs=5.0,
        group_rp_ma52_slope=0.01,
        group_score=100.0,
        group_overlay_score=82.0,
        data_available=True,
    )


def _local_proxy_group_context(**overrides) -> GroupContext:
    payload = {
        "industry_key": build_industry_key("Tech", "Software"),
        "group_name": "Tech",
        "group_state": "UNKNOWN",
        "breadth150_group": 68.0,
        "group_mrs": 5.0,
        "group_rp_ma52_slope": 0.01,
        "group_score": 50.0,
        "group_overlay_score": 82.0,
        "data_available": True,
        "assumption_flags": ("missing_core_group",),
        "group_truth_source": "local_proxy",
    }
    payload.update(overrides)
    return GroupContext(**payload)


def test_group_context_skips_metadata_only_symbols_for_expensive_daily_work(monkeypatch) -> None:
    analyzer = WeinsteinStage2Analyzer()
    industry_key = build_industry_key("Tech", "Software")
    loaded_frames = {
        "AAA": _weekly_to_daily(np.linspace(20.0, 35.0, 70), np.linspace(1000.0, 1200.0, 70)),
        "BBB": _weekly_to_daily(np.linspace(21.0, 36.0, 70), np.linspace(1000.0, 1200.0, 70)),
        "CCC": _weekly_to_daily(np.linspace(22.0, 37.0, 70), np.linspace(1000.0, 1200.0, 70)),
    }
    sector_map = {
        "AAA": "Tech",
        "BBB": "Tech",
        "CCC": "Tech",
        "ZZZ": "Tech",
    }
    industry_map = {symbol: "Software" for symbol in sector_map}
    normalized_symbols: list[str] = []
    original_normalize = analyzer._normalize_daily_frame

    def _capture_normalize(frame: pd.DataFrame) -> pd.DataFrame:
        if frame is not None and not frame.empty and "symbol" in frame.columns:
            normalized_symbols.append(str(frame["symbol"].iloc[0]))
        elif frame is not None and not frame.empty:
            normalized_symbols.append("loaded")
        else:
            raise AssertionError("metadata-only missing frames should not be normalized")
        return original_normalize(frame)

    for symbol, frame in loaded_frames.items():
        frame["symbol"] = symbol

    monkeypatch.setattr(analyzer, "_normalize_daily_frame", _capture_normalize)

    contexts, ranking = analyzer.compute_group_contexts(
        market="us",
        daily_frames=loaded_frames,
        benchmark_daily=_benchmark_daily(),
        benchmark_weekly=WeinsteinStage2Analyzer().build_weekly_bars(_benchmark_daily(), market="us"),
        sector_map=sector_map,
        industry_map=industry_map,
        leader_core=leader_core_bridge.empty_leader_core_snapshot("us", "2026-03-12"),
    )

    assert sorted(symbol for symbol in normalized_symbols if symbol != "loaded") == ["AAA", "BBB", "CCC"]
    assert contexts[industry_key].data_available is True
    assert int(ranking.loc[ranking["industry_key"] == industry_key, "member_count"].iloc[0]) == 4


def test_group_context_accepts_precomputed_benchmark_weekly() -> None:
    analyzer = WeinsteinStage2Analyzer()
    frames = {
        "AAA": _weekly_to_daily(np.linspace(20.0, 35.0, 70), np.linspace(1000.0, 1200.0, 70)),
        "BBB": _weekly_to_daily(np.linspace(21.0, 36.0, 70), np.linspace(1000.0, 1200.0, 70)),
        "CCC": _weekly_to_daily(np.linspace(22.0, 37.0, 70), np.linspace(1000.0, 1200.0, 70)),
    }
    sector_map = {symbol: "Tech" for symbol in frames}
    industry_map = {symbol: "Software" for symbol in frames}
    benchmark = _benchmark_daily()
    benchmark_weekly = analyzer.build_weekly_bars(benchmark, market="us")

    fallback_contexts, fallback_ranking = analyzer.compute_group_contexts(
        market="us",
        daily_frames=frames,
        benchmark_daily=benchmark,
        sector_map=sector_map,
        industry_map=industry_map,
        leader_core=leader_core_bridge.empty_leader_core_snapshot("us", "2026-03-12"),
    )
    precomputed_contexts, precomputed_ranking = analyzer.compute_group_contexts(
        market="us",
        daily_frames=frames,
        benchmark_daily=benchmark,
        benchmark_weekly=benchmark_weekly,
        sector_map=sector_map,
        industry_map=industry_map,
        leader_core=leader_core_bridge.empty_leader_core_snapshot("us", "2026-03-12"),
    )

    assert fallback_contexts == precomputed_contexts
    pd.testing.assert_frame_equal(fallback_ranking, precomputed_ranking)


def test_group_context_missing_core_uses_local_proxy_unknown_state_and_neutral_group_score() -> None:
    analyzer = WeinsteinStage2Analyzer()
    industry_key = build_industry_key("Tech", "Software")
    frames = {
        "AAA": _weekly_to_daily(np.linspace(20.0, 35.0, 70), np.linspace(1000.0, 1200.0, 70)),
        "BBB": _weekly_to_daily(np.linspace(21.0, 36.0, 70), np.linspace(1000.0, 1200.0, 70)),
        "CCC": _weekly_to_daily(np.linspace(22.0, 37.0, 70), np.linspace(1000.0, 1200.0, 70)),
    }
    sector_map = {symbol: "Tech" for symbol in frames}
    industry_map = {symbol: "Software" for symbol in frames}

    contexts, ranking = analyzer.compute_group_contexts(
        market="us",
        daily_frames=frames,
        benchmark_daily=_benchmark_daily(),
        sector_map=sector_map,
        industry_map=industry_map,
        leader_core=leader_core_bridge.empty_leader_core_snapshot("us", "2026-03-12"),
    )

    assert contexts[industry_key].group_state == "UNKNOWN"
    assert contexts[industry_key].group_score == 50.0
    assert contexts[industry_key].group_truth_source == "local_proxy"
    assert ranking.loc[ranking["industry_key"] == industry_key, "group_state"].iloc[0] == "UNKNOWN"


def _write_leader_core_artifacts(root, *, as_of: str) -> str:
    compat_market_root = root / "market_intel_compat" / "us"
    compat_market_root.mkdir(parents=True, exist_ok=True)
    industry_key = build_industry_key("Tech", "Software")
    summary = {
        "market": "us",
        "as_of": as_of,
        "schema_version": leader_core_bridge.LEADER_CORE_SCHEMA_VERSION,
        "engine_version": leader_core_bridge.LEADER_CORE_ENGINE_VERSION,
        "leader_health_score": 70.0,
        "leader_health_status": "HEALTHY",
        "confirmed_count": 1,
        "imminent_count": 0,
        "broken_count": 0,
    }
    group_rows = [
        {
            "market": "us",
            "as_of": as_of,
            "schema_version": leader_core_bridge.LEADER_CORE_SCHEMA_VERSION,
            "engine_version": leader_core_bridge.LEADER_CORE_ENGINE_VERSION,
            "industry_key": industry_key,
            "group_strength_score": 86.0,
            "group_state": "STRONG",
            "rank": 1.0,
            "leaders": ["BBB"],
        }
    ]
    (compat_market_root / "leader_market_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (compat_market_root / "industry_rotation.json").write_text(
        json.dumps(group_rows, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (compat_market_root / "leaders.json").write_text("[]", encoding="utf-8")
    (compat_market_root / "market_context.json").write_text(
        json.dumps(
            {
                "market": "us",
                "as_of": as_of,
                "schema_version": leader_core_bridge.MARKET_CONTEXT_SCHEMA_VERSION,
                "engine_version": "compat_market_context_v1",
                "regime_state": "uptrend",
                "top_state": "risk_on",
                "market_state": "uptrend",
                "breadth_state": "broad_participation",
                "concentration_state": "diversified",
                "leadership_state": "growth_ai",
                "prototype_market_alias": "RISK_ON",
                "market_alignment_score": 80.0,
                "breadth_support_score": 74.0,
                "rotation_support_score": 86.0,
                "leader_health_score": 70.0,
                "leader_health_status": "HEALTHY",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return str(compat_market_root)


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


def _continuation_breakout_closes() -> np.ndarray:
    return np.array(
        [
            60.0, 63.0, 66.0, 70.0, 74.0,
            78.0, 82.0, 86.0, 90.0, 94.0,
            97.0, 100.0, 99.0, 98.0, 96.0,
            97.0, 98.0, 100.0, 99.0, 97.0,
            96.0, 97.0, 99.0, 100.0, 101.0,
            100.0, 99.0, 98.0, 97.0, 99.0,
            100.0, 101.0, 100.5, 99.5, 100.8,
            101.0, 101.2, 101.5, 102.0, 108.5,
        ]
    )


def _generic_stage2_trend_closes() -> np.ndarray:
    return np.concatenate(
        [
            np.linspace(60.0, 82.0, 20, endpoint=False),
            np.linspace(82.0, 102.0, 20, endpoint=False),
            np.linspace(102.0, 122.0, 20),
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

    assert len(weekly) == 1
    assert weekly.iloc[0]["session_count"] == 4
    assert str(pd.Timestamp(weekly.iloc[0]["bar_end_date"]).date()) == "2026-01-08"


def test_weekly_builder_keeps_holiday_shortened_completed_week() -> None:
    analyzer = WeinsteinStage2Analyzer()
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2026-06-29",
                    "2026-06-30",
                    "2026-07-01",
                    "2026-07-02",
                ]
            ),
            "open": [10, 11, 12, 13],
            "high": [11, 12, 13, 14],
            "low": [9, 10, 11, 12],
            "close": [10.5, 11.5, 12.5, 13.5],
            "volume": [100, 110, 120, 130],
        }
    )

    weekly = analyzer.build_weekly_bars(frame)

    assert len(weekly) == 1
    assert weekly.iloc[0]["session_count"] == 4
    assert str(pd.Timestamp(weekly.iloc[0]["bar_end_date"]).date()) == "2026-07-02"


def test_weekly_builder_keeps_kr_holiday_shortened_completed_week() -> None:
    analyzer = WeinsteinStage2Analyzer()
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2026-04-13",
                    "2026-04-14",
                    "2026-04-15",
                    "2026-04-16",
                ]
            ),
            "open": [10, 11, 12, 13],
            "high": [11, 12, 13, 14],
            "low": [9, 10, 11, 12],
            "close": [10.5, 11.5, 12.5, 13.5],
            "volume": [100, 110, 120, 130],
        }
    )

    weekly = analyzer.build_weekly_bars(frame, market="kr")

    assert len(weekly) == 1
    assert weekly.iloc[0]["session_count"] == 4
    assert str(pd.Timestamp(weekly.iloc[0]["bar_end_date"]).date()) == "2026-04-16"


def test_analyze_symbol_keeps_near_breakout_stage1_as_base() -> None:
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

    assert result["timing_state"] == "BASE"
    assert result["stock_stage"] == "STAGE_1"
    assert result["percent_to_stage2"] is not None and result["percent_to_stage2"] <= 1.0
    assert result["priority_label"] == "WATCH"


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


def test_analyze_symbol_marks_lighter_volume_retest_as_secondary_candidate() -> None:
    analyzer = WeinsteinStage2Analyzer()
    closes = np.concatenate([_stage1_base_closes(), [102.2, 104.8, 105.4, 102.6]])
    volumes = np.linspace(700_000, 500_000, len(closes))
    volumes[-3] = 2_600_000
    volumes[-2] = 1_100_000
    volumes[-1] = 700_000
    daily = _weekly_to_daily(closes, volumes)

    result = analyzer.analyze_symbol(
        symbol="RET",
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

    assert result["timing_state"] == "RETEST_B"
    assert result["retest_signal"] is True
    assert result["retest_volume_lighter"] is True
    assert result["breakout_structure_pass"] is False


def test_analyze_symbol_local_proxy_nonweak_group_allows_breakout_progress() -> None:
    analyzer = WeinsteinStage2Analyzer()
    closes = np.concatenate([_stage1_base_closes(), [102.2, 104.8]])
    volumes = np.linspace(700_000, 500_000, len(closes))
    volumes[-1] = 2_600_000
    daily = _weekly_to_daily(closes, volumes)

    result = analyzer.analyze_symbol(
        symbol="LPX",
        market="us",
        daily_frame=daily,
        benchmark_symbol="SPY",
        benchmark_daily=_benchmark_daily(),
        market_context=_market_context(),
        group_context=_local_proxy_group_context(),
        exchange="NASDAQ",
        sector="Tech",
        industry_group="Software",
    )

    assert result["timing_state"] == "BREAKOUT_WEEK"
    assert result["group_state"] == "UNKNOWN"
    assert result["group_truth_source"] == "local_proxy"


def test_analyze_symbol_local_proxy_weak_only_vetoes_explicit_weak_groups() -> None:
    analyzer = WeinsteinStage2Analyzer()
    closes = np.concatenate([_stage1_base_closes(), [102.2, 104.8]])
    volumes = np.linspace(700_000, 500_000, len(closes))
    volumes[-1] = 2_600_000
    daily = _weekly_to_daily(closes, volumes)

    result = analyzer.analyze_symbol(
        symbol="LPW",
        market="us",
        daily_frame=daily,
        benchmark_symbol="SPY",
        benchmark_daily=_benchmark_daily(),
        market_context=_market_context(),
        group_context=_local_proxy_group_context(
            breadth150_group=35.0,
            group_mrs=-1.0,
            group_rp_ma52_slope=-0.01,
            group_overlay_score=30.0,
        ),
        exchange="NASDAQ",
        sector="Tech",
        industry_group="Software",
    )

    assert result["timing_state"] == "EXCLUDE"
    assert result["rejection_reason"] == "group_weak"


def test_analyze_symbol_data_poor_local_proxy_does_not_veto_breakout() -> None:
    analyzer = WeinsteinStage2Analyzer()
    closes = np.concatenate([_stage1_base_closes(), [102.2, 104.8]])
    volumes = np.linspace(700_000, 500_000, len(closes))
    volumes[-1] = 2_600_000
    daily = _weekly_to_daily(closes, volumes)

    result = analyzer.analyze_symbol(
        symbol="LPD",
        market="us",
        daily_frame=daily,
        benchmark_symbol="SPY",
        benchmark_daily=_benchmark_daily(),
        market_context=_market_context(),
        group_context=_local_proxy_group_context(
            breadth150_group=None,
            group_mrs=None,
            group_rp_ma52_slope=None,
            group_overlay_score=None,
            data_available=False,
            assumption_flags=("missing_core_group", "missing_group_data"),
        ),
        exchange="NASDAQ",
        sector="Tech",
        industry_group="Software",
    )

    assert result["timing_state"] == "BREAKOUT_WEEK"
    assert "missing_group_data" in result["assumption_flags"]


def test_local_proxy_group_score_stays_neutral_without_double_counting_overlay() -> None:
    analyzer = WeinsteinStage2Analyzer()
    closes = np.concatenate([_stage1_base_closes(), [102.2, 104.8]])
    volumes = np.linspace(700_000, 500_000, len(closes))
    volumes[-1] = 2_600_000
    daily = _weekly_to_daily(closes, volumes)

    local_proxy_result = analyzer.analyze_symbol(
        symbol="LPN",
        market="us",
        daily_frame=daily,
        benchmark_symbol="SPY",
        benchmark_daily=_benchmark_daily(),
        market_context=_market_context(),
        group_context=_local_proxy_group_context(group_score=50.0, group_overlay_score=82.0),
        exchange="NASDAQ",
        sector="Tech",
        industry_group="Software",
    )
    inflated_group_score_result = analyzer.analyze_symbol(
        symbol="LPI",
        market="us",
        daily_frame=daily,
        benchmark_symbol="SPY",
        benchmark_daily=_benchmark_daily(),
        market_context=_market_context(),
        group_context=_local_proxy_group_context(group_score=82.0, group_overlay_score=82.0),
        exchange="NASDAQ",
        sector="Tech",
        industry_group="Software",
    )

    assert round(
        inflated_group_score_result["early_stage2_score"] - local_proxy_result["early_stage2_score"],
        2,
    ) == 4.8


def test_analyze_symbol_classifies_continuation_breakout_as_stage_2b() -> None:
    analyzer = WeinsteinStage2Analyzer()
    closes = _continuation_breakout_closes()
    volumes = np.linspace(600_000, 500_000, len(closes))
    volumes[-1] = 2_800_000
    daily = _weekly_to_daily(closes, volumes)

    result = analyzer.analyze_symbol(
        symbol="CTN",
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

    assert result["timing_state"] == "BREAKOUT_WEEK"
    assert result["stock_stage"] == "STAGE_2B"
    assert result["breakout_type"] == "CONTINUATION_BREAKOUT"
    assert result["continuation_setup_pass"] is True
    assert result["continuation_quality_score"] is not None
    assert result["close_gt_ma50d"] is True
    assert result["close_gt_ma200d"] is True


def test_analyze_symbol_keeps_orderly_continuation_setup_as_stage_2b() -> None:
    analyzer = WeinsteinStage2Analyzer()
    closes = _continuation_breakout_closes()[:-1]
    volumes = np.linspace(600_000, 500_000, len(closes))
    daily = _weekly_to_daily(closes, volumes)

    result = analyzer.analyze_symbol(
        symbol="CTP",
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

    assert result["stock_stage"] == "STAGE_2B"
    assert result["timing_state"] == "EXCLUDE"
    assert result["continuation_setup_pass"] is True
    assert result["breakout_type"] is None


def test_analyze_symbol_uses_generic_stage_2_for_plain_uptrend() -> None:
    analyzer = WeinsteinStage2Analyzer()
    closes = _generic_stage2_trend_closes()
    volumes = np.linspace(700_000, 900_000, len(closes))
    daily = _weekly_to_daily(closes, volumes)

    result = analyzer.analyze_symbol(
        symbol="GEN",
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

    assert result["stock_stage"] == "STAGE_2"
    assert result["continuation_setup_pass"] is False


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
    benchmark_daily = _benchmark_daily()

    monkeypatch.setattr(weinstein_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(weinstein_screener, "get_weinstein_stage2_results_dir", lambda market: str(output_root))
    monkeypatch.setattr(WeinsteinStage2Screener, "_load_metadata", lambda self: metadata.copy())
    monkeypatch.setattr(WeinsteinStage2Screener, "_load_daily_frames", lambda self: {key: value.copy() for key, value in frames.items()})
    monkeypatch.setattr(
        weinstein_screener,
        "load_benchmark_data",
        lambda *args, **kwargs: ("SPY", benchmark_daily.copy()),
    )
    compat_market_root = _write_leader_core_artifacts(
        output_root,
        as_of=str(pd.Timestamp(benchmark_daily["date"].iloc[-1]).date()),
    )
    monkeypatch.setattr(
        leader_core_bridge,
        "get_market_intel_compat_root",
        lambda market: compat_market_root,
    )

    result = weinstein_screener.run_weinstein_stage2_screening(market="us")

    assert not result.empty
    assert "phase_bucket" in result.columns
    assert "group_truth_source" in result.columns
    assert "breadth150_market" not in result.columns
    assert "breadth150_group" not in result.columns
    assert "group_mrs" not in result.columns
    assert "group_rp_ma52_slope" not in result.columns
    assert "group_overlay_score" not in result.columns
    assert "group_score" not in result.columns
    assert "market_score" not in result.columns
    assert (output_root / "pattern_excluded_pool.csv").exists()
    assert (output_root / "actual_data_calibration.json").exists()
    assert not (output_root / "pattern_included_candidates.csv").exists()
    assert not (output_root / "pre_stage2_candidates.csv").exists()

    group_rankings = pd.read_csv(output_root / "group_rankings.csv")
    assert "group_strength_score" in group_rankings.columns
    assert "group_state" in group_rankings.columns
    assert "group_truth_source" in group_rankings.columns
    assert "group_overlay_score" not in group_rankings.columns
    assert "breadth150_group" not in group_rankings.columns
    assert "group_mrs" not in group_rankings.columns
    assert "group_rp_ma52_slope" not in group_rankings.columns
    assert "group_score" not in group_rankings.columns

    with open(output_root / "market_summary.json", "r", encoding="utf-8") as handle:
        summary = json.load(handle)
    assert summary["market_alias"] == "RISK_ON"
    assert summary["market_alignment_score"] == 80.0
    assert summary["breadth_support_score"] == 74.0
    assert summary["rotation_support_score"] == 86.0
    assert summary["leader_health_score"] == 70.0
    assert summary["market_truth_mode"] == "compat"
    assert "breadth150_market" not in summary


def test_run_weinstein_stage2_screening_supports_standalone_without_compat(monkeypatch) -> None:
    output_root = runtime_root("_test_runtime_weinstein_stage2_standalone")
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
    benchmark_daily = _benchmark_daily()

    monkeypatch.setattr(weinstein_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(weinstein_screener, "get_weinstein_stage2_results_dir", lambda market: str(output_root))
    monkeypatch.setattr(WeinsteinStage2Screener, "_load_metadata", lambda self: metadata.copy())
    monkeypatch.setattr(WeinsteinStage2Screener, "_load_daily_frames", lambda self: {key: value.copy() for key, value in frames.items()})
    monkeypatch.setattr(
        weinstein_screener,
        "load_benchmark_data",
        lambda *args, **kwargs: ("SPY", benchmark_daily.copy()),
    )
    monkeypatch.setattr(
        weinstein_screener,
        "load_market_truth_snapshot",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("compat market truth should not be read in standalone mode")),
    )

    result = weinstein_screener.run_weinstein_stage2_screening(market="us", standalone=True)

    assert not result.empty
    assert "group_truth_source" in result.columns
    with open(output_root / "market_summary.json", "r", encoding="utf-8") as handle:
        summary = json.load(handle)
    assert summary["market_truth_source"] == "local_standalone"
    assert summary["core_overlay_applied"] is False
    assert summary["market_truth_mode"] == "standalone_manual"
    assert summary["market_alias"] in {"RISK_ON", "NEUTRAL", "RISK_OFF"}
    assert summary["market_alignment_score"] is not None


def test_run_weinstein_stage2_screening_scopes_local_frames_to_benchmark_as_of(monkeypatch) -> None:
    output_root = runtime_root("_test_runtime_weinstein_stage2_local_asof")
    output_root.mkdir(parents=True, exist_ok=True)
    data_root = runtime_root("_test_runtime_weinstein_stage2_local_asof_data")
    market_data_dir = data_root / "us"
    market_data_dir.mkdir(parents=True, exist_ok=True)

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
    for symbol in frames:
        (market_data_dir / f"{symbol}.csv").write_text("date,close\n2025-01-02,1\n", encoding="utf-8")

    truncated_benchmark = _benchmark_daily().iloc[:-5].copy()
    expected_as_of = str(pd.Timestamp(truncated_benchmark["date"].iloc[-1]).date())
    observed_as_of: list[str | None] = []
    runtime_context = RuntimeContext(market="us")

    monkeypatch.setattr(weinstein_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(weinstein_screener, "get_weinstein_stage2_results_dir", lambda market: str(output_root))
    monkeypatch.setattr(weinstein_screener, "get_market_data_dir", lambda market: str(market_data_dir))
    monkeypatch.setattr(WeinsteinStage2Screener, "_load_metadata", lambda self: metadata.copy())
    monkeypatch.setattr(
        weinstein_screener,
        "load_benchmark_data",
        lambda *args, **kwargs: ("SPY", truncated_benchmark.copy()),
    )

    def _capture_frame(market, symbol, **kwargs):  # noqa: ANN001, ANN202
        observed_as_of.append(kwargs.get("as_of"))
        return frames[str(symbol).upper()].copy()

    monkeypatch.setattr(weinstein_screener, "load_local_ohlcv_frame", _capture_frame)
    compat_market_root = _write_leader_core_artifacts(output_root, as_of=expected_as_of)
    monkeypatch.setattr(
        leader_core_bridge,
        "get_market_intel_compat_root",
        lambda market: compat_market_root,
    )

    result = weinstein_screener.run_weinstein_stage2_screening(
        market="us",
        runtime_context=runtime_context,
    )

    assert not result.empty
    assert observed_as_of
    assert set(observed_as_of) == {expected_as_of}
    freshness = runtime_context.runtime_state["data_freshness"]["stages"]["weinstein_stage2"]
    assert freshness["counts"]["stale"] + freshness["counts"]["future_or_partial"] == len(frames)
    assert freshness["mode"] == "default_completed_session"


def test_run_weinstein_stage2_screening_preserves_explicit_runtime_as_of(monkeypatch) -> None:
    output_root = runtime_root("_test_runtime_weinstein_stage2_explicit_asof")
    output_root.mkdir(parents=True, exist_ok=True)
    data_root = runtime_root("_test_runtime_weinstein_stage2_explicit_asof_data")
    market_data_dir = data_root / "us"
    market_data_dir.mkdir(parents=True, exist_ok=True)

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
    for symbol in frames:
        (market_data_dir / f"{symbol}.csv").write_text("date,close\n2025-01-02,1\n", encoding="utf-8")

    benchmark_daily = _benchmark_daily()
    explicit_as_of = str(pd.Timestamp(benchmark_daily["date"].iloc[-8]).date())
    benchmark_latest = str(pd.Timestamp(benchmark_daily["date"].iloc[-1]).date())
    assert explicit_as_of != benchmark_latest
    observed_as_of: list[str | None] = []
    runtime_context = RuntimeContext(market="us", as_of_date=explicit_as_of)

    monkeypatch.setattr(weinstein_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(weinstein_screener, "get_weinstein_stage2_results_dir", lambda market: str(output_root))
    monkeypatch.setattr(weinstein_screener, "get_market_data_dir", lambda market: str(market_data_dir))
    monkeypatch.setattr(WeinsteinStage2Screener, "_load_metadata", lambda self: metadata.copy())
    monkeypatch.setattr(
        weinstein_screener,
        "load_benchmark_data",
        lambda *args, **kwargs: ("SPY", benchmark_daily.copy()),
    )

    def _capture_frame(market, symbol, **kwargs):  # noqa: ANN001, ANN202
        observed_as_of.append(kwargs.get("as_of"))
        return frames[str(symbol).upper()].copy()

    monkeypatch.setattr(weinstein_screener, "load_local_ohlcv_frame", _capture_frame)
    compat_market_root = _write_leader_core_artifacts(output_root, as_of=explicit_as_of)
    monkeypatch.setattr(
        leader_core_bridge,
        "get_market_intel_compat_root",
        lambda market: compat_market_root,
    )

    result = weinstein_screener.run_weinstein_stage2_screening(
        market="us",
        runtime_context=runtime_context,
    )

    assert not result.empty
    assert runtime_context.as_of_date == explicit_as_of
    assert observed_as_of
    assert set(observed_as_of) == {explicit_as_of}
    freshness = runtime_context.runtime_state["data_freshness"]["stages"]["weinstein_stage2"]
    assert freshness["mode"] == "explicit_replay"
