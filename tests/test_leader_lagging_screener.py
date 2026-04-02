from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from screeners import leader_core_bridge
from screeners.leader_lagging import screener as leader_lagging_screener
from screeners.leader_lagging.screener import LeaderLaggingAnalyzer, LeaderLaggingScreener
from screeners.leader_core_bridge import LeaderCoreSnapshot, annotate_frame_with_leader_core, build_industry_key
from tests._paths import runtime_root
from utils import market_runtime


def _piecewise_linear(segments: list[tuple[float, float, int]]) -> np.ndarray:
    parts: list[np.ndarray] = []
    for index, (start, end, length) in enumerate(segments):
        parts.append(np.linspace(start, end, length, endpoint=index == len(segments) - 1))
    return np.concatenate(parts)


def _daily_from_closes(
    closes: np.ndarray,
    volumes: np.ndarray,
    *,
    spreads: np.ndarray | None = None,
    start: str = "2024-01-02",
    gap_on_last: float = 0.0,
) -> pd.DataFrame:
    dates = pd.bdate_range(start=start, periods=len(closes))
    if spreads is None:
        spreads = np.full(len(closes), 0.012)

    rows: list[dict[str, float | pd.Timestamp]] = []
    prev_close = float(closes[0])
    for index, (date, close, volume, spread) in enumerate(zip(dates, closes, volumes, spreads)):
        close_value = float(close)
        if index == 0:
            open_value = close_value * 0.99
        else:
            open_value = prev_close
            if index == len(closes) - 1 and gap_on_last:
                open_value = prev_close * (1.0 + gap_on_last)
        high = max(open_value, close_value) * (1.0 + float(spread))
        low = min(open_value, close_value) * (1.0 - float(spread))
        rows.append(
            {
                "date": date,
                "open": open_value,
                "high": high,
                "low": low,
                "close": close_value,
                "volume": float(volume),
            }
        )
        prev_close = close_value
    return pd.DataFrame(rows)


def _benchmark_daily(length: int = 320) -> pd.DataFrame:
    closes = np.concatenate(
        [
            np.linspace(100.0, 118.0, 220, endpoint=False),
            np.linspace(118.0, 132.0, length - 220),
        ]
    )
    volumes = np.full(length, 4_500_000.0)
    spreads = np.full(length, 0.008)
    return _daily_from_closes(closes, volumes, spreads=spreads)


def _build_market_fixture() -> tuple[dict[str, pd.DataFrame], dict[str, dict[str, object]], pd.DataFrame]:
    leader_closes = np.concatenate(
        [
            _piecewise_linear([(40.0, 72.0, 180), (72.0, 92.0, 80)]),
            np.linspace(92.0, 120.0, 60),
        ]
    )
    leader_volumes = np.concatenate(
        [
            np.full(299, 1_400_000.0),
            np.array([5_000_000.0]),
            np.full(20, 3_000_000.0),
        ]
    )[: len(leader_closes)]

    follower_closes = np.concatenate(
        [
            _piecewise_linear([(40.0, 50.0, 210), (50.0, 58.0, 50)]),
            np.concatenate(
                [
                    np.linspace(58.0, 62.0, 40, endpoint=False),
                    np.linspace(62.0, 66.0, 20),
                ]
            ),
        ]
    )
    follower_volumes = np.concatenate([np.full(300, 950_000.0), np.full(20, 1_200_000.0)])[: len(follower_closes)]

    weak1_closes = _piecewise_linear([(40.0, 49.0, 200), (49.0, 55.0, 120)])
    weak2_closes = _piecewise_linear([(40.0, 37.0, 160), (37.0, 34.0, 80), (34.0, 38.0, 80)])
    weak1_volumes = np.full(len(weak1_closes), 600_000.0)
    weak2_volumes = np.full(len(weak2_closes), 550_000.0)

    frames = {
        "LEAD": _daily_from_closes(leader_closes, leader_volumes, gap_on_last=0.06),
        "FOLLOW": _daily_from_closes(follower_closes, follower_volumes),
        "WEAK1": _daily_from_closes(weak1_closes, weak1_volumes),
        "WEAK2": _daily_from_closes(weak2_closes, weak2_volumes),
    }
    metadata_map = {
        "LEAD": {"symbol": "LEAD", "sector": "Tech", "industry": "Software", "market_cap": 20_000_000_000},
        "FOLLOW": {"symbol": "FOLLOW", "sector": "Tech", "industry": "Software", "market_cap": 8_000_000_000},
        "WEAK1": {"symbol": "WEAK1", "sector": "Retail", "industry": "Apparel", "market_cap": 2_500_000_000},
        "WEAK2": {"symbol": "WEAK2", "sector": "Retail", "industry": "Apparel", "market_cap": 2_000_000_000},
    }
    return frames, metadata_map, _benchmark_daily(len(leader_closes))


def _build_leader_core_snapshot(
    metadata_map: dict[str, dict[str, object]],
    *,
    as_of: str,
) -> LeaderCoreSnapshot:
    tech_key = build_industry_key("Tech", "Software")
    retail_key = build_industry_key("Retail", "Apparel")
    return LeaderCoreSnapshot(
        market="us",
        as_of=as_of,
        summary={
            "market": "us",
            "as_of": as_of,
            "schema_version": leader_core_bridge.LEADER_CORE_SCHEMA_VERSION,
            "engine_version": leader_core_bridge.LEADER_CORE_ENGINE_VERSION,
            "leader_health_score": 72.0,
            "leader_health_status": "HEALTHY",
            "confirmed_count": 1,
            "imminent_count": 0,
            "broken_count": 1,
        },
        groups_by_key={
            tech_key: {
                "industry_key": tech_key,
                "group_strength_score": 88.0,
                "group_state": "STRONG",
                "rank": 1.0,
                "leaders": ["LEAD"],
            },
            retail_key: {
                "industry_key": retail_key,
                "group_strength_score": 24.0,
                "group_state": "WEAK",
                "rank": 2.0,
                "leaders": [],
            },
        },
        leaders_by_symbol={
            "LEAD": {
                "symbol": "LEAD",
                "industry_key": tech_key,
                "leader_score": 84.0,
                "leader_state": "CONFIRMED",
                "breakdown_score": 10.0,
                "breakdown_status": "OK",
            },
            "FOLLOW": {
                "symbol": "FOLLOW",
                "industry_key": tech_key,
                "leader_score": 56.0,
                "leader_state": "REJECT",
                "breakdown_score": 18.0,
                "breakdown_status": "OK",
            },
            "WEAK1": {
                "symbol": "WEAK1",
                "industry_key": retail_key,
                "leader_score": 28.0,
                "leader_state": "REJECT",
                "breakdown_score": 66.0,
                "breakdown_status": "BROKEN",
            },
            "WEAK2": {
                "symbol": "WEAK2",
                "industry_key": retail_key,
                "leader_score": 22.0,
                "leader_state": "REJECT",
                "breakdown_score": 74.0,
                "breakdown_status": "BROKEN",
            },
        },
    )


def _write_leader_core_artifacts(
    root: Path,
    metadata_map: dict[str, dict[str, object]],
    *,
    as_of: str,
) -> Path:
    compat_market_root = root / "market_intel_compat" / "us"
    compat_market_root.mkdir(parents=True, exist_ok=True)
    snapshot = _build_leader_core_snapshot(metadata_map, as_of=as_of)
    (compat_market_root / "leader_market_summary.json").write_text(
        json.dumps(snapshot.summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (compat_market_root / "industry_rotation.json").write_text(
        json.dumps(list(snapshot.groups_by_key.values()), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (compat_market_root / "leaders.json").write_text(
        json.dumps(list(snapshot.leaders_by_symbol.values()), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
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
                "market_alignment_score": 82.0,
                "breadth_support_score": 79.0,
                "rotation_support_score": 88.0,
                "leader_health_score": 72.0,
                "leader_health_status": "HEALTHY",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return compat_market_root


def test_analyzer_detects_confirmed_leader_and_high_quality_follower() -> None:
    frames, metadata_map, benchmark_daily = _build_market_fixture()
    analyzer = LeaderLaggingAnalyzer()

    feature_rows = [
        analyzer.compute_symbol_features(
            symbol=symbol,
            market="us",
            daily_frame=frame,
            benchmark_daily=benchmark_daily,
            metadata=metadata_map.get(symbol),
        )
        for symbol, frame in frames.items()
    ]
    feature_table = analyzer.finalize_feature_table(pd.DataFrame(feature_rows))
    snapshot = _build_leader_core_snapshot(metadata_map, as_of=str(feature_table["as_of_ts"].dropna().max()))
    feature_table = annotate_frame_with_leader_core(feature_table, snapshot)
    group_table = analyzer.compute_group_table(feature_table)
    market_context = analyzer.compute_market_context(
        market="us",
        benchmark_symbol="SPY",
        benchmark_daily=benchmark_daily,
        feature_table=feature_table,
        group_table=group_table,
    )
    leaders = analyzer.analyze_leaders(
        feature_table=feature_table,
        group_table=group_table,
        market_context=market_context,
    )
    confirmed_leaders = leaders[leaders["label"] == "Confirmed Leader"].copy()
    followers, pairs = analyzer.analyze_followers(
        feature_table=feature_table,
        leaders=confirmed_leaders,
        group_table=group_table,
        market_context=market_context,
        frames=frames,
    )

    lead_row = leaders.loc[leaders["symbol"] == "LEAD"].iloc[0].to_dict()
    follower_row = followers.loc[followers["symbol"] == "FOLLOW"].iloc[0].to_dict()
    pair_row = pairs.loc[pairs["follower_symbol"] == "FOLLOW"].iloc[0].to_dict()

    assert market_context.regime_state == "Risk-On"
    assert lead_row["label"] == "Confirmed Leader"
    assert follower_row["label"] == "High-Quality Follower"
    assert follower_row["linked_leader"] == "LEAD"
    assert pair_row["leader_symbol"] == "LEAD"
    assert "WEAK1" not in set(followers["symbol"].astype(str))
    assert "WEAK2" not in set(followers["symbol"].astype(str))


def test_weighted_rs_uses_benchmark_relative_ibd_formula() -> None:
    frames, metadata_map, benchmark_daily = _build_market_fixture()
    analyzer = LeaderLaggingAnalyzer()

    feature_row = analyzer.compute_symbol_features(
        symbol="LEAD",
        market="us",
        daily_frame=frames["LEAD"],
        benchmark_daily=benchmark_daily,
        metadata=metadata_map["LEAD"],
    )

    stock_close = pd.to_numeric(frames["LEAD"]["close"], errors="coerce").reset_index(drop=True)
    benchmark_close = pd.to_numeric(benchmark_daily["close"], errors="coerce").reset_index(drop=True)

    def _subperiod_return(series: pd.Series, end_offset: int, span: int) -> float | None:
        if len(series) <= end_offset + span:
            return None
        end_idx = len(series) - 1 - end_offset
        start_idx = end_idx - span
        start = float(series.iloc[start_idx])
        end = float(series.iloc[end_idx])
        if start == 0:
            return None
        return (end / start) - 1.0

    def _weighted_rs(end_offset: int) -> float:
        stock_ret_3m = _subperiod_return(stock_close, end_offset, 63)
        stock_ret_6m = _subperiod_return(stock_close, end_offset, 126)
        stock_ret_9m = _subperiod_return(stock_close, end_offset, 189)
        stock_ret_12m = _subperiod_return(stock_close, end_offset, 252)
        benchmark_ret_3m = _subperiod_return(benchmark_close, end_offset, 63)
        benchmark_ret_6m = _subperiod_return(benchmark_close, end_offset, 126)
        benchmark_ret_9m = _subperiod_return(benchmark_close, end_offset, 189)
        benchmark_ret_12m = _subperiod_return(benchmark_close, end_offset, 252)
        assert stock_ret_3m is not None and stock_ret_6m is not None and stock_ret_9m is not None and stock_ret_12m is not None
        assert benchmark_ret_3m is not None and benchmark_ret_6m is not None and benchmark_ret_9m is not None and benchmark_ret_12m is not None
        stock_score = (
            0.40 * stock_ret_3m
            + 0.20 * stock_ret_6m
            + 0.20 * stock_ret_9m
            + 0.20 * stock_ret_12m
        )
        benchmark_score = (
            0.40 * benchmark_ret_3m
            + 0.20 * benchmark_ret_6m
            + 0.20 * benchmark_ret_9m
            + 0.20 * benchmark_ret_12m
        )
        return (stock_score - benchmark_score) * 100.0

    assert feature_row["weighted_rs_raw"] == pytest.approx(_weighted_rs(0), rel=1e-9)
    assert feature_row["weighted_rs_prev_raw"] == pytest.approx(_weighted_rs(63), rel=1e-9)


def test_weighted_rs_preserves_order_when_benchmark_is_negative() -> None:
    analyzer = LeaderLaggingAnalyzer()
    benchmark = pd.Series(np.geomspace(100.0, 90.0, 320))
    strong_stock = pd.Series(np.geomspace(100.0, 110.0, 320))
    weak_stock = pd.Series(np.geomspace(100.0, 95.0, 320))

    strong_score = analyzer._benchmark_relative_weighted_rs(strong_stock, benchmark)
    weak_score = analyzer._benchmark_relative_weighted_rs(weak_stock, benchmark)

    assert strong_score is not None
    assert weak_score is not None
    assert strong_score > weak_score
    assert strong_score > 0.0


def test_follower_requires_positive_delta_rs_rank_qoq_even_if_rs_slopes_are_positive() -> None:
    frames, metadata_map, benchmark_daily = _build_market_fixture()
    analyzer = LeaderLaggingAnalyzer()

    feature_rows = [
        analyzer.compute_symbol_features(
            symbol=symbol,
            market="us",
            daily_frame=frame,
            benchmark_daily=benchmark_daily,
            metadata=metadata_map.get(symbol),
        )
        for symbol, frame in frames.items()
    ]
    feature_table = analyzer.finalize_feature_table(pd.DataFrame(feature_rows))
    feature_table.loc[feature_table["symbol"] == "FOLLOW", "delta_rs_rank_qoq"] = -3.0
    feature_table.loc[feature_table["symbol"] == "FOLLOW", "rs_line_20d_slope"] = 0.08
    feature_table.loc[feature_table["symbol"] == "FOLLOW", "mansfield_rs_slope"] = 0.09
    snapshot = _build_leader_core_snapshot(metadata_map, as_of=str(feature_table["as_of_ts"].dropna().max()))
    feature_table = annotate_frame_with_leader_core(feature_table, snapshot)
    group_table = analyzer.compute_group_table(feature_table)
    market_context = analyzer.compute_market_context(
        market="us",
        benchmark_symbol="SPY",
        benchmark_daily=benchmark_daily,
        feature_table=feature_table,
        group_table=group_table,
    )
    leaders = analyzer.analyze_leaders(
        feature_table=feature_table,
        group_table=group_table,
        market_context=market_context,
    )
    confirmed_leaders = leaders[leaders["label"] == "Confirmed Leader"].copy()
    followers, _ = analyzer.analyze_followers(
        feature_table=feature_table,
        leaders=confirmed_leaders,
        group_table=group_table,
        market_context=market_context,
        frames=frames,
    )

    follow_row = followers.loc[followers["symbol"] == "FOLLOW"].iloc[0].to_dict()
    assert bool(follow_row["hard_precondition_pass"]) is False
    assert follow_row["label"] == "Too Weak, Reject"


def test_run_leader_lagging_screening_persists_outputs(monkeypatch) -> None:
    frames, metadata_map, benchmark_daily = _build_market_fixture()
    output_root = runtime_root("_test_runtime_leader_lagging_run")
    output_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(leader_lagging_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(market_runtime, "get_leader_lagging_results_dir", lambda market: str(output_root))
    monkeypatch.setattr(LeaderLaggingScreener, "_load_frames", lambda self: {key: value.copy() for key, value in frames.items()})
    monkeypatch.setattr(LeaderLaggingScreener, "_load_metadata_map", lambda self: metadata_map.copy())
    monkeypatch.setattr(
        leader_lagging_screener,
        "load_benchmark_data",
        lambda *args, **kwargs: ("SPY", benchmark_daily.copy()),
    )
    as_of = str(next(iter(frames.values()))["date"].iloc[-1].date())
    compat_market_root = _write_leader_core_artifacts(output_root, metadata_map, as_of=as_of)
    monkeypatch.setattr(
        leader_core_bridge,
        "get_market_intel_compat_root",
        lambda market: str(compat_market_root),
    )

    result = leader_lagging_screener.run_leader_lagging_screening(market="us")

    assert {row["symbol"] for row in result["pattern_excluded_pool"]} >= {"LEAD", "FOLLOW"}
    assert {row["symbol"] for row in result["pattern_included_candidates"]} == {"LEAD", "FOLLOW"}
    assert {row["symbol"] for row in result["leaders"]} == {"LEAD"}
    assert {row["symbol"] for row in result["followers"]} == {"FOLLOW"}
    assert result["pairs"][0]["leader_symbol"] == "LEAD"
    assert result["pairs"][0]["follower_symbol"] == "FOLLOW"
    assert result["actual_data_calibration"]["leader_rs_rank_min"] >= 75.0

    assert (output_root / "pattern_excluded_pool.csv").exists()
    assert (output_root / "pattern_included_candidates.csv").exists()
    assert (output_root / "leaders.csv").exists()
    assert (output_root / "followers.csv").exists()
    assert (output_root / "leader_follower_pairs.csv").exists()
    assert (output_root / "group_dashboard.csv").exists()
    assert (output_root / "market_summary.json").exists()
    assert (output_root / "actual_data_calibration.json").exists()

    with open(output_root / "market_summary.json", "r", encoding="utf-8") as handle:
        summary = json.load(handle)
    assert summary["counts"]["confirmed_leaders"] == 1
    assert summary["counts"]["high_quality_followers"] == 1
    assert summary["market_alias"] == "RISK_ON"
    assert summary["market_alignment_score"] == 82.0
    assert summary["breadth_support_score"] == 79.0
    assert summary["rotation_support_score"] == 88.0
    assert summary["leader_health_score"] == 72.0
    assert "regime_score" not in summary
    assert "breadth_50" not in summary
    assert "breadth_200" not in summary
    assert "high_low_ratio" not in summary
    assert "top_group_share" not in summary

    group_dashboard = pd.read_csv(output_root / "group_dashboard.csv")
    assert "group_strength_score" in group_dashboard.columns
    assert "group_state" in group_dashboard.columns
    assert "group_rank" in group_dashboard.columns
    assert "group_overlay_score" not in group_dashboard.columns
    assert "industry_rs_pct" not in group_dashboard.columns
    assert "sector_rs_pct" not in group_dashboard.columns
