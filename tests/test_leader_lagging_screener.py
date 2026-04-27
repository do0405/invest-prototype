from __future__ import annotations

import json
import importlib
import warnings
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
from utils.runtime_context import RuntimeContext
from screeners.leader_lagging import algorithms as leader_algorithms
from screeners.leader_lagging import followers as follower_algorithms
from screeners.leader_lagging import quality as leader_quality
from screeners.leader_lagging import tuning as leader_tuning


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


def _tuning_diagnostics(
    *,
    rows: int,
    leader_rows: int,
    reject_reason: str = "",
    confidence_bucket: str = "high",
) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for index in range(rows):
        is_leader = index < leader_rows
        records.append(
            {
                "symbol": f"T{index:03d}",
                "label": "strong_leader" if is_leader else "reject",
                "leader_tier": "strong" if is_leader else "reject",
                "entry_suitability": "fresh" if is_leader else "avoid",
                "leader_score": 82.0 if is_leader else 55.0,
                "leader_sort_score": 82.0 if is_leader else 55.0,
                "confidence_bucket": confidence_bucket,
                "reject_reason_codes": "" if is_leader else reject_reason,
                "extended_reason_codes": "",
                "rs_rank_true": 82.0,
                "rs_proxy_confidence": 88.0,
                "hidden_rs_confidence": 75.0,
                "structure_confidence": 82.0,
                "liquidity_quality_score": 86.0,
                "trend_integrity_score": 78.0,
                "structure_readiness_score": 58.0,
                "extension_risk_score": 35.0,
            }
        )
    return pd.DataFrame(records)


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
    confirmed_leaders = leaders[leaders["symbol"].isin({"LEAD", "LEAD2"})].copy()
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
    assert lead_row["label"] == "strong_leader"
    assert lead_row["legacy_label"] == "Confirmed Leader"
    assert lead_row["leader_tier"] == "strong"
    assert lead_row["entry_suitability"] in {"fresh", "watch"}
    assert bool(lead_row["hybrid_gate_pass"]) is True
    assert bool(lead_row["strict_rs_gate_pass"]) is True
    assert follower_row["label"] == "High-Quality Follower"
    assert follower_row["linked_leader"] == "LEAD"
    assert pair_row["leader_symbol"] == "LEAD"
    assert "WEAK1" not in set(followers["symbol"].astype(str))
    assert "WEAK2" not in set(followers["symbol"].astype(str))


def test_compute_symbol_features_can_reuse_pre_normalized_benchmark_without_renormalizing(monkeypatch) -> None:
    frames, metadata_map, benchmark_daily = _build_market_fixture()
    analyzer = LeaderLaggingAnalyzer()

    baseline = analyzer.compute_symbol_features(
        symbol="LEAD",
        market="us",
        daily_frame=frames["LEAD"],
        benchmark_daily=benchmark_daily,
        metadata=metadata_map["LEAD"],
    )
    normalized_benchmark = analyzer.normalize_daily_frame(benchmark_daily)
    real_normalize = analyzer.normalize_daily_frame

    def _normalize_spy(frame: pd.DataFrame) -> pd.DataFrame:
        if frame is normalized_benchmark:
            raise AssertionError("pre-normalized benchmark should be reused")
        return real_normalize(frame)

    monkeypatch.setattr(analyzer, "normalize_daily_frame", _normalize_spy)

    optimized = analyzer.compute_symbol_features(
        symbol="LEAD",
        market="us",
        daily_frame=frames["LEAD"],
        benchmark_daily=normalized_benchmark,
        metadata=metadata_map["LEAD"],
        benchmark_is_normalized=True,
    )

    for key in (
        "symbol",
        "market",
        "bars",
        "weighted_rs_raw",
        "benchmark_relative_strength",
        "rs_line_20d_slope",
        "ret_20d",
        "ret_60d",
    ):
        if isinstance(baseline[key], float):
            assert optimized[key] == pytest.approx(baseline[key])
        else:
            assert optimized[key] == baseline[key]


def test_analyze_followers_normalizes_each_symbol_frame_once(monkeypatch) -> None:
    frames, metadata_map, benchmark_daily = _build_market_fixture()
    frames = {
        "LEAD": frames["LEAD"].copy(),
        "FOLLOW": frames["FOLLOW"].copy(),
        "LEAD2": frames["LEAD"].copy(),
    }
    metadata_map = {
        "LEAD": dict(metadata_map["LEAD"]),
        "FOLLOW": dict(metadata_map["FOLLOW"]),
        "LEAD2": {
            "symbol": "LEAD2",
            "sector": "Tech",
            "industry": "Software",
            "market_cap": 19_000_000_000,
        },
    }
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
    confirmed_leaders = leaders[leaders["symbol"].isin({"LEAD", "LEAD2"})].copy()

    normalize_calls = 0
    real_normalize = analyzer.normalize_daily_frame

    def _count_normalize(frame: pd.DataFrame) -> pd.DataFrame:
        nonlocal normalize_calls
        normalize_calls += 1
        return real_normalize(frame)

    monkeypatch.setattr(analyzer, "normalize_daily_frame", _count_normalize)
    normal_price_calls = 0
    real_normal_price_frame = follower_algorithms._normal_price_frame

    def _count_normal_price_frame(frame: pd.DataFrame, *, frame_is_normalized: bool = False) -> pd.DataFrame:
        nonlocal normal_price_calls
        normal_price_calls += 1
        return real_normal_price_frame(frame, frame_is_normalized=frame_is_normalized)

    monkeypatch.setattr(follower_algorithms, "_normal_price_frame", _count_normal_price_frame)
    monkeypatch.setenv("INVEST_PROTO_FOLLOWER_ANALYSIS_MODE", "full")

    followers, pairs = analyzer.analyze_followers(
        feature_table=feature_table,
        leaders=confirmed_leaders,
        group_table=group_table,
        market_context=market_context,
        frames=frames,
    )

    assert not followers.empty
    assert not pairs.empty
    assert normalize_calls == len(frames)
    assert normal_price_calls <= len(frames)


def test_analyze_followers_balanced_prefilter_skips_lag_for_failed_candidates(monkeypatch) -> None:
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
    confirmed_leaders = leaders[leaders["leader_tier"] == "strong"].copy()

    def _fail_lag_profile(*args, **kwargs):
        raise AssertionError("prefiltered candidates should not run lag correlation")

    monkeypatch.delenv("INVEST_PROTO_FOLLOWER_ANALYSIS_MODE", raising=False)
    monkeypatch.setattr(follower_algorithms, "lagged_return_profile_from_price_frames", _fail_lag_profile)
    prepare_calls = 0
    real_prepare_lag_price_frame = follower_algorithms.prepare_lag_price_frame

    def _count_prepare(frame: pd.DataFrame, *, frame_is_normalized: bool = False) -> pd.DataFrame:
        nonlocal prepare_calls
        prepare_calls += 1
        return real_prepare_lag_price_frame(frame, frame_is_normalized=frame_is_normalized)

    monkeypatch.setattr(follower_algorithms, "prepare_lag_price_frame", _count_prepare)

    followers, pairs = analyzer.analyze_followers(
        feature_table=feature_table,
        leaders=confirmed_leaders,
        group_table=group_table,
        market_context=market_context,
        frames=frames,
    )

    follow_row = followers.loc[followers["symbol"] == "FOLLOW"].iloc[0].to_dict()
    assert follow_row["label"] == "Too Weak, Reject"
    assert bool(follow_row["hard_precondition_pass"]) is False
    assert pairs.empty
    assert analyzer.last_follower_lag_pruning["skipped_by_prefilter"] >= 1
    assert analyzer.last_follower_lag_pruning["pair_evaluations"] == 0
    assert prepare_calls == 0
    assert analyzer.last_follower_lag_pruning["lag_frame_precompute_symbols"] == 0


def test_analyze_followers_balanced_caps_pairs_and_full_mode_keeps_all_pairs(monkeypatch) -> None:
    base_frames, base_metadata, benchmark_daily = _build_market_fixture()
    frames = {"FOLLOW": base_frames["FOLLOW"].copy()}
    metadata_map = {"FOLLOW": dict(base_metadata["FOLLOW"])}
    for index in range(6):
        symbol = f"LEAD{index}"
        frames[symbol] = base_frames["LEAD"].copy()
        metadata_map[symbol] = {
            "symbol": symbol,
            "sector": "Tech",
            "industry": "Software",
            "market_cap": 20_000_000_000 - index,
        }

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
    feature_table["core_group_strength_score"] = np.nan
    feature_table["core_group_rank"] = np.nan
    feature_table["core_group_state"] = ""
    feature_table.loc[
        feature_table["symbol"] == "FOLLOW",
        ["rs_rank", "delta_rs_rank_qoq", "distance_to_52w_high", "close_gt_50", "traded_value_20d"],
    ] = [75.0, 6.0, 0.12, True, 5_000_000.0]
    group_table = analyzer.compute_group_table(feature_table)
    group_table["industry_rs_pct"] = 90.0
    group_table["group_strength_score"] = 88.0
    market_context = analyzer.compute_market_context(
        market="us",
        benchmark_symbol="SPY",
        benchmark_daily=benchmark_daily,
        feature_table=feature_table,
        group_table=group_table,
    )
    leaders = feature_table[feature_table["symbol"].str.startswith("LEAD")].copy()
    leaders["label"] = "strong_leader"
    leaders["leader_tier"] = "strong"
    leaders["entry_suitability"] = "fresh"
    leaders["leader_score"] = [90.0 - index for index in range(len(leaders))]
    leaders["leader_sort_score"] = leaders["leader_score"]

    calls = 0

    def _count_lag_profile(*args, **kwargs):
        nonlocal calls
        calls += 1
        return {
            "lag_days": 1,
            "lagged_corr": 0.8,
            "lead_lag_profile": "1:0.8",
            "lag_profile_sample_count": 60,
            "lag_profile_stability_score": 90.0,
            "pair_evidence_confidence": 90.0,
            "follower_reject_reason_codes": "",
        }

    monkeypatch.setattr(follower_algorithms, "lagged_return_profile_from_price_frames", _count_lag_profile)
    monkeypatch.delenv("INVEST_PROTO_FOLLOWER_ANALYSIS_MODE", raising=False)
    monkeypatch.setenv("INVEST_PROTO_FOLLOWER_MAX_LEADERS_PER_INDUSTRY", "5")
    monkeypatch.setenv("INVEST_PROTO_FOLLOWER_MAX_PAIRS_PER_CANDIDATE", "3")
    analyzer.analyze_followers(
        feature_table=feature_table,
        leaders=leaders,
        group_table=group_table,
        market_context=market_context,
        frames=frames,
    )
    assert calls == 3
    assert analyzer.last_follower_lag_pruning["pair_candidates"] == 3
    assert analyzer.last_follower_lag_pruning["leader_pool_after_cap"] == 5
    assert analyzer.last_follower_lag_pruning["lag_frame_precompute_symbols"] == 4

    calls = 0
    monkeypatch.setenv("INVEST_PROTO_FOLLOWER_ANALYSIS_MODE", "full")
    analyzer.analyze_followers(
        feature_table=feature_table,
        leaders=leaders,
        group_table=group_table,
        market_context=market_context,
        frames=frames,
    )
    assert calls == 6
    assert analyzer.last_follower_lag_pruning["mode"] == "full"
    assert analyzer.last_follower_lag_pruning["leader_pool_after_cap"] == 6
    assert analyzer.last_follower_lag_pruning["lag_frame_precompute_symbols"] == 7


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


def test_feature_table_exposes_generic_rs_scores_without_excluded_field_names() -> None:
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

    expected_feature_columns = {
        "weighted_rs_score",
        "rs_rank_true",
        "rs_rank_proxy",
        "benchmark_relative_strength",
        "benchmark_relative_strength_slope",
        "rs_quality_score",
        "leadership_freshness_score",
        "momentum_persistence_score",
        "near_high_leadership_score",
        "hidden_rs_score",
        "extension_risk_score",
    }
    assert expected_feature_columns <= set(feature_table.columns)
    assert {"group_benchmark_relative_strength", "group_relative_strength_score"} <= set(group_table.columns)
    excluded_fragment = "man" + "sfield"
    assert not any(excluded_fragment in column.lower() for column in feature_table.columns)
    assert not any(excluded_fragment in column.lower() for column in group_table.columns)

    lead_row = feature_table.loc[feature_table["symbol"] == "LEAD"].iloc[0]
    weak_row = feature_table.loc[feature_table["symbol"] == "WEAK2"].iloc[0]
    assert lead_row["rs_rank_true"] >= weak_row["rs_rank_true"]
    assert 1.0 <= float(lead_row["rs_rank_proxy"]) <= 99.0


def test_hidden_rs_rewards_resilience_on_benchmark_down_days() -> None:
    dates = pd.bdate_range("2025-01-02", periods=80)
    benchmark_close = pd.Series(np.linspace(100.0, 92.0, 80))
    resilient_close = pd.Series(np.linspace(100.0, 103.0, 80))
    fragile_close = pd.Series(np.linspace(100.0, 86.0, 80))

    def _aligned(stock_close: pd.Series) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": dates,
                "adj_close": stock_close,
                "benchmark_adj_close": benchmark_close,
                "daily_return": stock_close.pct_change(),
                "benchmark_return": benchmark_close.pct_change(),
            }
        )

    resilient_score = leader_algorithms.hidden_rs_raw_from_aligned(_aligned(resilient_close))
    fragile_score = leader_algorithms.hidden_rs_raw_from_aligned(_aligned(fragile_close))

    assert resilient_score is not None
    assert fragile_score is not None
    assert resilient_score > fragile_score
    assert resilient_score > 0.0


def test_rs_rank_proxy_reports_sample_coverage_and_confidence() -> None:
    benchmark = pd.Series(np.geomspace(100.0, 118.0, 320))
    strong_stock = pd.Series(np.geomspace(100.0, 170.0, 320))
    short_benchmark = benchmark.tail(150).reset_index(drop=True)
    short_stock = strong_stock.tail(150).reset_index(drop=True)

    full_profile = leader_algorithms.rs_rank_proxy_profile_from_history(strong_stock, benchmark)
    short_profile = leader_algorithms.rs_rank_proxy_profile_from_history(short_stock, short_benchmark)

    assert 1.0 <= float(full_profile["rs_rank_proxy"]) <= 99.0
    assert full_profile["rs_proxy_sample_count"] >= short_profile["rs_proxy_sample_count"]
    assert full_profile["rs_proxy_component_coverage"] == 4
    assert short_profile["rs_proxy_component_coverage"] < full_profile["rs_proxy_component_coverage"]
    assert full_profile["rs_proxy_confidence"] > short_profile["rs_proxy_confidence"]


def test_rs_rank_proxy_reuses_numeric_inputs_for_history_offsets(monkeypatch) -> None:
    benchmark = pd.Series(np.geomspace(100.0, 118.0, 320))
    strong_stock = pd.Series(np.geomspace(100.0, 170.0, 320))
    original_to_numeric = leader_algorithms.pd.to_numeric
    calls = 0

    def _counted_to_numeric(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        nonlocal calls
        calls += 1
        return original_to_numeric(*args, **kwargs)

    monkeypatch.setattr(leader_algorithms.pd, "to_numeric", _counted_to_numeric)

    profile = leader_algorithms.rs_rank_proxy_profile_from_history(strong_stock, benchmark)

    assert 1.0 <= float(profile["rs_rank_proxy"]) <= 99.0
    assert profile["rs_proxy_component_coverage"] == 4
    assert calls <= 4


def test_hidden_rs_profile_exposes_diagnostic_components() -> None:
    dates = pd.bdate_range("2025-01-02", periods=90)
    benchmark_close = pd.Series(np.concatenate([np.linspace(100.0, 90.0, 55), np.linspace(90.0, 94.0, 35)]))
    resilient_close = pd.Series(np.concatenate([np.linspace(100.0, 99.0, 55), np.linspace(99.0, 106.0, 35)]))
    fragile_close = pd.Series(np.concatenate([np.linspace(100.0, 82.0, 55), np.linspace(82.0, 86.0, 35)]))

    def _aligned(stock_close: pd.Series) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": dates,
                "adj_close": stock_close,
                "benchmark_adj_close": benchmark_close,
                "daily_return": stock_close.pct_change(),
                "benchmark_return": benchmark_close.pct_change(),
            }
        )

    resilient_profile = leader_algorithms.hidden_rs_profile_from_aligned(_aligned(resilient_close))
    fragile_profile = leader_algorithms.hidden_rs_profile_from_aligned(_aligned(fragile_close))

    expected_fields = {
        "hidden_rs_raw",
        "hidden_rs_weak_day_count",
        "hidden_rs_down_day_excess_return",
        "hidden_rs_drawdown_resilience",
        "hidden_rs_weak_window_excess_return",
        "hidden_rs_confidence",
    }
    assert expected_fields <= set(resilient_profile)
    assert resilient_profile["hidden_rs_raw"] > fragile_profile["hidden_rs_raw"]
    assert resilient_profile["hidden_rs_down_day_excess_return"] > fragile_profile["hidden_rs_down_day_excess_return"]
    assert resilient_profile["hidden_rs_drawdown_resilience"] > fragile_profile["hidden_rs_drawdown_resilience"]
    assert resilient_profile["hidden_rs_weak_day_count"] >= 10
    assert 0.0 < float(resilient_profile["hidden_rs_confidence"]) <= 100.0


def _structure_fixture(kind: str) -> pd.DataFrame:
    trend = np.linspace(70.0, 96.0, 55)
    base = 100.0 + np.sin(np.linspace(0.0, 8.0 * np.pi, 45)) * 1.2
    closes = np.concatenate([trend, base])
    volumes = np.concatenate([np.full(55, 1_100_000.0), np.full(45, 720_000.0)])
    spreads = np.concatenate([np.full(55, 0.014), np.full(45, 0.006)])
    daily = _daily_from_closes(closes, volumes, spreads=spreads)
    daily["adj_close"] = daily["close"]

    if kind == "no_volume_breakout":
        daily.loc[daily.index[-1], "close"] = float(daily["high"].iloc[-20:-1].max() * 1.015)
        daily.loc[daily.index[-1], "adj_close"] = daily.loc[daily.index[-1], "close"]
        daily.loc[daily.index[-1], "high"] = float(daily.loc[daily.index[-1], "close"] * 1.002)
        daily.loc[daily.index[-1], "low"] = float(daily.loc[daily.index[-1], "close"] * 0.995)
        daily.loc[daily.index[-1], "volume"] = 520_000.0
    elif kind == "loose_base":
        loose_tail = 100.0 + np.sin(np.linspace(0.0, 6.0 * np.pi, 45)) * 8.5
        daily.loc[daily.index[-45:], "close"] = loose_tail
        daily.loc[daily.index[-45:], "adj_close"] = loose_tail
        daily.loc[daily.index[-45:], "high"] = loose_tail * 1.035
        daily.loc[daily.index[-45:], "low"] = loose_tail * 0.965
        daily.loc[daily.index[-45:], "volume"] = np.linspace(1_350_000.0, 1_650_000.0, 45)
    elif kind == "support_break":
        support_floor = float(daily["low"].iloc[-35:-5].min())
        for offset in (8, 6, 4):
            index = daily.index[-offset]
            daily.loc[index, "low"] = support_floor * 0.91
            daily.loc[index, "close"] = support_floor * 0.94
            daily.loc[index, "adj_close"] = daily.loc[index, "close"]
            daily.loc[index, "high"] = support_floor * 0.99
            daily.loc[index, "volume"] = 1_600_000.0
    elif kind == "failed_breakout":
        prior_box_high = float(daily["high"].iloc[-30:-5].max())
        for offset in range(5, 1, -1):
            index = daily.index[-offset]
            daily.loc[index, "high"] = prior_box_high * 1.035
            daily.loc[index, "close"] = prior_box_high * 0.985
            daily.loc[index, "adj_close"] = daily.loc[index, "close"]
            daily.loc[index, "low"] = prior_box_high * 0.972
            daily.loc[index, "volume"] = 1_450_000.0
        daily.loc[daily.index[-1], "close"] = prior_box_high * 0.965
        daily.loc[daily.index[-1], "adj_close"] = daily.loc[daily.index[-1], "close"]
        daily.loc[daily.index[-1], "high"] = prior_box_high * 0.988
        daily.loc[daily.index[-1], "low"] = prior_box_high * 0.955
        daily.loc[daily.index[-1], "volume"] = 1_250_000.0
    elif kind == "extended_breakout":
        box_high = float(daily["high"].iloc[-30:-1].max())
        daily.loc[daily.index[-1], "close"] = box_high * 1.18
        daily.loc[daily.index[-1], "adj_close"] = daily.loc[daily.index[-1], "close"]
        daily.loc[daily.index[-1], "high"] = box_high * 1.20
        daily.loc[daily.index[-1], "low"] = box_high * 1.12
        daily.loc[daily.index[-1], "volume"] = 3_100_000.0
    return daily


def test_structure_profile_separates_good_base_failed_and_no_volume_breakout() -> None:
    good_base = leader_algorithms.estimate_structure(_structure_fixture("good_base"))
    no_volume_breakout = leader_algorithms.estimate_structure(_structure_fixture("no_volume_breakout"))
    failed_breakout = leader_algorithms.estimate_structure(_structure_fixture("failed_breakout"))

    expected_fields = {
        "base_depth_pct",
        "loose_base_risk_score",
        "support_violation_count",
        "breakout_failure_count",
        "breakout_volume_quality_score",
        "structure_reject_reason_codes",
        "box_touch_count",
        "support_hold_count",
        "dry_volume_score",
        "failed_breakout_risk_score",
        "breakout_quality_score",
        "structure_confidence",
    }
    assert expected_fields <= set(good_base)
    assert bool(good_base["box_valid"]) is True
    assert good_base["box_touch_count"] >= 2
    assert good_base["support_hold_count"] >= 2
    assert good_base["structure_confidence"] >= 50.0
    assert bool(no_volume_breakout["breakout_confirmed"]) is False
    assert "no_volume_breakout" in str(no_volume_breakout["structure_reject_reason_codes"])
    assert no_volume_breakout["breakout_quality_score"] < good_base["structure_readiness_score"]
    assert "failed_breakout" in str(failed_breakout["structure_reject_reason_codes"])
    assert failed_breakout["failed_breakout_risk_score"] > good_base["failed_breakout_risk_score"]
    assert failed_breakout["breakout_quality_score"] < good_base["breakout_quality_score"]


def test_structure_profile_penalizes_loose_support_break_and_extended_breakouts() -> None:
    good_base = leader_algorithms.estimate_structure(_structure_fixture("good_base"))
    loose_base = leader_algorithms.estimate_structure(_structure_fixture("loose_base"))
    support_break = leader_algorithms.estimate_structure(_structure_fixture("support_break"))
    extended_breakout = leader_algorithms.estimate_structure(_structure_fixture("extended_breakout"))

    assert "loose_base" in str(loose_base["structure_reject_reason_codes"])
    assert loose_base["loose_base_risk_score"] > good_base["loose_base_risk_score"]
    assert loose_base["structure_readiness_score"] < good_base["structure_readiness_score"]
    assert "support_break" in str(support_break["structure_reject_reason_codes"])
    assert support_break["support_violation_count"] >= 1
    assert support_break["structure_readiness_score"] < good_base["structure_readiness_score"]
    assert "extended_breakout" in str(extended_breakout["structure_reject_reason_codes"])
    assert extended_breakout["breakout_quality_score"] < 80.0


def test_leader_quality_diagnostics_explain_confidence_reject_and_extended_reasons() -> None:
    feature_table = pd.DataFrame(
        [
            {
                "symbol": "STRONG",
                "bars": 320,
                "rs_proxy_confidence": 92.0,
                "rs_proxy_sample_count": 80,
                "rs_proxy_component_coverage": 4,
                "hidden_rs_confidence": 68.0,
                "hidden_rs_weak_day_count": 12,
                "structure_confidence": 84.0,
                "liquidity_quality_score": 88.0,
                "traded_value_20d": 50_000_000.0,
                "trend_integrity_score": 88.0,
                "rs_rank_true": 94.0,
                "rs_line_score": 87.0,
                "group_strength_score": 86.0,
                "structure_readiness_score": 72.0,
                "distance_to_52w_high": 0.06,
                "extension_risk_score": 28.0,
                "ret_20d": 0.08,
                "distance_from_ma50": 0.04,
                "rvol": 1.1,
                "close_gt_50": True,
            },
            {
                "symbol": "SHORT",
                "bars": 135,
                "rs_proxy_confidence": 22.0,
                "rs_proxy_sample_count": 3,
                "rs_proxy_component_coverage": 1,
                "hidden_rs_confidence": 0.0,
                "hidden_rs_weak_day_count": 0,
                "structure_confidence": 64.0,
                "liquidity_quality_score": 72.0,
                "traded_value_20d": 20_000_000.0,
                "trend_integrity_score": 76.0,
                "rs_rank_true": 86.0,
                "rs_line_score": 74.0,
                "group_strength_score": 76.0,
                "structure_readiness_score": 47.0,
                "distance_to_52w_high": 0.12,
                "extension_risk_score": 42.0,
                "ret_20d": 0.05,
                "distance_from_ma50": 0.05,
                "rvol": 1.0,
                "close_gt_50": True,
            },
            {
                "symbol": "REJECT",
                "bars": 320,
                "rs_proxy_confidence": 78.0,
                "rs_proxy_sample_count": 70,
                "rs_proxy_component_coverage": 4,
                "hidden_rs_confidence": 32.0,
                "hidden_rs_weak_day_count": 4,
                "structure_confidence": 22.0,
                "liquidity_quality_score": 18.0,
                "traded_value_20d": 100_000.0,
                "trend_integrity_score": 25.0,
                "rs_rank_true": 42.0,
                "rs_line_score": 25.0,
                "group_strength_score": 24.0,
                "structure_readiness_score": 18.0,
                "distance_to_52w_high": 0.44,
                "extension_risk_score": 10.0,
                "ret_20d": -0.08,
                "distance_from_ma50": 0.21,
                "rvol": 0.5,
                "close_gt_50": False,
            },
            {
                "symbol": "EXT",
                "bars": 310,
                "rs_proxy_confidence": 82.0,
                "rs_proxy_sample_count": 72,
                "rs_proxy_component_coverage": 4,
                "hidden_rs_confidence": 56.0,
                "hidden_rs_weak_day_count": 8,
                "structure_confidence": 70.0,
                "liquidity_quality_score": 79.0,
                "traded_value_20d": 42_000_000.0,
                "trend_integrity_score": 82.0,
                "rs_rank_true": 93.0,
                "rs_line_score": 86.0,
                "group_strength_score": 84.0,
                "structure_readiness_score": 66.0,
                "distance_to_52w_high": 0.01,
                "extension_risk_score": 79.0,
                "ret_20d": 0.33,
                "distance_from_ma50": 0.22,
                "rvol": 3.1,
                "close_gt_50": True,
            },
        ]
    )
    leaders = pd.DataFrame(
        [
            {"symbol": "STRONG", "label": "strong_leader", "leader_tier": "strong", "entry_suitability": "fresh", "leader_score": 88.0, "leader_sort_score": 88.0},
            {"symbol": "SHORT", "label": "emerging_leader", "leader_tier": "emerging", "entry_suitability": "watch", "leader_score": 72.0, "leader_sort_score": 72.0},
            {"symbol": "REJECT", "label": "reject", "leader_tier": "reject", "entry_suitability": "avoid", "leader_score": 24.0, "leader_sort_score": 0.0},
            {"symbol": "EXT", "label": "extended_leader", "leader_tier": "strong", "entry_suitability": "extended", "leader_score": 90.0, "leader_sort_score": 72.0},
        ]
    )

    diagnostics, summary = leader_quality.build_leader_quality_artifacts(
        feature_table=feature_table,
        leaders=leaders,
        group_table=pd.DataFrame(),
        calibration=LeaderLaggingAnalyzer().default_actual_data_calibration(),
    )

    strong = diagnostics.loc[diagnostics["symbol"] == "STRONG"].iloc[0]
    short = diagnostics.loc[diagnostics["symbol"] == "SHORT"].iloc[0]
    reject = diagnostics.loc[diagnostics["symbol"] == "REJECT"].iloc[0]
    extended = diagnostics.loc[diagnostics["symbol"] == "EXT"].iloc[0]

    assert strong["confidence_bucket"] == "high"
    assert short["confidence_bucket"] == "low"
    assert "short_history" in short["low_confidence_reason_codes"]
    assert "weak_rs_proxy_sample" in short["low_confidence_reason_codes"]
    assert "no_weak_market_sample" in short["low_confidence_reason_codes"]
    assert "liquidity_fail" in reject["reject_reason_codes"]
    assert "trend_fail" in reject["reject_reason_codes"]
    assert "rs_fail" in reject["reject_reason_codes"]
    assert "group_fail" in reject["reject_reason_codes"]
    assert "structure_fail" in reject["reject_reason_codes"]
    assert "near_high_fail" in reject["reject_reason_codes"]
    assert "short_term_return_extension" in extended["extended_reason_codes"]
    assert "ma50_distance_chase" in extended["extended_reason_codes"]
    assert "near_high_chase" in extended["extended_reason_codes"]
    assert "high_rvol_chase" in extended["extended_reason_codes"]
    assert "near_rs_threshold" in short["threshold_proximity_codes"]
    assert "near_structure_threshold" in short["threshold_proximity_codes"]
    assert "near_extension_threshold" in extended["threshold_proximity_codes"]
    assert summary["leader_count"] == 4
    assert summary["label_counts"]["reject"] == 1
    assert summary["confidence_bucket_counts"]["low"] == 1


def test_leader_threshold_tuning_skips_when_sample_is_too_small() -> None:
    base = LeaderLaggingAnalyzer().default_actual_data_calibration()

    tuned, report, summary = leader_tuning.build_leader_threshold_tuning(
        base_calibration=base,
        candidate_quality_diagnostics=_tuning_diagnostics(rows=12, leader_rows=0, reject_reason="rs_fail"),
        standalone=True,
    )

    assert tuned["leader_rs_rank_min"] == base["leader_rs_rank_min"]
    assert summary["eligible"] is False
    assert summary["leader_tuning_applied"] is False
    assert "insufficient_candidate_sample" in summary["reason_codes"]
    assert not report.empty


def test_leader_threshold_tuning_is_disabled_by_policy_when_not_enabled() -> None:
    base = LeaderLaggingAnalyzer().default_actual_data_calibration()

    tuned, report, summary = leader_tuning.build_leader_threshold_tuning(
        base_calibration=base,
        candidate_quality_diagnostics=_tuning_diagnostics(rows=80, leader_rows=20),
        standalone=False,
        enabled=False,
    )

    assert tuned == base
    assert summary["eligible"] is False
    assert summary["leader_tuning_applied"] is False
    assert "disabled_by_policy" in summary["reason_codes"]
    assert not report.empty


def test_leader_threshold_tuning_conservatively_relaxes_over_reject_distribution() -> None:
    base = LeaderLaggingAnalyzer().default_actual_data_calibration()

    tuned, report, summary = leader_tuning.build_leader_threshold_tuning(
        base_calibration=base,
        candidate_quality_diagnostics=_tuning_diagnostics(
            rows=80,
            leader_rows=0,
            reject_reason="rs_fail,near_high_fail",
        ),
        standalone=True,
    )

    assert summary["eligible"] is True
    assert summary["leader_tuning_applied"] is True
    assert tuned["leader_rs_rank_min"] == pytest.approx(base["leader_rs_rank_min"] - 3.0)
    assert tuned["leader_distance_to_high_max"] == pytest.approx(base["leader_distance_to_high_max"] + 0.03)
    assert tuned["leader_confirmed_score_min"] == pytest.approx(base["leader_confirmed_score_min"] - 2.0)
    assert tuned["leader_group_strength_min"] == base["leader_group_strength_min"]
    assert "over_reject_relaxation" in summary["reason_codes"]
    assert set(report["threshold_key"]) >= {
        "leader_rs_rank_min",
        "leader_distance_to_high_max",
        "leader_confirmed_score_min",
    }


def test_leader_threshold_tuning_conservatively_tightens_over_selected_distribution() -> None:
    base = LeaderLaggingAnalyzer().default_actual_data_calibration()

    tuned, _report, summary = leader_tuning.build_leader_threshold_tuning(
        base_calibration=base,
        candidate_quality_diagnostics=_tuning_diagnostics(rows=80, leader_rows=20),
        standalone=False,
    )

    assert summary["eligible"] is True
    assert summary["leader_tuning_applied"] is True
    assert tuned["leader_rs_rank_min"] == pytest.approx(base["leader_rs_rank_min"] + 3.0)
    assert tuned["leader_group_strength_min"] == pytest.approx(base["leader_group_strength_min"] + 3.0)
    assert tuned["leader_distance_to_high_max"] == pytest.approx(base["leader_distance_to_high_max"] - 0.03)
    assert tuned["leader_confirmed_score_min"] == pytest.approx(base["leader_confirmed_score_min"] + 2.0)
    assert "over_selection_tightening" in summary["reason_codes"]


def test_extended_leader_remains_output_candidate_with_entry_penalty() -> None:
    row = {
        "close_gt_50": True,
        "traded_value_20d": 100_000_000.0,
        "illiq_score": 80.0,
        "distance_from_52w_low": 1.0,
        "distance_to_52w_high": 0.01,
        "benchmark_relative_strength": 1.15,
        "rs_line_slope": 0.04,
        "rs_rank_true": 97.0,
        "rs_line_score": 90.0,
        "group_strength_score": 88.0,
        "trend_integrity_score": 92.0,
        "structure_readiness_score": 72.0,
        "leadership_freshness_score": 70.0,
        "hidden_rs_score": 65.0,
        "extension_risk_score": 86.0,
    }

    classification = leader_algorithms.classify_leader(
        row,
        calibration=LeaderLaggingAnalyzer().default_actual_data_calibration(),
        traded_value_floor=2_000_000.0,
        leader_score=84.0,
    )

    assert classification.hybrid_gate_pass is True
    assert classification.strict_rs_gate_pass is True
    assert classification.leader_tier == "strong"
    assert classification.label == "extended_leader"
    assert classification.legacy_label == "Extended Leader"
    assert classification.entry_suitability == "extended"
    assert classification.leader_sort_score < 84.0


def test_reject_or_avoid_leader_is_not_used_for_follower_pair_generation() -> None:
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
    strong_leader = leaders.loc[leaders["symbol"] == "LEAD"].iloc[[0]].copy()
    reject_leader = strong_leader.iloc[0].copy()
    reject_leader["symbol"] = "REJECT_LEAD"
    reject_leader["ticker"] = "REJECT_LEAD"
    reject_leader["label"] = "reject"
    reject_leader["leader_tier"] = "reject"
    reject_leader["entry_suitability"] = "avoid"
    reject_leader["ret_20d"] = 0.95
    reject_leader["ret_60d"] = 1.80
    reject_leader["event_proxy_score"] = 100.0
    frames["REJECT_LEAD"] = frames["LEAD"].copy()
    mixed_leaders = pd.concat([reject_leader.to_frame().T, strong_leader], ignore_index=True)

    followers, pairs = analyzer.analyze_followers(
        feature_table=feature_table,
        leaders=mixed_leaders,
        group_table=group_table,
        market_context=market_context,
        frames=frames,
    )

    assert not followers.empty
    assert not pairs.empty
    assert "REJECT_LEAD" not in set(pairs["leader_symbol"].astype(str))
    assert "REJECT_LEAD" not in set(followers["linked_leader"].astype(str))


def _lead_lag_frame_from_returns(returns: np.ndarray, *, start: str = "2025-01-02") -> pd.DataFrame:
    closes = 100.0 * np.cumprod(1.0 + returns)
    volumes = np.full(len(closes), 1_000_000.0)
    return _daily_from_closes(closes, volumes, start=start)


def test_follower_v2_lag_profile_separates_stable_and_unstable_pairs() -> None:
    follower_algorithms = importlib.import_module("screeners.leader_lagging.followers")
    leader_returns = np.zeros(100)
    leader_returns[[15, 25, 35, 45, 55, 65, 75, 85]] = [0.05, 0.04, 0.06, 0.05, 0.04, 0.06, 0.05, 0.04]
    stable_returns = np.roll(leader_returns, 3) * 0.72
    stable_returns[:3] = 0.0
    noisy_returns = np.sin(np.linspace(0.0, 9.0 * np.pi, 100)) * 0.012

    leader_frame = _lead_lag_frame_from_returns(leader_returns)
    stable_frame = _lead_lag_frame_from_returns(stable_returns)
    noisy_frame = _lead_lag_frame_from_returns(noisy_returns)

    stable = follower_algorithms.lagged_return_profile(stable_frame, leader_frame)
    noisy = follower_algorithms.lagged_return_profile(noisy_frame, leader_frame)

    assert stable["lag_days"] == 3
    assert stable["lag_profile_sample_count"] >= 20
    assert stable["lag_profile_stability_score"] >= 50.0
    assert stable["pair_evidence_confidence"] >= 55.0
    assert "unstable_lag_profile" not in str(stable["follower_reject_reason_codes"])
    assert noisy["pair_evidence_confidence"] < stable["pair_evidence_confidence"]
    assert "unstable_lag_profile" in str(noisy["follower_reject_reason_codes"])


def test_follower_v2_lag_profile_handles_constant_returns_without_runtime_warning() -> None:
    dates = pd.bdate_range("2025-01-02", periods=80)
    flat_frame = pd.DataFrame({"date": dates, "adj_close": np.full(80, 100.0)})

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        profile = follower_algorithms.lagged_return_profile(
            flat_frame,
            flat_frame,
            frames_are_normalized=True,
        )

    assert profile["lag_days"] is None
    assert profile["lagged_corr"] is None
    assert profile["pair_evidence_confidence"] == 0.0
    assert "unstable_lag_profile" in str(profile["follower_reject_reason_codes"])


def test_follower_v2_propagation_state_marks_caught_up_and_overreacted() -> None:
    follower_algorithms = importlib.import_module("screeners.leader_lagging.followers")

    assert follower_algorithms.propagation_state(0.45) == "early_response"
    assert follower_algorithms.propagation_state(0.95) == "already_caught_up"
    assert follower_algorithms.propagation_state(1.25) == "overreacted"
    assert follower_algorithms.catchup_room_score(0.20, 0.28, 0.45) > follower_algorithms.catchup_room_score(0.02, 0.03, 0.95)


def test_leader_follower_and_pair_outputs_include_upgrade_fields() -> None:
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
    confirmed_leaders = leaders[leaders["leader_tier"] == "strong"].copy()
    followers, pairs = analyzer.analyze_followers(
        feature_table=feature_table,
        leaders=confirmed_leaders,
        group_table=group_table,
        market_context=market_context,
        frames=frames,
    )

    leader_columns = {
        "weighted_rs_score",
        "rs_rank_true",
        "rs_rank_proxy",
        "rs_proxy_sample_count",
        "rs_proxy_component_coverage",
        "rs_proxy_confidence",
        "rs_quality_score",
        "leadership_freshness_score",
        "early_leader_score",
        "momentum_persistence_score",
        "near_high_leadership_score",
        "hidden_rs_score",
        "hidden_rs_weak_day_count",
        "hidden_rs_down_day_excess_return",
        "hidden_rs_drawdown_resilience",
        "hidden_rs_weak_window_excess_return",
        "hidden_rs_confidence",
        "leader_rs_state",
        "leader_tier",
        "entry_suitability",
        "legacy_label",
        "hybrid_gate_pass",
        "strict_rs_gate_pass",
        "fading_risk_score",
        "structure_readiness_score",
        "breakout_confirmation_score",
        "box_touch_count",
        "support_hold_count",
        "dry_volume_score",
        "failed_breakout_risk_score",
        "breakout_quality_score",
        "structure_confidence",
        "extension_risk_score",
        "source_evidence_tags",
    }
    follower_columns = {
        "peer_lead_score",
        "best_lag_days",
        "lagged_corr",
        "follower_confidence_score",
        "pair_evidence_confidence",
        "lag_profile_sample_count",
        "lag_profile_stability_score",
        "catchup_room_score",
        "propagation_state",
        "follower_reject_reason_codes",
        "propagation_ratio",
        "structure_preservation_score",
        "sympathy_freshness_score",
        "link_evidence_tags",
    }
    pair_columns = {
        "lag_days",
        "lead_lag_profile",
        "leader_event_return",
        "follower_event_return",
        "propagation_ratio",
        "connection_type",
        "pair_confidence",
        "pair_evidence_confidence",
        "lag_profile_sample_count",
        "lag_profile_stability_score",
        "catchup_room_score",
        "propagation_state",
    }

    assert leader_columns <= set(leaders.columns)
    assert "rotation_state" not in leaders.columns
    assert "leader_lifecycle_phase" not in leaders.columns
    assert "prior_cycle_exclusion_score" not in leaders.columns
    assert "rotation_candidate_score" not in leaders.columns
    assert follower_columns <= set(followers.columns)
    assert pair_columns <= set(pairs.columns)
    lead_row = leaders.loc[leaders["symbol"] == "LEAD"].iloc[0]
    follower_row = followers.loc[followers["symbol"] == "FOLLOW"].iloc[0]
    pair_row = pairs.loc[pairs["follower_symbol"] == "FOLLOW"].iloc[0]

    assert lead_row["leader_rs_state"] in {"rising", "stable", "fading", "weakening", "unknown"}
    assert lead_row["label"] in {"strong_leader", "emerging_leader", "extended_leader", "reject"}
    assert lead_row["legacy_label"] in {"Confirmed Leader", "Emerging Leader", "Extended Leader", "Too Weak, Reject"}
    assert lead_row["leader_tier"] in {"strong", "emerging", "reject"}
    assert lead_row["entry_suitability"] in {"fresh", "watch", "extended", "avoid"}
    assert float(lead_row["rs_proxy_confidence"]) > 0.0
    assert 0.0 <= float(lead_row["hidden_rs_confidence"]) <= 100.0
    assert float(lead_row["structure_confidence"]) > 0.0
    assert float(follower_row["peer_lead_score"]) >= 0.0
    assert float(follower_row["pair_evidence_confidence"]) >= 0.0
    assert follower_row["propagation_state"] in {"early_response", "already_caught_up", "overreacted", "no_response", "unknown"}
    assert follower_row["follower_confidence_score"] >= 0.0
    assert pair_row["lag_days"] in {1, 2, 3, 5}
    assert "same_industry" in str(pair_row["connection_type"])


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
    feature_table.loc[feature_table["symbol"] == "FOLLOW", "benchmark_relative_strength_slope"] = 0.09
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
    confirmed_leaders = leaders[leaders["leader_tier"] == "strong"].copy()
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
    assert (output_root / "leader_quality_diagnostics.csv").exists()
    assert (output_root / "leader_candidate_quality_diagnostics.csv").exists()
    assert (output_root / "leader_quality_summary.json").exists()
    assert (output_root / "leader_candidate_quality_summary.json").exists()
    assert (output_root / "leader_threshold_tuning_report.csv").exists()
    assert (output_root / "leader_threshold_tuning_report.json").exists()
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
    assert summary["leader_quality"]["leader_count"] >= 1
    assert summary["leader_quality"]["leader_count"] == len(result["leaders"])
    assert summary["leader_candidate_quality"]["leader_count"] >= summary["leader_quality"]["leader_count"]
    assert "follower_lag_pruning" in summary
    assert summary["follower_lag_pruning"]["mode"] == "balanced"
    assert summary["follower_lag_pruning"]["pair_evaluations"] >= 1
    assert result["market_summary"]["follower_lag_pruning"]["mode"] == "balanced"
    assert "confidence_bucket_counts" in summary["leader_quality"]
    assert "confidence_bucket_counts" in summary["leader_candidate_quality"]
    assert summary["actual_data_calibration"]["leader_tuning_applied"] is False
    assert summary["leader_threshold_tuning"]["eligible"] is False
    assert "regime_score" not in summary
    assert "breadth_50" not in summary
    assert "breadth_200" not in summary
    assert "high_low_ratio" not in summary
    assert "top_group_share" not in summary

    group_dashboard = pd.read_csv(output_root / "group_dashboard.csv", keep_default_na=False)
    assert "group_strength_score" in group_dashboard.columns
    assert "group_state" in group_dashboard.columns
    assert "group_rank" in group_dashboard.columns
    assert "group_overlay_score" not in group_dashboard.columns
    assert "industry_rs_pct" not in group_dashboard.columns
    assert "sector_rs_pct" not in group_dashboard.columns

    leader_quality_diagnostics = pd.read_csv(output_root / "leader_quality_diagnostics.csv", keep_default_na=False)
    leader_candidate_quality_diagnostics = pd.read_csv(output_root / "leader_candidate_quality_diagnostics.csv", keep_default_na=False)
    persisted_leaders = pd.read_csv(output_root / "leaders.csv", keep_default_na=False)
    assert {
        "leader_confidence_score",
        "confidence_bucket",
        "low_confidence_reason_codes",
        "reject_reason_codes",
        "extended_reason_codes",
        "threshold_proximity_codes",
    } <= set(leader_quality_diagnostics.columns)
    assert set(leader_quality_diagnostics["symbol"].astype(str)) == set(persisted_leaders["symbol"].astype(str))
    assert set(leader_candidate_quality_diagnostics["symbol"].astype(str)) >= set(leader_quality_diagnostics["symbol"].astype(str))
    assert "reject" in set(leader_candidate_quality_diagnostics["label"].astype(str))
    reject_candidate = leader_candidate_quality_diagnostics.loc[
        leader_candidate_quality_diagnostics["label"].astype(str) == "reject"
    ].iloc[0]
    assert reject_candidate["reject_reason_codes"]
    assert "reject" not in set(leader_quality_diagnostics["label"].astype(str))

    with open(output_root / "actual_data_calibration.json", "r", encoding="utf-8") as handle:
        persisted_calibration = json.load(handle)
    assert persisted_calibration["leader_tuning_applied"] is False
    assert "leader_tuning_adjustments" in persisted_calibration


def test_run_leader_lagging_screening_supports_standalone_without_compat(monkeypatch) -> None:
    frames, metadata_map, benchmark_daily = _build_market_fixture()
    output_root = runtime_root("_test_runtime_leader_lagging_standalone")
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
    monkeypatch.setattr(
        leader_lagging_screener,
        "load_market_truth_snapshot",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("compat market truth should not be read in standalone mode")),
    )

    result = leader_lagging_screener.run_leader_lagging_screening(market="us", standalone=True)

    assert isinstance(result["pattern_included_candidates"], list)
    assert result["market_summary"]["market_truth_source"] == "local_standalone"
    assert result["market_summary"]["core_overlay_applied"] is False
    assert result["market_summary"]["market_alias"] in {"RISK_ON", "NEUTRAL", "RISK_OFF"}
    assert result["market_summary"]["market_alignment_score"] is not None

    with open(output_root / "market_summary.json", "r", encoding="utf-8") as handle:
        summary = json.load(handle)
    assert summary["market_truth_source"] == "local_standalone"
    assert summary["core_overlay_applied"] is False
    assert (output_root / "pattern_included_candidates.csv").exists()


def test_compute_group_table_avoids_fillna_futurewarning() -> None:
    frames, metadata_map, benchmark_daily = _build_market_fixture()
    output_root = runtime_root("_test_runtime_leader_lagging_warning_regression")
    output_root.mkdir(parents=True, exist_ok=True)

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(leader_lagging_screener, "ensure_market_dirs", lambda market: None)
        monkeypatch.setattr(market_runtime, "get_leader_lagging_results_dir", lambda market: str(output_root))
        monkeypatch.setattr(LeaderLaggingScreener, "_load_frames", lambda self: {key: value.copy() for key, value in frames.items()})
        monkeypatch.setattr(LeaderLaggingScreener, "_load_metadata_map", lambda self: metadata_map.copy())
        monkeypatch.setattr(
            leader_lagging_screener,
            "load_benchmark_data",
            lambda *args, **kwargs: ("SPY", benchmark_daily.copy()),
        )
        monkeypatch.setattr(
            leader_lagging_screener,
            "load_market_truth_snapshot",
            lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("compat market truth should not be read in standalone mode")),
        )

        with warnings.catch_warnings():
            warnings.simplefilter("error", FutureWarning)
            result = leader_lagging_screener.run_leader_lagging_screening(market="us", standalone=True)
    finally:
        monkeypatch.undo()

    assert result["market_summary"]["market_truth_source"] == "local_standalone"
    group_dashboard = pd.read_csv(output_root / "group_dashboard.csv", keep_default_na=False)
    software_row = group_dashboard.loc[group_dashboard["industry_key"] == build_industry_key("Tech", "Software")].iloc[0]
    assert software_row["group_rank"] == pytest.approx(1.0)
    assert software_row["group_state"] == ""


def test_run_leader_lagging_screening_uses_benchmark_as_of_for_compat(monkeypatch) -> None:
    frames, metadata_map, benchmark_daily = _build_market_fixture()
    output_root = runtime_root("_test_runtime_leader_lagging_benchmark_asof")
    output_root.mkdir(parents=True, exist_ok=True)

    captured: dict[str, str] = {}
    truncated_benchmark = benchmark_daily.iloc[:-5].copy()
    expected_as_of = str(truncated_benchmark["date"].iloc[-1].date())
    fresher_frame_as_of = str(next(iter(frames.values()))["date"].iloc[-1].date())
    assert fresher_frame_as_of != expected_as_of

    monkeypatch.setattr(leader_lagging_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(market_runtime, "get_leader_lagging_results_dir", lambda market: str(output_root))
    monkeypatch.setattr(LeaderLaggingScreener, "_load_frames", lambda self: {key: value.copy() for key, value in frames.items()})
    monkeypatch.setattr(LeaderLaggingScreener, "_load_metadata_map", lambda self: metadata_map.copy())
    monkeypatch.setattr(
        leader_lagging_screener,
        "load_benchmark_data",
        lambda *args, **kwargs: ("SPY", truncated_benchmark.copy()),
    )

    def _capture_market_truth(market: str, *, as_of_date: str):
        captured["as_of_date"] = as_of_date
        return leader_core_bridge.MarketTruthSnapshot(
            market=market,
            as_of=as_of_date,
            leader_core=_build_leader_core_snapshot(metadata_map, as_of=as_of_date),
            market_context={
                "market": market,
                "as_of": as_of_date,
                "schema_version": leader_core_bridge.MARKET_CONTEXT_SCHEMA_VERSION,
                "engine_version": "compat_market_context_v1",
            },
            market_alias="RISK_ON",
            regime_state="uptrend",
            top_state="risk_on",
            market_state="uptrend",
            breadth_state="broad_participation",
            concentration_state="diversified",
            leadership_state="growth_ai",
            market_alignment_score=82.0,
            breadth_support_score=79.0,
            rotation_support_score=88.0,
            leader_health_score=72.0,
            leader_health_status="HEALTHY",
        )

    monkeypatch.setattr(
        leader_lagging_screener,
        "load_market_truth_snapshot",
        _capture_market_truth,
    )

    result = leader_lagging_screener.run_leader_lagging_screening(market="us")

    assert captured["as_of_date"] == expected_as_of
    assert result["market_summary"]["market_truth_source"] == "market_intel_compat"


def test_run_leader_lagging_screening_scopes_local_frames_to_benchmark_as_of(monkeypatch) -> None:
    frames, metadata_map, benchmark_daily = _build_market_fixture()
    output_root = runtime_root("_test_runtime_leader_lagging_local_asof")
    output_root.mkdir(parents=True, exist_ok=True)
    data_root = runtime_root("_test_runtime_leader_lagging_local_asof_data")
    market_data_dir = data_root / "us"
    market_data_dir.mkdir(parents=True, exist_ok=True)
    for symbol in frames:
        (market_data_dir / f"{symbol}.csv").write_text("date,close\n2025-01-02,1\n", encoding="utf-8")

    truncated_benchmark = benchmark_daily.iloc[:-5].copy()
    expected_as_of = str(truncated_benchmark["date"].iloc[-1].date())
    observed_as_of: list[str | None] = []
    runtime_context = RuntimeContext(market="us")

    monkeypatch.setattr(leader_lagging_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(market_runtime, "get_leader_lagging_results_dir", lambda market: str(output_root))
    monkeypatch.setattr(leader_lagging_screener, "get_market_data_dir", lambda market: str(market_data_dir))
    monkeypatch.setattr(LeaderLaggingScreener, "_load_metadata_map", lambda self: metadata_map.copy())
    monkeypatch.setattr(
        leader_lagging_screener,
        "load_benchmark_data",
        lambda *args, **kwargs: ("SPY", truncated_benchmark.copy()),
    )

    def _capture_frame(market, symbol, **kwargs):  # noqa: ANN001, ANN202
        observed_as_of.append(kwargs.get("as_of"))
        return frames[str(symbol).upper()].copy()

    monkeypatch.setattr(leader_lagging_screener, "load_local_ohlcv_frame", _capture_frame)
    monkeypatch.setattr(
        leader_lagging_screener,
        "load_market_truth_snapshot",
        lambda market, *, as_of_date: leader_core_bridge.MarketTruthSnapshot(
            market=market,
            as_of=as_of_date,
            leader_core=_build_leader_core_snapshot(metadata_map, as_of=as_of_date),
            market_context={
                "market": market,
                "as_of": as_of_date,
                "schema_version": leader_core_bridge.MARKET_CONTEXT_SCHEMA_VERSION,
                "engine_version": "compat_market_context_v1",
            },
            market_alias="RISK_ON",
            regime_state="uptrend",
            top_state="risk_on",
            market_state="uptrend",
            breadth_state="broad_participation",
            concentration_state="diversified",
            leadership_state="growth_ai",
            market_alignment_score=82.0,
            breadth_support_score=79.0,
            rotation_support_score=88.0,
            leader_health_score=72.0,
            leader_health_status="HEALTHY",
        ),
    )

    result = leader_lagging_screener.run_leader_lagging_screening(
        market="us",
        runtime_context=runtime_context,
    )

    assert result["market_summary"]["market_truth_source"] == "market_intel_compat"
    assert observed_as_of
    assert set(observed_as_of) == {expected_as_of}
    freshness = runtime_context.runtime_state["data_freshness"]["stages"]["leader_lagging"]
    assert freshness["counts"]["future_or_partial"] == len(frames)
    assert freshness["mode"] == "default_completed_session"


def test_run_leader_lagging_screening_preserves_explicit_runtime_as_of(monkeypatch) -> None:
    frames, metadata_map, benchmark_daily = _build_market_fixture()
    output_root = runtime_root("_test_runtime_leader_lagging_explicit_asof")
    output_root.mkdir(parents=True, exist_ok=True)
    data_root = runtime_root("_test_runtime_leader_lagging_explicit_asof_data")
    market_data_dir = data_root / "us"
    market_data_dir.mkdir(parents=True, exist_ok=True)
    for symbol in frames:
        (market_data_dir / f"{symbol}.csv").write_text("date,close\n2025-01-02,1\n", encoding="utf-8")

    explicit_as_of = str(benchmark_daily["date"].iloc[-8].date())
    benchmark_latest = str(benchmark_daily["date"].iloc[-1].date())
    assert explicit_as_of != benchmark_latest
    observed_as_of: list[str | None] = []
    captured: dict[str, str] = {}
    runtime_context = RuntimeContext(market="us", as_of_date=explicit_as_of)

    monkeypatch.setattr(leader_lagging_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(market_runtime, "get_leader_lagging_results_dir", lambda market: str(output_root))
    monkeypatch.setattr(leader_lagging_screener, "get_market_data_dir", lambda market: str(market_data_dir))
    monkeypatch.setattr(LeaderLaggingScreener, "_load_metadata_map", lambda self: metadata_map.copy())
    monkeypatch.setattr(
        leader_lagging_screener,
        "load_benchmark_data",
        lambda *args, **kwargs: ("SPY", benchmark_daily.copy()),
    )

    def _capture_frame(market, symbol, **kwargs):  # noqa: ANN001, ANN202
        observed_as_of.append(kwargs.get("as_of"))
        return frames[str(symbol).upper()].copy()

    def _capture_market_truth(market: str, *, as_of_date: str):
        captured["as_of_date"] = as_of_date
        return leader_core_bridge.MarketTruthSnapshot(
            market=market,
            as_of=as_of_date,
            leader_core=_build_leader_core_snapshot(metadata_map, as_of=as_of_date),
            market_context={
                "market": market,
                "as_of": as_of_date,
                "schema_version": leader_core_bridge.MARKET_CONTEXT_SCHEMA_VERSION,
                "engine_version": "compat_market_context_v1",
            },
            market_alias="RISK_ON",
            regime_state="uptrend",
            top_state="risk_on",
            market_state="uptrend",
            breadth_state="broad_participation",
            concentration_state="diversified",
            leadership_state="growth_ai",
            market_alignment_score=82.0,
            breadth_support_score=79.0,
            rotation_support_score=88.0,
            leader_health_score=72.0,
            leader_health_status="HEALTHY",
        )

    monkeypatch.setattr(leader_lagging_screener, "load_local_ohlcv_frame", _capture_frame)
    monkeypatch.setattr(leader_lagging_screener, "load_market_truth_snapshot", _capture_market_truth)

    result = leader_lagging_screener.run_leader_lagging_screening(
        market="us",
        runtime_context=runtime_context,
    )

    assert result["market_summary"]["market_truth_source"] == "market_intel_compat"
    assert runtime_context.as_of_date == explicit_as_of
    assert captured["as_of_date"] == explicit_as_of
    assert observed_as_of
    assert set(observed_as_of) == {explicit_as_of}
    freshness = runtime_context.runtime_state["data_freshness"]["stages"]["leader_lagging"]
    assert freshness["mode"] == "explicit_replay"


@pytest.mark.local_data
@pytest.mark.parametrize(
    ("market", "benchmark_symbol", "symbols"),
    [
        ("us", "SPY", ["AAPL", "NVDA", "QQQ"]),
        ("kr", "KOSPI", ["000270", "000660", "005930"]),
    ],
)
def test_short_local_data_smoke_exposes_v2_diagnostics(market: str, benchmark_symbol: str, symbols: list[str]) -> None:
    data_dir = Path("data") / market
    benchmark_path = data_dir / f"{benchmark_symbol}.csv"
    symbol_paths = [data_dir / f"{symbol}.csv" for symbol in symbols]
    present_paths = [path for path in symbol_paths if path.exists()]
    if not benchmark_path.exists() or len(present_paths) < 2:
        pytest.skip(f"local {market} smoke data is not available")

    analyzer = LeaderLaggingAnalyzer()
    benchmark_daily = pd.read_csv(benchmark_path)
    metadata_map = {
        path.stem.upper(): {
            "symbol": path.stem.upper(),
            "sector": f"{market.upper()}_SMOKE",
            "industry": "SHORT_UNIVERSE",
        }
        for path in present_paths
    }
    frames = {path.stem.upper(): pd.read_csv(path) for path in present_paths}
    feature_rows = [
        analyzer.compute_symbol_features(
            symbol=symbol,
            market=market,
            daily_frame=frame,
            benchmark_daily=benchmark_daily,
            metadata=metadata_map.get(symbol),
        )
        for symbol, frame in frames.items()
    ]
    feature_table = analyzer.finalize_feature_table(pd.DataFrame(feature_rows))
    if feature_table.empty:
        pytest.skip(f"local {market} smoke data did not produce features")
    as_of = str(feature_table["as_of_ts"].dropna().max())
    feature_table = annotate_frame_with_leader_core(
        feature_table,
        leader_core_bridge.empty_leader_core_snapshot(market, as_of),
    )
    group_table = analyzer.compute_group_table(feature_table)
    market_context = analyzer.compute_market_context(
        market=market,
        benchmark_symbol=benchmark_symbol,
        benchmark_daily=benchmark_daily,
        feature_table=feature_table,
        group_table=group_table,
    )
    leaders = analyzer.analyze_leaders(
        feature_table=feature_table,
        group_table=group_table,
        market_context=market_context,
    )

    required_columns = {
        "label",
        "leader_tier",
        "entry_suitability",
        "rs_proxy_confidence",
        "hidden_rs_confidence",
        "structure_confidence",
        "box_touch_count",
        "support_hold_count",
        "breakout_quality_score",
    }
    assert required_columns <= set(leaders.columns)
    quality_diagnostics, quality_summary = leader_quality.build_leader_quality_artifacts(
        feature_table=feature_table,
        leaders=leaders,
        group_table=group_table,
        calibration=analyzer.default_actual_data_calibration(),
    )
    assert {
        "leader_confidence_score",
        "confidence_bucket",
        "threshold_proximity_codes",
    } <= set(quality_diagnostics.columns)
    assert quality_summary["leader_count"] == len(leaders)
    assert set(leaders["label"].dropna().astype(str)) <= {
        "strong_leader",
        "emerging_leader",
        "extended_leader",
        "reject",
    }
    assert {"leader_lifecycle_phase", "prior_cycle_exclusion_score", "rotation_candidate_score"}.isdisjoint(
        leaders.columns
    )
    diagnostic_columns = [
        "rs_proxy_confidence",
        "hidden_rs_confidence",
        "structure_confidence",
        "box_touch_count",
        "support_hold_count",
        "breakout_quality_score",
    ]
    diagnostic_null_share = leaders[diagnostic_columns].isna().mean().mean()
    assert diagnostic_null_share < 0.50
