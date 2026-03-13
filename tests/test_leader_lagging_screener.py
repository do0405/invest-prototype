from __future__ import annotations

import json

import numpy as np
import pandas as pd

from screeners.leader_lagging import screener as leader_lagging_screener
from screeners.leader_lagging.screener import LeaderLaggingAnalyzer, LeaderLaggingScreener
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
                    np.linspace(58.0, 61.0, 40, endpoint=False),
                    np.linspace(61.0, 65.0, 20),
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
    followers, pairs = analyzer.analyze_followers(
        feature_table=feature_table,
        leaders=leaders,
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
