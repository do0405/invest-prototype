from __future__ import annotations

import numpy as np
import pandas as pd

from screeners.augment.chronos_rerank import generate_chronos_rerank_rows
from screeners.augment.pipeline import (
    build_buy_eligible_source_specs,
    build_merged_candidate_pool_rows,
)
from screeners.augment.stumpy_sidecar import generate_stumpy_summary_rows
from screeners.source_contracts import CANONICAL_SOURCE_SPECS


def _make_frame(
    symbol: str,
    closes: np.ndarray,
    *,
    volumes: np.ndarray | None = None,
) -> pd.DataFrame:
    close_array = np.asarray(closes, dtype=float)
    volume_array = np.asarray(
        volumes if volumes is not None else np.linspace(1_000_000, 1_400_000, len(close_array)),
        dtype=float,
    )
    dates = pd.date_range("2025-01-01", periods=len(close_array), freq="B")
    return pd.DataFrame(
        {
            "date": dates.strftime("%Y-%m-%d"),
            "symbol": symbol,
            "open": close_array,
            "high": close_array * 1.01,
            "low": close_array * 0.99,
            "close": close_array,
            "volume": volume_array,
        }
    )


def _distance_fn(left: np.ndarray, right: np.ndarray) -> float:
    left_norm = (left - left.mean()) / max(left.std(), 1e-9)
    right_norm = (right - right.mean()) / max(right.std(), 1e-9)
    return float(np.linalg.norm(left_norm - right_norm))


def _self_profile_fn(series: np.ndarray, window_size: int) -> float:
    trailing = np.asarray(series[-window_size:], dtype=float)
    prior = np.asarray(series[-(2 * window_size) : -window_size], dtype=float)
    if len(prior) != len(trailing):
        return 0.0
    return _distance_fn(trailing, prior)


def test_build_buy_eligible_source_specs_matches_canonical_filter() -> None:
    expected = [spec for spec in CANONICAL_SOURCE_SPECS if spec.buy_eligible]

    assert build_buy_eligible_source_specs() == expected


def test_build_merged_candidate_pool_rows_preserves_source_registry_contract() -> None:
    registry = {
        "AAA": {
            "symbol": "AAA",
            "market": "US",
            "buy_eligible": True,
            "watch_only": False,
            "screen_stage": "DAILY_FOCUS",
            "source_tags": ["QMG_DAILY", "TV_BREAKOUT_RVOL"],
            "primary_source_tag": "QMG_DAILY",
            "primary_source_stage": "DAILY_FOCUS",
            "primary_source_style": "TREND",
            "source_style_tags": ["TREND", "BREAKOUT"],
            "source_priority_score": 10.0,
            "trend_source_bonus": 4.0,
            "ug_source_bonus": 2.0,
            "source_overlap_bonus": 5.0,
            "sector": "Tech",
            "industry": "Software",
            "group_name": "Software",
            "as_of_ts": "2025-03-01",
        }
    }

    rows = build_merged_candidate_pool_rows(registry, market="us")

    assert rows == [
        {
            "symbol": "AAA",
            "market": "US",
            "buy_eligible": True,
            "watch_only": False,
            "screen_stage": "DAILY_FOCUS",
            "source_tags": ["QMG_DAILY", "TV_BREAKOUT_RVOL"],
            "primary_source_tag": "QMG_DAILY",
            "primary_source_stage": "DAILY_FOCUS",
            "primary_source_style": "TREND",
            "source_style_tags": ["TREND", "BREAKOUT"],
            "source_priority_score": 10.0,
            "trend_source_bonus": 4.0,
            "ug_source_bonus": 2.0,
            "source_overlap_bonus": 5.0,
            "sector": "Tech",
            "industry": "Software",
            "group_name": "Software",
            "as_of_ts": "2025-03-01",
        }
    ]


def test_build_merged_candidate_pool_rows_falls_back_to_pipeline_market() -> None:
    registry = {
        "AAA": {
            "symbol": "AAA",
            "buy_eligible": True,
            "watch_only": False,
            "screen_stage": "DAILY_FOCUS",
            "source_tags": ["QMG_DAILY"],
            "primary_source_tag": "QMG_DAILY",
            "primary_source_stage": "DAILY_FOCUS",
            "primary_source_style": "TREND",
            "source_style_tags": ["TREND"],
            "source_priority_score": 10.0,
            "trend_source_bonus": 4.0,
            "ug_source_bonus": 2.0,
            "source_overlap_bonus": 0.0,
            "sector": "",
            "industry": "",
            "group_name": "",
            "as_of_ts": None,
        }
    }

    rows = build_merged_candidate_pool_rows(registry, market="kr")

    assert rows[0]["market"] == "KR"


def test_generate_stumpy_summary_rows_clusters_similar_candidates_and_marks_singleton() -> None:
    base = np.linspace(100.0, 150.0, 180)
    frames = {
        "AAA": _make_frame("AAA", base, volumes=np.linspace(1_000_000, 1_300_000, 180)),
        "BBB": _make_frame("BBB", base * 1.01, volumes=np.linspace(1_050_000, 1_320_000, 180)),
        "CCC": _make_frame("CCC", np.linspace(160.0, 90.0, 180), volumes=np.linspace(2_000_000, 1_000_000, 180)),
    }
    source_rows = [{"symbol": symbol, "market": "us"} for symbol in ("AAA", "BBB", "CCC")]

    rows = generate_stumpy_summary_rows(
        source_rows=source_rows,
        source_tag="QMG_DAILY",
        market="us",
        load_ohlcv_frame_fn=lambda symbol, market, **_: frames.get(symbol, pd.DataFrame()),
        distance_fn=_distance_fn,
        self_profile_fn=_self_profile_fn,
    )

    assert {row["window_size"] for row in rows} == {40, 80, 120}
    grouped = {(row["symbol"], row["window_size"]): row for row in rows}

    assert grouped[("AAA", 80)]["stumpy_cluster_id"] == grouped[("BBB", 80)]["stumpy_cluster_id"]
    assert grouped[("AAA", 80)]["stumpy_status"] == "OK"
    assert grouped[("BBB", 80)]["stumpy_status"] == "OK"
    assert grouped[("CCC", 80)]["stumpy_status"] == "SINGLETON"
    assert grouped[("AAA", 80)]["stumpy_shape_label"] == "UP"
    assert grouped[("CCC", 80)]["stumpy_shape_label"] == "DOWN"
    assert grouped[("AAA", 80)]["stumpy_exemplar_symbol"] in {"AAA", "BBB"}


def test_generate_stumpy_summary_rows_handles_missing_ohlcv_and_short_history() -> None:
    frames = {
        "AAA": _make_frame("AAA", np.linspace(50.0, 80.0, 180)),
        "BBB": _make_frame("BBB", np.linspace(50.0, 70.0, 60)),
    }
    source_rows = [{"symbol": symbol, "market": "us"} for symbol in ("AAA", "BBB", "MISSING")]

    rows = generate_stumpy_summary_rows(
        source_rows=source_rows,
        source_tag="QMG_DAILY",
        market="us",
        load_ohlcv_frame_fn=lambda symbol, market, **_: frames.get(symbol, pd.DataFrame()),
        distance_fn=_distance_fn,
        self_profile_fn=_self_profile_fn,
    )
    grouped = {(row["symbol"], row["window_size"]): row for row in rows}

    assert grouped[("MISSING", 40)]["stumpy_status"] == "MISSING_OHLCV"
    assert grouped[("BBB", 80)]["stumpy_status"] == "INSUFFICIENT_HISTORY"
    assert grouped[("BBB", 120)]["stumpy_status"] == "INSUFFICIENT_HISTORY"


def test_generate_chronos_rerank_rows_scores_all_candidates_and_ranks_them() -> None:
    base = np.linspace(100.0, 150.0, 260)
    frames = {
        "AAA": _make_frame("AAA", base),
        "BBB": _make_frame("BBB", np.linspace(100.0, 120.0, 260)),
        "CCC": _make_frame("CCC", np.concatenate([np.linspace(100.0, 130.0, 240), np.linspace(140.0, 90.0, 20)])),
        "DDD": _make_frame("DDD", np.linspace(100.0, 110.0, 120)),
    }
    merged_pool = [
        {"symbol": "AAA", "market": "US", "source_tags": ["QMG_DAILY"], "primary_source_tag": "QMG_DAILY"},
        {"symbol": "BBB", "market": "US", "source_tags": ["WS_PRIMARY"], "primary_source_tag": "WS_PRIMARY"},
        {"symbol": "CCC", "market": "US", "source_tags": ["LL_LEADER"], "primary_source_tag": "LL_LEADER"},
        {"symbol": "DDD", "market": "US", "source_tags": ["TV_BREAKOUT_RVOL"], "primary_source_tag": "TV_BREAKOUT_RVOL"},
    ]

    def _forecast_fn(
        series_map: dict[str, np.ndarray],
        prediction_length: int,
        quantile_levels: list[float],
    ) -> dict[str, dict[float, np.ndarray]]:
        assert prediction_length == 20
        assert quantile_levels == [0.1, 0.5, 0.9]
        return {
            "AAA": {
                0.1: np.linspace(145.0, 150.0, 20),
                0.5: np.linspace(150.0, 165.0, 20),
                0.9: np.linspace(155.0, 180.0, 20),
            },
            "BBB": {
                0.1: np.linspace(118.0, 117.0, 20),
                0.5: np.linspace(120.0, 124.0, 20),
                0.9: np.linspace(122.0, 129.0, 20),
            },
            "CCC": {
                0.1: np.linspace(88.0, 70.0, 20),
                0.5: np.linspace(90.0, 95.0, 20),
                0.9: np.linspace(92.0, 103.0, 20),
            },
        }

    rows = generate_chronos_rerank_rows(
        merged_candidate_rows=merged_pool,
        market="us",
        load_ohlcv_frame_fn=lambda symbol, market, **_: frames.get(symbol, pd.DataFrame()),
        forecast_fn=_forecast_fn,
    )

    assert [row["symbol"] for row in rows] == ["AAA", "BBB", "CCC", "DDD"]
    by_symbol = {row["symbol"]: row for row in rows}

    assert by_symbol["AAA"]["fm_status"] == "OK"
    assert by_symbol["BBB"]["fm_status"] == "OK"
    assert by_symbol["CCC"]["fm_status"] == "UNSCORABLE"
    assert by_symbol["DDD"]["fm_status"] == "INSUFFICIENT_HISTORY"
    assert by_symbol["AAA"]["support_anchor_type"] == "EMA20_LOW20_MAX"
    assert by_symbol["AAA"]["fm_rerank_score"] > by_symbol["BBB"]["fm_rerank_score"]
    assert by_symbol["AAA"]["fm_rank"] == 1
    assert by_symbol["BBB"]["fm_rank"] == 2
