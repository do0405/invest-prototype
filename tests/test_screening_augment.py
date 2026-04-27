from __future__ import annotations

import json
import shutil
import warnings

import numpy as np
import pandas as pd
import pytest

import screeners.augment.pipeline as augment_pipeline
from screeners.augment.chronos_rerank import generate_chronos_rerank_rows
from screeners.augment.lag_diagnostics import generate_global_lag_diagnostic_rows
from screeners.augment.pipeline import (
    build_buy_eligible_source_specs,
    build_merged_candidate_pool_rows,
    run_screening_augment,
)
from screeners.augment.stumpy_sidecar import generate_stumpy_summary_rows
from screeners.augment.timesfm_rerank import generate_timesfm_rerank_rows
from screeners.source_contracts import CANONICAL_SOURCE_SPECS
from tests._paths import runtime_root


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


def _dense_quantile_levels() -> list[float]:
    return [0.05, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 0.95]


def _dense_forecast_payload() -> dict[str, dict[float, np.ndarray]]:
    return {
        "AAA": {
            0.05: np.linspace(144.0, 147.0, 20),
            0.10: np.linspace(145.0, 149.0, 20),
            0.20: np.linspace(146.0, 151.0, 20),
            0.30: np.linspace(147.0, 154.0, 20),
            0.40: np.linspace(148.0, 158.0, 20),
            0.50: np.linspace(150.0, 165.0, 20),
            0.60: np.linspace(151.0, 168.0, 20),
            0.70: np.linspace(152.0, 171.0, 20),
            0.80: np.linspace(154.0, 175.0, 20),
            0.90: np.linspace(156.0, 180.0, 20),
            0.95: np.linspace(158.0, 184.0, 20),
        },
        "BBB": {
            0.05: np.linspace(116.0, 111.0, 20),
            0.10: np.linspace(117.0, 113.0, 20),
            0.20: np.linspace(118.0, 115.0, 20),
            0.30: np.linspace(119.0, 117.0, 20),
            0.40: np.linspace(120.0, 121.0, 20),
            0.50: np.linspace(120.0, 124.0, 20),
            0.60: np.linspace(120.5, 125.0, 20),
            0.70: np.linspace(121.0, 125.8, 20),
            0.80: np.linspace(122.0, 126.5, 20),
            0.90: np.linspace(123.0, 127.2, 20),
            0.95: np.linspace(124.0, 127.8, 20),
        },
        "CCC": {
            0.05: np.linspace(82.0, 64.0, 20),
            0.10: np.linspace(86.0, 69.0, 20),
            0.20: np.linspace(88.0, 72.0, 20),
            0.30: np.linspace(89.0, 78.0, 20),
            0.40: np.linspace(89.5, 86.0, 20),
            0.50: np.linspace(90.0, 95.0, 20),
            0.60: np.linspace(91.0, 98.0, 20),
            0.70: np.linspace(92.0, 101.0, 20),
            0.80: np.linspace(93.0, 105.0, 20),
            0.90: np.linspace(94.0, 108.0, 20),
            0.95: np.linspace(95.0, 111.0, 20),
        },
    }


def test_build_buy_eligible_source_specs_matches_canonical_filter() -> None:
    expected = [spec for spec in CANONICAL_SOURCE_SPECS if spec.buy_eligible]

    assert build_buy_eligible_source_specs() == expected


def test_pipeline_removes_global_optional_dependency_gate() -> None:
    assert not hasattr(augment_pipeline, "_validate_optional_dependencies")


def test_build_merged_candidate_pool_rows_preserves_source_registry_contract() -> None:
    registry = {
        "AAA": {
            "symbol": "AAA",
            "market": "US",
            "source_disposition": "buy_eligible",
            "source_buy_eligible": True,
            "screen_stage": "PATTERN_INCLUDED",
            "source_tags": ["QMG_PATTERN_INCLUDED", "TV_BREAKOUT_RVOL"],
            "primary_source_tag": "QMG_PATTERN_INCLUDED",
            "primary_source_stage": "PATTERN_INCLUDED",
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
            "source_disposition": "buy_eligible",
            "source_buy_eligible": True,
            "buy_eligible": True,
            "watch_only": False,
            "screen_stage": "PATTERN_INCLUDED",
            "source_tags": ["QMG_PATTERN_INCLUDED", "TV_BREAKOUT_RVOL"],
            "primary_source_tag": "QMG_PATTERN_INCLUDED",
            "primary_source_stage": "PATTERN_INCLUDED",
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
            "source_disposition": "buy_eligible",
            "source_buy_eligible": True,
            "screen_stage": "PATTERN_INCLUDED",
            "source_tags": ["QMG_PATTERN_INCLUDED"],
            "primary_source_tag": "QMG_PATTERN_INCLUDED",
            "primary_source_stage": "PATTERN_INCLUDED",
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
    assert rows[0]["source_disposition"] == "buy_eligible"


def test_run_screening_augment_reuses_source_registry_snapshot(
    monkeypatch,
) -> None:
    writes: list[tuple[str, int]] = []
    root = runtime_root("_test_runtime_augment_snapshot")
    root.mkdir(parents=True, exist_ok=True)

    snapshot = {
        "schema_version": 2,
        "market": "US",
        "as_of_date": "2026-04-18",
        "registry": {
            "AAA": {
                "symbol": "AAA",
                "market": "US",
                "source_buy_eligible": True,
                "screen_stage": "PATTERN_INCLUDED",
                "source_tags": ["QMG_PATTERN_INCLUDED"],
                "primary_source_tag": "QMG_PATTERN_INCLUDED",
                "primary_source_stage": "PATTERN_INCLUDED",
                "primary_source_style": "TREND",
                "source_style_tags": ["TREND"],
                "source_priority_score": 10.0,
                "trend_source_bonus": 4.0,
                "ug_source_bonus": 2.0,
                "source_overlap_bonus": 0.0,
                "sector": "Tech",
                "industry": "Software",
                "group_name": "Software",
                "as_of_ts": "2026-04-18",
            }
        },
        "source_rows": [
            {
                "symbol": "AAA",
                "market": "US",
                "source_tag": "QMG_PATTERN_INCLUDED",
                "screen_stage": "PATTERN_INCLUDED",
                "relative_path": "qullamaggie/pattern_included_candidates.csv",
                "as_of_ts": "2026-04-18",
                "source_buy_eligible": True,
            }
        ],
    }

    monkeypatch.setattr(
        "screeners.augment.pipeline.ensure_market_dirs",
        lambda market: None,
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.get_market_screeners_root",
        lambda market: str(root / "screeners"),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.get_augment_results_dir",
        lambda market: str(root / "augment"),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.signal_source_registry.load_source_registry",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("snapshot-backed augment should not rebuild source registry from csv")
        ),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline._load_raw_source_rows",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("snapshot-backed augment should not rescan raw source rows from csv")
        ),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.generate_stumpy_summary_rows",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.generate_chronos_rerank_rows",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.generate_timesfm_rerank_rows",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline._write_records",
        lambda output_dir, filename_prefix, rows: writes.append((filename_prefix, len(rows))),
    )

    result = run_screening_augment(
        market="us",
        source_registry_snapshot=snapshot,
    )

    assert result["ok"] is True
    assert result["merged_candidates"] == 1
    assert ("merged_candidate_pool", 1) in writes


def test_run_screening_augment_fails_fast_on_isolated_root_without_screening_artifacts(
    monkeypatch,
) -> None:
    root = runtime_root("_test_runtime_augment_missing_prereq")
    screeners_root = root / "screeners"
    augment_root = root / "augment"
    shutil.rmtree(root, ignore_errors=True)
    screeners_root.mkdir(parents=True, exist_ok=True)
    augment_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("INVEST_PROTO_RESULTS_DIR", str(root))
    monkeypatch.setattr("screeners.augment.pipeline.ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(
        "screeners.augment.pipeline.get_market_screeners_root",
        lambda market: str(screeners_root),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.get_augment_results_dir",
        lambda market: str(augment_root),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.get_market_source_registry_snapshot_path",
        lambda market: str(screeners_root / "source_registry_snapshot.json"),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.signal_source_registry.read_source_registry_snapshot",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.signal_source_registry.load_source_registry",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("isolated augment run should fail before csv scan fallback")
        ),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline._load_raw_source_rows",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("isolated augment run should fail before raw source scan")
        ),
    )

    with pytest.raises(ValueError, match="prerequisite screening artifacts"):
        run_screening_augment(market="us")


def test_run_screening_augment_prefers_runtime_context_as_of_date(
    monkeypatch,
) -> None:
    root = runtime_root("_test_runtime_augment_runtime_context_asof")
    root.mkdir(parents=True, exist_ok=True)
    captured: dict[str, object] = {}
    runtime_context = augment_pipeline.RuntimeContext(
        market="us",
        as_of_date="2026-04-18",
    )
    snapshot = {
        "schema_version": 2,
        "market": "US",
        "as_of_date": "2026-04-18",
        "registry": {},
        "source_rows": [],
    }

    monkeypatch.setattr(
        "screeners.augment.pipeline.ensure_market_dirs",
        lambda market: None,
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.get_market_screeners_root",
        lambda market: str(root / "screeners"),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.get_augment_results_dir",
        lambda market: str(root / "augment"),
    )
    def _read_snapshot(path, *, market, as_of_date=None):  # noqa: ANN001
        captured["as_of_date"] = as_of_date
        return snapshot
    monkeypatch.setattr(
        "screeners.augment.pipeline.signal_source_registry.read_source_registry_snapshot",
        _read_snapshot,
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.generate_stumpy_summary_rows",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.generate_chronos_rerank_rows",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.generate_timesfm_rerank_rows",
        lambda **kwargs: [],
    )

    result = run_screening_augment(market="us", runtime_context=runtime_context)

    assert captured["as_of_date"] == "2026-04-18"
    assert result["ok"] is True


def test_run_screening_augment_passes_as_of_date_to_generators(
    monkeypatch,
) -> None:
    root = runtime_root("_test_runtime_augment_generator_asof")
    root.mkdir(parents=True, exist_ok=True)
    runtime_context = augment_pipeline.RuntimeContext(
        market="us",
        as_of_date="2026-04-18",
    )
    snapshot = {
        "schema_version": 3,
        "market": "US",
        "as_of_date": "2026-04-18",
        "registry": {
            "AAA": {
                "symbol": "AAA",
                "market": "US",
                "source_disposition": "buy_eligible",
                "source_buy_eligible": True,
                "buy_eligible": True,
                "screen_stage": "PATTERN_INCLUDED",
                "source_tags": ["QMG_PATTERN_INCLUDED"],
                "primary_source_tag": "QMG_PATTERN_INCLUDED",
                "primary_source_stage": "PATTERN_INCLUDED",
                "primary_source_style": "TREND",
                "source_style_tags": ["TREND"],
                "source_priority_score": 9.0,
            }
        },
        "source_rows": [
            {
                "symbol": "AAA",
                "market": "US",
                "source_tag": "QMG_PATTERN_INCLUDED",
                "screen_stage": "PATTERN_INCLUDED",
                "relative_path": "qullamaggie/pattern_included_candidates.csv",
                "source_buy_eligible": True,
                "buy_eligible": True,
                "watch_only": False,
                "source_disposition": "buy_eligible",
                "as_of_ts": "2026-04-18",
            }
        ],
    }
    captured: dict[str, str] = {}

    monkeypatch.setattr(augment_pipeline, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(
        augment_pipeline,
        "get_market_screeners_root",
        lambda market: str(root / "screeners"),
    )
    monkeypatch.setattr(
        augment_pipeline,
        "get_augment_results_dir",
        lambda market: str(root / "augment"),
    )
    monkeypatch.setattr(
        augment_pipeline.signal_source_registry,
        "read_source_registry_snapshot",
        lambda *args, **kwargs: snapshot,
    )
    monkeypatch.setitem(
        run_screening_augment.__globals__,
        "generate_stumpy_summary_rows",
        lambda **kwargs: captured.__setitem__("stumpy", kwargs["as_of_date"]) or [],
    )
    monkeypatch.setitem(
        run_screening_augment.__globals__,
        "generate_global_lag_diagnostic_rows",
        lambda **kwargs: captured.__setitem__("lag", kwargs["as_of_date"]) or [],
    )
    monkeypatch.setitem(
        run_screening_augment.__globals__,
        "generate_chronos_rerank_rows",
        lambda **kwargs: captured.__setitem__("chronos", kwargs["as_of_date"]) or [],
    )
    monkeypatch.setitem(
        run_screening_augment.__globals__,
        "generate_timesfm_rerank_rows",
        lambda **kwargs: captured.__setitem__("timesfm", kwargs["as_of_date"]) or [],
    )

    result = run_screening_augment(market="us", runtime_context=runtime_context)

    assert result["ok"] is True
    assert captured == {
        "stumpy": "2026-04-18",
        "lag": "2026-04-18",
        "chronos": "2026-04-18",
        "timesfm": "2026-04-18",
    }


def test_run_screening_augment_writes_shared_summary_contract(
    monkeypatch,
) -> None:
    root = runtime_root("_test_runtime_augment_summary_contract")
    screeners_root = root / "screeners"
    augment_root = root / "augment"
    screeners_root.mkdir(parents=True, exist_ok=True)
    augment_root.mkdir(parents=True, exist_ok=True)

    snapshot = {
        "schema_version": 2,
        "market": "US",
        "as_of_date": "2026-04-18",
        "registry": {
            "AAA": {
                "symbol": "AAA",
                "market": "US",
                "source_buy_eligible": True,
                "screen_stage": "PATTERN_INCLUDED",
                "source_tags": ["QMG_PATTERN_INCLUDED"],
                "primary_source_tag": "QMG_PATTERN_INCLUDED",
                "primary_source_stage": "PATTERN_INCLUDED",
                "primary_source_style": "TREND",
                "source_style_tags": ["TREND"],
                "source_priority_score": 10.0,
                "trend_source_bonus": 4.0,
                "ug_source_bonus": 2.0,
                "source_overlap_bonus": 0.0,
                "sector": "Tech",
                "industry": "Software",
                "group_name": "Software",
                "as_of_ts": "2026-04-18",
            }
        },
        "source_rows": [
            {
                "symbol": "AAA",
                "market": "US",
                "source_tag": "QMG_PATTERN_INCLUDED",
                "screen_stage": "PATTERN_INCLUDED",
                "relative_path": "qullamaggie/pattern_included_candidates.csv",
                "as_of_ts": "2026-04-18",
                "source_buy_eligible": True,
            }
        ],
    }

    monkeypatch.setattr("screeners.augment.pipeline.ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(
        "screeners.augment.pipeline.get_market_screeners_root",
        lambda market: str(screeners_root),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.get_augment_results_dir",
        lambda market: str(augment_root),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.generate_stumpy_summary_rows",
        lambda **kwargs: [
            {
                "symbol": "AAA",
                "market": "US",
                "source_tag": "QMG_PATTERN_INCLUDED",
                "window_size": 40,
                "stumpy_cluster_id": "QMG_PATTERN_INCLUDED_W40_C1",
                "stumpy_cluster_size": 1,
                "stumpy_exemplar_symbol": "AAA",
                "stumpy_price_motif_score": 77.0,
                "stumpy_self_discord_score": 50.0,
                "stumpy_volume_overlay_score": 40.0,
                "stumpy_shape_label": "UP",
                "stumpy_status": "SINGLETON",
            }
        ],
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.generate_chronos_rerank_rows",
        lambda **kwargs: [
            {
                "symbol": "AAA",
                "market": "US",
                "source_tags": ["QMG_PATTERN_INCLUDED"],
                "primary_source_tag": "QMG_PATTERN_INCLUDED",
                "support_anchor_price": 100.0,
                "support_anchor_type": "EMA20_LOW20_MAX",
                "fm_upside_5d_pct": 2.0,
                "fm_upside_10d_pct": 3.0,
                "fm_upside_20d_pct": 4.0,
                "fm_breach_margin_5d_pct": -1.0,
                "fm_breach_margin_10d_pct": -1.5,
                "fm_breach_margin_20d_pct": -2.0,
                "fm_dispersion_5d_pct": 1.0,
                "fm_dispersion_10d_pct": 1.2,
                "fm_dispersion_20d_pct": 1.4,
                "fm_rerank_score": 72.5,
                "fm_rank": 1,
                "fm_status": "OK",
                "up_close_prob_proxy_5d": 0.6,
                "up_close_prob_proxy_10d": 0.7,
                "up_close_prob_proxy_20d": 0.8,
                "down_close_prob_proxy_5d": 0.4,
                "down_close_prob_proxy_10d": 0.3,
                "down_close_prob_proxy_20d": 0.2,
                "support_breach_risk_proxy_5d": 0.15,
                "support_breach_risk_proxy_10d": 0.10,
                "support_breach_risk_proxy_20d": 0.08,
                "follow_through_quality_5d": 62.0,
                "follow_through_quality_10d": 70.0,
                "follow_through_quality_20d": 77.0,
                "fragility_score_5d": 20.0,
                "fragility_score_10d": 18.0,
                "fragility_score_20d": 17.0,
                "fm_model_id": "chronos2",
                "fm_model_family": "TSFM",
            }
        ],
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.generate_timesfm_rerank_rows",
        lambda **kwargs: [
            {
                "symbol": "AAA",
                "market": "US",
                "source_tags": ["QMG_PATTERN_INCLUDED"],
                "primary_source_tag": "QMG_PATTERN_INCLUDED",
                "support_anchor_price": 100.0,
                "support_anchor_type": "EMA20_LOW20_MAX",
                "fm_upside_5d_pct": 1.8,
                "fm_upside_10d_pct": 2.8,
                "fm_upside_20d_pct": 3.8,
                "fm_breach_margin_5d_pct": -0.9,
                "fm_breach_margin_10d_pct": -1.4,
                "fm_breach_margin_20d_pct": -1.8,
                "fm_dispersion_5d_pct": 1.1,
                "fm_dispersion_10d_pct": 1.3,
                "fm_dispersion_20d_pct": 1.5,
                "fm_rerank_score": 71.0,
                "fm_rank": 1,
                "fm_status": "OK",
                "up_close_prob_proxy_5d": 0.58,
                "up_close_prob_proxy_10d": 0.68,
                "up_close_prob_proxy_20d": 0.79,
                "down_close_prob_proxy_5d": 0.42,
                "down_close_prob_proxy_10d": 0.32,
                "down_close_prob_proxy_20d": 0.21,
                "support_breach_risk_proxy_5d": 0.14,
                "support_breach_risk_proxy_10d": 0.09,
                "support_breach_risk_proxy_20d": 0.07,
                "follow_through_quality_5d": 61.0,
                "follow_through_quality_10d": 69.0,
                "follow_through_quality_20d": 76.0,
                "fragility_score_5d": 22.0,
                "fragility_score_10d": 19.0,
                "fragility_score_20d": 18.0,
                "fm_model_id": "timesfm2p5",
                "fm_model_family": "TSFM",
            }
        ],
    )

    result = run_screening_augment(market="us", source_registry_snapshot=snapshot)

    expected_keys = {
        "status",
        "status_counts",
        "timings",
        "cache_stats",
        "rows_read",
        "rows_written",
        "module_summaries",
        "summary_path",
    }
    assert expected_keys <= set(result.keys())
    summary_path = augment_root / "augment_run_summary.json"
    assert result["summary_path"] == str(summary_path)
    assert summary_path.exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["market"] == "US"
    assert summary["status"] == "ok"
    assert summary["diagnostic_only"] is True
    assert summary["source_registry_provenance"]["snapshot_used"] is True
    assert summary["module_summaries"]["stumpy"]["status"] == "OK"
    assert summary["module_summaries"]["stumpy"]["input_count"] == 1
    assert summary["module_summaries"]["lag_diagnostics"]["input_count"] == 1
    assert summary["module_summaries"]["chronos2"]["status"] == "OK"
    assert summary["module_summaries"]["chronos2"]["input_count"] == 1
    assert summary["module_summaries"]["timesfm2p5"]["status"] == "OK"
    assert summary["module_summaries"]["timesfm2p5"]["input_count"] == 1
    assert result["runtime_metrics"]["augment"]["chronos2"]["input_count"] == 1
    assert result["timesfm_rows"] == 1


def test_run_screening_augment_soft_skips_stumpy_and_writes_runtime_skip_rows(
    monkeypatch,
) -> None:
    root = runtime_root("_test_runtime_augment_stumpy_skip")
    screeners_root = root / "screeners"
    augment_root = root / "augment"
    source_dir = screeners_root / "qullamaggie"
    source_dir.mkdir(parents=True, exist_ok=True)
    augment_root.mkdir(parents=True, exist_ok=True)

    snapshot = {
        "schema_version": 2,
        "market": "US",
        "as_of_date": "2026-04-18",
        "registry": {
            "AAA": {
                "symbol": "AAA",
                "market": "US",
                "source_buy_eligible": True,
                "screen_stage": "PATTERN_INCLUDED",
                "source_tags": ["QMG_PATTERN_INCLUDED"],
                "primary_source_tag": "QMG_PATTERN_INCLUDED",
                "primary_source_stage": "PATTERN_INCLUDED",
                "primary_source_style": "TREND",
                "source_style_tags": ["TREND"],
                "source_priority_score": 10.0,
                "trend_source_bonus": 4.0,
                "ug_source_bonus": 2.0,
                "source_overlap_bonus": 0.0,
                "sector": "Tech",
                "industry": "Software",
                "group_name": "Software",
                "as_of_ts": "2026-04-18",
            }
        },
        "source_rows": [
            {
                "symbol": "AAA",
                "market": "US",
                "source_tag": "QMG_PATTERN_INCLUDED",
                "screen_stage": "PATTERN_INCLUDED",
                "relative_path": "qullamaggie/pattern_included_candidates.csv",
                "as_of_ts": "2026-04-18",
                "source_buy_eligible": True,
            }
        ],
    }

    monkeypatch.setattr("screeners.augment.pipeline.ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(
        "screeners.augment.pipeline.get_market_screeners_root",
        lambda market: str(screeners_root),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.get_augment_results_dir",
        lambda market: str(augment_root),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.generate_stumpy_summary_rows",
        lambda **kwargs: (_ for _ in ()).throw(ImportError("No module named 'stumpy'")),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.generate_chronos_rerank_rows",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.generate_timesfm_rerank_rows",
        lambda **kwargs: [],
    )

    result = run_screening_augment(market="us", source_registry_snapshot=snapshot)

    stumpy_path = source_dir / "pattern_included_candidates_stumpy_summary.csv"
    assert stumpy_path.exists()
    stumpy_frame = pd.read_csv(stumpy_path)
    assert set(stumpy_frame["stumpy_status"]) == {"RUNTIME_SKIP"}
    assert result["status"] == "partial"
    assert result["module_summaries"]["stumpy"]["status"] == "SKIPPED_MISSING_DEP"


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
        source_tag="QMG_PATTERN_INCLUDED",
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
        source_tag="QMG_PATTERN_INCLUDED",
        market="us",
        load_ohlcv_frame_fn=lambda symbol, market, **_: frames.get(symbol, pd.DataFrame()),
        distance_fn=_distance_fn,
        self_profile_fn=_self_profile_fn,
    )
    grouped = {(row["symbol"], row["window_size"]): row for row in rows}

    assert grouped[("MISSING", 40)]["stumpy_status"] == "MISSING_OHLCV"
    assert grouped[("BBB", 80)]["stumpy_status"] == "INSUFFICIENT_HISTORY"
    assert grouped[("BBB", 120)]["stumpy_status"] == "INSUFFICIENT_HISTORY"


def test_generate_stumpy_summary_rows_uses_requested_as_of_date() -> None:
    observed_as_of: list[str | None] = []
    frame = _make_frame("AAA", np.linspace(100.0, 150.0, 180))

    rows = generate_stumpy_summary_rows(
        source_rows=[{"symbol": "AAA", "market": "us"}],
        source_tag="QMG_PATTERN_INCLUDED",
        market="us",
        as_of_date="2026-04-18",
        load_ohlcv_frame_fn=lambda symbol, market, as_of=None, **_: observed_as_of.append(as_of) or frame,  # noqa: E501
        distance_fn=_distance_fn,
        self_profile_fn=_self_profile_fn,
    )

    assert rows
    assert observed_as_of == ["2026-04-18"]


def test_generate_global_lag_diagnostic_rows_writes_signed_lag_output() -> None:
    base = np.linspace(100.0, 180.0, 180) + (np.sin(np.linspace(0.0, 12.0, 180)) * 4.0)
    shifted = np.concatenate([np.repeat(base[0], 3), base[:-3]])
    frames = {
        "AAA": _make_frame("AAA", base),
        "BBB": _make_frame("BBB", shifted),
        "CCC": _make_frame(
            "CCC",
            np.linspace(80.0, 95.0, 180) + (np.sin(np.linspace(0.0, 4.0, 180)) * 1.0),
        ),
    }
    merged_pool = [
        {
            "symbol": "AAA",
            "market": "US",
            "source_tags": ["QMG_PATTERN_INCLUDED"],
            "primary_source_style": "TREND",
        },
        {
            "symbol": "BBB",
            "market": "US",
            "source_tags": ["WS_PRIMARY"],
            "primary_source_style": "STRUCTURE",
        },
        {
            "symbol": "CCC",
            "market": "US",
            "source_tags": ["LL_LEADER"],
            "primary_source_style": "LEADERSHIP",
        },
    ]

    rows = generate_global_lag_diagnostic_rows(
        merged_candidate_rows=merged_pool,
        market="us",
        load_ohlcv_frame_fn=lambda symbol, market, **_: frames.get(symbol, pd.DataFrame()),
        top_k_per_anchor=2,
        window_sizes=(40,),
        max_lag_days=5,
    )

    assert rows


def test_generate_global_lag_diagnostic_rows_uses_requested_as_of_date() -> None:
    observed_as_of: list[str | None] = []
    frames = {
        "AAA": _make_frame("AAA", np.linspace(100.0, 140.0, 180)),
        "BBB": _make_frame("BBB", np.linspace(102.0, 142.0, 180)),
    }

    rows = generate_global_lag_diagnostic_rows(
        merged_candidate_rows=[
            {"symbol": "AAA", "market": "US", "source_tags": ["QMG_PATTERN_INCLUDED"]},
            {"symbol": "BBB", "market": "US", "source_tags": ["WS_PRIMARY"]},
        ],
        market="us",
        as_of_date="2026-04-18",
        load_ohlcv_frame_fn=lambda symbol, market, as_of=None, **_: observed_as_of.append(as_of) or frames[symbol],  # noqa: E501
        top_k_per_anchor=1,
        window_sizes=(40,),
        max_lag_days=2,
    )

    assert rows
    assert observed_as_of == ["2026-04-18", "2026-04-18"]
    anchor_rows = [row for row in rows if row["anchor_symbol"] == "AAA"]
    assert anchor_rows[0]["peer_symbol"] == "BBB"
    assert float(anchor_rows[0]["lag_correlation_score"]) > 0
    assert anchor_rows[0]["pair_rank_for_anchor"] == 1


def test_generate_global_lag_diagnostic_rows_handles_flat_paths_without_runtime_warning() -> None:
    frames = {
        "AAA": _make_frame("AAA", np.full(180, 100.0)),
        "BBB": _make_frame("BBB", np.full(180, 100.0)),
    }

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        rows = generate_global_lag_diagnostic_rows(
            merged_candidate_rows=[
                {"symbol": "AAA", "market": "US", "source_tags": ["QMG_PATTERN_INCLUDED"]},
                {"symbol": "BBB", "market": "US", "source_tags": ["WS_PRIMARY"]},
            ],
            market="us",
            load_ohlcv_frame_fn=lambda symbol, market, **_: frames[symbol],
            top_k_per_anchor=1,
            window_sizes=(40,),
            max_lag_days=2,
        )

    assert rows
    assert rows[0]["shape_similarity_score"] == 50.0
    assert rows[0]["lag_correlation_score"] == 0.0


def test_generate_chronos_rerank_rows_scores_all_candidates_and_ranks_them() -> None:
    base = np.linspace(100.0, 150.0, 260)
    frames = {
        "AAA": _make_frame("AAA", base),
        "BBB": _make_frame("BBB", np.linspace(100.0, 120.0, 260)),
        "CCC": _make_frame("CCC", np.concatenate([np.linspace(100.0, 130.0, 240), np.linspace(140.0, 90.0, 20)])),
        "DDD": _make_frame("DDD", np.linspace(100.0, 110.0, 120)),
    }
    merged_pool = [
        {"symbol": "AAA", "market": "US", "source_tags": ["QMG_PATTERN_INCLUDED"], "primary_source_tag": "QMG_PATTERN_INCLUDED"},
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
        assert quantile_levels == _dense_quantile_levels()
        return _dense_forecast_payload()

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
    assert by_symbol["AAA"]["fm_model_id"] == "chronos2"
    assert by_symbol["AAA"]["fm_model_family"] == "TSFM"
    assert by_symbol["AAA"]["up_close_prob_proxy_10d"] > by_symbol["BBB"]["up_close_prob_proxy_10d"]
    assert by_symbol["AAA"]["support_breach_risk_proxy_10d"] < by_symbol["BBB"]["support_breach_risk_proxy_10d"]
    assert by_symbol["AAA"]["follow_through_quality_10d"] > by_symbol["BBB"]["follow_through_quality_10d"]
    assert 0.0 <= by_symbol["AAA"]["fragility_score_10d"] <= 100.0
    assert 0.0 <= by_symbol["BBB"]["fragility_score_10d"] <= 100.0
    assert by_symbol["AAA"]["fm_rerank_score"] > by_symbol["BBB"]["fm_rerank_score"]
    assert by_symbol["AAA"]["fm_rank"] == 1
    assert by_symbol["BBB"]["fm_rank"] == 2
    assert "up_close_probability_10d" not in by_symbol["AAA"]
    assert "down_close_probability_10d" not in by_symbol["AAA"]


def test_generate_timesfm_rerank_rows_matches_chronos_schema_and_ranks_them() -> None:
    base = np.linspace(100.0, 150.0, 260)
    frames = {
        "AAA": _make_frame("AAA", base),
        "BBB": _make_frame("BBB", np.linspace(100.0, 120.0, 260)),
        "CCC": _make_frame("CCC", np.concatenate([np.linspace(100.0, 130.0, 240), np.linspace(140.0, 90.0, 20)])),
        "DDD": _make_frame("DDD", np.linspace(100.0, 110.0, 120)),
    }
    merged_pool = [
        {"symbol": "AAA", "market": "US", "source_tags": ["QMG_PATTERN_INCLUDED"], "primary_source_tag": "QMG_PATTERN_INCLUDED"},
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
        assert quantile_levels == _dense_quantile_levels()
        return _dense_forecast_payload()

    rows = generate_timesfm_rerank_rows(
        merged_candidate_rows=merged_pool,
        market="us",
        load_ohlcv_frame_fn=lambda symbol, market, **_: frames.get(symbol, pd.DataFrame()),
        forecast_fn=_forecast_fn,
    )

    assert [row["symbol"] for row in rows] == ["AAA", "BBB", "CCC", "DDD"]
    by_symbol = {row["symbol"]: row for row in rows}
    assert by_symbol["AAA"]["fm_model_id"] == "timesfm2p5"
    assert by_symbol["AAA"]["fm_model_family"] == "TSFM"
    assert by_symbol["AAA"]["fm_status"] == "OK"
    assert by_symbol["DDD"]["fm_status"] == "INSUFFICIENT_HISTORY"
    assert by_symbol["AAA"]["up_close_prob_proxy_20d"] > by_symbol["BBB"]["up_close_prob_proxy_20d"]
    assert by_symbol["AAA"]["fm_rerank_score"] > by_symbol["BBB"]["fm_rerank_score"]
    assert by_symbol["AAA"]["fm_rank"] == 1
    assert by_symbol["BBB"]["fm_rank"] == 2
    assert set(by_symbol["AAA"]) >= {
        "up_close_prob_proxy_5d",
        "down_close_prob_proxy_5d",
        "support_breach_risk_proxy_5d",
        "follow_through_quality_5d",
        "fragility_score_5d",
    }


def test_generate_timesfm_rerank_rows_uses_requested_as_of_date() -> None:
    observed_as_of: list[str | None] = []
    frames = {
        "AAA": _make_frame("AAA", np.linspace(100.0, 150.0, 260)),
    }

    rows = generate_timesfm_rerank_rows(
        merged_candidate_rows=[
            {
                "symbol": "AAA",
                "market": "US",
                "source_tags": ["QMG_PATTERN_INCLUDED"],
                "primary_source_tag": "QMG_PATTERN_INCLUDED",
            }
        ],
        market="us",
        as_of_date="2026-04-18",
        load_ohlcv_frame_fn=lambda symbol, market, as_of=None, **_: observed_as_of.append(as_of) or frames[symbol],  # noqa: E501
        forecast_fn=lambda series_map, prediction_length, quantile_levels: {
            "AAA": _dense_forecast_payload()["AAA"],
        },
    )

    assert rows[0]["fm_status"] == "OK"
    assert observed_as_of == ["2026-04-18"]


def test_run_screening_augment_classifies_chronos_soft_skip_states(
    monkeypatch,
) -> None:
    root = runtime_root("_test_runtime_augment_chronos_status")
    screeners_root = root / "screeners"
    augment_root = root / "augment"
    screeners_root.mkdir(parents=True, exist_ok=True)
    augment_root.mkdir(parents=True, exist_ok=True)

    snapshot = {
        "schema_version": 2,
        "market": "US",
        "as_of_date": "2026-04-18",
        "registry": {
            "AAA": {
                "symbol": "AAA",
                "market": "US",
                "source_buy_eligible": True,
                "screen_stage": "PATTERN_INCLUDED",
                "source_tags": ["QMG_PATTERN_INCLUDED"],
                "primary_source_tag": "QMG_PATTERN_INCLUDED",
                "primary_source_stage": "PATTERN_INCLUDED",
                "primary_source_style": "TREND",
                "source_style_tags": ["TREND"],
                "source_priority_score": 10.0,
                "trend_source_bonus": 4.0,
                "ug_source_bonus": 2.0,
                "source_overlap_bonus": 0.0,
                "sector": "Tech",
                "industry": "Software",
                "group_name": "Software",
                "as_of_ts": "2026-04-18",
            }
        },
        "source_rows": [
            {
                "symbol": "AAA",
                "market": "US",
                "source_tag": "QMG_PATTERN_INCLUDED",
                "screen_stage": "PATTERN_INCLUDED",
                "relative_path": "qullamaggie/pattern_included_candidates.csv",
                "as_of_ts": "2026-04-18",
                "source_buy_eligible": True,
            }
        ],
    }

    monkeypatch.setattr("screeners.augment.pipeline.ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(
        "screeners.augment.pipeline.get_market_screeners_root",
        lambda market: str(screeners_root),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.get_augment_results_dir",
        lambda market: str(augment_root),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.generate_stumpy_summary_rows",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.generate_timesfm_rerank_rows",
        lambda **kwargs: [],
    )

    failure_cases = [
        (ImportError("No module named 'chronos'"), "SKIPPED_MISSING_DEP"),
        (RuntimeError("MISSING_MODEL: chronos weights unavailable"), "SKIPPED_MISSING_MODEL"),
        (RuntimeError("forecast exploded"), "FAILED_RUNTIME"),
    ]
    for exc, expected_status in failure_cases:
        monkeypatch.setattr(
            "screeners.augment.pipeline.generate_chronos_rerank_rows",
            lambda **kwargs: (_ for _ in ()).throw(exc),
        )

        result = run_screening_augment(market="us", source_registry_snapshot=snapshot)

        assert result["status"] == "partial"
        assert result["module_summaries"]["chronos2"]["status"] == expected_status
        chronos_path = augment_root / "chronos2_rerank.csv"
        assert chronos_path.exists()
        chronos_frame = pd.read_csv(chronos_path)
        assert list(chronos_frame["symbol"]) == ["AAA"]
        assert "up_close_prob_proxy_5d" in chronos_frame.columns


def test_run_screening_augment_classifies_timesfm_soft_skip_states(
    monkeypatch,
) -> None:
    root = runtime_root("_test_runtime_augment_timesfm_status")
    screeners_root = root / "screeners"
    augment_root = root / "augment"
    screeners_root.mkdir(parents=True, exist_ok=True)
    augment_root.mkdir(parents=True, exist_ok=True)

    snapshot = {
        "schema_version": 2,
        "market": "US",
        "as_of_date": "2026-04-18",
        "registry": {
            "AAA": {
                "symbol": "AAA",
                "market": "US",
                "source_buy_eligible": True,
                "screen_stage": "PATTERN_INCLUDED",
                "source_tags": ["QMG_PATTERN_INCLUDED"],
                "primary_source_tag": "QMG_PATTERN_INCLUDED",
                "primary_source_stage": "PATTERN_INCLUDED",
                "primary_source_style": "TREND",
                "source_style_tags": ["TREND"],
                "source_priority_score": 10.0,
                "trend_source_bonus": 4.0,
                "ug_source_bonus": 2.0,
                "source_overlap_bonus": 0.0,
                "sector": "Tech",
                "industry": "Software",
                "group_name": "Software",
                "as_of_ts": "2026-04-18",
            }
        },
        "source_rows": [
            {
                "symbol": "AAA",
                "market": "US",
                "source_tag": "QMG_PATTERN_INCLUDED",
                "screen_stage": "PATTERN_INCLUDED",
                "relative_path": "qullamaggie/pattern_included_candidates.csv",
                "as_of_ts": "2026-04-18",
                "source_buy_eligible": True,
            }
        ],
    }

    monkeypatch.setattr("screeners.augment.pipeline.ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(
        "screeners.augment.pipeline.get_market_screeners_root",
        lambda market: str(screeners_root),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.get_augment_results_dir",
        lambda market: str(augment_root),
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.generate_stumpy_summary_rows",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "screeners.augment.pipeline.generate_chronos_rerank_rows",
        lambda **kwargs: [],
    )

    failure_cases = [
        (ImportError("No module named 'timesfm'"), "SKIPPED_MISSING_DEP"),
        (RuntimeError("MISSING_MODEL: timesfm weights unavailable"), "SKIPPED_MISSING_MODEL"),
        (RuntimeError("forecast exploded"), "FAILED_RUNTIME"),
    ]
    for exc, expected_status in failure_cases:
        monkeypatch.setattr(
            "screeners.augment.pipeline.generate_timesfm_rerank_rows",
            lambda **kwargs: (_ for _ in ()).throw(exc),
        )

        result = run_screening_augment(market="us", source_registry_snapshot=snapshot)

        assert result["status"] == "partial"
        assert result["module_summaries"]["timesfm2p5"]["status"] == expected_status
        timesfm_path = augment_root / "timesfm2p5_rerank.csv"
        assert timesfm_path.exists()
        timesfm_frame = pd.read_csv(timesfm_path)
        assert list(timesfm_frame["symbol"]) == ["AAA"]
        assert "up_close_prob_proxy_5d" in timesfm_frame.columns
