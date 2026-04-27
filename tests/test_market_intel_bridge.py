from __future__ import annotations

import json
import importlib
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from screeners import leader_core_bridge
from tests._paths import cache_root


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_json_bom(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")


def _base_summary(*, as_of: str = "2026-04-02", schema_version: str = "leader_core_v1") -> dict[str, object]:
    return {
        "market": "us",
        "as_of": as_of,
        "schema_version": schema_version,
        "engine_version": "leader_kernel_v1",
        "leader_health_score": 74.0,
        "leader_health_status": "HEALTHY",
        "confirmed_count": 1,
        "imminent_count": 0,
        "broken_count": 0,
    }


def _base_market_context(*, as_of: str = "2026-04-02", schema_version: str = "market_context_v1") -> dict[str, object]:
    return {
        "market": "us",
        "as_of": as_of,
        "schema_version": schema_version,
        "engine_version": "compat_market_context_v1",
        "regime_state": "uptrend",
        "top_state": "risk_on",
        "market_state": "uptrend",
        "breadth_state": "broad_participation",
        "concentration_state": "diversified",
        "leadership_state": "growth_ai",
        "prototype_market_alias": "RISK_ON",
        "market_alignment_score": 82.0,
        "breadth_support_score": 78.0,
        "rotation_support_score": 86.0,
        "leader_health_score": 74.0,
        "leader_health_status": "HEALTHY",
    }


def _base_group_rows(*, as_of: str = "2026-04-02") -> list[dict[str, object]]:
    return [
        {
            "market": "us",
            "as_of": as_of,
            "schema_version": "leader_core_v1",
            "engine_version": "leader_kernel_v1",
            "industry_key": "technology__semiconductors",
            "group_strength_score": 88.0,
            "group_state": "STRONG",
            "rank": 1.0,
            "leaders": ["NVDA", "AMD"],
        }
    ]


def _test_root(name: str) -> Path:
    root = cache_root("market_intel_bridge", name)
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _signal_engine_class():
    engine = importlib.import_module("screeners.signals.engine")
    return engine.MultiScreenerSignalEngine


def test_load_source_registry_merges_market_intel_core_buy_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = _test_root("merge")
    screeners_root = base / "screeners"
    compat_root = base / "market_intel_compat"
    _write_csv(
        screeners_root / "qullamaggie" / "pattern_included_candidates.csv",
        [{"symbol": "QQQ", "market": "us", "sector": "Technology", "industry": "Software"}],
    )
    _write_json(
        compat_root / "us" / "leader_market_summary.json",
        _base_summary(),
    )
    _write_json(
        compat_root / "us" / "market_context.json",
        _base_market_context(),
    )
    _write_json(
        compat_root / "us" / "industry_rotation.json",
        _base_group_rows(),
    )
    _write_json(
        compat_root / "us" / "leaders.json",
        [
            {
                "symbol": "NVDA",
                "market": "us",
                "as_of": "2026-04-02",
                "schema_version": "leader_core_v1",
                "engine_version": "leader_kernel_v1",
                "industry_key": "technology__semiconductors",
                "group_strength_score": 88.0,
                "group_state": "STRONG",
                "leader_score": 84.0,
                "leader_state": "CONFIRMED",
                "breakdown_score": 12.0,
                "breakdown_status": "OK",
            },
            {
                "symbol": "AMD",
                "market": "us",
                "as_of": "2026-04-02",
                "schema_version": "leader_core_v1",
                "engine_version": "leader_kernel_v1",
                "industry_key": "technology__semiconductors",
                "group_strength_score": 82.0,
                "group_state": "STRONG",
                "leader_score": 68.0,
                "leader_state": "EMERGING",
                "breakdown_score": 46.0,
                "breakdown_status": "IMMINENT",
            },
            {
                "symbol": "TSLA",
                "market": "us",
                "as_of": "2026-04-02",
                "schema_version": "leader_core_v1",
                "engine_version": "leader_kernel_v1",
                "industry_key": "consumer__auto",
                "group_strength_score": 30.0,
                "group_state": "WEAK",
                "leader_score": 58.0,
                "leader_state": "REJECT",
                "breakdown_score": 82.0,
                "breakdown_status": "BROKEN",
            },
        ],
    )

    monkeypatch.setattr(
        "screeners.signals.engine.get_market_screeners_root",
        lambda market: str(screeners_root),
    )
    monkeypatch.setattr(
        "screeners.signals.engine._source_registry.read_source_registry_snapshot",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "screeners.leader_core_bridge.get_market_intel_compat_root",
        lambda market: str(compat_root / market),
    )

    engine = _signal_engine_class()(market="us", as_of_date="2026-04-02")
    registry = engine._load_source_registry()

    nvda = registry["NVDA"]
    assert nvda["source_buy_eligible"] is True
    assert nvda["source_disposition"] == "buy_eligible"
    assert nvda["watch_only"] is False
    assert "MIC_LEADER_CORE" in nvda["source_tags"]
    assert nvda["primary_source_tag"] == "MIC_LEADER_CORE"
    assert nvda["screen_stage"] == "LEADER_CORE"
    assert nvda["group_state"] == "STRONG"
    assert nvda["leader_state"] == "CONFIRMED"
    assert nvda["breakdown_status"] == "OK"

    amd = registry["AMD"]
    assert amd["source_buy_eligible"] is False
    assert amd["source_disposition"] == "watch_only"
    assert amd["watch_only"] is True
    assert amd["breakdown_status"] == "IMMINENT"

    assert "TSLA" not in registry
    assert "QQQ" in registry


def test_load_source_registry_requires_same_day_market_intel_core_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = _test_root("stale")
    screeners_root = base / "screeners"
    compat_root = base / "market_intel_compat"
    _write_csv(
        screeners_root / "qullamaggie" / "pattern_included_candidates.csv",
        [{"symbol": "QQQ", "market": "us"}],
    )
    _write_json(
        compat_root / "us" / "leader_market_summary.json",
        _base_summary(as_of="2026-04-01"),
    )
    _write_json(
        compat_root / "us" / "market_context.json",
        _base_market_context(as_of="2026-04-01"),
    )
    _write_json(compat_root / "us" / "industry_rotation.json", _base_group_rows(as_of="2026-04-01"))
    _write_json(compat_root / "us" / "leaders.json", [])

    monkeypatch.setattr(
        "screeners.signals.engine.get_market_screeners_root",
        lambda market: str(screeners_root),
    )
    monkeypatch.setattr(
        "screeners.leader_core_bridge.get_market_intel_compat_root",
        lambda market: str(compat_root / market),
    )

    engine = _signal_engine_class()(market="us", as_of_date="2026-04-02")

    with pytest.raises(ValueError, match="leader core artifact is stale"):
        engine._load_source_registry()


def test_missing_leader_core_artifact_error_includes_remediation() -> None:
    base = _test_root("missing-compat")
    missing_root = base / "market_intel_compat" / "us"

    with pytest.raises(ValueError) as exc_info:
        leader_core_bridge.load_market_truth_snapshot(
            "us",
            as_of_date="2026-04-02",
            compat_root_resolver=lambda market: str(missing_root),
        )

    message = str(exc_info.value)
    assert str(missing_root / "leader_market_summary.json") in message
    assert "--standalone" in message
    assert "MARKET_INTEL_COMPAT_RESULTS_ROOT" in message


@pytest.mark.parametrize(
    ("name", "summary_payload", "context_payload", "expected_status"),
    [
        ("probe-compat", _base_summary(), _base_market_context(), "compat"),
        ("probe-missing", _base_summary(), None, "missing"),
        (
            "probe-stale",
            _base_summary(as_of="2026-04-01"),
            _base_market_context(),
            "stale",
        ),
        (
            "probe-schema",
            _base_summary(schema_version="legacy_v0"),
            _base_market_context(),
            "schema_mismatch",
        ),
    ],
)
def test_probe_market_intel_compat_availability_detects_expected_status(
    name: str,
    summary_payload: dict[str, object] | None,
    context_payload: dict[str, object] | None,
    expected_status: str,
) -> None:
    base = _test_root(name)
    compat_root = base / "market_intel_compat"
    if summary_payload is not None:
        _write_json(compat_root / "us" / "leader_market_summary.json", summary_payload)
    if context_payload is not None:
        _write_json(compat_root / "us" / "market_context.json", context_payload)

    result = leader_core_bridge.probe_market_intel_compat_availability(
        "us",
        as_of_date="2026-04-02",
        compat_root_resolver=lambda market: str(compat_root / market),
    )

    assert result.status == expected_status


def test_load_source_registry_requires_leader_core_schema_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = _test_root("schema")
    screeners_root = base / "screeners"
    compat_root = base / "market_intel_compat"
    _write_csv(
        screeners_root / "qullamaggie" / "pattern_included_candidates.csv",
        [{"symbol": "QQQ", "market": "us"}],
    )
    _write_json(
        compat_root / "us" / "leader_market_summary.json",
        _base_summary(schema_version="legacy_v0"),
    )
    _write_json(
        compat_root / "us" / "market_context.json",
        _base_market_context(),
    )
    _write_json(compat_root / "us" / "industry_rotation.json", _base_group_rows())
    _write_json(compat_root / "us" / "leaders.json", [])

    monkeypatch.setattr(
        "screeners.signals.engine.get_market_screeners_root",
        lambda market: str(screeners_root),
    )
    monkeypatch.setattr(
        "screeners.leader_core_bridge.get_market_intel_compat_root",
        lambda market: str(compat_root / market),
    )

    engine = _signal_engine_class()(market="us", as_of_date="2026-04-02")

    with pytest.raises(ValueError, match="leader core artifact schema mismatch"):
        engine._load_source_registry()


def test_load_source_registry_requires_market_truth_schema_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = _test_root("market-truth-schema")
    screeners_root = base / "screeners"
    compat_root = base / "market_intel_compat"
    _write_csv(
        screeners_root / "qullamaggie" / "pattern_included_candidates.csv",
        [{"symbol": "QQQ", "market": "us"}],
    )
    _write_json(
        compat_root / "us" / "leader_market_summary.json",
        _base_summary(),
    )
    _write_json(
        compat_root / "us" / "market_context.json",
        _base_market_context(schema_version="legacy_market_context_v0"),
    )
    _write_json(compat_root / "us" / "industry_rotation.json", _base_group_rows())
    _write_json(compat_root / "us" / "leaders.json", [])

    monkeypatch.setattr(
        "screeners.signals.engine.get_market_screeners_root",
        lambda market: str(screeners_root),
    )
    monkeypatch.setattr(
        "screeners.leader_core_bridge.get_market_intel_compat_root",
        lambda market: str(compat_root / market),
    )

    engine = _signal_engine_class()(market="us", as_of_date="2026-04-02")

    with pytest.raises(ValueError, match="market truth artifact schema mismatch"):
        engine._load_source_registry()


def test_load_source_registry_accepts_utf8_bom_compat_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = _test_root("utf8-bom")
    screeners_root = base / "screeners"
    compat_root = base / "market_intel_compat"
    _write_csv(
        screeners_root / "qullamaggie" / "pattern_included_candidates.csv",
        [{"symbol": "QQQ", "market": "us"}],
    )
    _write_json_bom(
        compat_root / "us" / "leader_market_summary.json",
        _base_summary(),
    )
    _write_json_bom(
        compat_root / "us" / "market_context.json",
        _base_market_context(),
    )
    _write_json_bom(
        compat_root / "us" / "industry_rotation.json",
        _base_group_rows(),
    )
    _write_json_bom(
        compat_root / "us" / "leaders.json",
        [],
    )

    monkeypatch.setattr(
        "screeners.signals.engine.get_market_screeners_root",
        lambda market: str(screeners_root),
    )
    monkeypatch.setattr(
        "screeners.signals.engine._source_registry.read_source_registry_snapshot",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "screeners.leader_core_bridge.get_market_intel_compat_root",
        lambda market: str(compat_root / market),
    )

    engine = _signal_engine_class()(market="us", as_of_date="2026-04-02")
    registry = engine._load_source_registry()

    assert "QQQ" in registry


def _benchmark_daily(*, start: str = "2025-01-02", end_value: float = 145.0) -> pd.DataFrame:
    dates = pd.bdate_range(start=start, periods=260)
    closes = pd.Series(np.linspace(100.0, end_value, len(dates)))
    return pd.DataFrame(
        {
            "date": dates,
            "open": closes * 0.99,
            "high": closes * 1.01,
            "low": closes * 0.98,
            "close": closes,
            "adj_close": closes,
            "volume": 1_000_000.0,
        }
    )


def test_leader_core_bridge_exposes_local_standalone_helpers() -> None:
    assert hasattr(__import__("screeners.leader_core_bridge", fromlist=["x"]), "empty_leader_core_snapshot")
    assert hasattr(__import__("screeners.leader_core_bridge", fromlist=["x"]), "build_local_market_truth_snapshot")


def test_build_local_market_truth_snapshot_uses_benchmark_trend() -> None:
    from screeners import leader_core_bridge as bridge

    benchmark_daily = _benchmark_daily(end_value=145.0)
    snapshot = bridge.build_local_market_truth_snapshot(
        market="us",
        as_of_date="2026-04-02",
        benchmark_symbol="SPY",
        benchmark_daily=benchmark_daily,
    )

    assert snapshot.market == "us"
    assert snapshot.market_alias == "RISK_ON"
    assert snapshot.top_state == "risk_on"
    assert snapshot.market_state == "uptrend"
    assert snapshot.breadth_state == "benchmark_only"
    assert snapshot.concentration_state == "local_unknown"
    assert snapshot.leadership_state == "local_screeners"
    assert snapshot.market_alignment_score is not None and snapshot.market_alignment_score >= 70.0
    assert snapshot.breadth_support_score == snapshot.market_alignment_score
    assert snapshot.rotation_support_score == snapshot.market_alignment_score
    assert snapshot.leader_health_score == snapshot.market_alignment_score
    assert snapshot.leader_health_status == "HEALTHY"
    assert snapshot.leader_core.groups_by_key == {}
    assert snapshot.leader_core.leaders_by_symbol == {}


def test_standalone_signal_engine_allows_empty_local_screener_outputs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = _test_root("standalone-empty")
    screeners_root = base / "screeners"
    screeners_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "screeners.signals.engine.get_market_screeners_root",
        lambda market: str(screeners_root),
    )
    monkeypatch.setattr(
        "screeners.signals.engine._market_intel_bridge.load_market_truth_snapshot",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("compat market truth should not be read in standalone mode")),
    )
    monkeypatch.setattr(
        "screeners.signals.engine._market_intel_bridge.load_leader_core_registry_entries",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("compat leader registry should not be read in standalone mode")),
    )
    monkeypatch.setattr(
        "screeners.signals.engine._market_intel_bridge.build_local_market_truth_snapshot",
        lambda **kwargs: type(
            "StandaloneTruth",
            (),
            {
                "market_alias": "RISK_ON",
                "regime_state": "uptrend",
                "top_state": "risk_on",
                "market_state": "uptrend",
                "breadth_state": "benchmark_only",
                "concentration_state": "local_unknown",
                "leadership_state": "local_screeners",
                "market_alignment_score": 82.0,
                "breadth_support_score": 82.0,
                "rotation_support_score": 82.0,
                "leader_health_score": 82.0,
                "leader_health_status": "HEALTHY",
                "leader_core": type("EmptyCore", (), {"groups_by_key": {}, "leaders_by_symbol": {}})(),
            },
        )(),
        raising=False,
    )

    engine = _signal_engine_class()(market="us", as_of_date="2026-04-02", standalone=True)
    registry = engine._load_source_registry()

    assert registry == {}
    assert engine.market_truth_snapshot is not None


def test_standalone_signal_engine_ignores_compat_and_applies_local_truth_overlay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = _test_root("standalone-overlay")
    screeners_root = base / "screeners"
    _write_csv(
        screeners_root / "qullamaggie" / "pattern_included_candidates.csv",
        [{"symbol": "QQQ", "market": "us", "sector": "Technology", "industry": "Software"}],
    )

    monkeypatch.setattr(
        "screeners.signals.engine.get_market_screeners_root",
        lambda market: str(screeners_root),
    )
    monkeypatch.setattr(
        "screeners.signals.engine._source_registry.read_source_registry_snapshot",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        "screeners.signals.engine._market_intel_bridge.load_market_truth_snapshot",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("compat market truth should not be read in standalone mode")),
    )
    monkeypatch.setattr(
        "screeners.signals.engine._market_intel_bridge.load_leader_core_registry_entries",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("compat leader registry should not be read in standalone mode")),
    )
    monkeypatch.setattr(
        "screeners.signals.engine._market_intel_bridge.build_local_market_truth_snapshot",
        lambda **kwargs: type(
            "StandaloneTruth",
            (),
            {
                "market_alias": "RISK_ON",
                "regime_state": "uptrend",
                "top_state": "risk_on",
                "market_state": "uptrend",
                "breadth_state": "benchmark_only",
                "concentration_state": "local_unknown",
                "leadership_state": "local_screeners",
                "market_alignment_score": 82.0,
                "breadth_support_score": 82.0,
                "rotation_support_score": 82.0,
                "leader_health_score": 82.0,
                "leader_health_status": "HEALTHY",
                "leader_core": type("EmptyCore", (), {"groups_by_key": {}, "leaders_by_symbol": {}})(),
            },
        )(),
        raising=False,
    )

    engine = _signal_engine_class()(market="us", as_of_date="2026-04-02", standalone=True)
    registry = engine._load_source_registry()

    qqq = registry["QQQ"]
    assert qqq["market_condition_state"] == "RISK_ON"
    assert qqq["market_alignment_score"] is not None and qqq["market_alignment_score"] >= 70.0
    assert qqq["market_truth_source"] == "local_standalone"
    assert qqq["core_overlay_applied"] is False
