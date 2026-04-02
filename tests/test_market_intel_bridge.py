from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

from screeners.signals.engine import MultiScreenerSignalEngine
from tests._paths import cache_root


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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


def test_load_source_registry_merges_market_intel_core_buy_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = _test_root("merge")
    screeners_root = base / "screeners"
    compat_root = base / "market_intel_compat"
    _write_csv(
        screeners_root / "qullamaggie" / "daily_focus_list.csv",
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
        "screeners.leader_core_bridge.get_market_intel_compat_root",
        lambda market: str(compat_root / market),
    )

    engine = MultiScreenerSignalEngine(market="us", as_of_date="2026-04-02")
    registry = engine._load_source_registry()

    nvda = registry["NVDA"]
    assert nvda["buy_eligible"] is True
    assert nvda["watch_only"] is False
    assert "MIC_LEADER_CORE" in nvda["source_tags"]
    assert nvda["primary_source_tag"] == "MIC_LEADER_CORE"
    assert nvda["screen_stage"] == "LEADER_CORE"
    assert nvda["group_state"] == "STRONG"
    assert nvda["leader_state"] == "CONFIRMED"
    assert nvda["breakdown_status"] == "OK"

    amd = registry["AMD"]
    assert amd["buy_eligible"] is False
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
        screeners_root / "qullamaggie" / "daily_focus_list.csv",
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

    engine = MultiScreenerSignalEngine(market="us", as_of_date="2026-04-02")

    with pytest.raises(ValueError, match="leader core artifact is stale"):
        engine._load_source_registry()


def test_load_source_registry_requires_leader_core_schema_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = _test_root("schema")
    screeners_root = base / "screeners"
    compat_root = base / "market_intel_compat"
    _write_csv(
        screeners_root / "qullamaggie" / "daily_focus_list.csv",
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

    engine = MultiScreenerSignalEngine(market="us", as_of_date="2026-04-02")

    with pytest.raises(ValueError, match="leader core artifact schema mismatch"):
        engine._load_source_registry()


def test_load_source_registry_requires_market_truth_schema_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = _test_root("market-truth-schema")
    screeners_root = base / "screeners"
    compat_root = base / "market_intel_compat"
    _write_csv(
        screeners_root / "qullamaggie" / "daily_focus_list.csv",
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

    engine = MultiScreenerSignalEngine(market="us", as_of_date="2026-04-02")

    with pytest.raises(ValueError, match="market truth artifact schema mismatch"):
        engine._load_source_registry()
