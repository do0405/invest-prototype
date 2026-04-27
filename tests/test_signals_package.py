from __future__ import annotations

import importlib
import sys
import shutil
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from screeners.source_contracts import (
    CANONICAL_SOURCE_SPECS,
    SOURCE_DISPOSITIONS,
    SOURCE_TAG_PRIORITY,
    SOURCE_TAG_STYLE,
)
from tests._paths import cache_root


def test_signals_package_import_is_lazy() -> None:
    sys.modules.pop("screeners.signals", None)
    sys.modules.pop("screeners.signals.engine", None)

    signals = importlib.import_module("screeners.signals")

    assert "screeners.signals.engine" not in sys.modules

    _ = signals.MultiScreenerSignalEngine

    assert "screeners.signals.engine" in sys.modules


def test_signals_package_preserves_legacy_public_aliases() -> None:
    signals = importlib.import_module("screeners.signals")
    engine = importlib.import_module("screeners.signals.engine")

    expected_aliases = {
        "SignalEngine",
        "QullamaggieSignalEngine",
        "run_signal_scan",
        "run_peg_imminent_screen",
        "run_qullamaggie_signal_scan",
    }

    assert expected_aliases <= set(signals.__all__)
    assert signals.SignalEngine is engine.MultiScreenerSignalEngine
    assert signals.QullamaggieSignalEngine is engine.MultiScreenerSignalEngine
    assert signals.run_signal_scan is engine.run_multi_screener_signal_scan
    assert signals.run_peg_imminent_screen is engine.run_peg_imminent_screen
    assert signals.run_qullamaggie_signal_scan is engine.run_qullamaggie_signal_scan


def test_signal_engine_uses_shared_source_contract() -> None:
    engine = importlib.import_module("screeners.signals.engine")

    assert engine._SOURCE_SPECS is CANONICAL_SOURCE_SPECS
    tags = {spec.source_tag for spec in CANONICAL_SOURCE_SPECS}
    assert "WS_PRE_STAGE2" not in tags
    assert "WS_PATTERN_INCLUDED" not in tags
    assert "QMG_UNIVERSE" not in tags
    assert {
        str(spec.source_disposition)
        for spec in CANONICAL_SOURCE_SPECS
    } <= {"buy_eligible", "watch_only"}
    assert SOURCE_DISPOSITIONS == ("buy_eligible", "watch_only")
    assert "PEG_ONLY" not in SOURCE_TAG_PRIORITY
    assert "PEG_ONLY" not in SOURCE_TAG_STYLE


def test_signal_writers_include_earnings_provider_diagnostics() -> None:
    writers = importlib.import_module("screeners.signals.writers")
    written: list[str] = []
    output_dir = cache_root("signals_package_writer")
    if output_dir.exists():
        shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    def _capture(output_dir: str, filename_prefix: str, rows: list[dict[str, object]]) -> None:
        assert output_dir == str(output_dir_path)
        written.append(filename_prefix)

    output_dir_path = output_dir

    writers.write_signal_outputs(
        str(output_dir_path),
        trend_event_rows=[],
        trend_state_rows=[],
        ug_event_rows=[],
        ug_state_rows=[],
        ug_combo_rows=[],
        all_signal_rows=[],
        buy_signals_all_rows=[],
        sell_signals_all_rows=[],
        buy_signals_screened_rows=[],
        sell_signals_screened_rows=[],
        open_cycle_rows=[],
        diagnostics=[],
        earnings_provider_diagnostics=[{"symbol": "AAOI"}],
        signal_universe_rows=[],
        source_registry_summary={},
        signal_summary={},
        write_records_fn=_capture,
    )

    assert "earnings_provider_diagnostics" in written
    assert "buy_signals_all_symbols_v1" in written
    assert "sell_signals_all_symbols_v1" in written
    assert "buy_signals_screened_symbols_v1" in written
    assert "sell_signals_screened_symbols_v1" in written


def test_signal_writers_expose_artifact_contract_constants() -> None:
    writers = importlib.import_module("screeners.signals.writers")

    assert writers.PUBLIC_TODAY_ONLY_SIGNAL_OUTPUTS == (
        "buy_signals_all_symbols_v1",
        "sell_signals_all_symbols_v1",
        "buy_signals_screened_symbols_v1",
        "sell_signals_screened_symbols_v1",
    )
    assert "all_signals_v2" in writers.INTERNAL_SIGNAL_DIAGNOSTIC_OUTPUTS
    assert "screen_signal_diagnostics" in writers.INTERNAL_SIGNAL_DIAGNOSTIC_OUTPUTS
    assert "signal_universe_snapshot" in writers.INTERNAL_SIGNAL_DIAGNOSTIC_OUTPUTS
    assert "open_family_cycles" not in writers.INTERNAL_SIGNAL_DIAGNOSTIC_OUTPUTS
    assert set(writers.PUBLIC_TODAY_ONLY_SIGNAL_OUTPUTS).isdisjoint(
        writers.INTERNAL_SIGNAL_DIAGNOSTIC_OUTPUTS
    )

    written: list[str] = []
    output_dir = cache_root("signals_package_writer_contract")
    if output_dir.exists():
        shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    writers.write_signal_outputs(
        str(output_dir),
        trend_event_rows=[],
        trend_state_rows=[],
        ug_event_rows=[],
        ug_state_rows=[],
        ug_combo_rows=[],
        all_signal_rows=[],
        buy_signals_all_rows=[],
        sell_signals_all_rows=[],
        buy_signals_screened_rows=[],
        sell_signals_screened_rows=[],
        open_cycle_rows=[],
        diagnostics=[],
        earnings_provider_diagnostics=[],
        signal_universe_rows=[],
        source_registry_summary={},
        signal_summary={},
        write_records_fn=lambda _output_dir, filename_prefix, _rows: written.append(filename_prefix),
    )

    assert tuple(written) == writers.SIGNAL_RECORD_OUTPUT_PREFIXES


def test_source_registry_can_load_v3_snapshot_without_scanning() -> None:
    source_registry = importlib.import_module("screeners.signals.source_registry")

    snapshot = {
        "schema_version": 3,
        "market": "US",
        "as_of_date": "2026-04-18",
        "registry": {
            "AAA": {
                "symbol": "AAA",
                "source_disposition": "buy_eligible",
                "source_buy_eligible": True,
                "buy_eligible": True,
                "watch_only": False,
                "screen_stage": "PATTERN_INCLUDED",
                "source_tags": ["QMG_PATTERN_INCLUDED"],
                "source_records": [],
                "sector": "Tech",
                "industry": "Software",
                "group_name": "Software",
                "as_of_ts": "2026-04-18",
            }
        },
        "source_rows": [],
    }

    registry = source_registry.load_source_registry(
        screeners_root="unused",
        market="us",
        source_specs=[],
        stage_priority=lambda stage: 0,
        source_tag_priority=lambda tag: 0.0,
        sorted_source_tags=lambda tags: list(tags or []),
        source_style_tags=lambda tags: list(tags or []),
        primary_source_style=lambda tags: "",
        source_priority_score=lambda tags: 0.0,
        source_engine_bonus=lambda tags, engine="TREND": 0.0,
        safe_text=lambda value: str(value or "").strip(),
        snapshot=snapshot,
    )

    assert registry["AAA"]["symbol"] == "AAA"
    assert registry["AAA"]["source_buy_eligible"] is True
    assert registry["AAA"]["source_disposition"] == "buy_eligible"
    assert registry["AAA"]["watch_only"] is False


def test_source_registry_rejects_v2_snapshot_and_rebuilds_from_sources() -> None:
    source_registry = importlib.import_module("screeners.signals.source_registry")
    root = cache_root("signals_package_snapshot_v2_rebuild")
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    source_dir = root / "screeners" / "qullamaggie"
    source_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"symbol": "BBB", "as_of_ts": "2026-04-18"}]).to_csv(
        source_dir / "pattern_included_candidates.csv",
        index=False,
    )
    old_snapshot = {
        "schema_version": 2,
        "market": "US",
        "as_of_date": "2026-04-18",
        "registry": {
            "AAA": {
                "symbol": "AAA",
                "source_disposition": "buy_eligible",
                "source_buy_eligible": True,
            }
        },
    }

    assert source_registry.snapshot_is_compatible(old_snapshot, market="us") is False
    registry = source_registry.load_source_registry(
        screeners_root=str(root / "screeners"),
        market="us",
        source_specs=[
            SimpleNamespace(
                relative_path="qullamaggie/pattern_included_candidates.csv",
                screen_stage="PATTERN_INCLUDED",
                source_tag="QMG_PATTERN_INCLUDED",
                buy_eligible=True,
                source_disposition="buy_eligible",
            )
        ],
        stage_priority=lambda stage: {"PATTERN_INCLUDED": 1}.get(str(stage), 0),
        source_tag_priority=lambda tag: 0.0,
        sorted_source_tags=lambda tags: list(tags or []),
        source_style_tags=lambda tags: list(tags or []),
        primary_source_style=lambda tags: "",
        source_priority_score=lambda tags: 0.0,
        source_engine_bonus=lambda tags, engine="TREND": 0.0,
        safe_text=lambda value: str(value or "").strip(),
        snapshot=old_snapshot,
    )

    assert set(registry) == {"BBB"}
    assert registry["BBB"]["source_disposition"] == "buy_eligible"


def test_source_registry_snapshot_defaults_as_of_to_latest_source_row() -> None:
    source_registry = importlib.import_module("screeners.signals.source_registry")
    root = cache_root("signals_package_snapshot_asof")
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    source_dir = root / "screeners" / "qullamaggie"
    source_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"symbol": "AAA", "as_of_ts": "2026-04-17"},
            {"symbol": "BBB", "as_of_ts": "2026-04-18"},
        ]
    ).to_csv(source_dir / "pattern_included_candidates.csv", index=False)

    snapshot = source_registry.build_source_registry_snapshot(
        screeners_root=str(root / "screeners"),
        market="us",
        source_specs=[
            SimpleNamespace(
                relative_path="qullamaggie/pattern_included_candidates.csv",
                screen_stage="PATTERN_INCLUDED",
                source_tag="QMG_PATTERN_INCLUDED",
                buy_eligible=True,
                source_disposition="buy_eligible",
            )
        ],
        stage_priority=lambda stage: {"PATTERN_INCLUDED": 1}.get(str(stage), 0),
        source_tag_priority=lambda tag: 0.0,
        sorted_source_tags=lambda tags: sorted(str(tag) for tag in (tags or []) if str(tag).strip()),
        source_style_tags=lambda tags: list(tags or []),
        primary_source_style=lambda tags: "",
        source_priority_score=lambda tags: 0.0,
        source_engine_bonus=lambda tags, engine="TREND": 0.0,
        safe_text=lambda value: str(value or "").strip(),
    )

    assert snapshot["as_of_date"] == "2026-04-18"
    assert snapshot["registry"]["BBB"]["source_disposition"] == "buy_eligible"


def test_source_registry_copies_leader_lagging_evidence_fields_into_source_records() -> None:
    source_registry = importlib.import_module("screeners.signals.source_registry")
    root = cache_root("signals_package_ll_evidence")
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    source_dir = root / "screeners" / "leader_lagging"
    source_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "as_of_ts": "2026-04-18",
                "source_evidence_tags": "rs_new_high_before_price,hidden_rs",
                "rs_quality_score": 91.5,
                "rs_proxy_confidence": 83.0,
                "hidden_rs_confidence": 72.0,
                "hidden_rs_down_day_excess_return": 0.012,
                "leader_rs_state": "rising",
                "leader_tier": "strong",
                "entry_suitability": "fresh",
                "legacy_label": "Confirmed Leader",
                "leader_confidence_score": 84.0,
                "confidence_bucket": "high",
                "low_confidence_reason_codes": "",
                "reject_reason_codes": "",
                "extended_reason_codes": "",
                "threshold_proximity_codes": "near_rs_threshold",
                "hybrid_gate_pass": True,
                "strict_rs_gate_pass": True,
                "box_touch_count": 5,
                "support_hold_count": 7,
                "dry_volume_score": 86.0,
                "breakout_quality_score": 64.0,
                "structure_confidence": 77.0,
                "base_depth_pct": 11.5,
                "loose_base_risk_score": 18.0,
                "support_violation_count": 0,
                "breakout_failure_count": 0,
                "breakout_volume_quality_score": 82.0,
                "structure_reject_reason_codes": "no_volume_breakout",
                "peer_lead_score": 77.0,
                "follower_confidence_score": 71.0,
                "pair_evidence_confidence": 68.0,
                "lag_profile_sample_count": 54,
                "lag_profile_stability_score": 62.0,
                "catchup_room_score": 74.0,
                "propagation_state": "early_response",
                "follower_reject_reason_codes": "unstable_lag_profile",
                "link_evidence_tags": "same_industry,best_lag_3",
            }
        ]
    ).to_csv(source_dir / "leaders.csv", index=False)

    snapshot = source_registry.build_source_registry_snapshot(
        screeners_root=str(root / "screeners"),
        market="us",
        source_specs=[
            SimpleNamespace(
                relative_path="leader_lagging/leaders.csv",
                screen_stage="LEADER",
                source_tag="LL_LEADER",
                buy_eligible=True,
                source_disposition="buy_eligible",
            )
        ],
        stage_priority=lambda stage: {"LEADER": 1}.get(str(stage), 0),
        source_tag_priority=lambda tag: 0.0,
        sorted_source_tags=lambda tags: sorted(str(tag) for tag in (tags or []) if str(tag).strip()),
        source_style_tags=lambda tags: list(tags or []),
        primary_source_style=lambda tags: "",
        source_priority_score=lambda tags: 0.0,
        source_engine_bonus=lambda tags, engine="TREND": 0.0,
        safe_text=lambda value: str(value or "").strip(),
    )

    record = snapshot["registry"]["AAA"]["source_records"][0]
    assert record["source_tag"] == "LL_LEADER"
    assert record["source_evidence_tags"] == "rs_new_high_before_price,hidden_rs"
    assert record["rs_quality_score"] == pytest.approx(91.5)
    assert record["rs_proxy_confidence"] == pytest.approx(83.0)
    assert record["hidden_rs_confidence"] == pytest.approx(72.0)
    assert record["hidden_rs_down_day_excess_return"] == pytest.approx(0.012)
    assert record["leader_rs_state"] == "rising"
    assert record["leader_tier"] == "strong"
    assert record["entry_suitability"] == "fresh"
    assert record["legacy_label"] == "Confirmed Leader"
    assert record["leader_confidence_score"] == pytest.approx(84.0)
    assert record["confidence_bucket"] == "high"
    assert record["threshold_proximity_codes"] == "near_rs_threshold"
    assert record["hybrid_gate_pass"] is True
    assert record["strict_rs_gate_pass"] is True
    assert record["box_touch_count"] == pytest.approx(5.0)
    assert record["support_hold_count"] == pytest.approx(7.0)
    assert record["dry_volume_score"] == pytest.approx(86.0)
    assert record["breakout_quality_score"] == pytest.approx(64.0)
    assert record["structure_confidence"] == pytest.approx(77.0)
    assert record["base_depth_pct"] == pytest.approx(11.5)
    assert record["loose_base_risk_score"] == pytest.approx(18.0)
    assert record["support_violation_count"] == pytest.approx(0.0)
    assert record["breakout_failure_count"] == pytest.approx(0.0)
    assert record["breakout_volume_quality_score"] == pytest.approx(82.0)
    assert record["structure_reject_reason_codes"] == "no_volume_breakout"
    assert record["follower_confidence_score"] == pytest.approx(71.0)
    assert record["pair_evidence_confidence"] == pytest.approx(68.0)
    assert record["lag_profile_sample_count"] == pytest.approx(54.0)
    assert record["lag_profile_stability_score"] == pytest.approx(62.0)
    assert record["catchup_room_score"] == pytest.approx(74.0)
    assert record["propagation_state"] == "early_response"
    assert record["follower_reject_reason_codes"] == "unstable_lag_profile"
    assert record["link_evidence_tags"] == "same_industry,best_lag_3"
    assert "rotation_state" not in record
    assert "leader_lifecycle_phase" not in record
    assert "prior_cycle_exclusion_score" not in record
    assert "rotation_candidate_score" not in record
    assert "source_url" not in record
