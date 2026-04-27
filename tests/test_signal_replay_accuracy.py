from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

import screeners.signals.engine as signal_engine


def _minimal_daily_frame(*, end: str = "2026-04-22") -> pd.DataFrame:
    dates = pd.bdate_range(end=end, periods=3)
    return pd.DataFrame(
        {
            "date": dates,
            "open": [98.0, 99.0, 100.0],
            "high": [101.0, 102.0, 103.0],
            "low": [97.0, 98.0, 99.0],
            "close": [100.0, 101.0, 102.0],
            "volume": [1_000_000.0, 1_100_000.0, 1_200_000.0],
        }
    )


def _patch_engine_init(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(signal_engine, "_load_metadata_map", lambda market: {})
    monkeypatch.setattr(signal_engine, "_load_financial_map", lambda market, symbols=None: {})


def test_signal_default_as_of_uses_latest_completed_session_when_benchmark_is_newer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_engine_init(monkeypatch)
    captured: dict[str, object] = {}
    benchmark_daily = _minimal_daily_frame(end="2026-04-22")

    def _resolve_latest_completed_as_of(**kwargs):  # noqa: ANN003
        captured.update(kwargs)
        return SimpleNamespace(
            as_of_date="2026-04-21",
            latest_completed_session="2026-04-21",
            benchmark_as_of_date="2026-04-22",
            freshness_status="future_benchmark_clipped",
            reason="benchmark_newer_than_latest_completed_session",
            explicit=False,
        )

    monkeypatch.setattr(signal_engine, "get_benchmark_candidates", lambda market: ["SPY"])
    monkeypatch.setattr(
        signal_engine,
        "load_local_ohlcv_frame",
        lambda market, symbol, price_policy=None: benchmark_daily.copy(),
    )
    monkeypatch.setattr(
        signal_engine,
        "resolve_latest_completed_as_of",
        _resolve_latest_completed_as_of,
    )

    engine = signal_engine.MultiScreenerSignalEngine(market="us")

    assert engine.as_of_date == "2026-04-21"
    assert captured["market"] == "us"
    assert captured["explicit_as_of"] is None
    assert captured["benchmark_as_of"] == "2026-04-22"


def test_signal_explicit_as_of_is_preserved_for_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_engine_init(monkeypatch)

    def _unexpected_completed_session_resolution(**_kwargs):  # noqa: ANN003
        raise AssertionError("explicit replay as_of must not be clipped")

    monkeypatch.setattr(
        signal_engine,
        "resolve_latest_completed_as_of",
        _unexpected_completed_session_resolution,
    )

    engine = signal_engine.MultiScreenerSignalEngine(
        market="us",
        as_of_date="2026-03-31",
    )

    assert engine.as_of_date == "2026-03-31"


def test_standalone_market_truth_loads_benchmark_with_resolved_as_of(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_engine_init(monkeypatch)
    captured_as_of: list[str | None] = []
    benchmark_daily = _minimal_daily_frame(end="2026-04-22")

    def _load_frame(market, symbol, **kwargs):  # noqa: ANN001, ANN003
        captured_as_of.append(kwargs.get("as_of"))
        return benchmark_daily.copy()

    monkeypatch.setattr(signal_engine, "get_primary_benchmark_symbol", lambda market: "SPY")
    monkeypatch.setattr(signal_engine, "get_benchmark_candidates", lambda market: ["SPY"])
    monkeypatch.setattr(signal_engine, "load_local_ohlcv_frame", _load_frame)
    monkeypatch.setattr(
        signal_engine._market_intel_bridge,
        "build_local_market_truth_snapshot",
        lambda **kwargs: SimpleNamespace(
            market_alias="RISK_ON",
            benchmark_rows=len(kwargs["benchmark_daily"]),
        ),
    )

    engine = signal_engine.MultiScreenerSignalEngine(
        market="us",
        as_of_date="2026-04-21",
        standalone=True,
    )
    snapshot = engine._load_local_market_truth_snapshot()

    assert captured_as_of == ["2026-04-21"]
    assert snapshot.benchmark_rows == len(benchmark_daily)


def test_public_projection_strips_internal_gate_diagnostics() -> None:
    row = signal_engine._build_signal_row(
        signal_date="2026-03-31",
        symbol="AAA",
        market="us",
        engine="UG",
        family="UG_PULLBACK",
        signal_kind="EVENT",
        signal_code="UG_BUY_PBB",
        action_type="BUY",
        conviction_grade="B",
        screen_stage="TEST",
        support_zone_low=98.0,
        support_zone_high=102.0,
        stop_level=95.0,
    )
    row.update(
        {
            "_artifact_action_type": "BUY",
            "bb_percent_b": 0.2,
            "bb_z_score": -0.8,
            "pocket_pivot_score": 100.0,
            "volume_quality_reason_codes": ["POCKET_PIVOT_DOWN_VOLUME_MAX"],
            "ug_pbb_score": 80.0,
            "ug_pbs_score": 0.0,
            "ug_mr_long_score": 0.0,
            "ug_mr_short_score": 0.0,
            "band_reversion_reason_codes": ["PBB_BAND_SUPPORT"],
        }
    )

    projected = signal_engine._project_scoped_signal_rows(
        [row],
        as_of_date="2026-03-31",
        scope_symbols={"AAA"},
        signal_side="BUY",
    )

    assert len(projected) == 1
    assert projected[0]["signal_code"] == "UG_BUY_PBB"
    blocked = {
        "_artifact_action_type",
        "bb_percent_b",
        "bb_z_score",
        "pocket_pivot_score",
        "volume_quality_reason_codes",
        "ug_pbb_score",
        "ug_pbs_score",
        "ug_mr_long_score",
        "ug_mr_short_score",
        "band_reversion_reason_codes",
    }
    assert blocked.isdisjoint(projected[0])


def test_public_projection_stays_today_only_with_internal_history_present() -> None:
    current = signal_engine._build_signal_row(
        signal_date="2026-03-31",
        symbol="AAA",
        market="us",
        engine="TREND",
        family="TF_BREAKOUT",
        signal_kind="EVENT",
        signal_code="TF_BUY_BREAKOUT",
        action_type="BUY",
        conviction_grade="A",
        screen_stage="TEST",
        support_zone_low=98.0,
        support_zone_high=102.0,
        stop_level=95.0,
    )
    current["_artifact_action_type"] = "BUY"
    historical = dict(current)
    historical["signal_date"] = "2026-03-30"
    historical["lead_buy_found_10d"] = True
    historical["lead_buy_found_15d"] = True

    projected = signal_engine._project_scoped_signal_rows(
        [historical, current],
        as_of_date="2026-03-31",
        scope_symbols={"AAA"},
        signal_side="BUY",
    )

    assert [row["signal_date"] for row in projected] == ["2026-03-31"]
    assert "lead_buy_found_10d" not in projected[0]
    assert "lead_buy_found_15d" not in projected[0]
