from __future__ import annotations

from types import SimpleNamespace
import shutil
from typing import Any

import numpy as np
import pandas as pd
import pytest

import screeners.signals.engine as signal_engine
from tests._paths import cache_root


def _daily_from_closes(
    closes: np.ndarray,
    volumes: np.ndarray,
    *,
    spreads: np.ndarray | None = None,
    start: str = "2025-01-02",
    end: str | None = None,
) -> pd.DataFrame:
    if end is not None:
        dates = pd.bdate_range(end=end, periods=len(closes))
    else:
        dates = pd.bdate_range(start=start, periods=len(closes))
    if spreads is None:
        spreads = np.full(len(closes), 0.01)

    rows: list[dict[str, float | pd.Timestamp]] = []
    prev_close = float(closes[0])
    for index, (date, close, volume, spread) in enumerate(zip(dates, closes, volumes, spreads)):
        close_value = float(close)
        open_value = close_value * 0.99 if index == 0 else prev_close
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


def _breakout_daily(*, end: str | None = None) -> pd.DataFrame:
    trend = np.linspace(40.0, 78.0, 90)
    base = np.array(
        [
            78.1,
            78.8,
            78.5,
            79.1,
            78.9,
            79.4,
            79.2,
            79.7,
            79.5,
            79.9,
            79.8,
            80.1,
            79.95,
            80.2,
            80.05,
            80.3,
            80.2,
            80.4,
            80.3,
            80.5,
            80.45,
            80.6,
            80.55,
            80.7,
            80.75,
        ]
    )
    breakout = np.array([81.1, 81.4, 81.9, 82.5, 84.8])
    closes = np.concatenate([trend, base, breakout])
    volumes = np.concatenate(
        [
            np.full(len(trend), 1_200_000.0),
            np.linspace(900_000.0, 480_000.0, len(base)),
            np.array([520_000.0, 500_000.0, 480_000.0, 520_000.0, 2_900_000.0]),
        ]
    )
    spreads = np.concatenate(
        [
            np.full(len(trend), 0.020),
            np.linspace(0.015, 0.004, len(base)),
            np.array([0.006, 0.006, 0.005, 0.005, 0.012]),
        ]
    )
    frame = _daily_from_closes(closes, volumes, spreads=spreads, end=end)
    prev_close = float(frame.iloc[-2]["close"])
    frame.loc[frame.index[-1], "open"] = prev_close * 1.010
    frame.loc[frame.index[-1], "high"] = prev_close * 1.060
    frame.loc[frame.index[-1], "low"] = prev_close * 1.006
    frame.loc[frame.index[-1], "close"] = prev_close * 1.046
    frame.loc[frame.index[-1], "volume"] = 2_900_000.0
    return frame


def _regular_pullback_daily(*, end: str | None = None) -> pd.DataFrame:
    trend = np.linspace(40.0, 74.0, 130)
    surge = np.array([76.0, 78.0, 80.0, 82.0, 84.0, 86.0, 88.0, 89.0, 90.0, 89.0])
    pullback = np.array([88.0, 87.2, 86.4, 85.6, 84.9, 84.4, 84.1, 84.9, 85.7, 86.8])
    closes = np.concatenate([trend, surge, pullback])
    volumes = np.concatenate(
        [
            np.full(len(trend), 1_100_000.0),
            np.linspace(1_600_000.0, 1_250_000.0, len(surge)),
            np.array(
                [860_000.0, 820_000.0, 780_000.0, 730_000.0, 680_000.0, 640_000.0, 610_000.0, 620_000.0, 700_000.0, 810_000.0]
            ),
        ]
    )
    spreads = np.concatenate(
        [
            np.full(len(trend), 0.018),
            np.full(len(surge), 0.020),
            np.array([0.015, 0.014, 0.013, 0.013, 0.012, 0.011, 0.011, 0.011, 0.012, 0.012]),
        ]
    )
    frame = _daily_from_closes(closes, volumes, spreads=spreads, end=end)
    prior = frame.iloc[-9:-1]
    channel_high = float(prior["high"].max())
    channel_low = float(prior["low"].min())
    prev_close = float(frame.iloc[-2]["close"])
    close_value = max(channel_high * 1.003, prev_close * 1.014)
    frame.loc[frame.index[-1], "open"] = prev_close * 1.001
    frame.loc[frame.index[-1], "close"] = close_value
    frame.loc[frame.index[-1], "high"] = close_value * 1.003
    frame.loc[frame.index[-1], "low"] = max(channel_low * 1.002, prev_close * 0.997)
    frame.loc[frame.index[-1], "volume"] = 830_000.0
    return frame


def _bullish_below_200_breakout_daily(*, end: str | None = None) -> pd.DataFrame:
    base = _breakout_daily(end=end)
    older_closes = np.linspace(180.0, 110.0, 120)
    older_volumes = np.full(len(older_closes), 1_000_000.0)
    older_spreads = np.full(len(older_closes), 0.018)
    closes = np.concatenate([older_closes, base["close"].to_numpy(dtype=float)])
    volumes = np.concatenate([older_volumes, base["volume"].to_numpy(dtype=float)])
    spreads = np.concatenate([older_spreads, np.full(len(base), 0.012)])
    return _daily_from_closes(closes, volumes, spreads=spreads, end=end or "2026-03-11")


def _metrics_for(
    frame: pd.DataFrame,
    *,
    symbol: str = "AAA",
    market: str = "us",
    metadata: dict[str, object] | None = None,
    financial_row: dict[str, object] | None = None,
    source_entry: dict[str, object] | None = None,
) -> dict[str, object]:
    metadata = metadata or {}
    financial_row = financial_row or {}
    source_entry = source_entry or {}
    feature_row = signal_engine._ANALYZER.compute_feature_row(symbol, market, frame, metadata)
    return signal_engine._build_metrics(
        symbol=symbol,
        market=market,
        frame=frame,
        metadata=metadata,
        financial_row=financial_row,
        feature_row=feature_row,
        source_entry=source_entry,
    )


def _engine_for_unit(monkeypatch, *, market: str = "us", as_of_date: str = "2026-03-11"):
    monkeypatch.setattr(signal_engine, "_load_metadata_map", lambda market: {})
    monkeypatch.setattr(signal_engine, "_load_financial_map", lambda market, symbols=None: {})
    monkeypatch.setattr(
        signal_engine._market_intel_bridge,
        "load_leader_core_registry_entries",
        lambda *args, **kwargs: {},
    )
    monkeypatch.setattr(
        signal_engine._market_intel_bridge,
        "load_market_truth_snapshot",
        lambda *args, **kwargs: SimpleNamespace(
            market_alias="RISK_ON",
            regime_state="uptrend",
            top_state="risk_on",
            market_state="uptrend",
            breadth_state="broad_participation",
            concentration_state="diversified",
            leadership_state="growth_ai",
            market_alignment_score=82.0,
            breadth_support_score=78.0,
            rotation_support_score=86.0,
            leader_health_score=74.0,
            leader_health_status="HEALTHY",
        ),
    )
    monkeypatch.setattr(
        signal_engine._market_intel_bridge,
        "load_market_truth_snapshot",
        lambda *args, **kwargs: SimpleNamespace(
            market_alias="RISK_ON",
            regime_state="uptrend",
            top_state="risk_on",
            market_state="uptrend",
            breadth_state="broad_participation",
            concentration_state="diversified",
            leadership_state="growth_ai",
            market_alignment_score=82.0,
            breadth_support_score=78.0,
            rotation_support_score=86.0,
            leader_health_score=74.0,
            leader_health_status="HEALTHY",
        ),
    )
    monkeypatch.setattr(
        signal_engine._market_intel_bridge,
        "load_market_truth_snapshot",
        lambda *args, **kwargs: SimpleNamespace(
            market="us",
            as_of=as_of_date,
            market_alias="RISK_ON",
            regime_state="uptrend",
            top_state="risk_on",
            market_state="uptrend",
            breadth_state="broad_participation",
            concentration_state="diversified",
            leadership_state="growth_ai",
            market_alignment_score=82.0,
            breadth_support_score=78.0,
            rotation_support_score=86.0,
            leader_health_score=74.0,
        ),
    )
    return signal_engine.MultiScreenerSignalEngine(market=market, as_of_date=as_of_date)


class _StubEarningsCollector(signal_engine.EarningsDataCollector):
    def __init__(self) -> None:
        super().__init__(cache_dir="")

    def collect(self, market, as_of_date=None):
        return pd.DataFrame(columns=["symbol", "startdatetime"])

    def get_earnings_surprise(self, symbol):
        return None


def _overlay_buy_like_row(
    *,
    signal_code: str = "TF_BUY_BREAKOUT",
    action_type: str = "BUY",
    conviction_grade: str = "A",
    market_condition_state: str = "GREEN",
    entry_price: float | None = 100.0,
    stop_level: float | None = 95.0,
) -> dict[str, object]:
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["BREAKOUT"],
    }
    metrics = dict(_metrics_for(_breakout_daily(end="2026-03-11"), source_entry=source_entry))
    metrics["date"] = "2026-03-11"
    metrics["screen_stage"] = "TEST"
    metrics["source_tags"] = ["QM_DAILY"]
    metrics["above_200ma"] = True
    metrics["alignment_state"] = "BULLISH"
    metrics["ema_turn_down"] = False
    metrics["liquidity_pass"] = True
    metrics["market_condition_state"] = market_condition_state
    metrics["market_condition_reason"] = f"MANUAL_{market_condition_state}"
    metrics["close"] = entry_price

    row = signal_engine._build_signal_row(
        signal_date="2026-03-11",
        symbol="AAA",
        market="us",
        engine="TREND",
        family="TF_BREAKOUT",
        signal_kind="EVENT",
        signal_code=signal_code,
        action_type=action_type,
        conviction_grade=conviction_grade,
        screen_stage="TEST",
        stop_level=stop_level,
        blended_entry_price=entry_price,
        source_tags=["QM_DAILY"],
    )

    return signal_engine._apply_update_overlay_rows(
        signal_engine._transform_signal_rows([row]),
        {"AAA": metrics},
    )[0]



def test_update_overlay_allows_ug_breakout_below_200_with_warning(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["BREAKOUT"],
    }
    metrics = _metrics_for(_bullish_below_200_breakout_daily(end="2026-03-11"), source_entry=source_entry)
    assert metrics["above_200ma"] is False
    assert metrics["alignment_state"] == "BULLISH"
    assert metrics["ema_turn_down"] is False
    assert metrics["breakout_ready"] is True

    events = engine._ug_buy_events(
        symbol="AAA",
        metrics=metrics,
        source_entry=source_entry,
        active_cycles={},
        signal_history=[],
    )
    raw_row = next(row for row in events if row["signal_code"] == "UG_BUY_BREAKOUT")
    dashboard_profile = engine._ug_dashboard_profile(metrics)
    state_code = engine._ug_state_code(metrics, dashboard_profile)
    raw_conviction = engine._ug_conviction(metrics, state_code, dashboard_profile)

    overlaid = signal_engine._apply_update_overlay_rows(
        signal_engine._transform_signal_rows(events),
        {"AAA": metrics},
    )
    row = next(row for row in overlaid if row["signal_code"] == "UG_BUY_BREAKOUT")

    assert raw_row["action_type"] == "BUY"
    assert "BELOW_200MA" in row["buy_warning_summary"]
    assert row["conviction_reason"] == "BELOW_200MA"
    assert row["conviction_grade"] == signal_engine._shift_grade(raw_conviction, -1)


def test_update_overlay_blocks_trend_regular_buy_when_ema_turns_down(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["PULLBACK"],
    }
    metrics = _metrics_for(_regular_pullback_daily(end="2026-03-11"), source_entry=source_entry)
    turned_metrics = dict(metrics)
    turned_metrics["ema_turn_down"] = True

    events = engine._trend_buy_events(
        symbol="AAA",
        metrics=turned_metrics,
        source_entry=source_entry,
        active_cycles={},
        peg_ready_map={},
        peg_context={},
    )

    assert not any(row["signal_code"] in {"TF_BUY_REGULAR", "TF_ADDON_PYRAMID"} for row in events)

    overlay_row = signal_engine._apply_update_overlay_rows(
        [
            {
                "symbol": "AAA",
                "engine": "TREND",
                "action_type": "WATCH",
                "signal_code": "TF_BUY_REGULAR",
                "conviction_grade": "A",
            }
        ],
        {"AAA": turned_metrics},
    )[0]
    assert "EMA_TURN_DOWN" in overlay_row["buy_warning_summary"]
    assert overlay_row["conviction_reason"] == "EMA_TURN_DOWN"
    assert overlay_row["conviction_grade"] == "D"


def test_update_overlay_assigns_top_sizing_for_a_green_buy() -> None:
    row = _overlay_buy_like_row(conviction_grade="A", market_condition_state="GREEN")

    assert row["sizing_tier"] == "TOP"
    assert row["sizing_status"] == "OK"
    assert row["sizing_reason"] == "GRADE_A|MARKET_GREEN"
    assert row["sizing_risk_budget_pct"] == pytest.approx(2.0)
    assert row["sizing_position_cap_pct"] == pytest.approx(20.0)
    assert row["sizing_stop_distance_pct"] == pytest.approx(5.0)
    assert row["sizing_raw_position_pct"] == pytest.approx(40.0)
    assert row["sizing_recommended_position_pct"] == pytest.approx(20.0)


def test_update_overlay_assigns_top_sizing_for_s_bullish_buy() -> None:
    row = _overlay_buy_like_row(conviction_grade="S", market_condition_state="BULLISH")

    assert row["sizing_tier"] == "TOP"
    assert row["sizing_status"] == "OK"
    assert row["sizing_reason"] == "GRADE_S|MARKET_BULLISH"
    assert row["sizing_risk_budget_pct"] == pytest.approx(2.0)
    assert row["sizing_position_cap_pct"] == pytest.approx(20.0)


def test_update_overlay_assigns_strong_sizing_for_b_green_buy() -> None:
    row = _overlay_buy_like_row(conviction_grade="B", market_condition_state="GREEN")

    assert row["sizing_tier"] == "STRONG"
    assert row["sizing_status"] == "OK"
    assert row["sizing_reason"] == "GRADE_B|MARKET_GREEN"
    assert row["sizing_risk_budget_pct"] == pytest.approx(1.5)
    assert row["sizing_position_cap_pct"] == pytest.approx(10.0)
    assert row["sizing_raw_position_pct"] == pytest.approx(30.0)
    assert row["sizing_recommended_position_pct"] == pytest.approx(10.0)


def test_update_overlay_falls_back_to_base_sizing_without_strong_market() -> None:
    row = _overlay_buy_like_row(conviction_grade="A", market_condition_state="UNKNOWN")

    assert row["sizing_tier"] == "BASE"
    assert row["sizing_status"] == "OK"
    assert row["sizing_reason"] == "BASE_FALLBACK"
    assert row["sizing_risk_budget_pct"] == pytest.approx(1.0)
    assert row["sizing_position_cap_pct"] == pytest.approx(10.0)
    assert row["sizing_raw_position_pct"] == pytest.approx(20.0)
    assert row["sizing_recommended_position_pct"] == pytest.approx(10.0)


def test_update_overlay_applies_sizing_to_trend_addon_buy() -> None:
    row = _overlay_buy_like_row(signal_code="TF_ADDON_PYRAMID", conviction_grade="B", market_condition_state="GREEN")

    assert row["action_type"] == "BUY"
    assert row["signal_code"] == "TF_ADDON_PYRAMID"
    assert row["sizing_tier"] == "STRONG"
    assert row["sizing_status"] == "OK"


def test_update_overlay_marks_invalid_stop_for_buy_sizing() -> None:
    row = _overlay_buy_like_row(conviction_grade="A", market_condition_state="GREEN", entry_price=100.0, stop_level=100.0)

    assert row["sizing_tier"] == "TOP"
    assert row["sizing_status"] == "INVALID_STOP"
    assert row["sizing_reason"] == "INVALID_STOP"
    assert row["sizing_stop_distance_pct"] is None
    assert row["sizing_raw_position_pct"] is None
    assert row["sizing_recommended_position_pct"] is None


def test_update_overlay_marks_non_buy_rows_with_non_buy_sizing_status() -> None:
    row = _overlay_buy_like_row(action_type="WATCH", conviction_grade="A", market_condition_state="GREEN")

    assert row["action_type"] == "WATCH"
    assert row["sizing_status"] == "NON_BUY"
    assert row["sizing_tier"] is None
    assert row["sizing_reason"] == "NON_BUY"
    assert row["sizing_risk_budget_pct"] is None
    assert row["sizing_recommended_position_pct"] is None


def test_peg_imminent_screener_marks_bullish_below_200_candidate(monkeypatch) -> None:
    monkeypatch.setattr(
        signal_engine,
        "load_local_ohlcv_frame",
        lambda market, symbol, **kwargs: _bullish_below_200_breakout_daily(end="2026-03-16").copy() if symbol == "AAA" else pd.DataFrame(),
    )
    screener = signal_engine.PEGImminentScreener(
        market="us",
        as_of_date="2026-03-16",
        upcoming_earnings_fetcher=lambda market, as_of_date, days: pd.DataFrame(),
    )

    rows = screener._compute_peg_ready([{"symbol": "AAA", "earnings_date": "2026-03-18"}])

    assert rows
    row = rows[0]
    assert row["symbol"] == "AAA"
    assert "BELOW_200MA" in row["reason_codes"]
    assert row["alignment_state"] == "BULLISH"


def test_run_signal_scan_v2_and_snapshot_include_overlay_fields(monkeypatch) -> None:
    base = cache_root("signal_engine_runtime")
    if base.exists():
        shutil.rmtree(base, ignore_errors=True)
    base.mkdir(parents=True, exist_ok=True)

    screeners_root = base / "screeners"
    signals_root = base / "signals"
    peg_root = screeners_root / "peg_imminent"
    (screeners_root / "qullamaggie").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(signal_engine, "get_market_screeners_root", lambda market: str(screeners_root))
    monkeypatch.setattr(signal_engine, "get_signal_engine_results_dir", lambda market: str(signals_root))
    monkeypatch.setattr(signal_engine, "get_peg_imminent_results_dir", lambda market: str(peg_root))
    monkeypatch.setattr(signal_engine, "ensure_market_dirs", lambda market, include_signal_dirs=False: None)
    monkeypatch.setattr(signal_engine, "_load_metadata_map", lambda market: {})
    monkeypatch.setattr(signal_engine, "_load_financial_map", lambda market, symbols=None: {})
    monkeypatch.setattr(
        signal_engine._market_intel_bridge,
        "load_leader_core_registry_entries",
        lambda *args, **kwargs: {},
    )
    monkeypatch.setattr(
        signal_engine._market_intel_bridge,
        "load_market_truth_snapshot",
        lambda *args, **kwargs: SimpleNamespace(
            market_alias="RISK_ON",
            regime_state="uptrend",
            top_state="risk_on",
            market_state="uptrend",
            breadth_state="broad_participation",
            concentration_state="diversified",
            leadership_state="growth_ai",
            market_alignment_score=82.0,
            breadth_support_score=78.0,
            rotation_support_score=86.0,
            leader_health_score=74.0,
            leader_health_status="HEALTHY",
        ),
    )
    monkeypatch.setattr(
        signal_engine,
        "load_local_ohlcv_frame",
        lambda market, symbol, **kwargs: _breakout_daily(end="2026-03-11").copy() if symbol == "AAA" else pd.DataFrame(),
    )

    pd.DataFrame([{"symbol": "AAA", "as_of_ts": "2026-03-11"}]).to_csv(
        screeners_root / "qullamaggie" / "daily_focus_list.csv",
        index=False,
    )

    result = signal_engine.run_multi_screener_signal_scan(
        market="us",
        as_of_date="2026-03-11",
        upcoming_earnings_fetcher=lambda market, as_of_date, days: pd.DataFrame(),
        earnings_collector=_StubEarningsCollector(),
    )

    v2_row = next(row for row in result["all_signals_v2"] if row["symbol"] == "AAA" and row["engine"] == "TREND")
    v2_buy_row = next(
        row
        for row in result["all_signals_v2"]
        if row["symbol"] == "AAA" and row["engine"] == "TREND" and row["action_type"] == "BUY"
    )
    snapshot_row = next(row for row in result["signal_universe_snapshot"] if row["symbol"] == "AAA")

    for row in (v2_row, snapshot_row):
        assert row["ema_alignment_state"]
        assert "market_condition_state" in row
        assert "pullback_quality" in row
        assert "buy_warning_summary" in row
        assert "aux_signal_summary" in row
        assert "conviction_reason" in row

    assert v2_buy_row["sizing_status"] == "OK"
    assert v2_buy_row["sizing_tier"] in {"BASE", "STRONG", "TOP"}
    assert "sizing_recommended_position_pct" in v2_buy_row
    assert "sizing_risk_budget_pct" in v2_buy_row
    assert "sizing_position_cap_pct" in v2_buy_row
    assert "sizing_status" not in snapshot_row

    expected_outputs = [
        signals_root / "all_signals_v2.csv",
        signals_root / "signal_universe_snapshot.csv",
        signals_root / "open_family_cycles.csv",
        signals_root / "signal_event_history.csv",
        signals_root / "signal_state_history.csv",
        signals_root / "source_registry_summary.json",
        signals_root / "signal_summary.json",
    ]
    for output_path in expected_outputs:
        assert output_path.exists(), output_path

    assert result["signal_summary"]["counts"]["signal_state_history"] > 0


def test_run_signal_scan_persists_state_history_separately_from_event_history(monkeypatch) -> None:
    base = cache_root("signal_engine_state_history_runtime")
    if base.exists():
        shutil.rmtree(base, ignore_errors=True)
    base.mkdir(parents=True, exist_ok=True)

    screeners_root = base / "screeners"
    signals_root = base / "signals"
    peg_root = screeners_root / "peg_imminent"
    (screeners_root / "qullamaggie").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(signal_engine, "get_market_screeners_root", lambda market: str(screeners_root))
    monkeypatch.setattr(signal_engine, "get_signal_engine_results_dir", lambda market: str(signals_root))
    monkeypatch.setattr(signal_engine, "get_peg_imminent_results_dir", lambda market: str(peg_root))
    monkeypatch.setattr(signal_engine, "ensure_market_dirs", lambda market, include_signal_dirs=False: None)
    monkeypatch.setattr(signal_engine, "_load_metadata_map", lambda market: {})
    monkeypatch.setattr(signal_engine, "_load_financial_map", lambda market, symbols=None: {})
    monkeypatch.setattr(
        signal_engine._market_intel_bridge,
        "load_leader_core_registry_entries",
        lambda *args, **kwargs: {},
    )
    monkeypatch.setattr(
        signal_engine._market_intel_bridge,
        "load_market_truth_snapshot",
        lambda *args, **kwargs: SimpleNamespace(
            market_alias="RISK_ON",
            regime_state="uptrend",
            top_state="risk_on",
            market_state="uptrend",
            breadth_state="broad_participation",
            concentration_state="diversified",
            leadership_state="growth_ai",
            market_alignment_score=82.0,
            breadth_support_score=78.0,
            rotation_support_score=86.0,
            leader_health_score=74.0,
            leader_health_status="HEALTHY",
        ),
    )
    monkeypatch.setattr(
        signal_engine,
        "load_local_ohlcv_frame",
        lambda market, symbol, **kwargs: _breakout_daily(end="2026-03-11").copy() if symbol == "AAA" else pd.DataFrame(),
    )

    pd.DataFrame([{"symbol": "AAA", "as_of_ts": "2026-03-11"}]).to_csv(
        screeners_root / "qullamaggie" / "daily_focus_list.csv",
        index=False,
    )

    signal_engine.run_multi_screener_signal_scan(
        market="us",
        as_of_date="2026-03-11",
        upcoming_earnings_fetcher=lambda market, as_of_date, days: pd.DataFrame(),
        earnings_collector=_StubEarningsCollector(),
    )

    event_history = pd.read_csv(signals_root / "signal_event_history.csv")
    state_history = pd.read_csv(signals_root / "signal_state_history.csv")

    assert set(event_history["signal_kind"]) == {"EVENT"}
    assert set(state_history["signal_kind"]) <= {"STATE", "AUX"}
    assert "UG_COMBO_TREND" in set(state_history["signal_code"])
    assert "TF_TRAILING_LEVEL" in set(state_history["signal_code"])
    assert "UG_STATE_GREEN" in set(state_history["signal_code"])


def test_state_history_persistence_includes_state_aux_alert_and_combo_rows(monkeypatch) -> None:
    base = cache_root("signal_engine_state_history_unit")
    if base.exists():
        shutil.rmtree(base, ignore_errors=True)
    base.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(signal_engine, "get_signal_engine_results_dir", lambda market: str(base))
    monkeypatch.setattr(signal_engine, "_load_metadata_map", lambda market: {})
    monkeypatch.setattr(signal_engine, "_load_financial_map", lambda market, symbols=None: {})
    monkeypatch.setattr(
        signal_engine._market_intel_bridge,
        "load_leader_core_registry_entries",
        lambda *args, **kwargs: {},
    )
    engine = signal_engine.MultiScreenerSignalEngine(market="us", as_of_date="2026-03-31")

    event_row = signal_engine._build_signal_row(
        signal_date="2026-03-31",
        symbol="AAA",
        market="us",
        engine="TREND",
        family="TF_BREAKOUT",
        signal_kind="EVENT",
        signal_code="TF_BUY_BREAKOUT",
        action_type="BUY",
        conviction_grade="B",
        screen_stage="TEST",
    )
    state_rows = [
        signal_engine._build_signal_row(
            signal_date="2026-03-31",
            symbol="AAA",
            market="us",
            engine="TREND",
            family="TF_BREAKOUT",
            signal_kind="STATE",
            signal_code="TF_AGGRESSIVE_ALERT",
            action_type="ALERT",
            conviction_grade="C",
            screen_stage="TEST",
        ),
        signal_engine._build_signal_row(
            signal_date="2026-03-31",
            symbol="AAA",
            market="us",
            engine="TREND",
            family="TF_BREAKOUT",
            signal_kind="STATE",
            signal_code="TF_TRAILING_LEVEL",
            action_type="STATE",
            conviction_grade="C",
            screen_stage="TEST",
        ),
        signal_engine._build_signal_row(
            signal_date="2026-03-31",
            symbol="AAA",
            market="us",
            engine="TREND",
            family="TF_BREAKOUT",
            signal_kind="STATE",
            signal_code="TF_ADDON_READY",
            action_type="STATE",
            conviction_grade="C",
            screen_stage="TEST",
        ),
        signal_engine._build_signal_row(
            signal_date="2026-03-31",
            symbol="AAA",
            market="us",
            engine="UG",
            family="UG_STATE",
            signal_kind="STATE",
            signal_code="UG_STATE_GREEN",
            action_type="STATE",
            conviction_grade="B",
            screen_stage="TEST",
        ),
        signal_engine._build_signal_row(
            signal_date="2026-03-31",
            symbol="BBB",
            market="us",
            engine="UG",
            family="UG_STATE",
            signal_kind="STATE",
            signal_code="UG_STATE_ORANGE",
            action_type="STATE",
            conviction_grade="C",
            screen_stage="TEST",
        ),
        signal_engine._build_signal_row(
            signal_date="2026-03-31",
            symbol="CCC",
            market="us",
            engine="UG",
            family="UG_STATE",
            signal_kind="STATE",
            signal_code="UG_STATE_RED",
            action_type="STATE",
            conviction_grade="D",
            screen_stage="TEST",
        ),
        signal_engine._build_signal_row(
            signal_date="2026-03-31",
            symbol="AAA",
            market="us",
            engine="UG",
            family="UG_STRATEGY",
            signal_kind="STATE",
            signal_code="UG_COMBO_TREND",
            action_type="STATE",
            conviction_grade="B",
            screen_stage="TEST",
            strategy_combo="UG_COMBO_TREND",
        ),
        signal_engine._build_signal_row(
            signal_date="2026-03-31",
            symbol="BBB",
            market="us",
            engine="UG",
            family="UG_STRATEGY",
            signal_kind="STATE",
            signal_code="UG_COMBO_PULLBACK",
            action_type="STATE",
            conviction_grade="C",
            screen_stage="TEST",
            strategy_combo="UG_COMBO_PULLBACK",
        ),
        signal_engine._build_signal_row(
            signal_date="2026-03-31",
            symbol="CCC",
            market="us",
            engine="UG",
            family="UG_STRATEGY",
            signal_kind="STATE",
            signal_code="UG_COMBO_SQUEEZE",
            action_type="STATE",
            conviction_grade="C",
            screen_stage="TEST",
            strategy_combo="UG_COMBO_SQUEEZE",
        ),
    ]

    engine._persist_signal_history([], [event_row, *state_rows])
    engine._persist_state_history([], state_rows)

    event_history = pd.read_csv(base / "signal_event_history.csv")
    state_history = pd.read_csv(base / "signal_state_history.csv")

    assert set(event_history["signal_code"]) == {"TF_BUY_BREAKOUT"}
    assert set(state_history["signal_code"]) == {
        "TF_AGGRESSIVE_ALERT",
        "TF_TRAILING_LEVEL",
        "TF_ADDON_READY",
        "UG_STATE_GREEN",
        "UG_STATE_ORANGE",
        "UG_STATE_RED",
        "UG_COMBO_TREND",
        "UG_COMBO_PULLBACK",
        "UG_COMBO_SQUEEZE",
    }


def test_signal_code_boundary_helpers_separate_active_legacy_and_reference_exit_strings() -> None:
    assert signal_engine._is_active_signal_code("TF_SELL_RESISTANCE_REJECT") is True
    assert signal_engine._is_active_signal_code("TF_SELL_PBS") is False
    assert signal_engine._is_active_signal_code("TF_SELL_SUB200") is False
    assert signal_engine._is_active_signal_code("UG_SELL_MR_SHORT_OR_PBS") is False
    assert signal_engine._is_reference_exit_helper_literal("UG_SELL_MR_SHORT_OR_PBS") is True



def test_load_metadata_map_skips_blank_symbol_rows(monkeypatch) -> None:
    base = cache_root("signal_engine_metadata_map")
    if base.exists():
        shutil.rmtree(base, ignore_errors=True)
    base.mkdir(parents=True, exist_ok=True)
    metadata_path = base / "stock_metadata_us.csv"
    pd.DataFrame(
        [
            {"symbol": "AAPL", "sector": "Tech"},
            {"symbol": None, "sector": "Ignore"},
        ]
    ).to_csv(metadata_path, index=False)

    monkeypatch.setattr(
        signal_engine, "get_stock_metadata_path", lambda market: str(metadata_path)
    )

    metadata_map = signal_engine._load_metadata_map("us")

    assert set(metadata_map) == {"AAPL"}
    assert metadata_map["AAPL"]["sector"] == "Tech"


def test_load_financial_map_uses_filename_stem_when_cached_symbol_missing(
    monkeypatch,
) -> None:
    cache_dir = cache_root("signal_engine_financial_map")
    if cache_dir.exists():
        shutil.rmtree(cache_dir, ignore_errors=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"symbol": None, "provider_symbol": None, "sales_growth_qoq": 12.5},
        ]
    ).to_csv(cache_dir / "005930.csv", index=False)

    monkeypatch.setattr(
        signal_engine, "get_financial_cache_dir", lambda market: str(cache_dir)
    )

    financial_map = signal_engine._load_financial_map("kr", symbols=["005930"])

    assert set(financial_map) == {"005930"}
    assert financial_map["005930"]["symbol"] == "005930"
    assert financial_map["005930"]["sales_growth_qoq"] == 12.5


def _ug_sell_metrics(*, date: str = "2026-03-11") -> dict[str, object]:
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["BREAKOUT"],
    }
    metrics = _metrics_for(_breakout_daily(end=date), source_entry=source_entry)
    updated = dict(metrics)
    updated["date"] = date
    updated["screen_stage"] = "TEST"
    updated["source_tags"] = ["QM_DAILY"]
    close_value = max(signal_engine._safe_float(updated.get("close")) or 0.0, 85.0)
    updated["close"] = close_value
    updated["high"] = max(signal_engine._safe_float(updated.get("high")) or 0.0, close_value)
    updated["ug_pbs_ready"] = False
    updated["ug_mr_short_ready"] = True
    return updated


def _ug_cycle(
    *,
    family: str = "UG_PULLBACK",
    reference_exit_signal: str = "UG_SELL_MR_SHORT_OR_PBS",
    trim_count: int = 0,
    last_trim_date: str = "",
    partial_exit_active: bool = False,
    current_position_units: float = 1.0,
) -> dict[str, object]:
    return {
        "family_cycle_id": f"UG:{family}:AAA:2026-03-10",
        "engine": "UG",
        "family": family,
        "symbol": "AAA",
        "opened_on": "2026-03-10",
        "last_signal_date": "2026-03-10",
        "buy_signal_code": "UG_BUY_PBB" if family == "UG_PULLBACK" else "UG_BUY_BREAKOUT",
        "screen_stage": "TEST",
        "entry_price": 80.0,
        "support_zone_low": 74.0,
        "support_zone_high": 82.0,
        "stop_level": 74.0,
        "source_tags": ["QM_DAILY"],
        "primary_source_style": "BREAKOUT",
        "source_fit_score": 90.0,
        "source_fit_label": "HIGH",
        "reference_exit_signal": reference_exit_signal,
        "trim_count": trim_count,
        "last_trim_date": last_trim_date,
        "partial_exit_active": partial_exit_active,
        "base_position_units": 1.0,
        "current_position_units": current_position_units,
    }


def _trend_sell_metrics(
    *,
    date: str = "2026-03-11",
    family: str = "TF_BREAKOUT",
) -> dict[str, object]:
    source_style = "PULLBACK" if family == "TF_REGULAR_PULLBACK" else "BREAKOUT"
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": [source_style],
    }
    base_frame = (
        _regular_pullback_daily(end=date)
        if family == "TF_REGULAR_PULLBACK"
        else _breakout_daily(end=date)
    )
    metrics = _metrics_for(base_frame, source_entry=source_entry)
    updated = dict(metrics)
    updated["date"] = date
    updated["screen_stage"] = "TEST"
    updated["source_tags"] = ["QM_DAILY"]
    return updated


def _trend_cycle(
    *,
    family: str = "TF_BREAKOUT",
    tp1_level: float = 90.0,
    tp2_level: float = 95.0,
    trailing_level: float = 70.0,
    protected_stop_level: float = 70.0,
    support_zone_low: float = 60.0,
    support_zone_high: float = 92.0,
    risk_free_armed: bool = False,
    tp1_hit: bool = False,
    tp2_hit: bool = False,
    trim_count: int = 0,
) -> dict[str, object]:
    return {
        "family_cycle_id": f"TREND:{family}:AAA:2026-03-10",
        "engine": "TREND",
        "family": family,
        "symbol": "AAA",
        "opened_on": "2026-03-10",
        "last_signal_date": "2026-03-10",
        "buy_signal_code": "TF_BUY_BREAKOUT",
        "screen_stage": "TEST",
        "entry_price": 80.0,
        "break_even_level": 80.0,
        "support_zone_low": support_zone_low,
        "support_zone_high": support_zone_high,
        "stop_level": support_zone_low,
        "trailing_level": trailing_level,
        "trailing_mode": "BREAKOUT_FAST",
        "tp1_level": tp1_level,
        "tp2_level": tp2_level,
        "tp_plan": "TP_BREAKOUT_2R_3R",
        "tp1_hit": tp1_hit,
        "tp2_hit": tp2_hit,
        "trim_count": trim_count,
        "last_trim_date": "",
        "partial_exit_active": trim_count > 0,
        "risk_free_armed": risk_free_armed,
        "add_on_count": 0,
        "add_on_slot": 0,
        "max_add_ons": 2,
        "tranche_pct": None,
        "next_addon_allowed": False,
        "last_addon_date": "",
        "pyramid_state": "INITIAL",
        "protected_stop_level": protected_stop_level,
        "base_position_units": 1.0,
        "current_position_units": 1.0,
        "blended_entry_price": 80.0,
        "last_trailing_confirmed_level": trailing_level,
        "last_protected_stop_level": protected_stop_level,
        "last_pyramid_reference_level": protected_stop_level,
        "source_tags": ["QM_DAILY"],
        "primary_source_style": "PULLBACK" if family == "TF_REGULAR_PULLBACK" else "BREAKOUT",
        "source_fit_score": 90.0,
        "source_fit_label": "HIGH",
    }


def test_update_cycles_persists_reference_exit_signal_for_ug_buy_rows(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    metrics = _ug_sell_metrics()
    buy_row = signal_engine._build_signal_row(
        signal_date="2026-03-11",
        symbol="AAA",
        market="us",
        engine="UG",
        family="UG_PULLBACK",
        signal_kind="EVENT",
        signal_code="UG_BUY_PBB",
        action_type="BUY",
        conviction_grade="B",
        screen_stage="TEST",
        support_zone_low=74.0,
        support_zone_high=82.0,
        stop_level=74.0,
        source_tags=["QM_DAILY"],
        primary_source_style="BREAKOUT",
        source_fit_score=90.0,
        source_fit_label="HIGH",
        reference_exit_signal="UG_SELL_MR_SHORT_OR_PBS",
    )

    updated = engine._update_cycles(
        [buy_row],
        {},
        {"AAA": metrics},
    )

    cycle = updated[("UG", "UG_PULLBACK", "AAA")]
    assert cycle["reference_exit_signal"] == "UG_SELL_MR_SHORT_OR_PBS"


def test_update_cycles_ignores_reference_exit_signal_for_non_pullback_ug_buy_rows(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    metrics = _ug_sell_metrics()
    buy_row = signal_engine._build_signal_row(
        signal_date="2026-03-11",
        symbol="AAA",
        market="us",
        engine="UG",
        family="UG_BREAKOUT",
        signal_kind="EVENT",
        signal_code="UG_BUY_BREAKOUT",
        action_type="BUY",
        conviction_grade="B",
        screen_stage="TEST",
        support_zone_low=74.0,
        support_zone_high=82.0,
        stop_level=74.0,
        source_tags=["QM_DAILY"],
        primary_source_style="BREAKOUT",
        source_fit_score=90.0,
        source_fit_label="HIGH",
        reference_exit_signal="UG_SELL_MR_SHORT_OR_PBS",
    )

    updated = engine._update_cycles(
        [buy_row],
        {},
        {"AAA": metrics},
    )

    cycle = updated[("UG", "UG_BREAKOUT", "AAA")]
    assert cycle["reference_exit_signal"] == ""


def test_ug_mr_short_trims_twice_and_waits_for_pbs_exit(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    key: tuple[str, str, str] = ("UG", "UG_PULLBACK", "AAA")
    active_cycles: dict[tuple[str, str, str], dict[str, Any]] = {key: _ug_cycle()}

    first_metrics = _ug_sell_metrics(date="2026-03-11")
    first_events = engine._ug_sell_events(
        symbol="AAA",
        metrics=first_metrics,
        cycle=active_cycles[key],
    )
    first_row = next(row for row in first_events if row["signal_code"] == "UG_SELL_MR_SHORT")

    assert first_row["action_type"] == "TRIM"

    updated_cycles = engine._update_cycles(first_events, active_cycles, {"AAA": first_metrics})
    updated_cycle = updated_cycles[key]
    assert updated_cycle["trim_count"] == 1
    assert updated_cycle["partial_exit_active"] is True
    assert updated_cycle["last_trim_date"] == "2026-03-11"
    assert updated_cycle["current_position_units"] == 0.5

    second_metrics = _ug_sell_metrics(date="2026-03-12")
    second_events = engine._ug_sell_events(
        symbol="AAA",
        metrics=second_metrics,
        cycle=updated_cycle,
    )
    second_row = next(row for row in second_events if row["signal_code"] == "UG_SELL_MR_SHORT")

    assert second_row["action_type"] == "TRIM"

    twice_trimmed_cycles = engine._update_cycles(
        second_events,
        updated_cycles,
        {"AAA": second_metrics},
    )
    twice_trimmed_cycle = twice_trimmed_cycles[key]
    assert twice_trimmed_cycle["trim_count"] == 2
    assert twice_trimmed_cycle["current_position_units"] == 0.25

    third_metrics = _ug_sell_metrics(date="2026-03-13")
    third_events = engine._ug_sell_events(
        symbol="AAA",
        metrics=third_metrics,
        cycle=twice_trimmed_cycle,
    )

    assert not any(row["signal_code"] == "UG_SELL_MR_SHORT" for row in third_events)

    exit_metrics = dict(third_metrics)
    exit_metrics["date"] = "2026-03-14"
    exit_metrics["ug_mr_short_ready"] = False
    exit_metrics["ug_pbs_ready"] = True
    exit_events = engine._ug_sell_events(
        symbol="AAA",
        metrics=exit_metrics,
        cycle=twice_trimmed_cycle,
    )
    exit_row = next(row for row in exit_events if row["signal_code"] == "UG_SELL_PBS")

    assert exit_row["action_type"] == "EXIT"

    final_cycles = engine._update_cycles(exit_events, twice_trimmed_cycles, {"AAA": exit_metrics})
    assert key not in final_cycles


def test_ug_breakdown_exit_closes_cycle(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    key: tuple[str, str, str] = ("UG", "UG_PULLBACK", "AAA")
    active_cycles: dict[tuple[str, str, str], dict[str, Any]] = {key: _ug_cycle(reference_exit_signal="")}
    metrics = _ug_sell_metrics(date="2026-03-15")
    metrics["close"] = 73.5
    metrics["high"] = 74.0
    metrics["ug_mr_short_ready"] = False
    metrics["ug_pbs_ready"] = False

    events = engine._ug_sell_events(symbol="AAA", metrics=metrics, cycle=active_cycles[key])
    row = next(row for row in events if row["signal_code"] == "UG_SELL_BREAKDOWN")

    assert row["action_type"] == "EXIT"

    updated = engine._update_cycles(events, active_cycles, {"AAA": metrics})
    assert key not in updated


def test_ug_pbs_is_allowed_without_reference_exit_hint(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    key: tuple[str, str, str] = ("UG", "UG_BREAKOUT", "AAA")
    active_cycles: dict[tuple[str, str, str], dict[str, Any]] = {key: _ug_cycle(family="UG_BREAKOUT", reference_exit_signal="")}
    metrics = _ug_sell_metrics(date="2026-03-16")
    metrics["ug_mr_short_ready"] = False
    metrics["ug_pbs_ready"] = True
    metrics["bb_mid"] = 84.0
    metrics["high"] = 85.0
    metrics["close"] = 83.5

    events = engine._ug_sell_events(symbol="AAA", metrics=metrics, cycle=active_cycles[key])
    row = next(row for row in events if row["signal_code"] == "UG_SELL_PBS")

    assert row["action_type"] == "EXIT"

    updated = engine._update_cycles(events, active_cycles, {"AAA": metrics})
    assert key not in updated


def test_ug_non_pullback_reference_exit_hint_does_not_gate_sell_events(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    key: tuple[str, str, str] = ("UG", "UG_BREAKOUT", "AAA")
    active_cycles: dict[tuple[str, str, str], dict[str, Any]] = {key: _ug_cycle(family="UG_BREAKOUT", reference_exit_signal="UG_SELL_PBS")}
    metrics = _ug_sell_metrics(date="2026-03-17")
    metrics["ug_mr_short_ready"] = True
    metrics["ug_pbs_ready"] = False

    events = engine._ug_sell_events(symbol="AAA", metrics=metrics, cycle=active_cycles[key])

    assert any(row["signal_code"] == "UG_SELL_MR_SHORT" for row in events)


def test_ug_pullback_noncanonical_reference_exit_hint_is_ignored(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    key: tuple[str, str, str] = ("UG", "UG_PULLBACK", "AAA")
    active_cycles: dict[tuple[str, str, str], dict[str, Any]] = {key: _ug_cycle(reference_exit_signal="UG_SELL_PBS")}
    metrics = _ug_sell_metrics(date="2026-03-18")
    metrics["ug_mr_short_ready"] = True
    metrics["ug_pbs_ready"] = False

    events = engine._ug_sell_events(symbol="AAA", metrics=metrics, cycle=active_cycles[key])

    assert any(row["signal_code"] == "UG_SELL_MR_SHORT" for row in events)


def test_ug_trim_state_survives_open_cycle_round_trip(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    cycle = _ug_cycle(
        trim_count=1,
        last_trim_date="2026-03-11",
        partial_exit_active=True,
        current_position_units=0.5,
    )

    sanitized = signal_engine._sanitize_cycle_row(cycle)
    hydrated = engine._hydrate_loaded_cycle(sanitized)

    assert hydrated["trim_count"] == 1
    assert hydrated["last_trim_date"] == "2026-03-11"
    assert hydrated["partial_exit_active"] is True
    assert hydrated["current_position_units"] == 0.5
    assert hydrated["reference_exit_signal"] == "UG_SELL_MR_SHORT_OR_PBS"


def test_trend_tp_hits_update_cycle_state(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    key: tuple[str, str, str] = ("TREND", "TF_BREAKOUT", "AAA")
    active_cycles: dict[tuple[str, str, str], dict[str, Any]] = {key: _trend_cycle()}
    metrics = _trend_sell_metrics(date="2026-03-11", family="TF_BREAKOUT")
    metrics["high"] = 96.0
    metrics["close"] = 94.0
    metrics["in_channel8"] = False
    metrics["channel_low8"] = 55.0

    events = engine._trend_sell_events(symbol="AAA", metrics=metrics, cycle=active_cycles[key])
    signal_codes = {row["signal_code"] for row in events}

    assert {"TF_SELL_TP1", "TF_SELL_TP2"} <= signal_codes

    updated = engine._update_cycles(events, active_cycles, {"AAA": metrics})
    cycle = updated[key]
    assert cycle["tp1_hit"] is True
    assert cycle["tp2_hit"] is True
    assert cycle["trim_count"] == 2
    assert cycle["risk_free_armed"] is True


def test_trend_trailing_break_closes_cycle(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    key: tuple[str, str, str] = ("TREND", "TF_BREAKOUT", "AAA")
    active_cycles: dict[tuple[str, str, str], dict[str, Any]] = {key: _trend_cycle(tp1_level=120.0, tp2_level=130.0, trailing_level=90.0, protected_stop_level=90.0)}
    metrics = _trend_sell_metrics(date="2026-03-12", family="TF_BREAKOUT")
    metrics["close"] = 89.0
    metrics["high"] = 89.5
    metrics["in_channel8"] = False
    metrics["channel_low8"] = 70.0
    metrics["bb_mid"] = 80.0
    metrics["daily_return_pct"] = 1.0

    events = engine._trend_sell_events(symbol="AAA", metrics=metrics, cycle=active_cycles[key])
    row = next(row for row in events if row["signal_code"] == "TF_SELL_TRAILING_BREAK")

    assert row["action_type"] == "SELL"

    updated = engine._update_cycles(events, active_cycles, {"AAA": metrics})
    assert key not in updated


def test_trend_momentum_end_closes_cycle(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    key: tuple[str, str, str] = ("TREND", "TF_MOMENTUM", "AAA")
    active_cycles: dict[tuple[str, str, str], dict[str, Any]] = {key: _trend_cycle(family="TF_MOMENTUM", tp1_level=120.0, tp2_level=130.0)}
    metrics = _trend_sell_metrics(date="2026-03-24", family="TF_BREAKOUT")
    metrics["rsi14"] = 49.0
    metrics["macd_hist"] = -0.2
    metrics["ema10"] = 85.0
    metrics["fast_ref"] = 85.0
    metrics["close"] = 84.0
    metrics["high"] = 84.5
    metrics["in_channel8"] = False
    metrics["channel_low8"] = 70.0
    metrics["bb_mid"] = 75.0
    metrics["daily_return_pct"] = 0.5

    events = engine._trend_sell_events(symbol="AAA", metrics=metrics, cycle=active_cycles[key])
    row = next(row for row in events if row["signal_code"] == "TF_SELL_MOMENTUM_END")

    assert row["action_type"] == "SELL"

    updated = engine._update_cycles(events, active_cycles, {"AAA": metrics})
    assert key not in updated


def test_trend_resistance_reject_uses_canonical_code(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    key: tuple[str, str, str] = ("TREND", "TF_REGULAR_PULLBACK", "AAA")
    active_cycles: dict[tuple[str, str, str], dict[str, Any]] = {
        key: _trend_cycle(
            family="TF_REGULAR_PULLBACK",
            tp1_level=120.0,
            tp2_level=130.0,
            trailing_level=70.0,
            protected_stop_level=70.0,
        )
    }
    metrics = _trend_sell_metrics(date="2026-03-13", family="TF_REGULAR_PULLBACK")
    metrics["close"] = 79.0
    metrics["high"] = 82.0
    metrics["bb_mid"] = 80.0
    metrics["daily_return_pct"] = -1.0
    metrics["in_channel8"] = False
    metrics["channel_low8"] = 72.0

    events = engine._trend_sell_events(symbol="AAA", metrics=metrics, cycle=active_cycles[key])
    row = next(row for row in events if row["signal_code"] == "TF_SELL_RESISTANCE_REJECT")

    assert row["action_type"] == "SELL"
    assert not any(candidate["signal_code"] == "TF_SELL_PBS" for candidate in events)

    updated = engine._update_cycles(events, active_cycles, {"AAA": metrics})
    assert key not in updated


def test_tf_sell_contract_uses_resistance_reject_only() -> None:
    row = signal_engine._build_signal_row(
        signal_date="2026-03-11",
        symbol="AAA",
        market="us",
        engine="TREND",
        family="TF_REGULAR_PULLBACK",
        signal_kind="EVENT",
        signal_code="TF_SELL_RESISTANCE_REJECT",
        action_type="SELL",
        conviction_grade="C",
        screen_stage="TEST",
    )

    transformed = signal_engine._transform_signal_rows([row])[0]

    assert transformed["signal_code"] == "TF_SELL_RESISTANCE_REJECT"
    assert signal_engine._signal_code_label("TF_SELL_PBS") == "TF_SELL_PBS"
    assert signal_engine._signal_code_label("TF_SELL_SUB200") == "TF_SELL_SUB200"


def test_trend_regular_buy_emits_signal(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["PULLBACK"],
    }
    metrics = _metrics_for(_regular_pullback_daily(end="2026-03-31"), source_entry=source_entry)

    events = engine._trend_buy_events(
        symbol="AAA",
        metrics=metrics,
        source_entry=source_entry,
        active_cycles={},
        peg_ready_map={},
        peg_context={},
    )

    assert any(row["signal_code"] == "TF_BUY_REGULAR" for row in events)


def test_trend_breakout_and_momentum_buy_signals_can_coexist(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["BREAKOUT"],
    }
    metrics = _metrics_for(_breakout_daily(end="2026-03-31"), source_entry=source_entry)

    events = engine._trend_buy_events(
        symbol="AAA",
        metrics=metrics,
        source_entry=source_entry,
        active_cycles={},
        peg_ready_map={},
        peg_context={},
    )

    signal_codes = {row["signal_code"] for row in events}
    assert {"TF_BUY_BREAKOUT", "TF_BUY_MOMENTUM"} <= signal_codes


def test_trend_addon_pyramid_emits_when_addon_context_is_ready(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["BREAKOUT"],
    }
    metrics = _metrics_for(_breakout_daily(end="2026-03-31"), source_entry=source_entry)
    metrics = dict(metrics)
    metrics.update(
        {
            "date": "2026-03-31",
            "close": 92.0,
            "low": 89.0,
            "build_up_ready": True,
            "setup_active": True,
            "in_channel8": False,
            "ema_turn_down": False,
            "alignment_state": "BULLISH",
        }
    )
    cycle = _trend_cycle(
        family="TF_BREAKOUT",
        trailing_level=70.0,
        protected_stop_level=70.0,
    )
    cycle.update(
        {
            "last_trailing_confirmed_level": 70.0,
            "last_protected_stop_level": 70.0,
            "last_pyramid_reference_level": 70.0,
            "add_on_count": 0,
            "add_on_slot": 0,
            "max_add_ons": 2,
            "break_even_level": 80.0,
            "blended_entry_price": 80.0,
            "current_position_units": 1.0,
            "base_position_units": 1.0,
        }
    )

    events = engine._trend_buy_events(
        symbol="AAA",
        metrics=metrics,
        source_entry=source_entry,
        active_cycles={("TREND", "TF_BREAKOUT", "AAA"): cycle},
        peg_ready_map={},
        peg_context={},
    )

    assert any(row["signal_code"] == "TF_ADDON_PYRAMID" for row in events)


def test_trend_peg_event_rows_emit_alert_and_watch(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY", "PEG_READY"],
        "source_style_tags": ["PEG"],
    }
    metrics = _metrics_for(_breakout_daily(end="2026-03-31"), source_entry=source_entry)

    confirmed = engine._trend_buy_events(
        symbol="AAA",
        metrics=metrics,
        source_entry=source_entry,
        active_cycles={},
        peg_ready_map={"AAA": True},
        peg_context={"event_day": True, "event_confirmed": True, "gap_low": 90.0, "half_gap": 94.0},
    )
    missed = engine._trend_buy_events(
        symbol="AAA",
        metrics=metrics,
        source_entry=source_entry,
        active_cycles={},
        peg_ready_map={"AAA": True},
        peg_context={"event_day": True, "missed": True},
    )

    assert any(
        row["signal_code"] == "TF_PEG_EVENT" and row["action_type"] == "ALERT"
        for row in confirmed
    )
    assert any(
        row["signal_code"] == "TF_PEG_EVENT" and row["action_type"] == "WATCH"
        for row in missed
    )


def test_trend_peg_pullback_buy_emits_signal(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY", "PEG_READY"],
        "source_style_tags": ["PEG"],
    }
    metrics = _metrics_for(_breakout_daily(end="2026-03-31"), source_entry=source_entry)
    metrics = dict(metrics)
    metrics.update(
        {
            "date": "2026-03-31",
            "low": 91.0,
            "close": 95.0,
            "ema_turn_down": False,
            "alignment_state": "BULLISH",
        }
    )

    events = engine._trend_buy_events(
        symbol="AAA",
        metrics=metrics,
        source_entry=source_entry,
        active_cycles={},
        peg_ready_map={"AAA": True},
        peg_context={"peg_active": True, "gap_low": 90.0, "half_gap": 94.0, "event_high": 100.0},
    )

    assert any(row["signal_code"] == "TF_BUY_PEG_PULLBACK" for row in events)


def test_trend_peg_rebreak_buy_emits_signal(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY", "PEG_READY"],
        "source_style_tags": ["PEG"],
    }
    metrics = _metrics_for(_breakout_daily(end="2026-03-31"), source_entry=source_entry)
    metrics = dict(metrics)
    metrics.update(
        {
            "date": "2026-03-31",
            "close": 100.5,
            "low": 96.0,
            "ema_turn_down": False,
            "alignment_state": "BULLISH",
        }
    )

    events = engine._trend_buy_events(
        symbol="AAA",
        metrics=metrics,
        source_entry=source_entry,
        active_cycles={},
        peg_ready_map={"AAA": True},
        peg_context={"peg_active": True, "gap_low": 90.0, "half_gap": 94.0, "event_high": 100.0},
    )

    assert any(row["signal_code"] == "TF_BUY_PEG_REBREAK" for row in events)


def test_trend_breakdown_sell_emits_without_channel_break_when_only_support_fails(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    cycle = _trend_cycle(
        family="TF_BREAKOUT",
        support_zone_low=60.0,
        support_zone_high=92.0,
        trailing_level=40.0,
        protected_stop_level=40.0,
        tp1_level=120.0,
        tp2_level=130.0,
    )
    metrics = _trend_sell_metrics(date="2026-03-31", family="TF_BREAKOUT")
    metrics.update({"close": 59.0, "high": 60.0, "channel_low8": 55.0, "in_channel8": False})

    events = engine._trend_sell_events(symbol="AAA", metrics=metrics, cycle=cycle)
    signal_codes = {row["signal_code"] for row in events}

    assert "TF_SELL_BREAKDOWN" in signal_codes
    assert "TF_SELL_CHANNEL_BREAK" not in signal_codes


def test_trend_channel_break_sell_emits_without_breakdown_when_only_channel_fails(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    cycle = _trend_cycle(
        family="TF_BREAKOUT",
        support_zone_low=50.0,
        support_zone_high=92.0,
        trailing_level=40.0,
        protected_stop_level=40.0,
        tp1_level=120.0,
        tp2_level=130.0,
    )
    metrics = _trend_sell_metrics(date="2026-03-31", family="TF_BREAKOUT")
    metrics.update({"close": 60.5, "high": 60.8, "channel_low8": 61.0, "in_channel8": False})

    events = engine._trend_sell_events(symbol="AAA", metrics=metrics, cycle=cycle)
    signal_codes = {row["signal_code"] for row in events}

    assert "TF_SELL_CHANNEL_BREAK" in signal_codes
    assert "TF_SELL_BREAKDOWN" not in signal_codes


def test_trend_state_rows_emit_setup_build_up_vcp_and_aggressive_alert(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["BREAKOUT"],
    }
    metrics = _metrics_for(_breakout_daily(end="2026-03-31"), source_entry=source_entry)
    metrics = dict(metrics)
    metrics.update(
        {
            "date": "2026-03-31",
            "setup_active": True,
            "vcp_active": True,
            "build_up_ready": True,
            "aggressive_ready": True,
            "in_channel8": True,
            "volume_dry": True,
            "near_high_ready": True,
            "squeeze_active": True,
            "tight_active": True,
        }
    )

    rows = engine._trend_state_rows(symbol="AAA", metrics=metrics, source_entry=source_entry)
    signal_codes = {row["signal_code"] for row in rows}

    assert {
        "TF_SETUP_ACTIVE",
        "TF_VCP_ACTIVE",
        "TF_BUILDUP_READY",
        "TF_AGGRESSIVE_ALERT",
    } <= signal_codes


def test_trend_level_and_addon_state_rows_emit_from_active_cycle(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["BREAKOUT"],
    }
    metrics = _metrics_for(_breakout_daily(end="2026-03-31"), source_entry=source_entry)
    cycle = _trend_cycle(
        family="TF_BREAKOUT",
        trailing_level=85.0,
        protected_stop_level=86.0,
        risk_free_armed=True,
        trim_count=1,
    )
    cycle.update(
        {
            "next_addon_allowed": True,
            "addon_next_slot": 1,
            "addon_tranche_pct": 0.5,
            "addon_reason_codes": ["ADDON_READY"],
            "add_on_count": 0,
        }
    )
    active_cycles: dict[tuple[str, str, str], dict[str, Any]] = {("TREND", "TF_BREAKOUT", "AAA"): cycle}

    level_rows = engine._build_level_state_rows(active_cycles, {"AAA": metrics})
    addon_rows = engine._build_trend_addon_state_rows(active_cycles, {"AAA": metrics}, {})

    assert {
        "TF_TRAILING_LEVEL",
        "TF_PROTECTED_STOP_LEVEL",
        "TF_BREAKEVEN_LEVEL",
        "TF_TP1_LEVEL",
        "TF_TP2_LEVEL",
    } <= {row["signal_code"] for row in level_rows}
    assert {"TF_ADDON_READY", "TF_ADDON_SLOT1_READY"} <= {
        row["signal_code"] for row in addon_rows
    }


def test_trend_addon_slot2_state_row_emits_when_second_slot_ready(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["BREAKOUT"],
    }
    metrics = _metrics_for(_breakout_daily(end="2026-03-31"), source_entry=source_entry)
    cycle = _trend_cycle(
        family="TF_BREAKOUT",
        trailing_level=88.0,
        protected_stop_level=89.0,
        risk_free_armed=True,
        trim_count=1,
    )
    cycle.update(
        {
            "next_addon_allowed": True,
            "addon_next_slot": 2,
            "addon_tranche_pct": 0.3,
            "addon_reason_codes": ["ADDON_READY"],
            "add_on_count": 1,
            "add_on_slot": 1,
        }
    )

    rows = engine._build_trend_addon_state_rows(
        {("TREND", "TF_BREAKOUT", "AAA"): cycle},
        {"AAA": metrics},
        {},
    )

    assert {"TF_ADDON_READY", "TF_ADDON_SLOT2_READY"} <= {
        row["signal_code"] for row in rows
    }


def test_ug_pullback_and_squeeze_breakout_signals_can_coexist(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["BREAKOUT"],
    }
    metrics = _metrics_for(_breakout_daily(end="2026-03-31"), source_entry=source_entry)
    metrics = dict(metrics)
    metrics.update(
        {
            "date": "2026-03-31",
            "nh60": True,
            "breakout_ready": True,
            "ema_turn_down": False,
            "recent_orange_ready10": True,
            "recent_squeeze_ready10": True,
            "rvol20": 2.5,
            "ug_pbb_ready": True,
            "pullback_profile_pass": True,
        }
    )

    events = engine._ug_buy_events(
        symbol="AAA",
        metrics=metrics,
        source_entry=source_entry,
        active_cycles={},
        signal_history=[],
    )

    event_map = {row["signal_code"]: row for row in events}
    assert {"UG_BUY_SQUEEZE_BREAKOUT", "UG_BUY_PBB"} <= set(event_map)
    assert event_map["UG_BUY_PBB"]["reference_exit_signal"] == "UG_SELL_MR_SHORT_OR_PBS"


def test_ug_mr_long_buy_emits_signal(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["BREAKOUT"],
    }
    metrics = _metrics_for(_breakout_daily(end="2026-03-31"), source_entry=source_entry)
    metrics = dict(metrics)
    metrics.update(
        {
            "date": "2026-03-31",
            "ug_mr_long_ready": True,
            "alignment_state": "BULLISH",
            "ema_turn_down": False,
        }
    )

    events = engine._ug_buy_events(
        symbol="AAA",
        metrics=metrics,
        source_entry=source_entry,
        active_cycles={},
        signal_history=[],
    )

    assert any(row["signal_code"] == "UG_BUY_MR_LONG" for row in events)


def test_ug_state_rows_emit_primary_and_aux_codes(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["BREAKOUT"],
    }
    metrics = _metrics_for(_breakout_daily(end="2026-03-31"), source_entry=source_entry)
    metrics = dict(metrics)
    metrics.update(
        {
            "date": "2026-03-31",
            "nh60": True,
            "rvol20": 2.5,
            "w_active": True,
            "vcp_active": True,
            "squeeze_active": True,
            "tight_active": True,
        }
    )

    rows = engine._ug_state_rows(symbol="AAA", metrics=metrics, source_entry=source_entry)
    signal_codes = {row["signal_code"] for row in rows}

    assert {
        "UG_STATE_GREEN",
        "UG_NH60",
        "UG_VOL2X",
        "UG_W",
        "UG_VCP",
        "UG_SQUEEZE",
        "UG_TIGHT",
    } <= signal_codes


def test_ug_state_rows_emit_orange_profile(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["BREAKOUT"],
    }
    metrics = _metrics_for(_breakout_daily(end="2026-03-31"), source_entry=source_entry)
    metrics = dict(metrics)
    metrics.update(
        {
            "date": "2026-03-31",
            "breakout_ready": False,
            "ug_pbb_ready": False,
            "ug_mr_long_ready": False,
            "recent_orange_ready10": True,
            "recent_squeeze_ready10": True,
            "squeeze_active": True,
            "vcp_active": True,
            "tight_active": True,
            "nh60": False,
        }
    )

    rows = engine._ug_state_rows(symbol="AAA", metrics=metrics, source_entry=source_entry)

    assert any(row["signal_code"] == "UG_STATE_ORANGE" for row in rows)


def test_ug_state_rows_emit_red_profile(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["BREAKOUT"],
    }
    metrics = _metrics_for(_breakout_daily(end="2026-03-31"), source_entry=source_entry)
    metrics = dict(metrics)
    metrics.update(
        {
            "date": "2026-03-31",
            "breakout_ready": False,
            "ug_pbb_ready": False,
            "ug_mr_long_ready": False,
            "recent_orange_ready10": False,
            "recent_squeeze_ready10": False,
            "squeeze_active": False,
            "vcp_active": False,
            "tight_active": False,
            "nh60": False,
            "above_200ma": False,
            "alignment_state": "BEARISH",
            "ema_turn_down": True,
            "liquidity_pass": False,
        }
    )

    rows = engine._ug_state_rows(symbol="AAA", metrics=metrics, source_entry=source_entry)

    assert any(row["signal_code"] == "UG_STATE_RED" for row in rows)


def test_ug_combo_rows_emit_trend_and_pullback_context(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["BREAKOUT"],
    }
    metrics = _metrics_for(_breakout_daily(end="2026-03-31"), source_entry=source_entry)
    metrics = dict(metrics)
    metrics.update(
        {
            "date": "2026-03-31",
            "nh60": True,
            "breakout_ready": True,
            "ug_pbb_ready": True,
            "above_200ma": True,
            "squeeze_active": True,
            "vcp_active": True,
            "tight_active": True,
        }
    )
    active_cycles: dict[tuple[str, str, str], dict[str, Any]] = {("UG", "UG_BREAKOUT", "AAA"): _ug_cycle(family="UG_BREAKOUT", reference_exit_signal="")}

    rows = engine._ug_strategy_combo_rows(
        symbol="AAA",
        metrics=metrics,
        source_entry=source_entry,
        active_cycles=active_cycles,
    )

    assert {"UG_COMBO_TREND", "UG_COMBO_PULLBACK"} <= {row["signal_code"] for row in rows}


def test_ug_combo_rows_emit_squeeze_context(monkeypatch) -> None:
    engine = _engine_for_unit(monkeypatch)
    source_entry = {
        "buy_eligible": True,
        "screen_stage": "TEST",
        "source_tags": ["QM_DAILY"],
        "source_style_tags": ["BREAKOUT"],
    }
    metrics = _metrics_for(_breakout_daily(end="2026-03-31"), source_entry=source_entry)
    metrics = dict(metrics)
    metrics.update(
        {
            "date": "2026-03-31",
            "breakout_ready": False,
            "ug_pbb_ready": False,
            "ug_mr_long_ready": False,
            "recent_orange_ready10": True,
            "recent_squeeze_ready10": True,
            "squeeze_active": True,
            "vcp_active": True,
            "tight_active": True,
            "nh60": False,
        }
    )

    rows = engine._ug_strategy_combo_rows(
        symbol="AAA",
        metrics=metrics,
        source_entry=source_entry,
        active_cycles={},
    )

    assert any(row["signal_code"] == "UG_COMBO_SQUEEZE" for row in rows)
