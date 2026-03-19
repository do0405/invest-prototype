from __future__ import annotations

import shutil

import numpy as np
import pandas as pd

import screeners.qullamaggie as qullamaggie
import screeners.signals as signals
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
    return signal_engine.MultiScreenerSignalEngine(market=market, as_of_date=as_of_date)


class _StubEarningsCollector(signal_engine.EarningsDataCollector):
    def __init__(self) -> None:
        super().__init__(cache_dir="")

    def collect(self, market, as_of_date=None):
        return pd.DataFrame(columns=["symbol", "startdatetime"])

    def get_earnings_surprise(self, symbol):
        return None


def test_signals_package_restores_legacy_exports() -> None:
    assert signals.SignalEngine is signals.MultiScreenerSignalEngine
    assert signals.QullamaggieSignalEngine is signals.MultiScreenerSignalEngine
    assert signals.run_signal_scan is signals.run_multi_screener_signal_scan


def test_qullamaggie_package_forwards_legacy_signal_exports() -> None:
    assert qullamaggie.MultiScreenerSignalEngine is signals.MultiScreenerSignalEngine
    assert qullamaggie.SignalEngine is signals.SignalEngine
    assert qullamaggie.QullamaggieSignalEngine is signals.QullamaggieSignalEngine
    assert qullamaggie.run_signal_scan is signals.run_signal_scan
    assert qullamaggie.run_multi_screener_signal_scan is signals.run_multi_screener_signal_scan
    assert qullamaggie.run_peg_imminent_screen is signals.run_peg_imminent_screen
    assert qullamaggie.run_qullamaggie_signal_scan is signals.run_qullamaggie_signal_scan


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
        signal_engine._transform_signal_rows(events, contract_version=signal_engine._CONTRACT_VERSION_V2),
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
        signal_engine,
        "load_local_ohlcv_frame",
        lambda market, symbol, **kwargs: _breakout_daily(end="2026-03-11").copy() if symbol == "AAA" else pd.DataFrame(),
    )

    pd.DataFrame([{"symbol": "AAA", "as_of_ts": "2026-03-11"}]).to_csv(
        screeners_root / "qullamaggie" / "daily_focus_list.csv",
        index=False,
    )

    result = signal_engine.run_signal_scan(
        market="us",
        as_of_date="2026-03-11",
        upcoming_earnings_fetcher=lambda market, as_of_date, days: pd.DataFrame(),
        earnings_collector=_StubEarningsCollector(),
    )

    v2_row = next(row for row in result["all_signals_v2"] if row["symbol"] == "AAA" and row["engine"] == "TREND")
    snapshot_row = next(row for row in result["signal_universe_snapshot"] if row["symbol"] == "AAA")

    for row in (v2_row, snapshot_row):
        assert row["ema_alignment_state"]
        assert "market_condition_state" in row
        assert "pullback_quality" in row
        assert "buy_warning_summary" in row
        assert "aux_signal_summary" in row
        assert "conviction_reason" in row


def test_run_signal_scan_alias_matches_multi_screener_helper() -> None:
    assert signal_engine.run_signal_scan is signal_engine.run_multi_screener_signal_scan


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
