from __future__ import annotations

import importlib
import importlib.util
from typing import Any

import numpy as np
import pandas as pd
import pytest

import screeners.qullamaggie as qullamaggie
from screeners.qullamaggie.core import QullamaggieAnalyzer
from screeners.qullamaggie import screener as qullamaggie_screener
from tests._paths import runtime_root
from utils.screener_utils import save_screening_results as real_save_screening_results


def _row_record(row: pd.Series) -> dict[str, Any]:
    return {str(key): value for key, value in row.to_dict().items()}


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
        spreads = np.full(len(closes), 0.01)

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


def _benchmark_daily(length: int = 140) -> pd.DataFrame:
    closes = np.linspace(100.0, 124.0, length)
    volumes = np.linspace(4_000_000.0, 4_500_000.0, length)
    spreads = np.full(length, 0.008)
    return _daily_from_closes(closes, volumes, spreads=spreads)


def _breakout_daily() -> pd.DataFrame:
    trend = np.linspace(40.0, 78.0, 90)
    base = np.array(
        [
            78.1, 78.8, 78.5, 79.1, 78.9,
            79.4, 79.2, 79.7, 79.5, 79.9,
            79.8, 80.1, 79.95, 80.2, 80.05,
            80.3, 80.2, 80.4, 80.3, 80.5,
            80.45, 80.6, 80.55, 80.7, 80.75,
        ]
    )
    breakout = np.array([80.85, 80.95, 81.05, 81.10, 84.2])
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
    return _daily_from_closes(closes, volumes, spreads=spreads)


def _leader_daily() -> pd.DataFrame:
    closes = np.linspace(28.0, 56.0, 140)
    volumes = np.full(len(closes), 1_000_000.0)
    spreads = np.full(len(closes), 0.012)
    return _daily_from_closes(closes, volumes, spreads=spreads)


def _steady_daily() -> pd.DataFrame:
    closes = np.linspace(20.0, 36.0, 140)
    volumes = np.full(len(closes), 850_000.0)
    spreads = np.full(len(closes), 0.013)
    return _daily_from_closes(closes, volumes, spreads=spreads)


def _ep_daily() -> pd.DataFrame:
    trend = np.linspace(35.0, 46.5, 45)
    base = np.array(
        [
            46.8, 47.1, 47.6, 47.9, 48.2,
            48.6, 49.0, 49.3, 49.7, 50.1,
            50.5, 50.8, 51.1, 51.4, 51.7,
            52.0, 52.2, 52.5, 52.8, 53.0,
            53.2, 53.5, 53.8, 54.2, 54.5,
        ]
    )
    closes = np.concatenate([trend, base, np.array([55.0, 60.0])])
    volumes = np.concatenate(
        [
            np.full(len(trend), 900_000.0),
            np.linspace(850_000.0, 700_000.0, len(base)),
            np.array([950_000.0, 4_200_000.0]),
        ]
    )
    spreads = np.concatenate(
        [
            np.full(len(trend), 0.030),
            np.full(len(base), 0.028),
            np.array([0.028, 0.045]),
        ]
    )
    return _daily_from_closes(closes, volumes, spreads=spreads, gap_on_last=0.12)


def test_qullamaggie_hides_legacy_signal_aliases_when_signals_module_is_unavailable(monkeypatch) -> None:
    original_find_spec = importlib.util.find_spec

    def _patched_find_spec(name: str, package: str | None = None):  # noqa: ANN202
        if name == "screeners.signals":
            return None
        return original_find_spec(name, package)

    with monkeypatch.context() as patch:
        patch.setattr(importlib.util, "find_spec", _patched_find_spec)
        reloaded = importlib.reload(qullamaggie)
        assert "SignalEngine" not in reloaded.__all__
        assert "run_signal_scan" not in reloaded.__all__
        with pytest.raises(AttributeError):
            getattr(reloaded, "SignalEngine")

    importlib.reload(qullamaggie)


def test_breakout_analyzer_identifies_actionable_candidate() -> None:
    analyzer = QullamaggieAnalyzer()
    frames = {
        "AAA": _breakout_daily(),
        "BBB": _leader_daily(),
        "CCC": _steady_daily(),
    }
    feature_rows = [analyzer.compute_feature_row(symbol, "us", frame) for symbol, frame in frames.items()]
    feature_table = analyzer.finalize_feature_table(pd.DataFrame(feature_rows))
    feature_map = {str(row["symbol"]): _row_record(row) for _, row in feature_table.iterrows()}
    feature_map["AAA"]["breakout_universe_pass"] = True
    regime = analyzer.compute_market_regime(
        market="us",
        benchmark_symbol="SPY",
        benchmark_daily=_benchmark_daily(len(next(iter(frames.values())))),
        feature_table=feature_table,
    )

    result = analyzer.analyze_breakout(
        "AAA",
        frames["AAA"],
        market="us",
        feature_row=feature_map["AAA"],
        regime=regime,
    )

    assert result["passed"] is True
    assert result["setup_family"] == "BREAKOUT"
    assert result["stock_grade"] in {"A++", "A+"}
    assert result["setup_score"] is not None and result["setup_score"] >= 70.0
    assert result["candidate_stage"] in {"DAILY_FOCUS", "WEEKLY_FOCUS", "WIDE_LIST"}
    assert "TIGHT_BASE" in result["reason_codes"]


def test_breakout_analyzer_requires_universe_gate_before_passing() -> None:
    analyzer = QullamaggieAnalyzer()
    frames = {
        "AAA": _breakout_daily(),
        "BBB": _leader_daily(),
        "CCC": _steady_daily(),
    }
    feature_rows = [analyzer.compute_feature_row(symbol, "us", frame) for symbol, frame in frames.items()]
    feature_table = analyzer.finalize_feature_table(pd.DataFrame(feature_rows))
    feature_map = {str(row["symbol"]): _row_record(row) for _, row in feature_table.iterrows()}
    feature_map["AAA"]["breakout_universe_pass"] = True
    regime = analyzer.compute_market_regime(
        market="us",
        benchmark_symbol="SPY",
        benchmark_daily=_benchmark_daily(len(next(iter(frames.values())))),
        feature_table=feature_table,
    )

    forced_row = dict(feature_map["AAA"])
    forced_row["breakout_universe_pass"] = False

    result = analyzer.analyze_breakout(
        "AAA",
        frames["AAA"],
        market="us",
        feature_row=forced_row,
        regime=regime,
    )

    assert result["passed"] is False
    assert "OUTSIDE_BREAKOUT_UNIVERSE" in result["fail_codes"]


def test_episode_pivot_analyzer_promotes_core_when_event_is_present() -> None:
    analyzer = QullamaggieAnalyzer()
    daily = _ep_daily()
    feature_table = analyzer.finalize_feature_table(pd.DataFrame([analyzer.compute_feature_row("EPX", "us", daily)]))
    feature_row = _row_record(feature_table.iloc[0])
    regime = analyzer.compute_market_regime(
        market="us",
        benchmark_symbol="SPY",
        benchmark_daily=_benchmark_daily(len(daily)),
        feature_table=feature_table,
    )
    earnings_payload = {
        "meets_criteria": True,
        "eps_surprise_pct": 32.0,
        "revenue_surprise_pct": 24.0,
        "yoy_eps_growth": 180.0,
        "yoy_revenue_growth": 28.0,
        "eps_estimate": 1.2,
    }

    result = analyzer.analyze_episode_pivot(
        "EPX",
        daily,
        True,
        market="us",
        feature_row=feature_row,
        regime=regime,
        earnings_payload=earnings_payload,
    )

    assert result["passed"] is True
    assert result["ep_type"] == "EP_CORE"
    assert result["setup_grade"] == "5-star"
    assert "HAS_EVENT_DATA" in result["data_flags"]
    assert "EARNINGS_CATALYST" in result["reason_codes"]


def test_episode_pivot_requires_universe_gate_before_passing() -> None:
    analyzer = QullamaggieAnalyzer()
    daily = _ep_daily()
    feature_table = analyzer.finalize_feature_table(pd.DataFrame([analyzer.compute_feature_row("EPX", "us", daily)]))
    feature_row = _row_record(feature_table.iloc[0])
    feature_row["ep_universe_pass"] = False
    regime = analyzer.compute_market_regime(
        market="us",
        benchmark_symbol="SPY",
        benchmark_daily=_benchmark_daily(len(daily)),
        feature_table=feature_table,
    )
    earnings_payload = {
        "meets_criteria": True,
        "eps_surprise_pct": 32.0,
        "revenue_surprise_pct": 24.0,
        "yoy_eps_growth": 180.0,
        "yoy_revenue_growth": 28.0,
        "eps_estimate": 1.2,
    }

    result = analyzer.analyze_episode_pivot(
        "EPX",
        daily,
        True,
        market="us",
        feature_row=feature_row,
        regime=regime,
        earnings_payload=earnings_payload,
    )

    assert result["passed"] is False
    assert "OUTSIDE_EP_UNIVERSE" in result["fail_codes"]


def test_breakout_universe_requires_multi_horizon_relative_strength() -> None:
    analyzer = QullamaggieAnalyzer()
    feature_table = analyzer.finalize_feature_table(
        pd.DataFrame([analyzer.compute_feature_row("AAA", "us", _breakout_daily())])
    )
    feature_table.loc[:, "ret_1m_pctile"] = 45.0
    feature_table.loc[:, "ret_3m_pctile"] = 88.0
    feature_table.loc[:, "ret_6m_pctile"] = 86.0

    calibration = analyzer.build_actual_data_calibration(feature_table, market="us")
    calibrated = analyzer.apply_actual_data_calibration(
        feature_table,
        market="us",
        calibration=calibration,
    )

    assert bool(calibrated.iloc[0]["breakout_universe_pass"]) is False


def test_run_qullamaggie_screening_builds_watchlists(monkeypatch) -> None:
    frames = {
        "AAA": _breakout_daily(),
        "BBB": _leader_daily(),
        "CCC": _steady_daily(),
    }
    output_root = runtime_root("_test_runtime_qullamaggie_watchlists")
    output_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(qullamaggie_screener, "_load_market_symbols", lambda market: sorted(frames))
    monkeypatch.setattr(qullamaggie_screener, "load_local_ohlcv_frame", lambda market, symbol, **kwargs: frames[symbol].copy())
    monkeypatch.setattr(qullamaggie_screener, "load_benchmark_data", lambda *args, **kwargs: ("SPY", _benchmark_daily(len(_breakout_daily()))))
    monkeypatch.setattr(qullamaggie_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(qullamaggie_screener, "get_qullamaggie_results_dir", lambda market: str(output_root))
    monkeypatch.setattr(qullamaggie_screener, "save_screening_results", real_save_screening_results)
    monkeypatch.setattr(qullamaggie_screener, "track_new_tickers", lambda **kwargs: [])
    monkeypatch.setattr(qullamaggie_screener, "create_screener_summary", lambda **kwargs: None)
    real_build_context = qullamaggie_screener._build_context

    def _patched_build_context(*args, **kwargs):  # noqa: ANN002, ANN003
        context = real_build_context(*args, **kwargs)
        if "AAA" in context.get("feature_map", {}):
            context["feature_map"]["AAA"] = {
                **context["feature_map"]["AAA"],
                "breakout_universe_pass": True,
            }
        return context

    monkeypatch.setattr(qullamaggie_screener, "_build_context", _patched_build_context)

    result = qullamaggie_screener.run_qullamaggie_screening(setup_type="breakout", market="us")

    assert result["market_regime"]["regime_state"] in {"RISK_ON", "RISK_ON_AGGRESSIVE"}
    assert len(result["breakout"]) >= 1
    assert result["breakout"][0]["symbol"] == "AAA"
    assert len(result["universe_list"]) >= 1
    assert len(result["pattern_excluded_pool"]) >= 1
    assert len(result["pattern_included_candidates"]) >= 1
    assert len(result["wide_list"]) >= 1
    assert result["actual_data_calibration"]["breakout_min_compression_score"] >= 58.0
    assert len(result["weekly_focus"]) >= 1
    assert (output_root / "pattern_excluded_pool.csv").exists()
    assert (output_root / "pattern_included_candidates.csv").exists()
    assert (output_root / "pre_pattern_quant_financial_candidates.csv").exists()
    assert any(output_root.glob("pre_pattern_quant_financial_candidates_*.csv"))
    assert (output_root / "actual_data_calibration.json").exists()


def test_episode_pivot_skips_earnings_fetch_until_technical_prefilter_passes() -> None:
    analyzer = QullamaggieAnalyzer()
    daily = _ep_daily()
    feature_table = analyzer.finalize_feature_table(pd.DataFrame([analyzer.compute_feature_row("EPX", "us", daily)]))
    feature_row = _row_record(feature_table.iloc[0])
    feature_row["gap_pct"] = 0.01
    feature_row["rvol"] = 1.0
    feature_row["dcr"] = 0.5
    feature_row["no_excessive_run"] = False
    regime = analyzer.compute_market_regime(
        market="us",
        benchmark_symbol="SPY",
        benchmark_daily=_benchmark_daily(len(daily)),
        feature_table=feature_table,
    )

    class _Collector:
        def get_earnings_surprise(self, symbol: str):  # noqa: ANN201
            raise AssertionError(f"earnings fetch should be skipped for {symbol}")

    result = analyzer.analyze_episode_pivot(
        "EPX",
        daily,
        True,
        market="us",
        feature_row=feature_row,
        regime=regime,
        earnings_collector=_Collector(),
    )

    assert "EVENT_FETCH_SKIPPED_TECHNICAL_PREFILTER" in result["data_flags"]
    assert result["earnings_surprise"] is False


def test_episode_pivot_fetches_earnings_after_technical_prefilter_passes() -> None:
    analyzer = QullamaggieAnalyzer()
    daily = _ep_daily()
    feature_table = analyzer.finalize_feature_table(pd.DataFrame([analyzer.compute_feature_row("EPX", "us", daily)]))
    feature_row = _row_record(feature_table.iloc[0])
    feature_row["gap_pct"] = 0.18
    feature_row["rvol"] = 3.2
    feature_row["dcr"] = 0.72
    feature_row["no_excessive_run"] = True
    feature_row["neglected_base_score"] = 70.0
    feature_row["open"] = float(feature_row.get("close") or 100.0) * 0.98
    regime = analyzer.compute_market_regime(
        market="us",
        benchmark_symbol="SPY",
        benchmark_daily=_benchmark_daily(len(daily)),
        feature_table=feature_table,
    )

    calls: list[str] = []

    class _Collector:
        def get_earnings_surprise(self, symbol: str):  # noqa: ANN201
            calls.append(symbol)
            return {
                "meets_criteria": True,
                "eps_surprise_pct": 28.0,
                "revenue_surprise_pct": 22.0,
                "yoy_eps_growth": 120.0,
                "yoy_revenue_growth": 24.0,
                "eps_estimate": 1.0,
            }

    result = analyzer.analyze_episode_pivot(
        "EPX",
        daily,
        True,
        market="us",
        feature_row=feature_row,
        regime=regime,
        earnings_collector=_Collector(),
    )

    assert calls == ["EPX"]
    assert result["earnings_surprise"] is True
    assert "EARNINGS_CATALYST" in result["reason_codes"]


def test_episode_pivot_does_not_treat_unavailable_earnings_payload_as_event_data() -> None:
    analyzer = QullamaggieAnalyzer()
    daily = _ep_daily()
    feature_table = analyzer.finalize_feature_table(pd.DataFrame([analyzer.compute_feature_row("EPX", "us", daily)]))
    feature_row = _row_record(feature_table.iloc[0])
    feature_row["gap_pct"] = 0.18
    feature_row["rvol"] = 3.2
    feature_row["dcr"] = 0.72
    feature_row["no_excessive_run"] = True
    feature_row["neglected_base_score"] = 70.0
    feature_row["open"] = float(feature_row.get("close") or 100.0) * 0.98
    regime = analyzer.compute_market_regime(
        market="us",
        benchmark_symbol="SPY",
        benchmark_daily=_benchmark_daily(len(daily)),
        feature_table=feature_table,
    )

    result = analyzer.analyze_episode_pivot(
        "EPX",
        daily,
        True,
        market="us",
        feature_row=feature_row,
        regime=regime,
        earnings_payload={
            "fetch_status": "rate_limited",
            "unavailable_reason": "rate limited",
            "meets_criteria": False,
        },
    )

    assert result["earnings_surprise"] is False
    assert result["earnings_fetch_status"] == "rate_limited"
    assert "EARNINGS_RATE_LIMITED" in result["data_flags"]
    assert "EVENT_PROXY_PRESENT" not in result["reason_codes"]


def test_universe_list_requires_hard_universe_pass(monkeypatch) -> None:
    output_root = runtime_root("_test_runtime_qullamaggie_universe_gate")
    output_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(qullamaggie_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(qullamaggie_screener, "get_qullamaggie_results_dir", lambda market: str(output_root))
    monkeypatch.setattr(qullamaggie_screener.QullamaggieScreener, "_load_frames", lambda self: {})
    monkeypatch.setattr(
        qullamaggie_screener,
        "_build_context",
        lambda *args, **kwargs: {
            "market": "us",
            "feature_table": pd.DataFrame(
                [
                    {
                        "symbol": "SCOREONLY",
                        "as_of_ts": "2026-03-14",
                        "stock_grade": "A++",
                        "a_pp_score": 92.0,
                        "focus_seed_score": 90.0,
                        "compression_score": 82.0,
                        "high_52w_proximity": 0.95,
                        "pivot_price": 100.0,
                        "stop_price": 92.0,
                        "risk_unit_pct": 8.0,
                        "breakout_universe_pass": False,
                        "ep_universe_pass": False,
                        "has_sector_mapping": True,
                        "has_fundamentals": True,
                    }
                ]
            ),
            "feature_map": {},
            "calibration": {},
            "frames": {},
            "metadata_map": {},
            "benchmark_symbol": "SPY",
            "benchmark_daily": pd.DataFrame(),
            "regime": qullamaggie_screener.MarketRegime(
                market_code="US",
                benchmark_symbol="SPY",
                regime_state="RISK_ON",
                regime_score=75.0,
                market_trend_score=75.0,
                breadth_score=75.0,
                opportunity_score=75.0,
                focus_list_density=0.0,
                breakout_success_proxy=0.0,
                reason_codes=("TEST",),
                data_flags=("TEST",),
            ),
            "earnings_collector": None,
        },
    )
    monkeypatch.setattr(qullamaggie_screener, "_write_records", lambda *args, **kwargs: None)
    monkeypatch.setattr(qullamaggie_screener.QullamaggieScreener, "_persist_results", lambda self, results: None)

    screener = qullamaggie_screener.QullamaggieScreener(market="us", enable_earnings_filter=False)
    result = screener.run(setup_type="breakout")

    assert result["universe_list"] == []
    assert result["pattern_excluded_pool"] == []
