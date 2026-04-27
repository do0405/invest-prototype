from __future__ import annotations

from pathlib import Path

import pandas as pd

from tests._paths import runtime_root
from screeners.markminervini import integrated_screener


def _reset_dir(path: Path) -> None:
    if path.exists():
        for child in sorted(path.rglob("*"), reverse=True):
            try:
                if child.is_file():
                    child.unlink()
                elif child.is_dir():
                    child.rmdir()
            except Exception:
                continue
    path.mkdir(parents=True, exist_ok=True)


def test_fetch_ohlcv_data_prefers_local_csv(monkeypatch):
    root = runtime_root("_test_runtime_integrated_ohlcv_local_first")
    _reset_dir(root)
    monkeypatch.setattr(integrated_screener, "project_root", str(root))

    us_dir = root / "data" / "us"
    us_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05", "2026-01-06"],
            "Open": [100.0, 101.0, 102.0],
            "High": [101.0, 102.0, 103.0],
            "Low": [99.0, 100.0, 101.0],
            "Close": [100.5, 101.5, 102.5],
            "Volume": [1000, 1200, 1300],
            "symbol": ["AAA", "AAA", "AAA"],
        }
    ).to_csv(us_dir / "AAA.csv", index=False)

    def _fail_external(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("external fetch should not run when local csv exists")

    monkeypatch.setattr(integrated_screener.yf, "download", _fail_external)

    screener = integrated_screener.IntegratedScreener()
    frame = screener.fetch_ohlcv_data("AAA", days=60)

    assert frame is not None
    assert not frame.empty
    assert "Close" in frame.columns
    assert isinstance(frame.index, pd.DatetimeIndex)


def test_fetch_ohlcv_data_filters_local_csv_to_as_of(monkeypatch):
    root = runtime_root("_test_runtime_integrated_ohlcv_as_of")
    _reset_dir(root)
    monkeypatch.setattr(integrated_screener, "project_root", str(root))

    us_dir = root / "data" / "us"
    us_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05", "2026-01-06"],
            "Open": [100.0, 101.0, 102.0],
            "High": [101.0, 102.0, 103.0],
            "Low": [99.0, 100.0, 101.0],
            "Close": [100.5, 101.5, 102.5],
            "Volume": [1000, 1200, 1300],
            "symbol": ["AAA", "AAA", "AAA"],
        }
    ).to_csv(us_dir / "AAA.csv", index=False)

    def _fail_external(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("external fetch should not run when local csv exists")

    monkeypatch.setattr(integrated_screener.yf, "download", _fail_external)

    screener = integrated_screener.IntegratedScreener()
    frame = screener.fetch_ohlcv_data("AAA", days=60, as_of_date="2026-01-05")

    assert not frame.empty
    assert frame.index.max().strftime("%Y-%m-%d") == "2026-01-05"
    assert "2026-01-06" not in {value.strftime("%Y-%m-%d") for value in frame.index}


def test_fetch_ohlcv_data_writes_through_to_local_csv(monkeypatch):
    root = runtime_root("_test_runtime_integrated_ohlcv_write_through")
    _reset_dir(root)
    monkeypatch.setattr(integrated_screener, "project_root", str(root))

    downloaded = pd.DataFrame(
        {
            "Open": [30.0, 31.0, 32.0],
            "High": [31.0, 32.0, 33.0],
            "Low": [29.5, 30.5, 31.5],
            "Close": [30.5, 31.5, 32.5],
            "Adj Close": [30.4, 31.4, 32.4],
            "Volume": [5000, 5200, 5400],
        },
        index=pd.to_datetime(["2026-02-10", "2026-02-11", "2026-02-12"]),
    )

    monkeypatch.setattr(
        integrated_screener.yf,
        "download",
        lambda *args, **kwargs: downloaded.copy(),  # noqa: ARG005, ANN002, ANN003
    )

    screener = integrated_screener.IntegratedScreener()
    frame = screener.fetch_ohlcv_data("ZZZ", days=90)

    assert frame is not None
    assert not frame.empty

    cache_file = root / "data" / "us" / "ZZZ.csv"
    assert cache_file.exists()

    cached = pd.read_csv(cache_file)
    assert {
        "date",
        "Open",
        "High",
        "Low",
        "Close",
        "Adj Close",
        "Volume",
        "Dividends",
        "Stock Splits",
        "Split Factor",
        "symbol",
    }.issubset(set(cached.columns))
    assert str(cached.iloc[-1]["symbol"]) == "ZZZ"
    assert float(cached.iloc[-1]["Adj Close"]) == 32.4


def test_fetch_ohlcv_data_uses_strict_preferred_provider_for_kr(monkeypatch):
    root = runtime_root("_test_runtime_integrated_ohlcv_kr_preferred_provider")
    _reset_dir(root)
    monkeypatch.setattr(integrated_screener, "project_root", str(root))

    calls: list[str] = []
    downloaded = pd.DataFrame(
        {
            "Open": [70.0, 71.0, 72.0],
            "High": [71.0, 72.0, 73.0],
            "Low": [69.5, 70.5, 71.5],
            "Close": [70.5, 71.5, 72.5],
            "Adj Close": [70.4, 71.4, 72.4],
            "Volume": [1000, 1100, 1200],
        },
        index=pd.to_datetime(["2026-02-10", "2026-02-11", "2026-02-12"]),
    )

    monkeypatch.setattr(
        integrated_screener,
        "iter_preferred_provider_symbols",
        lambda symbol, market, strict=False: ["005930.KS"] if strict else [symbol],
    )
    monkeypatch.setattr(
        integrated_screener.yf,
        "download",
        lambda provider_symbol, *args, **kwargs: calls.append(provider_symbol) or downloaded.copy(),
    )

    screener = integrated_screener.IntegratedScreener(market="kr")
    frame = screener.fetch_ohlcv_data("005930", days=90)

    assert frame is not None
    assert not frame.empty
    assert calls == ["005930.KS"]


def test_run_integrated_screening_writes_pattern_split_aliases(monkeypatch):
    root = runtime_root("_test_runtime_integrated_pattern_split_aliases")
    _reset_dir(root)
    results_dir = root / "results" / "us" / "screeners" / "markminervini"
    results_dir.mkdir(parents=True, exist_ok=True)

    with_rs_path = results_dir / "with_rs.csv"
    pd.DataFrame(
        [
            {"symbol": "AAA", "met_count": 7, "rs_score": 96.0},
            {"symbol": "BBB", "met_count": 6, "rs_score": 91.0},
        ]
    ).to_csv(with_rs_path, index=False)

    monkeypatch.setattr(integrated_screener, "project_root", str(root))
    monkeypatch.setattr(integrated_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(integrated_screener, "get_markminervini_results_dir", lambda market: str(results_dir))
    monkeypatch.setattr(integrated_screener, "get_markminervini_with_rs_path", lambda market: str(with_rs_path))
    monkeypatch.setattr(
        integrated_screener,
        "get_markminervini_advanced_financial_results_path",
        lambda market: str(results_dir / "advanced_financial_results.csv"),
    )
    monkeypatch.setattr(
        integrated_screener,
        "get_markminervini_integrated_results_path",
        lambda market: str(results_dir / "integrated_results.csv"),
    )
    monkeypatch.setattr(
        integrated_screener,
        "get_markminervini_integrated_pattern_results_path",
        lambda market: str(results_dir / "integrated_pattern_results.csv"),
    )

    screener = integrated_screener.IntegratedScreener(market="us")
    sample_dates = pd.bdate_range("2025-01-02", periods=240, tz="UTC")
    sample_ohlcv = pd.DataFrame(
        {
            "Date": sample_dates,
            "Open": pd.Series(range(240), dtype=float) * 0.2 + 30.0,
            "High": pd.Series(range(240), dtype=float) * 0.2 + 31.0,
            "Low": pd.Series(range(240), dtype=float) * 0.2 + 29.0,
            "Close": pd.Series(range(240), dtype=float) * 0.2 + 30.5,
            "Volume": [1_500_000.0] * 240,
        }
    ).set_index("Date")
    monkeypatch.setattr(screener, "fetch_ohlcv_data", lambda symbol, days=365: sample_ohlcv.copy())
    monkeypatch.setattr(
        screener.pattern_analyzer,
        "analyze_patterns_enhanced",
        lambda symbol, stock_data: {
            "vcp": {
                "detected": symbol == "AAA",
                "confidence": 0.82,
                "confidence_level": "High",
                "state_detail": "FORMING_VCP" if symbol == "AAA" else "NONE",
                "state_bucket": "FORMING" if symbol == "AAA" else "NONE",
                "pattern_start": "2026-01-01",
                "pattern_end": "2026-02-12",
                "pivot_price": 33.0,
                "invalidation_price": 29.5,
                "breakout_date": None,
                "breakout_price": None,
                "breakout_volume": None,
                "volume_multiple": None,
                "distance_to_pivot_pct": -1.2,
                "extended": False,
                "dimensional_scores": {},
                "metrics": {},
                "pivots": [],
            },
            "cup_handle": {
                "detected": False,
                "confidence": 0.0,
                "confidence_level": "None",
                "state_detail": "NONE",
                "state_bucket": "NONE",
                "pattern_start": None,
                "pattern_end": None,
                "pivot_price": None,
                "invalidation_price": None,
                "breakout_date": None,
                "breakout_price": None,
                "breakout_volume": None,
                "volume_multiple": None,
                "distance_to_pivot_pct": None,
                "extended": False,
                "dimensional_scores": {},
                "metrics": {},
                "pivots": [],
            },
        },
    )

    result = screener.run_integrated_screening()

    assert "pattern_stage_summary" in result.columns
    assert "actual_data_pattern_priority_score" in result.columns
    assert "leader_score_component" in result.columns
    assert "pattern_pre_gate_pass" in result.columns
    assert bool(result.loc[result["symbol"] == "AAA", "pattern_included"].iloc[0]) is True
    assert bool(result.loc[result["symbol"] == "AAA", "actionable_pattern_pass"].iloc[0]) is True
    assert (results_dir / "integrated_without_patterns.csv").exists()
    assert (results_dir / "pre_pattern_quant_financial_candidates.csv").exists()
    assert any(results_dir.glob("pre_pattern_quant_financial_candidates_*.csv"))
    assert (results_dir / "integrated_with_patterns.csv").exists()
    assert (results_dir / "integrated_actionable_patterns.csv").exists()
    assert (results_dir / "actual_data_pattern_calibration.json").exists()


def test_run_integrated_screening_applies_liquidity_pre_gate(monkeypatch):
    root = runtime_root("_test_runtime_integrated_pre_gate")
    _reset_dir(root)
    results_dir = root / "results" / "us" / "screeners" / "markminervini"
    results_dir.mkdir(parents=True, exist_ok=True)

    with_rs_path = results_dir / "with_rs.csv"
    pd.DataFrame(
        [
            {"symbol": "AAA", "met_count": 7, "rs_score": 90.0, "distance_to_52w_high": 0.08},
            {"symbol": "BBB", "met_count": 7, "rs_score": 88.0, "distance_to_52w_high": 0.10},
        ]
    ).to_csv(with_rs_path, index=False)

    monkeypatch.setattr(integrated_screener, "project_root", str(root))
    monkeypatch.setattr(integrated_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(integrated_screener, "get_markminervini_results_dir", lambda market: str(results_dir))
    monkeypatch.setattr(integrated_screener, "get_markminervini_with_rs_path", lambda market: str(with_rs_path))
    monkeypatch.setattr(
        integrated_screener,
        "get_markminervini_advanced_financial_results_path",
        lambda market: str(results_dir / "advanced_financial_results.csv"),
    )
    monkeypatch.setattr(
        integrated_screener,
        "get_markminervini_integrated_results_path",
        lambda market: str(results_dir / "integrated_results.csv"),
    )
    monkeypatch.setattr(
        integrated_screener,
        "get_markminervini_integrated_pattern_results_path",
        lambda market: str(results_dir / "integrated_pattern_results.csv"),
    )

    screener = integrated_screener.IntegratedScreener(market="us")
    strong_dates = pd.bdate_range("2025-01-02", periods=240, tz="UTC")
    strong = pd.DataFrame(
        {
            "Date": strong_dates,
            "Open": pd.Series(range(240), dtype=float) * 0.15 + 40.0,
            "High": pd.Series(range(240), dtype=float) * 0.15 + 41.0,
            "Low": pd.Series(range(240), dtype=float) * 0.15 + 39.0,
            "Close": pd.Series(range(240), dtype=float) * 0.15 + 40.5,
            "Volume": [2_000_000.0] * 240,
        }
    ).set_index("Date")
    weak = strong.copy()
    weak["Volume"] = 2_000.0

    monkeypatch.setattr(
        screener,
        "fetch_ohlcv_data",
        lambda symbol, days=365: strong.copy() if symbol == "AAA" else weak.copy(),
    )
    monkeypatch.setattr(
        screener.pattern_analyzer,
        "analyze_patterns_enhanced",
        lambda symbol, stock_data: {
            "vcp": {
                "detected": True,
                "confidence": 0.81,
                "confidence_level": "High",
                "state_detail": "FORMING_VCP",
                "state_bucket": "FORMING",
                "pattern_start": "2025-09-01",
                "pattern_end": "2025-12-01",
                "pivot_price": 80.0,
                "invalidation_price": 70.0,
                "breakout_date": None,
                "breakout_price": None,
                "breakout_volume": None,
                "volume_multiple": None,
                "distance_to_pivot_pct": -0.03,
                "extended": False,
                "dimensional_scores": {
                    "technical_quality": 0.8,
                    "volume_confirmation": 0.8,
                    "temporal_validity": 0.8,
                    "market_context": 0.8,
                },
                "metrics": {"tightness_pct": 0.05},
                "pivots": [],
            },
            "cup_handle": {
                "detected": False,
                "confidence": 0.0,
                "confidence_level": "None",
                "state_detail": "NONE",
                "state_bucket": "NONE",
                "pattern_start": None,
                "pattern_end": None,
                "pivot_price": None,
                "invalidation_price": None,
                "breakout_date": None,
                "breakout_price": None,
                "breakout_volume": None,
                "volume_multiple": None,
                "distance_to_pivot_pct": None,
                "extended": False,
                "dimensional_scores": {},
                "metrics": {},
                "pivots": [],
            },
        },
    )

    result = screener.run_integrated_screening()

    aaa = result.loc[result["symbol"] == "AAA"].iloc[0]
    bbb = result.loc[result["symbol"] == "BBB"].iloc[0]
    assert bool(aaa["pattern_pre_gate_pass"]) is True
    assert bool(bbb["pattern_pre_gate_pass"]) is False
    assert bbb["pattern_pre_gate_reason"] == "liquidity_below_market_threshold"
    assert bool(bbb["pattern_included"]) is False


def test_merge_technical_and_financial_preserves_kr_leading_zero_symbols(monkeypatch):
    root = runtime_root("_test_runtime_integrated_kr_symbol_merge")
    _reset_dir(root)
    results_dir = root / "results" / "kr" / "screeners" / "markminervini"
    results_dir.mkdir(parents=True, exist_ok=True)

    with_rs_path = results_dir / "with_rs.csv"
    advanced_path = results_dir / "advanced_financial_results.csv"
    integrated_path = results_dir / "integrated_results.csv"

    pd.DataFrame(
        [
            {"symbol": "001510", "met_count": 7, "rs_score": 98.41},
        ]
    ).to_csv(with_rs_path, index=False)
    pd.DataFrame(
        [
            {
                "symbol": "001510",
                "provider_symbol": "001510.KS",
                "fin_met_count": 4,
                "fetch_status": "complete",
                "unavailable_reason": None,
                "has_error": False,
            }
        ]
    ).to_csv(advanced_path, index=False)

    monkeypatch.setattr(integrated_screener, "project_root", str(root))
    monkeypatch.setattr(integrated_screener, "ensure_market_dirs", lambda market: None)
    monkeypatch.setattr(integrated_screener, "get_markminervini_results_dir", lambda market: str(results_dir))
    monkeypatch.setattr(integrated_screener, "get_markminervini_with_rs_path", lambda market: str(with_rs_path))
    monkeypatch.setattr(
        integrated_screener,
        "get_markminervini_advanced_financial_results_path",
        lambda market: str(advanced_path),
    )
    monkeypatch.setattr(
        integrated_screener,
        "get_markminervini_integrated_results_path",
        lambda market: str(integrated_path),
    )
    monkeypatch.setattr(
        integrated_screener,
        "get_markminervini_integrated_pattern_results_path",
        lambda market: str(results_dir / "integrated_pattern_results.csv"),
    )

    screener = integrated_screener.IntegratedScreener(market="kr")
    merged = screener.merge_technical_and_financial()

    assert len(merged) == 1
    assert str(merged.loc[0, "symbol"]) == "001510"
    assert str(merged.loc[0, "provider_symbol"]) == "001510.KS"

    written = pd.read_csv(integrated_path, dtype={"symbol": "string", "provider_symbol": "string"})
    assert written.loc[0, "symbol"] == "001510"
    assert written.loc[0, "provider_symbol"] == "001510.KS"
