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
    assert {"date", "Open", "High", "Low", "Close", "Volume", "symbol"}.issubset(set(cached.columns))
    assert str(cached.iloc[-1]["symbol"]) == "ZZZ"


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
    sample_ohlcv = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2026-02-10", "2026-02-11", "2026-02-12"], utc=True),
            "Open": [30.0, 31.0, 32.0],
            "High": [31.0, 32.0, 33.0],
            "Low": [29.0, 30.0, 31.0],
            "Close": [30.5, 31.5, 32.5],
            "Volume": [1000, 1200, 1800],
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
    assert bool(result.loc[result["symbol"] == "AAA", "pattern_included"].iloc[0]) is True
    assert bool(result.loc[result["symbol"] == "AAA", "actionable_pattern_pass"].iloc[0]) is True
    assert (results_dir / "integrated_without_patterns.csv").exists()
    assert (results_dir / "integrated_with_patterns.csv").exists()
    assert (results_dir / "integrated_actionable_patterns.csv").exists()
    assert (results_dir / "actual_data_pattern_calibration.json").exists()
