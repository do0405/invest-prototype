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
