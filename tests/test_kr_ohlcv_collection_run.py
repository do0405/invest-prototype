from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

from tests._paths import runtime_root
from data_collectors.kr_ohlcv_collector import collect_kr_ohlcv_csv


def _reset_runtime_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


class _FakeStockModule:
    def get_nearest_business_day_in_a_week(self, day_str: str) -> str:
        _ = day_str
        return "20260224"

    def get_market_ticker_list(self, as_of_yyyymmdd: str, market: str):  # noqa: ANN001
        _ = as_of_yyyymmdd
        if market == "KOSPI":
            return ["005930"]
        if market == "KOSDAQ":
            return ["035720"]
        return []

    def get_market_ohlcv_by_date(self, start: str, end: str, ticker: str):  # noqa: ANN001
        _ = (start, end)
        if ticker == "035720":
            raise RuntimeError("simulated_kr_fetch_failure")

        frame = pd.DataFrame(
            {
                "\ub0a0\uc9dc": ["2026-02-21", "2026-02-24"],
                "\uc2dc\uac00": [100, 101],
                "\uace0\uac00": [105, 106],
                "\uc800\uac00": [99, 100],
                "\uc885\uac00": [103, 104],
                "\uac70\ub798\ub7c9": [10000, 12000],
            }
        )
        return frame


class _FakeStockModuleWithEtf(_FakeStockModule):
    def get_market_ticker_list(self, as_of_yyyymmdd: str, market: str):  # noqa: ANN001
        _ = as_of_yyyymmdd
        if market == "KOSPI":
            return ["005930"]
        if market == "KOSDAQ":
            return []
        if market == "ETF":
            return ["069500", "114800"]
        return []


def test_collect_kr_ohlcv_csv_summary_and_canonical_outputs():
    runtime_dir = runtime_root("_test_runtime_kr_collect")
    _reset_runtime_dir(runtime_dir)
    summary = collect_kr_ohlcv_csv(
        days=30,
        include_kosdaq=True,
        stock_module=_FakeStockModule(),
        output_dir=str(runtime_dir),
        as_of=datetime(2026, 2, 24),
    )

    assert summary["schema_version"] == "1.0"
    assert summary["source"] == "pykrx"
    assert summary["market"] == "kr"
    assert summary["total"] == 2
    assert summary["saved"] == 1
    assert summary["failed"] == 1
    assert isinstance(summary["failed_samples"], list) and len(summary["failed_samples"]) == 1
    assert summary["failed_samples"][0]["ticker"] == "035720"

    saved_path = runtime_dir / "005930.csv"
    assert saved_path.exists()
    df = pd.read_csv(saved_path)
    assert list(df.columns) == ["date", "symbol", "open", "high", "low", "close", "volume"]
    assert str(df.iloc[0]["symbol"]).zfill(6) == "005930"


def test_collect_kr_ohlcv_csv_respects_ticker_override():
    runtime_dir = runtime_root("_test_runtime_kr_collect")
    _reset_runtime_dir(runtime_dir)

    summary = collect_kr_ohlcv_csv(
        days=10,
        include_kosdaq=False,
        stock_module=_FakeStockModule(),
        output_dir=str(runtime_dir),
        tickers=["005930"],
        as_of=datetime(2026, 2, 24),
    )

    assert summary["total"] == 1
    assert summary["saved"] == 1
    assert summary["failed"] == 0
    assert (runtime_dir / "005930.csv").exists()


def test_collect_kr_ohlcv_csv_includes_etf_universe_by_default():
    runtime_dir = runtime_root("_test_runtime_kr_collect_etf")
    _reset_runtime_dir(runtime_dir)

    summary = collect_kr_ohlcv_csv(
        days=10,
        include_kosdaq=False,
        stock_module=_FakeStockModuleWithEtf(),
        output_dir=str(runtime_dir),
        as_of=datetime(2026, 2, 24),
    )

    assert summary["include_etf"] is True
    assert summary["total"] == 3
    assert summary["saved"] == 3
    assert (runtime_dir / "005930.csv").exists()
    assert (runtime_dir / "069500.csv").exists()
    assert (runtime_dir / "114800.csv").exists()


def test_collect_kr_ohlcv_csv_can_disable_etf_universe():
    runtime_dir = runtime_root("_test_runtime_kr_collect_no_etf")
    _reset_runtime_dir(runtime_dir)

    summary = collect_kr_ohlcv_csv(
        days=10,
        include_kosdaq=False,
        include_etf=False,
        stock_module=_FakeStockModuleWithEtf(),
        output_dir=str(runtime_dir),
        as_of=datetime(2026, 2, 24),
    )

    assert summary["include_etf"] is False
    assert summary["total"] == 1
    assert summary["saved"] == 1
    assert (runtime_dir / "005930.csv").exists()
    assert not (runtime_dir / "069500.csv").exists()


