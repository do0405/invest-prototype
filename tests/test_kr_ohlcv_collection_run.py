from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

import data_collectors.kr_ohlcv_collector as collector
from tests._paths import runtime_root
from data_collectors.kr_ohlcv_collector import collect_kr_ohlcv_csv


def _reset_runtime_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def _disable_kr_waits(monkeypatch) -> None:
    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(collector, "KR_OHLCV_REQUEST_DELAY_SECONDS", 0.0)
    monkeypatch.setattr(collector, "KR_OHLCV_EMPTY_RETRY_DELAY_SECONDS", 0.0)
    monkeypatch.setattr(collector, "KR_OHLCV_CHUNK_PAUSE_SECONDS", 0.0)
    monkeypatch.setattr(collector, "KR_OHLCV_RATE_LIMIT_COOLDOWN_SECONDS", 0.0)
    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_via_naver",
        lambda **kwargs: (pd.DataFrame(), "Naver Finance unavailable"),
    )
    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_via_fdr",
        lambda *args, **kwargs: (pd.DataFrame(), None, "FinanceDataReader unavailable"),
    )
    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_via_yfinance",
        lambda *args, **kwargs: (pd.DataFrame(), None, "yfinance unavailable"),
    )


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


class _LatestOnlyStockModule(_FakeStockModule):
    def get_market_ohlcv_by_date(self, start: str, end: str, ticker: str):  # noqa: ANN001
        raise AssertionError(f"unexpected fetch for latest ticker {ticker}: {start}->{end}")


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


class _FakeStockModuleWithEtfAndEtn(_FakeStockModuleWithEtf):
    def get_market_ticker_list(self, as_of_yyyymmdd: str, market: str):  # noqa: ANN001
        tickers = super().get_market_ticker_list(as_of_yyyymmdd, market)
        if market == "ETN":
            return ["530001"]
        return tickers


class _IncrementalStockModule(_FakeStockModule):
    def get_market_ohlcv_by_date(self, start: str, end: str, ticker: str):  # noqa: ANN001
        _ = end
        assert start == "20260222"
        return pd.DataFrame(
            {
                "날짜": ["2026-02-24"],
                "시가": [101],
                "고가": [106],
                "저가": [100],
                "종가": [104],
                "거래량": [12000],
            }
        )


class _EmptyStockModule(_FakeStockModule):
    def get_market_ohlcv_by_date(self, start: str, end: str, ticker: str):  # noqa: ANN001
        _ = (start, end, ticker)
        return pd.DataFrame(columns=["날짜", "시가", "고가", "저가", "종가", "거래량"])


class _RateLimitedStockModule(_FakeStockModule):
    def get_market_ohlcv_by_date(self, start: str, end: str, ticker: str):  # noqa: ANN001
        _ = (start, end, ticker)
        raise RuntimeError("HTTP 429 Too Many Requests")


class _BrokenUniverseStockModule(_FakeStockModule):
    def get_market_ticker_list(self, as_of_yyyymmdd: str, market: str):  # noqa: ANN001
        _ = as_of_yyyymmdd
        raise RuntimeError(f"universe fetch failed for {market}")


class _BrokenPykrxStockModule(_BrokenUniverseStockModule):
    def get_market_ohlcv_by_date(self, start: str, end: str, ticker: str):  # noqa: ANN001
        _ = (start, end, ticker)
        raise RuntimeError("pykrx connection failed")


class _FakeFdrModule:
    @staticmethod
    def StockListing(name: str):  # noqa: ANN001
        if name == "KOSPI":
            return pd.DataFrame({"Symbol": ["005930"]})
        if name == "KOSDAQ":
            return pd.DataFrame({"Symbol": ["035720"]})
        if name == "ETF/KR":
            return pd.DataFrame({"Symbol": ["069500"]})
        return pd.DataFrame()

    @staticmethod
    def DataReader(symbol: str, start: str, end: str):  # noqa: ANN001
        _ = (start, end)
        if symbol in {"005930", "KRX:005930"}:
            return pd.DataFrame(
                {
                    "Date": ["2026-02-21", "2026-02-24"],
                    "Open": [100, 101],
                    "High": [105, 106],
                    "Low": [99, 100],
                    "Close": [103, 104],
                    "Volume": [10000, 12000],
                }
            )
        raise RuntimeError(f"unexpected FDR symbol {symbol}")


def _fake_naver_daily_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "날짜": ["2026-02-21", "2026-02-24"],
            "시가": [100, 101],
            "고가": [105, 106],
            "저가": [99, 100],
            "종가": [103, 104],
            "거래량": [10000, 12000],
        }
    )


def test_collect_kr_ohlcv_csv_summary_and_canonical_outputs(monkeypatch):
    _disable_kr_waits(monkeypatch)
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


def test_collect_kr_ohlcv_csv_respects_ticker_override(monkeypatch):
    _disable_kr_waits(monkeypatch)
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


def test_collect_kr_ohlcv_csv_includes_etf_universe_by_default(monkeypatch):
    _disable_kr_waits(monkeypatch)
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


def test_collect_kr_ohlcv_csv_includes_etn_universe_by_default(monkeypatch):
    _disable_kr_waits(monkeypatch)
    runtime_dir = runtime_root("_test_runtime_kr_collect_etn")
    _reset_runtime_dir(runtime_dir)

    summary = collect_kr_ohlcv_csv(
        days=10,
        include_kosdaq=False,
        stock_module=_FakeStockModuleWithEtfAndEtn(),
        output_dir=str(runtime_dir),
        as_of=datetime(2026, 2, 24),
    )

    assert summary["include_etn"] is True
    assert summary["total"] == 4
    assert summary["saved"] == 4
    assert (runtime_dir / "530001.csv").exists()


def test_collect_kr_ohlcv_csv_can_disable_etf_universe(monkeypatch):
    _disable_kr_waits(monkeypatch)
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


def test_collect_kr_ohlcv_csv_skips_latest_existing_file(monkeypatch):
    _disable_kr_waits(monkeypatch)
    runtime_dir = runtime_root("_test_runtime_kr_collect_latest")
    _reset_runtime_dir(runtime_dir)
    existing = pd.DataFrame(
        {
            "date": ["2026-02-21", "2026-02-24"],
            "symbol": ["005930", "005930"],
            "open": [100, 101],
            "high": [105, 106],
            "low": [99, 100],
            "close": [103, 104],
            "volume": [10000, 12000],
        }
    )
    existing.to_csv(runtime_dir / "005930.csv", index=False)

    summary = collect_kr_ohlcv_csv(
        days=30,
        include_kosdaq=False,
        stock_module=_LatestOnlyStockModule(),
        output_dir=str(runtime_dir),
        tickers=["005930"],
        as_of=datetime(2026, 2, 24),
    )

    assert summary["latest"] == 1
    assert summary["saved"] == 0
    saved = pd.read_csv(runtime_dir / "005930.csv")
    assert len(saved) == 2


def test_collect_kr_ohlcv_csv_merges_incremental_data(monkeypatch):
    _disable_kr_waits(monkeypatch)
    runtime_dir = runtime_root("_test_runtime_kr_collect_incremental")
    _reset_runtime_dir(runtime_dir)
    existing = pd.DataFrame(
        {
            "date": ["2026-02-21"],
            "symbol": ["005930"],
            "open": [100],
            "high": [105],
            "low": [99],
            "close": [103],
            "volume": [10000],
        }
    )
    existing.to_csv(runtime_dir / "005930.csv", index=False)

    summary = collect_kr_ohlcv_csv(
        days=30,
        include_kosdaq=False,
        stock_module=_IncrementalStockModule(),
        output_dir=str(runtime_dir),
        tickers=["005930"],
        as_of=datetime(2026, 2, 24),
    )

    assert summary["saved"] == 1
    saved = pd.read_csv(runtime_dir / "005930.csv")
    assert list(saved["date"]) == ["2026-02-21", "2026-02-24"]


def test_collect_kr_ohlcv_csv_keeps_existing_on_empty_response(monkeypatch):
    _disable_kr_waits(monkeypatch)
    runtime_dir = runtime_root("_test_runtime_kr_collect_empty_keep")
    _reset_runtime_dir(runtime_dir)
    existing = pd.DataFrame(
        {
            "date": ["2026-02-21"],
            "symbol": ["005930"],
            "open": [100],
            "high": [105],
            "low": [99],
            "close": [103],
            "volume": [10000],
        }
    )
    existing.to_csv(runtime_dir / "005930.csv", index=False)

    summary = collect_kr_ohlcv_csv(
        days=30,
        include_kosdaq=False,
        stock_module=_EmptyStockModule(),
        output_dir=str(runtime_dir),
        tickers=["005930"],
        as_of=datetime(2026, 2, 24),
    )

    assert summary["kept_existing"] == 1
    assert summary["skipped_empty"] == 0
    saved = pd.read_csv(runtime_dir / "005930.csv")
    assert list(saved["date"]) == ["2026-02-21"]


def test_collect_kr_ohlcv_csv_counts_rate_limited_without_existing(monkeypatch):
    _disable_kr_waits(monkeypatch)
    runtime_dir = runtime_root("_test_runtime_kr_collect_rate_limit")
    _reset_runtime_dir(runtime_dir)

    summary = collect_kr_ohlcv_csv(
        days=30,
        include_kosdaq=False,
        stock_module=_RateLimitedStockModule(),
        output_dir=str(runtime_dir),
        tickers=["005930"],
        as_of=datetime(2026, 2, 24),
    )

    assert summary["rate_limited"] == 1
    assert summary["failed"] == 0
    assert not (runtime_dir / "005930.csv").exists()


def test_collect_kr_ohlcv_csv_raises_when_universe_resolution_fails(monkeypatch):
    _disable_kr_waits(monkeypatch)
    runtime_dir = runtime_root("_test_runtime_kr_collect_empty_universe")
    _reset_runtime_dir(runtime_dir)

    with pytest.raises(RuntimeError, match="KR ticker universe is empty"):
        collect_kr_ohlcv_csv(
            days=30,
            include_kosdaq=True,
            stock_module=_BrokenUniverseStockModule(),
            output_dir=str(runtime_dir),
            as_of=datetime(2026, 2, 24),
        )


def test_collect_kr_ohlcv_csv_uses_fdr_fallback_when_pykrx_unavailable(monkeypatch):
    _disable_kr_waits(monkeypatch)
    runtime_dir = runtime_root("_test_runtime_kr_collect_fdr_fallback")
    _reset_runtime_dir(runtime_dir)
    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_via_fdr",
        lambda fdr_module, **kwargs: (
            collector._normalize_kr_ohlcv_frame(
                _FakeFdrModule.DataReader(kwargs["ticker"], kwargs["start_dt"].strftime("%Y-%m-%d"), kwargs["end_dt"].strftime("%Y-%m-%d")),
                kwargs["ticker"],
            ),
            kwargs["ticker"],
            None,
        ),
    )

    summary = collect_kr_ohlcv_csv(
        days=30,
        include_kosdaq=False,
        stock_module=_BrokenPykrxStockModule(),
        fdr_module=_FakeFdrModule(),
        output_dir=str(runtime_dir),
        tickers=["005930"],
        as_of=datetime(2026, 2, 24),
    )

    assert summary["saved"] == 1
    assert "fdr" in summary["sources_used"]
    saved = pd.read_csv(runtime_dir / "005930.csv")
    assert list(saved["date"]) == ["2026-02-21", "2026-02-24"]


def test_collect_kr_ohlcv_csv_uses_naver_fallback_when_pykrx_unavailable(monkeypatch):
    _disable_kr_waits(monkeypatch)
    runtime_dir = runtime_root("_test_runtime_kr_collect_naver_fallback")
    _reset_runtime_dir(runtime_dir)
    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_via_naver",
        lambda **kwargs: (
            collector._normalize_kr_ohlcv_frame(_fake_naver_daily_frame(), kwargs["ticker"]),
            None,
        ),
    )

    summary = collect_kr_ohlcv_csv(
        days=30,
        include_kosdaq=False,
        stock_module=_BrokenPykrxStockModule(),
        output_dir=str(runtime_dir),
        tickers=["005930"],
        as_of=datetime(2026, 2, 24),
    )

    assert summary["saved"] == 1
    assert "naver" in summary["sources_used"]
    saved = pd.read_csv(runtime_dir / "005930.csv")
    assert list(saved["date"]) == ["2026-02-21", "2026-02-24"]


