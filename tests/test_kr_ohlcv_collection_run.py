from __future__ import annotations

import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import cast

import pandas as pd
import pytest

import data_collectors.kr_ohlcv_collector as collector
from data_collectors.kr_ohlcv_collector import collect_kr_ohlcv_csv
from tests._paths import runtime_root


def _reset_runtime_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def _disable_kr_waits(monkeypatch) -> None:
    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(collector, "wait_for_yahoo_request_slot", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(collector, "extend_yahoo_cooldown", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(collector, "_collect_index_benchmarks", lambda *args, **kwargs: [])


def _frame_for(ticker: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2026-02-21", "2026-02-24"],
            "symbol": [ticker, ticker],
            "open": [100, 101],
            "high": [105, 106],
            "low": [99, 100],
            "close": [103, 104],
            "volume": [10000, 12000],
        }
    )


def test_collect_kr_ohlcv_csv_uses_request_delay_between_chunks(monkeypatch):
    runtime_dir = runtime_root("_test_runtime_kr_chunk_pause")
    _reset_runtime_dir(runtime_dir)
    sleeps: list[float] = []

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector, "wait_for_yahoo_request_slot", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(collector, "extend_yahoo_cooldown", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(collector, "_collect_index_benchmarks", lambda *args, **kwargs: [])
    monkeypatch.setattr(collector, "KR_OHLCV_CHUNK_SIZE", 2)
    monkeypatch.setattr(collector.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(collector, "load_kr_symbol_universe", lambda **kwargs: ["005930", "000660", "035720"])
    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_with_fallback",
        lambda **kwargs: (_frame_for(kwargs["ticker"]), f"yfinance:{kwargs['ticker']}.KS", None),
    )

    collect_kr_ohlcv_csv(
        days=30,
        include_kosdaq=True,
        output_dir=str(runtime_dir),
        as_of=datetime(2026, 2, 24),
    )

    assert sleeps == [collector.KR_OHLCV_REQUEST_DELAY_SECONDS]


def test_collect_kr_ohlcv_csv_skips_chunk_pause_when_all_latest(monkeypatch):
    runtime_dir = runtime_root("_test_runtime_kr_collect_latest_pause_skip")
    _reset_runtime_dir(runtime_dir)
    sleeps: list[float] = []

    def _future_cached_frame(ticker: str) -> pd.DataFrame:
        dates = pd.date_range(end="2026-03-05", periods=collector.KR_OHLCV_TARGET_BARS, freq="D")
        return pd.DataFrame(
            {
                "date": dates,
                "symbol": [ticker] * len(dates),
                "open": range(100, 100 + len(dates)),
                "high": range(101, 101 + len(dates)),
                "low": range(99, 99 + len(dates)),
                "close": range(100, 100 + len(dates)),
                "volume": range(1000, 1000 + len(dates)),
            }
        )

    for ticker in ("005930", "000660"):
        _future_cached_frame(ticker).to_csv(runtime_dir / f"{ticker}.csv", index=False)

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector, "wait_for_yahoo_request_slot", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(collector, "extend_yahoo_cooldown", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(collector, "_collect_index_benchmarks", lambda *args, **kwargs: [])
    monkeypatch.setattr(collector, "load_kr_symbol_universe", lambda **kwargs: ["005930", "000660"])
    monkeypatch.setattr(collector, "KR_OHLCV_CHUNK_SIZE", 1)
    monkeypatch.setattr(collector.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_with_fallback",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("latest chunks must not fetch")),
    )

    summary = collect_kr_ohlcv_csv(
        days=30,
        include_kosdaq=True,
        output_dir=str(runtime_dir),
        as_of=datetime(2026, 2, 24),
    )

    assert summary["latest"] == 2
    assert sleeps == []


def test_collect_kr_ohlcv_csv_summary_and_canonical_outputs(monkeypatch):
    _disable_kr_waits(monkeypatch)
    runtime_dir = runtime_root("_test_runtime_kr_collect")
    _reset_runtime_dir(runtime_dir)

    monkeypatch.setattr(collector, "load_kr_symbol_universe", lambda **kwargs: ["005930", "035720"])
    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_with_fallback",
        lambda **kwargs: (_frame_for(kwargs["ticker"]), "yfinance:005930.KS", None) if kwargs["ticker"] == "005930" else (pd.DataFrame(), "unavailable", "unexpected failure"),
    )

    summary = collect_kr_ohlcv_csv(
        days=30,
        include_kosdaq=True,
        output_dir=str(runtime_dir),
        as_of=datetime(2026, 2, 24),
    )

    assert summary["schema_version"] == "1.0"
    assert summary["source"] == "yfinance"
    assert summary["market"] == "kr"
    assert summary["total"] == 2
    assert summary["saved"] == 1
    assert summary["failed"] == 1
    failed_samples = cast(list[dict[str, object]], summary["failed_samples"])
    assert failed_samples[0]["ticker"] == "035720"

    saved_path = runtime_dir / "005930.csv"
    assert saved_path.exists()
    df = pd.read_csv(saved_path)
    assert [
        "date",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adj_close",
        "dividends",
        "stock_splits",
        "split_factor",
    ] == list(df.columns)
    assert str(df.iloc[0]["symbol"]).zfill(6) == "005930"


def test_collect_kr_ohlcv_csv_does_not_wait_on_yahoo_when_fdr_succeeds(monkeypatch):
    runtime_dir = runtime_root("_test_runtime_kr_fdr_no_yahoo_wait")
    _reset_runtime_dir(runtime_dir)

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(collector, "extend_yahoo_cooldown", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(collector, "_collect_index_benchmarks", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        collector,
        "wait_for_yahoo_request_slot",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("FDR primary success must not wait on Yahoo throttle")
        ),
    )
    monkeypatch.setattr(collector, "load_kr_symbol_universe", lambda **kwargs: ["005930"])
    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_via_fdr",
        lambda **kwargs: (_frame_for(kwargs["ticker"]), kwargs["ticker"], None),
    )
    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_via_yfinance",
        lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("Yahoo fallback should not run when FDR succeeds")
        ),
    )

    summary = collect_kr_ohlcv_csv(
        days=30,
        include_kosdaq=True,
        output_dir=str(runtime_dir),
        as_of=datetime(2026, 2, 24),
        fdr_module=object(),
    )

    assert summary["saved"] == 1
    assert summary["source"] == "fdr_ohlcv"


def test_collect_kr_ohlcv_csv_waits_only_for_yahoo_fallback_after_fdr_failure(monkeypatch):
    runtime_dir = runtime_root("_test_runtime_kr_fdr_fallback_yahoo_wait")
    _reset_runtime_dir(runtime_dir)
    waits: list[str] = []

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(collector, "extend_yahoo_cooldown", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(collector, "_collect_index_benchmarks", lambda *args, **kwargs: [])
    monkeypatch.setattr(collector, "wait_for_yahoo_request_slot", lambda source, **kwargs: waits.append(source) or 0.0)
    monkeypatch.setattr(collector, "load_kr_symbol_universe", lambda **kwargs: ["005930"])
    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_via_fdr",
        lambda **kwargs: (pd.DataFrame(), None, "fdr empty"),
    )
    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_via_yfinance",
        lambda **kwargs: (_frame_for(kwargs["ticker"]), "005930.KS", None),
    )

    summary = collect_kr_ohlcv_csv(
        days=30,
        include_kosdaq=True,
        output_dir=str(runtime_dir),
        as_of=datetime(2026, 2, 24),
        fdr_module=object(),
    )

    assert summary["saved"] == 1
    assert waits == ["KR OHLCV"]


def test_collect_kr_ohlcv_csv_reports_fdr_and_yahoo_lane_diagnostics(monkeypatch):
    runtime_dir = runtime_root("_test_runtime_kr_collect_diagnostics")
    _reset_runtime_dir(runtime_dir)
    waits: list[str] = []

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(collector, "KR_OHLCV_CHUNK_SIZE", 2)
    monkeypatch.setattr(collector, "extend_yahoo_cooldown", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(collector, "_collect_index_benchmarks", lambda *args, **kwargs: ["KOSPI"])
    monkeypatch.setattr(collector, "wait_for_yahoo_request_slot", lambda source, **kwargs: waits.append(source) or 0.75)
    monkeypatch.setattr(collector, "load_kr_symbol_universe", lambda **kwargs: ["005930", "000660"])
    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_via_fdr",
        lambda **kwargs: (
            (_frame_for(kwargs["ticker"]), kwargs["ticker"], None)
            if kwargs["ticker"] == "005930"
            else (pd.DataFrame(), None, "fdr empty")
        ),
    )
    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_via_yfinance",
        lambda **kwargs: (_frame_for(kwargs["ticker"]), f"{kwargs['ticker']}.KS", None),
    )

    summary = collect_kr_ohlcv_csv(
        days=30,
        include_kosdaq=True,
        output_dir=str(runtime_dir),
        as_of=datetime(2026, 2, 24),
        fdr_module=object(),
    )

    timings = summary["timings"]
    counts = summary["collector_diagnostics"]["counts"]
    assert summary["saved"] == 2
    assert waits == ["KR OHLCV"]
    assert timings["provider_wait_seconds"] == 0.75
    assert timings["provider_fetch_seconds"] >= 0.0
    assert timings["index_benchmark_seconds"] >= 0.0
    assert counts["fdr_successes"] == 1
    assert counts["fdr_fallback_symbols"] == 1
    assert counts["yahoo_fallback_symbols"] == 1


def test_collect_kr_ohlcv_csv_checkpoints_state_once_per_chunk(monkeypatch):
    runtime_dir = runtime_root("_test_runtime_kr_collect_chunk_state_writes")
    _reset_runtime_dir(runtime_dir)
    writes: list[str] = []

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(collector, "KR_OHLCV_CHUNK_SIZE", 2)
    monkeypatch.setattr(collector, "wait_for_yahoo_request_slot", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(collector, "extend_yahoo_cooldown", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(collector, "_collect_index_benchmarks", lambda *args, **kwargs: [])
    monkeypatch.setattr(collector, "_write_kr_collector_run_state", lambda path, state: writes.append(str(path)))
    monkeypatch.setattr(collector, "load_kr_symbol_universe", lambda **kwargs: ["005930", "000660"])
    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_with_fallback",
        lambda **kwargs: (_frame_for(kwargs["ticker"]), f"yfinance:{kwargs['ticker']}.KS", None),
    )

    summary = collect_kr_ohlcv_csv(
        days=30,
        include_kosdaq=True,
        output_dir=str(runtime_dir),
        as_of=datetime(2026, 2, 24),
    )

    assert summary["saved"] == 2
    assert len(writes) == 3


def test_collect_kr_ohlcv_csv_respects_ticker_override(monkeypatch):
    _disable_kr_waits(monkeypatch)
    runtime_dir = runtime_root("_test_runtime_kr_collect_override")
    _reset_runtime_dir(runtime_dir)

    monkeypatch.setattr(collector, "_fetch_kr_ohlcv_with_fallback", lambda **kwargs: (_frame_for(kwargs["ticker"]), "yfinance:005930.KS", None))

    summary = collect_kr_ohlcv_csv(
        days=10,
        output_dir=str(runtime_dir),
        tickers=["005930"],
        as_of=datetime(2026, 2, 24),
    )

    assert summary["total"] == 1
    assert summary["saved"] == 1
    assert summary["failed"] == 0
    assert (runtime_dir / "005930.csv").exists()


def test_collect_kr_ohlcv_csv_refetches_overlap_for_latest_existing_file(monkeypatch):
    _disable_kr_waits(monkeypatch)
    runtime_dir = runtime_root("_test_runtime_kr_collect_latest")
    _reset_runtime_dir(runtime_dir)
    existing_dates = pd.date_range(end="2026-02-24", periods=collector.KR_OHLCV_TARGET_BARS, freq="D")
    existing = pd.DataFrame(
        {
            "date": existing_dates.strftime("%Y-%m-%d"),
            "symbol": ["005930"] * len(existing_dates),
            "open": range(100, 100 + len(existing_dates)),
            "high": range(101, 101 + len(existing_dates)),
            "low": range(99, 99 + len(existing_dates)),
            "close": range(100, 100 + len(existing_dates)),
            "volume": range(1000, 1000 + len(existing_dates)),
        }
    )
    existing.to_csv(runtime_dir / "005930.csv", index=False)

    captured: dict[str, datetime] = {}

    def _fetch(**kwargs):
        captured["start_dt"] = cast(datetime, kwargs["start_dt"])
        return (
            pd.DataFrame(
                {
                    "date": ["2026-02-23", "2026-02-24"],
                    "symbol": [kwargs["ticker"], kwargs["ticker"]],
                    "open": [100, 101],
                    "high": [105, 107],
                    "low": [99, 100],
                    "close": [103, 105],
                    "volume": [10000, 13000],
                }
            ),
            "yfinance:005930.KS",
            None,
        )

    monkeypatch.setattr(collector, "_fetch_kr_ohlcv_with_fallback", _fetch)

    summary = collect_kr_ohlcv_csv(
        days=30,
        output_dir=str(runtime_dir),
        tickers=["005930"],
        as_of=datetime(2026, 2, 24),
    )

    assert summary["saved"] == 1
    assert summary["latest"] == 0
    assert captured["start_dt"].strftime("%Y-%m-%d") == "2026-02-23"
    saved = pd.read_csv(runtime_dir / "005930.csv")
    assert list(saved["date"].tail(2)) == ["2026-02-23", "2026-02-24"]
    assert int(saved.iloc[-1]["volume"]) == 13000


def test_collect_kr_ohlcv_csv_rechecks_completed_same_day_symbols(monkeypatch):
    _disable_kr_waits(monkeypatch)
    runtime_dir = runtime_root("_test_runtime_kr_collect_run_state_recheck")
    _reset_runtime_dir(runtime_dir)

    calls: list[str] = []

    def _fetch(**kwargs):
        ticker = str(kwargs["ticker"])
        calls.append(ticker)
        return (_frame_for(ticker), f"yfinance:{ticker}.KS", None)

    monkeypatch.setattr(collector, "_fetch_kr_ohlcv_with_fallback", _fetch)

    collect_kr_ohlcv_csv(
        days=30,
        output_dir=str(runtime_dir),
        tickers=["005930"],
        as_of=datetime(2026, 2, 24),
    )
    collect_kr_ohlcv_csv(
        days=30,
        output_dir=str(runtime_dir),
        tickers=["005930"],
        as_of=datetime(2026, 2, 24),
    )

    assert calls == ["005930", "005930"]


def test_collect_kr_ohlcv_csv_backfills_short_cached_history(monkeypatch):
    _disable_kr_waits(monkeypatch)
    runtime_dir = runtime_root("_test_runtime_kr_collect_backfill")
    _reset_runtime_dir(runtime_dir)
    existing = _frame_for("005930")
    existing.to_csv(runtime_dir / "005930.csv", index=False)

    captured: dict[str, object] = {}

    def _fetch(**kwargs):
        captured["start_dt"] = kwargs["start_dt"]
        return (_frame_for(kwargs["ticker"]), "yfinance:005930.KS", None)

    monkeypatch.setattr(collector, "_fetch_kr_ohlcv_with_fallback", _fetch)

    as_of = datetime(2026, 2, 24)
    summary = collect_kr_ohlcv_csv(
        days=30,
        output_dir=str(runtime_dir),
        tickers=["005930"],
        as_of=as_of,
    )

    assert summary["saved"] == 1
    assert captured["start_dt"] == as_of - timedelta(days=30)


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

    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_with_fallback",
        lambda **kwargs: (
            pd.DataFrame(
                {
                    "date": ["2026-02-24"],
                    "symbol": [kwargs["ticker"]],
                    "open": [101],
                    "high": [106],
                    "low": [100],
                    "close": [104],
                    "volume": [12000],
                }
            ),
            "yfinance:005930.KS",
            None,
        ),
    )

    summary = collect_kr_ohlcv_csv(
        days=30,
        output_dir=str(runtime_dir),
        tickers=["005930"],
        as_of=datetime(2026, 2, 24),
    )

    assert summary["saved"] == 1
    saved = pd.read_csv(runtime_dir / "005930.csv")
    assert list(saved["date"]) == ["2026-02-21", "2026-02-24"]


def test_collect_kr_ohlcv_csv_keeps_existing_on_soft_unavailable(monkeypatch):
    _disable_kr_waits(monkeypatch)
    runtime_dir = runtime_root("_test_runtime_kr_collect_soft_keep")
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

    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_with_fallback",
        lambda **kwargs: (pd.DataFrame(), "unavailable", "possibly delisted; no timezone found"),
    )

    summary = collect_kr_ohlcv_csv(
        days=30,
        output_dir=str(runtime_dir),
        tickers=["005930"],
        as_of=datetime(2026, 2, 24),
    )

    assert summary["kept_existing"] == 1
    assert summary["soft_unavailable"] == 0
    saved = pd.read_csv(runtime_dir / "005930.csv")
    assert len(saved) == 1


def test_collect_kr_ohlcv_csv_counts_soft_unavailable_without_existing(monkeypatch):
    _disable_kr_waits(monkeypatch)
    runtime_dir = runtime_root("_test_runtime_kr_collect_soft_new")
    _reset_runtime_dir(runtime_dir)

    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_with_fallback",
        lambda **kwargs: (pd.DataFrame(), "unavailable", "possibly delisted; no timezone found"),
    )

    summary = collect_kr_ohlcv_csv(
        days=30,
        output_dir=str(runtime_dir),
        tickers=["005930"],
        as_of=datetime(2026, 2, 24),
    )

    assert summary["soft_unavailable"] == 1
    assert not (runtime_dir / "005930.csv").exists()


def test_collect_kr_ohlcv_csv_counts_rate_limited_without_existing(monkeypatch):
    cooldowns: list[float] = []
    _disable_kr_waits(monkeypatch)
    monkeypatch.setattr(collector, "extend_yahoo_cooldown", lambda source, seconds: cooldowns.append(seconds))
    runtime_dir = runtime_root("_test_runtime_kr_collect_rate_limit")
    _reset_runtime_dir(runtime_dir)

    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_with_fallback",
        lambda **kwargs: (pd.DataFrame(), "unavailable", "HTTP 429 Too Many Requests"),
    )

    summary = collect_kr_ohlcv_csv(
        days=30,
        output_dir=str(runtime_dir),
        tickers=["005930"],
        as_of=datetime(2026, 2, 24),
    )

    assert summary["rate_limited"] == 1
    assert summary["failed"] == 0
    assert cooldowns == [collector.KR_OHLCV_RATE_LIMIT_COOLDOWN_SECONDS, collector.KR_OHLCV_RATE_LIMIT_COOLDOWN_SECONDS]
    assert not (runtime_dir / "005930.csv").exists()


def test_collect_kr_ohlcv_csv_raises_when_universe_is_empty(monkeypatch):
    _disable_kr_waits(monkeypatch)
    runtime_dir = runtime_root("_test_runtime_kr_collect_empty_universe")
    _reset_runtime_dir(runtime_dir)

    monkeypatch.setattr(collector, "load_kr_symbol_universe", lambda **kwargs: [])

    with pytest.raises(RuntimeError, match="KR ticker universe is empty"):
        collect_kr_ohlcv_csv(days=30, output_dir=str(runtime_dir), as_of=datetime(2026, 2, 24))


def test_collect_index_benchmarks_prefers_fdr_and_keeps_canonical_symbol_names(monkeypatch):
    runtime_dir = runtime_root("_test_runtime_kr_collect_fdr_indexes")
    _reset_runtime_dir(runtime_dir)

    class _FDR:
        @staticmethod
        def DataReader(symbol: str, start: str | None = None, end: str | None = None) -> pd.DataFrame:
            assert start == "2026-02-01"
            assert end == "2026-02-24"
            if symbol == "KS11":
                close = [2600, 2610]
            elif symbol == "KQ11":
                close = [850, 860]
            else:
                raise AssertionError(f"unexpected index symbol: {symbol}")
            return pd.DataFrame(
                {
                    "Open": close,
                    "High": [value + 5 for value in close],
                    "Low": [value - 5 for value in close],
                    "Close": close,
                    "Volume": [100000, 120000],
                },
                index=pd.to_datetime(["2026-02-21", "2026-02-24"]),
            )

    monkeypatch.setattr(
        collector,
        "_fetch_yfinance_index_ohlcv",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("yfinance index fallback should not run when FDR succeeds")),
    )

    saved = collector._collect_index_benchmarks(
        start_yyyymmdd="20260201",
        end_yyyymmdd="20260224",
        target_dir=str(runtime_dir),
        fdr_module=_FDR(),
    )

    assert saved == ["KOSPI", "KOSDAQ"]
    kospi = pd.read_csv(runtime_dir / "KOSPI.csv")
    kosdaq = pd.read_csv(runtime_dir / "KOSDAQ.csv")
    assert list(kospi["symbol"]) == ["KOSPI", "KOSPI"]
    assert list(kosdaq["symbol"]) == ["KOSDAQ", "KOSDAQ"]
