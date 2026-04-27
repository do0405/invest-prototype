from __future__ import annotations

import inspect
import shutil
from datetime import datetime

import pandas as pd

import data_collectors.kr_ohlcv_collector as collector
from data_collectors.kr_ohlcv_collector import _fetch_kr_ohlcv_with_fallback, _normalize_kr_ohlcv_frame
from tests._paths import runtime_root


def test_kr_ohlcv_public_signatures_remove_legacy_stock_module() -> None:
    collect_parameters = inspect.signature(collector.collect_kr_ohlcv_csv).parameters
    benchmark_parameters = inspect.signature(collector._collect_index_benchmarks).parameters

    assert "stock_module" not in collect_parameters
    assert "stock_module" not in benchmark_parameters


def test_resolve_market_day_does_not_use_intraday_session_before_close() -> None:
    before_close = collector._resolve_market_day(datetime(2026, 4, 21, 15, 0))
    after_close = collector._resolve_market_day(datetime(2026, 4, 21, 15, 31))

    assert before_close.strftime("%Y-%m-%d") == "2026-04-20"
    assert after_close.strftime("%Y-%m-%d") == "2026-04-21"


def test_normalize_kr_ohlcv_frame_with_korean_headers():
    raw = pd.DataFrame(
        {
            "시가": [10, 11],
            "고가": [11, 12],
            "저가": [9, 10],
            "종가": [10.5, 11.5],
            "거래량": [1000, 2000],
        },
        index=pd.to_datetime(["2026-01-01", "2026-01-02"]),
    )

    frame = _normalize_kr_ohlcv_frame(raw, ticker="005930")
    assert list(frame.columns) == [
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
    ]
    assert frame.iloc[-1]["symbol"] == "005930"
    assert float(frame.iloc[-1]["close"]) == 11.5
    assert float(frame.iloc[-1]["adj_close"]) == 11.5
    assert float(frame.iloc[-1]["dividends"]) == 0.0
    assert float(frame.iloc[-1]["stock_splits"]) == 0.0
    assert float(frame.iloc[-1]["split_factor"]) == 1.0


def test_fetch_kr_ohlcv_with_fallback_prefers_fdr_and_defaults_action_columns(monkeypatch):
    class _FDR:
        @staticmethod
        def DataReader(symbol: str, start: str | None = None, end: str | None = None) -> pd.DataFrame:
            assert symbol == "005930"
            assert start == "2026-02-01"
            assert end == "2026-02-24"
            return pd.DataFrame(
                {
                    "Open": [100, 101],
                    "High": [102, 103],
                    "Low": [99, 100],
                    "Close": [101, 102],
                    "Volume": [1000, 1200],
                },
                index=pd.to_datetime(["2026-02-21", "2026-02-24"]),
            )

    monkeypatch.setattr(
        collector,
        "_fetch_kr_ohlcv_via_yfinance",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("yfinance fallback should not run when FDR succeeds")),
    )

    frame, source, error = _fetch_kr_ohlcv_with_fallback(
        ticker="005930",
        start_dt=datetime(2026, 2, 1),
        end_dt=datetime(2026, 2, 24),
        end_yyyymmdd="20260224",
        stock_client=None,
        fdr_module=_FDR(),
        provider_mode="yfinance_only",
    )

    assert error is None
    assert source == "fdr_ohlcv:005930"
    assert list(frame["date"]) == ["2026-02-21", "2026-02-24"]
    assert list(frame["symbol"]) == ["005930", "005930"]
    assert list(frame["adj_close"]) == [101.0, 102.0]
    assert list(frame["dividends"]) == [0.0, 0.0]
    assert list(frame["stock_splits"]) == [0.0, 0.0]
    assert list(frame["split_factor"]) == [1.0, 1.0]


def test_collect_kr_ohlcv_csv_writes_state_and_rechecks_completed_same_day(monkeypatch):
    root = runtime_root("_test_runtime_kr_ohlcv_resume")
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    data_dir = root / "kr"
    data_dir.mkdir(parents=True, exist_ok=True)
    calls: list[str] = []

    def _fake_fetch(**kwargs):  # noqa: ANN003
        ticker = kwargs["ticker"]
        calls.append(ticker)
        if ticker == "005930":
            return (
                pd.DataFrame(
                    {
                        "date": ["2026-04-21"],
                        "symbol": [ticker],
                        "open": [10.0],
                        "high": [11.0],
                        "low": [9.0],
                        "close": [10.5],
                        "volume": [1000],
                        "adj_close": [10.5],
                        "dividends": [0.0],
                        "stock_splits": [0.0],
                        "split_factor": [1.0],
                    }
                ),
                "fdr_ohlcv:005930",
                None,
            )
        return pd.DataFrame(), "unavailable", "429 Too Many Requests"

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector, "wait_for_yahoo_request_slot", lambda *args, **kwargs: None)
    monkeypatch.setattr(collector.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(collector, "_fetch_kr_ohlcv_with_fallback", _fake_fetch)
    monkeypatch.setattr(collector, "_collect_index_benchmarks", lambda **kwargs: [])

    first = collector.collect_kr_ohlcv_csv(
        output_dir=str(data_dir),
        tickers=["005930", "000660"],
        as_of=datetime(2026, 4, 21, 16, 0),
    )

    calls.clear()

    def _second_fetch(**kwargs):  # noqa: ANN003
        ticker = kwargs["ticker"]
        calls.append(ticker)
        return (
            pd.DataFrame(
                {
                    "date": ["2026-04-21"],
                    "symbol": [ticker],
                    "open": [20.0],
                    "high": [21.0],
                    "low": [19.0],
                    "close": [20.5],
                    "volume": [2000],
                    "adj_close": [20.5],
                    "dividends": [0.0],
                    "stock_splits": [0.0],
                    "split_factor": [1.0],
                }
            ),
            f"fdr_ohlcv:{ticker}",
            None,
        )

    monkeypatch.setattr(collector, "_fetch_kr_ohlcv_with_fallback", _second_fetch)

    second = collector.collect_kr_ohlcv_csv(
        output_dir=str(data_dir),
        tickers=["005930", "000660"],
        as_of=datetime(2026, 4, 21, 16, 0),
    )

    state_path = root / "results" / "kr" / "runtime" / "collector_run_state.json"
    state = pd.read_json(state_path, typ="series")
    assert first["market"] == "kr"
    assert first["ok"] is False
    assert first["status"] == "degraded"
    assert first["retryable"] is True
    assert state["status_counts"]["saved"] == 2
    assert "005930" in state["completed_symbols"]
    assert "000660" in state["completed_symbols"]
    assert calls == ["000660", "005930"]
    assert second["ok"] is True
    assert second["status"] == "ok"
