from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import shutil

import pandas as pd
import pytest

import data_collector as dc
from tests._paths import runtime_root as runtime_test_root


def _price_frame(symbol: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-03-11", tz="UTC")],
            "symbol": [symbol],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [1000],
        }
    )


def _collector_run_state_path(root: Path) -> Path:
    return root / "results" / "us" / "runtime" / "collector_run_state.json"


def test_normalize_us_delisted_reason_collapses_same_no_price_data_cause():
    evok_msg = (
        "$EVOK: possibly delisted; no price data found  "
        '(1d 2025-12-18 -> 2026-03-12) (Yahoo error = "No data found, symbol may be delisted")'
    )
    mfb_msg = "$MFB: possibly delisted; no price data found  (1d 2024-12-17 -> 2026-03-12)"

    assert dc._normalize_us_delisted_reason(evok_msg) == "possibly delisted; no price data found"
    assert dc._normalize_us_delisted_reason(mfb_msg) == "possibly delisted; no price data found"


def test_normalize_us_delisted_reason_preserves_distinct_root_causes():
    assert dc._normalize_us_delisted_reason("$RBOT.W: possibly delisted; no timezone found") == (
        "possibly delisted; no timezone found"
    )
    assert dc._normalize_us_delisted_reason(
        'HTTP Error 404: {"quoteSummary":{"result":null,"error":{"code":"Not Found","description":"Quote not found for symbol: PARA"}}}'
    ) == "quote not found"


def test_is_us_rate_limit_error_matches_common_throttling_messages():
    assert dc._is_us_rate_limit_error("HTTP Error 429: Too Many Requests")
    assert dc._is_us_rate_limit_error("Too Many Requests. Rate limited. Try after a while.")


def test_format_us_chunk_summary_reports_status_counts():
    summary = dc._format_us_chunk_summary(
        145,
        2359,
        ["latest", "latest", "latest", "kept_existing", "soft_unavailable"],
    )

    assert "processed=5" in summary
    assert "saved 0" in summary
    assert "latest 3" in summary
    assert "kept 1" in summary
    assert "soft 1" in summary
    assert "delisted 0" in summary
    assert "rate_limited 0" in summary
    assert "failed 0" in summary


def test_fetch_us_single_does_not_request_income_statement(monkeypatch):
    class _Ticker:
        @property
        def income_stmt(self):  # noqa: ANN201
            raise AssertionError("income_stmt should not be requested for OHLCV collection")

        def history(self, **kwargs):  # noqa: ANN003, ANN201
            return pd.DataFrame(
                {"Close": [10.5], "Volume": [1000]},
                index=pd.DatetimeIndex(["2026-03-11"], name="date"),
            )

    monkeypatch.setattr(dc, "_wait_for_us_rate_limit_cooldown", lambda: None)
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(dc, "_configure_yfinance_logger", lambda: None)
    monkeypatch.setattr(dc.yf, "Ticker", lambda ticker: _Ticker())

    frame = dc.fetch_us_single("AAA", start=date(2026, 3, 1), end=date(2026, 3, 12))

    assert list(frame["symbol"]) == ["AAA"]
    assert "date" in frame.columns


def test_fetch_us_single_raises_rate_limit_error_for_429(monkeypatch):
    class _Ticker:
        def history(self, **kwargs):  # noqa: ANN003, ANN201
            raise RuntimeError("HTTP Error 429: Too Many Requests")

    monkeypatch.setattr(dc, "_wait_for_us_rate_limit_cooldown", lambda: None)
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(dc, "_configure_yfinance_logger", lambda: None)
    monkeypatch.setattr(dc.yf, "Ticker", lambda ticker: _Ticker())

    with pytest.raises(dc.RateLimitError):
        dc.fetch_us_single("AAA", start=date(2026, 3, 1), end=date(2026, 3, 12))


def test_fetch_and_save_us_ohlcv_chunked_refetches_overlap_window(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_overlap_us_ohlcv")
    if runtime_root.exists():
        shutil.rmtree(runtime_root, ignore_errors=True)

    data_dir = runtime_root
    save_dir = data_dir / "us"
    data_dir.mkdir(parents=True, exist_ok=True)
    save_dir.mkdir(parents=True, exist_ok=True)

    existing_dates = pd.date_range(end="2026-03-11", periods=dc.US_OHLCV_TARGET_BARS, freq="D", tz="UTC")
    existing = pd.DataFrame(
        {
            "date": existing_dates,
            "symbol": ["AAA"] * len(existing_dates),
            "open": range(10, 10 + len(existing_dates)),
            "high": range(11, 11 + len(existing_dates)),
            "low": range(9, 9 + len(existing_dates)),
            "close": range(10, 10 + len(existing_dates)),
            "volume": range(1000, 1000 + len(existing_dates)),
        }
    )
    existing.to_csv(save_dir / "AAA.csv", index=False)

    captured: dict[str, object] = {}

    def _fetch(symbol, start, end):
        captured["start"] = start
        captured["end"] = end
        return pd.DataFrame(
            {
                "date": [
                    pd.Timestamp("2026-03-11", tz="UTC"),
                    pd.Timestamp("2026-03-12", tz="UTC"),
                    pd.Timestamp("2026-03-13", tz="UTC"),
                ],
                "symbol": [symbol, symbol, symbol],
                "open": [11.1, 12.0, 13.0],
                "high": [12.1, 13.0, 14.0],
                "low": [10.1, 11.0, 12.0],
                "close": [11.9, 12.5, 13.5],
                "volume": [1200, 1300, 1400],
            }
        )

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "get_us_market_today", lambda: date(2026, 3, 12))
    monkeypatch.setattr(dc, "fetch_us_single", _fetch)

    dc.fetch_and_save_us_ohlcv_chunked(
        ["AAA"],
        save_dir=str(save_dir),
        chunk_size=1,
        pause=0.0,
        max_workers=1,
    )

    assert captured["start"] == date(2026, 3, 10)
    assert captured["end"] == date(2026, 3, 13)
    saved = pd.read_csv(save_dir / "AAA.csv")
    assert list(saved["date"].tail(2)) == ["2026-03-11 00:00:00+00:00", "2026-03-12 00:00:00+00:00"]
    assert "2026-03-13 00:00:00+00:00" not in set(saved["date"])
    assert float(saved.iloc[-2]["close"]) == 11.9


def test_fetch_and_save_us_ohlcv_chunked_discards_invalid_cached_dates(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_invalid_cached_dates_us_ohlcv")
    if runtime_root.exists():
        shutil.rmtree(runtime_root, ignore_errors=True)

    data_dir = runtime_root
    save_dir = data_dir / "us"
    data_dir.mkdir(parents=True, exist_ok=True)
    save_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "date": ["not-a-date"],
            "symbol": ["AAA"],
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [1],
        }
    ).to_csv(save_dir / "AAA.csv", index=False)

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "get_us_market_today", lambda: date(2026, 3, 12))
    monkeypatch.setattr(
        dc,
        "fetch_us_single",
        lambda symbol, start, end: pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-03-11", tz="UTC"), pd.Timestamp("2026-03-12", tz="UTC")],
                "symbol": [symbol, symbol],
                "open": [10.0, 11.0],
                "high": [11.0, 12.0],
                "low": [9.0, 10.0],
                "close": [10.5, 11.5],
                "volume": [1000, 1100],
            }
        ),
    )

    dc.fetch_and_save_us_ohlcv_chunked(
        ["AAA"],
        save_dir=str(save_dir),
        chunk_size=1,
        pause=0.0,
        max_workers=1,
    )

    saved = pd.read_csv(save_dir / "AAA.csv")
    assert list(saved["date"]) == ["2026-03-11 00:00:00+00:00", "2026-03-12 00:00:00+00:00"]
    assert not saved["date"].isna().any()


def test_fetch_and_save_us_ohlcv_chunked_backfills_short_cached_history(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_short_cached_history_us_ohlcv")
    if runtime_root.exists():
        shutil.rmtree(runtime_root, ignore_errors=True)

    data_dir = runtime_root
    save_dir = data_dir / "us"
    data_dir.mkdir(parents=True, exist_ok=True)
    save_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-03-10", tz="UTC"), pd.Timestamp("2026-03-11", tz="UTC")],
            "symbol": ["AAA", "AAA"],
            "open": [10.0, 11.0],
            "high": [11.0, 12.0],
            "low": [9.0, 10.0],
            "close": [10.5, 11.5],
            "volume": [1000, 1100],
        }
    ).to_csv(save_dir / "AAA.csv", index=False)

    today = date(2026, 3, 12)
    captured: dict[str, object] = {}

    def _fetch(symbol, start, end):
        captured["start"] = start
        return pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-03-12", tz="UTC")],
                "symbol": [symbol],
                "open": [12.0],
                "high": [13.0],
                "low": [11.0],
                "close": [12.5],
                "volume": [1300],
            }
        )

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "get_us_market_today", lambda: today)
    monkeypatch.setattr(dc, "fetch_us_single", _fetch)

    dc.fetch_and_save_us_ohlcv_chunked(
        ["AAA"],
        save_dir=str(save_dir),
        chunk_size=1,
        pause=0.0,
        max_workers=1,
    )

    assert captured["start"] == today - timedelta(days=dc.US_OHLCV_DEFAULT_LOOKBACK_DAYS)


def test_fetch_and_save_us_ohlcv_chunked_batches_same_window_new_symbols(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_us_ohlcv_batch_new_symbols")
    if runtime_root.exists():
        shutil.rmtree(runtime_root, ignore_errors=True)

    data_dir = runtime_root
    save_dir = data_dir / "us"
    data_dir.mkdir(parents=True, exist_ok=True)
    save_dir.mkdir(parents=True, exist_ok=True)

    batch_calls: list[tuple[tuple[str, ...], date, date]] = []
    single_calls: list[str] = []

    def _batch(symbols, start, end):
        batch_calls.append((tuple(symbols), start, end))
        return {
            symbol: pd.DataFrame(
                {
                    "date": [pd.Timestamp("2026-03-12", tz="UTC")],
                    "symbol": [symbol],
                    "open": [10.0],
                    "high": [11.0],
                    "low": [9.0],
                    "close": [10.5],
                    "volume": [1000],
                }
            )
            for symbol in symbols
        }

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "get_us_market_today", lambda: date(2026, 3, 12))
    monkeypatch.setattr(dc, "US_OHLCV_BATCH_SIZE", 2)
    monkeypatch.setattr(dc, "_fetch_us_batch_ohlcv", _batch)
    monkeypatch.setattr(dc, "fetch_us_single", lambda symbol, start, end: single_calls.append(symbol) or _price_frame(symbol))

    dc.fetch_and_save_us_ohlcv_chunked(
        ["AAA", "BBB"],
        save_dir=str(save_dir),
        chunk_size=2,
        pause=0.0,
        max_workers=1,
    )

    assert batch_calls == [
        (
            ("AAA", "BBB"),
            date(2026, 3, 12) - timedelta(days=dc.US_OHLCV_DEFAULT_LOOKBACK_DAYS),
            date(2026, 3, 13),
        )
    ]
    assert single_calls == []
    assert (save_dir / "AAA.csv").exists()
    assert (save_dir / "BBB.csv").exists()


def test_fetch_and_save_us_ohlcv_chunked_batches_same_window_existing_symbols(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_us_ohlcv_batch_existing_symbols")
    if runtime_root.exists():
        shutil.rmtree(runtime_root, ignore_errors=True)

    data_dir = runtime_root
    save_dir = data_dir / "us"
    data_dir.mkdir(parents=True, exist_ok=True)
    save_dir.mkdir(parents=True, exist_ok=True)

    today = date(2026, 3, 12)
    existing_dates = pd.date_range(end="2026-03-11", periods=dc.US_OHLCV_TARGET_BARS, freq="D", tz="UTC")
    for symbol in ("AAA", "BBB"):
        pd.DataFrame(
            {
                "date": existing_dates,
                "symbol": [symbol] * len(existing_dates),
                "open": range(10, 10 + len(existing_dates)),
                "high": range(11, 11 + len(existing_dates)),
                "low": range(9, 9 + len(existing_dates)),
                "close": range(10, 10 + len(existing_dates)),
                "volume": range(1000, 1000 + len(existing_dates)),
            }
        ).to_csv(save_dir / f"{symbol}.csv", index=False)

    batch_calls: list[tuple[tuple[str, ...], date, date]] = []
    single_calls: list[str] = []

    def _batch(symbols, start, end):
        batch_calls.append((tuple(symbols), start, end))
        return {
            symbol: pd.DataFrame(
                {
                    "date": [pd.Timestamp("2026-03-11", tz="UTC"), pd.Timestamp("2026-03-12", tz="UTC")],
                    "symbol": [symbol, symbol],
                    "open": [11.0, 12.0],
                    "high": [12.0, 13.0],
                    "low": [10.0, 11.0],
                    "close": [11.5, 12.5],
                    "volume": [1200, 1300],
                }
            )
            for symbol in symbols
        }

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "get_us_market_today", lambda: today)
    monkeypatch.setattr(dc, "US_OHLCV_BATCH_SIZE", 4)
    monkeypatch.setattr(dc, "_fetch_us_batch_ohlcv", _batch)
    monkeypatch.setattr(dc, "fetch_us_single", lambda symbol, start, end: single_calls.append(symbol) or _price_frame(symbol))

    dc.fetch_and_save_us_ohlcv_chunked(
        ["AAA", "BBB"],
        save_dir=str(save_dir),
        chunk_size=2,
        pause=0.0,
        max_workers=1,
    )

    assert batch_calls == [(("AAA", "BBB"), date(2026, 3, 10), date(2026, 3, 13))]
    assert single_calls == []


def test_fetch_and_save_us_ohlcv_chunked_falls_back_when_batch_omits_symbol(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_us_ohlcv_batch_missing_symbol")
    if runtime_root.exists():
        shutil.rmtree(runtime_root, ignore_errors=True)

    data_dir = runtime_root
    save_dir = data_dir / "us"
    data_dir.mkdir(parents=True, exist_ok=True)
    save_dir.mkdir(parents=True, exist_ok=True)

    batch_calls: list[tuple[str, ...]] = []
    single_calls: list[str] = []

    def _batch(symbols, start, end):
        batch_calls.append(tuple(symbols))
        return {"AAA": _price_frame("AAA")}

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "get_us_market_today", lambda: date(2026, 3, 12))
    monkeypatch.setattr(dc, "US_OHLCV_BATCH_SIZE", 2)
    monkeypatch.setattr(dc, "_fetch_us_batch_ohlcv", _batch)
    monkeypatch.setattr(dc, "fetch_us_single", lambda symbol, start, end: single_calls.append(symbol) or _price_frame(symbol))

    dc.fetch_and_save_us_ohlcv_chunked(
        ["AAA", "BBB"],
        save_dir=str(save_dir),
        chunk_size=2,
        pause=0.0,
        max_workers=1,
    )

    assert batch_calls == [("AAA", "BBB")]
    assert single_calls == ["BBB"]
    assert (save_dir / "AAA.csv").exists()
    assert (save_dir / "BBB.csv").exists()


def test_fetch_and_save_us_ohlcv_chunked_does_not_mark_rate_limited_symbol_delisted(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_rate_limit_ohlcv")
    if runtime_root.exists():
        shutil.rmtree(runtime_root, ignore_errors=True)

    data_dir = runtime_root
    save_dir = data_dir / "us"
    data_dir.mkdir(parents=True, exist_ok=True)
    save_dir.mkdir(parents=True, exist_ok=True)

    cooldowns: list[float] = []

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "get_us_market_today", lambda: date(2026, 3, 12))
    monkeypatch.setattr(dc, "fetch_us_single", lambda *args, **kwargs: (_ for _ in ()).throw(dc.RateLimitError("429")))
    monkeypatch.setattr(dc, "_extend_us_rate_limit_cooldown", lambda seconds: cooldowns.append(seconds))

    dc.fetch_and_save_us_ohlcv_chunked(
        ["AAA"],
        save_dir=str(save_dir),
        chunk_size=1,
        pause=0.0,
        max_workers=1,
    )

    assert cooldowns == [
        dc.US_OHLCV_RATE_LIMIT_COOLDOWN_SECONDS,
        dc.US_OHLCV_RATE_LIMIT_COOLDOWN_SECONDS,
    ]
    assert not (save_dir / "AAA.csv").exists()


def test_fetch_and_save_us_ohlcv_chunked_keeps_existing_data_for_delisted_response(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_delisted_existing_ohlcv")
    if runtime_root.exists():
        shutil.rmtree(runtime_root, ignore_errors=True)

    data_dir = runtime_root
    save_dir = data_dir / "us"
    data_dir.mkdir(parents=True, exist_ok=True)
    save_dir.mkdir(parents=True, exist_ok=True)

    existing_path = save_dir / "AAA.csv"
    existing = _price_frame("AAA")
    existing["date"] = [pd.Timestamp("2026-03-10", tz="UTC")]
    existing.to_csv(existing_path, index=False)

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "get_us_market_today", lambda: date(2026, 3, 12))
    monkeypatch.setattr(
        dc,
        "fetch_us_single",
        lambda *args, **kwargs: (_ for _ in ()).throw(dc.DelistedSymbolError("possibly delisted; no timezone found")),
    )

    dc.fetch_and_save_us_ohlcv_chunked(
        ["AAA"],
        save_dir=str(save_dir),
        chunk_size=1,
        pause=0.0,
        max_workers=1,
    )

    saved = pd.read_csv(existing_path)
    assert len(saved) == 1
    assert saved.loc[0, "symbol"] == "AAA"


def test_fetch_and_save_us_ohlcv_chunked_hard_unavailable_marks_symbol_inactive(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_hard_unavailable_existing_ohlcv")
    if runtime_root.exists():
        shutil.rmtree(runtime_root, ignore_errors=True)

    data_dir = runtime_root
    save_dir = data_dir / "us"
    data_dir.mkdir(parents=True, exist_ok=True)
    save_dir.mkdir(parents=True, exist_ok=True)

    existing_path = save_dir / "AAA.csv"
    existing = _price_frame("AAA")
    existing["date"] = [pd.Timestamp("2026-03-10", tz="UTC")]
    existing.to_csv(existing_path, index=False)

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "get_us_market_today", lambda: date(2026, 3, 12))
    monkeypatch.setattr(
        dc,
        "fetch_us_single",
        lambda *args, **kwargs: (_ for _ in ()).throw(dc.DelistedSymbolError("quote not found")),
    )

    dc.fetch_and_save_us_ohlcv_chunked(
        ["AAA"],
        save_dir=str(save_dir),
        chunk_size=1,
        pause=0.0,
        max_workers=1,
    )

    saved = pd.read_csv(existing_path)
    assert saved.empty


def test_collect_data_uses_conservative_us_ohlcv_defaults(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(dc, "update_symbol_list", lambda: {"AAA"})
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "ensure_dir", lambda directory: None)
    monkeypatch.setattr(dc, "fetch_and_save_us_ohlcv_chunked", lambda **kwargs: captured.update(kwargs))

    dc.collect_data(update_symbols=True)

    assert captured["chunk_size"] == dc.US_OHLCV_CHUNK_SIZE == dc.US_OHLCV_BATCH_SIZE
    assert captured["pause"] == dc.US_OHLCV_CHUNK_PAUSE_SECONDS
    assert captured["pause"] == dc.US_OHLCV_REQUEST_DELAY_SECONDS
    assert captured["max_workers"] == dc.US_OHLCV_MAX_WORKERS == 1


def test_fetch_and_save_us_ohlcv_chunked_skips_pause_when_chunk_is_all_latest(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_us_ohlcv_latest_pause_skip")
    if runtime_root.exists():
        shutil.rmtree(runtime_root, ignore_errors=True)

    data_dir = runtime_root
    save_dir = data_dir / "us"
    save_dir.mkdir(parents=True, exist_ok=True)
    sleeps: list[float] = []

    def _future_cached_frame(symbol: str) -> pd.DataFrame:
        dates = pd.date_range(end="2026-03-20", periods=dc.US_OHLCV_TARGET_BARS, freq="D", tz="UTC")
        return pd.DataFrame(
            {
                "date": dates,
                "symbol": [symbol] * len(dates),
                "open": range(10, 10 + len(dates)),
                "high": range(11, 11 + len(dates)),
                "low": range(9, 9 + len(dates)),
                "close": range(10, 10 + len(dates)),
                "volume": range(1000, 1000 + len(dates)),
            }
        )

    for symbol in ("AAA", "BBB"):
        _future_cached_frame(symbol).to_csv(save_dir / f"{symbol}.csv", index=False)

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "get_us_market_today", lambda: date(2026, 3, 12))
    monkeypatch.setattr(dc.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(
        dc,
        "fetch_us_single",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("latest chunks must not fetch")),
    )
    monkeypatch.setattr(
        dc,
        "_fetch_us_batch_ohlcv",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("latest chunks must not batch fetch")),
    )

    summary = dc.fetch_and_save_us_ohlcv_chunked(
        ["AAA", "BBB"],
        save_dir=str(save_dir),
        chunk_size=1,
        pause=9.0,
        max_workers=1,
    )

    assert summary["latest"] == 2
    assert sleeps == []


def test_fetch_and_save_us_ohlcv_chunked_persists_collector_run_state(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_us_ohlcv_run_state")
    if runtime_root.exists():
        shutil.rmtree(runtime_root, ignore_errors=True)

    data_dir = runtime_root
    save_dir = data_dir / "us"
    save_dir.mkdir(parents=True, exist_ok=True)

    def _fetch(symbol, start, end):
        if symbol == "AAA":
            return pd.DataFrame(
                {
                    "date": [pd.Timestamp("2026-03-12", tz="UTC")],
                    "symbol": [symbol],
                    "open": [10.0],
                    "high": [11.0],
                    "low": [9.0],
                    "close": [10.5],
                    "volume": [1000],
                }
            )
        raise dc.RateLimitError("429")

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "get_us_market_today", lambda: date(2026, 3, 12))
    monkeypatch.setattr(dc, "fetch_us_single", _fetch)

    summary = dc.fetch_and_save_us_ohlcv_chunked(
        ["AAA", "BBB"],
        save_dir=str(save_dir),
        chunk_size=1,
        pause=0.0,
        max_workers=1,
    )

    state_path = _collector_run_state_path(runtime_root)
    assert state_path.exists()
    payload = pd.read_json(state_path, typ="series")
    assert payload["market"] == "us"
    assert payload["as_of_date"] == "2026-03-12"
    assert "AAA" in payload["completed_symbols"]
    assert "BBB" in payload["retry_queue"]
    assert payload["status_counts"]["saved"] == 1
    assert payload["status_counts"]["rate_limited"] == 1
    assert payload["last_symbol"] == "BBB"
    assert "cooldown_snapshot" in payload
    assert summary["market"] == "us"
    assert summary["as_of"] == "2026-03-12"
    assert summary["total"] == 2
    assert summary["status_counts"]["saved"] == 1
    assert summary["status_counts"]["rate_limited"] == 1
    assert summary["retry_queue_size"] == 1
    assert summary["failed_samples"][0]["ticker"] == "BBB"
    assert summary["ok"] is False
    assert summary["status"] == "degraded"
    assert summary["retryable"] is True
    assert Path(str(summary["collector_state_path"])).resolve() == state_path.resolve()


def test_fetch_and_save_us_ohlcv_chunked_reports_live_bottleneck_diagnostics(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_us_ohlcv_diagnostics")
    if runtime_root.exists():
        shutil.rmtree(runtime_root, ignore_errors=True)

    data_dir = runtime_root
    save_dir = data_dir / "us"
    save_dir.mkdir(parents=True, exist_ok=True)

    class _Ticker:
        def history(self, **kwargs):  # noqa: ANN003, ANN201
            return pd.DataFrame(
                {"Close": [20.5], "Volume": [2000]},
                index=pd.DatetimeIndex(["2026-03-12"], name="date"),
            )

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(dc, "_configure_yfinance_logger", lambda: None)
    monkeypatch.setattr(dc, "get_us_market_today", lambda: date(2026, 3, 12))
    monkeypatch.setattr(dc, "_us_rate_limit_cooldown_until", 0.0)
    monkeypatch.setattr(dc.yf, "Ticker", lambda ticker: _Ticker())
    monkeypatch.setattr(dc, "wait_for_yahoo_request_slot", lambda source, *, min_interval=0.0: 1.25)
    monkeypatch.setattr(
        dc,
        "_fetch_us_batch_ohlcv",
        lambda symbols, start, end: {"AAA": _price_frame("AAA")},
    )

    summary = dc.fetch_and_save_us_ohlcv_chunked(
        ["AAA", "BBB"],
        save_dir=str(save_dir),
        chunk_size=2,
        pause=0.0,
        max_workers=1,
    )

    timings = summary["timings"]
    diagnostics = summary["collector_diagnostics"]
    counts = diagnostics["counts"]
    assert timings["process_total_seconds"] >= 0.0
    assert timings["batch_prefetch_seconds"] >= 0.0
    assert timings["provider_wait_seconds"] == 1.25
    assert timings["provider_fetch_seconds"] >= 0.0
    assert timings["merge_write_seconds"] >= 0.0
    assert counts["batch_prefetch_symbols"] == 2
    assert counts["batch_prefetch_hits"] == 1
    assert counts["batch_prefetch_misses"] == 1
    assert counts["single_fetches"] == 1

    state_path = _collector_run_state_path(runtime_root)
    payload = pd.read_json(state_path, typ="series")
    snapshot = payload["diagnostics_snapshot"]
    assert snapshot["counts"]["single_fetches"] == 1


def test_fetch_and_save_us_ohlcv_chunked_reuses_cached_prepare_frame_for_batch_grouping(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_us_ohlcv_prepare_cache")
    if runtime_root.exists():
        shutil.rmtree(runtime_root, ignore_errors=True)

    data_dir = runtime_root
    save_dir = data_dir / "us"
    save_dir.mkdir(parents=True, exist_ok=True)

    for symbol in ("AAA", "BBB"):
        pd.DataFrame(
            {
                "date": pd.date_range(end="2026-03-11", periods=dc.US_OHLCV_TARGET_BARS, freq="D", tz="UTC"),
                "symbol": [symbol] * dc.US_OHLCV_TARGET_BARS,
                "open": range(10, 10 + dc.US_OHLCV_TARGET_BARS),
                "high": range(11, 11 + dc.US_OHLCV_TARGET_BARS),
                "low": range(9, 9 + dc.US_OHLCV_TARGET_BARS),
                "close": range(10, 10 + dc.US_OHLCV_TARGET_BARS),
                "volume": range(1000, 1000 + dc.US_OHLCV_TARGET_BARS),
            }
        ).to_csv(save_dir / f"{symbol}.csv", index=False)

    read_paths: list[str] = []
    original_read_csv = dc.pd.read_csv

    def _read_csv(path, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        if str(path).endswith(("AAA.csv", "BBB.csv")):
            read_paths.append(str(path))
        return original_read_csv(path, *args, **kwargs)

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "get_us_market_today", lambda: date(2026, 3, 12))
    monkeypatch.setattr(dc.pd, "read_csv", _read_csv)
    monkeypatch.setattr(
        dc,
        "_fetch_us_batch_ohlcv",
        lambda symbols, start, end: {symbol: _price_frame(symbol) for symbol in symbols},
    )

    summary = dc.fetch_and_save_us_ohlcv_chunked(
        ["AAA", "BBB"],
        save_dir=str(save_dir),
        chunk_size=2,
        pause=0.0,
        max_workers=1,
    )

    assert summary["saved"] == 2
    assert len(read_paths) == 2


def test_fetch_and_save_us_ohlcv_chunked_checkpoints_state_once_per_chunk(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_us_ohlcv_chunk_state_writes")
    if runtime_root.exists():
        shutil.rmtree(runtime_root, ignore_errors=True)

    data_dir = runtime_root
    save_dir = data_dir / "us"
    save_dir.mkdir(parents=True, exist_ok=True)
    writes: list[str] = []

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "get_us_market_today", lambda: date(2026, 3, 12))
    monkeypatch.setattr(dc, "_write_collector_run_state", lambda path, state: writes.append(str(path)))
    monkeypatch.setattr(
        dc,
        "_fetch_us_batch_ohlcv",
        lambda symbols, start, end: {symbol: _price_frame(symbol) for symbol in symbols},
    )

    summary = dc.fetch_and_save_us_ohlcv_chunked(
        ["AAA", "BBB", "CCC", "DDD"],
        save_dir=str(save_dir),
        chunk_size=4,
        pause=0.0,
        max_workers=1,
    )

    assert summary["saved"] == 4
    assert len(writes) == 2


def test_fetch_and_save_us_ohlcv_chunked_rechecks_completed_same_day_symbols(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_us_ohlcv_run_state_reuse")
    if runtime_root.exists():
        shutil.rmtree(runtime_root, ignore_errors=True)

    data_dir = runtime_root
    save_dir = data_dir / "us"
    save_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "get_us_market_today", lambda: date(2026, 3, 12))

    first_pass_calls: list[str] = []

    def _first_fetch(symbol, start, end):
        first_pass_calls.append(symbol)
        if symbol == "AAA":
            return pd.DataFrame(
                {
                    "date": [pd.Timestamp("2026-03-12", tz="UTC")],
                    "symbol": [symbol],
                    "open": [10.0],
                    "high": [11.0],
                    "low": [9.0],
                    "close": [10.5],
                    "volume": [1000],
                }
            )
        raise dc.RateLimitError("429")

    monkeypatch.setattr(dc, "fetch_us_single", _first_fetch)
    dc.fetch_and_save_us_ohlcv_chunked(
        ["AAA", "BBB"],
        save_dir=str(save_dir),
        chunk_size=1,
        pause=0.0,
        max_workers=1,
    )

    second_pass_calls: list[str] = []

    def _second_fetch(symbol, start, end):
        second_pass_calls.append(symbol)
        return pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-03-12", tz="UTC")],
                "symbol": [symbol],
                "open": [12.0],
                "high": [13.0],
                "low": [11.0],
                "close": [12.5],
                "volume": [1300],
            }
        )

    monkeypatch.setattr(dc, "fetch_us_single", _second_fetch)
    dc.fetch_and_save_us_ohlcv_chunked(
        ["AAA", "BBB"],
        save_dir=str(save_dir),
        chunk_size=1,
        pause=0.0,
        max_workers=1,
    )

    assert first_pass_calls == ["AAA", "BBB", "BBB"]
    assert second_pass_calls == ["BBB", "AAA"]


def test_collector_run_state_retries_generic_failed_symbols_on_same_day() -> None:
    state = dc._default_collector_run_state(market="us", as_of_date="2026-03-12")

    dc._record_collector_symbol_status(state, symbol="AAA", status="saved")
    dc._record_collector_symbol_status(state, symbol="BBB", status="failed")
    dc._record_collector_symbol_status(state, symbol="CCC", status="delisted")

    assert dc._collector_tickers_for_run(["AAA", "BBB", "CCC"], state) == ["BBB"]
    assert dc._collector_tickers_for_run(
        ["AAA", "BBB", "CCC"],
        state,
        skip_completed=False,
        terminal_statuses={"delisted"},
    ) == ["BBB", "AAA"]
    assert "BBB" in state["retry_queue"]
    assert "BBB" not in state["failed_symbols"]


def test_collect_data_returns_structured_us_ohlcv_summary(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(dc, "update_symbol_list", lambda: {"AAA", "BBB"})
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "ensure_dir", lambda directory: None)
    monkeypatch.setattr(
        dc,
        "fetch_and_save_us_ohlcv_chunked",
        lambda **kwargs: captured.update(kwargs)
        or {
            "market": "us",
            "as_of": "2026-03-12",
            "total": 2,
            "saved": 2,
            "failed": 0,
            "status_counts": {"saved": 2},
            "ok": True,
            "status": "ok",
            "retryable": False,
        },
    )

    summary = dc.collect_data(update_symbols=True)

    assert summary["market"] == "us"
    assert summary["total"] == 2
    assert summary["ok"] is True
    assert captured["tickers"] == ["AAA", "BBB"]


def test_wait_for_us_rate_limit_cooldown_uses_conservative_request_interval(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(dc, "_us_rate_limit_cooldown_until", 0.0)
    monkeypatch.setattr(dc.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(
        dc,
        "wait_for_yahoo_request_slot",
        lambda source, *, min_interval=0.0: captured.update(
            {"source": source, "min_interval": min_interval}
        ),
    )

    dc._wait_for_us_rate_limit_cooldown()

    assert captured == {"source": "US OHLCV", "min_interval": 1.0}


def test_configure_yfinance_logger_disables_hidden_exceptions(monkeypatch):
    monkeypatch.setattr(dc, "_yfinance_logger_configured", False)
    monkeypatch.setattr(dc.yf.config.debug, "hide_exceptions", True)

    dc._configure_yfinance_logger()

    assert dc.yf.config.debug.hide_exceptions is False
