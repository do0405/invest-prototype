from __future__ import annotations

from datetime import date
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

    assert "처리 5개" in summary
    assert "저장 0" in summary
    assert "최신 3" in summary
    assert "유지 1" in summary
    assert "soft 1" in summary
    assert "상폐 0" in summary
    assert "제한 0" in summary
    assert "실패 0" in summary


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
    monkeypatch.setattr(dc.time, "sleep", lambda seconds: None)
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
    monkeypatch.setattr(dc.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(dc.yf, "Ticker", lambda ticker: _Ticker())

    with pytest.raises(dc.RateLimitError):
        dc.fetch_us_single("AAA", start=date(2026, 3, 1), end=date(2026, 3, 12))


def test_fetch_us_single_raises_delisted_error_from_yfinance_empty_response(monkeypatch):
    class _Ticker:
        def history(self, **kwargs):  # noqa: ANN003, ANN201
            dc.yf_shared._ERRORS["AAA"] = "possibly delisted; no timezone found"
            return pd.DataFrame()

    monkeypatch.setattr(dc, "_wait_for_us_rate_limit_cooldown", lambda: None)
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(dc, "_configure_yfinance_logger", lambda: None)
    monkeypatch.setattr(dc.yf, "Ticker", lambda ticker: _Ticker())

    with pytest.raises(dc.DelistedSymbolError):
        dc.fetch_us_single("AAA", start=date(2026, 3, 1), end=date(2026, 3, 12))


def test_fetch_us_single_raises_delisted_error_for_yfinance_exception(monkeypatch):
    class _Ticker:
        def history(self, **kwargs):  # noqa: ANN003, ANN201
            raise dc.YFTzMissingError("AAA")

    monkeypatch.setattr(dc, "_wait_for_us_rate_limit_cooldown", lambda: None)
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(dc.yf, "Ticker", lambda ticker: _Ticker())

    with pytest.raises(dc.DelistedSymbolError):
        dc.fetch_us_single("AAA", start=date(2026, 3, 1), end=date(2026, 3, 12))


def test_delisted_symbol_error_uses_canonical_reason():
    error = dc.DelistedSymbolError(
        '$EVOK: possibly delisted; no price data found  (1d 2025-12-18 -> 2026-03-12) (Yahoo error = "No data found")'
    )

    assert str(error) == "possibly delisted; no price data found"


def test_delisted_symbol_error_classifies_soft_and_hard_causes():
    soft_error = dc.DelistedSymbolError("$AAA: possibly delisted; no timezone found")
    hard_error = dc.DelistedSymbolError('HTTP Error 404: {"description":"Quote not found for symbol: AAA"}')

    assert soft_error.is_soft is True
    assert hard_error.is_soft is False


def test_fetch_and_save_us_ohlcv_chunked_does_not_mark_rate_limited_symbol_delisted(
    monkeypatch,
):
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
        dc.US_OHLCV_RATE_LIMIT_COOLDOWN_SECONDS * 2,
    ]
    assert not (save_dir / "AAA.csv").exists()


def test_fetch_and_save_us_ohlcv_chunked_keeps_existing_data_for_delisted_response(
    monkeypatch,
):
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


def test_fetch_and_save_us_ohlcv_chunked_soft_signal_without_existing_keeps_retryable_state(
    monkeypatch,
):
    runtime_root = runtime_test_root("_test_runtime_soft_unavailable_new_ohlcv")
    if runtime_root.exists():
        shutil.rmtree(runtime_root, ignore_errors=True)

    data_dir = runtime_root
    save_dir = data_dir / "us"
    data_dir.mkdir(parents=True, exist_ok=True)
    save_dir.mkdir(parents=True, exist_ok=True)

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

    assert not (save_dir / "AAA.csv").exists()


def test_fetch_and_save_us_ohlcv_chunked_hard_unavailable_marks_symbol_inactive(
    monkeypatch,
):
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
    monkeypatch.setattr(
        dc,
        "fetch_and_save_us_ohlcv_chunked",
        lambda **kwargs: captured.update(kwargs),
    )

    dc.collect_data(update_symbols=True)

    assert captured["chunk_size"] == dc.US_OHLCV_CHUNK_SIZE == 5
    assert captured["pause"] == dc.US_OHLCV_CHUNK_PAUSE_SECONDS == 6.0
    assert captured["max_workers"] == dc.US_OHLCV_MAX_WORKERS == 2


def test_configure_yfinance_logger_disables_hidden_exceptions(monkeypatch):
    monkeypatch.setattr(dc, "_yfinance_logger_configured", False)
    monkeypatch.setattr(dc.yf.config.debug, "hide_exceptions", True)

    dc._configure_yfinance_logger()

    assert dc.yf.config.debug.hide_exceptions is False
