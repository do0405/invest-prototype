from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd
import pytest

from tests._paths import runtime_root
from data_collectors import stock_metadata_collector as collector


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


def test_get_symbols_for_us_uses_seed_universe_and_excludes_special_issue(monkeypatch):
    root = runtime_root("_test_runtime_us_metadata_symbols")
    _reset_dir(root)
    data_dir = root
    us_dir = root / "us"
    us_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = data_dir / "stock_metadata.csv"

    pd.DataFrame([{"symbol": "AAPL"}, {"symbol": "SQQQ"}, {"symbol": "AACIW"}]).to_csv(
        data_dir / "nasdaq_symbols.csv",
        index=False,
    )
    pd.DataFrame([{"symbol": "GLD", "fetch_status": "complete", "source": "cache", "last_attempted_at": "2026-03-14T00:00:00Z"}]).to_csv(metadata_path, index=False)

    monkeypatch.setattr(collector, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(collector, "get_market_data_dir", lambda market: str(us_dir))
    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))

    symbols = collector.get_symbols("us")

    assert "AAPL" in symbols
    assert "SQQQ" in symbols
    assert "GLD" in symbols
    assert "AACIW" not in symbols
    assert "^VIX" in symbols


def test_metadata_main_prints_target_summary_and_saved_message(monkeypatch, capsys):
    root = runtime_root("_test_runtime_metadata_progress_output")
    _reset_dir(root)
    metadata_path = root / "stock_metadata.csv"
    pd.DataFrame([
        {
            "symbol": "AAA",
            "market": "us",
            "market_cap": 1000,
            "earnings_growth": 18.0,
            "return_on_equity": 0.21,
            "fetch_status": "complete",
            "source": "cache",
            "last_attempted_at": "2026-03-14T00:00:00Z",
        }
    ]).to_csv(metadata_path, index=False)

    monkeypatch.setattr(collector, "get_symbols", lambda market="us", stock_module=None: ["AAA", "BBB", "CCC"])
    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))
    monkeypatch.setattr(
        collector,
        "load_cached_metadata",
        lambda market="us", max_age_days=7, allow_stale=True: pd.DataFrame([
            {
                "symbol": "AAA",
                "market": "us",
                "market_cap": 1000,
                "earnings_growth": 18.0,
                "return_on_equity": 0.21,
                "fetch_status": "complete",
                "source": "cache",
                "last_attempted_at": "2026-03-14T00:00:00Z",
            }
        ]),
    )
    monkeypatch.setattr(
        collector,
        "collect_stock_metadata",
        lambda symbols, **kwargs: pd.DataFrame([
            {"symbol": symbol, "market": "us", "fetch_status": "complete", "source": "yfinance", "last_attempted_at": "2026-03-14T00:00:00Z"}
            for symbol in symbols
        ]),
    )
    monkeypatch.setattr(collector.time, "sleep", lambda seconds: None)

    collector.main(market="us")

    captured = capsys.readouterr()
    assert "[Metadata] Batch 1/1 (us) - size=3, processed=0/3" in captured.out
    assert "[Metadata] Checkpoint saved (us) - processed=3/3" in captured.out
    assert "[Metadata] Target summary (us) - total=3, cached=1, missing=3" in captured.out
    assert "[Metadata] Saved (us) - total=3" in captured.out


def test_collect_stock_metadata_prints_progress(monkeypatch, capsys):
    monkeypatch.setattr(collector, "wait_for_yahoo_request_slot", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(
        collector,
        "fetch_metadata",
        lambda symbol, **kwargs: {
            "symbol": symbol,
            "market": "us",
            "sector": "Tech",
            "market_cap": 1000,
            "fetch_status": "complete",
            "source": "yfinance",
            "last_attempted_at": "2026-03-14T00:00:00Z",
        },
    )

    collector.collect_stock_metadata(["AAA", "BBB", "CCC"], market="us", max_workers=1, delay=0)

    captured = capsys.readouterr()
    assert "[Metadata] Fetch started (us) - total=3, workers=1" in captured.out
    assert "[Metadata] Progress (us) - completed=3/3, success=3" in captured.out


def test_record_from_yfinance_populates_growth_fields_and_fast_info_fallback():
    record = collector._record_from_yfinance(
        "AAPL",
        "us",
        "AAPL",
        {
            "revenueGrowth": 0.157,
            "earningsQuarterlyGrowth": 0.159,
            "returnOnEquity": 1.52,
            "trailingPE": 29.4,
        },
        {
            "marketCap": 3_759_000_000_000,
            "shares": 14_697_926_000,
            "exchange": "NMS",
        },
    )

    assert record["exchange"] == "NMS"
    assert record["market_cap"] == 3_759_000_000_000
    assert record["shares_outstanding"] == 14_697_926_000
    assert record["revenue_growth"] == pytest.approx(15.7)
    assert record["earnings_growth"] == pytest.approx(15.9)
    assert record["return_on_equity"] == pytest.approx(1.52)


def test_get_missing_symbols_refreshes_entire_universe_every_run():
    cached = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "market": "us",
                "market_cap": 1000,
                "exchange": "NMS",
                "fetch_status": "complete",
                "source": "cache",
                "last_attempted_at": "2026-03-14T00:00:00Z",
            },
            {
                "symbol": "BBB",
                "market": "us",
                "market_cap": 1000,
                "fetch_status": "partial_fast_info",
                "source": "yfinance",
                "last_attempted_at": "2026-03-14T00:00:00Z",
            },
            {
                "symbol": "CCC",
                "market": "us",
                "fetch_status": "not_found",
                "source": "yfinance",
                "last_attempted_at": "2026-03-14T00:00:00Z",
            },
        ]
    )

    missing = collector.get_missing_symbols(cached, ["AAA", "BBB", "CCC", "DDD"])

    assert missing == ["AAA", "BBB", "CCC", "DDD"]


def test_fetch_metadata_skips_yahooquery_when_yfinance_is_sufficient(monkeypatch):
    monkeypatch.setattr(collector, "iter_provider_symbols", lambda symbol, market: [symbol])
    monkeypatch.setattr(collector, "wait_for_yahoo_request_slot", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(collector, "extend_yahoo_cooldown", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(
        collector,
        "_fetch_yfinance_info_quietly",
        lambda provider_symbol: (
            {
                "exchange": "NMS",
                "sector": "Technology",
                "industry": "Software",
                "revenueGrowth": 0.21,
                "earningsQuarterlyGrowth": 0.34,
                "returnOnEquity": 0.42,
                "marketCap": 1000,
                "sharesOutstanding": 10,
            },
            {},
            False,
            False,
        ),
    )

    def _fail_yahooquery(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("yahooquery should not run when yfinance already returned complete metadata")

    monkeypatch.setattr(collector, "fetch_metadata_yahooquery", _fail_yahooquery)

    record = collector.fetch_metadata("AAPL", market="us", max_retries=1, delay=0.0)

    assert record["exchange"] == "NMS"
    assert record["revenue_growth"] == pytest.approx(21.0)
    assert record["earnings_growth"] == pytest.approx(34.0)
    assert record["return_on_equity"] == pytest.approx(0.42)
    assert record["fetch_status"] == "complete"
    assert record["source"] == "yfinance"


def test_fetch_metadata_uses_yahooquery_when_yfinance_is_sparse(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(collector, "iter_provider_symbols", lambda symbol, market: [symbol])
    monkeypatch.setattr(collector, "wait_for_yahoo_request_slot", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(collector, "extend_yahoo_cooldown", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(
        collector,
        "_fetch_yfinance_info_quietly",
        lambda provider_symbol: (
            {
                "exchange": "NMS",
                "marketCap": 1000,
                "sharesOutstanding": 10,
            },
            {},
            False,
            False,
        ),
    )

    def _fetch_yahooquery(symbol, provider_symbol, **kwargs):  # noqa: ANN001, ANN201
        calls.append(provider_symbol)
        record = collector._blank_record(symbol, "us", provider_symbol=provider_symbol)
        record["sector"] = "Technology"
        record["industry"] = "Software"
        return record, False

    monkeypatch.setattr(collector, "fetch_metadata_yahooquery", _fetch_yahooquery)

    record = collector.fetch_metadata("AAPL", market="us", max_retries=1, delay=0.0)

    assert calls == ["AAPL"]
    assert record["sector"] == "Technology"
    assert record["industry"] == "Software"
    assert record["market_cap"] == 1000
    assert record["fetch_status"] == "complete"
    assert record["source"] == "yfinance+yahooquery"


def test_fetch_metadata_keeps_fast_info_when_yfinance_info_is_rate_limited(monkeypatch):
    cooldowns: list[float] = []

    monkeypatch.setattr(collector, "iter_provider_symbols", lambda symbol, market: [symbol])
    monkeypatch.setattr(collector, "wait_for_yahoo_request_slot", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(collector, "extend_yahoo_cooldown", lambda source, seconds: cooldowns.append(seconds))
    monkeypatch.setattr(
        collector,
        "_fetch_yfinance_info_quietly",
        lambda provider_symbol: ({}, {"exchange": "NMS", "marketCap": 1000, "shares": 10}, False, True),
    )
    monkeypatch.setattr(
        collector,
        "fetch_metadata_yahooquery",
        lambda *args, **kwargs: (collector._blank_record("AAPL", "us", provider_symbol="AAPL"), False),
    )

    record = collector.fetch_metadata("AAPL", market="us", max_retries=1, delay=0.0)

    assert record["exchange"] == "NMS"
    assert record["market_cap"] == 1000
    assert record["shares_outstanding"] == 10
    assert record["fetch_status"] == "partial_fast_info"
    assert cooldowns == [collector.METADATA_RATE_LIMIT_COOLDOWN_SECONDS]


def test_load_cached_metadata_rejects_outdated_schema(monkeypatch):
    root = runtime_root("_test_runtime_metadata_schema_outdated")
    _reset_dir(root)
    metadata_path = root / "stock_metadata.csv"
    pd.DataFrame([{"symbol": "AAA", "market": "us", "revenue_growth": 0.18}]).to_csv(metadata_path, index=False)

    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))

    cached = collector.load_cached_metadata("us", max_age_days=7, allow_stale=True)

    assert cached is None


def test_load_cached_metadata_reuses_stale_cache(monkeypatch):
    root = runtime_root("_test_runtime_metadata_stale_cache")
    _reset_dir(root)
    metadata_path = root / "stock_metadata.csv"
    pd.DataFrame([
        {
            "symbol": "AAA",
            "market": "us",
            "exchange": "NMS",
            "market_cap": 1000,
            "earnings_growth": 18.0,
            "return_on_equity": 0.21,
            "fetch_status": "complete",
            "source": "cache",
            "last_attempted_at": "2026-03-14T00:00:00Z",
        }
    ]).to_csv(metadata_path, index=False)
    stale_time = time.time() - (10 * 24 * 3600)
    os.utime(metadata_path, (stale_time, stale_time))

    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))

    cached = collector.load_cached_metadata("us", max_age_days=7, allow_stale=True)

    assert cached is not None
    assert list(cached["symbol"]) == ["AAA"]


def test_metadata_main_raises_for_empty_symbol_universe(monkeypatch):
    root = runtime_root("_test_runtime_metadata_empty_universe")
    _reset_dir(root)
    metadata_path = root / "stock_metadata.csv"

    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))
    monkeypatch.setattr(collector, "get_symbols", lambda market="us", stock_module=None: [])

    with pytest.raises(RuntimeError, match="Metadata symbol universe is empty"):
        collector.main(market="kr")


def test_merge_metadata_preserves_previous_complete_record_when_refresh_is_partial():
    cached = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "market": "us",
                "exchange": "NMS",
                "sector": "Technology",
                "industry": "Software",
                "market_cap": 1000,
                "shares_outstanding": 10,
                "revenue_growth": 20.0,
                "earnings_growth": 30.0,
                "return_on_equity": 0.4,
                "fetch_status": "complete",
                "source": "cache",
                "last_attempted_at": "2026-03-13T00:00:00Z",
            }
        ]
    )
    refreshed = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "market": "us",
                "exchange": "NMS",
                "market_cap": 1100,
                "fetch_status": "partial_fast_info",
                "source": "yfinance",
                "last_attempted_at": "2026-03-14T00:00:00Z",
            }
        ]
    )

    merged = collector.merge_metadata(cached, refreshed, market="us")

    assert len(merged) == 1
    row = merged.iloc[0]
    assert row["fetch_status"] == "complete"
    assert row["market_cap"] == 1100
    assert row["revenue_growth"] == 20.0
    assert row["earnings_growth"] == 30.0
    assert row["last_attempted_at"] == "2026-03-14T00:00:00Z"
