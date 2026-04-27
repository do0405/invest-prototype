from __future__ import annotations

import inspect
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
        data_dir / "broad_us_seed.csv",
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


def test_get_symbols_signature_removes_legacy_stock_module() -> None:
    parameters = inspect.signature(collector.get_symbols).parameters

    assert "stock_module" not in parameters


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
            "last_attempted_at": "2026-04-18T00:00:00Z",
        }
    ]).to_csv(metadata_path, index=False)

    monkeypatch.setattr(collector, "get_symbols", lambda market="us": ["AAA", "BBB", "CCC"])
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
                "last_attempted_at": "2026-04-18T00:00:00Z",
            }
        ]),
    )
    monkeypatch.setattr(
        collector,
        "collect_stock_metadata",
            lambda symbols, **kwargs: pd.DataFrame([
            {"symbol": symbol, "market": "us", "fetch_status": "complete", "source": "yfinance", "last_attempted_at": "2026-04-18T00:00:00Z"}
            for symbol in symbols
        ]),
    )
    monkeypatch.setattr(collector.time, "sleep", lambda seconds: None)

    collector.main(market="us")

    captured = capsys.readouterr()
    assert "[Metadata] Batch 1/1 (us) - size=2, processed=0/2" in captured.out
    assert "[Metadata] Checkpoint saved (us) - processed=2/2" in captured.out
    assert "[Metadata] Target summary (us) - total=3, cached=1, missing=2" in captured.out
    assert "refresh_counts=cached_fresh=1, stale_complete=0, retryable=0, not_found_cached=0, missing=2, to_fetch=2" in captured.out
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


def test_collect_stock_metadata_defaults_to_probe_verified_three_paced_workers(monkeypatch, capsys):
    monkeypatch.delenv("INVEST_PROTO_METADATA_MAX_WORKERS", raising=False)
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

    collector.collect_stock_metadata(["AAA", "BBB", "CCC"], market="us", delay=0)

    captured = capsys.readouterr()
    assert "[Metadata] Fetch started (us) - total=3, workers=3" in captured.out


def test_fetch_metadata_retries_yahooquery_rate_limit_after_cooldown(monkeypatch):
    calls: list[str] = []
    cooldowns: list[tuple[str, float]] = []

    monkeypatch.setattr(collector, "iter_provider_symbols", lambda symbol, market: [symbol])
    monkeypatch.setattr(collector, "wait_for_yahoo_request_slot", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(
        collector,
        "_fetch_yfinance_info_quietly",
        lambda provider_symbol: (
            {"quoteType": "EQUITY", "longName": "Example Inc", "exchange": "NMS"},
            {},
            False,
            False,
        ),
    )
    monkeypatch.setattr(collector, "extend_yahoo_cooldown", lambda source, seconds: cooldowns.append((source, seconds)) or seconds)

    def _fetch_yahooquery(symbol: str, provider_symbol: str, **kwargs):  # noqa: ANN001, ANN202
        calls.append(provider_symbol)
        if len(calls) == 1:
            record = collector._blank_record(symbol, "us", provider_symbol=provider_symbol)
            return record, False, True
        record = collector._blank_record(symbol, "us", provider_symbol=provider_symbol)
        record.update(
            {
                "sector": "Technology",
                "industry": "Software",
                "market_cap": 1000,
                "fetch_status": "complete",
                "source": "yahooquery",
            }
        )
        return record, False, False

    monkeypatch.setattr(collector, "fetch_metadata_yahooquery", _fetch_yahooquery)

    record = collector.fetch_metadata("AAA", market="us", max_retries=2, delay=0.0)

    assert len(calls) == 2
    assert cooldowns == [("US metadata", collector.METADATA_RATE_LIMIT_COOLDOWN_SECONDS)]
    assert record["fetch_status"] == "complete"
    assert record["sector"] == "Technology"


def test_fetch_metadata_uses_ohlcv_aligned_request_delay_by_default(monkeypatch):
    captured: list[float] = []

    monkeypatch.setattr(collector, "iter_provider_symbols", lambda symbol, market: [symbol])
    monkeypatch.setattr(
        collector,
        "wait_for_yahoo_request_slot",
        lambda source, *, min_interval=0.0: captured.append(min_interval) or 0.0,
    )
    monkeypatch.setattr(
        collector,
        "_fetch_yfinance_info_quietly",
        lambda provider_symbol: (
            {
                "quoteType": "EQUITY",
                "longName": "Example Inc",
                "exchange": "NMS",
                "sector": "Technology",
                "industry": "Software",
                "marketCap": 1000,
                "sharesOutstanding": 10,
            },
            {},
            False,
            False,
        ),
    )

    collector.fetch_metadata("AAA", market="us", max_retries=1)

    assert captured == [collector.METADATA_MIN_REQUEST_DELAY_SECONDS]
    assert collector.METADATA_MIN_REQUEST_DELAY_SECONDS == 1.0


def test_metadata_probe_candidates_do_not_go_below_ohlcv_request_interval():
    assert collector.METADATA_RATE_LIMIT_PROBE_CANDIDATES[0] == (
        collector.METADATA_MAX_WORKERS,
        collector.METADATA_MIN_REQUEST_DELAY_SECONDS,
    )
    assert all(
        interval >= collector.METADATA_MIN_REQUEST_DELAY_SECONDS
        for _workers, interval in collector.METADATA_RATE_LIMIT_PROBE_CANDIDATES
    )


def test_metadata_batch_defaults_reduce_checkpoint_overhead():
    assert collector.METADATA_BATCH_SIZE == 200
    assert collector.METADATA_BATCH_PAUSE_SECONDS == 0.0


def test_metadata_main_skips_batch_pause_by_default(monkeypatch):
    root = runtime_root("_test_runtime_metadata_batch_pause")
    _reset_dir(root)
    metadata_path = root / "stock_metadata.csv"
    sleeps: list[float] = []

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))
    monkeypatch.setattr(collector, "get_symbols", lambda market: ["AAA", "BBB", "CCC"])
    monkeypatch.setattr(collector, "METADATA_BATCH_SIZE", 2)
    monkeypatch.setattr(collector.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(
        collector,
        "collect_stock_metadata",
        lambda symbols, **kwargs: pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "market": "us",
                    "sector": "Tech",
                    "market_cap": 1000,
                    "fetch_status": "complete",
                    "source": "yfinance",
                    "last_attempted_at": "2026-03-14T00:00:00Z",
                }
                for symbol in symbols
            ]
        ),
    )

    collector.main(market="us")

    assert sleeps == []


def test_metadata_main_falls_back_to_one_worker_and_slower_delay_after_rate_limit(monkeypatch, capsys):
    root = runtime_root("_test_runtime_metadata_worker_fallback")
    _reset_dir(root)
    metadata_path = root / "stock_metadata.csv"
    collect_calls: list[tuple[int, float]] = []

    def _collect(symbols, **kwargs):  # noqa: ANN001, ANN202
        collect_calls.append((int(kwargs.get("max_workers") or 0), float(kwargs.get("delay") or 0.0)))
        status = "rate_limited" if len(collect_calls) == 1 else "complete"
        frame = pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "market": "us",
                    "sector": "Tech" if status == "complete" else "",
                    "market_cap": 1000 if status == "complete" else None,
                    "fetch_status": status,
                    "source": "yfinance",
                    "last_attempted_at": "2026-04-18T00:00:00Z",
                }
                for symbol in symbols
            ]
        )
        frame.attrs["collector_diagnostics"] = {"counts": {"provider_fetch_symbols": len(symbols)}}
        frame.attrs["timings"] = {}
        return frame

    monkeypatch.setenv("INVEST_PROTO_METADATA_MAX_WORKERS", "2")
    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))
    monkeypatch.setattr(collector, "get_symbols", lambda market: ["AAA", "BBB", "CCC", "DDD"])
    monkeypatch.setattr(collector, "METADATA_BATCH_SIZE", 2)
    monkeypatch.setattr(collector.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(collector, "collect_stock_metadata", _collect)

    collector.main(market="us")

    captured = capsys.readouterr()
    assert collect_calls == [
        (2, pytest.approx(collector.METADATA_MIN_REQUEST_DELAY_SECONDS)),
        (1, pytest.approx(1.12)),
        (1, pytest.approx(1.12)),
    ]
    assert "[Throttle] Metadata throttle fallback (us) - rate_limited=2, workers=1, delay=1.12s" in captured.out


def test_metadata_main_falls_back_after_transient_throttle_event(monkeypatch, capsys):
    root = runtime_root("_test_runtime_metadata_transient_throttle")
    _reset_dir(root)
    metadata_path = root / "stock_metadata.csv"
    collect_calls: list[tuple[list[str], int, float]] = []
    snapshots = iter(
        [
            {"rate_limit_count": {"US metadata": 0}},
            {"rate_limit_count": {"US metadata": 1}},
            {"rate_limit_count": {"US metadata": 1}},
        ]
    )

    def _collect(symbols, **kwargs):  # noqa: ANN001, ANN202
        collect_calls.append((list(symbols), int(kwargs.get("max_workers") or 0), float(kwargs.get("delay") or 0.0)))
        frame = pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "market": "us",
                    "sector": "Tech",
                    "market_cap": 1000,
                    "fetch_status": "complete",
                    "source": "yfinance",
                    "last_attempted_at": "2026-04-18T00:00:00Z",
                }
                for symbol in symbols
            ]
        )
        frame.attrs["collector_diagnostics"] = {"counts": {"provider_fetch_symbols": len(symbols)}}
        frame.attrs["timings"] = {}
        return frame

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))
    monkeypatch.setattr(collector, "get_symbols", lambda market: ["AAA", "BBB", "CCC", "DDD"])
    monkeypatch.setattr(collector, "METADATA_BATCH_SIZE", 2)
    monkeypatch.setattr(collector.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(collector, "collect_stock_metadata", _collect)
    monkeypatch.setattr(collector, "get_yahoo_throttle_state", lambda: next(snapshots, {"rate_limit_count": {"US metadata": 1}}))

    collector.main(market="us")

    captured = capsys.readouterr()
    assert collect_calls == [
        (["AAA", "BBB"], 3, pytest.approx(collector.METADATA_MIN_REQUEST_DELAY_SECONDS)),
        (["CCC", "DDD"], 3, pytest.approx(1.12)),
    ]
    assert "[Throttle] Metadata throttle fallback (us) - rate_limit_events=1, workers=3, delay=1.12s" in captured.out


def test_metadata_main_steps_workers_down_after_three_rate_limit_events(monkeypatch):
    root = runtime_root("_test_runtime_metadata_stepwise_worker_fallback")
    _reset_dir(root)
    metadata_path = root / "stock_metadata.csv"
    collect_calls: list[tuple[list[str], int, float]] = []
    snapshots = iter(
        [
            {"rate_limit_count": {"US metadata": 0}},
            {"rate_limit_count": {"US metadata": 1}},
            {"rate_limit_count": {"US metadata": 1}},
            {"rate_limit_count": {"US metadata": 2}},
            {"rate_limit_count": {"US metadata": 2}},
            {"rate_limit_count": {"US metadata": 3}},
            {"rate_limit_count": {"US metadata": 3}},
            {"rate_limit_count": {"US metadata": 4}},
            {"rate_limit_count": {"US metadata": 4}},
            {"rate_limit_count": {"US metadata": 5}},
            {"rate_limit_count": {"US metadata": 5}},
            {"rate_limit_count": {"US metadata": 5}},
        ]
    )

    def _collect(symbols, **kwargs):  # noqa: ANN001, ANN202
        collect_calls.append((list(symbols), int(kwargs.get("max_workers") or 0), float(kwargs.get("delay") or 0.0)))
        frame = pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "market": "us",
                    "sector": "Tech",
                    "market_cap": 1000,
                    "fetch_status": "complete",
                    "source": "yfinance",
                    "last_attempted_at": "2026-04-18T00:00:00Z",
                }
                for symbol in symbols
            ]
        )
        frame.attrs["collector_diagnostics"] = {"counts": {"provider_fetch_symbols": len(symbols)}}
        frame.attrs["timings"] = {}
        return frame

    monkeypatch.delenv("INVEST_PROTO_METADATA_MAX_WORKERS", raising=False)
    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))
    monkeypatch.setattr(collector, "get_symbols", lambda market: ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF"])
    monkeypatch.setattr(collector, "METADATA_BATCH_SIZE", 1)
    monkeypatch.setattr(collector.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(collector, "collect_stock_metadata", _collect)
    monkeypatch.setattr(collector, "get_yahoo_throttle_state", lambda: next(snapshots, {"rate_limit_count": {"US metadata": 5}}))

    collector.main(market="us")

    assert collect_calls == [
        (["AAA"], 3, pytest.approx(1.00)),
        (["BBB"], 3, pytest.approx(1.12)),
        (["CCC"], 3, pytest.approx(1.24)),
        (["DDD"], 3, pytest.approx(1.36)),
        (["EEE"], 2, pytest.approx(1.48)),
        (["FFF"], 1, pytest.approx(1.60)),
    ]


def test_metadata_rate_limit_probe_does_not_write_csv_and_recommends_fastest_passing_candidate(monkeypatch, capsys):
    collect_calls: list[tuple[list[str], int, float]] = []

    def _collect(symbols, **kwargs):  # noqa: ANN001, ANN202
        collect_calls.append((list(symbols), int(kwargs["max_workers"]), float(kwargs["delay"])))
        return pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "market": "us",
                    "sector": "Technology",
                    "market_cap": 1000,
                    "fetch_status": "complete",
                    "source": "yfinance",
                    "last_attempted_at": "2026-04-18T00:00:00Z",
                }
                for symbol in symbols
            ]
        )

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector, "get_symbols", lambda market: ["AAA", "BBB", "CCC", "DDD", "EEE"])
    monkeypatch.setattr(collector, "load_cached_metadata", lambda market, **kwargs: None)
    monkeypatch.setattr(collector, "collect_stock_metadata", _collect)
    monkeypatch.setattr(collector, "_write_metadata_csv", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("probe must not write csv")))
    monkeypatch.setattr(collector, "reset_yahoo_throttle_state", lambda: None)
    timestamps = iter([100.0, 120.0, 200.0, 210.0])
    monkeypatch.setattr(collector.time, "time", lambda: next(timestamps))
    monkeypatch.setattr(
        collector,
        "get_yahoo_throttle_state",
        lambda: {
            "next_request_in": 0.0,
            "cooldown_in": 0.0,
            "last_rate_limit_source": "",
            "last_request_source": "US metadata",
            "adaptive_interval_scale": {"US metadata": 0.45},
            "attempt_count": {"US metadata": 4},
            "success_count": {"US metadata": 4},
            "success_streak": {"US metadata": 4},
            "rate_limit_count": {"US metadata": 0},
        },
    )

    result = collector.run_metadata_rate_limit_probe(
        market="us",
        probe_count=4,
        candidate_profiles=((2, 0.75), (3, 0.50)),
        run_canary=False,
    )

    captured = capsys.readouterr()
    assert collect_calls == [
        (["AAA", "BBB", "CCC", "DDD"], 2, 0.75),
        (["AAA", "BBB", "CCC", "DDD"], 3, 0.50),
    ]
    assert result["recommended_profile"] == {"workers": 3, "interval": 0.50}
    assert "recommended_profile=workers=3, interval=0.50" in captured.out


def test_metadata_rate_limit_probe_stops_after_rate_limited_candidate(monkeypatch, capsys):
    statuses_by_call = [["complete", "complete"], ["rate_limited", "complete"]]
    collect_calls: list[tuple[int, float]] = []

    def _collect(symbols, **kwargs):  # noqa: ANN001, ANN202
        collect_calls.append((int(kwargs["max_workers"]), float(kwargs["delay"])))
        statuses = statuses_by_call[len(collect_calls) - 1]
        return pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "market": "us",
                    "fetch_status": status,
                    "source": "yfinance",
                    "last_attempted_at": "2026-04-18T00:00:00Z",
                }
                for symbol, status in zip(symbols, statuses)
            ]
        )

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector, "get_symbols", lambda market: ["AAA", "BBB"])
    monkeypatch.setattr(collector, "load_cached_metadata", lambda market, **kwargs: None)
    monkeypatch.setattr(collector, "collect_stock_metadata", _collect)
    monkeypatch.setattr(collector, "reset_yahoo_throttle_state", lambda: None)
    monkeypatch.setattr(
        collector,
        "get_yahoo_throttle_state",
        lambda: {
            "next_request_in": 0.0,
            "cooldown_in": 0.0,
            "last_rate_limit_source": "",
            "last_request_source": "US metadata",
            "adaptive_interval_scale": {"US metadata": 0.45},
            "attempt_count": {"US metadata": 2},
            "success_count": {"US metadata": 2},
            "success_streak": {"US metadata": 2},
            "rate_limit_count": {"US metadata": 0},
        },
    )

    result = collector.run_metadata_rate_limit_probe(
        market="us",
        probe_count=2,
        candidate_profiles=((2, 0.75), (3, 0.50), (4, 0.40)),
        run_canary=False,
    )

    captured = capsys.readouterr()
    assert collect_calls == [(2, 0.75), (3, 0.50)]
    assert result["recommended_profile"] == {"workers": 2, "interval": 0.75}
    assert result["results"][-1]["ok"] is False
    assert "recommended_profile=workers=2, interval=0.75" in captured.out


def test_metadata_rate_limit_probe_recommends_fastest_passing_candidate_not_last(monkeypatch):
    timestamps = iter([100.0, 110.0, 200.0, 230.0])

    def _collect(symbols, **kwargs):  # noqa: ANN001, ANN202
        return pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "market": "us",
                    "fetch_status": "complete",
                    "source": "yfinance",
                    "last_attempted_at": "2026-04-18T00:00:00Z",
                }
                for symbol in symbols
            ]
        )

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector, "get_symbols", lambda market: ["AAA", "BBB"])
    monkeypatch.setattr(collector, "load_cached_metadata", lambda market, **kwargs: None)
    monkeypatch.setattr(collector, "collect_stock_metadata", _collect)
    monkeypatch.setattr(collector, "reset_yahoo_throttle_state", lambda: None)
    monkeypatch.setattr(collector.time, "time", lambda: next(timestamps))
    monkeypatch.setattr(
        collector,
        "get_yahoo_throttle_state",
        lambda: {
            "next_request_in": 0.0,
            "cooldown_in": 0.0,
            "last_rate_limit_source": "",
            "last_request_source": "US metadata",
            "adaptive_interval_scale": {"US metadata": 0.45},
            "attempt_count": {"US metadata": 2},
            "success_count": {"US metadata": 2},
            "success_streak": {"US metadata": 2},
            "rate_limit_count": {"US metadata": 0},
        },
    )

    result = collector.run_metadata_rate_limit_probe(
        market="us",
        probe_count=2,
        candidate_profiles=((2, 0.75), (1, 0.75)),
        run_canary=False,
    )

    assert result["recommended_profile"] == {"workers": 2, "interval": 0.75}


def test_metadata_rate_limit_probe_prefers_complete_cached_symbols(monkeypatch):
    collect_calls: list[list[str]] = []
    cached = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "market": "us",
                "exchange": "NMS",
                "market_cap": 1000,
                "fetch_status": "complete",
                "last_attempted_at": "2026-04-18T00:00:00Z",
            },
            {
                "symbol": "BBB",
                "market": "us",
                "fetch_status": "not_found",
                "last_attempted_at": "2026-04-18T00:00:00Z",
            },
            {
                "symbol": "CCC",
                "market": "us",
                "fetch_status": "failed",
                "last_attempted_at": "2026-04-18T00:00:00Z",
            },
            {
                "symbol": "DDD",
                "market": "us",
                "exchange": "NMS",
                "market_cap": 2000,
                "fetch_status": "complete",
                "last_attempted_at": "2026-04-18T00:00:00Z",
            },
        ]
    )

    def _collect(symbols, **kwargs):  # noqa: ANN001, ANN202
        collect_calls.append(list(symbols))
        return pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "market": "us",
                    "fetch_status": "complete",
                    "source": "yfinance",
                    "last_attempted_at": "2026-04-18T00:00:00Z",
                }
                for symbol in symbols
            ]
        )

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector, "get_symbols", lambda market: ["AAA", "BBB", "CCC", "DDD", "EEE"])
    monkeypatch.setattr(collector, "load_cached_metadata", lambda market, **kwargs: cached)
    monkeypatch.setattr(collector, "collect_stock_metadata", _collect)
    monkeypatch.setattr(collector, "reset_yahoo_throttle_state", lambda: None)
    monkeypatch.setattr(
        collector,
        "get_yahoo_throttle_state",
        lambda: {
            "next_request_in": 0.0,
            "cooldown_in": 0.0,
            "last_rate_limit_source": "",
            "last_request_source": "US metadata",
            "adaptive_interval_scale": {"US metadata": 0.45},
            "attempt_count": {"US metadata": 2},
            "success_count": {"US metadata": 2},
            "success_streak": {"US metadata": 2},
            "rate_limit_count": {"US metadata": 0},
        },
    )

    result = collector.run_metadata_rate_limit_probe(
        market="us",
        probe_count=2,
        candidate_profiles=((2, 0.75),),
        run_canary=False,
    )

    assert result["symbols"] == ["AAA", "DDD"]
    assert collect_calls == [["AAA", "DDD"]]


def test_metadata_rate_limit_probe_aborts_when_canary_is_rate_limited(monkeypatch, capsys):
    collect_calls: list[tuple[list[str], int, float]] = []

    def _collect(symbols, **kwargs):  # noqa: ANN001, ANN202
        collect_calls.append((list(symbols), int(kwargs["max_workers"]), float(kwargs["delay"])))
        return pd.DataFrame(
            [
                {
                    "symbol": symbols[0],
                    "market": "us",
                    "fetch_status": "rate_limited",
                    "source": "yfinance",
                    "last_attempted_at": "2026-04-18T00:00:00Z",
                }
            ]
        )

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector, "get_symbols", lambda market: ["AAA", "BBB"])
    monkeypatch.setattr(collector, "load_cached_metadata", lambda market, **kwargs: None)
    monkeypatch.setattr(collector, "collect_stock_metadata", _collect)
    monkeypatch.setattr(collector, "reset_yahoo_throttle_state", lambda: None)
    monkeypatch.setattr(
        collector,
        "get_yahoo_throttle_state",
        lambda: {
            "next_request_in": 0.0,
            "cooldown_in": 30.0,
            "last_rate_limit_source": "US metadata",
            "last_request_source": "US metadata",
            "adaptive_interval_scale": {"US metadata": 0.72},
            "attempt_count": {"US metadata": 1},
            "success_count": {"US metadata": 0},
            "success_streak": {"US metadata": 0},
            "rate_limit_count": {"US metadata": 1},
        },
    )

    result = collector.run_metadata_rate_limit_probe(
        market="us",
        probe_count=2,
        candidate_profiles=((2, 0.75),),
    )

    captured = capsys.readouterr()
    assert collect_calls == [(["AAA"], 1, pytest.approx(2.0))]
    assert result["probe_blocked"] is True
    assert result["results"] == []
    assert "Canary blocked" in captured.out


def test_metadata_main_retries_rate_limited_symbols_after_cooldown_with_lower_pressure(monkeypatch):
    root = runtime_root("_test_runtime_metadata_rate_limit_retry")
    _reset_dir(root)
    metadata_path = root / "stock_metadata.csv"
    collect_calls: list[tuple[list[str], int, float]] = []
    sleeps: list[float] = []

    def _collect(symbols, **kwargs):  # noqa: ANN001, ANN202
        collect_calls.append((list(symbols), int(kwargs["max_workers"]), float(kwargs["delay"])))
        statuses = ["complete", "rate_limited"] if len(collect_calls) == 1 else ["complete"]
        return pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "market": "us",
                    "sector": "Tech" if status == "complete" else "",
                    "market_cap": 1000 if status == "complete" else None,
                    "fetch_status": status,
                    "source": "yfinance",
                    "last_attempted_at": "2026-04-18T00:00:00Z",
                }
                for symbol, status in zip(symbols, statuses)
            ]
        )

    monkeypatch.setenv("INVEST_PROTO_METADATA_MAX_WORKERS", "2")
    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))
    monkeypatch.setattr(collector, "get_symbols", lambda market: ["AAA", "BBB"])
    monkeypatch.setattr(collector, "METADATA_BATCH_SIZE", 2)
    monkeypatch.setattr(collector, "METADATA_RATE_LIMIT_COOLDOWN_SECONDS", 45.0)
    monkeypatch.setattr(collector.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(collector, "collect_stock_metadata", _collect)

    final_df = collector.main(market="us")

    assert collect_calls == [
        (["AAA", "BBB"], 2, pytest.approx(collector.METADATA_MIN_REQUEST_DELAY_SECONDS)),
        (["BBB"], 1, pytest.approx(1.12)),
    ]
    assert sleeps == [45.0]
    by_symbol = {str(row["symbol"]): row for _, row in final_df.iterrows()}
    assert by_symbol["BBB"]["fetch_status"] == "complete"


def test_metadata_main_skips_batch_pause_when_no_provider_fetch_symbols(monkeypatch):
    root = runtime_root("_test_runtime_metadata_reference_only_pause_skip")
    _reset_dir(root)
    metadata_path = root / "stock_metadata.csv"
    sleeps: list[float] = []

    def _reference_only(symbols, **kwargs):  # noqa: ANN001, ANN202
        frame = pd.DataFrame(
            [
                {
                    "symbol": symbol,
                    "market": "kr",
                    "provider_symbol": f"{symbol}.KS",
                    "name": f"KR {symbol}",
                    "exchange": "KOSPI",
                    "security_type": "COMMON_STOCK",
                    "sector": "Information Technology",
                    "industry": "Semiconductors",
                    "market_cap": 1000,
                    "shares_outstanding": 10,
                    "fetch_status": "complete",
                    "source": "fdr_listing+financedatabase",
                    "last_attempted_at": "2026-04-18T00:00:00Z",
                }
                for symbol in symbols
            ]
        )
        frame.attrs["collector_diagnostics"] = {"counts": {"provider_fetch_symbols": 0}}
        frame.attrs["timings"] = {}
        return frame

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))
    monkeypatch.setattr(collector, "get_symbols", lambda market: ["005930", "000660"])
    monkeypatch.setattr(collector, "METADATA_BATCH_SIZE", 1)
    monkeypatch.setattr(collector.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(collector, "_prefetch_kr_reference_metadata", lambda symbols: {})
    monkeypatch.setattr(collector, "collect_stock_metadata", _reference_only)

    collector.main(market="kr")

    assert sleeps == []


def test_metadata_main_prefills_kr_missing_universe_before_yahoo_queue(monkeypatch):
    root = runtime_root("_test_runtime_metadata_main_kr_prefill")
    _reset_dir(root)
    metadata_path = root / "stock_metadata_kr.csv"
    collected_batches: list[list[str]] = []

    def _prefill(symbols: list[str]) -> dict[str, dict[str, object]]:
        assert symbols == ["005930", "000660"]
        return {
            "005930": {
                "symbol": "005930",
                "market": "kr",
                "provider_symbol": "005930.KS",
                "name": "Samsung Electronics",
                "security_type": "COMMON_STOCK",
                "sector": "Information Technology",
                "industry": "Semiconductors",
                "market_cap": 1000,
                "shares_outstanding": 10,
                "source": "fdr_listing+financedatabase",
            },
            "000660": {
                "symbol": "000660",
                "market": "kr",
                "provider_symbol": "000660.KS",
                "name": "SK Hynix",
                "exchange": "KOSPI",
                "security_type": "COMMON_STOCK",
                "source": "fdr_listing+financedatabase",
            },
        }

    def _collect(symbols, **kwargs):  # noqa: ANN001, ANN202
        collected_batches.append(list(symbols))
        frame = pd.DataFrame(
            [
                {
                    "symbol": "000660",
                    "market": "kr",
                    "provider_symbol": "000660.KS",
                    "name": "SK Hynix",
                    "exchange": "KOSPI",
                    "security_type": "COMMON_STOCK",
                    "sector": "Information Technology",
                    "industry": "Semiconductors",
                    "market_cap": 2000,
                    "shares_outstanding": 20,
                    "fetch_status": "complete",
                    "source": "fdr_listing+financedatabase+yfinance",
                    "last_attempted_at": "2026-04-18T00:00:00Z",
                }
            ]
        )
        frame.attrs["collector_diagnostics"] = {"counts": {"provider_fetch_symbols": len(symbols)}}
        frame.attrs["timings"] = {}
        return frame

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))
    monkeypatch.setattr(collector, "get_symbols", lambda market: ["005930", "000660"])
    monkeypatch.setattr(collector, "_prefetch_kr_reference_metadata", _prefill)
    monkeypatch.setattr(collector, "collect_stock_metadata", _collect)

    final_df = collector.main(market="kr")

    assert collected_batches == []
    by_symbol = {str(row["symbol"]): row for _, row in final_df.iterrows()}
    assert by_symbol["005930"]["fetch_status"] == "complete"
    assert by_symbol["005930"]["source"] == "fdr_listing+financedatabase"
    assert by_symbol["000660"]["fetch_status"] == "complete"
    assert by_symbol["000660"]["source"] == "fdr_listing+financedatabase"


def test_metadata_main_does_not_repeat_failed_kr_prefill_inside_batches(monkeypatch):
    root = runtime_root("_test_runtime_metadata_main_kr_prefill_sentinel")
    _reset_dir(root)
    metadata_path = root / "stock_metadata_kr.csv"
    prefill_calls: list[list[str]] = []
    fetched_symbols: list[str] = []

    def _prefill(symbols: list[str]) -> dict[str, dict[str, object]]:
        prefill_calls.append(list(symbols))
        return {}

    def _fetch(symbol: str, **kwargs):  # noqa: ANN001, ANN201
        fetched_symbols.append(symbol)
        record = collector._blank_record(symbol, "kr", provider_symbol=f"{symbol}.KS")
        record.update(
            {
                "exchange": "KOSPI",
                "sector": "Information Technology",
                "industry": "Semiconductors",
                "market_cap": 1000,
                "shares_outstanding": 10,
                "fetch_status": "complete",
                "source": "yfinance",
                "last_attempted_at": "2026-04-18T00:00:00Z",
            }
        )
        return record

    monkeypatch.setattr(collector, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(collector, "bootstrap_yfinance_cache", lambda: None)
    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))
    monkeypatch.setattr(collector, "get_symbols", lambda market: ["005930", "000660"])
    monkeypatch.setattr(collector, "METADATA_BATCH_SIZE", 1)
    monkeypatch.setattr(collector, "_prefetch_kr_reference_metadata", _prefill)
    monkeypatch.setattr(collector, "fetch_metadata", _fetch)
    monkeypatch.setattr(collector.time, "sleep", lambda seconds: None)

    collector.main(market="kr")

    assert prefill_calls == [["005930", "000660"]]
    assert fetched_symbols == ["005930", "000660"]


def test_record_from_yfinance_populates_growth_fields_and_fast_info_fallback():
    record = collector._record_from_yfinance(
        "AAPL",
        "us",
        "AAPL",
        {
            "quoteType": "ETF",
            "longName": "Example Growth ETF",
            "fundFamily": "Example Funds",
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
    assert record["name"] == "Example Growth ETF"
    assert record["quote_type"] == "ETF"
    assert record["security_type"] == "ETF"
    assert record["fund_family"] == "Example Funds"
    assert record["market_cap"] == 3_759_000_000_000
    assert record["shares_outstanding"] == 14_697_926_000
    assert record["revenue_growth"] == pytest.approx(15.7)
    assert record["earnings_growth"] == pytest.approx(15.9)
    assert record["return_on_equity"] == pytest.approx(1.52)


def test_collect_stock_metadata_uses_metadata_worker_env(monkeypatch, capsys):
    monkeypatch.setenv("INVEST_PROTO_METADATA_MAX_WORKERS", "3")
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

    collector.collect_stock_metadata(["AAA", "BBB", "CCC", "DDD"], market="us", delay=0)

    captured = capsys.readouterr()
    assert "[Metadata] Fetch started (us) - total=4, workers=3" in captured.out


def test_get_missing_symbols_reuses_fresh_complete_not_found_and_recent_retryable_rows():
    cached = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "market": "us",
                "market_cap": 1000,
                "exchange": "NMS",
                "fetch_status": "complete",
                "source": "cache",
                "last_attempted_at": "2026-04-18T00:00:00Z",
            },
            {
                "symbol": "BBB",
                "market": "us",
                "market_cap": 1000,
                "fetch_status": "partial_fast_info",
                "source": "yfinance",
                "last_attempted_at": "2026-04-18T00:00:00Z",
            },
            {
                "symbol": "CCC",
                "market": "us",
                "fetch_status": "not_found",
                "source": "yfinance",
                "last_attempted_at": "2026-04-18T00:00:00Z",
            },
        ]
    )

    missing = collector.get_missing_symbols(
        cached,
        ["AAA", "BBB", "CCC", "DDD"],
        now_ts=time.mktime(time.strptime("2026-04-18T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ")),
    )

    assert missing == ["DDD"]


def test_get_missing_symbols_retries_statuses_after_status_specific_ttl() -> None:
    cached = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "market": "us",
                "market_cap": 1000,
                "exchange": "NMS",
                "fetch_status": "complete",
                "source": "cache",
                "last_attempted_at": "2026-04-01T00:00:00Z",
            },
            {
                "symbol": "BBB",
                "market": "us",
                "market_cap": 1000,
                "fetch_status": "partial_fast_info",
                "source": "yfinance",
                "last_attempted_at": "2026-04-10T00:00:00Z",
            },
            {
                "symbol": "CCC",
                "market": "us",
                "fetch_status": "not_found",
                "source": "yfinance",
                "last_attempted_at": "2026-04-01T00:00:00Z",
            },
        ]
    )

    missing = collector.get_missing_symbols(
        cached,
        ["AAA", "BBB", "CCC", "DDD"],
        now_ts=time.mktime(time.strptime("2026-04-18T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ")),
    )

    assert missing == ["BBB", "DDD"]


def test_get_missing_symbols_reuses_fresh_partial_etf_rows_when_fundamentals_are_not_expected():
    cached = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "market": "us",
                "exchange": "PCX",
                "quote_type": "ETF",
                "security_type": "ETF",
                "fund_family": "Example Funds",
                "fetch_status": "partial_fast_info",
                "fundamentals_expected": False,
                "earnings_expected": False,
                "source": "yfinance",
                "last_attempted_at": "2026-04-18T00:00:00Z",
            },
            {
                "symbol": "BBB",
                "market": "us",
                "exchange": "NMS",
                "fetch_status": "partial_fast_info",
                "fundamentals_expected": True,
                "source": "yfinance",
                "last_attempted_at": "2026-04-10T00:00:00Z",
            },
        ]
    )

    missing = collector.get_missing_symbols(
        cached,
        ["AAA", "BBB"],
        now_ts=time.mktime(time.strptime("2026-04-18T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ")),
    )

    assert missing == ["BBB"]


def test_get_missing_symbols_retries_complete_rows_after_complete_ttl() -> None:
    cached = pd.DataFrame(
        [
            {
                "symbol": "AAA",
                "market": "us",
                "market_cap": 1000,
                "exchange": "NMS",
                "fetch_status": "complete",
                "source": "cache",
                "last_attempted_at": "2026-03-18T00:00:00Z",
            }
        ]
    )

    missing = collector.get_missing_symbols(
        cached,
        ["AAA"],
        now_ts=time.mktime(time.strptime("2026-04-18T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ")),
    )

    assert missing == ["AAA"]


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


def test_fetch_metadata_treats_etf_identity_as_complete_when_fundamentals_are_not_expected(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(collector, "iter_provider_symbols", lambda symbol, market: [symbol])
    monkeypatch.setattr(collector, "wait_for_yahoo_request_slot", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(collector, "extend_yahoo_cooldown", lambda *args, **kwargs: 0.0)
    monkeypatch.setattr(
        collector,
        "_fetch_yfinance_info_quietly",
        lambda provider_symbol: (
            {
                "quoteType": "ETF",
                "longName": "Example Allocation ETF",
                "fundFamily": "Example Funds",
                "exchange": "PCX",
            },
            {},
            False,
            False,
        ),
    )

    def _fail_yahooquery(*args, **kwargs):  # noqa: ANN002, ANN003
        calls.append("yahooquery")
        raise AssertionError("yahooquery should not run when ETF identity metadata is already sufficient")

    monkeypatch.setattr(collector, "fetch_metadata_yahooquery", _fail_yahooquery)

    record = collector.fetch_metadata("AAA", market="us", max_retries=1, delay=0.0)

    assert calls == []
    assert record["security_type"] == "ETF"
    assert record["fundamentals_expected"] is False
    assert record["fetch_status"] == "complete"
    assert record["source"] == "yfinance"


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
    monkeypatch.setattr(collector, "get_symbols", lambda market="us": [])

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


def test_records_from_fdr_listing_populates_kr_name_security_type_and_provider_symbol():
    frame = pd.DataFrame(
        [
            {
                "Code": "000660",
                "Name": "SK Hynix",
                "Market": "KOSPI",
                "Sector": "Semiconductors",
                "Industry": "Semiconductors",
                "Marcap": 1000,
                "Stocks": 10,
            }
        ]
    )

    records = collector._records_from_fdr_listing(frame, listing_name="KOSPI")

    assert records["000660"]["name"] == "SK Hynix"
    assert records["000660"]["exchange"] == "KOSPI"
    assert records["000660"]["security_type"] == "COMMON_STOCK"
    assert records["000660"]["provider_symbol"] == "000660.KS"


def test_collect_stock_metadata_for_kr_skips_yahoo_when_reference_prefill_is_complete(monkeypatch):
    attempted_symbols: list[str] = []

    def _prefill(symbols: list[str]) -> dict[str, dict[str, object]]:
        assert symbols == ["005930"]
        return {
            "005930": {
                "symbol": "005930",
                "market": "kr",
                "provider_symbol": "005930.KS",
                "name": "Samsung Electronics",
                "exchange": "KOSPI",
                "security_type": "COMMON_STOCK",
                "sector": "Information Technology",
                "industry": "Semiconductors",
                "market_cap": 1000,
                "shares_outstanding": 10,
                "source": "fdr_listing+financedatabase",
            }
        }

    def _fail_fetch(symbol: str, **kwargs):  # noqa: ANN001, ANN201
        attempted_symbols.append(symbol)
        raise AssertionError("Yahoo fallback should not run when reference metadata is already complete")

    monkeypatch.setattr(collector, "_prefetch_kr_reference_metadata", _prefill)
    monkeypatch.setattr(collector, "fetch_metadata", _fail_fetch)

    frame = collector.collect_stock_metadata(["005930"], market="kr", max_workers=1, delay=0.0)

    assert attempted_symbols == []
    row = frame.iloc[0]
    assert row["provider_symbol"] == "005930.KS"
    assert row["name"] == "Samsung Electronics"
    assert row["sector"] == "Information Technology"
    assert row["industry"] == "Semiconductors"
    assert row["source"] == "fdr_listing+financedatabase"
    assert frame.attrs["collector_diagnostics"]["counts"]["kr_reference_complete"] == 1
    assert frame.attrs["collector_diagnostics"]["counts"]["provider_fetch_symbols"] == 0
    assert frame.attrs["timings"]["provider_fetch_seconds"] == 0.0


def test_collect_stock_metadata_for_kr_accepts_reference_identity_without_yahoo(monkeypatch):
    attempted_symbols: list[str] = []

    def _prefill(symbols: list[str]) -> dict[str, dict[str, object]]:
        assert symbols == ["005930"]
        return {
            "005930": {
                "symbol": "005930",
                "market": "kr",
                "provider_symbol": "005930.KS",
                "name": "Samsung Electronics",
                "exchange": "KOSPI",
                "security_type": "COMMON_STOCK",
                "source": "fdr_listing",
            }
        }

    def _fail_fetch(symbol: str, **kwargs):  # noqa: ANN001, ANN201
        attempted_symbols.append(symbol)
        raise AssertionError("Yahoo fallback should not run for listed KR reference identity")

    monkeypatch.setattr(collector, "_prefetch_kr_reference_metadata", _prefill)
    monkeypatch.setattr(collector, "fetch_metadata", _fail_fetch)

    frame = collector.collect_stock_metadata(["005930"], market="kr", max_workers=1, delay=0.0)

    assert attempted_symbols == []
    row = frame.iloc[0]
    assert row["fetch_status"] == "complete"
    assert row["exchange"] == "KOSPI"
    assert row["source"] == "fdr_listing"
    assert frame.attrs["collector_diagnostics"]["counts"]["provider_fetch_symbols"] == 0


def test_collect_stock_metadata_for_kr_merges_reference_prefill_with_yahoo_fallback(monkeypatch):
    def _prefill(symbols: list[str]) -> dict[str, dict[str, object]]:
        assert symbols == ["005930"]
        return {
            "005930": {
                "symbol": "005930",
                "market": "kr",
                "provider_symbol": "005930.KS",
                "name": "Samsung Electronics",
                "security_type": "COMMON_STOCK",
                "source": "fdr_listing+financedatabase",
            }
        }

    def _fetch(symbol: str, **kwargs):  # noqa: ANN001, ANN201
        record = collector._blank_record(symbol, "kr", provider_symbol="005930.KS")
        record.update(
            {
                "exchange": "KOSPI",
                "sector": "Information Technology",
                "industry": "Semiconductors",
                "market_cap": 1000,
                "shares_outstanding": 10,
                "fetch_status": "complete",
                "source": "yfinance",
                "last_attempted_at": "2026-04-18T00:00:00Z",
            }
        )
        return record

    monkeypatch.setattr(collector, "_prefetch_kr_reference_metadata", _prefill)
    monkeypatch.setattr(collector, "fetch_metadata", _fetch)

    frame = collector.collect_stock_metadata(["005930"], market="kr", max_workers=1, delay=0.0)

    row = frame.iloc[0]
    assert row["provider_symbol"] == "005930.KS"
    assert row["name"] == "Samsung Electronics"
    assert row["exchange"] == "KOSPI"
    assert row["sector"] == "Information Technology"
    assert row["industry"] == "Semiconductors"
    assert row["source"] == "fdr_listing+financedatabase+yfinance"


def test_load_cached_metadata_preserves_zero_padded_kr_symbols(monkeypatch):
    root = runtime_root("_test_runtime_metadata_kr_zero_pad")
    _reset_dir(root)
    metadata_path = root / "stock_metadata_kr.csv"
    pd.DataFrame(
        [
            {
                "symbol": "000020",
                "market": "kr",
                "provider_symbol": "000020.KS",
                "name": "동화약품",
                "exchange": "KOSPI",
                "earnings_growth": 12.0,
                "return_on_equity": 0.18,
                "fetch_status": "complete",
                "source": "fdr_listing_prefill",
                "last_attempted_at": "2026-04-17T00:00:00Z",
            }
        ]
    ).to_csv(metadata_path, index=False, encoding="utf-8-sig")

    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))

    cached = collector.load_cached_metadata("kr", max_age_days=7, allow_stale=True)

    assert cached is not None
    assert str(cached.iloc[0]["symbol"]) == "000020"
    assert str(cached.iloc[0]["provider_symbol"]) == "000020.KS"
    assert str(cached.iloc[0]["name"]) == "동화약품"
