from __future__ import annotations

import sys
import types
from pathlib import Path

import pandas as pd

from tests._paths import runtime_root
from utils import market_data_contract
from utils.market_data_contract import (
    OhlcvFreshnessSummary,
    PricePolicy,
    describe_ohlcv_freshness,
    load_local_ohlcv_frames_ordered,
    load_local_ohlcv_frame,
    normalize_ohlcv_frame,
)
from utils.runtime_context import RuntimeContext
from utils.screening_cache import feature_row_cache_get_or_compute


def test_normalize_ohlcv_frame_handles_mixed_timezone_dates() -> None:
    frame = pd.DataFrame(
        {
            "date": [
                "2026-01-02 00:00:00",
                "2026-01-03T00:00:00+09:00",
                "2026-01-04T00:00:00Z",
            ],
            "open": [10.0, 11.0, 12.0],
            "high": [11.0, 12.0, 13.0],
            "low": [9.0, 10.0, 11.0],
            "close": [10.5, 11.5, 12.5],
            "volume": [100, 110, 120],
        }
    )

    normalized = normalize_ohlcv_frame(frame, symbol="AAA")

    assert len(normalized) == 3
    assert list(normalized["date"]) == ["2026-01-02", "2026-01-03", "2026-01-04"]
    assert {
        "date",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "raw_close",
        "adj_close",
        "price_adjustment_factor",
        "price_adjustment_source",
        "price_policy",
        "volume",
    }.issubset(set(normalized.columns))


def test_split_adjusted_policy_keeps_raw_prices_without_split_evidence() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05"],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.0, 101.0],
            "Adj Close": [97.0, 98.0],
            "volume": [1000, 1100],
        }
    )

    normalized = normalize_ohlcv_frame(
        frame,
        symbol="AAA",
        price_policy=PricePolicy.SPLIT_ADJUSTED,
    )

    assert list(normalized["close"]) == [100.0, 101.0]
    assert set(normalized["price_adjustment_source"]) == {"raw"}
    assert set(normalized["price_policy"]) == {PricePolicy.SPLIT_ADJUSTED.value}


def test_total_return_adjusted_policy_uses_adj_close_proxy() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05"],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.0, 101.0],
            "Adj Close": [97.0, 98.0],
            "volume": [1000, 1100],
        }
    )

    normalized = normalize_ohlcv_frame(
        frame,
        symbol="AAA",
        price_policy=PricePolicy.TOTAL_RETURN_ADJUSTED,
    )

    assert normalized["close"].round(4).tolist() == [97.0, 98.0]
    assert set(normalized["price_adjustment_source"]) == {"adj_close_proxy"}
    assert set(normalized["price_policy"]) == {PricePolicy.TOTAL_RETURN_ADJUSTED.value}


def test_split_adjusted_policy_uses_explicit_split_factor_when_present() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05", "2026-01-06"],
            "open": [1000.0, 100.0, 101.0],
            "high": [1010.0, 102.0, 103.0],
            "low": [990.0, 98.0, 99.0],
            "close": [1000.0, 100.0, 101.0],
            "Adj Close": [970.0, 97.0, 98.0],
            "Stock Splits": [0.0, 10.0, 0.0],
            "Split Factor": [0.1, 1.0, 1.0],
            "volume": [1000, 1100, 1200],
        }
    )

    normalized = normalize_ohlcv_frame(
        frame,
        symbol="AAA",
        price_policy=PricePolicy.SPLIT_ADJUSTED,
    )

    assert normalized["close"].round(4).tolist() == [100.0, 100.0, 101.0]
    assert normalized["split_factor"].round(4).tolist() == [0.1, 1.0, 1.0]
    assert normalized["price_adjustment_source"].tolist() == ["split_factor", "split_factor", "split_factor"]


def test_total_return_adjusted_uses_explicit_split_factor_without_adj_close() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05"],
            "open": [1000.0, 100.0],
            "high": [1010.0, 102.0],
            "low": [990.0, 98.0],
            "close": [1000.0, 100.0],
            "Stock Splits": [0.0, 10.0],
            "Split Factor": [0.1, 1.0],
            "volume": [1000, 1100],
        }
    )

    normalized = normalize_ohlcv_frame(
        frame,
        symbol="AAA",
        price_policy=PricePolicy.TOTAL_RETURN_ADJUSTED,
    )

    assert normalized["close"].round(4).tolist() == [100.0, 100.0]
    assert normalized["price_adjustment_source"].tolist() == ["split_factor", "split_factor"]


def test_split_adjusted_policy_derives_factor_from_stock_splits_when_explicit_factor_missing() -> None:
    frame = pd.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05"],
            "open": [1000.0, 100.0],
            "high": [1010.0, 102.0],
            "low": [990.0, 98.0],
            "close": [1000.0, 100.0],
            "Stock Splits": [0.0, 10.0],
            "volume": [1000, 1100],
        }
    )

    normalized = normalize_ohlcv_frame(
        frame,
        symbol="AAA",
        price_policy=PricePolicy.SPLIT_ADJUSTED,
    )

    assert normalized["close"].round(4).tolist() == [100.0, 100.0]
    assert normalized["split_factor"].round(4).tolist() == [0.1, 1.0]
    assert normalized["price_adjustment_source"].tolist() == [
        "stock_splits_cumulative",
        "raw",
    ]


def test_load_local_ohlcv_frame_roundtrips_capitalized_cache_with_split_factor(monkeypatch) -> None:
    root = runtime_root("_test_runtime_market_data_roundtrip")
    root.mkdir(parents=True, exist_ok=True)
    cache_path = root / "AAA.csv"
    pd.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05"],
            "Open": [1000.0, 100.0],
            "High": [1010.0, 102.0],
            "Low": [990.0, 98.0],
            "Close": [1000.0, 100.0],
            "Adj Close": [980.0, 98.0],
            "Volume": [1000, 1100],
            "Dividends": [0.0, 0.0],
            "Stock Splits": [0.0, 10.0],
            "Split Factor": [0.1, 1.0],
            "symbol": ["AAA", "AAA"],
        }
    ).to_csv(cache_path, index=False)

    monkeypatch.setattr(market_data_contract, "_market_dir", lambda market: str(root))

    split_adjusted = load_local_ohlcv_frame("us", "AAA", price_policy=PricePolicy.SPLIT_ADJUSTED)
    total_return = load_local_ohlcv_frame("us", "AAA", price_policy=PricePolicy.TOTAL_RETURN_ADJUSTED)

    assert split_adjusted["close"].round(4).tolist() == [100.0, 100.0]
    assert split_adjusted["raw_close"].round(4).tolist() == [1000.0, 100.0]
    assert split_adjusted["price_adjustment_source"].tolist() == ["split_factor", "split_factor"]
    assert total_return["close"].round(4).tolist() == [980.0, 98.0]
    assert total_return["price_adjustment_source"].tolist() == ["adj_close_proxy", "adj_close_proxy"]


def test_parallel_ohlcv_loader_preserves_order_as_of_and_cache_copy(monkeypatch) -> None:
    from utils.runtime_context import RuntimeContext

    root = runtime_root("_test_runtime_parallel_ohlcv_loader")
    root.mkdir(parents=True, exist_ok=True)
    for symbol, closes in {
        "AAA": [10.0, 11.0, 12.0],
        "BBB": [20.0, 21.0, 22.0],
        "CCC": [30.0, 31.0, 32.0],
    }.items():
        pd.DataFrame(
            {
                "date": ["2026-01-02", "2026-01-05", "2026-01-06"],
                "open": closes,
                "high": [value + 1 for value in closes],
                "low": [value - 1 for value in closes],
                "close": closes,
                "volume": [1000, 1100, 1200],
            }
        ).to_csv(root / f"{symbol}.csv", index=False)

    monkeypatch.setattr(market_data_contract, "_market_dir", lambda market: str(root))
    runtime_context = RuntimeContext(market="us")

    first = load_local_ohlcv_frames_ordered(
        "us",
        ["BBB", "AAA", "CCC"],
        as_of="2026-01-05",
        price_policy=PricePolicy.SPLIT_ADJUSTED,
        runtime_context=runtime_context,
        max_workers=3,
    )
    first["BBB"].loc[:, "close"] = 999.0
    second = load_local_ohlcv_frames_ordered(
        "us",
        ["BBB", "AAA", "CCC"],
        as_of="2026-01-05",
        price_policy=PricePolicy.SPLIT_ADJUSTED,
        runtime_context=runtime_context,
        max_workers=3,
    )

    assert list(first.keys()) == ["BBB", "AAA", "CCC"]
    assert list(second.keys()) == ["BBB", "AAA", "CCC"]
    assert second["BBB"]["close"].tolist() == [20.0, 21.0]
    assert second["AAA"]["date"].tolist() == ["2026-01-02", "2026-01-05"]
    assert runtime_context.cache_stats["misses"] == 3
    assert runtime_context.cache_stats["hits"] == 3


def test_ohlcv_parquet_sidecar_cache_hit_and_source_invalidation(monkeypatch) -> None:
    root = runtime_root("_test_runtime_ohlcv_parquet_cache")
    root.mkdir(parents=True, exist_ok=True)
    source_path = root / "AAA.csv"
    pd.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05"],
            "open": [10.0, 11.0],
            "high": [11.0, 12.0],
            "low": [9.0, 10.0],
            "close": [10.0, 11.0],
            "volume": [1000, 1100],
            "unused_blob": ["x", "y"],
        }
    ).to_csv(source_path, index=False)

    parquet_store: dict[str, pd.DataFrame] = {}
    read_csv_calls: list[dict[str, object]] = []

    original_read_csv = pd.read_csv

    def _read_csv(path, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        if Path(path) == source_path:
            read_csv_calls.append(dict(kwargs))
        return original_read_csv(path, *args, **kwargs)

    def _to_parquet(self, path, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        parquet_store[str(path)] = self.copy()
        Path(path).write_text("parquet-stub", encoding="utf-8")

    def _read_parquet(path, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        key = str(path)
        if key in parquet_store:
            return parquet_store[key].copy()
        matching = [frame for stored_path, frame in parquet_store.items() if stored_path.startswith(key)]
        if matching:
            return matching[-1].copy()
        raise FileNotFoundError(key)

    monkeypatch.setenv("INVEST_PROTO_OHLCV_PARQUET_CACHE", "1")
    monkeypatch.setattr(market_data_contract, "_market_dir", lambda market: str(root))
    monkeypatch.setattr(market_data_contract, "_OHLCV_PARQUET_DISABLED_REASON", "")
    monkeypatch.setattr(pd, "read_csv", _read_csv)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", _to_parquet)
    monkeypatch.setattr(pd, "read_parquet", _read_parquet)

    first = load_local_ohlcv_frame(
        "us",
        "AAA",
        required_columns=("date", "open", "high", "low", "close", "volume"),
    )
    second = load_local_ohlcv_frame(
        "us",
        "AAA",
        required_columns=("date", "open", "high", "low", "close", "volume"),
    )

    assert first["close"].tolist() == [10.0, 11.0]
    assert second["close"].tolist() == [10.0, 11.0]
    assert len(read_csv_calls) == 1
    assert "usecols" in read_csv_calls[0]

    pd.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-05"],
            "open": [10.0, 12.0],
            "high": [11.0, 13.0],
            "low": [9.0, 11.0],
            "close": [10.0, 12.0],
            "volume": [1000, 1200],
            "unused_blob": ["x", "z"],
        }
    ).to_csv(source_path, index=False)

    third = load_local_ohlcv_frame(
        "us",
        "AAA",
        required_columns=("date", "open", "high", "low", "close", "volume"),
    )

    assert third["close"].tolist() == [10.0, 12.0]
    assert len(read_csv_calls) == 2


def test_feature_row_cache_hits_and_invalidates_on_source_change(monkeypatch) -> None:
    root = runtime_root("_test_runtime_feature_row_cache")
    source_path = root / "AAA.csv"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_text("date,close\n2026-01-02,10\n", encoding="utf-8")
    runtime_context = RuntimeContext(market="us")
    calls = {"count": 0}

    def _compute() -> dict[str, object]:
        calls["count"] += 1
        return {"symbol": "AAA", "score": float(calls["count"])}

    monkeypatch.setenv("INVEST_PROTO_FEATURE_ROW_CACHE", "1")
    monkeypatch.setattr("utils.screening_cache.get_market_data_dir", lambda market: str(root))

    first = feature_row_cache_get_or_compute(
        namespace="unit",
        market="us",
        symbol="AAA",
        as_of="2026-01-05",
        feature_version="v1",
        source_path=str(source_path),
        compute_fn=_compute,
        runtime_context=runtime_context,
    )
    second = feature_row_cache_get_or_compute(
        namespace="unit",
        market="us",
        symbol="AAA",
        as_of="2026-01-05",
        feature_version="v1",
        source_path=str(source_path),
        compute_fn=_compute,
        runtime_context=runtime_context,
    )

    assert first == {"symbol": "AAA", "score": 1.0}
    assert second == first
    assert calls["count"] == 1

    source_path.write_text("date,close\n2026-01-02,12\n", encoding="utf-8")
    third = feature_row_cache_get_or_compute(
        namespace="unit",
        market="us",
        symbol="AAA",
        as_of="2026-01-05",
        feature_version="v1",
        source_path=str(source_path),
        compute_fn=_compute,
        runtime_context=runtime_context,
    )

    assert third == {"symbol": "AAA", "score": 2.0}
    assert runtime_context.runtime_metrics["feature_analysis"]["cache_hits"] == 1
    assert runtime_context.runtime_metrics["feature_analysis"]["cache_misses"] == 2


def test_runtime_worker_count_reduces_default_internal_workers_when_stage_parallel(monkeypatch) -> None:
    monkeypatch.setenv("INVEST_PROTO_SCREENING_STAGE_PARALLEL", "1")
    monkeypatch.setenv("INVEST_PROTO_SCREENING_STAGE_WORKERS", "4")
    monkeypatch.delenv("INVEST_PROTO_SYMBOL_ANALYSIS_WORKERS", raising=False)
    monkeypatch.setattr(market_data_contract.os, "cpu_count", lambda: 8)
    runtime_context = RuntimeContext(market="us")

    workers = market_data_contract._runtime_worker_count(
        100,
        env_var="INVEST_PROTO_SYMBOL_ANALYSIS_WORKERS",
        cap=8,
        runtime_context=runtime_context,
        scope="unit.symbol_analysis",
    )

    assert workers == 2
    assert runtime_context.runtime_metrics["worker_budget"]["unit.symbol_analysis"]["workers"] == 2

    monkeypatch.setenv("INVEST_PROTO_SYMBOL_ANALYSIS_WORKERS", "6")
    assert (
        market_data_contract._runtime_worker_count(
            100,
            env_var="INVEST_PROTO_SYMBOL_ANALYSIS_WORKERS",
            cap=8,
        )
        == 6
    )


def test_runtime_worker_count_relaxes_leader_lagging_budget_when_shared_cache_is_enabled(monkeypatch) -> None:
    monkeypatch.setenv("INVEST_PROTO_SCREENING_STAGE_PARALLEL", "1")
    monkeypatch.setenv("INVEST_PROTO_SCREENING_STAGE_WORKERS", "4")
    monkeypatch.setenv("INVEST_PROTO_SCREENING_SHARED_OHLCV_CACHE", "1")
    monkeypatch.delenv("INVEST_PROTO_FOLLOWER_ANALYSIS_WORKERS", raising=False)
    monkeypatch.setattr(market_data_contract.os, "cpu_count", lambda: 8)

    assert (
        market_data_contract._runtime_worker_count(
            100,
            env_var="INVEST_PROTO_FOLLOWER_ANALYSIS_WORKERS",
            cap=4,
            scope="leader_lagging.follower_analysis",
        )
        == 4
    )

    monkeypatch.setenv("INVEST_PROTO_SCREENING_SHARED_OHLCV_CACHE", "0")
    assert (
        market_data_contract._runtime_worker_count(
            100,
            env_var="INVEST_PROTO_FOLLOWER_ANALYSIS_WORKERS",
            cap=4,
            scope="leader_lagging.follower_analysis",
        )
        == 2
    )


def test_download_yfinance_ohlcv_requests_actions_and_preserves_split_adjusted_semantics(monkeypatch) -> None:
    requested: dict[str, object] = {}

    history = pd.DataFrame(
        {
            "Open": [1000.0, 100.0],
            "High": [1010.0, 102.0],
            "Low": [990.0, 98.0],
            "Close": [1000.0, 100.0],
            "Volume": [1000, 1100],
            "Stock Splits": [0.0, 10.0],
        },
        index=pd.to_datetime(["2026-01-02", "2026-01-05"]),
    )

    class _FakeTicker:
        def __init__(self, symbol: str) -> None:
            requested["symbol"] = symbol

        def history(self, **kwargs):  # noqa: ANN003
            requested.update(kwargs)
            return history.copy()

    fake_module = types.ModuleType("yfinance")
    setattr(fake_module, "Ticker", _FakeTicker)
    monkeypatch.setitem(sys.modules, "yfinance", fake_module)

    normalized = market_data_contract._download_yfinance_ohlcv(
        "AAA",
        price_policy=PricePolicy.SPLIT_ADJUSTED,
    )

    assert requested["symbol"] == "AAA"
    assert requested["actions"] is True
    assert normalized["close"].round(4).tolist() == [100.0, 100.0]
    assert normalized["price_adjustment_source"].tolist() == [
        "stock_splits_cumulative",
        "raw",
    ]


def test_describe_ohlcv_freshness_marks_closed_stale_future_and_empty() -> None:
    closed = describe_ohlcv_freshness(
        pd.DataFrame({"date": ["2026-04-20", "2026-04-21"], "close": [10.0, 11.0]}),
        market="us",
        as_of="2026-04-21",
        latest_completed_session="2026-04-21",
        symbol="AAA",
    )
    stale = describe_ohlcv_freshness(
        pd.DataFrame({"date": ["2026-04-17"], "close": [10.0]}),
        market="us",
        as_of="2026-04-21",
        latest_completed_session="2026-04-21",
        symbol="BBB",
    )
    future = describe_ohlcv_freshness(
        pd.DataFrame({"date": ["2026-04-22"], "close": [10.0]}),
        market="us",
        as_of="2026-04-21",
        latest_completed_session="2026-04-21",
        symbol="CCC",
    )
    empty = describe_ohlcv_freshness(
        pd.DataFrame({"close": [10.0]}),
        market="us",
        as_of="2026-04-21",
        latest_completed_session="2026-04-21",
        symbol="DDD",
    )

    assert closed.status == "closed"
    assert closed.latest_date == "2026-04-21"
    assert stale.status == "stale"
    assert future.status == "future_or_partial"
    assert empty.status == "empty"

    summary = OhlcvFreshnessSummary.from_reports([closed, stale, future, empty])

    assert summary.counts == {
        "closed": 1,
        "stale": 1,
        "future_or_partial": 1,
        "empty": 1,
    }
    assert summary.target_date == "2026-04-21"
    assert summary.latest_completed_session == "2026-04-21"
    assert summary.mode == "default_completed_session"
    assert [item["symbol"] for item in summary.examples] == ["BBB", "CCC", "DDD"]


def test_describe_ohlcv_freshness_records_explicit_replay_mode() -> None:
    report = describe_ohlcv_freshness(
        pd.DataFrame({"date": ["2026-04-17", "2026-04-21"], "close": [10.0, 11.0]}),
        market="us",
        as_of="2026-04-17",
        latest_completed_session="2026-04-21",
        explicit_as_of=True,
        symbol="AAA",
    )

    assert report.status == "future_or_partial"
    assert report.target_date == "2026-04-17"
    assert report.latest_completed_session == "2026-04-21"
    assert report.mode == "explicit_replay"
