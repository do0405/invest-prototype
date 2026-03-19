from __future__ import annotations

import sys
import types

import pandas as pd

from tests._paths import runtime_root
from utils import market_data_contract
from utils.market_data_contract import PricePolicy, load_local_ohlcv_frame, normalize_ohlcv_frame


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


def test_download_yfinance_ohlcv_requests_actions_and_preserves_split_adjusted_semantics(monkeypatch) -> None:
    requested: dict[str, object] = {}

    history = pd.DataFrame(
        {
            "Open": [100.0, 100.0],
            "High": [101.0, 102.0],
            "Low": [99.0, 98.0],
            "Close": [100.0, 100.0],
            "Adj Close": [970.0, 97.0],
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
    assert normalized["price_adjustment_source"].tolist() == ["raw", "raw"]
