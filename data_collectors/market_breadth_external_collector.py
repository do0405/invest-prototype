#!/usr/bin/env python3
"""Collect ETF flow/RS external snapshots as CSV caches for market breadth features."""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

import pandas as pd

from config import EXTERNAL_DATA_DIR
from utils.external_data_cache import write_csv_atomic
from utils.market_data_contract import load_benchmark_data, load_market_ohlcv_frames, normalize_market


DEFAULT_SECTOR_ETFS: dict[str, list[str]] = {
    "us": [
        "XLB",
        "XLC",
        "XLE",
        "XLF",
        "XLI",
        "XLK",
        "XLP",
        "XLRE",
        "XLU",
        "XLV",
        "XLY",
    ],
    "kr": [],
}

DEFAULT_BENCHMARKS: dict[str, list[str]] = {
    "us": ["SPY", "QQQ", "DIA", "IWM"],
    "kr": ["069500", "102110"],
}


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).replace(",", ""))
    except Exception:
        return None


def _safe_score_0_100(value: float | None) -> float | None:
    if value is None:
        return None
    return max(0.0, min(100.0, float(value)))


def _normalize_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame()
    normalized = frame.copy()
    lowered = {str(column).lower(): column for column in normalized.columns}
    if "date" not in lowered:
        if "datetime" in lowered:
            normalized = normalized.rename(columns={lowered["datetime"]: "date"})
        else:
            return pd.DataFrame()
    elif lowered["date"] != "date":
        normalized = normalized.rename(columns={lowered["date"]: "date"})

    if "close" not in lowered:
        if "adj close" in lowered:
            normalized = normalized.rename(columns={lowered["adj close"]: "close"})
        elif "adj_close" in lowered:
            normalized = normalized.rename(columns={lowered["adj_close"]: "close"})
        else:
            return pd.DataFrame()
    elif lowered["close"] != "close":
        normalized = normalized.rename(columns={lowered["close"]: "close"})

    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce", utc=True).dt.tz_localize(None)
    normalized["close"] = pd.to_numeric(normalized["close"], errors="coerce")
    normalized = normalized.dropna(subset=["date", "close"]).sort_values("date")
    if normalized.empty:
        return pd.DataFrame()
    return normalized[["date", "close"]].copy()


def _shares_cache_path(market: str, symbol: str) -> str:
    return os.path.join(
        EXTERNAL_DATA_DIR,
        "etf",
        "shares_outstanding",
        normalize_market(market),
        f"{symbol}.csv",
    )


def _flow_output_path(market: str) -> str:
    return os.path.join(EXTERNAL_DATA_DIR, "etf", "flows", f"{normalize_market(market)}_sector_etf_flows.csv")


def _rs_output_path(market: str) -> str:
    return os.path.join(EXTERNAL_DATA_DIR, "etf", "rs", f"{normalize_market(market)}_sector_rs.csv")


def _load_cached_shares(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        frame = pd.read_csv(path)
    except Exception:
        return pd.DataFrame()
    if frame.empty:
        return pd.DataFrame()
    lowered = {str(column).lower(): column for column in frame.columns}
    if "date" not in lowered:
        return pd.DataFrame()
    value_col = lowered.get("shares_outstanding") or lowered.get("shares")
    if value_col is None:
        return pd.DataFrame()
    normalized = frame.rename(columns={lowered["date"]: "date", value_col: "shares_outstanding"})
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce", utc=True).dt.tz_localize(None)
    normalized["shares_outstanding"] = pd.to_numeric(normalized["shares_outstanding"], errors="coerce")
    normalized = normalized.dropna(subset=["date", "shares_outstanding"]).sort_values("date")
    if normalized.empty:
        return pd.DataFrame()
    return normalized[["date", "shares_outstanding"]].copy()


def _fetch_shares_yfinance(symbol: str, start: str, end: str) -> pd.DataFrame:
    try:
        import yfinance as yf  # type: ignore
    except Exception:
        return pd.DataFrame()

    try:
        ticker = yf.Ticker(symbol)
        shares_series = ticker.get_shares_full(start=start, end=end)
    except Exception:
        return pd.DataFrame()

    if shares_series is None:
        return pd.DataFrame()
    if isinstance(shares_series, pd.DataFrame):
        if shares_series.empty:
            return pd.DataFrame()
        if shares_series.shape[1] == 1:
            series = shares_series.iloc[:, 0]
        else:
            first_col = shares_series.columns[0]
            series = shares_series[first_col]
    else:
        series = shares_series

    if series is None or len(series) == 0:
        return pd.DataFrame()

    frame = pd.DataFrame({"date": series.index, "shares_outstanding": series.values})
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce", utc=True).dt.tz_localize(None)
    frame["shares_outstanding"] = pd.to_numeric(frame["shares_outstanding"], errors="coerce")
    frame = frame.dropna(subset=["date", "shares_outstanding"]).sort_values("date")
    if frame.empty:
        return pd.DataFrame()
    return frame[["date", "shares_outstanding"]].copy()


def _merge_price_shares(price_frame: pd.DataFrame, shares_frame: pd.DataFrame) -> pd.DataFrame:
    if price_frame.empty or shares_frame.empty:
        return pd.DataFrame()
    merged = price_frame.merge(shares_frame, on="date", how="left").sort_values("date")
    merged["shares_outstanding"] = merged["shares_outstanding"].ffill()
    merged = merged.dropna(subset=["shares_outstanding"]).copy()
    if merged.empty:
        return pd.DataFrame()
    merged["delta_shares"] = merged["shares_outstanding"].diff()
    merged["nav_used"] = merged["close"]
    merged["flow_usd"] = merged["delta_shares"] * merged["nav_used"]
    merged["aum"] = merged["shares_outstanding"] * merged["nav_used"]
    merged["aum_prev"] = merged["aum"].shift(1)
    merged["flow_pct_aum"] = merged["flow_usd"] / merged["aum_prev"].replace({0.0: pd.NA})
    return merged


def _build_flow_rows(
    *,
    market: str,
    symbol: str,
    price_frame: pd.DataFrame,
    as_of: str,
) -> tuple[pd.DataFrame, str]:
    cache_path = _shares_cache_path(market=market, symbol=symbol)
    shares = _load_cached_shares(cache_path)
    source = "cached_csv"

    if shares.empty and normalize_market(market) == "us":
        start = (price_frame["date"].iloc[0] - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
        shares = _fetch_shares_yfinance(symbol, start=start, end=as_of)
        if not shares.empty:
            source = "yfinance"
            write_csv_atomic(shares, cache_path, index=False)

    if shares.empty:
        return pd.DataFrame(), source

    merged = _merge_price_shares(price_frame=price_frame, shares_frame=shares)
    if merged.empty:
        return pd.DataFrame(), source

    merged = merged[merged["date"] <= pd.to_datetime(as_of)].copy()
    if merged.empty:
        return pd.DataFrame(), source

    merged["symbol"] = symbol
    merged["method"] = "delta_so_x_nav"
    merged["source"] = source
    output_cols = [
        "date",
        "symbol",
        "delta_shares",
        "nav_used",
        "shares_outstanding",
        "flow_usd",
        "flow_pct_aum",
        "method",
        "source",
    ]
    return merged[output_cols].copy(), source


def _build_rs_rows(
    *,
    symbol: str,
    symbol_price: pd.DataFrame,
    benchmark_symbol: str,
    benchmark_price: pd.DataFrame,
    as_of: str,
    window: int,
) -> pd.DataFrame:
    if symbol_price.empty or benchmark_price.empty:
        return pd.DataFrame()
    merged = symbol_price.rename(columns={"close": "close_symbol"}).merge(
        benchmark_price.rename(columns={"close": "close_benchmark"}),
        on="date",
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame()
    merged = merged[merged["date"] <= pd.to_datetime(as_of)].copy()
    if merged.empty:
        return pd.DataFrame()
    lookback = max(2, int(window))
    merged["ret_20d_symbol"] = merged["close_symbol"] / merged["close_symbol"].shift(lookback) - 1.0
    merged["ret_20d_benchmark"] = merged["close_benchmark"] / merged["close_benchmark"].shift(lookback) - 1.0
    merged["rs_spread_20d"] = merged["ret_20d_symbol"] - merged["ret_20d_benchmark"]
    merged["rs_ratio"] = merged["close_symbol"] / merged["close_benchmark"].replace({0.0: pd.NA})
    merged["rs_score"] = merged["rs_spread_20d"].apply(
        lambda value: _safe_score_0_100(None if pd.isna(value) else (50.0 + (float(value) * 250.0)))
    )
    merged["symbol"] = symbol
    merged["benchmark_symbol"] = benchmark_symbol
    merged["source"] = "local_csv_or_provider"
    return merged[
        [
            "date",
            "symbol",
            "benchmark_symbol",
            "ret_20d_symbol",
            "ret_20d_benchmark",
            "rs_spread_20d",
            "rs_ratio",
            "rs_score",
            "source",
        ]
    ].copy()


def _resolve_target_symbols(market: str, symbols: Iterable[str] | None) -> list[str]:
    if symbols is not None:
        resolved = [str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()]
        return sorted(set(resolved))
    defaults = DEFAULT_SECTOR_ETFS.get(normalize_market(market), [])
    return sorted(set(defaults))


def collect_market_breadth_external_csv(
    *,
    market: str = "us",
    as_of: str | None = None,
    symbols: Iterable[str] | None = None,
    benchmark_symbol: str | None = None,
    rs_window: int | None = None,
) -> Dict[str, Any]:
    market_key = normalize_market(market)
    run_date = as_of or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    target_symbols = _resolve_target_symbols(market=market_key, symbols=symbols)
    if not target_symbols:
        return {
            "market": market_key,
            "as_of": run_date,
            "flow_rows": 0,
            "rs_rows": 0,
            "symbols": 0,
            "message": "no_target_symbols",
        }

    fetched = load_market_ohlcv_frames(
        market_key,
        target_symbols,
        as_of=run_date,
        allow_yfinance_fallback=True,
    )
    normalized_prices: dict[str, pd.DataFrame] = {}
    for symbol in target_symbols:
        frame = fetched.get(symbol) if isinstance(fetched, dict) else None
        price_frame = _normalize_price_frame(frame) if isinstance(frame, pd.DataFrame) else pd.DataFrame()
        if not price_frame.empty:
            normalized_prices[symbol] = price_frame

    benchmark_candidates = [str(benchmark_symbol).strip().upper()] if benchmark_symbol else DEFAULT_BENCHMARKS.get(market_key, [])
    benchmark_symbol, benchmark_frame = load_benchmark_data(
        market_key,
        benchmark_candidates,
        as_of=run_date,
        allow_yfinance_fallback=True,
    )
    benchmark_price = _normalize_price_frame(benchmark_frame) if benchmark_frame is not None else pd.DataFrame()
    if benchmark_symbol is None:
        benchmark_symbol = benchmark_candidates[0] if benchmark_candidates else ""

    resolved_rs_window = int(rs_window or 20)
    if resolved_rs_window <= 0:
        resolved_rs_window = 20

    flow_rows: List[pd.DataFrame] = []
    rs_rows: List[pd.DataFrame] = []
    sources: dict[str, str] = {}
    for symbol, price_frame in normalized_prices.items():
        flow_frame, flow_source = _build_flow_rows(
            market=market_key,
            symbol=symbol,
            price_frame=price_frame,
            as_of=run_date,
        )
        if not flow_frame.empty:
            flow_rows.append(flow_frame)
        sources[symbol] = flow_source

        rs_frame = _build_rs_rows(
            symbol=symbol,
            symbol_price=price_frame,
            benchmark_symbol=benchmark_symbol,
            benchmark_price=benchmark_price,
            as_of=run_date,
            window=resolved_rs_window,
        )
        if not rs_frame.empty:
            rs_rows.append(rs_frame)

    flow_output = pd.concat(flow_rows, ignore_index=True) if flow_rows else pd.DataFrame(
        columns=["date", "symbol", "delta_shares", "nav_used", "shares_outstanding", "flow_usd", "flow_pct_aum", "method", "source"]
    )
    rs_output = pd.concat(rs_rows, ignore_index=True) if rs_rows else pd.DataFrame(
        columns=["date", "symbol", "benchmark_symbol", "ret_20d_symbol", "ret_20d_benchmark", "rs_spread_20d", "rs_ratio", "rs_score", "source"]
    )

    flow_path = _flow_output_path(market_key)
    rs_path = _rs_output_path(market_key)
    write_csv_atomic(flow_output, flow_path, index=False)
    write_csv_atomic(rs_output, rs_path, index=False)

    return {
        "market": market_key,
        "as_of": run_date,
        "flow_path": flow_path,
        "rs_path": rs_path,
        "flow_rows": int(len(flow_output)),
        "rs_rows": int(len(rs_output)),
        "symbols": int(len(normalized_prices)),
        "flow_sources": sources,
        "benchmark_symbol": benchmark_symbol,
        "rs_window": resolved_rs_window,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect market breadth external CSV snapshots")
    parser.add_argument("--market", default="us", help="Target market (us|kr)")
    parser.add_argument("--as-of", default=None, help="As-of date (YYYY-MM-DD)")
    parser.add_argument("--symbols", default="", help="Comma separated ETF symbols override")
    parser.add_argument("--benchmark", default=None, help="Benchmark symbol override")
    parser.add_argument("--rs-window", type=int, default=20, help="RS lookback window")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    symbols = [item.strip() for item in str(args.symbols).split(",") if item.strip()] if args.symbols else None
    summary = collect_market_breadth_external_csv(
        market=args.market,
        as_of=args.as_of,
        symbols=symbols,
        benchmark_symbol=args.benchmark,
        rs_window=args.rs_window,
    )
    print(summary)


if __name__ == "__main__":
    main()
