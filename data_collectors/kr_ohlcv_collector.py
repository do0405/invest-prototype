"""KR OHLCV collector using pykrx and CSV artifacts."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

from config import DATA_KR_DIR
from utils.market_data_contract import (
    CANONICAL_OHLCV_COLUMNS,
    LEGACY_MOJIBAKE_ALIASES,
    OHLCV_COLUMN_ALIASES,
    normalize_ohlcv_columns,
)
from utils import ensure_dir

CANONICAL_KR_OHLCV_COLUMNS = CANONICAL_OHLCV_COLUMNS
KR_OHLCV_COLUMN_ALIASES = {
    alias: canonical
    for canonical, aliases in OHLCV_COLUMN_ALIASES.items()
    for alias in aliases
}
for canonical, aliases in LEGACY_MOJIBAKE_ALIASES.items():
    for alias in aliases:
        KR_OHLCV_COLUMN_ALIASES[alias] = canonical

def _normalize_kr_ohlcv_frame(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    frame = df.copy()
    frame = frame.rename(columns=KR_OHLCV_COLUMN_ALIASES)
    frame = normalize_ohlcv_columns(frame)
    if "date" not in frame.columns:
        frame = frame.reset_index()
        if len(frame.columns) > 0:
            frame = frame.rename(columns={frame.columns[0]: "date"})
        frame = normalize_ohlcv_columns(frame)
    if "date" not in frame.columns:
        return pd.DataFrame(columns=list(CANONICAL_KR_OHLCV_COLUMNS))
    frame = frame.loc[:, ~frame.columns.duplicated()]

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    frame["symbol"] = ticker

    for col in ("open", "high", "low", "close", "volume"):
        if col not in frame.columns:
            frame[col] = 0

    frame = frame[list(CANONICAL_KR_OHLCV_COLUMNS)]
    frame = frame.dropna(subset=["date", "close"]).copy()

    for col in ("open", "high", "low", "close", "volume"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")

    frame = frame.dropna(subset=["close"])
    frame = frame.sort_values("date").reset_index(drop=True)
    return frame


def _resolve_business_day_yyyymmdd(stock_module, day: datetime) -> str:
    day_str = day.strftime("%Y%m%d")
    try:
        return stock_module.get_nearest_business_day_in_a_week(day_str)
    except Exception:
        return day_str


def _collect_ticker_list(
    stock_module,
    as_of_yyyymmdd: str,
    include_kosdaq: bool,
    include_etf: bool,
    include_etn: bool,
) -> List[str]:
    tickers: List[str] = []
    markets = ["KOSPI"]
    if include_kosdaq:
        markets.append("KOSDAQ")
    if include_etf:
        markets.append("ETF")
    if include_etn:
        markets.append("ETN")

    for market in markets:
        try:
            tickers.extend(stock_module.get_market_ticker_list(as_of_yyyymmdd, market=market))
        except Exception:
            continue

    return sorted(set(tickers))


def collect_kr_ohlcv_csv(
    days: int = 450,
    include_kosdaq: bool = True,
    include_etf: bool = True,
    include_etn: bool = False,
    *,
    stock_module=None,
    output_dir: Optional[str] = None,
    tickers: Optional[List[str]] = None,
    as_of: Optional[datetime] = None,
    max_failed_samples: int = 20,
) -> Dict[str, object]:
    """Collect KR OHLCV for all tickers and save canonical CSV files.

    Args:
        days: History window from resolved business day.
        include_kosdaq: Include KOSDAQ universe in addition to KOSPI.
        include_etf: Include ETF universe (including leveraged/inverse ETFs).
        include_etn: Include ETN universe.
        stock_module: Optional injected module compatible with ``pykrx.stock`` API.
        output_dir: Optional destination directory (defaults to ``DATA_KR_DIR``).
        tickers: Optional explicit ticker universe override.
        as_of: Optional 기준 시점 for reproducible runs.
        max_failed_samples: Max failed ticker records to include in summary.
    """
    stock_client = stock_module
    if stock_client is None:
        from pykrx import stock as stock_client

    target_dir = output_dir or DATA_KR_DIR
    ensure_dir(target_dir)

    end_dt = as_of or datetime.now()
    end_yyyymmdd = _resolve_business_day_yyyymmdd(stock_client, end_dt)
    start_dt = datetime.strptime(end_yyyymmdd, "%Y%m%d") - timedelta(days=days)
    start_yyyymmdd = start_dt.strftime("%Y%m%d")

    universe = sorted(
        set(
            tickers
            or _collect_ticker_list(
                stock_client,
                end_yyyymmdd,
                include_kosdaq=include_kosdaq,
                include_etf=include_etf,
                include_etn=include_etn,
            )
        )
    )
    saved = 0
    failed = 0
    skipped_empty = 0
    failed_samples: List[Dict[str, str]] = []

    for ticker in universe:
        try:
            raw = stock_client.get_market_ohlcv_by_date(start_yyyymmdd, end_yyyymmdd, ticker)
            normalized = _normalize_kr_ohlcv_frame(raw, ticker=ticker)
            if normalized.empty:
                skipped_empty += 1
                continue
            out_path = os.path.join(target_dir, f"{ticker}.csv")
            normalized.to_csv(out_path, index=False)
            saved += 1
        except Exception as exc:
            failed += 1
            if len(failed_samples) < max_failed_samples:
                failed_samples.append({"ticker": ticker, "error": str(exc)})

    return {
        "schema_version": "1.0",
        "source": "pykrx",
        "market": "kr",
        "include_kosdaq": bool(include_kosdaq),
        "include_etf": bool(include_etf),
        "include_etn": bool(include_etn),
        "as_of": end_yyyymmdd,
        "from": start_yyyymmdd,
        "to": end_yyyymmdd,
        "total": len(universe),
        "saved": saved,
        "failed": failed,
        "skipped_empty": skipped_empty,
        "failed_samples": failed_samples,
        "data_dir": target_dir,
    }


if __name__ == "__main__":
    summary = collect_kr_ohlcv_csv()
    print(summary)
    
    
