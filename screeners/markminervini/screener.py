from __future__ import annotations

import os
from typing import Any

import pandas as pd

from utils import ensure_dir
from utils.market_data_contract import load_benchmark_data, load_local_ohlcv_frame
from utils.market_runtime import (
    ensure_market_dirs,
    get_benchmark_candidates,
    get_markminervini_results_dir,
    get_markminervini_with_rs_path,
    get_market_data_dir,
    get_primary_benchmark_symbol,
    is_index_symbol,
    market_key,
)
from utils.progress_runtime import is_progress_tick, progress_interval
from utils.relative_strength import calculate_rs_score


def _build_empty_result() -> dict[str, Any]:
    return {**{f"cond{i}": False for i in range(1, 8)}, "met_count": 0}


def calculate_trend_template(frame: pd.DataFrame) -> dict[str, Any]:
    if frame is None or frame.empty or len(frame) < 200:
        return _build_empty_result()

    df = frame.copy().sort_values("date").reset_index(drop=True)
    for column in ("open", "high", "low", "close", "volume"):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.dropna(subset=["close", "high", "low"])
    if len(df) < 200:
        return _build_empty_result()

    df["ma50"] = df["close"].rolling(window=50).mean()
    df["ma150"] = df["close"].rolling(window=150).mean()
    df["ma200"] = df["close"].rolling(window=200).mean()
    df["high_52w"] = df["high"].rolling(window=252, min_periods=200).max()
    df["low_52w"] = df["low"].rolling(window=252, min_periods=200).min()

    latest = df.iloc[-1]
    ma150_60d_ago = df["ma150"].iloc[-60] if len(df) >= 210 else pd.NA
    ma200_20d_ago = df["ma200"].iloc[-20] if len(df) >= 220 else pd.NA

    result = {
        "cond1": bool(pd.notna(latest["ma150"]) and pd.notna(latest["ma200"]) and latest["close"] > latest["ma150"] > latest["ma200"]),
        "cond2": bool(pd.notna(ma150_60d_ago) and pd.notna(latest["ma150"]) and latest["ma150"] > ma150_60d_ago),
        "cond3": bool(pd.notna(ma200_20d_ago) and pd.notna(latest["ma200"]) and latest["ma200"] > ma200_20d_ago),
        "cond4": bool(pd.notna(latest["ma50"]) and latest["close"] > latest["ma50"]),
        "cond5": bool(pd.notna(latest["low_52w"]) and latest["close"] >= latest["low_52w"] * 1.3),
        "cond6": bool(pd.notna(latest["high_52w"]) and latest["close"] >= latest["high_52w"] * 0.75),
        "cond7": bool(pd.notna(latest["ma50"]) and pd.notna(latest["ma150"]) and latest["ma50"] > latest["ma150"]),
    }
    result["met_count"] = int(sum(int(result[key]) for key in result if key.startswith("cond")))
    return result


def _list_symbols(market: str) -> list[str]:
    data_dir = get_market_data_dir(market)
    if not os.path.isdir(data_dir):
        return []

    symbols: list[str] = []
    for name in os.listdir(data_dir):
        if not name.endswith(".csv"):
            continue
        symbol = os.path.splitext(name)[0].strip().upper()
        if symbol:
            symbols.append(symbol)
    return sorted(set(symbols))


def _resolve_rs_scores(market: str, symbols: list[str]) -> tuple[pd.Series, str]:
    benchmark_symbol, benchmark_frame = load_benchmark_data(
        market,
        get_benchmark_candidates(market),
        allow_yfinance_fallback=True,
    )
    benchmark_name = benchmark_symbol or get_primary_benchmark_symbol(market)
    all_frames: list[pd.DataFrame] = []

    if benchmark_frame is not None and not benchmark_frame.empty:
        benchmark_payload = benchmark_frame.loc[:, ["date", "symbol", "close"]].copy()
        benchmark_payload["symbol"] = benchmark_name
        all_frames.append(benchmark_payload)

    for symbol in symbols:
        frame = load_local_ohlcv_frame(market, symbol)
        if frame.empty or len(frame) < 126:
            continue
        payload = frame.loc[:, ["date", "symbol", "close"]].copy()
        payload["symbol"] = symbol
        all_frames.append(payload)

    if len(all_frames) < 2:
        return pd.Series(dtype=float), benchmark_name

    combined = pd.concat(all_frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["date", "symbol"], keep="last")
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce", utc=True)
    combined = combined.dropna(subset=["date", "close"])
    if combined.empty:
        return pd.Series(dtype=float), benchmark_name

    combined = combined.set_index(["date", "symbol"]).sort_index()
    rs_scores = calculate_rs_score(
        combined,
        price_col="close",
        use_enhanced=True,
        benchmark_symbol=benchmark_name,
    )
    return rs_scores, benchmark_name


def run_market_screening(market: str = "us") -> pd.DataFrame:
    normalized_market = market_key(market)
    ensure_market_dirs(normalized_market)
    results_dir = get_markminervini_results_dir(normalized_market)
    results_path = get_markminervini_with_rs_path(normalized_market)
    ensure_dir(results_dir)

    symbols = [
        symbol
        for symbol in _list_symbols(normalized_market)
        if not is_index_symbol(normalized_market, symbol)
    ]
    print(f"[MarkMinervini] Technical screening started ({normalized_market}) - symbols={len(symbols)}")
    if not symbols:
        empty = pd.DataFrame(columns=["symbol"] + [f"cond{i}" for i in range(1, 9)] + ["met_count", "rs_score"])
        empty.to_csv(results_path, index=False)
        empty.to_json(results_path.replace(".csv", ".json"), orient="records", indent=2, force_ascii=False)
        return empty

    rows: list[dict[str, Any]] = []
    interval = progress_interval(len(symbols), target_updates=8, min_interval=50)
    for index, symbol in enumerate(symbols, start=1):
        frame = load_local_ohlcv_frame(normalized_market, symbol)
        if frame.empty or len(frame) < 200:
            if is_progress_tick(index, len(symbols), interval):
                print(
                    f"[MarkMinervini] Technical progress ({normalized_market}) - "
                    f"processed={index}/{len(symbols)}, eligible={len(rows)}"
                )
            continue
        trend = calculate_trend_template(frame)
        latest = frame.iloc[-1]
        rows.append(
            {
                "symbol": symbol,
                **trend,
                "close": float(pd.to_numeric(latest["close"], errors="coerce")),
                "volume": float(pd.to_numeric(latest["volume"], errors="coerce")),
                "date": str(latest["date"]),
            }
        )
        if is_progress_tick(index, len(symbols), interval):
            print(
                f"[MarkMinervini] Technical progress ({normalized_market}) - "
                f"processed={index}/{len(symbols)}, eligible={len(rows)}"
            )

    result_df = pd.DataFrame(rows)
    if result_df.empty:
        result_df = pd.DataFrame(columns=["symbol"] + [f"cond{i}" for i in range(1, 9)] + ["met_count", "rs_score"])
        result_df.to_csv(results_path, index=False)
        result_df.to_json(results_path.replace(".csv", ".json"), orient="records", indent=2, force_ascii=False)
        return result_df

    print(f"[MarkMinervini] RS calculation started ({normalized_market}) - candidates={len(result_df)}")
    rs_scores, benchmark_symbol = _resolve_rs_scores(normalized_market, symbols)
    rs_dict = {str(index).upper(): float(value) for index, value in rs_scores.items()}
    result_df["rs_score"] = result_df["symbol"].map(rs_dict).fillna(0.0)
    result_df["rs_benchmark"] = benchmark_symbol
    result_df["cond8"] = result_df["rs_score"] >= 85.0

    condition_cols = [f"cond{i}" for i in range(1, 9)]
    result_df["met_count"] = result_df[condition_cols].astype(int).sum(axis=1)

    filtered_df = result_df[result_df[condition_cols].all(axis=1)].copy()
    filtered_df = filtered_df.sort_values(by="rs_score", ascending=False).reset_index(drop=True)

    filtered_df.to_csv(results_path, index=False)
    filtered_df.to_json(results_path.replace(".csv", ".json"), orient="records", indent=2, force_ascii=False)
    print(
        f"[MarkMinervini] Technical screening saved ({normalized_market}) - "
        f"qualified={len(filtered_df)}, benchmark={benchmark_symbol}"
    )
    return filtered_df


def run_us_screening() -> pd.DataFrame:
    return run_market_screening("us")


def run_screening(market: str = "us") -> pd.DataFrame:
    return run_market_screening(market)


if __name__ == "__main__":
    run_market_screening("us")
