from __future__ import annotations

import os
import time
from typing import Any

import pandas as pd

from utils.io_utils import write_dataframe_csv_with_fallback, write_dataframe_json_with_fallback
from utils.io_utils import ensure_dir
from utils.indicator_helpers import rolling_max, rolling_min, rolling_sma, rolling_traded_value_median
from utils.market_data_contract import (
    OhlcvFreshnessSummary,
    PricePolicy,
    SCREENING_OHLCV_READ_COLUMNS,
    describe_ohlcv_freshness,
    load_benchmark_data,
    load_local_ohlcv_frames_ordered,
    load_local_ohlcv_frame,
)
from utils.market_runtime import (
    ensure_market_dirs,
    get_benchmark_candidates,
    get_markminervini_results_dir,
    get_markminervini_with_rs_path,
    get_market_data_dir,
    get_primary_benchmark_symbol,
    is_index_symbol,
    limit_runtime_symbols,
    market_key,
)
from utils.progress_runtime import is_progress_tick, progress_interval
from utils.relative_strength import calculate_rs_score
from utils.runtime_context import runtime_context_has_explicit_as_of
from utils.typing_utils import to_float_or_none


def _build_empty_result() -> dict[str, Any]:
    return {
        **{f"cond{i}": False for i in range(1, 9)},
        "bars": 0,
        "tv_median20": None,
        "distance_to_52w_high": None,
        "met_count": 0,
    }


def calculate_trend_template(frame: pd.DataFrame) -> dict[str, Any]:
    if frame is None or frame.empty or len(frame) < 220:
        return _build_empty_result()

    df = frame.copy().sort_values("date").reset_index(drop=True)
    for column in ("open", "high", "low", "close", "volume"):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.dropna(subset=["close", "high", "low"])
    if len(df) < 220:
        return _build_empty_result()

    df["ma50"] = rolling_sma(df["close"], 50)
    df["ma150"] = rolling_sma(df["close"], 150)
    df["ma200"] = rolling_sma(df["close"], 200)
    df["high_52w"] = rolling_max(df["high"], 252, min_periods=200)
    df["low_52w"] = rolling_min(df["low"], 252, min_periods=200)
    df["tv_median20"] = rolling_traded_value_median(df, 20, min_periods=10)

    latest = df.iloc[-1]
    close = to_float_or_none(latest.get("close"))
    ma50 = to_float_or_none(latest.get("ma50"))
    ma150 = to_float_or_none(latest.get("ma150"))
    ma200 = to_float_or_none(latest.get("ma200"))
    high_52w = to_float_or_none(latest.get("high_52w"))
    low_52w = to_float_or_none(latest.get("low_52w"))
    tv_median20 = to_float_or_none(latest.get("tv_median20"))
    ma150_60d_ago = to_float_or_none(df["ma150"].iloc[-60]) if len(df) >= 210 else None
    ma200_20d_ago = to_float_or_none(df["ma200"].iloc[-20]) if len(df) >= 220 else None
    distance_to_52w_high = None
    if close is not None and high_52w is not None and high_52w != 0:
        distance_to_52w_high = 1.0 - (close / high_52w)

    result = {
        "cond1": bool(close is not None and ma150 is not None and ma200 is not None and close > ma150 > ma200),
        "cond2": bool(ma150_60d_ago is not None and ma150 is not None and ma150 > ma150_60d_ago),
        "cond3": bool(ma200_20d_ago is not None and ma200 is not None and ma200 > ma200_20d_ago),
        "cond4": bool(close is not None and ma50 is not None and close > ma50),
        "cond5": bool(close is not None and low_52w is not None and close >= low_52w * 1.25),
        "cond6": bool(close is not None and high_52w is not None and close >= high_52w * 0.75),
        "cond7": bool(ma50 is not None and ma150 is not None and ma50 > ma150),
        "cond8": False,
        "bars": int(len(df)),
        "tv_median20": tv_median20,
        "distance_to_52w_high": distance_to_52w_high,
    }
    result["met_count"] = int(sum(int(result[key]) for key in [f"cond{i}" for i in range(1, 8)]))
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


def _frame_as_of_date(frame: pd.DataFrame) -> str | None:
    if frame is None or frame.empty or "date" not in frame.columns:
        return None
    latest = pd.to_datetime(frame["date"].iloc[-1], errors="coerce")
    if pd.isna(latest):
        return None
    return latest.strftime("%Y-%m-%d")


def _resolve_rs_scores(
    market: str,
    symbols: list[str],
    *,
    benchmark_symbol: str | None = None,
    benchmark_frame: pd.DataFrame | None = None,
    as_of_date: str | None = None,
    frame_map: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.Series, str]:
    resolved_benchmark_symbol = benchmark_symbol
    resolved_benchmark_frame = benchmark_frame
    if resolved_benchmark_frame is None:
        resolved_benchmark_symbol, resolved_benchmark_frame = load_benchmark_data(
            market,
            get_benchmark_candidates(market),
            allow_yfinance_fallback=True,
            price_policy=PricePolicy.SPLIT_ADJUSTED,
        )
    benchmark_name = resolved_benchmark_symbol or get_primary_benchmark_symbol(market)
    scoped_as_of = as_of_date or _frame_as_of_date(
        resolved_benchmark_frame
        if resolved_benchmark_frame is not None
        else pd.DataFrame()
    )
    all_frames: list[pd.DataFrame] = []

    if resolved_benchmark_frame is not None and not resolved_benchmark_frame.empty:
        benchmark_payload = resolved_benchmark_frame.loc[:, ["date", "close"]].copy()
        if scoped_as_of:
            benchmark_payload["date"] = pd.to_datetime(benchmark_payload["date"], errors="coerce")
            benchmark_payload = benchmark_payload.dropna(subset=["date"])
            benchmark_payload = benchmark_payload[
                benchmark_payload["date"] <= pd.Timestamp(scoped_as_of)
            ]
            benchmark_payload["date"] = benchmark_payload["date"].dt.strftime("%Y-%m-%d")
        if benchmark_payload.empty:
            benchmark_payload = pd.DataFrame(columns=["date", "close"])
        benchmark_payload["symbol"] = benchmark_name
        all_frames.append(benchmark_payload)

    for symbol in symbols:
        frame = (
            frame_map.get(symbol, pd.DataFrame())
            if frame_map is not None
            else load_local_ohlcv_frame(
                market,
                symbol,
                as_of=scoped_as_of,
                price_policy=PricePolicy.SPLIT_ADJUSTED,
            )
        )
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


def run_market_screening(
    market: str = "us",
    *,
    as_of_date: str | None = None,
    runtime_context: Any | None = None,
) -> pd.DataFrame:
    normalized_market = market_key(market)
    ensure_market_dirs(normalized_market)
    results_dir = get_markminervini_results_dir(normalized_market)
    results_path = get_markminervini_with_rs_path(normalized_market)
    ensure_dir(results_dir)
    requested_as_of = str(as_of_date or "").strip()
    scoped_as_of = str(
        as_of_date
        or getattr(runtime_context, "as_of_date", "")
        or ""
    ).strip() or None
    explicit_replay = bool(requested_as_of) or runtime_context_has_explicit_as_of(runtime_context)
    benchmark_symbol, benchmark_frame = load_benchmark_data(
        normalized_market,
        get_benchmark_candidates(normalized_market),
        as_of=scoped_as_of,
        allow_yfinance_fallback=True,
        price_policy=PricePolicy.SPLIT_ADJUSTED,
    )
    as_of_date = scoped_as_of or _frame_as_of_date(benchmark_frame)
    if runtime_context is not None and as_of_date:
        runtime_context.set_as_of_date(as_of_date)

    symbols = [
        symbol
        for symbol in _list_symbols(normalized_market)
        if not is_index_symbol(normalized_market, symbol)
    ]
    symbols = limit_runtime_symbols(symbols)
    print(f"[MarkMinervini] Technical screening started ({normalized_market}) - symbols={len(symbols)}")
    if not symbols:
        empty = pd.DataFrame(columns=["symbol"] + [f"cond{i}" for i in range(1, 9)] + ["met_count", "rs_score"])
        write_dataframe_csv_with_fallback(
            empty,
            results_path,
            index=False,
            runtime_context=runtime_context,
            metric_label="markminervini.technical.csv",
        )
        write_dataframe_json_with_fallback(
            empty,
            results_path.replace(".csv", ".json"),
            orient="records",
            indent=2,
            force_ascii=False,
            runtime_context=runtime_context,
            metric_label="markminervini.technical.json",
        )
        return empty

    rows: list[dict[str, Any]] = []
    freshness_reports = []
    interval = progress_interval(len(symbols), target_updates=8, min_interval=50)
    frame_load_started = time.perf_counter()
    frame_map = load_local_ohlcv_frames_ordered(
        normalized_market,
        symbols,
        as_of=as_of_date,
        price_policy=PricePolicy.SPLIT_ADJUSTED,
        runtime_context=runtime_context,
        required_columns=SCREENING_OHLCV_READ_COLUMNS,
        worker_scope="markminervini.frame_load",
        load_frame_fn=load_local_ohlcv_frame,
    )
    if runtime_context is not None:
        runtime_context.add_timing(
            "markminervini.frame_load_seconds",
            time.perf_counter() - frame_load_started,
        )
    for index, symbol in enumerate(symbols, start=1):
        frame = frame_map.get(symbol, pd.DataFrame())
        freshness_reports.append(
            describe_ohlcv_freshness(
                frame,
                market=normalized_market,
                symbol=symbol,
                as_of=as_of_date,
                latest_completed_session=as_of_date,
                explicit_as_of=explicit_replay,
            )
        )
        if frame.empty or len(frame) < 220:
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
    if runtime_context is not None:
        runtime_context.update_data_freshness(
            "markminervini_technical",
            OhlcvFreshnessSummary.from_reports(freshness_reports),
        )
    if result_df.empty:
        result_df = pd.DataFrame(columns=["symbol"] + [f"cond{i}" for i in range(1, 9)] + ["met_count", "rs_score"])
        write_dataframe_csv_with_fallback(
            result_df,
            results_path,
            index=False,
            runtime_context=runtime_context,
            metric_label="markminervini.technical.csv",
        )
        write_dataframe_json_with_fallback(
            result_df,
            results_path.replace(".csv", ".json"),
            orient="records",
            indent=2,
            force_ascii=False,
            runtime_context=runtime_context,
            metric_label="markminervini.technical.json",
        )
        return result_df

    print(f"[MarkMinervini] RS calculation started ({normalized_market}) - candidates={len(result_df)}")
    rs_scores, benchmark_symbol = _resolve_rs_scores(
        normalized_market,
        symbols,
        benchmark_symbol=benchmark_symbol,
        benchmark_frame=benchmark_frame,
        as_of_date=as_of_date,
        frame_map=frame_map,
    )
    rs_dict = {str(index).upper(): float(value) for index, value in rs_scores.items()}
    result_df["rs_score"] = result_df["symbol"].map(rs_dict).fillna(0.0)
    result_df["rs_benchmark"] = benchmark_symbol
    result_df["cond8"] = result_df["rs_score"] >= 70.0
    result_df["recommended_rs_pass"] = result_df["cond8"]
    distance_to_high_series = (
        pd.to_numeric(result_df["distance_to_52w_high"], errors="coerce")
        if "distance_to_52w_high" in result_df.columns
        else pd.Series(1.0, index=result_df.index, dtype=float)
    )
    result_df["recommended_distance_to_high_pass"] = distance_to_high_series.fillna(1.0) <= 0.15

    mandatory_condition_cols = [f"cond{i}" for i in range(1, 8)]
    result_df["met_count"] = result_df[mandatory_condition_cols].astype(int).sum(axis=1)

    filtered_df = result_df[result_df[mandatory_condition_cols].all(axis=1)].copy()
    filtered_df = filtered_df.sort_values(by="rs_score", ascending=False).reset_index(drop=True)

    write_dataframe_csv_with_fallback(
        filtered_df,
        results_path,
        index=False,
        runtime_context=runtime_context,
        metric_label="markminervini.technical.csv",
    )
    write_dataframe_json_with_fallback(
        filtered_df,
        results_path.replace(".csv", ".json"),
        orient="records",
        indent=2,
        force_ascii=False,
        runtime_context=runtime_context,
        metric_label="markminervini.technical.json",
    )
    print(
        f"[MarkMinervini] Technical screening saved ({normalized_market}) - "
        f"qualified={len(filtered_df)}, benchmark={benchmark_symbol}"
    )
    return filtered_df

# Compatibility shim for older direct callers; runtime entrypoints use
# `run_market_screening()` directly.
def run_us_screening() -> pd.DataFrame:
    return run_market_screening("us")


# Compatibility shim for older direct callers; runtime entrypoints use
# `run_market_screening()` directly.
def run_screening(market: str = "us") -> pd.DataFrame:
    return run_market_screening(market)


if __name__ == "__main__":
    run_market_screening("us")
