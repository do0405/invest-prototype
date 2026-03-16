from __future__ import annotations

from typing import Any

import pandas as pd

from .market_data_contract import PricePolicy, normalize_ohlcv_frame, resolve_price_policy
from .typing_utils import to_float_or_none


def normalize_indicator_frame(
    frame: pd.DataFrame,
    *,
    symbol: str = "",
    price_policy: PricePolicy | str = PricePolicy.SPLIT_ADJUSTED,
    utc_dates: bool = False,
) -> pd.DataFrame:
    normalized = normalize_ohlcv_frame(
        frame,
        symbol=str(symbol or "").strip().upper(),
        price_policy=resolve_price_policy(price_policy),
    )
    if normalized.empty:
        return normalized

    daily = normalized.copy()
    daily["date"] = pd.to_datetime(daily["date"], errors="coerce", utc=utc_dates)
    for column in ("open", "high", "low", "close", "raw_open", "raw_high", "raw_low", "raw_close", "adj_close", "volume"):
        if column in daily.columns:
            daily[column] = pd.to_numeric(daily[column], errors="coerce")
    daily = daily.dropna(subset=["date", "open", "high", "low", "close"]).copy()
    if daily.empty:
        return daily
    return daily.sort_values("date").reset_index(drop=True)


def rolling_sma(series: pd.Series, window: int, *, min_periods: int | None = None) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").rolling(window, min_periods=min_periods or window).mean()


def rolling_median(series: pd.Series, window: int, *, min_periods: int | None = None) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").rolling(window, min_periods=min_periods or window).median()


def rolling_max(series: pd.Series, window: int, *, min_periods: int | None = None) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").rolling(window, min_periods=min_periods or window).max()


def rolling_min(series: pd.Series, window: int, *, min_periods: int | None = None) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").rolling(window, min_periods=min_periods or window).min()


def rolling_ema(series: pd.Series, span: int, *, adjust: bool = False) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").ewm(span=span, adjust=adjust).mean()


def ema_last(series: pd.Series, span: int) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if len(numeric) < span:
        return None
    value = rolling_ema(numeric, span, adjust=False).iloc[-1]
    return to_float_or_none(value)


def true_range(frame: pd.DataFrame, *, close_col: str = "close") -> pd.Series:
    prev_close = pd.to_numeric(frame[close_col], errors="coerce").shift(1)
    tr = pd.concat(
        [
            pd.to_numeric(frame["high"], errors="coerce") - pd.to_numeric(frame["low"], errors="coerce"),
            (pd.to_numeric(frame["high"], errors="coerce") - prev_close).abs(),
            (pd.to_numeric(frame["low"], errors="coerce") - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return pd.to_numeric(tr, errors="coerce")


def rolling_atr(
    frame: pd.DataFrame,
    window: int,
    *,
    close_col: str = "close",
    min_periods: int | None = None,
) -> pd.Series:
    return true_range(frame, close_col=close_col).rolling(window, min_periods=min_periods or window).mean()


def traded_value_series(frame: pd.DataFrame, *, close_col: str = "close") -> pd.Series:
    return pd.to_numeric(frame[close_col], errors="coerce") * pd.to_numeric(frame["volume"], errors="coerce")


def rolling_average_volume(frame: pd.DataFrame, window: int, *, min_periods: int | None = None) -> pd.Series:
    return pd.to_numeric(frame["volume"], errors="coerce").rolling(window, min_periods=min_periods or window).mean()


def rolling_traded_value(frame: pd.DataFrame, window: int, *, close_col: str = "close", min_periods: int | None = None) -> pd.Series:
    return traded_value_series(frame, close_col=close_col).rolling(window, min_periods=min_periods or window).mean()


def rolling_traded_value_median(
    frame: pd.DataFrame,
    window: int,
    *,
    close_col: str = "close",
    min_periods: int | None = None,
) -> pd.Series:
    return traded_value_series(frame, close_col=close_col).rolling(window, min_periods=min_periods or window).median()


def atr_percent(frame: pd.DataFrame, *, length: int = 14, close_col: str = "close") -> float | None:
    if len(frame) < length + 1:
        return None
    atr = to_float_or_none(rolling_atr(frame, length, close_col=close_col, min_periods=length).iloc[-1])
    close = to_float_or_none(pd.to_numeric(frame[close_col], errors="coerce").iloc[-1])
    if atr is None or close is None or close == 0:
        return None
    return float((atr / close) * 100.0)


def adr_percent(frame: pd.DataFrame, *, length: int = 20, close_col: str = "close") -> float | None:
    if len(frame) < length:
        return None
    close = pd.to_numeric(frame[close_col], errors="coerce").replace({0: pd.NA})
    adr = ((pd.to_numeric(frame["high"], errors="coerce") - pd.to_numeric(frame["low"], errors="coerce")) / close * 100.0).rolling(
        window=length,
        min_periods=length,
    ).mean().iloc[-1]
    return to_float_or_none(adr)


def average_traded_value(frame: pd.DataFrame, window: int, *, close_col: str = "close") -> float | None:
    if len(frame) < window:
        return None
    typical_price = (
        pd.to_numeric(frame["high"], errors="coerce")
        + pd.to_numeric(frame["low"], errors="coerce")
        + pd.to_numeric(frame[close_col], errors="coerce")
    ) / 3.0
    traded_value = typical_price * pd.to_numeric(frame["volume"], errors="coerce")
    value = traded_value.rolling(window=window, min_periods=window).mean().iloc[-1]
    return to_float_or_none(value)


def relative_volume(frame: pd.DataFrame, *, window: int = 20) -> float | None:
    if len(frame) < window:
        return None
    average_volume = to_float_or_none(
        pd.to_numeric(frame["volume"], errors="coerce").rolling(window=window, min_periods=window).mean().iloc[-1]
    )
    latest_volume = to_float_or_none(pd.to_numeric(frame["volume"], errors="coerce").iloc[-1])
    if average_volume is None or average_volume == 0 or latest_volume is None:
        return None
    return float(latest_volume / average_volume)


def pct_change_over_period(frame: pd.DataFrame, periods: int, *, close_col: str = "close") -> float | None:
    if len(frame) <= periods:
        return None
    series = pd.to_numeric(frame[close_col], errors="coerce")
    start = to_float_or_none(series.iloc[-periods - 1])
    end = to_float_or_none(series.iloc[-1])
    if start is None or end is None or start == 0:
        return None
    return float((end / start - 1.0) * 100.0)
