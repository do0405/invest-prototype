from __future__ import annotations

from typing import Any, Mapping

import numpy as np
import pandas as pd


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(number):
        return None
    return number


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame:
        return pd.Series(dtype=float)
    return pd.to_numeric(frame[column], errors="coerce")


def _true_range(frame: pd.DataFrame) -> pd.Series:
    high = _numeric(frame, "high")
    low = _numeric(frame, "low")
    close = _numeric(frame, "close")
    prev_close = close.shift(1)
    return pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)


def _close_position_pct(
    *,
    high: float | None,
    low: float | None,
    close: float | None,
) -> float | None:
    if high is None or low is None or close is None or high <= low:
        return None
    return (close - low) / (high - low)


def bullish_rvol50(frame: pd.DataFrame) -> float | None:
    if len(frame) < 20:
        return None
    volume = _numeric(frame, "volume")
    close = _numeric(frame, "close")
    open_ = _numeric(frame, "open")
    high = _numeric(frame, "high")
    low = _numeric(frame, "low")
    latest_volume = _safe_float(volume.iloc[-1])
    if latest_volume is None:
        return None
    if len(volume.dropna()) >= 51:
        average_window = volume.iloc[-51:-1]
    else:
        average_window = volume.iloc[:-1].tail(50)
    average_volume = _safe_float(average_window.mean())
    if average_volume is None or average_volume <= 0:
        return None
    latest_close = _safe_float(close.iloc[-1])
    latest_open = _safe_float(open_.iloc[-1])
    close_position = _close_position_pct(
        high=_safe_float(high.iloc[-1]),
        low=_safe_float(low.iloc[-1]),
        close=latest_close,
    )
    if latest_close is None or latest_open is None:
        return None
    if latest_close <= latest_open or (close_position or 0.0) < 0.55:
        return 0.0
    return latest_volume / average_volume


def detect_dry_volume(frame: pd.DataFrame) -> bool:
    volume = _numeric(frame, "volume")
    if len(volume.dropna()) < 55:
        return False
    recent_5 = _safe_float(volume.iloc[-5:].mean())
    previous_5 = _safe_float(volume.iloc[-10:-5].mean())
    average_50 = _safe_float(volume.iloc[-50:].mean())
    if (
        recent_5 is None
        or previous_5 is None
        or average_50 is None
        or average_50 <= 0
    ):
        return False
    return recent_5 <= (average_50 * 0.60) and recent_5 < previous_5


def detect_tight_range(frame: pd.DataFrame) -> bool:
    if len(frame) < 70:
        return False
    high = _numeric(frame, "high")
    low = _numeric(frame, "low")
    true_range = _true_range(frame)
    atr10 = true_range.rolling(10, min_periods=10).mean()
    atr14 = true_range.rolling(14, min_periods=14).mean()
    latest_atr10 = _safe_float(atr10.iloc[-1])
    latest_atr14 = _safe_float(atr14.iloc[-1])
    if latest_atr10 is None or latest_atr14 is None or latest_atr14 <= 0:
        return False
    atr_window = atr10.dropna().tail(60)
    if len(atr_window) < 30:
        return False
    percentile_rank = float((atr_window <= latest_atr10).mean())
    recent_range = _safe_float(high.iloc[-10:].max() - low.iloc[-10:].min())
    if recent_range is None:
        return False
    compressed_bars = 0
    for offset in range(5, 0, -1):
        end = None if offset == 1 else -offset + 1
        high_window = high.iloc[-offset - 9 : end]
        low_window = low.iloc[-offset - 9 : end]
        atr_value = _safe_float(atr14.iloc[-offset])
        range_value = _safe_float(high_window.max() - low_window.min())
        if (
            atr_value is not None
            and range_value is not None
            and range_value <= atr_value * 1.5
        ):
            compressed_bars += 1
    return (
        percentile_rank <= 0.25
        and recent_range <= latest_atr14 * 1.5
        and compressed_bars >= 5
    )


def detect_w_pattern(frame: pd.DataFrame) -> dict[str, Any]:
    if len(frame) < 40:
        return {"w_pending": False, "w_confirmed": False, "w_neckline_level": None}
    lows = _numeric(frame, "low").iloc[-80:]
    highs = _numeric(frame, "high").iloc[-80:]
    close = _numeric(frame, "close").iloc[-80:]
    if len(lows.dropna()) < 30:
        return {"w_pending": False, "w_confirmed": False, "w_neckline_level": None}
    split = len(lows) // 2
    left = lows.iloc[:split]
    right = lows.iloc[split:]
    left_idx = left.idxmin()
    right_idx = right.idxmin()
    left_low = _safe_float(lows.loc[left_idx])
    right_low = _safe_float(lows.loc[right_idx])
    if left_low is None or right_low is None or left_low <= 0 or right_low <= 0:
        return {"w_pending": False, "w_confirmed": False, "w_neckline_level": None}
    low_similarity = abs(left_low - right_low) / left_low
    if low_similarity > 0.08:
        return {"w_pending": False, "w_confirmed": False, "w_neckline_level": None}
    between = highs.loc[left_idx:right_idx]
    neckline = _safe_float(between.max())
    latest_close = _safe_float(close.iloc[-1])
    if neckline is None or latest_close is None:
        return {"w_pending": False, "w_confirmed": False, "w_neckline_level": None}
    floor = min(left_low, right_low)
    pending = neckline >= floor * 1.06
    confirmed = pending and latest_close >= neckline * 1.005
    return {
        "w_pending": pending,
        "w_confirmed": confirmed,
        "w_neckline_level": neckline if pending else None,
    }


def detect_vcp_features(
    frame: pd.DataFrame,
    *,
    volume_dry: bool,
    tight_active: bool,
    bullish_rvol50_value: float | None,
    close_position_pct: float | None,
    risk_heat: bool,
) -> dict[str, Any]:
    if len(frame) < 80:
        return {
            "vcp_setup_active": False,
            "vcp_pivot_level": None,
            "vcp_pivot_breakout": False,
            "vcp_contraction_count": 0,
        }
    high = _numeric(frame, "high")
    low = _numeric(frame, "low")
    close = _numeric(frame, "close")
    open_ = _numeric(frame, "open")
    lookback_high = high.iloc[-60:]
    lookback_low = low.iloc[-60:]
    segment_count = 4
    segment_size = len(lookback_low) // segment_count
    depths: list[float] = []
    lows: list[float] = []
    for index in range(segment_count):
        start = index * segment_size
        end = (index + 1) * segment_size if index < segment_count - 1 else len(lookback_low)
        seg_high = _safe_float(lookback_high.iloc[start:end].max())
        seg_low = _safe_float(lookback_low.iloc[start:end].min())
        if seg_high is None or seg_low is None or seg_high <= 0:
            continue
        depths.append(((seg_high - seg_low) / seg_high) * 100.0)
        lows.append(seg_low)
    contraction_count = len(depths)
    if contraction_count < 2:
        setup_active = False
    else:
        higher_lows = all(
            lows[i] >= lows[i - 1] * 0.985 for i in range(1, len(lows))
        )
        shrinking_depth = all(
            depths[i] <= depths[i - 1] * 1.05 for i in range(1, len(depths))
        )
        final_depth = depths[-1]
        first_depth = depths[0]
        prior_close = _safe_float(close.iloc[-80])
        latest_close = _safe_float(close.iloc[-1])
        prior_uptrend = (
            prior_close is not None
            and latest_close is not None
            and prior_close > 0
            and latest_close >= prior_close * 1.20
        )
        setup_active = bool(
            prior_uptrend
            and 2 <= contraction_count <= 5
            and higher_lows
            and shrinking_depth
            and final_depth <= min(8.0, first_depth * 0.5)
            and volume_dry
            and tight_active
        )
    pivot_window = high.iloc[-20:-1]
    pivot_level = _safe_float(pivot_window.max()) if not pivot_window.empty else None
    latest_close = _safe_float(close.iloc[-1])
    latest_open = _safe_float(open_.iloc[-1])
    pivot_breakout = bool(
        setup_active
        and pivot_level is not None
        and latest_close is not None
        and latest_open is not None
        and latest_close > pivot_level
        and latest_close > latest_open
        and (close_position_pct or 0.0) >= 0.65
        and (bullish_rvol50_value or 0.0) >= 1.5
        and not risk_heat
    )
    return {
        "vcp_setup_active": setup_active,
        "vcp_pivot_level": pivot_level if setup_active else None,
        "vcp_pivot_breakout": pivot_breakout,
        "vcp_contraction_count": contraction_count if setup_active else 0,
    }


def detect_pocket_pivot(frame: pd.DataFrame, lookback: int = 10) -> dict[str, Any]:
    default = {
        "pocket_pivot": False,
        "pocket_pivot_down_volume_max": None,
        "pocket_pivot_score": 0.0,
        "reason_codes": [],
    }
    if len(frame) < lookback + 1:
        return default
    close = _numeric(frame, "close")
    open_ = _numeric(frame, "open")
    high = _numeric(frame, "high")
    low = _numeric(frame, "low")
    volume = _numeric(frame, "volume")
    latest_close = _safe_float(close.iloc[-1])
    latest_open = _safe_float(open_.iloc[-1])
    latest_volume = _safe_float(volume.iloc[-1])
    close_position = _close_position_pct(
        high=_safe_float(high.iloc[-1]),
        low=_safe_float(low.iloc[-1]),
        close=latest_close,
    )
    if latest_close is None or latest_open is None or latest_volume is None:
        return default

    prior = frame.iloc[-lookback - 1 : -1].copy()
    prior_close = _numeric(prior, "close")
    prior_open = _numeric(prior, "open")
    prior_volume = _numeric(prior, "volume")
    down_volume = prior_volume[prior_close < prior_open].dropna()
    if down_volume.empty:
        return {
            **default,
            "reason_codes": ["POCKET_PIVOT_NO_DOWN_VOLUME_REFERENCE"],
        }
    down_volume_max = _safe_float(down_volume.max())
    bullish_bar = latest_close > latest_open and (close_position or 0.0) >= 0.60
    volume_pass = bool(down_volume_max is not None and latest_volume > down_volume_max)
    if not bullish_bar:
        return {
            **default,
            "pocket_pivot_down_volume_max": down_volume_max,
            "reason_codes": ["POCKET_PIVOT_CLOSE_FAIL"],
        }
    if not volume_pass:
        return {
            **default,
            "pocket_pivot_down_volume_max": down_volume_max,
            "reason_codes": ["POCKET_PIVOT_VOLUME_FAIL"],
        }
    return {
        "pocket_pivot": True,
        "pocket_pivot_down_volume_max": down_volume_max,
        "pocket_pivot_score": 100.0,
        "reason_codes": ["POCKET_PIVOT_DOWN_VOLUME_MAX"],
    }


def _metric_value(metrics: Mapping[str, Any], name: str) -> float | None:
    return _safe_float(metrics.get(name))


def _latest_ohlc(frame: pd.DataFrame, metrics: Mapping[str, Any], name: str) -> float | None:
    seeded = _metric_value(metrics, name)
    if seeded is not None:
        return seeded
    series = _numeric(frame, name)
    if series.empty:
        return None
    return _safe_float(series.iloc[-1])


def _band_values(
    frame: pd.DataFrame, metrics: Mapping[str, Any]
) -> tuple[float | None, float | None, float | None]:
    lower = _metric_value(metrics, "bb_lower")
    mid = _metric_value(metrics, "bb_mid")
    upper = _metric_value(metrics, "bb_upper")
    if lower is not None or mid is not None or upper is not None:
        return lower, mid, upper
    close = _numeric(frame, "close")
    if len(close.dropna()) < 20:
        return None, None, None
    mid_series = close.rolling(20, min_periods=20).mean()
    std_series = close.rolling(20, min_periods=20).std()
    mid = _safe_float(mid_series.iloc[-1])
    std = _safe_float(std_series.iloc[-1])
    if mid is None or std is None:
        return None, None, None
    return mid - (std * 2.0), mid, mid + (std * 2.0)


def _band_position(
    *,
    close: float | None,
    lower: float | None,
    mid: float | None,
    upper: float | None,
    metrics: Mapping[str, Any],
) -> tuple[float | None, float | None]:
    percent_b = _metric_value(metrics, "bb_percent_b")
    z_score = _metric_value(metrics, "bb_z_score")
    if close is None:
        return percent_b, z_score
    if percent_b is None and lower is not None and upper is not None and upper > lower:
        percent_b = (close - lower) / (upper - lower)
    if z_score is None and mid is not None and upper is not None and upper > mid:
        std_proxy = (upper - mid) / 2.0
        if std_proxy > 0:
            z_score = (close - mid) / std_proxy
    return percent_b, z_score


def score_band_reversion(
    frame: pd.DataFrame,
    metrics_seed: Mapping[str, Any],
) -> dict[str, Any]:
    metrics = dict(metrics_seed)
    close = _latest_ohlc(frame, metrics, "close")
    open_ = _latest_ohlc(frame, metrics, "open")
    high = _latest_ohlc(frame, metrics, "high")
    low = _latest_ohlc(frame, metrics, "low")
    lower, mid, upper = _band_values(frame, metrics)
    percent_b, z_score = _band_position(
        close=close, lower=lower, mid=mid, upper=upper, metrics=metrics
    )
    rsi = _metric_value(metrics, "rsi14")
    daily_return = _metric_value(metrics, "daily_return_pct")
    close_position = _metric_value(metrics, "close_position_pct")
    if close_position is None:
        close_position = _close_position_pct(high=high, low=low, close=close)

    bullish_close = (close_position or 0.0) >= 0.55
    weak_close = (close_position if close_position is not None else 1.0) <= 0.45
    green_bar = bool(close is not None and open_ is not None and close > open_)
    red_bar = bool(close is not None and open_ is not None and close < open_)
    positive_return = (daily_return or 0.0) > 0.0
    negative_return = (daily_return or 0.0) < 0.0
    risk_heat = bool(metrics.get("risk_heat"))
    above_200ma = bool(metrics.get("above_200ma"))
    bullish_context = (
        _safe_text(metrics.get("alignment_state")).upper() == "BULLISH"
        and bool(metrics.get("support_trend_rising"))
        and not bool(metrics.get("ema_turn_down"))
    )
    pullback_profile_pass = bool(metrics.get("pullback_profile_pass"))

    lower_touch = bool(
        lower is not None
        and low is not None
        and low <= lower * 1.01
    )
    lower_reclaim = bool(
        lower_touch
        and close is not None
        and lower is not None
        and close >= lower
        and bullish_close
    )
    mid_support = bool(
        mid is not None
        and low is not None
        and close is not None
        and low <= mid
        and close >= mid
        and bullish_close
    )
    pbb_overheat = bool(
        (z_score is not None and z_score > 1.0)
        or (percent_b is not None and percent_b > 0.80)
        or (rsi is not None and rsi >= 70.0)
        or risk_heat
    )
    pbb_ready = bool(
        above_200ma
        and bullish_context
        and pullback_profile_pass
        and (lower_reclaim or mid_support)
        and not pbb_overheat
    )

    failed_mid = bool(
        mid is not None
        and high is not None
        and close is not None
        and high >= mid
        and close < mid
    )
    failed_upper = bool(
        upper is not None
        and high is not None
        and close is not None
        and high >= upper
        and close < upper
    )
    rsi_fade = bool(rsi is not None and rsi <= 50.0)
    z_fade = bool(z_score is not None and z_score <= 0.5)
    pbs_ready = bool(
        (failed_mid or failed_upper)
        and weak_close
        and (negative_return or red_bar)
        and (rsi_fade or z_fade or failed_mid)
    )

    oversold = bool(
        (z_score is not None and z_score <= -1.0)
        or (percent_b is not None and percent_b <= 0.10)
        or lower_touch
    )
    mr_long_ready = bool(
        oversold
        and (rsi is None or rsi <= 40.0)
        and (positive_return or green_bar)
        and bullish_close
        and close is not None
        and (lower is None or close >= lower * 0.98)
    )

    upper_touch = bool(
        upper is not None
        and high is not None
        and high >= upper
    )
    overheated = bool(
        risk_heat
        or upper_touch
        or (z_score is not None and z_score >= 1.0)
        or (percent_b is not None and percent_b >= 0.90)
    )
    mr_short_ready = bool(
        overheated
        and (rsi is None or rsi >= 65.0 or risk_heat or (z_score or 0.0) >= 1.0)
        and (negative_return or red_bar or weak_close)
        and (
            upper is None
            or close is None
            or close < upper
            or weak_close
        )
    )

    breakdown_risk = bool(
        close is not None
        and (
            (lower is not None and close < lower)
            or (
                _metric_value(metrics, "bb_zone_low") is not None
                and close <= (_metric_value(metrics, "bb_zone_low") or close)
            )
        )
    )

    reason_codes: list[str] = []
    if pbb_ready:
        reason_codes.append("PBB_BAND_SUPPORT")
    if pbs_ready:
        reason_codes.append("PBS_FAILED_RECLAIM")
    if mr_long_ready:
        reason_codes.append("MR_LONG_OVERSOLD_RECLAIM")
    if mr_short_ready:
        reason_codes.append("MR_SHORT_OVERHEAT_REJECT")
    if breakdown_risk:
        reason_codes.append("SIGMA_LOWER_BAND_BREAK")

    return {
        "bb_percent_b": percent_b,
        "bb_z_score": z_score,
        "pbb_ready": pbb_ready,
        "pbb_score": 80.0 if pbb_ready else 0.0,
        "pbs_ready": pbs_ready,
        "pbs_score": 85.0 if pbs_ready else 0.0,
        "mr_long_ready": mr_long_ready,
        "mr_long_score": 70.0 if mr_long_ready else 0.0,
        "mr_short_ready": mr_short_ready,
        "mr_short_score": 75.0 if mr_short_ready else 0.0,
        "breakdown_risk": breakdown_risk,
        "reason_codes": reason_codes,
    }


def score_exit_pressure(
    frame: pd.DataFrame,
    metrics_seed: Mapping[str, Any],
    cycle: Mapping[str, Any],
) -> dict[str, Any]:
    metrics = dict(metrics_seed)
    close = _latest_ohlc(frame, metrics, "close")
    high = _latest_ohlc(frame, metrics, "high")
    low = _latest_ohlc(frame, metrics, "low")
    atr14 = _metric_value(metrics, "atr14")
    if atr14 is None:
        atr_series = _true_range(frame).rolling(14, min_periods=14).mean()
        if not atr_series.empty:
            atr14 = _safe_float(atr_series.iloc[-1])
    high_series = _numeric(frame, "high")
    chandelier_stop = None
    if atr14 is not None and atr14 > 0 and len(high_series.dropna()) >= 22:
        highest_22 = _safe_float(high_series.dropna().tail(22).max())
        if highest_22 is not None:
            chandelier_stop = highest_22 - (atr14 * 3.0)

    trailing_level = _metric_value(cycle, "trailing_level")
    protected_stop_level = _metric_value(cycle, "protected_stop_level")
    stop_candidates = [
        value
        for value in (trailing_level, protected_stop_level, chandelier_stop)
        if value is not None
    ]
    effective_trailing_level = max(stop_candidates) if stop_candidates else None

    support_low = _metric_value(cycle, "support_zone_low")
    support_break = bool(
        support_low is not None and close is not None and close <= support_low
    )
    support_break_severity = None
    if support_break and atr14 is not None and atr14 > 0 and close is not None:
        support_break_severity = max((support_low - close) / atr14, 0.0)

    lower, mid, upper = _band_values(frame, metrics)
    close_position = _metric_value(metrics, "close_position_pct")
    if close_position is None:
        close_position = _close_position_pct(high=high, low=low, close=close)
    daily_return = _metric_value(metrics, "daily_return_pct")
    channel_active = bool(metrics.get("in_channel8"))
    resistance_candidates = [
        value
        for value in (
            upper if high is not None and upper is not None and high >= upper else None,
            mid,
            _metric_value(cycle, "support_zone_high"),
        )
        if value is not None
    ]
    resistance_level = (
        min(
            [value for value in resistance_candidates if high is not None and high >= value],
            default=None,
        )
        if resistance_candidates
        else None
    )
    resistance_reject = bool(
        not channel_active
        and resistance_level is not None
        and high is not None
        and close is not None
        and high >= resistance_level
        and close < resistance_level
        and (daily_return or 0.0) < 0.0
        and (close_position if close_position is not None else 1.0) <= 0.45
    )
    trailing_break = bool(
        not channel_active
        and effective_trailing_level is not None
        and close is not None
        and close <= effective_trailing_level
    )

    reason_codes: list[str] = []
    if chandelier_stop is not None:
        reason_codes.append("ATR_CHANDELIER_22_3")
    if support_break:
        reason_codes.append("SUPPORT_CLOSE_BREACH")
        if support_break_severity is not None and support_break_severity >= 0.25:
            reason_codes.append("SUPPORT_FAIL_ATR_BUFFER")
    if resistance_reject:
        reason_codes.append("FAILED_RECLAIM_REJECT")
    if trailing_break:
        reason_codes.append("TRAILING_CLOSE_BREACH")

    pressure_score = 0.0
    if support_break:
        pressure_score += 40.0
    if trailing_break:
        pressure_score += 35.0
    if resistance_reject:
        pressure_score += 25.0

    return {
        "chandelier_long_stop": chandelier_stop,
        "effective_trailing_level": effective_trailing_level,
        "support_break": support_break,
        "support_break_severity": support_break_severity,
        "resistance_reject": resistance_reject,
        "resistance_reject_level": resistance_level,
        "trailing_break": trailing_break,
        "exit_pressure_score": min(100.0, pressure_score),
        "reason_codes": reason_codes,
    }
