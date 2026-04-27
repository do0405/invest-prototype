from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import numpy as np
import pandas as pd

from utils.indicator_helpers import rolling_atr, rolling_average_volume


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        casted = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(casted) or np.isinf(casted):
        return None
    return casted


def clamp(value: float | None, low: float = 0.0, high: float = 1.0) -> float:
    if value is None or np.isnan(value) or np.isinf(value):
        return low
    return float(min(max(value, low), high))


def safe_divide(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def score_ratio(value: float | None, good_min: float, bad_min: float) -> float:
    if value is None:
        return 0.0
    if value >= good_min:
        return 1.0
    if value <= bad_min:
        return 0.0
    return clamp((value - bad_min) / max(good_min - bad_min, 1e-9))


def score_inverse(value: float | None, good_max: float, bad_max: float) -> float:
    if value is None:
        return 0.0
    if value <= good_max:
        return 1.0
    if value >= bad_max:
        return 0.0
    return clamp((bad_max - value) / max(bad_max - good_max, 1e-9))


def score_range(value: float | None, ideal_low: float, ideal_high: float, min_low: float, max_high: float) -> float:
    if value is None:
        return 0.0
    if ideal_low <= value <= ideal_high:
        return 1.0
    if value < min_low or value > max_high:
        return 0.0
    if value < ideal_low:
        return clamp((value - min_low) / max(ideal_low - min_low, 1e-9))
    return clamp((max_high - value) / max(max_high - ideal_high, 1e-9))


def weighted_mean(items: list[tuple[float | None, float]]) -> float:
    active = [(float(value), float(weight)) for value, weight in items if value is not None]
    if not active:
        return 0.0
    total_weight = sum(weight for _, weight in active)
    if total_weight <= 0:
        return 0.0
    return sum(value * weight for value, weight in active) / total_weight


def _max_drawdown(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    rolling_peak = numeric.cummax().replace(0, np.nan)
    drawdowns = 1.0 - (numeric / rolling_peak)
    if drawdowns.dropna().empty:
        return None
    return safe_float(drawdowns.max())


def subperiod_return(series: pd.Series, end_offset: int, span: int) -> float | None:
    numeric = _numeric_series(series)
    return _subperiod_return_numeric(numeric, end_offset, span)


def _numeric_series(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").reset_index(drop=True)


def _subperiod_return_numeric(numeric: pd.Series, end_offset: int, span: int) -> float | None:
    if len(numeric) <= end_offset + span:
        return None
    end_idx = len(numeric) - 1 - end_offset
    start_idx = end_idx - span
    if start_idx < 0:
        return None
    start = safe_float(numeric.iloc[start_idx])
    end = safe_float(numeric.iloc[end_idx])
    if start is None or start == 0 or end is None:
        return None
    ratio = safe_divide(end, start)
    if ratio is None:
        return None
    return ratio - 1.0


RS_COMPONENTS: tuple[tuple[int, float], ...] = (
    (63, 0.40),
    (126, 0.20),
    (189, 0.20),
    (252, 0.20),
)


def _benchmark_relative_weighted_rs_with_coverage(
    stock_series: pd.Series,
    benchmark_series: pd.Series,
    *,
    end_offset: int = 0,
) -> tuple[float | None, int]:
    return _benchmark_relative_weighted_rs_with_coverage_numeric(
        _numeric_series(stock_series),
        _numeric_series(benchmark_series),
        end_offset=end_offset,
    )


def _benchmark_relative_weighted_rs_with_coverage_numeric(
    stock_series: pd.Series,
    benchmark_series: pd.Series,
    *,
    end_offset: int = 0,
) -> tuple[float | None, int]:
    paired_components: list[tuple[float | None, float]] = []
    for span, weight in RS_COMPONENTS:
        stock_return = _subperiod_return_numeric(stock_series, end_offset, span)
        benchmark_return = _subperiod_return_numeric(benchmark_series, end_offset, span)
        if stock_return is None or benchmark_return is None:
            continue
        paired_components.append((stock_return - benchmark_return, weight))
    if not paired_components:
        return None, 0
    return weighted_mean(paired_components) * 100.0, len(paired_components)


def benchmark_relative_weighted_rs(
    stock_series: pd.Series,
    benchmark_series: pd.Series,
    *,
    end_offset: int = 0,
) -> float | None:
    score, _coverage = _benchmark_relative_weighted_rs_with_coverage(
        stock_series,
        benchmark_series,
        end_offset=end_offset,
    )
    return score


def rs_rank_proxy_profile_from_history(
    stock_series: pd.Series,
    benchmark_series: pd.Series,
    *,
    history_offsets: int = 126,
) -> dict[str, float | int | None]:
    stock_numeric = _numeric_series(stock_series)
    benchmark_numeric = _numeric_series(benchmark_series)
    current, current_coverage = _benchmark_relative_weighted_rs_with_coverage_numeric(
        stock_numeric,
        benchmark_numeric,
        end_offset=0,
    )
    if current is None:
        return {
            "rs_rank_proxy": None,
            "rs_proxy_sample_count": 0,
            "rs_proxy_component_coverage": 0,
            "rs_proxy_confidence": 0.0,
        }

    max_offset = max(0, min(history_offsets, len(stock_numeric) - 64, len(benchmark_numeric) - 64))
    scores: list[float] = []
    for offset in range(max_offset + 1):
        score, coverage = _benchmark_relative_weighted_rs_with_coverage_numeric(
            stock_numeric,
            benchmark_numeric,
            end_offset=offset,
        )
        if score is not None and coverage == current_coverage:
            scores.append(float(score))

    sample_count = len(scores)
    if sample_count < 5:
        proxy = 1.0 + (98.0 * clamp((current + 30.0) / 60.0))
        confidence = 100.0 * weighted_mean(
            [
                (clamp(sample_count / 5.0), 0.35),
                (clamp(current_coverage / len(RS_COMPONENTS)), 0.65),
            ]
        ) * 0.45
    else:
        rank_pct = sum(1 for score in scores if score <= current) / sample_count
        proxy = 1.0 + (98.0 * clamp(rank_pct))
        confidence = 100.0 * weighted_mean(
            [
                (clamp((sample_count - 4.0) / 60.0), 0.55),
                (clamp(current_coverage / len(RS_COMPONENTS)), 0.45),
            ]
        )

    return {
        "rs_rank_proxy": proxy,
        "rs_proxy_sample_count": sample_count,
        "rs_proxy_component_coverage": current_coverage,
        "rs_proxy_confidence": clamp(confidence / 100.0) * 100.0,
    }


def rs_rank_proxy_from_history(
    stock_series: pd.Series,
    benchmark_series: pd.Series,
    *,
    history_offsets: int = 126,
) -> float | None:
    return safe_float(
        rs_rank_proxy_profile_from_history(
            stock_series,
            benchmark_series,
            history_offsets=history_offsets,
        ).get("rs_rank_proxy")
    )


def hidden_rs_profile_from_aligned(aligned: pd.DataFrame) -> dict[str, float | int | None]:
    if aligned.empty:
        return {
            "hidden_rs_raw": None,
            "hidden_rs_weak_day_count": 0,
            "hidden_rs_down_day_excess_return": None,
            "hidden_rs_drawdown_resilience": None,
            "hidden_rs_weak_window_excess_return": None,
            "hidden_rs_confidence": 0.0,
        }
    window_scores: list[tuple[float | None, float]] = []
    down_day_excess_items: list[tuple[float | None, float]] = []
    drawdown_resilience_items: list[tuple[float | None, float]] = []
    weak_window_excess_items: list[tuple[float | None, float]] = []
    max_weak_day_count = 0
    for length, weight in ((21, 0.25), (65, 0.45), (126, 0.30)):
        window = aligned.tail(length).copy()
        if len(window) < max(15, length // 2):
            continue
        stock_returns = pd.to_numeric(window["daily_return"], errors="coerce")
        benchmark_returns = pd.to_numeric(window["benchmark_return"], errors="coerce")
        weak_days = window[benchmark_returns < 0.0]
        if len(weak_days) < 3:
            continue
        max_weak_day_count = max(max_weak_day_count, int(len(weak_days)))
        relative_down_day_return = safe_float(
            (
                pd.to_numeric(weak_days["daily_return"], errors="coerce")
                - pd.to_numeric(weak_days["benchmark_return"], errors="coerce")
            ).mean()
        )
        stock_window_return = subperiod_return(window["adj_close"], 0, min(len(window) - 1, length - 1))
        benchmark_window_return = subperiod_return(window["benchmark_adj_close"], 0, min(len(window) - 1, length - 1))
        relative_window_return = None
        if stock_window_return is not None and benchmark_window_return is not None:
            relative_window_return = stock_window_return - benchmark_window_return
        stock_drawdown = _max_drawdown(window["adj_close"])
        benchmark_drawdown = _max_drawdown(window["benchmark_adj_close"])
        drawdown_resilience = None
        if stock_drawdown is not None and benchmark_drawdown is not None:
            drawdown_resilience = benchmark_drawdown - stock_drawdown
        latest_close = safe_float(window["adj_close"].iloc[-1])
        recent_median = safe_float(window["adj_close"].tail(max(5, length // 5)).median())
        support_hold = 1.0 if latest_close is not None and recent_median is not None and latest_close >= recent_median else 0.0
        raw_score = weighted_mean(
            [
                ((relative_down_day_return or 0.0) * 100.0, 0.45),
                ((relative_window_return or 0.0) * 100.0, 0.25),
                ((drawdown_resilience or 0.0) * 100.0, 0.20),
                (support_hold * 5.0, 0.10),
            ]
        )
        window_scores.append((raw_score, weight))
        down_day_excess_items.append((relative_down_day_return, weight))
        drawdown_resilience_items.append((drawdown_resilience, weight))
        weak_window_excess_items.append((relative_window_return, weight))
    if not window_scores:
        return {
            "hidden_rs_raw": None,
            "hidden_rs_weak_day_count": max_weak_day_count,
            "hidden_rs_down_day_excess_return": None,
            "hidden_rs_drawdown_resilience": None,
            "hidden_rs_weak_window_excess_return": None,
            "hidden_rs_confidence": 0.0,
        }
    confidence = 100.0 * weighted_mean(
        [
            (clamp(max_weak_day_count / 20.0), 0.65),
            (clamp(len(window_scores) / 3.0), 0.35),
        ]
    )
    return {
        "hidden_rs_raw": weighted_mean(window_scores),
        "hidden_rs_weak_day_count": max_weak_day_count,
        "hidden_rs_down_day_excess_return": weighted_mean(down_day_excess_items),
        "hidden_rs_drawdown_resilience": weighted_mean(drawdown_resilience_items),
        "hidden_rs_weak_window_excess_return": weighted_mean(weak_window_excess_items),
        "hidden_rs_confidence": confidence,
    }


def hidden_rs_raw_from_aligned(aligned: pd.DataFrame) -> float | None:
    return safe_float(hidden_rs_profile_from_aligned(aligned).get("hidden_rs_raw"))


def estimate_structure(daily: pd.DataFrame) -> dict[str, Any]:
    if len(daily) < 60:
        return {
            "base_length": None,
            "base_tightness": None,
            "volatility_contraction": None,
            "range_compression": None,
            "pivot_proximity": None,
            "breakout_volume_expansion": None,
            "structure_quality_score": None,
            "recent_pivot_high": None,
            "box_high": None,
            "box_low": None,
            "box_valid": False,
            "breakout_confirmed": False,
            "structure_readiness_score": None,
            "breakout_confirmation_score": None,
            "dry_volume_ratio": None,
            "box_touch_count": 0,
            "support_hold_count": 0,
            "dry_volume_score": None,
            "failed_breakout_risk_score": None,
            "breakout_quality_score": None,
            "structure_confidence": 0.0,
            "base_depth_pct": None,
            "loose_base_risk_score": None,
            "support_violation_count": 0,
            "breakout_failure_count": 0,
            "breakout_volume_quality_score": None,
            "structure_reject_reason_codes": "insufficient_history",
        }

    atr20 = safe_float(rolling_atr(daily, 20, close_col="adj_close", min_periods=6).iloc[-1])
    atr60 = safe_float(rolling_atr(daily, 60, close_col="adj_close", min_periods=20).iloc[-1])
    atr_ratio = safe_divide(atr20, atr60)
    volatility_contraction = None if atr_ratio is None else 1.0 - min(atr_ratio, 2.0)

    best_length = None
    best_length_int = None
    best_high = None
    best_low = None
    best_tightness = None
    best_score = -1.0
    for length in (15, 20, 25, 30, 40, 50, 60):
        window = daily.iloc[-length:].copy()
        base_high = safe_float(window["high"].max())
        base_low = safe_float(window["low"].min())
        if base_high is None or base_low is None or base_high <= base_low or base_high == 0:
            continue
        depth = (base_high - base_low) / base_high
        tightness = 1.0 - depth
        close_last = safe_float(window["adj_close"].iloc[-1])
        if close_last is None:
            continue
        close_pos = (close_last - base_low) / max(base_high - base_low, 1e-9)
        score = (
            (score_inverse(depth, 0.15, 0.40) * 0.42)
            + (score_ratio(close_pos, 0.70, 0.30) * 0.30)
            + (score_range(float(length), 20.0, 45.0, 10.0, 70.0) * 0.18)
            + (score_ratio(tightness, 0.78, 0.58) * 0.10)
        )
        if score > best_score:
            best_score = score
            best_length = float(length)
            best_length_int = int(length)
            best_high = base_high
            best_low = base_low
            best_tightness = tightness

    range_10 = safe_float((((daily["high"] - daily["low"]) / daily["adj_close"].replace(0, np.nan)).tail(10).mean()))
    range_40 = safe_float((((daily["high"] - daily["low"]) / daily["adj_close"].replace(0, np.nan)).tail(40).mean()))
    range_ratio = safe_divide(range_10, range_40)
    range_compression = None if range_ratio is None else 1.0 - min(range_ratio, 2.0)

    avg_volume_20 = safe_float(rolling_average_volume(daily, 20, min_periods=5).iloc[-1])
    avg_volume_5 = safe_float(rolling_average_volume(daily, 5, min_periods=3).iloc[-1])
    latest_volume = safe_float(daily["volume"].iloc[-1])
    dry_volume_ratio = safe_divide(avg_volume_5, avg_volume_20)
    breakout_volume_expansion = safe_divide(latest_volume, avg_volume_20)

    recent_high = safe_float(daily["high"].tail(30).max())
    close = safe_float(daily["adj_close"].iloc[-1])
    pivot_proximity = None
    if recent_high is not None and recent_high != 0 and close is not None:
        close_to_recent_high = safe_divide(close, recent_high)
        if close_to_recent_high is not None:
            pivot_proximity = 1.0 - min(abs(close_to_recent_high - 1.0) / 0.10, 1.0)

    box_high = None
    box_low = None
    repeated_resistance_tests = 0
    support_holds = 0
    support_violation_count = 0
    breakout_failure_count = 0
    base_depth_pct = None
    if best_length_int is not None and best_length_int >= 2:
        box_window = daily.iloc[-best_length_int:-1].copy()
        if not box_window.empty:
            box_high = safe_float(box_window["high"].max())
            box_low = safe_float(box_window["low"].min())
            if box_high is not None and box_low is not None and box_high > 0:
                base_depth_pct = ((box_high - box_low) / box_high) * 100.0
            if box_high is not None and box_high > 0:
                repeated_resistance_tests = int(((pd.to_numeric(box_window["high"], errors="coerce") / box_high) >= 0.975).sum())
            if box_low is not None and box_low > 0:
                support_holds = int((pd.to_numeric(box_window["low"], errors="coerce") >= (box_low * 0.985)).sum())
                support_floor = safe_float(pd.to_numeric(box_window["low"], errors="coerce").median())
                support_close_floor = safe_float(pd.to_numeric(box_window["adj_close"], errors="coerce").median())
                if support_floor is not None and support_floor > 0 and support_close_floor is not None and support_close_floor > 0:
                    support_violation_count = int(
                        (
                            (
                                pd.to_numeric(box_window["low"], errors="coerce")
                                < support_floor * 0.94
                            )
                            | (
                                pd.to_numeric(box_window["adj_close"], errors="coerce")
                                < support_close_floor * 0.94
                            )
                        ).sum()
                    )
    if box_high is None:
        box_high = best_high
    if box_low is None:
        box_low = best_low

    structure_intact = bool(close is not None and box_low is not None and close >= box_low)
    dry_volume_ok = (dry_volume_ratio is None) or dry_volume_ratio <= 1.15
    dry_volume_score = score_inverse(dry_volume_ratio, 0.70, 1.20) * 100.0
    breakout_volume_quality_score = score_ratio(breakout_volume_expansion, 1.50, 0.80) * 100.0
    box_touch_score = score_ratio(float(repeated_resistance_tests), 3.0, 0.0) * 100.0
    support_hold_score = score_ratio(float(support_holds), 4.0, 0.0) * 100.0
    depth_ratio = None if base_depth_pct is None else base_depth_pct / 100.0
    loose_base_risk_score = weighted_mean(
        [
            (score_ratio(depth_ratio, 0.24, 0.12) * 100.0, 0.55),
            (score_ratio(1.0 - (safe_float(best_tightness) or 0.0), 0.28, 0.14) * 100.0, 0.25),
            (score_ratio(float(support_violation_count), 3.0, 0.0) * 100.0, 0.20),
        ]
    )
    box_valid = bool(
        structure_intact
        and repeated_resistance_tests >= 2
        and support_holds >= 2
        and support_violation_count <= 2
        and (safe_float(best_tightness) or 0.0) >= 0.60
        and (safe_float(range_compression) or 0.0) >= -0.25
        and loose_base_risk_score < 65.0
        and dry_volume_ok
    )
    close_above_box = bool(close is not None and box_high is not None and close > box_high)
    breakout_confirmed = bool(close_above_box and (safe_float(breakout_volume_expansion) or 0.0) >= 1.20)
    failed_breakout_risk_score = 0.0
    if box_high is not None and box_high > 0 and not close_above_box:
        recent = daily.tail(10)
        breakout_failure_count = int(
            (
                (pd.to_numeric(recent["high"], errors="coerce") > box_high)
                & (pd.to_numeric(recent["adj_close"], errors="coerce") < box_high)
            ).sum()
        )
        failed_breakout_risk_score = score_ratio(float(breakout_failure_count), 2.0, 0.0) * 100.0
    if best_length_int is not None and best_length_int > 12:
        prior_resistance_window = daily.iloc[-best_length_int:-10].copy()
        prior_resistance = safe_float(pd.to_numeric(prior_resistance_window["high"], errors="coerce").max())
        if prior_resistance is not None and prior_resistance > 0 and close is not None and close < prior_resistance:
            recent_failed_breaks = int(
                (
                    (pd.to_numeric(daily["high"].tail(10), errors="coerce") > (prior_resistance * 1.005))
                    & (pd.to_numeric(daily["adj_close"].tail(10), errors="coerce") < prior_resistance)
                ).sum()
            )
            breakout_failure_count = max(breakout_failure_count, recent_failed_breaks)
            failed_breakout_risk_score = max(
                failed_breakout_risk_score,
                score_ratio(float(recent_failed_breaks), 2.0, 0.0) * 100.0,
            )
    extended_breakout_risk_score = 0.0
    if close is not None and box_high is not None and box_high > 0 and close_above_box:
        extension_above_box = (close / box_high) - 1.0
        extended_breakout_risk_score = score_ratio(extension_above_box, 0.15, 0.04) * 100.0

    reject_reasons: list[str] = []
    if loose_base_risk_score >= 55.0 or (base_depth_pct is not None and base_depth_pct >= 20.0):
        reject_reasons.append("loose_base")
    if support_violation_count >= 3:
        reject_reasons.append("support_break")
    if failed_breakout_risk_score >= 50.0 or breakout_failure_count >= 2:
        reject_reasons.append("failed_breakout")
    if close_above_box and breakout_volume_quality_score < 45.0:
        reject_reasons.append("no_volume_breakout")
    if close_above_box and extended_breakout_risk_score >= 65.0:
        reject_reasons.append("extended_breakout")
    if repeated_resistance_tests < 2:
        reject_reasons.append("insufficient_touches")

    structure_quality_score = weighted_mean(
        [
            (((safe_float(best_tightness) or 0.0) * 100.0), 0.30),
            (((safe_float(volatility_contraction) or 0.0) * 100.0), 0.22),
            (((safe_float(range_compression) or 0.0) * 100.0), 0.20),
            (((safe_float(pivot_proximity) or 0.0) * 100.0), 0.18),
            (dry_volume_score, 0.10),
        ]
    )
    structure_confidence = weighted_mean(
        [
            (score_ratio(float(len(daily)), 120.0, 50.0) * 100.0, 0.25),
            (score_ratio(float(best_length_int or 0), 30.0, 10.0) * 100.0, 0.20),
            (box_touch_score, 0.20),
            (support_hold_score, 0.20),
            (100.0 if avg_volume_20 is not None and avg_volume_20 > 0 else 0.0, 0.15),
        ]
    )
    structure_readiness_score = weighted_mean(
        [
            (structure_quality_score, 0.28),
            (100.0 if box_valid else 25.0, 0.14),
            (box_touch_score, 0.10),
            (support_hold_score, 0.12),
            (dry_volume_score, 0.10),
            ((100.0 - loose_base_risk_score), 0.10),
            ((100.0 - (score_ratio(float(support_violation_count), 3.0, 0.0) * 100.0)), 0.08),
            ((100.0 - failed_breakout_risk_score), 0.08),
        ]
    )
    breakout_quality_score = weighted_mean(
        [
            (100.0 if close_above_box else 0.0, 0.30),
            (breakout_volume_quality_score, 0.22),
            (structure_readiness_score, 0.22),
            ((100.0 - failed_breakout_risk_score), 0.14),
            ((100.0 - extended_breakout_risk_score), 0.12),
        ]
    )
    breakout_confirmation_score = weighted_mean(
        [
            (100.0 if close_above_box else 0.0, 0.35),
            (breakout_volume_quality_score, 0.30),
            ((100.0 - failed_breakout_risk_score), 0.20),
            (structure_confidence, 0.15),
        ]
    )
    if close_above_box and breakout_volume_quality_score < 45.0:
        breakout_quality_score = min(breakout_quality_score, 55.0)
        breakout_confirmation_score = min(breakout_confirmation_score, 50.0)
    if close_above_box and extended_breakout_risk_score >= 65.0:
        breakout_quality_score = min(breakout_quality_score, 75.0)
    return {
        "base_length": best_length,
        "base_tightness": ((safe_float(best_tightness) or 0.0) * 100.0) if best_tightness is not None else None,
        "volatility_contraction": ((safe_float(volatility_contraction) or 0.0) * 100.0) if volatility_contraction is not None else None,
        "range_compression": ((safe_float(range_compression) or 0.0) * 100.0) if range_compression is not None else None,
        "pivot_proximity": ((safe_float(pivot_proximity) or 0.0) * 100.0) if pivot_proximity is not None else None,
        "breakout_volume_expansion": breakout_volume_expansion,
        "structure_quality_score": structure_quality_score,
        "recent_pivot_high": best_high,
        "box_high": box_high,
        "box_low": box_low,
        "box_valid": box_valid,
        "breakout_confirmed": breakout_confirmed,
        "structure_readiness_score": structure_readiness_score,
        "breakout_confirmation_score": breakout_confirmation_score,
        "dry_volume_ratio": dry_volume_ratio,
        "box_touch_count": repeated_resistance_tests,
        "support_hold_count": support_holds,
        "dry_volume_score": dry_volume_score,
        "failed_breakout_risk_score": failed_breakout_risk_score,
        "breakout_quality_score": breakout_quality_score,
        "structure_confidence": structure_confidence,
        "base_depth_pct": base_depth_pct,
        "loose_base_risk_score": loose_base_risk_score,
        "support_violation_count": support_violation_count,
        "breakout_failure_count": breakout_failure_count,
        "breakout_volume_quality_score": breakout_volume_quality_score,
        "structure_reject_reason_codes": ",".join(dict.fromkeys(reject_reasons)),
    }


def leader_rs_state_from_row(row: Mapping[str, Any]) -> str:
    rs_rank = safe_float(row.get("rs_rank_true")) or safe_float(row.get("rs_rank")) or 0.0
    rs_delta = safe_float(row.get("delta_rs_rank_qoq")) or 0.0
    rs_slope = safe_float(row.get("rs_line_slope")) or safe_float(row.get("rs_line_20d_slope")) or 0.0
    structure_score = safe_float(row.get("structure_readiness_score")) or safe_float(row.get("structure_quality_score")) or 0.0
    trend_score = safe_float(row.get("trend_integrity_score")) or 0.0
    distribution_days = safe_float(row.get("distribution_day_count_20d")) or 0.0
    if trend_score < 35.0 or structure_score < 25.0 or distribution_days >= 7.0:
        return "weakening"
    if rs_rank >= 85.0 and rs_delta > 0.0 and rs_slope > 0.0 and structure_score >= 45.0:
        return "rising"
    if rs_rank >= 85.0 and abs(rs_delta) <= 8.0 and rs_slope >= -0.01 and structure_score >= 45.0:
        return "stable"
    if rs_delta <= -8.0 or rs_slope <= -0.03:
        return "fading"
    return "unknown"


def leader_score_v2(row: Mapping[str, Any], *, breadth_context_score: float, score_multiplier: float) -> float:
    raw_score = weighted_mean(
        [
            (safe_float(row.get("group_strength_score")), 0.12),
            (safe_float(row.get("trend_integrity_score")), 0.11),
            (safe_float(row.get("rs_quality_score")), 0.22),
            (safe_float(row.get("leadership_freshness_score")), 0.14),
            (safe_float(row.get("momentum_persistence_score")), 0.09),
            (safe_float(row.get("near_high_leadership_score")), 0.07),
            (safe_float(row.get("hidden_rs_score")), 0.08),
            (safe_float(row.get("structure_readiness_score")), 0.09),
            (safe_float(row.get("breakout_confirmation_score")), 0.03),
            (safe_float(row.get("volume_demand_score")), 0.03),
            (safe_float(row.get("liquidity_quality_score")), 0.03),
            (100.0 - (safe_float(row.get("extension_risk_score")) or 0.0), 0.04),
            (breadth_context_score, 0.03),
        ]
    )
    return raw_score * score_multiplier


@dataclass(frozen=True)
class LeaderClassification:
    label: str
    legacy_label: str
    leader_tier: str
    entry_suitability: str
    hybrid_gate_pass: bool
    strict_rs_gate_pass: bool
    hard_gate_pass: bool
    extended_flag: bool
    leader_sort_score: float
    phase_bucket: str


def classify_leader(
    row: Mapping[str, Any],
    *,
    calibration: Mapping[str, float],
    traded_value_floor: float,
    leader_score: float,
) -> LeaderClassification:
    rs_rank = safe_float(row.get("rs_rank_true")) or safe_float(row.get("rs_rank")) or 0.0
    rs_line_score = safe_float(row.get("rs_line_score")) or 0.0
    rs_slope = safe_float(row.get("rs_line_slope")) or safe_float(row.get("rs_line_20d_slope")) or 0.0
    relative_strength = safe_float(row.get("benchmark_relative_strength"))
    group_strength = safe_float(row.get("group_strength_score")) or 0.0
    trend_score = safe_float(row.get("trend_integrity_score")) or 0.0
    structure_score = safe_float(row.get("structure_readiness_score")) or 0.0
    hidden_rs = safe_float(row.get("hidden_rs_score")) or 0.0
    freshness = safe_float(row.get("leadership_freshness_score")) or 0.0
    extension = safe_float(row.get("extension_risk_score")) or 0.0
    traded_value = safe_float(row.get("traded_value_20d")) or 0.0
    illiq_score = safe_float(row.get("illiq_score")) or 0.0
    distance_from_low_value = safe_float(row.get("distance_from_52w_low"))
    distance_to_high_value = safe_float(row.get("distance_to_52w_high"))
    pivot_proximity_value = safe_float(row.get("pivot_proximity"))
    distance_from_low = 0.0 if distance_from_low_value is None else distance_from_low_value
    distance_to_high = 1.0 if distance_to_high_value is None else distance_to_high_value
    pivot_proximity = 0.0 if pivot_proximity_value is None else pivot_proximity_value
    rvol = safe_float(row.get("rvol")) or 0.0

    liquidity_gate = traded_value >= traded_value_floor and illiq_score >= 20.0
    structure_preserved = bool(row.get("close_gt_50")) and trend_score >= 45.0 and structure_score >= 35.0 and distance_from_low >= 0.15
    relative_strength_gate = bool((relative_strength is not None and relative_strength >= 1.0) or rs_slope > 0.0)
    rs_or_structure_evidence = any(
        [
            rs_rank >= calibration.get("leader_rs_rank_min", 85.0),
            rs_line_score >= 62.0,
            bool(row.get("rs_new_high_before_price_flag")),
            hidden_rs >= 65.0,
            structure_score >= 70.0,
            bool(row.get("breakout_confirmed")),
        ]
    )
    hybrid_gate = bool(liquidity_gate and structure_preserved and relative_strength_gate and rs_or_structure_evidence)
    strict_gate = bool(
        liquidity_gate
        and relative_strength_gate
        and trend_score >= 70.0
        and structure_score >= 45.0
        and rs_rank >= calibration.get("leader_rs_rank_min", 85.0)
        and rs_line_score >= calibration.get("leader_rs_line_score_min", 70.0)
        and group_strength >= calibration.get("leader_group_strength_min", 70.0)
        and distance_to_high <= calibration.get("leader_distance_to_high_max", 0.20)
    )

    extended = bool(
        extension >= 75.0
        or (
            distance_to_high <= calibration.get("leader_extended_distance_to_high_max", 0.03)
            and (pivot_proximity < calibration.get("leader_extended_pivot_proximity_max", 25.0) or rvol >= 2.5)
        )
    )
    if not hybrid_gate:
        leader_tier = "reject"
        entry_suitability = "avoid"
        label = "reject"
        legacy_label = "Too Weak, Reject"
        phase_bucket = "NONE"
    else:
        leader_tier = "strong" if strict_gate else "emerging"
        if extended:
            entry_suitability = "extended"
            label = "extended_leader"
            legacy_label = "Extended Leader"
            phase_bucket = "COMPLETED_NOT_FRESH"
        else:
            entry_suitability = "fresh" if freshness >= 55.0 and structure_score >= 45.0 and extension < 65.0 else "watch"
            label = "strong_leader" if leader_tier == "strong" else "emerging_leader"
            legacy_label = "Confirmed Leader" if leader_tier == "strong" else "Emerging Leader"
            phase_bucket = "RECENT_OR_ACTIONABLE" if leader_tier == "strong" else "FORMING"

    sort_penalty = 18.0 if entry_suitability == "extended" else 45.0 if entry_suitability == "avoid" else 0.0
    leader_sort_score = max(0.0, leader_score - sort_penalty)
    return LeaderClassification(
        label=label,
        legacy_label=legacy_label,
        leader_tier=leader_tier,
        entry_suitability=entry_suitability,
        hybrid_gate_pass=hybrid_gate,
        strict_rs_gate_pass=strict_gate,
        hard_gate_pass=hybrid_gate,
        extended_flag=extended,
        leader_sort_score=leader_sort_score,
        phase_bucket=phase_bucket,
    )


def source_evidence_tags_from_row(row: Mapping[str, Any]) -> str:
    tags: list[str] = []
    if bool(row.get("rs_new_high_before_price_flag")):
        tags.append("rs_new_high_before_price")
    if (safe_float(row.get("hidden_rs_score")) or 0.0) >= 65.0:
        tags.append("hidden_rs")
    if (safe_float(row.get("near_high_leadership_score")) or 0.0) >= 90.0:
        tags.append("near_52w_high")
    leader_rs_state = str(row.get("leader_rs_state") or "")
    if leader_rs_state and leader_rs_state != "unknown":
        tags.append(f"rs_state_{leader_rs_state}")
    if bool(row.get("breakout_confirmed")):
        tags.append("breakout_confirmed")
    if (safe_float(row.get("extension_risk_score")) or 0.0) >= 70.0:
        tags.append("extension_risk")
    return ",".join(dict.fromkeys([tag for tag in tags if tag]))
