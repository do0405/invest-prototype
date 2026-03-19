from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from utils.actual_data_calibration import bounded_quantile_value
from utils.indicator_helpers import normalize_indicator_frame, rolling_sma
from utils.io_utils import ensure_dir
from utils.market_data_contract import PricePolicy, load_benchmark_data, load_local_ohlcv_frame
from utils.market_runtime import (
    ensure_market_dirs,
    get_benchmark_candidates,
    get_market_data_dir,
    get_primary_benchmark_symbol,
    get_stock_metadata_path,
    get_weinstein_stage2_results_dir,
    is_index_symbol,
    market_key,
)
from utils.progress_runtime import is_progress_tick, progress_interval
from utils.typing_utils import is_na_like, series_to_str_text_dict, series_value_counts_to_int_dict, to_float_or_none


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        casted = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(casted) or np.isinf(casted):
        return None
    return casted


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    if np.isnan(value) or np.isinf(value):
        return low
    return float(min(max(value, low), high))


def _date_str(value: Any) -> str | None:
    ts = pd.to_datetime(value, errors="coerce")
    if is_na_like(ts):
        return None
    return ts.strftime("%Y-%m-%d")


def _ratio_or_none(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _pct_change_value(start: float | None, end: float | None) -> float | None:
    ratio = _ratio_or_none(end, start)
    if ratio is None:
        return None
    return ratio - 1.0


def _pct_slope(series: pd.Series, end_idx: int, lookback: int) -> float | None:
    if end_idx < lookback:
        return None
    current = _safe_float(series.iloc[end_idx])
    previous = _safe_float(series.iloc[end_idx - lookback])
    if current is None or previous is None or previous == 0:
        return None
    return (current / previous - 1.0) / float(lookback)


def _score_range(value: float | None, ideal_low: float, ideal_high: float, min_low: float, max_high: float) -> float:
    if value is None:
        return 0.0
    if ideal_low <= value <= ideal_high:
        return 1.0
    if value < min_low or value > max_high:
        return 0.0
    if value < ideal_low:
        return _clamp((value - min_low) / max(ideal_low - min_low, 1e-9))
    return _clamp((max_high - value) / max(max_high - ideal_high, 1e-9))


def _score_inverse(value: float | None, good_max: float, bad_max: float) -> float:
    if value is None:
        return 0.0
    if value <= good_max:
        return 1.0
    if value >= bad_max:
        return 0.0
    return _clamp((bad_max - value) / max(bad_max - good_max, 1e-9))


def _score_ratio(value: float | None, good_min: float, bad_min: float) -> float:
    if value is None:
        return 0.0
    if value >= good_min:
        return 1.0
    if value <= bad_min:
        return 0.0
    return _clamp((value - bad_min) / max(good_min - bad_min, 1e-9))


@dataclass(frozen=True)
class BaseWindow:
    start_idx: int
    end_idx: int
    start_date: str | None
    end_date: str | None
    base_high: float
    base_low: float
    duration_weeks: int
    width_pct: float
    around_ma_ratio: float
    contraction_ratio: float | None
    close_position: float
    quality_score: float


@dataclass(frozen=True)
class MarketContext:
    benchmark_symbol: str
    market_state: str
    breadth150_market: float | None
    benchmark_close: float | None
    ma30w: float | None
    ma30w_slope_4w: float | None
    market_score: float
    assumption_flags: tuple[str, ...] = ()


@dataclass(frozen=True)
class GroupContext:
    group_name: str
    group_state: str
    breadth150_group: float | None
    group_mrs: float | None
    group_rp_ma52_slope: float | None
    group_score: float
    data_available: bool
    assumption_flags: tuple[str, ...] = ()


@dataclass(frozen=True)
class BreakoutSignal:
    breakout_idx: int
    breakout_date: str | None
    breakout_level: float
    breakout_age_weeks: int
    breakout_close: float
    breakout_week_volume: float | None
    weekly_volume_ratio: float | None
    base_window: BaseWindow
    resistance_pass: bool
    resistance_confidence: str
    breakout_type: str
    volume_reference: str
    continuation_setup_pass: bool = False
    continuation_quality_score: float | None = None
    continuation_lifecycle_state: str | None = None
    continuation_volume_dryup_ratio: float | None = None


@dataclass(frozen=True)
class ContinuationSetup:
    base_window: BaseWindow
    prior_run_pct: float | None
    support_hold_ratio: float
    volume_dryup_ratio: float | None
    quality_score: float
    lifecycle_weeks_above_ma30: int
    lifecycle_state: str
    breakout_ready: bool


class WeinsteinStage2Analyzer:
    BREAKOUT_BUFFER = 0.005
    SMALL_BAND = 0.02
    FLAT_THRESHOLD = 0.0025
    TURNING_UP_THRESHOLD = -0.0005
    PRE_STAGE2_MAX_PCT = 5.0
    RETEST_MAX_AGE = 8
    CONTINUATION_MIN_WEEKS = 4
    CONTINUATION_MAX_WEEKS = 16

    def _normalize_daily_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        return normalize_indicator_frame(frame, price_policy=PricePolicy.SPLIT_ADJUSTED)

    def build_weekly_bars(self, frame: pd.DataFrame) -> pd.DataFrame:
        daily = self._normalize_daily_frame(frame)
        if daily.empty:
            return pd.DataFrame(
                columns=["week_key", "bar_end_date", "open", "high", "low", "close", "volume", "session_count"]
            )

        daily["week_key"] = daily["date"].dt.to_period("W-SUN")
        weekly = (
            daily.groupby("week_key", sort=True)
            .agg(
                bar_end_date=("date", "last"),
                open=("open", "first"),
                high=("high", "max"),
                low=("low", "min"),
                close=("close", "last"),
                volume=("volume", "sum"),
                session_count=("date", "count"),
            )
            .reset_index()
        )
        weekly = weekly.dropna(subset=["bar_end_date", "open", "high", "low", "close"]).copy()
        if weekly.empty:
            return weekly

        weekly["range_pct"] = (weekly["high"] - weekly["low"]) / weekly["close"].replace(0, np.nan)
        weekly["ma10w"] = rolling_sma(weekly["close"], 10, min_periods=4)
        weekly["ma30w"] = rolling_sma(weekly["close"], 30, min_periods=10)
        weekly["ma40w"] = rolling_sma(weekly["close"], 40, min_periods=12)
        weekly["volume_avg_prev_4w"] = weekly["volume"].shift(1).rolling(4, min_periods=1).mean()
        weekly["volume_avg_prev_10w"] = weekly["volume"].shift(1).rolling(10, min_periods=2).mean()
        weekly["volume_avg_prev_52w"] = weekly["volume"].shift(1).rolling(52, min_periods=8).mean()
        for column, ratio_col in (
            ("volume_avg_prev_4w", "volume_ratio_4w"),
            ("volume_avg_prev_10w", "volume_ratio_10w"),
            ("volume_avg_prev_52w", "volume_ratio_52w"),
        ):
            weekly[ratio_col] = weekly["volume"] / weekly[column].replace(0, np.nan)
        weekly["weekly_volume_ratio"] = weekly["volume_ratio_4w"]
        return weekly.reset_index(drop=True)

    def _determine_mode(self, weekly: pd.DataFrame, benchmark_weekly: pd.DataFrame) -> tuple[str, list[str], int]:
        flags: list[str] = []
        penalty = 0
        if len(weekly) >= 156 and len(benchmark_weekly) >= 156:
            return "FULL_FIDELITY_MODE", flags, penalty
        flags.append("bootstrap_mode")
        penalty += 8
        return "BOOTSTRAP_MODE", flags, penalty

    def _compute_rs_features(self, weekly: pd.DataFrame, benchmark_weekly: pd.DataFrame) -> tuple[pd.DataFrame, list[str], int]:
        flags: list[str] = []
        penalty = 0
        result = weekly.copy()
        result["rp"] = np.nan
        result["rp_ma52"] = np.nan
        result["mrs"] = np.nan
        result["rs_proxy"] = np.nan
        result["rp_ma52_slope"] = np.nan
        result["rs_proxy_slope"] = np.nan

        if benchmark_weekly.empty:
            flags.append("missing_benchmark_history")
            penalty += 15
            return result, flags, penalty

        benchmark = benchmark_weekly[["bar_end_date", "close"]].rename(columns={"close": "benchmark_close"}).copy()
        merged = pd.merge_asof(
            result.sort_values("bar_end_date"),
            benchmark.sort_values("bar_end_date"),
            on="bar_end_date",
            direction="backward",
        )
        merged["rp"] = (merged["close"] / merged["benchmark_close"].replace(0, np.nan)) * 100.0
        merged["rp_ma52"] = merged["rp"].rolling(52, min_periods=26).mean()
        merged["mrs"] = ((merged["rp"] / merged["rp_ma52"]) - 1.0) * 100.0

        available = len(merged)
        proxy_window = min(26, max(8, available // 2))
        merged["rp_proxy_ma"] = merged["rp"].rolling(proxy_window, min_periods=max(4, proxy_window // 2)).mean()
        if available > 1:
            rp_start = _safe_float(merged["rp"].iloc[max(0, available - proxy_window)])
            rp_end = _safe_float(merged["rp"].iloc[-1])
            if rp_start and rp_end and rp_start > 0:
                merged.loc[:, "rs_proxy"] = (rp_end / rp_start) - 1.0

        for idx in range(len(merged)):
            merged.loc[idx, "rp_ma52_slope"] = _pct_slope(merged["rp_ma52"], idx, 4)
            merged.loc[idx, "rs_proxy_slope"] = _pct_slope(merged["rp_proxy_ma"], idx, 4)

        if merged["mrs"].dropna().empty:
            flags.append("bootstrap_rs_proxy")
            penalty += 8

        return merged, flags, penalty

    def _compute_overhead_reference(
        self,
        weekly: pd.DataFrame,
        end_idx: int,
        mode: str,
    ) -> tuple[float | None, str, list[str], int]:
        flags: list[str] = []
        penalty = 0
        if end_idx < 0 or weekly.empty:
            return None, "LOW", ["missing_history"], 12

        if mode == "FULL_FIDELITY_MODE" and end_idx + 1 >= 104:
            reference = _safe_float(weekly.iloc[max(0, end_idx - 103) : end_idx + 1]["high"].max())
            return reference, "HIGH", flags, penalty

        reference = _safe_float(weekly.iloc[: end_idx + 1]["high"].max())
        flags.append("missing_104w_resistance")
        penalty += 6
        return reference, "LOW", flags, penalty

    def _price_crosses_ma_frequently(self, weekly: pd.DataFrame, end_idx: int, window: int = 12) -> bool:
        if end_idx < 2:
            return False
        start = max(0, end_idx - window + 1)
        segment = weekly.iloc[start : end_idx + 1].copy()
        if segment.empty or segment["ma30w"].dropna().empty:
            return False
        sign = pd.Series(np.sign(segment["close"] - segment["ma30w"]), index=segment.index)
        changes = int((sign != sign.shift(1)).sum())
        return changes >= 4

    def _daily_trend_snapshot(self, daily: pd.DataFrame) -> dict[str, float | bool | None]:
        if daily.empty:
            return {
                "daily_close": None,
                "ma50d": None,
                "ma200d": None,
                "ma50d_slope_20d": None,
                "ma200d_slope_20d": None,
                "close_gt_ma50d": False,
                "close_gt_ma200d": False,
                "ma50d_gt_ma200d": False,
            }

        ma50d_series = rolling_sma(daily["close"], 50, min_periods=20)
        ma200d_series = rolling_sma(daily["close"], 200, min_periods=80)
        daily_close = _safe_float(daily["close"].iloc[-1])
        ma50d = _safe_float(ma50d_series.iloc[-1])
        ma200d = _safe_float(ma200d_series.iloc[-1])
        return {
            "daily_close": daily_close,
            "ma50d": ma50d,
            "ma200d": ma200d,
            "ma50d_slope_20d": _pct_slope(ma50d_series, len(daily) - 1, 20),
            "ma200d_slope_20d": _pct_slope(ma200d_series, len(daily) - 1, 20),
            "close_gt_ma50d": bool(daily_close is not None and ma50d is not None and daily_close > ma50d),
            "close_gt_ma200d": bool(daily_close is not None and ma200d is not None and daily_close > ma200d),
            "ma50d_gt_ma200d": bool(ma50d is not None and ma200d is not None and ma50d > ma200d),
        }

    def _weekly_alignment_snapshot(self, weekly: pd.DataFrame, idx: int) -> dict[str, float | bool | None]:
        if weekly.empty or idx < 0 or idx >= len(weekly):
            return {
                "close": None,
                "ma10w": None,
                "ma30w": None,
                "ma40w": None,
                "ma30w_slope_4w": None,
                "ma40w_slope_4w": None,
                "close_gt_ma10w": False,
                "close_gt_ma30w": False,
                "close_gt_ma40w": False,
                "ma10w_gt_ma30w": False,
                "ma30w_gt_ma40w": False,
            }

        close = _safe_float(weekly.loc[idx, "close"])
        ma10w = _safe_float(weekly.loc[idx, "ma10w"]) if "ma10w" in weekly.columns else None
        ma30w = _safe_float(weekly.loc[idx, "ma30w"]) if "ma30w" in weekly.columns else None
        ma40w = _safe_float(weekly.loc[idx, "ma40w"]) if "ma40w" in weekly.columns else None
        return {
            "close": close,
            "ma10w": ma10w,
            "ma30w": ma30w,
            "ma40w": ma40w,
            "ma30w_slope_4w": _pct_slope(weekly["ma30w"], idx, 4) if "ma30w" in weekly.columns else None,
            "ma40w_slope_4w": _pct_slope(weekly["ma40w"], idx, 4) if "ma40w" in weekly.columns else None,
            "close_gt_ma10w": bool(close is not None and ma10w is not None and close > ma10w),
            "close_gt_ma30w": bool(close is not None and ma30w is not None and close > ma30w),
            "close_gt_ma40w": bool(close is not None and ma40w is not None and close > ma40w),
            "ma10w_gt_ma30w": bool(ma10w is not None and ma30w is not None and ma10w > ma30w),
            "ma30w_gt_ma40w": bool(ma30w is not None and ma40w is not None and ma30w >= ma40w),
        }

    def _trend_alignment_pass(self, weekly: pd.DataFrame, idx: int) -> bool:
        alignment = self._weekly_alignment_snapshot(weekly, idx)
        ma30w = _safe_float(alignment.get("ma30w"))
        if ma30w is None or not bool(alignment.get("close_gt_ma30w")):
            return False

        ma30w_slope_4w = _safe_float(alignment.get("ma30w_slope_4w"))
        if ma30w_slope_4w is None or ma30w_slope_4w < 0:
            return False

        ma40w = _safe_float(alignment.get("ma40w"))
        if ma40w is not None and not bool(alignment.get("close_gt_ma40w")):
            return False

        ma40w_slope_4w = _safe_float(alignment.get("ma40w_slope_4w"))
        if ma40w is not None and ma40w_slope_4w is not None and ma40w_slope_4w < 0:
            return False

        ma10w = _safe_float(alignment.get("ma10w"))
        if ma10w is not None and not bool(alignment.get("close_gt_ma10w")):
            return False

        if ma10w is not None and ma30w is not None and not bool(alignment.get("ma10w_gt_ma30w")):
            return False

        if ma40w is not None and ma30w is not None and not bool(alignment.get("ma30w_gt_ma40w")):
            return False

        return True

    def _classify_breakout_type(self, weekly: pd.DataFrame, base_window: BaseWindow) -> str:
        prior_end = base_window.start_idx - 1
        if prior_end < 8:
            return "STAGE2_BREAKOUT"

        prior_start = max(0, prior_end - 7)
        prior_window = weekly.iloc[prior_start : prior_end + 1].copy()
        if prior_window.empty:
            return "STAGE2_BREAKOUT"

        prior_close_start = _safe_float(prior_window["close"].iloc[0])
        prior_close_end = _safe_float(prior_window["close"].iloc[-1])
        ma30w_last = _safe_float(prior_window["ma30w"].iloc[-1]) if "ma30w" in prior_window.columns else None
        ma40w_last = _safe_float(prior_window["ma40w"].iloc[-1]) if "ma40w" in prior_window.columns else None
        slope4 = _pct_slope(prior_window["ma30w"], len(prior_window) - 1, 4) if "ma30w" in prior_window.columns else None
        prior_run = _pct_change_value(prior_close_start, prior_close_end)

        if (
            prior_run is not None
            and prior_run >= 0.12
            and ma30w_last is not None
            and prior_close_end is not None
            and prior_close_end > ma30w_last
            and (ma40w_last is None or ma30w_last >= ma40w_last)
            and (slope4 is None or slope4 > 0)
        ):
            return "CONTINUATION_BREAKOUT"
        return "STAGE2_BREAKOUT"

    def _post_breakout_structure_pass(self, weekly: pd.DataFrame, breakout_signal: BreakoutSignal, latest_idx: int) -> bool:
        if latest_idx <= breakout_signal.breakout_idx:
            return True
        post = weekly.iloc[breakout_signal.breakout_idx : latest_idx + 1].copy()
        if len(post) <= 1:
            return True
        highs = pd.to_numeric(post["high"], errors="coerce")
        lows = pd.to_numeric(post["low"], errors="coerce")
        for position in range(1, len(post)):
            prev_high = _safe_float(highs.iloc[position - 1])
            curr_high = _safe_float(highs.iloc[position])
            prev_low = _safe_float(lows.iloc[position - 1])
            curr_low = _safe_float(lows.iloc[position])
            if prev_high is None or curr_high is None or prev_low is None or curr_low is None:
                return False
            if curr_high < prev_high * (1.0 - self.SMALL_BAND):
                return False
            if curr_low < prev_low * (1.0 - self.SMALL_BAND):
                return False
        return True

    def _lighter_volume_retest(
        self,
        weekly: pd.DataFrame,
        latest_idx: int,
        breakout_signal: BreakoutSignal,
    ) -> bool:
        if latest_idx <= breakout_signal.breakout_idx:
            return False
        current_volume = _safe_float(weekly.loc[latest_idx, "volume"])
        breakout_week_volume = breakout_signal.breakout_week_volume
        breakout_volume_ratio = breakout_signal.weekly_volume_ratio
        current_ratio = _safe_float(weekly.loc[latest_idx, "weekly_volume_ratio"])
        if current_volume is None or breakout_week_volume is None or breakout_week_volume == 0:
            return False
        lighter_absolute = current_volume <= breakout_week_volume * 0.9
        lighter_relative = (
            current_ratio is not None
            and breakout_volume_ratio is not None
            and current_ratio <= max(1.2, breakout_volume_ratio * 0.75)
        )
        return bool(lighter_absolute or lighter_relative)

    def _resolve_weekly_volume_ratio(self, weekly: pd.DataFrame, idx: int, mode: str) -> tuple[float | None, str]:
        preferred = [
            ("volume_ratio_4w", "4w"),
            ("volume_ratio_10w", "10w"),
        ]
        if mode == "FULL_FIDELITY_MODE":
            preferred.append(("volume_ratio_52w", "52w"))
        else:
            preferred.append(("volume_ratio_52w", "52w_proxy"))

        for column, label in preferred:
            if column not in weekly.columns:
                continue
            value = _safe_float(weekly.loc[idx, column])
            if value is not None:
                return value, label
        return None, "missing"

    def _find_best_base(self, weekly: pd.DataFrame, end_idx: int, mode: str) -> BaseWindow | None:
        if weekly.empty or end_idx < 19:
            return None

        min_duration = 20 if mode == "BOOTSTRAP_MODE" else 26
        max_duration = min(65, end_idx + 1)
        candidates: list[BaseWindow] = []
        slope8 = _pct_slope(weekly["ma30w"], end_idx, 8)
        for duration in range(min_duration, max_duration + 1):
            start_idx = end_idx - duration + 1
            if start_idx < 0:
                continue
            window = weekly.iloc[start_idx : end_idx + 1].copy()
            if len(window) < duration:
                continue
            if window["ma30w"].notna().sum() < max(8, duration // 2):
                continue

            base_high = _safe_float(window["high"].max())
            base_low = _safe_float(window["low"].min())
            if base_high is None or base_low is None or base_low <= 0 or base_high <= base_low:
                continue
            width_pct = (base_high - base_low) / base_low
            if width_pct > 0.8 or width_pct < 0.05:
                continue

            ma_distance = (window["close"] / window["ma30w"].replace(0, np.nan) - 1.0).abs()
            around_ma_ratio = float((ma_distance <= 0.08).mean())
            if around_ma_ratio < 0.35:
                continue

            first_ranges = window["range_pct"].head(min(5, len(window))).dropna()
            last_ranges = window["range_pct"].tail(min(5, len(window))).dropna()
            contraction_ratio = None
            first_range_median = _safe_float(first_ranges.median()) if not first_ranges.empty else None
            last_range_median = _safe_float(last_ranges.median()) if not last_ranges.empty else None
            if first_range_median is not None and first_range_median > 0 and last_range_median is not None:
                contraction_ratio = float(last_range_median / first_range_median)

            latest_window_close = _safe_float(window["close"].iloc[-1])
            if latest_window_close is None:
                continue
            close_position = (latest_window_close - base_low) / max(base_high - base_low, 1e-9)
            duration_score = _score_range(duration, 26, 52, min_duration, 65)
            flatness_score = _score_inverse(abs(slope8) if slope8 is not None else None, self.FLAT_THRESHOLD, 0.01)
            width_score = _score_range(width_pct, 0.10, 0.45, 0.05, 0.80)
            contraction_score = _score_inverse(contraction_ratio, 1.0, 1.25)
            score = float(
                np.mean(
                    [
                        duration_score,
                        flatness_score,
                        around_ma_ratio,
                        width_score,
                        contraction_score,
                        _score_ratio(close_position, 0.55, 0.25),
                    ]
                )
            )
            if score < 0.5:
                continue

            candidates.append(
                BaseWindow(
                    start_idx=start_idx,
                    end_idx=end_idx,
                    start_date=_date_str(window["bar_end_date"].iloc[0]),
                    end_date=_date_str(window["bar_end_date"].iloc[-1]),
                    base_high=base_high,
                    base_low=base_low,
                    duration_weeks=duration,
                    width_pct=width_pct,
                    around_ma_ratio=around_ma_ratio,
                    contraction_ratio=contraction_ratio,
                    close_position=close_position,
                    quality_score=score,
                )
            )

        if not candidates:
            return None
        return max(candidates, key=lambda item: item.quality_score)

    def _volume_dryup_ratio(self, weekly: pd.DataFrame, start_idx: int, end_idx: int) -> float | None:
        if weekly.empty or start_idx < 0 or end_idx < start_idx:
            return None
        base_window = weekly.iloc[start_idx : end_idx + 1]
        reference_start = max(0, start_idx - max(8, end_idx - start_idx + 1))
        reference_window = weekly.iloc[reference_start:start_idx]
        if reference_window.empty:
            return None
        base_volume = _safe_float(pd.to_numeric(base_window["volume"], errors="coerce").median())
        reference_volume = _safe_float(pd.to_numeric(reference_window["volume"], errors="coerce").median())
        return _ratio_or_none(base_volume, reference_volume)

    def _consecutive_weeks_above_ma30(self, weekly: pd.DataFrame, end_idx: int) -> int:
        if weekly.empty or end_idx < 0:
            return 0
        count = 0
        for idx in range(end_idx, -1, -1):
            close = _safe_float(weekly.loc[idx, "close"])
            ma30w = _safe_float(weekly.loc[idx, "ma30w"])
            if close is None or ma30w is None or close <= ma30w:
                break
            count += 1
        return count

    def _find_continuation_base(self, weekly: pd.DataFrame, end_idx: int) -> BaseWindow | None:
        if weekly.empty or end_idx < (self.CONTINUATION_MIN_WEEKS + 6):
            return None

        candidates: list[BaseWindow] = []
        max_duration = min(self.CONTINUATION_MAX_WEEKS, end_idx + 1)
        slope4 = _pct_slope(weekly["ma30w"], end_idx, 4)
        if slope4 is None or slope4 <= 0:
            return None

        for duration in range(self.CONTINUATION_MIN_WEEKS, max_duration + 1):
            start_idx = end_idx - duration + 1
            if start_idx < 0:
                continue
            window = weekly.iloc[start_idx : end_idx + 1].copy()
            prior_start = max(0, start_idx - max(8, duration))
            prior_window = weekly.iloc[prior_start:start_idx].copy()
            if len(prior_window) < max(4, duration // 2):
                continue
            if window["ma30w"].notna().sum() < max(3, duration // 2):
                continue

            base_high = _safe_float(window["high"].max())
            base_low = _safe_float(window["low"].min())
            if base_high is None or base_low is None or base_low <= 0 or base_high <= base_low:
                continue
            width_pct = (base_high - base_low) / base_low
            if width_pct > 0.30 or width_pct < 0.03:
                continue

            prior_close_start = _safe_float(prior_window["close"].iloc[0])
            prior_close_end = _safe_float(prior_window["close"].iloc[-1])
            prior_run_pct = _pct_change_value(prior_close_start, prior_close_end)
            if prior_run_pct is None:
                continue
            if prior_run_pct < 0.12:
                continue

            ma30_support = window["ma30w"].replace(0, np.nan)
            support_hold_ratio = float(
                (
                    (window["low"] >= ma30_support * 0.97)
                    | (window["close"] >= ma30_support)
                ).mean()
            )
            if support_hold_ratio < 0.60:
                continue

            latest_window_close = _safe_float(window["close"].iloc[-1])
            if latest_window_close is None:
                continue
            close_position = (latest_window_close - base_low) / max(base_high - base_low, 1e-9)
            if close_position < 0.45:
                continue
            first_close = _safe_float(window["close"].iloc[0])
            last_close = _safe_float(window["close"].iloc[-1])
            net_progress_pct = _pct_change_value(first_close, last_close)
            if net_progress_pct is not None:
                net_progress_pct = abs(net_progress_pct)
            if net_progress_pct is None or net_progress_pct > 0.10:
                continue

            first_ranges = window["range_pct"].head(max(2, duration // 2)).dropna()
            last_ranges = window["range_pct"].tail(max(2, duration // 2)).dropna()
            contraction_ratio = None
            first_range_median = _safe_float(first_ranges.median()) if not first_ranges.empty else None
            last_range_median = _safe_float(last_ranges.median()) if not last_ranges.empty else None
            if first_range_median is not None and first_range_median > 0 and last_range_median is not None:
                contraction_ratio = float(last_range_median / first_range_median)

            dryup_ratio = self._volume_dryup_ratio(weekly, start_idx, end_idx)
            quality_score = float(
                np.mean(
                    [
                        _score_ratio(prior_run_pct, 0.20, 0.08),
                        _score_inverse(width_pct, 0.18, 0.32),
                        _score_ratio(support_hold_ratio, 0.85, 0.55),
                        _score_inverse(dryup_ratio, 0.85, 1.15),
                        _score_inverse(contraction_ratio, 0.95, 1.25),
                        _score_inverse(net_progress_pct, 0.05, 0.14),
                        _score_ratio(close_position, 0.65, 0.40),
                    ]
                )
            )
            if quality_score < 0.58:
                continue

            candidates.append(
                BaseWindow(
                    start_idx=start_idx,
                    end_idx=end_idx,
                    start_date=_date_str(window["bar_end_date"].iloc[0]),
                    end_date=_date_str(window["bar_end_date"].iloc[-1]),
                    base_high=base_high,
                    base_low=base_low,
                    duration_weeks=duration,
                    width_pct=width_pct,
                    around_ma_ratio=support_hold_ratio,
                    contraction_ratio=contraction_ratio,
                    close_position=close_position,
                    quality_score=quality_score,
                )
            )

        if not candidates:
            return None
        return max(candidates, key=lambda item: item.quality_score)

    def _evaluate_continuation_setup(
        self,
        weekly: pd.DataFrame,
        base_window: BaseWindow | None,
    ) -> ContinuationSetup | None:
        if base_window is None or weekly.empty:
            return None

        prior_start = max(0, base_window.start_idx - max(8, base_window.duration_weeks))
        prior_window = weekly.iloc[prior_start : base_window.start_idx].copy()
        if prior_window.empty:
            return None

        prior_close_start = _safe_float(prior_window["close"].iloc[0])
        prior_close_end = _safe_float(prior_window["close"].iloc[-1])
        prior_run_pct = _pct_change_value(prior_close_start, prior_close_end)

        window = weekly.iloc[base_window.start_idx : base_window.end_idx + 1].copy()
        ma30_support = window["ma30w"].replace(0, np.nan)
        support_hold_ratio = float(
            (
                (window["low"] >= ma30_support * 0.97)
                | (window["close"] >= ma30_support)
            ).mean()
        )
        volume_dryup_ratio = self._volume_dryup_ratio(weekly, base_window.start_idx, base_window.end_idx)
        lifecycle_weeks = self._consecutive_weeks_above_ma30(weekly, base_window.start_idx - 1)
        if lifecycle_weeks <= 26:
            lifecycle_state = "EARLY"
        elif lifecycle_weeks <= 52:
            lifecycle_state = "MID"
        else:
            lifecycle_state = "LATE"

        quality_score = float(
            np.mean(
                [
                    base_window.quality_score,
                    _score_ratio(prior_run_pct, 0.20, 0.08),
                    _score_ratio(support_hold_ratio, 0.85, 0.55),
                    _score_inverse(volume_dryup_ratio, 0.85, 1.15),
                ]
            )
        )
        breakout_ready = bool(
            (prior_run_pct is not None and prior_run_pct >= 0.12)
            and base_window.duration_weeks <= self.CONTINUATION_MAX_WEEKS
            and support_hold_ratio >= 0.65
            and base_window.close_position >= 0.55
            and (volume_dryup_ratio is None or volume_dryup_ratio <= 1.08)
        )
        return ContinuationSetup(
            base_window=base_window,
            prior_run_pct=prior_run_pct,
            support_hold_ratio=support_hold_ratio,
            volume_dryup_ratio=volume_dryup_ratio,
            quality_score=quality_score,
            lifecycle_weeks_above_ma30=lifecycle_weeks,
            lifecycle_state=lifecycle_state,
            breakout_ready=breakout_ready,
        )

    def _rs_pass(self, weekly: pd.DataFrame, idx: int) -> tuple[bool, float]:
        mrs = _safe_float(weekly.loc[idx, "mrs"]) if idx in weekly.index else None
        rp_ma52_slope = _safe_float(weekly.loc[idx, "rp_ma52_slope"]) if idx in weekly.index else None
        rs_proxy = _safe_float(weekly.loc[idx, "rs_proxy"]) if idx in weekly.index else None
        rs_proxy_slope = _safe_float(weekly.loc[idx, "rs_proxy_slope"]) if idx in weekly.index else None
        if mrs is not None:
            passed = mrs > 0 and (rp_ma52_slope is None or rp_ma52_slope >= 0)
            score = _clamp(0.5 + (mrs / 20.0), 0.0, 1.0)
            return passed, score
        if rs_proxy is not None:
            passed = rs_proxy > 0 and (rs_proxy_slope is None or rs_proxy_slope >= 0)
            score = _clamp(0.5 + (rs_proxy / 0.4), 0.0, 1.0)
            return passed, score
        return False, 0.0

    def _detect_recent_breakout(self, weekly: pd.DataFrame, mode: str) -> BreakoutSignal | None:
        if weekly.empty or len(weekly) < 24:
            return None
        latest_idx = len(weekly) - 1
        earliest_idx = max(10, latest_idx - 8)
        first_valid: BreakoutSignal | None = None
        for idx in range(earliest_idx, latest_idx + 1):
            close = _safe_float(weekly.loc[idx, "close"])
            if close is None:
                continue
            weekly_alignment_pass = self._trend_alignment_pass(weekly, idx)
            weekly_volume_ratio, volume_reference = self._resolve_weekly_volume_ratio(weekly, idx, mode)
            breakout_week_volume = _safe_float(weekly.loc[idx, "volume"])
            rs_pass, _ = self._rs_pass(weekly, idx)
            if not (
                weekly_alignment_pass
                and weekly_volume_ratio is not None
                and weekly_volume_ratio >= 2.0
                and rs_pass
            ):
                continue

            breakout_candidates: list[BreakoutSignal] = []

            continuation_base = self._find_continuation_base(weekly, idx - 1)
            continuation_setup = self._evaluate_continuation_setup(weekly, continuation_base)
            if (
                continuation_base is not None
                and continuation_setup is not None
                and continuation_setup.breakout_ready
                and close > continuation_base.base_high * (1.0 + self.BREAKOUT_BUFFER)
            ):
                overhead_reference, resistance_confidence, _, _ = self._compute_overhead_reference(weekly, idx - 1, mode)
                resistance_pass = (
                    overhead_reference is None
                    or continuation_base.base_high >= overhead_reference * 0.95
                    or close >= overhead_reference * (1.0 + self.BREAKOUT_BUFFER)
                )
                if resistance_pass or mode == "BOOTSTRAP_MODE":
                    breakout_candidates.append(
                        BreakoutSignal(
                            breakout_idx=idx,
                            breakout_date=_date_str(weekly.loc[idx, "bar_end_date"]),
                            breakout_level=continuation_base.base_high,
                            breakout_age_weeks=latest_idx - idx,
                            breakout_close=close,
                            breakout_week_volume=breakout_week_volume,
                            weekly_volume_ratio=weekly_volume_ratio,
                            base_window=continuation_base,
                            resistance_pass=resistance_pass,
                            resistance_confidence=resistance_confidence,
                            breakout_type="CONTINUATION_BREAKOUT",
                            volume_reference=volume_reference,
                            continuation_setup_pass=True,
                            continuation_quality_score=continuation_setup.quality_score,
                            continuation_lifecycle_state=continuation_setup.lifecycle_state,
                            continuation_volume_dryup_ratio=continuation_setup.volume_dryup_ratio,
                        )
                    )

            stage1_base = self._find_best_base(weekly, idx - 1, mode)
            if stage1_base is not None and close > stage1_base.base_high * (1.0 + self.BREAKOUT_BUFFER):
                overhead_reference, resistance_confidence, _, _ = self._compute_overhead_reference(weekly, idx - 1, mode)
                resistance_pass = (
                    overhead_reference is None
                    or stage1_base.base_high >= overhead_reference * 0.95
                    or close >= overhead_reference * (1.0 + self.BREAKOUT_BUFFER)
                )
                breakout_type = self._classify_breakout_type(weekly, stage1_base)
                if breakout_type == "STAGE2_BREAKOUT" and (resistance_pass or mode == "BOOTSTRAP_MODE"):
                    breakout_candidates.append(
                        BreakoutSignal(
                            breakout_idx=idx,
                            breakout_date=_date_str(weekly.loc[idx, "bar_end_date"]),
                            breakout_level=stage1_base.base_high,
                            breakout_age_weeks=latest_idx - idx,
                            breakout_close=close,
                            breakout_week_volume=breakout_week_volume,
                            weekly_volume_ratio=weekly_volume_ratio,
                            base_window=stage1_base,
                            resistance_pass=resistance_pass,
                            resistance_confidence=resistance_confidence,
                            breakout_type="STAGE2_BREAKOUT",
                            volume_reference=volume_reference,
                        )
                    )

            if not breakout_candidates:
                continue

            breakout_signal = max(
                breakout_candidates,
                key=lambda item: (
                    item.breakout_type == "CONTINUATION_BREAKOUT",
                    item.continuation_quality_score or 0.0,
                    item.base_window.quality_score,
                    item.breakout_level,
                ),
            )
            if first_valid is None:
                first_valid = breakout_signal
        return first_valid

    def _determine_stock_stage(
        self,
        weekly: pd.DataFrame,
        latest_idx: int,
        latest_base: BaseWindow | None,
        breakout_signal: BreakoutSignal | None,
        continuation_setup: ContinuationSetup | None,
    ) -> str:
        close = _safe_float(weekly.loc[latest_idx, "close"])
        ma30w = _safe_float(weekly.loc[latest_idx, "ma30w"])
        ma40w = _safe_float(weekly.loc[latest_idx, "ma40w"]) if "ma40w" in weekly.columns else None
        ma10w = _safe_float(weekly.loc[latest_idx, "ma10w"]) if "ma10w" in weekly.columns else None
        slope4 = _pct_slope(weekly["ma30w"], latest_idx, 4)
        slope8 = _pct_slope(weekly["ma30w"], latest_idx, 8)
        if (
            close is not None
            and breakout_signal is not None
            and breakout_signal.breakout_age_weeks <= 8
            and ma30w is not None
            and close > ma30w
            and (ma40w is None or close > ma40w)
            and close >= breakout_signal.breakout_level
        ):
            if breakout_signal.breakout_type == "CONTINUATION_BREAKOUT":
                return "STAGE_2B"
            return "STAGE_2A"
        if (
            close is not None
            and latest_base is not None
            and close <= latest_base.base_high * (1.0 + self.BREAKOUT_BUFFER)
            and slope8 is not None
            and abs(slope8) <= max(self.FLAT_THRESHOLD * 2.0, 0.005)
        ):
            return "STAGE_1"
        if close is not None and ma30w is not None and close < ma30w and (ma40w is None or close < ma40w) and slope8 is not None and slope8 < -self.FLAT_THRESHOLD:
            return "STAGE_4"
        if (
            close is not None
            and ma30w is not None
            and close > ma30w
            and (ma40w is None or close > ma40w)
            and slope8 is not None
            and slope8 > 0
            and (ma10w is None or close > ma10w)
        ):
            if continuation_setup is not None and continuation_setup.breakout_ready:
                return "STAGE_2B"
            return "STAGE_2"
        if slope8 is not None and abs(slope8) <= self.FLAT_THRESHOLD and self._price_crosses_ma_frequently(weekly, latest_idx):
            return "STAGE_3"
        if latest_base is not None and slope4 is not None and slope4 >= self.TURNING_UP_THRESHOLD:
            return "STAGE_1"
        if latest_base is not None:
            return "STAGE_1"
        return "STAGE_3" if slope8 is not None and abs(slope8) <= self.FLAT_THRESHOLD else "STAGE_4"

    def _timing_priority(self, timing_state: str) -> tuple[int, str]:
        mapping = {
            "BREAKOUT_WEEK": (100, "P1"),
            "PRE_STAGE2_HIGH": (95, "P1"),
            "FRESH_STAGE2_W1": (90, "P2"),
            "PRE_STAGE2_MEDIUM": (85, "P2"),
            "FRESH_STAGE2_W2": (75, "P3"),
            "PRE_STAGE2_LOW": (65, "S1"),
            "RETEST_B": (55, "S1"),
            "BASE": (25, "WATCH"),
            "TOO_LATE_FOR_PRIMARY": (0, "EXCLUDED"),
            "FAIL": (0, "EXCLUDED"),
            "EXCLUDE": (0, "EXCLUDED"),
        }
        return mapping.get(timing_state, (0, "EXCLUDED"))

    def _confidence_tier(self, penalty_points: int) -> str:
        if penalty_points <= 8:
            return "HIGH"
        if penalty_points <= 18:
            return "MEDIUM"
        return "LOW"

    def analyze_symbol(
        self,
        *,
        symbol: str,
        market: str,
        daily_frame: pd.DataFrame,
        benchmark_symbol: str,
        benchmark_daily: pd.DataFrame,
        market_context: MarketContext,
        group_context: GroupContext | None = None,
        exchange: str = "",
        sector: str = "",
        industry_group: str = "",
    ) -> dict[str, Any]:
        normalized_market = market_key(market)
        daily = self._normalize_daily_frame(daily_frame)
        weekly = self.build_weekly_bars(daily)
        benchmark_weekly = self.build_weekly_bars(benchmark_daily)
        daily_trend = self._daily_trend_snapshot(daily)

        default_group = group_context or GroupContext(
            group_name=sector or "Unknown",
            group_state="GROUP_NEUTRAL",
            breadth150_group=None,
            group_mrs=None,
            group_rp_ma52_slope=None,
            group_score=50.0,
            data_available=False,
            assumption_flags=("missing_group_data",),
        )

        assumption_flags: list[str] = list(market_context.assumption_flags) + list(default_group.assumption_flags)
        penalty_points = 0
        if "missing_group_data" in assumption_flags:
            penalty_points += 6

        if weekly.empty or len(weekly) < 30:
            return {
                "symbol": symbol,
                "market": normalized_market,
                "exchange": exchange,
                "benchmark_symbol": benchmark_symbol,
                "mode": "BOOTSTRAP_MODE",
                "market_state": market_context.market_state,
                "group_state": default_group.group_state,
                "stock_stage": "STAGE_1",
                "timing_state": "EXCLUDE",
                "base_high": None,
                "base_low": None,
                "percent_to_stage2": None,
                "breakout_age_weeks": None,
                "weekly_close": None,
                "daily_close": None,
                "ma10w": None,
                "ma30w": None,
                "ma40w": None,
                "ma50d": None,
                "ma200d": None,
                "ma30w_slope_4w": None,
                "ma40w_slope_4w": None,
                "rp": None,
                "mrs": None,
                "rs_proxy": None,
                "rp_ma52_slope": None,
                "weekly_volume_ratio": None,
                "weekly_volume_reference": None,
                "overhead_reference": None,
                "resistance_confidence": "LOW",
                "breadth150_market": market_context.breadth150_market,
                "breadth150_group": default_group.breadth150_group,
                "early_stage2_score": 0.0,
                "confidence_tier": "LOW",
                "priority_label": "EXCLUDED",
                "rejection_reason": "insufficient_weekly_history",
                "sector": sector,
                "industry_group": industry_group,
                "assumption_flags": sorted(set(assumption_flags + ["insufficient_weekly_history"])),
                "confidence_penalty_points": 20,
                "base_duration_weeks": None,
                "base_quality_score": None,
                "group_name": default_group.group_name,
                "retest_signal": False,
                "retest_volume_lighter": False,
                "breakout_level": None,
                "breakout_date": None,
                "breakout_type": None,
                "continuation_setup_pass": False,
                "continuation_quality_score": None,
                "continuation_lifecycle_state": None,
                "continuation_volume_dryup_ratio": None,
                "breakout_structure_pass": False,
                "close_gt_ma10w": False,
                "close_gt_ma40w": False,
                "close_gt_ma50d": False,
                "close_gt_ma200d": False,
                "ma50d_gt_ma200d": False,
                "bar_end_date": None,
            }

        mode, mode_flags, mode_penalty = self._determine_mode(weekly, benchmark_weekly)
        assumption_flags.extend(mode_flags)
        penalty_points += mode_penalty

        weekly, rs_flags, rs_penalty = self._compute_rs_features(weekly, benchmark_weekly)
        assumption_flags.extend(rs_flags)
        penalty_points += rs_penalty

        latest_idx = len(weekly) - 1
        latest_base = self._find_best_base(weekly, latest_idx, mode)
        breakout_signal = self._detect_recent_breakout(weekly, mode)
        active_base = breakout_signal.base_window if breakout_signal is not None else latest_base
        current_continuation_base = self._find_continuation_base(weekly, latest_idx)
        current_continuation_setup = self._evaluate_continuation_setup(weekly, current_continuation_base)
        stock_stage = self._determine_stock_stage(
            weekly,
            latest_idx,
            latest_base,
            breakout_signal,
            current_continuation_setup,
        )

        latest_close = _safe_float(weekly.loc[latest_idx, "close"])
        latest_low = _safe_float(weekly.loc[latest_idx, "low"])
        ma10w = _safe_float(weekly.loc[latest_idx, "ma10w"]) if "ma10w" in weekly.columns else None
        ma30w = _safe_float(weekly.loc[latest_idx, "ma30w"])
        ma40w = _safe_float(weekly.loc[latest_idx, "ma40w"]) if "ma40w" in weekly.columns else None
        ma30w_slope_4w = _pct_slope(weekly["ma30w"], latest_idx, 4)
        ma40w_slope_4w = _pct_slope(weekly["ma40w"], latest_idx, 4) if "ma40w" in weekly.columns else None
        rp = _safe_float(weekly.loc[latest_idx, "rp"])
        mrs = _safe_float(weekly.loc[latest_idx, "mrs"])
        rs_proxy = _safe_float(weekly.loc[latest_idx, "rs_proxy"])
        rp_ma52_slope = _safe_float(weekly.loc[latest_idx, "rp_ma52_slope"])
        current_volume_ratio, current_volume_reference = self._resolve_weekly_volume_ratio(weekly, latest_idx, mode)
        overhead_reference, resistance_confidence, resistance_flags, resistance_penalty = self._compute_overhead_reference(
            weekly,
            latest_idx,
            mode,
        )
        assumption_flags.extend(resistance_flags)
        penalty_points += resistance_penalty
        rs_pass, rs_score_unit = self._rs_pass(weekly, latest_idx)

        percent_to_stage2 = None
        base_high = None
        base_low = None
        base_duration_weeks = None
        base_quality_score = None
        if active_base is not None:
            base_high = active_base.base_high
            base_low = active_base.base_low
            base_duration_weeks = active_base.duration_weeks
            base_quality_score = active_base.quality_score
            percent_to_stage2 = (
                ((active_base.base_high - latest_close) / latest_close) * 100.0
                if latest_close is not None and latest_close > 0
                else None
            )

        group_is_weak = default_group.data_available and default_group.group_state == "GROUP_WEAK"
        rejection_reason = ""
        timing_state = "EXCLUDE"
        breakout_age_weeks = breakout_signal.breakout_age_weeks if breakout_signal is not None else None
        retest_signal = False
        retest_volume_lighter = False
        breakout_structure_pass = False if breakout_signal is None else self._post_breakout_structure_pass(weekly, breakout_signal, latest_idx)
        daily_close = _safe_float(daily_trend.get("daily_close"))
        ma50d = _safe_float(daily_trend.get("ma50d"))
        ma200d = _safe_float(daily_trend.get("ma200d"))
        close_gt_ma50d = bool(daily_trend.get("close_gt_ma50d"))
        close_gt_ma200d = bool(daily_trend.get("close_gt_ma200d"))
        ma50d_gt_ma200d = bool(daily_trend.get("ma50d_gt_ma200d"))
        close_gt_ma10w = bool(latest_close is not None and ma10w is not None and latest_close > ma10w)
        close_gt_ma40w = bool(latest_close is not None and ma40w is not None and latest_close > ma40w)
        weekly_trend_ok = self._trend_alignment_pass(weekly, latest_idx)
        daily_trend_ok = ((ma50d is None and ma200d is None) or (close_gt_ma50d and close_gt_ma200d and ma50d_gt_ma200d))

        if market_context.market_state == "MARKET_STAGE4_RISK":
            timing_state = "EXCLUDE"
            rejection_reason = "market_stage4_risk"
        else:
            if breakout_signal is not None:
                breakout_level = breakout_signal.breakout_level
                breakout_age_weeks = breakout_signal.breakout_age_weeks
                if latest_close is None or latest_close < breakout_level:
                    timing_state = "FAIL"
                    rejection_reason = "missing_latest_close" if latest_close is None else "lost_breakout_level"
                else:
                    retest_volume_lighter = self._lighter_volume_retest(weekly, latest_idx, breakout_signal)
                    retest_signal = (
                        breakout_age_weeks is not None
                        and 1 <= breakout_age_weeks <= self.RETEST_MAX_AGE
                        and latest_low is not None
                        and latest_low <= breakout_level * (1.0 + self.SMALL_BAND)
                        and latest_close >= breakout_level
                        and rs_pass
                        and ma30w is not None
                        and latest_close >= ma30w
                        and retest_volume_lighter
                    )
                    if group_is_weak:
                        timing_state = "EXCLUDE"
                        rejection_reason = "group_weak"
                    elif not weekly_trend_ok or not daily_trend_ok:
                        timing_state = "EXCLUDE"
                        rejection_reason = "trend_alignment_failed"
                    elif breakout_age_weeks == 0:
                        timing_state = "BREAKOUT_WEEK"
                    elif breakout_age_weeks == 1 and breakout_structure_pass:
                        timing_state = "FRESH_STAGE2_W1"
                    elif breakout_age_weeks == 2 and breakout_structure_pass:
                        timing_state = "FRESH_STAGE2_W2"
                    elif retest_signal:
                        timing_state = "RETEST_B"
                    else:
                        timing_state = "TOO_LATE_FOR_PRIMARY"
                        rejection_reason = "breakout_follow_through_missing" if breakout_age_weeks in {1, 2} else "breakout_age_ge_3"
            elif (
                active_base is not None
                and stock_stage == "STAGE_1"
                and percent_to_stage2 is not None
                and percent_to_stage2 <= self.PRE_STAGE2_MAX_PCT
                and latest_close is not None
                and ma30w is not None
                and latest_close < active_base.base_high * (1.0 + self.BREAKOUT_BUFFER)
                and latest_close >= ma30w * 0.97
                and (ma30w_slope_4w is None or ma30w_slope_4w >= self.TURNING_UP_THRESHOLD)
                and rs_pass
                and not group_is_weak
                and weekly_trend_ok
                and daily_trend_ok
            ):
                if percent_to_stage2 <= 1.0:
                    timing_state = "PRE_STAGE2_HIGH"
                elif percent_to_stage2 <= 2.0:
                    timing_state = "PRE_STAGE2_MEDIUM"
                else:
                    timing_state = "PRE_STAGE2_LOW"
            elif active_base is not None and stock_stage == "STAGE_1":
                timing_state = "BASE"
                rejection_reason = "base_not_close_enough_to_breakout"
            else:
                timing_state = "EXCLUDE"
                rejection_reason = "stage_not_actionable"

        freshness_points, priority_label = self._timing_priority(timing_state)
        market_score = market_context.market_score
        group_score = default_group.group_score
        relative_strength_score = round(rs_score_unit * 100.0, 2)
        if timing_state in {"BREAKOUT_WEEK", "FRESH_STAGE2_W1", "FRESH_STAGE2_W2"}:
            volume_quality_score = round(_score_ratio(current_volume_ratio, 2.0, 1.1) * 100.0, 2)
        elif timing_state.startswith("PRE_STAGE2"):
            recent_volume_ratio = _safe_float(weekly.loc[latest_idx, "volume_ratio_4w"])
            volume_quality_score = round(_score_inverse(recent_volume_ratio, 1.0, 1.4) * 100.0, 2)
        else:
            volume_quality_score = round(_score_ratio(current_volume_ratio, 1.2, 0.8) * 100.0, 2)
        base_quality_score_points = round((base_quality_score or 0.0) * 100.0, 2)
        early_stage2_score = (
            0.25 * freshness_points
            + 0.15 * market_score
            + 0.15 * group_score
            + 0.15 * relative_strength_score
            + 0.15 * volume_quality_score
            + 0.15 * base_quality_score_points
        )
        early_stage2_score = max(0.0, round(early_stage2_score - penalty_points, 2))

        return {
            "symbol": symbol,
            "market": normalized_market,
            "exchange": exchange,
            "benchmark_symbol": benchmark_symbol,
            "mode": mode,
            "market_state": market_context.market_state,
            "group_state": default_group.group_state,
            "stock_stage": stock_stage,
            "timing_state": timing_state,
            "base_high": _safe_float(base_high),
            "base_low": _safe_float(base_low),
            "percent_to_stage2": _safe_float(percent_to_stage2),
            "breakout_age_weeks": breakout_age_weeks,
            "weekly_close": latest_close,
            "daily_close": daily_close,
            "ma10w": _safe_float(ma10w),
            "ma30w": _safe_float(ma30w),
            "ma40w": _safe_float(ma40w),
            "ma50d": ma50d,
            "ma200d": ma200d,
            "ma30w_slope_4w": _safe_float(ma30w_slope_4w),
            "ma40w_slope_4w": _safe_float(ma40w_slope_4w),
            "rp": rp,
            "mrs": mrs,
            "rs_proxy": rs_proxy,
            "rp_ma52_slope": rp_ma52_slope,
            "weekly_volume_ratio": current_volume_ratio,
            "weekly_volume_reference": breakout_signal.volume_reference if breakout_signal is not None else current_volume_reference,
            "overhead_reference": overhead_reference,
            "resistance_confidence": breakout_signal.resistance_confidence if breakout_signal is not None else resistance_confidence,
            "breadth150_market": market_context.breadth150_market,
            "breadth150_group": default_group.breadth150_group,
            "early_stage2_score": early_stage2_score,
            "confidence_tier": self._confidence_tier(penalty_points),
            "priority_label": priority_label,
            "rejection_reason": rejection_reason,
            "sector": sector,
            "industry_group": industry_group,
            "assumption_flags": sorted(set(assumption_flags)),
            "confidence_penalty_points": penalty_points,
            "base_duration_weeks": base_duration_weeks,
            "base_quality_score": _safe_float(base_quality_score),
            "group_name": default_group.group_name,
            "retest_signal": retest_signal,
            "retest_volume_lighter": retest_volume_lighter,
            "breakout_level": breakout_signal.breakout_level if breakout_signal is not None else None,
            "breakout_date": breakout_signal.breakout_date if breakout_signal is not None else None,
            "breakout_type": breakout_signal.breakout_type if breakout_signal is not None else None,
            "continuation_setup_pass": bool(
                (breakout_signal is not None and breakout_signal.continuation_setup_pass)
                or (current_continuation_setup is not None and current_continuation_setup.breakout_ready)
            ),
            "continuation_quality_score": (
                _safe_float(breakout_signal.continuation_quality_score) if breakout_signal is not None else None
            ) or (
                _safe_float(current_continuation_setup.quality_score) if current_continuation_setup is not None else None
            ),
            "continuation_lifecycle_state": (
                breakout_signal.continuation_lifecycle_state
                if breakout_signal is not None and breakout_signal.continuation_lifecycle_state
                else (current_continuation_setup.lifecycle_state if current_continuation_setup is not None else None)
            ),
            "continuation_volume_dryup_ratio": (
                _safe_float(breakout_signal.continuation_volume_dryup_ratio) if breakout_signal is not None else None
            ) or (
                _safe_float(current_continuation_setup.volume_dryup_ratio) if current_continuation_setup is not None else None
            ),
            "breakout_structure_pass": breakout_structure_pass,
            "close_gt_ma10w": close_gt_ma10w,
            "close_gt_ma40w": close_gt_ma40w,
            "close_gt_ma50d": close_gt_ma50d,
            "close_gt_ma200d": close_gt_ma200d,
            "ma50d_gt_ma200d": ma50d_gt_ma200d,
            "bar_end_date": _date_str(weekly.loc[latest_idx, "bar_end_date"]),
        }

    def compute_market_context(
        self,
        *,
        market: str,
        benchmark_symbol: str,
        benchmark_daily: pd.DataFrame,
        daily_frames: dict[str, pd.DataFrame],
    ) -> MarketContext:
        assumption_flags: list[str] = []
        above_flags = []
        for frame in daily_frames.values():
            daily = self._normalize_daily_frame(frame)
            if daily.empty or len(daily) < 150:
                continue
            ma150 = to_float_or_none(rolling_sma(daily["close"], 150, min_periods=50).iloc[-1])
            close = to_float_or_none(daily["close"].iloc[-1])
            if ma150 is not None and close is not None:
                above_flags.append(bool(close > ma150))
        breadth150_market = float(np.mean(above_flags) * 100.0) if above_flags else None
        if breadth150_market is None:
            assumption_flags.append("missing_market_breadth")

        benchmark_weekly = self.build_weekly_bars(benchmark_daily)
        benchmark_close = _safe_float(benchmark_weekly["close"].iloc[-1]) if not benchmark_weekly.empty else None
        ma30w = _safe_float(benchmark_weekly["ma30w"].iloc[-1]) if not benchmark_weekly.empty else None
        ma30w_slope_4w = _pct_slope(benchmark_weekly["ma30w"], len(benchmark_weekly) - 1, 4) if len(benchmark_weekly) >= 5 else None

        if benchmark_weekly.empty:
            assumption_flags.append("missing_benchmark_history")
            market_state = "MARKET_NEUTRAL"
        elif benchmark_close is not None and ma30w is not None and ma30w_slope_4w is not None and breadth150_market is not None:
            if benchmark_close > ma30w and ma30w_slope_4w > 0 and breadth150_market >= 40:
                market_state = "MARKET_STAGE2_FAVORABLE"
            elif breadth150_market < 40:
                market_state = "MARKET_STAGE4_RISK"
            else:
                market_state = "MARKET_NEUTRAL"
        elif breadth150_market is not None and breadth150_market < 40:
            market_state = "MARKET_STAGE4_RISK"
        else:
            market_state = "MARKET_NEUTRAL"

        if market_state == "MARKET_STAGE2_FAVORABLE":
            market_score = 100.0 if breadth150_market is not None and breadth150_market >= 60 else 80.0
        elif market_state == "MARKET_STAGE4_RISK":
            market_score = 10.0
        else:
            market_score = 60.0

        return MarketContext(
            benchmark_symbol=benchmark_symbol,
            market_state=market_state,
            breadth150_market=breadth150_market,
            benchmark_close=benchmark_close,
            ma30w=ma30w,
            ma30w_slope_4w=_safe_float(ma30w_slope_4w),
            market_score=market_score,
            assumption_flags=tuple(sorted(set(assumption_flags))),
        )

    def compute_group_contexts(
        self,
        *,
        market: str,
        daily_frames: dict[str, pd.DataFrame],
        benchmark_daily: pd.DataFrame,
        sector_map: dict[str, str],
    ) -> tuple[dict[str, GroupContext], pd.DataFrame]:
        groups: dict[str, list[str]] = {}
        for symbol, sector in sector_map.items():
            if not sector:
                continue
            groups.setdefault(str(sector).strip(), []).append(symbol)

        benchmark_weekly = self.build_weekly_bars(benchmark_daily)
        contexts: dict[str, GroupContext] = {}
        ranking_rows: list[dict[str, Any]] = []

        for sector, members in sorted(groups.items()):
            if len(members) < 3:
                contexts[sector] = GroupContext(
                    group_name=sector,
                    group_state="GROUP_NEUTRAL",
                    breadth150_group=None,
                    group_mrs=None,
                    group_rp_ma52_slope=None,
                    group_score=50.0,
                    data_available=False,
                    assumption_flags=("missing_group_data",),
                )
                continue

            above_flags: list[bool] = []
            normalized_closes: list[pd.DataFrame] = []
            for symbol in members:
                daily = self._normalize_daily_frame(daily_frames.get(symbol, pd.DataFrame()))
                if daily.empty or len(daily) < 60:
                    continue
                if len(daily) >= 150:
                    ma150 = to_float_or_none(rolling_sma(daily["close"], 150, min_periods=50).iloc[-1])
                    latest_close = to_float_or_none(daily["close"].iloc[-1])
                    if ma150 is not None and latest_close is not None:
                        above_flags.append(bool(latest_close > ma150))
                base_close = to_float_or_none(daily["close"].iloc[0])
                if base_close is None or base_close <= 0:
                    continue
                normalized = daily[["date", "close"]].copy()
                normalized["close_norm"] = normalized["close"] / base_close
                normalized["symbol"] = symbol
                normalized_closes.append(normalized[["date", "symbol", "close_norm"]])

            if len(normalized_closes) < 3:
                contexts[sector] = GroupContext(
                    group_name=sector,
                    group_state="GROUP_NEUTRAL",
                    breadth150_group=float(np.mean(above_flags) * 100.0) if above_flags else None,
                    group_mrs=None,
                    group_rp_ma52_slope=None,
                    group_score=50.0,
                    data_available=False,
                    assumption_flags=("missing_group_data",),
                )
                continue

            combined = pd.concat(normalized_closes, ignore_index=True)
            group_series = combined.pivot_table(index="date", values="close_norm", aggfunc="mean").reset_index()
            group_series = group_series.rename(columns={"close_norm": "close"})
            group_series["open"] = group_series["close"]
            group_series["high"] = group_series["close"]
            group_series["low"] = group_series["close"]
            group_series["volume"] = 0.0
            group_weekly = self.build_weekly_bars(group_series)
            group_weekly, _, _ = self._compute_rs_features(group_weekly, benchmark_weekly)

            breadth150_group = float(np.mean(above_flags) * 100.0) if above_flags else None
            latest_idx = len(group_weekly) - 1
            group_mrs = _safe_float(group_weekly.loc[latest_idx, "mrs"]) if latest_idx >= 0 else None
            group_rp_ma52_slope = _safe_float(group_weekly.loc[latest_idx, "rp_ma52_slope"]) if latest_idx >= 0 else None

            if group_mrs is not None and group_mrs > 0 and (group_rp_ma52_slope is None or group_rp_ma52_slope >= 0) and (breadth150_group is None or breadth150_group >= 40):
                group_state = "GROUP_STRONG"
                group_score = 100.0
            elif breadth150_group is not None and breadth150_group < 40 and (group_mrs is None or group_mrs <= 0):
                group_state = "GROUP_WEAK"
                group_score = 0.0
            else:
                group_state = "GROUP_NEUTRAL"
                group_score = 60.0

            contexts[sector] = GroupContext(
                group_name=sector,
                group_state=group_state,
                breadth150_group=breadth150_group,
                group_mrs=group_mrs,
                group_rp_ma52_slope=group_rp_ma52_slope,
                group_score=group_score,
                data_available=True,
                assumption_flags=(),
            )
            ranking_rows.append(
                {
                    "group_name": sector,
                    "group_state": group_state,
                    "breadth150_group": breadth150_group,
                    "group_mrs": group_mrs,
                    "group_rp_ma52_slope": group_rp_ma52_slope,
                    "group_score": group_score,
                    "member_count": len(members),
                }
            )

        ranking_df = pd.DataFrame(ranking_rows)
        if not ranking_df.empty:
            ranking_df = ranking_df.sort_values(
                ["group_score", "breadth150_group", "group_mrs"],
                ascending=[False, False, False],
            ).reset_index(drop=True)
        return contexts, ranking_df


class WeinsteinStage2Screener:
    def __init__(self, *, market: str = "us") -> None:
        self.market = market_key(market)
        ensure_market_dirs(self.market)
        self.results_dir = get_weinstein_stage2_results_dir(self.market)
        ensure_dir(self.results_dir)
        self.analyzer = WeinsteinStage2Analyzer()

    def _derive_actual_data_calibration(self, results_df: pd.DataFrame) -> dict[str, float]:
        calibration = {
            "pre_stage2_high_max_pct": 1.0,
            "pre_stage2_medium_max_pct": 2.0,
            "pre_stage2_low_max_pct": 5.0,
        }
        if results_df.empty or "percent_to_stage2" not in results_df.columns:
            return calibration

        base_pool = results_df[
            (results_df["stock_stage"] == "STAGE_1")
            & pd.to_numeric(results_df["percent_to_stage2"], errors="coerce").notna()
            & (pd.to_numeric(results_df["percent_to_stage2"], errors="coerce") >= 0.0)
        ].copy()
        if base_pool.empty:
            return calibration

        calibration["pre_stage2_high_max_pct"] = bounded_quantile_value(
            base_pool["percent_to_stage2"],
            0.20,
            calibration["pre_stage2_high_max_pct"],
            lower=0.5,
            upper=2.0,
            positive_only=True,
        )
        calibration["pre_stage2_medium_max_pct"] = bounded_quantile_value(
            base_pool["percent_to_stage2"],
            0.40,
            calibration["pre_stage2_medium_max_pct"],
            lower=max(calibration["pre_stage2_high_max_pct"] + 0.3, 1.2),
            upper=4.0,
            positive_only=True,
        )
        calibration["pre_stage2_low_max_pct"] = bounded_quantile_value(
            base_pool["percent_to_stage2"],
            0.70,
            calibration["pre_stage2_low_max_pct"],
            lower=max(calibration["pre_stage2_medium_max_pct"] + 0.5, 2.0),
            upper=8.0,
            positive_only=True,
        )
        calibration["pre_stage2_medium_max_pct"] = max(
            calibration["pre_stage2_medium_max_pct"],
            calibration["pre_stage2_high_max_pct"] + 0.3,
        )
        calibration["pre_stage2_low_max_pct"] = max(
            calibration["pre_stage2_low_max_pct"],
            calibration["pre_stage2_medium_max_pct"] + 0.5,
        )
        return calibration

    def _apply_actual_data_calibration(
        self,
        results_df: pd.DataFrame,
        calibration: dict[str, float],
    ) -> pd.DataFrame:
        if results_df.empty:
            return results_df

        table = results_df.copy()
        stage1_mask = (
            table["stock_stage"].astype(str) == "STAGE_1"
        ) & table["timing_state"].astype(str).isin(["BASE", "PRE_STAGE2_HIGH", "PRE_STAGE2_MEDIUM", "PRE_STAGE2_LOW"])

        for index in table[stage1_mask].index:
            percent_to_stage2 = _safe_float(table.at[index, "percent_to_stage2"])
            if percent_to_stage2 is None:
                table.at[index, "timing_state"] = "BASE"
                table.at[index, "rejection_reason"] = "base_not_close_enough_to_breakout"
                continue
            if percent_to_stage2 <= calibration["pre_stage2_high_max_pct"]:
                table.at[index, "timing_state"] = "PRE_STAGE2_HIGH"
                table.at[index, "rejection_reason"] = ""
            elif percent_to_stage2 <= calibration["pre_stage2_medium_max_pct"]:
                table.at[index, "timing_state"] = "PRE_STAGE2_MEDIUM"
                table.at[index, "rejection_reason"] = ""
            elif percent_to_stage2 <= calibration["pre_stage2_low_max_pct"]:
                table.at[index, "timing_state"] = "PRE_STAGE2_LOW"
                table.at[index, "rejection_reason"] = ""
            else:
                table.at[index, "timing_state"] = "BASE"
                table.at[index, "rejection_reason"] = "base_not_close_enough_to_breakout"
        return table

    def _load_metadata(self) -> pd.DataFrame:
        metadata_path = get_stock_metadata_path(self.market)
        if not os.path.exists(metadata_path):
            return pd.DataFrame()
        frame = pd.read_csv(metadata_path)
        if frame.empty or "symbol" not in frame.columns:
            return pd.DataFrame()
        frame["symbol"] = frame["symbol"].astype(str).str.upper()
        for column in ("sector", "industry", "exchange"):
            if column in frame.columns:
                frame[column] = frame[column].fillna("").astype(str)
        return frame

    def _load_daily_frames(self) -> dict[str, pd.DataFrame]:
        data_dir = get_market_data_dir(self.market)
        if not os.path.isdir(data_dir):
            return {}
        frames: dict[str, pd.DataFrame] = {}
        candidate_files = [name for name in sorted(os.listdir(data_dir)) if name.endswith(".csv")]
        interval = progress_interval(len(candidate_files), target_updates=8, min_interval=50)
        print(f"[Weinstein] Frame load started ({self.market}) - files={len(candidate_files)}")
        for index, name in enumerate(candidate_files, start=1):
            symbol = os.path.splitext(name)[0].strip().upper()
            if not symbol or is_index_symbol(self.market, symbol):
                if is_progress_tick(index, len(candidate_files), interval):
                    print(
                        f"[Weinstein] Frame load progress ({self.market}) - "
                        f"processed={index}/{len(candidate_files)}, loaded={len(frames)}"
                    )
                continue
            frame = load_local_ohlcv_frame(self.market, symbol, price_policy=PricePolicy.SPLIT_ADJUSTED)
            if not frame.empty:
                frames[symbol] = frame
            if is_progress_tick(index, len(candidate_files), interval):
                print(
                    f"[Weinstein] Frame load progress ({self.market}) - "
                    f"processed={index}/{len(candidate_files)}, loaded={len(frames)}"
                )
        return frames

    def _persist_outputs(
        self,
        results_df: pd.DataFrame,
        group_rankings: pd.DataFrame,
        market_summary: dict[str, Any],
        actual_data_calibration: dict[str, float],
    ) -> None:
        structural_pool = pd.DataFrame()
        pattern_included = pd.DataFrame()
        if not results_df.empty and {"stock_stage", "rp", "mrs", "rs_proxy", "timing_state", "early_stage2_score", "symbol"}.issubset(results_df.columns):
            structural_pool = results_df[
                results_df["stock_stage"].isin(["STAGE_1", "STAGE_2", "STAGE_2A", "STAGE_2B"])
                & (
                    (pd.to_numeric(results_df["rp"], errors="coerce").fillna(0.0) > 0.0)
                    | (pd.to_numeric(results_df["mrs"], errors="coerce").fillna(0.0) > 0.0)
                    | (pd.to_numeric(results_df["rs_proxy"], errors="coerce").fillna(0.0) > 0.0)
                )
            ].copy()
            if not structural_pool.empty:
                structural_pool = structural_pool.sort_values(
                    ["early_stage2_score", "timing_state", "symbol"],
                    ascending=[False, True, True],
                ).reset_index(drop=True)
            pattern_included = results_df[~results_df["timing_state"].isin(["EXCLUDE", "FAIL"])].copy()
            if not pattern_included.empty:
                pattern_included = pattern_included.sort_values(
                    ["early_stage2_score", "timing_state", "symbol"],
                    ascending=[False, True, True],
                ).reset_index(drop=True)
        outputs = {
            "all_results": results_df,
            "pattern_excluded_pool": structural_pool,
            "pattern_included_candidates": pattern_included,
            "primary_candidates": results_df[results_df["timing_state"].isin(
                ["PRE_STAGE2_HIGH", "PRE_STAGE2_MEDIUM", "BREAKOUT_WEEK", "FRESH_STAGE2_W1", "FRESH_STAGE2_W2"]
            )].copy(),
            "secondary_candidates": results_df[results_df["timing_state"].isin(["PRE_STAGE2_LOW", "RETEST_B"])].copy(),
            "pre_stage2_candidates": results_df[results_df["timing_state"].isin(
                ["PRE_STAGE2_HIGH", "PRE_STAGE2_MEDIUM", "PRE_STAGE2_LOW"]
            )].copy(),
            "breakout_week_candidates": results_df[results_df["timing_state"] == "BREAKOUT_WEEK"].copy(),
            "fresh_stage2_candidates": results_df[results_df["timing_state"].isin(
                ["FRESH_STAGE2_W1", "FRESH_STAGE2_W2"]
            )].copy(),
            "retest_candidates": results_df[results_df["timing_state"] == "RETEST_B"].copy(),
            "late_stage2_excluded": results_df[results_df["timing_state"] == "TOO_LATE_FOR_PRIMARY"].copy(),
            "rejected_candidates_with_reason": results_df[results_df["timing_state"].isin(["EXCLUDE", "FAIL", "BASE"])].copy(),
            "group_rankings": group_rankings,
        }
        for stem, frame in outputs.items():
            csv_path = os.path.join(self.results_dir, f"{stem}.csv")
            json_path = os.path.join(self.results_dir, f"{stem}.json")
            frame.to_csv(csv_path, index=False)
            frame.to_json(json_path, orient="records", indent=2, force_ascii=False)

        summary_path = os.path.join(self.results_dir, "market_summary.json")
        with open(summary_path, "w", encoding="utf-8") as handle:
            json.dump(market_summary, handle, ensure_ascii=False, indent=2)
        with open(os.path.join(self.results_dir, "actual_data_calibration.json"), "w", encoding="utf-8") as handle:
            json.dump(actual_data_calibration, handle, ensure_ascii=False, indent=2)

    def run(self) -> pd.DataFrame:
        metadata = self._load_metadata()
        exchange_map = series_to_str_text_dict(metadata.set_index("symbol")["exchange"]) if "exchange" in metadata.columns else {}
        sector_map = series_to_str_text_dict(metadata.set_index("symbol")["sector"]) if "sector" in metadata.columns else {}
        industry_map = series_to_str_text_dict(metadata.set_index("symbol")["industry"]) if "industry" in metadata.columns else {}
        print(
            f"[Weinstein] Metadata loaded ({self.market}) - "
            f"rows={len(metadata)}, has_exchange={'exchange' in metadata.columns}"
        )

        frames = self._load_daily_frames()
        print(f"[Weinstein] Daily frames loaded ({self.market}) - symbols={len(frames)}")
        benchmark_symbol, benchmark_daily = load_benchmark_data(
            self.market,
            get_benchmark_candidates(self.market),
            allow_yfinance_fallback=True,
            price_policy=PricePolicy.SPLIT_ADJUSTED,
        )
        benchmark_symbol = benchmark_symbol or get_primary_benchmark_symbol(self.market)
        print(f"[Weinstein] Market context build started ({self.market}) - benchmark={benchmark_symbol}")
        market_context = self.analyzer.compute_market_context(
            market=self.market,
            benchmark_symbol=benchmark_symbol,
            benchmark_daily=benchmark_daily,
            daily_frames=frames,
        )
        group_contexts, group_rankings = self.analyzer.compute_group_contexts(
            market=self.market,
            daily_frames=frames,
            benchmark_daily=benchmark_daily,
            sector_map=sector_map,
        )

        rows: list[dict[str, Any]] = []
        total_symbols = len(frames)
        interval = progress_interval(total_symbols, target_updates=8, min_interval=50)
        print(f"[Weinstein] Symbol analysis started ({self.market}) - symbols={total_symbols}")
        for index, (symbol, frame) in enumerate(frames.items(), start=1):
            sector = str(sector_map.get(symbol, "") or "")
            rows.append(
                self.analyzer.analyze_symbol(
                    symbol=symbol,
                    market=self.market,
                    daily_frame=frame,
                    benchmark_symbol=benchmark_symbol,
                    benchmark_daily=benchmark_daily,
                    market_context=market_context,
                    group_context=group_contexts.get(sector),
                    exchange=str(exchange_map.get(symbol, "") or ""),
                    sector=sector,
                    industry_group=str(industry_map.get(symbol, "") or ""),
                )
            )
            if is_progress_tick(index, total_symbols, interval):
                print(
                    f"[Weinstein] Symbol analysis progress ({self.market}) - "
                    f"processed={index}/{total_symbols}, rows={len(rows)}"
                )

        results_df = pd.DataFrame(rows)
        actual_data_calibration = self._derive_actual_data_calibration(results_df)
        results_df = self._apply_actual_data_calibration(results_df, actual_data_calibration)
        if not results_df.empty:
            phase_map = {
                "BASE": "FORMING",
                "PRE_STAGE2_HIGH": "FORMING",
                "PRE_STAGE2_MEDIUM": "FORMING",
                "PRE_STAGE2_LOW": "FORMING",
                "BREAKOUT_WEEK": "RECENT_BREAKOUT",
                "FRESH_STAGE2_W1": "RECENT_BREAKOUT",
                "FRESH_STAGE2_W2": "RECENT_BREAKOUT",
                "RETEST_B": "RECENT_BREAKOUT",
                "TOO_LATE_FOR_PRIMARY": "COMPLETED_NOT_FRESH",
                "FAIL": "FAILED",
                "EXCLUDE": "NONE",
            }
            results_df["phase_bucket"] = results_df["timing_state"].map(phase_map).fillna("NONE")
            timing_rank = {
                "BREAKOUT_WEEK": 0,
                "PRE_STAGE2_HIGH": 1,
                "FRESH_STAGE2_W1": 2,
                "PRE_STAGE2_MEDIUM": 3,
                "FRESH_STAGE2_W2": 4,
                "PRE_STAGE2_LOW": 5,
                "RETEST_B": 6,
                "BASE": 7,
                "TOO_LATE_FOR_PRIMARY": 8,
                "FAIL": 9,
                "EXCLUDE": 10,
            }
            results_df["timing_rank"] = results_df["timing_state"].map(timing_rank).fillna(999)
            results_df = results_df.sort_values(
                ["timing_rank", "early_stage2_score", "confidence_penalty_points", "symbol"],
                ascending=[True, False, True, True],
            ).drop(columns=["timing_rank"]).reset_index(drop=True)

        market_summary = {
            "market": self.market,
            "benchmark_symbol": benchmark_symbol,
            "market_state": market_context.market_state,
            "breadth150_market": market_context.breadth150_market,
            "benchmark_close": market_context.benchmark_close,
            "benchmark_ma30w": market_context.ma30w,
            "benchmark_ma30w_slope_4w": market_context.ma30w_slope_4w,
            "assumption_flags": list(market_context.assumption_flags),
            "actual_data_calibration": actual_data_calibration,
            "symbol_count": int(len(results_df)),
            "state_counts": series_value_counts_to_int_dict(results_df["timing_state"]) if not results_df.empty else {},
        }
        self._persist_outputs(results_df, group_rankings, market_summary, actual_data_calibration)
        print(
            f"[Weinstein] Outputs saved ({self.market}) - "
            f"results={len(results_df)}, primary={int(market_summary['state_counts'].get('PRE_STAGE2_HIGH', 0)) + int(market_summary['state_counts'].get('PRE_STAGE2_MEDIUM', 0))}"
        )
        return results_df


def run_weinstein_stage2_screening(*, market: str = "us") -> pd.DataFrame:
    return WeinsteinStage2Screener(market=market).run()
