from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Sequence, cast

import numpy as np
import pandas as pd

from utils.indicator_helpers import (
    normalize_indicator_frame,
    rolling_atr,
    rolling_average_volume,
    rolling_max,
    rolling_min,
    rolling_sma,
    rolling_traded_value,
    rolling_traded_value_median,
)
from utils.market_data_contract import PricePolicy

logger = logging.getLogger(__name__)


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


def _safe_divide(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _frame_value_as_float(frame: pd.DataFrame, index: Any, column: str) -> float | None:
    try:
        value = frame.at[index, column]
    except Exception:
        return None
    return _safe_float(value)


def _mean_score(values: Sequence[float | bool | None], *, default: float = 0.0) -> float:
    active = [float(value) for value in values if value is not None]
    if not active:
        return default
    return float(np.mean(active))


def _series_median(series: pd.Series) -> float | None:
    if series is None or series.empty:
        return None
    value = pd.to_numeric(series, errors="coerce").dropna().median()
    return _safe_float(value)


def _date_str(value: Any) -> str | None:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%d")


@dataclass(frozen=True)
class PivotPoint:
    index: int
    date: str | None
    pivot_type: str
    pivot_price: float
    prominence_pct: float
    source_window: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "pivot_index": int(self.index),
            "pivot_date": self.date,
            "pivot_type": self.pivot_type,
            "pivot_price": round(self.pivot_price, 4),
            "prominence_pct": round(self.prominence_pct, 4),
            "source_window": int(self.source_window),
        }


@dataclass
class DimensionalScores:
    technical_quality: float = 0.0
    volume_confirmation: float = 0.0
    temporal_validity: float = 0.0
    market_context: float = 0.0

    def to_dict(self) -> dict[str, float]:
        return {
            "technical_quality": round(float(self.technical_quality), 4),
            "volume_confirmation": round(float(self.volume_confirmation), 4),
            "temporal_validity": round(float(self.temporal_validity), 4),
            "market_context": round(float(self.market_context), 4),
        }


@dataclass
class PatternCandidate:
    pattern_type: str
    state_detail: str
    state_bucket: str
    detected: bool
    confidence: float
    confidence_level: str
    pattern_start: str | None = None
    pattern_end: str | None = None
    pivot_price: float | None = None
    invalidation_price: float | None = None
    breakout_date: str | None = None
    breakout_price: float | None = None
    breakout_volume: float | None = None
    volume_multiple: float | None = None
    distance_to_pivot_pct: float | None = None
    extended: bool = False
    dimensional_scores: dict[str, float] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    pivots: list[dict[str, Any]] = field(default_factory=list)

    def to_output(self) -> dict[str, Any]:
        return {
            "pattern_type": self.pattern_type,
            "detected": bool(self.detected),
            "state_detail": self.state_detail,
            "state_bucket": self.state_bucket,
            "confidence": round(float(self.confidence), 3),
            "confidence_level": self.confidence_level,
            "pattern_start": self.pattern_start,
            "pattern_end": self.pattern_end,
            "pivot_price": _safe_float(self.pivot_price),
            "invalidation_price": _safe_float(self.invalidation_price),
            "breakout_date": self.breakout_date,
            "breakout_price": _safe_float(self.breakout_price),
            "breakout_volume": _safe_float(self.breakout_volume),
            "volume_multiple": _safe_float(self.volume_multiple),
            "distance_to_pivot_pct": _safe_float(self.distance_to_pivot_pct),
            "extended": bool(self.extended),
            "dimensional_scores": self.dimensional_scores,
            "metrics": self.metrics,
            "pivots": self.pivots,
        }


class EnhancedPatternAnalyzer:
    """Rule-based pattern analyzer rebuilt around structural pivot logic."""

    def __init__(self) -> None:
        self.MIN_PATTERN_BARS = 220
        self.DETECTION_THRESHOLD = 0.58
        self.HIGH_CONFIDENCE_THRESHOLD = 0.8
        self.DIMENSION_WEIGHTS = {
            "technical_quality": 0.38,
            "volume_confirmation": 0.25,
            "temporal_validity": 0.2,
            "market_context": 0.17,
        }
        self.EXTREMA_RADIUS = 3
        self.BREAKOUT_EPS = 0.003
        self.BREAKOUT_VOL_MULT = 1.4

    def _empty_pattern_output(self, pattern_type: str) -> dict[str, Any]:
        return PatternCandidate(
            pattern_type=pattern_type,
            detected=False,
            state_detail="NONE",
            state_bucket="NONE",
            confidence=0.0,
            confidence_level="None",
            dimensional_scores=DimensionalScores().to_dict(),
        ).to_output()

    def _prepare_ohlcv(self, stock_data: pd.DataFrame) -> pd.DataFrame:
        if stock_data is None or stock_data.empty:
            return pd.DataFrame()

        df = normalize_indicator_frame(
            stock_data,
            price_policy=PricePolicy.SPLIT_ADJUSTED,
            utc_dates=True,
        )
        if df.empty:
            return df

        df["tr"] = rolling_atr(df, 1, min_periods=1)
        df["atr14"] = rolling_atr(df, 14, min_periods=3)
        df["atr20"] = rolling_atr(df, 20, min_periods=5)
        df["natr10"] = rolling_atr(df, 10, min_periods=3) / df["close"].replace(0, np.nan)
        df["natr20"] = df["atr20"] / df["close"].replace(0, np.nan)
        df["range_pct"] = (df["high"] - df["low"]) / df["close"].replace(0, np.nan)
        df["vol_ma10"] = rolling_average_volume(df, 10, min_periods=3)
        df["vol_ma20"] = rolling_average_volume(df, 20, min_periods=5)
        df["vol_ma50"] = rolling_average_volume(df, 50, min_periods=10)
        df["sma20"] = rolling_sma(df["close"], 20, min_periods=5)
        df["sma50"] = rolling_sma(df["close"], 50, min_periods=10)
        df["sma150"] = rolling_sma(df["close"], 150, min_periods=30)
        df["sma200"] = rolling_sma(df["close"], 200, min_periods=50)
        df["rolling_high_252"] = rolling_max(df["high"], 252, min_periods=30)
        df["rolling_low_252"] = rolling_min(df["low"], 252, min_periods=30)
        df["traded_value"] = rolling_traded_value(df, 1, min_periods=1)
        df["tv_ma20"] = rolling_traded_value(df, 20, min_periods=10)
        df["tv_median20"] = rolling_traded_value_median(df, 20, min_periods=10)
        df["smoothed_close"] = df["close"].rolling(5, center=True, min_periods=1).mean()
        return df

    def evaluate_prerequisites(self, stock_data: pd.DataFrame) -> dict[str, Any]:
        df = self._prepare_ohlcv(stock_data)
        if df.empty:
            return {
                "bars": 0,
                "tv_median20": None,
                "recent_data_quality_pass": False,
                "trend_template_lite_pass": False,
            }

        latest = df.iloc[-1]
        recent_window = df.tail(min(60, len(df))).copy()
        recent_data_quality_pass = bool(
            not recent_window.empty
            and pd.to_numeric(recent_window["volume"], errors="coerce").fillna(0.0).gt(0).all()
        )
        close = _safe_float(latest.get("close"))
        sma50 = _safe_float(latest.get("sma50"))
        sma150 = _safe_float(latest.get("sma150"))
        sma200 = _safe_float(latest.get("sma200"))
        prev_sma200 = _safe_float(df["sma200"].iloc[-21]) if len(df) >= 220 else None
        rolling_high_252 = _safe_float(latest.get("rolling_high_252"))
        rolling_low_252 = _safe_float(latest.get("rolling_low_252"))
        has_52w_bounds = (
            rolling_high_252 is not None
            and rolling_high_252 != 0
            and rolling_low_252 is not None
            and rolling_low_252 != 0
        )
        trend_template_lite_pass = bool(
            close is not None
            and sma50 is not None
            and sma150 is not None
            and sma200 is not None
            and close > sma50
            and close > sma150
            and close > sma200
            and sma50 > sma150 >= sma200
            and prev_sma200 is not None
            and sma200 > prev_sma200
            and has_52w_bounds
            and rolling_high_252 is not None
            and rolling_low_252 is not None
            and close >= rolling_high_252 * 0.75
            and close >= rolling_low_252 * 1.25
        )
        return {
            "bars": int(len(df)),
            "tv_median20": _safe_float(latest.get("tv_median20")),
            "recent_data_quality_pass": recent_data_quality_pass,
            "trend_template_lite_pass": trend_template_lite_pass,
        }

    def _normalize_score(self, score: float) -> float:
        if np.isnan(score) or np.isinf(score):
            return 0.0
        return float(min(max(score, 0.0), 1.0))

    def _range_score(
        self,
        value: float | None,
        ideal_low: float,
        ideal_high: float,
        min_low: float,
        max_high: float,
    ) -> float:
        if value is None:
            return 0.0
        if ideal_low <= value <= ideal_high:
            return 1.0
        if value < min_low or value > max_high:
            return 0.0
        if value < ideal_low:
            denom = ideal_low - min_low
            return self._normalize_score((value - min_low) / denom) if denom > 0 else 0.0
        denom = max_high - ideal_high
        return self._normalize_score((max_high - value) / denom) if denom > 0 else 0.0

    def _inverse_score(self, value: float | None, good_max: float, bad_max: float) -> float:
        if value is None:
            return 0.0
        if value <= good_max:
            return 1.0
        if value >= bad_max:
            return 0.0
        denom = bad_max - good_max
        return self._normalize_score((bad_max - value) / denom) if denom > 0 else 0.0

    def _ratio_score(self, value: float | None, good_min: float, bad_min: float) -> float:
        if value is None:
            return 0.0
        if value >= good_min:
            return 1.0
        if value <= bad_min:
            return 0.0
        denom = good_min - bad_min
        return self._normalize_score((value - bad_min) / denom) if denom > 0 else 0.0

    def _prominence_floor(self, df: pd.DataFrame, idx: int) -> float:
        natr20 = _safe_float(df.loc[idx, "natr20"]) if idx in df.index else None
        return max(0.04, 1.5 * (natr20 or 0.0))

    def _choose_more_extreme(self, left: PivotPoint, right: PivotPoint) -> PivotPoint:
        if left.pivot_type != right.pivot_type:
            return left if left.prominence_pct >= right.prominence_pct else right
        if left.pivot_type == "H":
            return left if left.pivot_price >= right.pivot_price else right
        return left if left.pivot_price <= right.pivot_price else right

    def _merge_pivots(self, pivots: list[PivotPoint]) -> list[PivotPoint]:
        if not pivots:
            return []

        ordered = sorted(
            pivots,
            key=lambda pivot: (pivot.index, pivot.pivot_type, -pivot.prominence_pct, pivot.source_window),
        )
        deduped: list[PivotPoint] = []
        for pivot in ordered:
            if deduped and pivot.index == deduped[-1].index and pivot.pivot_type == deduped[-1].pivot_type:
                if pivot.prominence_pct > deduped[-1].prominence_pct:
                    deduped[-1] = pivot
                continue
            deduped.append(pivot)

        merged: list[PivotPoint] = []
        for pivot in deduped:
            if not merged:
                merged.append(pivot)
                continue
            last = merged[-1]
            if pivot.index - last.index <= 2:
                merged[-1] = self._choose_more_extreme(last, pivot)
                continue
            if pivot.pivot_type == last.pivot_type:
                merged[-1] = self._choose_more_extreme(last, pivot)
                continue
            price_gap = abs(pivot.pivot_price - last.pivot_price) / max(pivot.pivot_price, last.pivot_price, 1e-9)
            if price_gap < min(last.prominence_pct, pivot.prominence_pct) * 0.35:
                if pivot.prominence_pct > last.prominence_pct:
                    merged[-1] = pivot
                continue
            merged.append(pivot)
        return merged

    def _append_recent_endpoint_pivots(
        self,
        df: pd.DataFrame,
        pivots: list[PivotPoint],
        radius: int,
    ) -> list[PivotPoint]:
        if df.empty:
            return pivots

        recent = df.iloc[max(0, len(df) - (radius + 6)) :].copy()
        if recent.empty:
            return pivots

        candidates = list(pivots)
        endpoint_specs = [
            ("H", int(recent["high"].idxmax()), float(recent["high"].max()), float(recent["low"].min())),
            ("L", int(recent["low"].idxmin()), float(recent["low"].min()), float(recent["high"].max())),
        ]
        for pivot_type, idx, price, opposite_price in endpoint_specs:
            if any(existing.pivot_type == pivot_type and abs(existing.index - idx) <= 2 for existing in pivots):
                continue
            prominence = (
                (price - opposite_price) / max(price, 1e-9)
                if pivot_type == "H"
                else (opposite_price - price) / max(opposite_price, 1e-9)
            )
            if prominence < max(0.03, self._prominence_floor(df, idx) * 0.8):
                continue
            candidates.append(
                PivotPoint(
                    index=idx,
                    date=_date_str(df.loc[idx, "date"]),
                    pivot_type=pivot_type,
                    pivot_price=price,
                    prominence_pct=prominence,
                    source_window=2 * radius + 1,
                )
            )
        return candidates

    def _extract_pivots(self, df: pd.DataFrame) -> list[PivotPoint]:
        if df.empty or len(df) < (2 * self.EXTREMA_RADIUS + 3):
            return []

        smoothed = df["smoothed_close"].to_numpy(dtype=float)
        candidates: list[PivotPoint] = []
        for idx in range(self.EXTREMA_RADIUS, len(df) - self.EXTREMA_RADIUS):
            segment = smoothed[idx - self.EXTREMA_RADIUS : idx + self.EXTREMA_RADIUS + 1]
            if np.isnan(segment).any():
                continue

            center = segment[self.EXTREMA_RADIUS]
            left = segment[: self.EXTREMA_RADIUS]
            right = segment[self.EXTREMA_RADIUS + 1 :]
            local_window = df.iloc[idx - self.EXTREMA_RADIUS : idx + self.EXTREMA_RADIUS + 1]

            if center > np.nanmax(left) and center >= np.nanmax(right):
                raw_idx = int(local_window["high"].idxmax())
                price = _frame_value_as_float(df, raw_idx, "high")
                low_min = _safe_float(local_window["low"].min())
                prominence = _safe_divide((price - low_min) if price is not None and low_min is not None else None, price)
                if price is None or prominence is None:
                    continue
                if prominence >= self._prominence_floor(df, raw_idx):
                    candidates.append(
                        PivotPoint(
                            index=raw_idx,
                            date=_date_str(df.loc[raw_idx, "date"]),
                            pivot_type="H",
                            pivot_price=price,
                            prominence_pct=prominence,
                            source_window=2 * self.EXTREMA_RADIUS + 1,
                        )
                    )

            if center < np.nanmin(left) and center <= np.nanmin(right):
                raw_idx = int(local_window["low"].idxmin())
                price = _frame_value_as_float(df, raw_idx, "low")
                high_max = _safe_float(local_window["high"].max())
                prominence = _safe_divide((high_max - price) if price is not None and high_max is not None else None, high_max)
                if price is None or prominence is None:
                    continue
                if prominence >= self._prominence_floor(df, raw_idx):
                    candidates.append(
                        PivotPoint(
                            index=raw_idx,
                            date=_date_str(df.loc[raw_idx, "date"]),
                            pivot_type="L",
                            pivot_price=price,
                            prominence_pct=prominence,
                            source_window=2 * self.EXTREMA_RADIUS + 1,
                        )
                    )

        merged = self._merge_pivots(candidates)
        merged = self._append_recent_endpoint_pivots(df, merged, self.EXTREMA_RADIUS)
        return self._merge_pivots(merged)

    def _prior_run_up_pct(self, df: pd.DataFrame, start_idx: int) -> float | None:
        if start_idx <= 5:
            return None
        left = max(0, start_idx - 120)
        prior_window = df.iloc[left:start_idx]
        if prior_window.empty:
            return None
        prior_low = _safe_float(prior_window["low"].min())
        start_high = _safe_float(df.loc[start_idx, "high"])
        ret_63d = None
        if start_idx >= 63:
            base_close = _safe_float(df.loc[start_idx - 63, "close"])
            start_close = _safe_float(df.loc[start_idx, "close"])
            if base_close and start_close and base_close > 0:
                ret_63d = (start_close / base_close) - 1.0
        run_up = None
        if prior_low and start_high and prior_low > 0:
            run_up = (start_high / prior_low) - 1.0
        if run_up is None:
            return ret_63d
        if ret_63d is None:
            return run_up
        return max(run_up, ret_63d)

    def _recent_tightness_pct(self, df: pd.DataFrame, pivot_price: float, bars: int = 5) -> float | None:
        if df.empty or pivot_price <= 0:
            return None
        tail = df.tail(min(bars, len(df)))
        if tail.empty:
            return None
        return (float(tail["high"].max()) - float(tail["low"].min())) / pivot_price

    def _volume_base(self, row: pd.Series) -> float:
        for column in ("vol_ma50", "vol_ma20", "vol_ma10", "volume"):
            value = _safe_float(row.get(column))
            if value and value > 0:
                return value
        return 1.0

    def _check_breakout_last_rows(
        self,
        df: pd.DataFrame,
        pivot_price: float | None,
        invalidation_price: float | None,
        last_n: int = 5,
    ) -> dict[str, Any]:
        empty_result = {
            "breakout_found": False,
            "valid_recent": False,
            "breakout_date": None,
            "breakout_price": None,
            "breakout_volume": None,
            "volume_multiple": None,
            "volume_score": 0.0,
            "extended": False,
        }
        if pivot_price is None or pivot_price <= 0 or df.empty:
            return empty_result

        tail = df.tail(min(last_n, len(df))).copy()
        if tail.empty:
            return empty_result

        base_volume = tail.apply(self._volume_base, axis=1)
        price_pass = tail["close"] >= pivot_price * (1.0 + self.BREAKOUT_EPS)
        aggressive_pass = (tail["high"] >= pivot_price * (1.0 + self.BREAKOUT_EPS)) & (tail["close"] >= pivot_price)
        volume_pass = tail["volume"] >= self.BREAKOUT_VOL_MULT * base_volume
        hits = tail[(price_pass | aggressive_pass) & volume_pass]
        if hits.empty:
            return empty_result

        breakout_idx = int(hits.index[0])
        breakout_row = cast(pd.Series, df.loc[breakout_idx])
        breakout_volume = _safe_float(breakout_row["volume"])
        base_vol = self._volume_base(breakout_row)
        volume_multiple = _safe_divide(breakout_volume, base_vol)

        post_breakout = df.iloc[breakout_idx:]
        latest_close = float(df["close"].iloc[-1])
        highest_after_breakout = float(post_breakout["high"].max()) if not post_breakout.empty else latest_close
        pullback_pct = (
            (highest_after_breakout - latest_close) / highest_after_breakout
            if highest_after_breakout > 0
            else 0.0
        )
        held_above_pivot = latest_close >= pivot_price * 0.97
        not_failed = invalidation_price is None or latest_close >= invalidation_price * 0.98
        valid_recent = held_above_pivot and pullback_pct <= 0.08 and not_failed

        return {
            "breakout_found": True,
            "valid_recent": valid_recent,
            "breakout_date": _date_str(breakout_row["date"]),
            "breakout_price": _safe_float(breakout_row["close"]),
            "breakout_volume": breakout_volume,
            "volume_multiple": volume_multiple,
            "volume_score": self._range_score(volume_multiple, 1.4, 3.5, 1.0, 5.0),
            "extended": latest_close >= pivot_price * 1.10,
        }

    def _confidence_level(self, confidence: float) -> str:
        if confidence >= self.HIGH_CONFIDENCE_THRESHOLD:
            return "High"
        if confidence >= self.DETECTION_THRESHOLD:
            return "Medium"
        if confidence >= 0.4:
            return "Low"
        return "None"

    def _state_bucket(self, detail: str) -> str:
        if detail.startswith("BREAKOUT"):
            return "BROKEOUT_RECENT"
        if detail.startswith("COMPLETED") or detail.startswith("READY"):
            return "COMPLETED"
        if detail.startswith("FORMING"):
            return "FORMING"
        if detail.startswith("FAILED"):
            return "FAILED"
        if detail.startswith("STALE"):
            return "STALE"
        return "NONE"

    def _build_candidate(
        self,
        pattern_type: str,
        state_detail: str,
        confidence: float,
        dimensional_scores: DimensionalScores,
        *,
        pattern_start: str | None,
        pattern_end: str | None,
        pivot_price: float | None,
        invalidation_price: float | None,
        breakout: dict[str, Any],
        distance_to_pivot_pct: float | None,
        metrics: dict[str, Any],
        pivots: list[PivotPoint],
    ) -> PatternCandidate:
        state_bucket = self._state_bucket(state_detail)
        detected = state_bucket not in {"FAILED", "NONE"} and confidence >= self.DETECTION_THRESHOLD
        return PatternCandidate(
            pattern_type=pattern_type,
            state_detail=state_detail,
            state_bucket=state_bucket,
            detected=detected,
            confidence=confidence,
            confidence_level=self._confidence_level(confidence),
            pattern_start=pattern_start,
            pattern_end=pattern_end,
            pivot_price=pivot_price,
            invalidation_price=invalidation_price,
            breakout_date=breakout.get("breakout_date"),
            breakout_price=breakout.get("breakout_price"),
            breakout_volume=breakout.get("breakout_volume"),
            volume_multiple=breakout.get("volume_multiple"),
            distance_to_pivot_pct=distance_to_pivot_pct,
            extended=bool(breakout.get("extended", False)),
            dimensional_scores=dimensional_scores.to_dict(),
            metrics=metrics,
            pivots=[pivot.to_dict() for pivot in pivots],
        )

    def _detect_vcp(self, df: pd.DataFrame, pivots: list[PivotPoint]) -> PatternCandidate | None:
        if len(df) < 40 or len(pivots) < 5:
            return None

        candidates: list[PatternCandidate] = []
        latest_idx = len(df) - 1
        recent_pivots = [pivot for pivot in pivots if pivot.index >= max(0, latest_idx - 130)]
        for end_pos, end_pivot in enumerate(recent_pivots):
            if end_pivot.pivot_type != "H":
                continue
            for contraction_count in range(4, 1, -1):
                start_pos = end_pos - contraction_count * 2
                if start_pos < 0:
                    continue
                sequence = recent_pivots[start_pos : end_pos + 1]
                if len(sequence) != 2 * contraction_count + 1:
                    continue
                expected_types = ["H" if idx % 2 == 0 else "L" for idx in range(len(sequence))]
                if [pivot.pivot_type for pivot in sequence] != expected_types:
                    continue

                highs = sequence[0::2]
                lows = sequence[1::2]
                base_start_idx = highs[0].index
                base_len = latest_idx - base_start_idx + 1
                if base_len < 15 or base_len > 120:
                    continue

                contraction_bars = [low.index - highs[idx].index for idx, low in enumerate(lows)]
                recovery_bars = [highs[idx + 1].index - low.index for idx, low in enumerate(lows)]
                if min(contraction_bars) < 4 or min(recovery_bars) < 3:
                    continue

                depths = []
                for idx, low in enumerate(lows):
                    ref_high = highs[idx].pivot_price
                    depth = (ref_high - low.pivot_price) / max(ref_high, 1e-9)
                    depths.append(depth)
                if depths[0] < 0.06 or depths[0] > 0.45 or max(depths) > 0.5:
                    continue

                decrease_checks = [
                    depths[idx + 1] <= depths[idx] * 0.85 or depths[idx + 1] <= depths[idx] - 0.02
                    for idx in range(len(depths) - 1)
                ]
                higher_lows = [
                    lows[idx + 1].pivot_price >= lows[idx].pivot_price
                    for idx in range(len(lows) - 1)
                ]

                latest_high_is_breakout = (
                    len(highs) >= 3
                    and highs[-1].pivot_price > highs[-2].pivot_price * (1.0 + self.BREAKOUT_EPS)
                )
                if latest_high_is_breakout:
                    pivot_price = highs[-2].pivot_price
                    resistance_highs = highs[-3:-1] if len(highs) >= 4 else [highs[-2]]
                    structure_highs = resistance_highs
                else:
                    resistance_highs = highs[-2:]
                    structure_highs = highs
                    pivot_price = max(pivot.pivot_price for pivot in resistance_highs)
                if not resistance_highs:
                    continue
                resistance_band_pct = (
                    max(pivot.pivot_price for pivot in structure_highs) - min(pivot.pivot_price for pivot in structure_highs)
                ) / max(pivot_price, 1e-9)
                last_two_gap_pct = (
                    abs(resistance_highs[-1].pivot_price - resistance_highs[-2].pivot_price) / max(pivot_price, 1e-9)
                    if len(resistance_highs) >= 2
                    else 0.0
                )
                if resistance_band_pct > 0.1:
                    continue

                base_window = df.iloc[base_start_idx : latest_idx + 1]
                first_ten = base_window.head(min(10, len(base_window)))
                first_half = base_window.head(max(5, len(base_window) // 2))
                last_ten = base_window.tail(min(10, len(base_window)))
                first_range = _series_median(first_ten["range_pct"])
                last_range = _series_median(last_ten["range_pct"])
                range_ratio = _safe_divide(last_range, first_range) if first_range is not None and first_range > 0 else None
                first_natr = _series_median(first_ten["natr10"])
                last_natr = _series_median(last_ten["natr10"])
                natr_ratio = _safe_divide(last_natr, first_natr) if first_natr is not None and first_natr > 0 else None
                first_vol = _series_median(first_half["volume"])
                last_vol = _series_median(last_ten["volume"])
                volume_ratio = _safe_divide(last_vol, first_vol) if first_vol is not None and first_vol > 0 else None
                tightness_pct = self._recent_tightness_pct(df, pivot_price, bars=5)
                prior_run_up_pct = self._prior_run_up_pct(df, base_start_idx)
                latest_close = float(df["close"].iloc[-1])
                invalidation_price = lows[-1].pivot_price
                distance_to_pivot_pct = (latest_close / pivot_price) - 1.0 if pivot_price > 0 else None
                breakout = self._check_breakout_last_rows(df, pivot_price, invalidation_price, last_n=5)

                technical_quality = _mean_score(
                    [
                        self._range_score(depths[0], 0.08, 0.35, 0.05, 0.45),
                        self._inverse_score(depths[-1], 0.12, 0.18),
                        _mean_score(decrease_checks, default=0.0),
                        _mean_score(higher_lows, default=0.5),
                        self._inverse_score(resistance_band_pct, 0.06, 0.10),
                        self._inverse_score(last_two_gap_pct, 0.03, 0.06),
                    ]
                )
                breakout_volume_score = _safe_float(breakout.get("volume_score")) if breakout.get("breakout_found") else None
                volume_confirmation = _mean_score(
                    [
                        self._inverse_score(volume_ratio, 0.8, 1.05),
                        self._inverse_score(natr_ratio, 0.8, 1.05),
                        self._inverse_score(range_ratio, 0.8, 1.05),
                        breakout_volume_score if breakout_volume_score is not None else self._inverse_score(volume_ratio, 0.8, 1.05),
                    ]
                )
                contraction_scores = [self._range_score(bars, 5, 25, 4, 35) for bars in contraction_bars + recovery_bars]
                temporal_validity = _mean_score(
                    [
                        self._range_score(base_len, 20, 80, 15, 120),
                        _mean_score(contraction_scores),
                        self._inverse_score(tightness_pct, 0.08, 0.12),
                    ]
                )
                sma50_last = _safe_float(df["sma50"].iloc[-1])
                rolling_high_252_last = _safe_float(df["rolling_high_252"].iloc[-1])
                market_context = _mean_score(
                    [
                        self._range_score(prior_run_up_pct, 0.25, 1.0, 0.12, 1.5),
                        1.0 if sma50_last is not None and latest_close >= sma50_last else 0.3,
                        1.0 if rolling_high_252_last is not None and latest_close >= rolling_high_252_last * 0.75 else 0.4,
                    ]
                )
                dimensional_scores = DimensionalScores(
                    technical_quality=self._normalize_score(float(technical_quality)),
                    volume_confirmation=self._normalize_score(float(volume_confirmation)),
                    temporal_validity=self._normalize_score(float(temporal_validity)),
                    market_context=self._normalize_score(float(market_context)),
                )
                score_dict = dimensional_scores.to_dict()
                confidence = self._normalize_score(
                    sum(score_dict[dimension] * weight for dimension, weight in self.DIMENSION_WEIGHTS.items())
                )

                structure_complete = (
                    all(decrease_checks)
                    and depths[-1] <= 0.14
                    and last_two_gap_pct <= 0.03
                    and resistance_band_pct <= 0.06
                    and (tightness_pct is not None and tightness_pct <= 0.08)
                    and (natr_ratio is not None and natr_ratio <= 1.0)
                )

                if breakout.get("valid_recent"):
                    state_detail = "BREAKOUT_VCP_RECENT"
                elif latest_close < invalidation_price * 0.98:
                    state_detail = "FAILED_VCP"
                elif structure_complete:
                    state_detail = "COMPLETED_VCP"
                else:
                    state_detail = "FORMING_VCP"

                metrics = {
                    "contractions": contraction_count,
                    "depths_pct": [round(depth, 4) for depth in depths],
                    "contraction_bars": [int(bars) for bars in contraction_bars],
                    "recovery_bars": [int(bars) for bars in recovery_bars],
                    "prior_run_up_pct": _safe_float(prior_run_up_pct),
                    "resistance_band_pct": _safe_float(resistance_band_pct),
                    "last_two_high_gap_pct": _safe_float(last_two_gap_pct),
                    "higher_lows_ratio": round(float(np.mean(higher_lows)), 4) if higher_lows else None,
                    "tightness_pct": _safe_float(tightness_pct),
                    "volume_ratio_last10_vs_first_half": _safe_float(volume_ratio),
                    "natr_ratio_last10_vs_first10": _safe_float(natr_ratio),
                    "range_ratio_last10_vs_first10": _safe_float(range_ratio),
                }
                candidates.append(
                    self._build_candidate(
                        "VCP",
                        state_detail,
                        confidence,
                        dimensional_scores,
                        pattern_start=_date_str(df.loc[base_start_idx, "date"]),
                        pattern_end=_date_str(df.loc[latest_idx, "date"]),
                        pivot_price=pivot_price,
                        invalidation_price=invalidation_price,
                        breakout=breakout,
                        distance_to_pivot_pct=distance_to_pivot_pct,
                        metrics=metrics,
                        pivots=sequence,
                    )
                )

        if not candidates:
            return None
        return max(candidates, key=lambda candidate: (candidate.detected, candidate.confidence))

    def _detect_cup_handle(self, df: pd.DataFrame, pivots: list[PivotPoint]) -> PatternCandidate | None:
        if len(df) < 60:
            return None

        latest_idx = len(df) - 1
        high_pivots = [
            pivot
            for pivot in pivots
            if pivot.pivot_type == "H" and pivot.index >= max(0, latest_idx - 180)
        ]
        recent_low_pivots = [
            pivot
            for pivot in pivots
            if pivot.pivot_type == "L" and pivot.index >= max(0, latest_idx - 35)
        ]
        known_high_indexes = {pivot.index for pivot in high_pivots}
        for low_pivot in recent_low_pivots:
            pre_low_window = df.iloc[max(0, low_pivot.index - 25) : low_pivot.index]
            if len(pre_low_window) < 5:
                continue
            rim_idx = int(pre_low_window["high"].idxmax())
            if rim_idx in known_high_indexes:
                continue
            rim_price = _frame_value_as_float(df, rim_idx, "high")
            low_min = _safe_float(pre_low_window["low"].min())
            prominence = _safe_divide((rim_price - low_min) if rim_price is not None and low_min is not None else None, rim_price)
            if rim_price is None or prominence is None:
                continue
            if prominence < max(0.03, self._prominence_floor(df, rim_idx) * 0.75):
                continue
            high_pivots.append(
                PivotPoint(
                    index=rim_idx,
                    date=_date_str(df.loc[rim_idx, "date"]),
                    pivot_type="H",
                    pivot_price=rim_price,
                    prominence_pct=prominence,
                    source_window=0,
                )
            )
            known_high_indexes.add(rim_idx)

        high_pivots = sorted(high_pivots, key=lambda pivot: pivot.index)
        if len(high_pivots) < 2:
            return None

        candidates: list[PatternCandidate] = []
        for left_pos, left_rim in enumerate(high_pivots[:-1]):
            for right_rim in high_pivots[left_pos + 1 :]:
                cup_len = right_rim.index - left_rim.index
                if cup_len < 30 or cup_len > 160:
                    continue
                if latest_idx - right_rim.index > 25:
                    continue

                cup_window = df.iloc[left_rim.index : right_rim.index + 1]
                if cup_window.empty:
                    continue

                bottom_idx = int(cup_window["low"].idxmin())
                if bottom_idx - left_rim.index < 5 or right_rim.index - bottom_idx < 5:
                    continue

                bottom_price = _frame_value_as_float(df, bottom_idx, "low")
                if bottom_price is None:
                    continue
                rim_level = max(left_rim.pivot_price, right_rim.pivot_price)
                rim_diff_pct = abs(left_rim.pivot_price - right_rim.pivot_price) / max(rim_level, 1e-9)
                cup_depth_pct = (rim_level - bottom_price) / max(rim_level, 1e-9)
                if cup_depth_pct < 0.08 or cup_depth_pct > 0.45 or rim_diff_pct > 0.08:
                    continue

                bottom_threshold = bottom_price + (rim_level - bottom_price) * 0.25
                bottom_zone_width = int((cup_window["close"] <= bottom_threshold).sum())
                left_width = bottom_idx - left_rim.index
                right_width = right_rim.index - bottom_idx
                balance_ratio = min(left_width, right_width) / max(left_width, right_width)
                recovery_ratio = right_rim.pivot_price / max(left_rim.pivot_price, 1e-9)

                after_bottom = df.iloc[bottom_idx + 1 : bottom_idx + 4]
                v_shape_recovery = None
                if not after_bottom.empty:
                    recovery_high = float(after_bottom["high"].max())
                    v_shape_recovery = (recovery_high - bottom_price) / max(rim_level - bottom_price, 1e-9)

                post_rim = df.iloc[right_rim.index + 1 : min(len(df), right_rim.index + 21)]
                state_detail = "FORMING_CUP" if recovery_ratio < 0.93 else "FORMING_HANDLE"
                handle_slice = post_rim.head(0)
                price_only_break_idx = None
                if recovery_ratio >= 0.93 and not post_rim.empty:
                    price_only_break = post_rim[
                        (post_rim["close"] >= right_rim.pivot_price * (1.0 + self.BREAKOUT_EPS))
                        | (
                            (post_rim["high"] >= right_rim.pivot_price * (1.0 + self.BREAKOUT_EPS))
                            & (post_rim["close"] >= right_rim.pivot_price)
                        )
                    ]
                    if not price_only_break.empty:
                        price_only_break_idx = int(price_only_break.index[0])
                        handle_slice = df.iloc[right_rim.index + 1 : price_only_break_idx]
                    else:
                        handle_slice = post_rim

                handle_len = len(handle_slice)
                handle_low_price = None
                handle_depth_pct = None
                handle_volume_ratio = None
                handle_upper_half = None
                handle_vs_advance = None
                pivot_price = right_rim.pivot_price
                invalidation_price = bottom_price
                breakout_context = {
                    "breakout_found": False,
                    "valid_recent": False,
                    "breakout_date": None,
                    "breakout_price": None,
                    "breakout_volume": None,
                    "volume_multiple": None,
                    "volume_score": 0.0,
                    "extended": False,
                }

                if handle_len > 0:
                    pivot_window_end = handle_slice.index[-1] + 1
                    pivot_window = df.iloc[right_rim.index:pivot_window_end]
                    pivot_price = float(pivot_window["high"].max()) if not pivot_window.empty else right_rim.pivot_price
                    handle_low_price = float(handle_slice["low"].min())
                    invalidation_price = handle_low_price
                    handle_depth_pct = (pivot_price - handle_low_price) / max(pivot_price, 1e-9)
                    handle_upper_half = handle_low_price >= (bottom_price + rim_level) / 2.0
                    advance = max(right_rim.pivot_price - bottom_price, 1e-9)
                    handle_vs_advance = (pivot_price - handle_low_price) / advance

                    right_side_window = df.iloc[max(bottom_idx, right_rim.index - 15) : right_rim.index + 1]
                    right_side_volume = _series_median(right_side_window["volume"]) if not right_side_window.empty else None
                    handle_volume = _series_median(handle_slice["volume"])
                    handle_volume_ratio = (
                        handle_volume / right_side_volume
                        if handle_volume is not None and right_side_volume and right_side_volume > 0
                        else None
                    )

                    handle_complete = (
                        5 <= handle_len <= 20
                        and handle_depth_pct <= 0.12
                        and handle_vs_advance <= 0.35
                        and bool(handle_upper_half)
                    )
                    breakout_context = self._check_breakout_last_rows(df, pivot_price, invalidation_price, last_n=5)
                    if breakout_context.get("valid_recent") and handle_complete:
                        state_detail = "BREAKOUT_CWH_RECENT"
                    elif float(df["close"].iloc[-1]) < invalidation_price * 0.98:
                        state_detail = "FAILED_CWH"
                    elif handle_complete:
                        state_detail = "COMPLETED_CWH"
                    else:
                        state_detail = "FORMING_HANDLE"
                elif recovery_ratio >= 0.93:
                    state_detail = "FORMING_HANDLE"

                prior_run_up_pct = self._prior_run_up_pct(df, left_rim.index)
                latest_close = float(df["close"].iloc[-1])
                distance_to_pivot_pct = (latest_close / pivot_price) - 1.0 if pivot_price > 0 else None

                technical_quality = np.mean(
                    [
                        self._range_score(cup_depth_pct, 0.12, 0.35, 0.08, 0.45),
                        self._inverse_score(rim_diff_pct, 0.05, 0.08),
                        self._ratio_score(recovery_ratio, 0.95, 0.85),
                        self._ratio_score(balance_ratio, 0.5, 0.25),
                        self._ratio_score(float(bottom_zone_width) / max(float(cup_len), 1.0), 0.1, 0.04),
                        self._inverse_score(v_shape_recovery, 0.45, 0.75) if v_shape_recovery is not None else 0.5,
                        self._inverse_score(handle_depth_pct, 0.10, 0.16) if handle_depth_pct is not None else 0.35,
                    ]
                )
                right_side_volume_ratio = None
                if not post_rim.empty:
                    cup_volume = _series_median(cup_window["volume"])
                    post_volume = _series_median(post_rim["volume"])
                    if cup_volume and cup_volume > 0 and post_volume is not None:
                        right_side_volume_ratio = post_volume / cup_volume
                volume_confirmation = np.mean(
                    [
                        self._inverse_score(handle_volume_ratio, 0.9, 1.15) if handle_volume_ratio is not None else 0.4,
                        breakout_context.get("volume_score", 0.0)
                        if breakout_context.get("breakout_found")
                        else self._inverse_score(handle_volume_ratio, 0.9, 1.15) if handle_volume_ratio is not None else 0.4,
                        self._inverse_score(right_side_volume_ratio, 0.95, 1.2)
                        if right_side_volume_ratio is not None
                        else 0.4,
                    ]
                )
                temporal_validity = np.mean(
                    [
                        self._range_score(cup_len, 35, 130, 30, 160),
                        self._range_score(handle_len, 5, 20, 3, 25) if handle_len > 0 else 0.35,
                        self._ratio_score(balance_ratio, 0.5, 0.25),
                    ]
                )
                sma50_last = _safe_float(df["sma50"].iloc[-1])
                rolling_high_252_last = _safe_float(df["rolling_high_252"].iloc[-1])
                market_context = _mean_score(
                    [
                        self._range_score(prior_run_up_pct, 0.25, 1.0, 0.12, 1.5),
                        1.0 if sma50_last is not None and latest_close >= sma50_last else 0.3,
                        1.0 if rolling_high_252_last is not None and latest_close >= rolling_high_252_last * 0.75 else 0.4,
                    ]
                )
                dimensional_scores = DimensionalScores(
                    technical_quality=self._normalize_score(float(technical_quality)),
                    volume_confirmation=self._normalize_score(float(volume_confirmation)),
                    temporal_validity=self._normalize_score(float(temporal_validity)),
                    market_context=self._normalize_score(float(market_context)),
                )
                score_dict = dimensional_scores.to_dict()
                confidence = self._normalize_score(
                    sum(score_dict[dimension] * weight for dimension, weight in self.DIMENSION_WEIGHTS.items())
                )

                metrics = {
                    "cup_len": int(cup_len),
                    "cup_depth_pct": _safe_float(cup_depth_pct),
                    "rim_diff_pct": _safe_float(rim_diff_pct),
                    "recovery_ratio": _safe_float(recovery_ratio),
                    "balance_ratio": _safe_float(balance_ratio),
                    "bottom_zone_width": int(bottom_zone_width),
                    "bottom_zone_ratio": _safe_float(float(bottom_zone_width) / max(float(cup_len), 1.0)),
                    "v_shape_recovery_ratio": _safe_float(v_shape_recovery),
                    "handle_len": int(handle_len),
                    "handle_depth_pct": _safe_float(handle_depth_pct),
                    "handle_volume_ratio": _safe_float(handle_volume_ratio),
                    "handle_upper_half": handle_upper_half,
                    "handle_vs_cup_advance": _safe_float(handle_vs_advance),
                    "prior_run_up_pct": _safe_float(prior_run_up_pct),
                    "price_only_break_index": price_only_break_idx,
                }
                pivots_used = [
                    left_rim,
                    PivotPoint(
                        index=bottom_idx,
                        date=_date_str(df.loc[bottom_idx, "date"]),
                        pivot_type="L",
                        pivot_price=bottom_price,
                        prominence_pct=cup_depth_pct,
                        source_window=0,
                    ),
                    right_rim,
                ]
                candidates.append(
                    self._build_candidate(
                        "CUP_HANDLE",
                        state_detail,
                        confidence,
                        dimensional_scores,
                        pattern_start=_date_str(df.loc[left_rim.index, "date"]),
                        pattern_end=_date_str(df.loc[latest_idx, "date"]),
                        pivot_price=pivot_price,
                        invalidation_price=invalidation_price,
                        breakout=breakout_context,
                        distance_to_pivot_pct=distance_to_pivot_pct,
                        metrics=metrics,
                        pivots=pivots_used,
                    )
                )

        if not candidates:
            return None
        return max(candidates, key=lambda candidate: (candidate.detected, candidate.confidence))

    def analyze_patterns_enhanced(self, symbol: str, stock_data: pd.DataFrame) -> dict[str, dict[str, Any]]:
        try:
            df = self._prepare_ohlcv(stock_data)
            if df.empty or len(df) < self.MIN_PATTERN_BARS:
                return {
                    "vcp": self._empty_pattern_output("VCP"),
                    "cup_handle": self._empty_pattern_output("CUP_HANDLE"),
                }

            pivots = self._extract_pivots(df)
            vcp_candidate = self._detect_vcp(df, pivots)
            cup_candidate = self._detect_cup_handle(df, pivots)

            return {
                "vcp": vcp_candidate.to_output() if vcp_candidate else self._empty_pattern_output("VCP"),
                "cup_handle": cup_candidate.to_output() if cup_candidate else self._empty_pattern_output("CUP_HANDLE"),
            }
        except Exception:
            logger.exception("%s: pattern analysis failed", symbol)
            return {
                "vcp": self._empty_pattern_output("VCP"),
                "cup_handle": self._empty_pattern_output("CUP_HANDLE"),
            }
