from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from utils.actual_data_calibration import bounded_quantile_value
from utils.io_utils import ensure_dir
from utils.market_data_contract import load_benchmark_data, load_local_ohlcv_frame
from utils.market_runtime import (
    ensure_market_dirs,
    get_benchmark_candidates,
    get_market_data_dir,
    get_primary_benchmark_symbol,
    get_stock_metadata_path,
    is_index_symbol,
    market_key,
)
from utils.progress_runtime import is_progress_tick, progress_interval


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


def _clamp(value: float | None, low: float = 0.0, high: float = 1.0) -> float:
    if value is None:
        return low
    if np.isnan(value) or np.isinf(value):
        return low
    return float(min(max(value, low), high))


def _score_ratio(value: float | None, good_min: float, bad_min: float) -> float:
    if value is None:
        return 0.0
    if value >= good_min:
        return 1.0
    if value <= bad_min:
        return 0.0
    return _clamp((value - bad_min) / max(good_min - bad_min, 1e-9))


def _score_inverse(value: float | None, good_max: float, bad_max: float) -> float:
    if value is None:
        return 0.0
    if value <= good_max:
        return 1.0
    if value >= bad_max:
        return 0.0
    return _clamp((bad_max - value) / max(bad_max - good_max, 1e-9))


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


def _weighted_mean(items: list[tuple[float | None, float]]) -> float:
    active = [(float(value), float(weight)) for value, weight in items if value is not None]
    if not active:
        return 0.0
    total_weight = sum(weight for _, weight in active)
    if total_weight <= 0:
        return 0.0
    return sum(value * weight for value, weight in active) / total_weight


def _round_or_none(value: float | None, digits: int = 2) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _coalesce(value: float | None, default: float) -> float:
    return default if value is None else float(value)


def _as_date_str(value: Any) -> str | None:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%d")


def _unique(items: list[str]) -> list[str]:
    return list(dict.fromkeys([item for item in items if item]))


def _leader_phase_bucket(label: str) -> str:
    if label == "Emerging Leader":
        return "FORMING"
    if label == "Confirmed Leader":
        return "RECENT_OR_ACTIONABLE"
    if label == "Extended Leader":
        return "COMPLETED_NOT_FRESH"
    return "NONE"


def _follower_phase_bucket(label: str) -> str:
    if label == "Early Sympathy Candidate":
        return "FORMING"
    if label == "High-Quality Follower":
        return "RECENT_OR_ACTIONABLE"
    if label == "Watch Only":
        return "COMPLETED_NOT_FRESH"
    return "NONE"


def _winsorized_zscore(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    if valid.empty:
        return pd.Series(0.0, index=series.index, dtype=float)
    mean = float(valid.mean())
    std = float(valid.std(ddof=0))
    if std <= 1e-9:
        return pd.Series(0.0, index=series.index, dtype=float)
    clipped = numeric.clip(lower=mean - (3.0 * std), upper=mean + (3.0 * std))
    z = (clipped - mean) / std
    return z.fillna(0.0)


def _zscore_to_percent(zscore: pd.Series, *, higher_is_better: bool = True) -> pd.Series:
    adjusted = zscore if higher_is_better else -zscore
    return (_clamp_series((adjusted + 3.0) / 6.0) * 100.0).astype(float)


def _clamp_series(series: pd.Series, low: float = 0.0, high: float = 1.0) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").fillna(low)
    return numeric.clip(lower=low, upper=high)


def _percentile_series(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.dropna().empty:
        return pd.Series(50.0, index=series.index, dtype=float)
    if numeric.nunique(dropna=True) <= 1:
        return pd.Series(100.0, index=series.index, dtype=float)
    return numeric.rank(pct=True, method="average").fillna(0.5) * 100.0


def _rolling_atr(frame: pd.DataFrame, window: int) -> pd.Series:
    prev_close = frame["adj_close"].shift(1)
    tr = pd.concat(
        [
            frame["high"] - frame["low"],
            (frame["high"] - prev_close).abs(),
            (frame["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(window, min_periods=max(5, window // 3)).mean()


@dataclass(frozen=True)
class MarketProfile:
    market_code: str
    timezone: str
    currency: str
    traded_value_floor: float
    min_history: int


@dataclass(frozen=True)
class MarketContext:
    benchmark_symbol: str
    regime_state: str
    regime_score: float
    breadth_50: float | None
    breadth_200: float | None
    high_low_ratio: float | None
    top_group_share: float | None
    score_multiplier: float
    reason_codes: tuple[str, ...]


class LeaderLaggingAnalyzer:
    MARKET_PROFILES: dict[str, MarketProfile] = {
        "us": MarketProfile(
            market_code="US",
            timezone="America/New_York",
            currency="USD",
            traded_value_floor=2_000_000.0,
            min_history=280,
        ),
        "kr": MarketProfile(
            market_code="KR",
            timezone="Asia/Seoul",
            currency="KRW",
            traded_value_floor=1_000_000_000.0,
            min_history=280,
        ),
    }

    def market_profile(self, market: str) -> MarketProfile:
        return self.MARKET_PROFILES.get(market_key(market), self.MARKET_PROFILES["us"])

    def default_actual_data_calibration(self) -> dict[str, float]:
        return {
            "leader_rs_rank_min": 85.0,
            "leader_group_rs_min": 75.0,
            "leader_distance_to_high_max": 0.20,
            "leader_distance_from_low_min": 0.30,
            "leader_group_strength_min": 70.0,
            "leader_rs_line_score_min": 70.0,
            "leader_confirmed_score_min": 78.0,
            "leader_emerging_score_min": 72.0,
            "leader_extended_distance_to_high_max": 0.03,
            "leader_extended_pivot_proximity_max": 25.0,
            "leader_tier1_boost_min": 3.0,
            "follower_group_rs_min": 75.0,
            "follower_rs_rank_min": 65.0,
            "follower_rs_rank_max": 90.0,
            "follower_distance_to_high_max": 0.25,
            "follower_hygiene_distance_to_high_max": 0.30,
            "follower_leader_gap_60d_max": 0.50,
            "follower_pair_link_min": 70.0,
            "follower_underreaction_min": 70.0,
            "follower_underreaction_emerging_min": 55.0,
            "follower_underreaction_watch_min": 40.0,
            "follower_rs_inflection_min": 60.0,
            "follower_confirmed_score_min": 78.0,
            "follower_emerging_score_min": 68.0,
            "follower_watch_score_min": 58.0,
            "broad_pool_rs_rank_min": 65.0,
        }

    def normalize_daily_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "adj_close", "volume"])

        daily = frame.copy()
        rename_map: dict[str, str] = {}
        for column in daily.columns:
            lowered = str(column).strip().lower()
            if lowered in {"date", "open", "high", "low", "close", "adj_close", "volume"}:
                rename_map[column] = lowered
        daily = daily.rename(columns=rename_map)

        if "date" not in daily.columns:
            daily = daily.reset_index()
            daily = daily.rename(columns={daily.columns[0]: "date"})
        if "adj_close" not in daily.columns:
            daily["adj_close"] = daily.get("close")
        for column in ("open", "high", "low", "close", "adj_close"):
            if column not in daily.columns:
                daily[column] = daily.get("close")
        if "volume" not in daily.columns:
            daily["volume"] = 0.0

        daily["date"] = pd.to_datetime(daily["date"], errors="coerce")
        for column in ("open", "high", "low", "close", "adj_close", "volume"):
            daily[column] = pd.to_numeric(daily[column], errors="coerce")

        daily = daily.dropna(subset=["date", "close", "adj_close"]).copy()
        if daily.empty:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "adj_close", "volume"])
        return daily.sort_values("date").reset_index(drop=True)

    def _align_relative_strength(self, daily: pd.DataFrame, benchmark_daily: pd.DataFrame) -> pd.DataFrame:
        benchmark = benchmark_daily[["date", "adj_close"]].rename(columns={"adj_close": "benchmark_adj_close"}).copy()
        aligned = pd.merge_asof(
            daily.sort_values("date"),
            benchmark.sort_values("date"),
            on="date",
            direction="backward",
        )
        aligned["benchmark_adj_close"] = pd.to_numeric(aligned["benchmark_adj_close"], errors="coerce")
        aligned["rs_line"] = aligned["adj_close"] / aligned["benchmark_adj_close"].replace(0, np.nan)
        return aligned

    def _compute_subperiod_return(self, series: pd.Series, end_offset: int, span: int) -> float | None:
        if len(series) <= end_offset + span:
            return None
        end_idx = len(series) - 1 - end_offset
        start_idx = end_idx - span
        if start_idx < 0:
            return None
        start = _safe_float(series.iloc[start_idx])
        end = _safe_float(series.iloc[end_idx])
        if start in {None, 0} or end is None:
            return None
        return (end / start) - 1.0

    def _estimate_structure(self, daily: pd.DataFrame) -> dict[str, float | None]:
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
            }

        atr20 = _safe_float(_rolling_atr(daily, 20).iloc[-1])
        atr60 = _safe_float(_rolling_atr(daily, 60).iloc[-1])
        volatility_contraction = None
        if atr20 is not None and atr60 not in {None, 0}:
            volatility_contraction = 1.0 - min(atr20 / atr60, 2.0)

        best_length = None
        best_score = -1.0
        best_high = None
        best_tightness = None
        for length in (15, 20, 25, 30, 40, 50, 60):
            window = daily.iloc[-length:].copy()
            base_high = _safe_float(window["high"].max())
            base_low = _safe_float(window["low"].min())
            if base_high in {None, 0} or base_low is None or base_high <= base_low:
                continue
            depth = (base_high - base_low) / base_high
            tightness = 1.0 - depth
            close_pos = (_safe_float(window["adj_close"].iloc[-1]) - base_low) / max(base_high - base_low, 1e-9)
            score = (
                (_score_inverse(depth, 0.15, 0.40) * 0.45)
                + (_score_ratio(close_pos, 0.70, 0.30) * 0.30)
                + (_score_range(float(length), 20.0, 45.0, 10.0, 70.0) * 0.25)
            )
            if score > best_score:
                best_score = score
                best_length = float(length)
                best_high = base_high
                best_tightness = tightness

        range_10 = ((daily["high"] - daily["low"]) / daily["adj_close"].replace(0, np.nan)).tail(10).mean()
        range_40 = ((daily["high"] - daily["low"]) / daily["adj_close"].replace(0, np.nan)).tail(40).mean()
        range_compression = None
        if pd.notna(range_10) and pd.notna(range_40) and range_40 not in {0, None}:
            range_compression = 1.0 - min(float(range_10 / range_40), 2.0)

        recent_high = _safe_float(daily["high"].tail(30).max())
        close = _safe_float(daily["adj_close"].iloc[-1])
        pivot_proximity = None
        if recent_high not in {None, 0} and close is not None:
            pivot_proximity = 1.0 - min(abs((close / recent_high) - 1.0) / 0.10, 1.0)

        breakout_volume_expansion = None
        avg_volume_20 = _safe_float(daily["volume"].rolling(20, min_periods=5).mean().iloc[-1])
        if avg_volume_20 not in {None, 0}:
            breakout_volume_expansion = (_safe_float(daily["volume"].iloc[-1]) or 0.0) / avg_volume_20

        structure_quality_score = _weighted_mean(
            [
                ((_safe_float(best_tightness) or 0.0) * 100.0, 0.35),
                ((_safe_float(volatility_contraction) or 0.0) * 100.0, 0.25),
                ((_safe_float(range_compression) or 0.0) * 100.0, 0.20),
                ((_safe_float(pivot_proximity) or 0.0) * 100.0, 0.20),
            ]
        )
        return {
            "base_length": best_length,
            "base_tightness": (_safe_float(best_tightness) or 0.0) * 100.0 if best_tightness is not None else None,
            "volatility_contraction": (_safe_float(volatility_contraction) or 0.0) * 100.0 if volatility_contraction is not None else None,
            "range_compression": (_safe_float(range_compression) or 0.0) * 100.0 if range_compression is not None else None,
            "pivot_proximity": (_safe_float(pivot_proximity) or 0.0) * 100.0 if pivot_proximity is not None else None,
            "breakout_volume_expansion": breakout_volume_expansion,
            "structure_quality_score": structure_quality_score,
            "recent_pivot_high": best_high,
        }

    def compute_symbol_features(
        self,
        *,
        symbol: str,
        market: str,
        daily_frame: pd.DataFrame,
        benchmark_daily: pd.DataFrame,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        profile = self.market_profile(market)
        metadata = dict(metadata or {})
        daily = self.normalize_daily_frame(daily_frame)
        benchmark = self.normalize_daily_frame(benchmark_daily)
        if daily.empty or benchmark.empty:
            return {
                "symbol": symbol,
                "market": profile.market_code,
                "bars": 0,
            }

        aligned = self._align_relative_strength(daily, benchmark)
        aligned["ma50"] = aligned["adj_close"].rolling(50, min_periods=20).mean()
        aligned["ma150"] = aligned["adj_close"].rolling(150, min_periods=60).mean()
        aligned["ma200"] = aligned["adj_close"].rolling(200, min_periods=80).mean()
        aligned["atr20"] = _rolling_atr(aligned, 20)
        aligned["atr60"] = _rolling_atr(aligned, 60)
        aligned["adv20"] = aligned["volume"].rolling(20, min_periods=5).mean()
        aligned["adv50"] = aligned["volume"].rolling(50, min_periods=10).mean()
        aligned["traded_value"] = aligned["adj_close"] * aligned["volume"]
        aligned["traded_value_20d"] = aligned["traded_value"].rolling(20, min_periods=5).mean()
        aligned["traded_value_50d"] = aligned["traded_value"].rolling(50, min_periods=10).mean()
        aligned["daily_return"] = aligned["adj_close"].pct_change()
        aligned["rs_line_sma252"] = aligned["rs_line"].rolling(252, min_periods=80).mean()
        aligned["hhv_252"] = aligned["adj_close"].rolling(252, min_periods=60).max()
        aligned["llv_252"] = aligned["adj_close"].rolling(252, min_periods=60).min()

        latest = aligned.iloc[-1]
        close = _safe_float(latest["adj_close"])
        ma50 = _safe_float(latest["ma50"])
        ma150 = _safe_float(latest["ma150"])
        ma200 = _safe_float(latest["ma200"])
        atr20 = _safe_float(latest["atr20"])
        adv20 = _safe_float(latest["adv20"])
        traded_value_20d = _safe_float(latest["traded_value_20d"])
        rs_line = pd.to_numeric(aligned["rs_line"], errors="coerce")
        rs_line_last = _safe_float(rs_line.iloc[-1])
        rs_line_20d_slope = self._compute_subperiod_return(rs_line, 0, 20)
        mansfield_rs = None
        rs_line_sma252 = _safe_float(latest["rs_line_sma252"])
        if rs_line_last is not None and rs_line_sma252 not in {None, 0}:
            mansfield_rs = 100.0 * ((rs_line_last / rs_line_sma252) - 1.0)

        mansfield_series = 100.0 * (rs_line / aligned["rs_line_sma252"].replace(0, np.nan) - 1.0)
        mansfield_rs_slope = self._compute_subperiod_return(mansfield_series.ffill(), 0, 20)
        weighted_rs_raw = _weighted_mean(
            [
                (self._compute_subperiod_return(aligned["adj_close"], 0, 63), 0.40),
                (self._compute_subperiod_return(aligned["adj_close"], 63, 63), 0.20),
                (self._compute_subperiod_return(aligned["adj_close"], 126, 63), 0.20),
                (self._compute_subperiod_return(aligned["adj_close"], 189, 63), 0.20),
            ]
        )
        weighted_rs_prev_raw = _weighted_mean(
            [
                (self._compute_subperiod_return(aligned["adj_close"], 63, 63), 0.40),
                (self._compute_subperiod_return(aligned["adj_close"], 126, 63), 0.20),
                (self._compute_subperiod_return(aligned["adj_close"], 189, 63), 0.20),
                (self._compute_subperiod_return(aligned["adj_close"], 252, 63), 0.20),
            ]
        )
        mom_12_1 = None
        if len(aligned) > 273:
            start = _safe_float(aligned["adj_close"].iloc[-273])
            end = _safe_float(aligned["adj_close"].iloc[-22])
            if start not in {None, 0} and end is not None:
                mom_12_1 = (end / start) - 1.0

        rs_line_distance_to_high = None
        rs_250_high = _safe_float(rs_line.tail(250).max())
        if rs_line_last is not None and rs_250_high not in {None, 0}:
            rs_line_distance_to_high = 1.0 - (rs_line_last / rs_250_high)
        price_250_high = _safe_float(aligned["adj_close"].tail(250).max())
        rs_line_65d_high_flag = bool(rs_line_last is not None and rs_line_last >= (_safe_float(rs_line.tail(65).max()) or 0.0))
        rs_line_250d_high_flag = bool(rs_line_last is not None and rs_line_last >= (rs_250_high or 0.0))
        rs_new_high_before_price_flag = bool(
            rs_line_250d_high_flag
            and close is not None
            and price_250_high not in {None, 0}
            and close < price_250_high
        )

        hhv_252 = _safe_float(latest["hhv_252"]) or _safe_float(aligned["adj_close"].max())
        llv_252 = _safe_float(latest["llv_252"]) or _safe_float(aligned["adj_close"].min())
        distance_to_52w_high = None
        distance_from_52w_low = None
        if close is not None and hhv_252 not in {None, 0}:
            distance_to_52w_high = 1.0 - (close / hhv_252)
        if close is not None and llv_252 not in {None, 0}:
            distance_from_52w_low = (close / llv_252) - 1.0

        ma200_slope_20d = None
        if len(aligned) >= 221:
            prev_ma200 = _safe_float(aligned["ma200"].iloc[-21])
            if prev_ma200 not in {None, 0} and ma200 is not None:
                ma200_slope_20d = (ma200 / prev_ma200) - 1.0

        trend_integrity_score = _weighted_mean(
            [
                (100.0 if close is not None and ma50 is not None and close > ma50 else 0.0, 0.20),
                (100.0 if ma50 is not None and ma150 is not None and ma50 > ma150 else 0.0, 0.20),
                (100.0 if ma150 is not None and ma200 is not None and ma150 > ma200 else 0.0, 0.20),
                ((_score_ratio(ma200_slope_20d, 0.01, -0.01) * 100.0), 0.20),
                (
                    (
                        _score_inverse(
                            abs((close / ma50) - 1.0) if close is not None and ma50 not in {None, 0} else None,
                            0.12,
                            0.30,
                        )
                        * 100.0
                    ),
                    0.20,
                ),
            ]
        )

        structure = self._estimate_structure(aligned)
        up_volume = aligned.loc[aligned["daily_return"] > 0, "volume"].tail(20).sum()
        down_volume = aligned.loc[aligned["daily_return"] < 0, "volume"].tail(20).sum()
        up_volume_ratio = None
        if down_volume > 0:
            up_volume_ratio = float(up_volume / down_volume)
        distribution_day_count_20d = int(
            (
                (aligned["daily_return"] < 0)
                & (aligned["volume"] > aligned["volume"].shift(1))
            ).tail(20).sum()
        )
        rvol = None
        adv50 = _safe_float(latest["adv50"])
        if adv50 not in {None, 0}:
            rvol = (_safe_float(latest["volume"]) or 0.0) / adv50
        traded_value = _safe_float(latest["traded_value"])
        illiq = None
        recent_illiq = (aligned["daily_return"].abs() / aligned["traded_value"].replace(0, np.nan)).tail(20)
        if not recent_illiq.dropna().empty:
            illiq = float(recent_illiq.mean())

        event_gap_up_flag = False
        event_proxy_score = None
        if len(aligned) >= 2:
            prev_close = _safe_float(aligned["adj_close"].iloc[-2])
            open_price = _safe_float(aligned["open"].iloc[-1])
            gap_pct = None
            if open_price is not None and prev_close not in {None, 0}:
                gap_pct = (open_price / prev_close) - 1.0
                event_gap_up_flag = gap_pct >= 0.05
            event_proxy_score = _weighted_mean(
                [
                    ((_score_ratio(gap_pct, 0.05, -0.02) * 100.0), 0.35),
                    ((_score_ratio(rvol, 1.8, 0.8) * 100.0), 0.35),
                    ((_score_ratio(self._compute_subperiod_return(aligned["adj_close"], 0, 5), 0.08, -0.03) * 100.0), 0.30),
                ]
            )

        sector = str(metadata.get("sector") or "").strip()
        industry = str(metadata.get("industry") or "").strip()
        return {
            "symbol": str(symbol).upper(),
            "market": profile.market_code,
            "as_of_ts": _as_date_str(latest["date"]),
            "sector": sector,
            "industry": industry,
            "bars": int(len(aligned)),
            "close": close,
            "ma50": ma50,
            "ma150": ma150,
            "ma200": ma200,
            "ma200_slope_20d": ma200_slope_20d,
            "close_gt_50": bool(close is not None and ma50 is not None and close > ma50),
            "close_gt_200": bool(close is not None and ma200 is not None and close > ma200),
            "weighted_rs_raw": weighted_rs_raw,
            "weighted_rs_prev_raw": weighted_rs_prev_raw,
            "mom_12_1": mom_12_1,
            "rs_line_20d_slope": rs_line_20d_slope,
            "rs_line_65d_high_flag": rs_line_65d_high_flag,
            "rs_line_250d_high_flag": rs_line_250d_high_flag,
            "rs_new_high_before_price_flag": rs_new_high_before_price_flag,
            "rs_line_distance_to_high": rs_line_distance_to_high,
            "mansfield_rs": mansfield_rs,
            "mansfield_rs_slope": mansfield_rs_slope,
            "distance_to_52w_high": distance_to_52w_high,
            "distance_from_52w_low": distance_from_52w_low,
            "ret_20d": self._compute_subperiod_return(aligned["adj_close"], 0, 20),
            "ret_60d": self._compute_subperiod_return(aligned["adj_close"], 0, 60),
            "ret_120d": self._compute_subperiod_return(aligned["adj_close"], 0, 120),
            "ret_252d": self._compute_subperiod_return(aligned["adj_close"], 0, 252),
            "adv20": adv20,
            "adv50": adv50,
            "traded_value": traded_value,
            "traded_value_20d": traded_value_20d,
            "traded_value_50d": _safe_float(latest["traded_value_50d"]),
            "atr20_pct": ((atr20 / close) * 100.0) if atr20 is not None and close not in {None, 0} else None,
            "rvol": rvol,
            "up_volume_ratio": up_volume_ratio,
            "distribution_day_count_20d": distribution_day_count_20d,
            "illiq": illiq,
            "trend_integrity_score": trend_integrity_score,
            "base_length": structure["base_length"],
            "base_tightness": structure["base_tightness"],
            "volatility_contraction": structure["volatility_contraction"],
            "range_compression": structure["range_compression"],
            "pivot_proximity": structure["pivot_proximity"],
            "breakout_volume_expansion": structure["breakout_volume_expansion"],
            "structure_quality_score": structure["structure_quality_score"],
            "recent_pivot_high": structure["recent_pivot_high"],
            "event_gap_up_flag": event_gap_up_flag,
            "event_proxy_score": event_proxy_score,
            "market_cap": _safe_float(metadata.get("market_cap")),
            "group_name": industry or sector,
        }

    def _apply_group_fallback(self, feature_table: pd.DataFrame) -> pd.DataFrame:
        table = feature_table.copy()
        if table.empty:
            return table
        if "group_name" not in table.columns:
            table["group_name"] = ""
        missing_group = table["group_name"].fillna("").astype(str).str.strip() == ""
        if missing_group.any():
            ret_rank = _percentile_series(table["ret_60d"].fillna(0.0))
            vol_rank = _percentile_series(table["atr20_pct"].fillna(0.0))
            fallback = (
                "CLUSTER_"
                + ((ret_rank // 25).astype(int)).astype(str)
                + "_"
                + ((vol_rank // 34).astype(int)).astype(str)
            )
            table.loc[missing_group, "group_name"] = fallback.loc[missing_group]
            table.loc[table["sector"].fillna("").astype(str).str.strip() == "", "sector"] = "FALLBACK_CLUSTER"
            table.loc[table["industry"].fillna("").astype(str).str.strip() == "", "industry"] = table["group_name"]
        return table

    def finalize_feature_table(self, feature_table: pd.DataFrame) -> pd.DataFrame:
        if feature_table.empty:
            return feature_table
        table = self._apply_group_fallback(feature_table)
        for column in (
            "weighted_rs_raw",
            "weighted_rs_prev_raw",
            "mom_12_1",
            "traded_value_20d",
            "illiq",
            "ret_20d",
            "ret_60d",
            "ret_120d",
            "ret_252d",
            "trend_integrity_score",
            "structure_quality_score",
            "event_proxy_score",
        ):
            table[column] = pd.to_numeric(table.get(column), errors="coerce")

        table["rs_rank"] = _percentile_series(table["weighted_rs_raw"])
        table["prev_rs_rank"] = _percentile_series(table["weighted_rs_prev_raw"])
        rank_delta = table["rs_rank"] - table["prev_rs_rank"]
        raw_delta = (table["weighted_rs_raw"].fillna(0.0) - table["weighted_rs_prev_raw"].fillna(0.0)) * 100.0
        table["delta_rs_rank_qoq"] = np.where(rank_delta.abs() >= 0.5, rank_delta, raw_delta)
        table["rs_rank_score"] = table["rs_rank"]
        table["traded_value_score"] = _zscore_to_percent(_winsorized_zscore(table["traded_value_20d"]), higher_is_better=True)
        table["illiq_score"] = _zscore_to_percent(_winsorized_zscore(table["illiq"]), higher_is_better=False)
        table["liquidity_quality_score"] = (
            (0.65 * table["traded_value_score"]) + (0.35 * table["illiq_score"])
        )
        table["norm_momentum_score"] = (
            0.5 * _zscore_to_percent(_winsorized_zscore(table["ret_120d"] - table["ret_20d"].fillna(0.0)), higher_is_better=True)
            + 0.5 * _zscore_to_percent(_winsorized_zscore(table["mom_12_1"]), higher_is_better=True)
        )
        table["rs_line_score"] = table.apply(
            lambda row: _weighted_mean(
                [
                    ((_score_ratio(_safe_float(row.get("rs_line_20d_slope")), 0.05, -0.05) * 100.0), 0.30),
                    ((_score_inverse(_safe_float(row.get("rs_line_distance_to_high")), 0.02, 0.20) * 100.0), 0.20),
                    ((_score_ratio(_safe_float(row.get("mansfield_rs")), 0.05, -0.10) * 100.0), 0.20),
                    (100.0 if bool(row.get("rs_line_250d_high_flag")) else 0.0, 0.15),
                    (100.0 if bool(row.get("rs_new_high_before_price_flag")) else 0.0, 0.15),
                ]
            ),
            axis=1,
        )
        table["volume_demand_score"] = table.apply(
            lambda row: _weighted_mean(
                [
                    ((_score_ratio(_safe_float(row.get("rvol")), 1.5, 0.8) * 100.0), 0.30),
                    ((_score_ratio(_safe_float(row.get("up_volume_ratio")), 1.3, 0.7) * 100.0), 0.25),
                    ((_score_ratio(_safe_float(row.get("breakout_volume_expansion")), 1.5, 0.8) * 100.0), 0.25),
                    ((_score_inverse(_safe_float(row.get("distribution_day_count_20d")), 2.0, 6.0) * 100.0), 0.20),
                ]
            ),
            axis=1,
        )
        table["risk_flag"] = table.apply(
            lambda row: "EXTENDED"
            if _coalesce(_safe_float(row.get("distance_to_52w_high")), 1.0) <= 0.03
            and _coalesce(_safe_float(row.get("pivot_proximity")), 0.0) < 20.0
            else "",
            axis=1,
        )
        return table

    def compute_group_table(self, feature_table: pd.DataFrame) -> pd.DataFrame:
        if feature_table.empty:
            return pd.DataFrame()

        group_table = (
            feature_table.groupby("group_name", dropna=False)
            .agg(
                sector=("sector", lambda values: next((str(item) for item in values if str(item)), "")),
                industry=("industry", lambda values: next((str(item) for item in values if str(item)), "")),
                group_member_count=("symbol", "count"),
                group_return_20d=("ret_20d", "mean"),
                group_return_60d=("ret_60d", "mean"),
                group_mansfield_rs=("mansfield_rs", "mean"),
                group_rs_slope_20d=("rs_line_20d_slope", "mean"),
                group_pct_above_50dma=("close_gt_50", "mean"),
                group_new_high_share=("distance_to_52w_high", lambda s: float((pd.to_numeric(s, errors="coerce") <= 0.08).mean())),
            )
            .reset_index()
        )
        group_table["group_return_20d_score"] = _zscore_to_percent(_winsorized_zscore(group_table["group_return_20d"]), higher_is_better=True)
        group_table["group_return_60d_score"] = _zscore_to_percent(_winsorized_zscore(group_table["group_return_60d"]), higher_is_better=True)
        group_table["group_mansfield_score"] = _zscore_to_percent(_winsorized_zscore(group_table["group_mansfield_rs"]), higher_is_better=True)
        group_table["group_rs_slope_score"] = _zscore_to_percent(_winsorized_zscore(group_table["group_rs_slope_20d"]), higher_is_better=True)
        group_table["industry_rs_pct"] = _percentile_series(group_table["group_return_60d"])
        group_table["sector_rs_pct"] = _percentile_series(group_table["group_return_20d"])
        group_table["group_strength_score"] = group_table.apply(
            lambda row: _weighted_mean(
                [
                    (_safe_float(row.get("industry_rs_pct")), 0.25),
                    (_safe_float(row.get("sector_rs_pct")), 0.15),
                    (_safe_float(row.get("group_mansfield_score")), 0.20),
                    (_safe_float(row.get("group_return_20d_score")), 0.15),
                    (_safe_float(row.get("group_return_60d_score")), 0.10),
                    (((_safe_float(row.get("group_pct_above_50dma")) or 0.0) * 100.0), 0.10),
                    (((_safe_float(row.get("group_new_high_share")) or 0.0) * 100.0), 0.05),
                ]
            ),
            axis=1,
        )
        group_table = group_table.sort_values(
            ["group_strength_score", "industry_rs_pct", "group_return_60d"],
            ascending=[False, False, False],
        ).reset_index(drop=True)
        group_table["group_rank"] = np.arange(1, len(group_table) + 1)
        return group_table

    def compute_market_context(
        self,
        *,
        market: str,
        benchmark_symbol: str,
        benchmark_daily: pd.DataFrame,
        feature_table: pd.DataFrame,
        group_table: pd.DataFrame,
    ) -> MarketContext:
        benchmark = self.normalize_daily_frame(benchmark_daily)
        reasons: list[str] = []
        breadth_50 = None
        breadth_200 = None
        high_low_ratio = None
        top_group_share = None

        if not feature_table.empty:
            breadth_50 = float(feature_table["close_gt_50"].fillna(False).mean() * 100.0)
            breadth_200 = float(feature_table["close_gt_200"].fillna(False).mean() * 100.0)
            new_high_count = int((feature_table["distance_to_52w_high"].fillna(1.0) <= 0.05).sum())
            new_low_count = int((feature_table["distance_from_52w_low"].fillna(1.0) <= 0.15).sum())
            high_low_ratio = float((new_high_count + 1) / (new_low_count + 1))
            if not group_table.empty:
                top_cutoff = max(1, int(np.ceil(len(group_table) * 0.2)))
                top_groups = set(group_table.head(top_cutoff)["group_name"].astype(str))
                top_group_share = float(feature_table["group_name"].astype(str).isin(top_groups).mean() * 100.0)

        trend_score = 50.0
        if not benchmark.empty and len(benchmark) >= 220:
            benchmark["ma50"] = benchmark["adj_close"].rolling(50, min_periods=20).mean()
            benchmark["ma200"] = benchmark["adj_close"].rolling(200, min_periods=80).mean()
            close = _safe_float(benchmark["adj_close"].iloc[-1])
            ma50 = _safe_float(benchmark["ma50"].iloc[-1])
            ma200 = _safe_float(benchmark["ma200"].iloc[-1])
            slope200 = None
            if len(benchmark) >= 221:
                prev_ma200 = _safe_float(benchmark["ma200"].iloc[-21])
                if prev_ma200 not in {None, 0} and ma200 is not None:
                    slope200 = (ma200 / prev_ma200) - 1.0
            trend_score = _weighted_mean(
                [
                    (100.0 if close is not None and ma50 is not None and close > ma50 else 0.0, 0.30),
                    (100.0 if ma50 is not None and ma200 is not None and ma50 > ma200 else 0.0, 0.30),
                    ((_score_ratio(slope200, 0.01, -0.01) * 100.0), 0.20),
                    ((_score_ratio(top_group_share / 100.0 if top_group_share is not None else None, 0.35, 0.10) * 100.0), 0.20),
                ]
            )

        breadth_score = _weighted_mean(
            [
                (breadth_50, 0.35),
                (breadth_200, 0.25),
                ((_score_ratio(high_low_ratio, 2.0, 0.7) * 100.0), 0.20),
                (top_group_share, 0.20),
            ]
        )
        regime_score = (0.55 * trend_score) + (0.45 * breadth_score)

        if regime_score >= 75.0:
            regime_state = "Risk-On"
            score_multiplier = 1.0
        elif regime_score >= 55.0:
            regime_state = "Neutral"
            score_multiplier = 0.88
        else:
            regime_state = "Risk-Off"
            score_multiplier = 0.70

        if trend_score >= 70.0:
            reasons.append("BENCHMARK_UPTREND")
        if (breadth_50 or 0.0) >= 60.0:
            reasons.append("BREADTH_SUPPORTIVE")
        if (top_group_share or 0.0) >= 35.0:
            reasons.append("TOP_GROUPS_EXPANDING")
        if not reasons:
            reasons.append("REGIME_MIXED")

        return MarketContext(
            benchmark_symbol=str(benchmark_symbol or "").upper(),
            regime_state=regime_state,
            regime_score=round(regime_score, 2),
            breadth_50=_round_or_none(breadth_50),
            breadth_200=_round_or_none(breadth_200),
            high_low_ratio=_round_or_none(high_low_ratio),
            top_group_share=_round_or_none(top_group_share),
            score_multiplier=score_multiplier,
            reason_codes=tuple(_unique(reasons)),
        )

    def build_actual_data_calibration(
        self,
        *,
        feature_table: pd.DataFrame,
        group_table: pd.DataFrame,
    ) -> dict[str, float]:
        calibration = self.default_actual_data_calibration()
        if feature_table.empty:
            return calibration

        candidate_pool = feature_table[feature_table["close_gt_50"].fillna(False)].copy()
        if candidate_pool.empty:
            candidate_pool = feature_table.copy()

        calibration["leader_rs_rank_min"] = bounded_quantile_value(
            candidate_pool["rs_rank"],
            0.75,
            calibration["leader_rs_rank_min"],
            lower=75.0,
            upper=95.0,
        )
        calibration["leader_distance_to_high_max"] = bounded_quantile_value(
            candidate_pool["distance_to_52w_high"],
            0.55,
            calibration["leader_distance_to_high_max"],
            lower=0.08,
            upper=0.25,
            positive_only=True,
        )
        calibration["leader_distance_from_low_min"] = bounded_quantile_value(
            candidate_pool["distance_from_52w_low"],
            0.45,
            calibration["leader_distance_from_low_min"],
            lower=0.15,
            upper=0.80,
            positive_only=True,
        )
        calibration["leader_extended_distance_to_high_max"] = bounded_quantile_value(
            candidate_pool["distance_to_52w_high"],
            0.25,
            calibration["leader_extended_distance_to_high_max"],
            lower=0.01,
            upper=0.08,
            positive_only=True,
        )
        calibration["follower_rs_rank_min"] = bounded_quantile_value(
            candidate_pool["rs_rank"],
            0.50,
            calibration["follower_rs_rank_min"],
            lower=55.0,
            upper=80.0,
        )
        calibration["follower_rs_rank_max"] = bounded_quantile_value(
            candidate_pool["rs_rank"],
            0.90,
            calibration["follower_rs_rank_max"],
            lower=calibration["follower_rs_rank_min"] + 8.0,
            upper=95.0,
        )
        calibration["follower_distance_to_high_max"] = bounded_quantile_value(
            candidate_pool["distance_to_52w_high"],
            0.70,
            calibration["follower_distance_to_high_max"],
            lower=0.10,
            upper=0.35,
            positive_only=True,
        )
        calibration["follower_hygiene_distance_to_high_max"] = max(
            calibration["follower_distance_to_high_max"],
            bounded_quantile_value(
                candidate_pool["distance_to_52w_high"],
                0.80,
                calibration["follower_hygiene_distance_to_high_max"],
                lower=0.15,
                upper=0.40,
                positive_only=True,
            ),
        )
        calibration["broad_pool_rs_rank_min"] = bounded_quantile_value(
            feature_table["rs_rank"],
            0.50,
            calibration["broad_pool_rs_rank_min"],
            lower=55.0,
            upper=80.0,
        )

        if not group_table.empty:
            calibration["leader_group_rs_min"] = bounded_quantile_value(
                group_table["industry_rs_pct"],
                0.65,
                calibration["leader_group_rs_min"],
                lower=65.0,
                upper=90.0,
            )
            calibration["leader_group_strength_min"] = bounded_quantile_value(
                group_table["group_strength_score"],
                0.55,
                calibration["leader_group_strength_min"],
                lower=60.0,
                upper=85.0,
            )
            calibration["follower_group_rs_min"] = bounded_quantile_value(
                group_table["industry_rs_pct"],
                0.55,
                calibration["follower_group_rs_min"],
                lower=60.0,
                upper=88.0,
            )

        return calibration

    def _assign_leader_labels(
        self,
        leaders: pd.DataFrame,
        *,
        calibration: dict[str, float],
    ) -> tuple[pd.DataFrame, dict[str, float]]:
        if leaders.empty:
            return leaders, calibration

        eligible = leaders[leaders["hard_gate_pass"].fillna(False)].copy()
        calibrated = dict(calibration)
        calibrated["leader_group_strength_min"] = bounded_quantile_value(
            eligible["group_strength_score"],
            0.55,
            calibrated["leader_group_strength_min"],
            lower=60.0,
            upper=85.0,
        )
        calibrated["leader_rs_line_score_min"] = bounded_quantile_value(
            eligible["rs_line_score"],
            0.55,
            calibrated["leader_rs_line_score_min"],
            lower=55.0,
            upper=85.0,
        )
        calibrated["leader_confirmed_score_min"] = bounded_quantile_value(
            eligible["leader_score"],
            0.75,
            calibrated["leader_confirmed_score_min"],
            lower=70.0,
            upper=90.0,
        )
        calibrated["leader_emerging_score_min"] = bounded_quantile_value(
            eligible["leader_score"],
            0.50,
            calibrated["leader_emerging_score_min"],
            lower=60.0,
            upper=84.0,
        )
        calibrated["leader_tier1_boost_min"] = bounded_quantile_value(
            eligible["tier1_boost_count"],
            0.60,
            calibrated["leader_tier1_boost_min"],
            lower=2.0,
            upper=4.0,
        )

        labeled = leaders.copy()
        labels: list[str] = []
        risk_flags: list[str] = []
        for _, row in labeled.iterrows():
            warnings = [flag for flag in str(row.get("risk_flag") or "").split(",") if flag]
            hard_gate = bool(row.get("hard_gate_pass"))
            extended = bool(row.get("extended_flag"))
            leader_score = _safe_float(row.get("leader_score")) or 0.0
            tier1_boosts = _safe_float(row.get("tier1_boost_count")) or 0.0
            rs_line_score = _safe_float(row.get("rs_line_score")) or 0.0
            group_strength = _safe_float(row.get("group_strength_score")) or 0.0

            if not hard_gate:
                label = "Too Weak, Reject"
                warnings.append("HARD_GATE_FAIL")
            elif extended and leader_score >= calibrated["leader_emerging_score_min"]:
                label = "Extended Leader"
                warnings.append("EXTENDED")
            elif (
                leader_score >= calibrated["leader_confirmed_score_min"]
                and tier1_boosts >= calibrated["leader_tier1_boost_min"]
                and rs_line_score >= calibrated["leader_rs_line_score_min"]
                and group_strength >= calibrated["leader_group_strength_min"]
            ):
                label = "Confirmed Leader"
            elif leader_score >= calibrated["leader_emerging_score_min"]:
                label = "Emerging Leader"
            else:
                label = "Too Weak, Reject"

            labels.append(label)
            risk_flags.append(",".join(_unique(warnings)))

        labeled["label"] = labels
        labeled["phase_bucket"] = labeled["label"].map(_leader_phase_bucket).fillna("NONE")
        labeled["risk_flag"] = risk_flags
        return labeled, calibrated

    def _assign_follower_labels(
        self,
        followers: pd.DataFrame,
        *,
        calibration: dict[str, float],
    ) -> tuple[pd.DataFrame, dict[str, float]]:
        if followers.empty:
            return followers, calibration

        eligible = followers[
            followers["hard_precondition_pass"].fillna(False)
            & followers["hygiene_pass"].fillna(False)
        ].copy()
        calibrated = dict(calibration)
        calibrated["follower_confirmed_score_min"] = bounded_quantile_value(
            eligible["follower_score"],
            0.75,
            calibrated["follower_confirmed_score_min"],
            lower=68.0,
            upper=90.0,
        )
        calibrated["follower_emerging_score_min"] = bounded_quantile_value(
            eligible["follower_score"],
            0.50,
            calibrated["follower_emerging_score_min"],
            lower=58.0,
            upper=82.0,
        )
        calibrated["follower_watch_score_min"] = bounded_quantile_value(
            eligible["follower_score"],
            0.30,
            calibrated["follower_watch_score_min"],
            lower=48.0,
            upper=75.0,
        )
        calibrated["follower_pair_link_min"] = bounded_quantile_value(
            eligible["pair_link_score"],
            0.55,
            calibrated["follower_pair_link_min"],
            lower=55.0,
            upper=85.0,
        )
        calibrated["follower_underreaction_min"] = bounded_quantile_value(
            eligible["underreaction_score"],
            0.55,
            calibrated["follower_underreaction_min"],
            lower=50.0,
            upper=85.0,
        )
        calibrated["follower_underreaction_emerging_min"] = bounded_quantile_value(
            eligible["underreaction_score"],
            0.40,
            calibrated["follower_underreaction_emerging_min"],
            lower=40.0,
            upper=75.0,
        )
        calibrated["follower_underreaction_watch_min"] = bounded_quantile_value(
            eligible["underreaction_score"],
            0.25,
            calibrated["follower_underreaction_watch_min"],
            lower=25.0,
            upper=65.0,
        )
        calibrated["follower_rs_inflection_min"] = bounded_quantile_value(
            eligible["rs_inflection_score"],
            0.55,
            calibrated["follower_rs_inflection_min"],
            lower=45.0,
            upper=80.0,
        )

        labeled = followers.copy()
        labels: list[str] = []
        risk_flags: list[str] = []
        for _, row in labeled.iterrows():
            warnings = [flag for flag in str(row.get("risk_flag") or "").split(",") if flag]
            hard_precondition = bool(row.get("hard_precondition_pass"))
            hygiene_pass = bool(row.get("hygiene_pass"))
            follower_score = _safe_float(row.get("follower_score")) or 0.0
            pair_link = _safe_float(row.get("pair_link_score")) or 0.0
            underreaction = _safe_float(row.get("underreaction_score")) or 0.0
            rs_inflection = _safe_float(row.get("rs_inflection_score")) or 0.0

            if not hard_precondition or not hygiene_pass:
                label = "Too Weak, Reject"
                warnings.append("FOLLOWER_FILTER_FAIL")
            elif (
                follower_score >= calibrated["follower_confirmed_score_min"]
                and pair_link >= calibrated["follower_pair_link_min"]
                and underreaction >= calibrated["follower_underreaction_min"]
                and rs_inflection >= calibrated["follower_rs_inflection_min"]
            ):
                label = "High-Quality Follower"
            elif (
                follower_score >= calibrated["follower_emerging_score_min"]
                and underreaction >= calibrated["follower_underreaction_emerging_min"]
            ):
                label = "Early Sympathy Candidate"
            elif (
                follower_score >= calibrated["follower_watch_score_min"]
                and underreaction >= calibrated["follower_underreaction_watch_min"]
            ):
                label = "Watch Only"
            else:
                label = "Too Weak, Reject"

            labels.append(label)
            risk_flags.append(",".join(_unique(warnings)))

        labeled["label"] = labels
        labeled["phase_bucket"] = labeled["label"].map(_follower_phase_bucket).fillna("NONE")
        labeled["risk_flag"] = risk_flags
        return labeled, calibrated

    def analyze_leaders(
        self,
        *,
        feature_table: pd.DataFrame,
        group_table: pd.DataFrame,
        market_context: MarketContext,
        calibration: dict[str, float] | None = None,
    ) -> pd.DataFrame:
        if feature_table.empty:
            return pd.DataFrame()

        calibration_map = dict(calibration or self.build_actual_data_calibration(feature_table=feature_table, group_table=group_table))

        table = feature_table.merge(
            group_table[
                [
                    "group_name",
                    "industry_rs_pct",
                    "sector_rs_pct",
                    "group_strength_score",
                    "group_rank",
                    "group_new_high_share",
                ]
            ],
            on="group_name",
            how="left",
        ).copy()
        breadth_context_score = 100.0 if market_context.regime_state == "Risk-On" else 65.0 if market_context.regime_state == "Neutral" else 35.0
        leader_rows: list[dict[str, Any]] = []
        for _, row in table.iterrows():
            row_dict = row.to_dict()
            reasons: list[str] = []
            warnings: list[str] = []

            hard_gate = all(
                [
                    bool(row.get("close_gt_50")),
                    _safe_float(row.get("ma50")) is not None and _safe_float(row.get("ma150")) is not None and (_safe_float(row.get("ma50")) > _safe_float(row.get("ma150"))),
                    _safe_float(row.get("ma150")) is not None and _safe_float(row.get("ma200")) is not None and (_safe_float(row.get("ma150")) > _safe_float(row.get("ma200"))),
                    (_safe_float(row.get("ma200_slope_20d")) or -1.0) > 0,
                    (_safe_float(row.get("rs_rank")) or 0.0) >= calibration_map["leader_rs_rank_min"],
                    (_safe_float(row.get("industry_rs_pct")) or 0.0) >= calibration_map["leader_group_rs_min"],
                    _coalesce(_safe_float(row.get("distance_to_52w_high")), 1.0) <= calibration_map["leader_distance_to_high_max"],
                    (_safe_float(row.get("distance_from_52w_low")) or 0.0) >= calibration_map["leader_distance_from_low_min"],
                    (_safe_float(row.get("traded_value_20d")) or 0.0) >= self.market_profile(row.get("market", "us")).traded_value_floor,
                    (_safe_float(row.get("illiq_score")) or 0.0) >= 20.0,
                    (_safe_float(row.get("mansfield_rs")) or -1.0) > 0.0,
                ]
            )
            tier1_boosts = sum(
                [
                    (_safe_float(row.get("rs_rank")) or 0.0) >= 92.0,
                    bool(row.get("rs_line_250d_high_flag")),
                    bool(row.get("rs_new_high_before_price_flag")),
                    (_safe_float(row.get("rvol")) or 0.0) >= 1.5,
                    (_safe_float(row.get("group_new_high_share")) or 0.0) >= 0.25,
                ]
            )
            leader_score = _weighted_mean(
                [
                    (_safe_float(row.get("group_strength_score")), 0.18),
                    (_safe_float(row.get("trend_integrity_score")), 0.18),
                    (_safe_float(row.get("structure_quality_score")), 0.14),
                    (_safe_float(row.get("rs_rank_score")), 0.14),
                    (_safe_float(row.get("rs_line_score")), 0.10),
                    (_safe_float(row.get("volume_demand_score")), 0.08),
                    (_safe_float(row.get("liquidity_quality_score")), 0.08),
                    (breadth_context_score, 0.05),
                    (_safe_float(row.get("event_proxy_score")), 0.05),
                ]
            ) * market_context.score_multiplier

            extended = bool(
                _coalesce(_safe_float(row.get("distance_to_52w_high")), 1.0) <= calibration_map["leader_extended_distance_to_high_max"]
                and (
                    (_coalesce(_safe_float(row.get("pivot_proximity")), 0.0) < calibration_map["leader_extended_pivot_proximity_max"])
                    or (_safe_float(row.get("rvol")) or 0.0) >= 2.5
                )
            )
            if (_safe_float(row.get("rs_rank")) or 0.0) >= 95.0:
                reasons.append("TOP_RS")
            if bool(row.get("rs_new_high_before_price_flag")):
                reasons.append("RS_LEADS_PRICE")
            if (_safe_float(row.get("industry_rs_pct")) or 0.0) >= max(calibration_map["leader_group_rs_min"] + 5.0, 85.0):
                reasons.append("TOP_GROUP")
            if (_safe_float(row.get("structure_quality_score")) or 0.0) >= 70.0:
                reasons.append("TIGHT_STRUCTURE")
            if (_safe_float(row.get("volume_demand_score")) or 0.0) >= 70.0:
                reasons.append("VOLUME_SUPPORT")

            leader_rows.append(
                {
                    "ticker": row.get("symbol"),
                    "symbol": row.get("symbol"),
                    "market": row.get("market"),
                    "sector": row.get("sector"),
                    "industry": row.get("industry"),
                    "group_name": row.get("group_name"),
                    "leader_score": _round_or_none(leader_score),
                    "rs_rank": _round_or_none(_safe_float(row.get("rs_rank"))),
                    "rs_line_20d_slope": _round_or_none(_safe_float(row.get("rs_line_20d_slope"))),
                    "rs_new_high_before_price_flag": bool(row.get("rs_new_high_before_price_flag")),
                    "distance_to_52w_high": _round_or_none(_coalesce(_safe_float(row.get("distance_to_52w_high")), 0.0) * 100.0),
                    "group_rank": int(row.get("group_rank")) if pd.notna(row.get("group_rank")) else None,
                    "top_reason_1": reasons[0] if reasons else "",
                    "top_reason_2": reasons[1] if len(reasons) > 1 else "",
                    "reason_codes": _unique(reasons),
                    "hard_gate_pass": hard_gate,
                    "extended_flag": extended,
                    "tier1_boost_count": int(tier1_boosts),
                    "group_strength_score": _round_or_none(_safe_float(row.get("group_strength_score"))),
                    "trend_integrity_score": _round_or_none(_safe_float(row.get("trend_integrity_score"))),
                    "structure_quality_score": _round_or_none(_safe_float(row.get("structure_quality_score"))),
                    "rs_line_score": _round_or_none(_safe_float(row.get("rs_line_score"))),
                    "volume_demand_score": _round_or_none(_safe_float(row.get("volume_demand_score"))),
                    "liquidity_quality_score": _round_or_none(_safe_float(row.get("liquidity_quality_score"))),
                    "event_proxy_score": _round_or_none(_safe_float(row.get("event_proxy_score"))),
                    "ret_20d": _safe_float(row.get("ret_20d")),
                    "ret_60d": _safe_float(row.get("ret_60d")),
                    "as_of_ts": row.get("as_of_ts"),
                    "risk_flag": "",
                }
            )

        leaders = pd.DataFrame(leader_rows)
        if leaders.empty:
            return leaders
        leaders, _ = self._assign_leader_labels(leaders, calibration=calibration_map)
        return leaders.sort_values(["leader_score", "rs_rank"], ascending=[False, False]).reset_index(drop=True)

    def _pair_link_score(
        self,
        candidate: pd.Series,
        leader: pd.Series,
        corr_score: float | None,
    ) -> float:
        same_industry = 100.0 if str(candidate.get("industry") or "") == str(leader.get("industry") or "") and str(candidate.get("industry") or "") else 0.0
        same_subindustry = 100.0 if str(candidate.get("group_name") or "") == str(leader.get("group_name") or "") and str(candidate.get("group_name") or "") else 0.0
        same_theme = 100.0 if str(candidate.get("sector") or "") == str(leader.get("sector") or "") and str(candidate.get("sector") or "") else 0.0
        event_overlap = 100.0 if (_safe_float(leader.get("event_proxy_score")) or 0.0) >= 65.0 else 45.0
        return _weighted_mean(
            [
                (same_industry, 0.45),
                (same_subindustry, 0.20),
                (same_theme, 0.15),
                (corr_score, 0.10),
                (event_overlap, 0.10),
            ]
        )

    def _rolling_return_correlation(self, candidate_frame: pd.DataFrame, leader_frame: pd.DataFrame) -> float | None:
        candidate = self.normalize_daily_frame(candidate_frame)[["date", "adj_close"]].copy()
        leader = self.normalize_daily_frame(leader_frame)[["date", "adj_close"]].copy()
        merged = candidate.merge(leader, on="date", suffixes=("_candidate", "_leader"))
        if len(merged) < 40:
            return None
        corr = merged["adj_close_candidate"].pct_change().tail(60).corr(merged["adj_close_leader"].pct_change().tail(60))
        return _safe_float(corr)

    def analyze_followers(
        self,
        *,
        feature_table: pd.DataFrame,
        leaders: pd.DataFrame,
        group_table: pd.DataFrame,
        market_context: MarketContext,
        frames: dict[str, pd.DataFrame],
        calibration: dict[str, float] | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        if feature_table.empty or leaders.empty:
            return pd.DataFrame(), pd.DataFrame()

        calibration_map = dict(calibration or self.build_actual_data_calibration(feature_table=feature_table, group_table=group_table))

        leader_pool = leaders[leaders["label"] == "Confirmed Leader"].copy()
        if leader_pool.empty:
            return pd.DataFrame(), pd.DataFrame()

        table = feature_table.merge(
            group_table[
                ["group_name", "industry_rs_pct", "group_strength_score", "group_rank"]
            ],
            on="group_name",
            how="left",
        ).copy()
        confirmed_symbols = set(leader_pool["symbol"].astype(str))
        follower_rows: list[dict[str, Any]] = []
        pair_rows: list[dict[str, Any]] = []
        breadth_context_score = 100.0 if market_context.regime_state == "Risk-On" else 65.0 if market_context.regime_state == "Neutral" else 35.0

        for _, candidate in table.iterrows():
            symbol = str(candidate.get("symbol"))
            if symbol in confirmed_symbols:
                continue
            peers = leader_pool[leader_pool["group_name"] == candidate.get("group_name")].copy()
            if peers.empty:
                continue

            best_payload: dict[str, Any] | None = None
            for _, leader in peers.iterrows():
                candidate_frame = frames.get(symbol, pd.DataFrame())
                leader_frame = frames.get(str(leader.get("symbol")), pd.DataFrame())
                corr = self._rolling_return_correlation(candidate_frame, leader_frame)
                corr_score = _score_ratio(corr, 0.70, 0.20) * 100.0
                pair_link_score = self._pair_link_score(candidate, leader, corr_score)
                leader_gap_20d = (_safe_float(leader.get("ret_20d")) or 0.0) - (_safe_float(candidate.get("ret_20d")) or 0.0)
                leader_gap_60d = (_safe_float(leader.get("ret_60d")) or 0.0) - (_safe_float(candidate.get("ret_60d")) or 0.0)
                propagation_ratio_20d = None
                if (_safe_float(leader.get("ret_20d")) or 0.0) > 0:
                    propagation_ratio_20d = (_safe_float(candidate.get("ret_20d")) or 0.0) / (_safe_float(leader.get("ret_20d")) or 1.0)
                underreaction_score = _weighted_mean(
                    [
                        ((_score_range(leader_gap_20d, 0.05, 0.25, 0.0, 0.40) * 100.0), 0.35),
                        ((_score_range(leader_gap_60d, 0.10, 0.35, 0.0, 0.50) * 100.0), 0.35),
                        ((_score_range(propagation_ratio_20d, 0.25, 0.80, 0.10, 1.00) * 100.0), 0.30),
                    ]
                )
                rs_inflection_score = _weighted_mean(
                    [
                        ((_score_ratio(_safe_float(candidate.get("rs_line_20d_slope")), 0.03, -0.03) * 100.0), 0.35),
                        ((_score_ratio(_safe_float(candidate.get("mansfield_rs_slope")), 0.03, -0.03) * 100.0), 0.30),
                        ((_score_ratio(_safe_float(candidate.get("delta_rs_rank_qoq")), 8.0, -5.0) * 100.0), 0.15),
                        (100.0 if bool(candidate.get("rs_line_65d_high_flag")) else 35.0, 0.10),
                        (100.0 if bool(candidate.get("rs_new_high_before_price_flag")) else 35.0, 0.10),
                    ]
                )
                catalyst_sympathy_score = _weighted_mean(
                    [
                        (_safe_float(leader.get("event_proxy_score")), 0.60),
                        ((_score_ratio(_safe_float(candidate.get("ret_20d")), 0.05, -0.05) * 100.0), 0.40),
                    ]
                )
                follower_score = _weighted_mean(
                    [
                        (pair_link_score, 0.25),
                        (_safe_float(candidate.get("group_strength_score")), 0.18),
                        (underreaction_score, 0.15),
                        (_safe_float(candidate.get("trend_integrity_score")), 0.12),
                        (rs_inflection_score, 0.10),
                        (_safe_float(candidate.get("volume_demand_score")), 0.08),
                        (_safe_float(candidate.get("liquidity_quality_score")), 0.07),
                        (breadth_context_score, 0.05),
                    ]
                ) * market_context.score_multiplier

                payload = {
                    "linked_leader": leader.get("symbol"),
                    "leader_label": leader.get("label"),
                    "pair_link_score": pair_link_score,
                    "leader_gap_20d": leader_gap_20d,
                    "leader_gap_60d": leader_gap_60d,
                    "propagation_ratio_20d": propagation_ratio_20d,
                    "underreaction_score": underreaction_score,
                    "rs_inflection_score": rs_inflection_score,
                    "catalyst_sympathy_score": catalyst_sympathy_score,
                    "follower_score": follower_score,
                    "corr": corr,
                }
                if best_payload is None or (payload["follower_score"] or 0.0) > (best_payload["follower_score"] or 0.0):
                    best_payload = payload

            if best_payload is None:
                continue

            hard_precondition = all(
                [
                    (_safe_float(candidate.get("industry_rs_pct")) or 0.0) >= calibration_map["follower_group_rs_min"],
                    bool(candidate.get("close_gt_50")),
                    _coalesce(_safe_float(candidate.get("distance_to_52w_high")), 1.0) <= calibration_map["follower_distance_to_high_max"],
                    calibration_map["follower_rs_rank_min"] <= (_safe_float(candidate.get("rs_rank")) or 0.0) < calibration_map["follower_rs_rank_max"],
                    (
                        (_safe_float(candidate.get("delta_rs_rank_qoq")) or -999.0) > 0.0
                        or (
                            (_safe_float(candidate.get("rs_line_20d_slope")) or -1.0) > 0.0
                            and (_safe_float(candidate.get("mansfield_rs_slope")) or -1.0) > 0.0
                        )
                    ),
                    (_safe_float(candidate.get("traded_value_20d")) or 0.0) >= self.market_profile(candidate.get("market", "us")).traded_value_floor,
                ]
            )
            hygiene_pass = all(
                [
                    bool(candidate.get("close_gt_200")) or (_safe_float(candidate.get("ma200_slope_20d")) or -1.0) > 0.0,
                    _coalesce(_safe_float(candidate.get("distance_to_52w_high")), 1.0) <= calibration_map["follower_hygiene_distance_to_high_max"],
                    (_safe_float(candidate.get("illiq_score")) or 0.0) >= 20.0,
                    (_safe_float(best_payload.get("leader_gap_60d")) or 0.0) <= calibration_map["follower_leader_gap_60d_max"],
                ]
            )

            reasons: list[str] = []
            warnings: list[str] = []
            if (_safe_float(best_payload.get("pair_link_score")) or 0.0) >= calibration_map["follower_pair_link_min"]:
                reasons.append("STRONG_PAIR_LINK")
            if (_safe_float(best_payload.get("underreaction_score")) or 0.0) >= calibration_map["follower_underreaction_min"]:
                reasons.append("CATCH_UP_POTENTIAL")
            if (_safe_float(best_payload.get("rs_inflection_score")) or 0.0) >= calibration_map["follower_rs_inflection_min"]:
                reasons.append("RS_INFLECTING")
            if (_safe_float(candidate.get("group_strength_score")) or 0.0) >= 75.0:
                reasons.append("STRONG_GROUP")
            if (_safe_float(best_payload.get("catalyst_sympathy_score")) or 0.0) >= 65.0:
                reasons.append("SYMPATHY_SETUP")

            follower_row = {
                "ticker": symbol,
                "symbol": symbol,
                "linked_leader": best_payload.get("linked_leader"),
                "market": candidate.get("market"),
                "sector": candidate.get("sector"),
                "industry": candidate.get("industry"),
                "group_name": candidate.get("group_name"),
                "follower_score": _round_or_none(_safe_float(best_payload.get("follower_score"))),
                "pair_link_score": _round_or_none(_safe_float(best_payload.get("pair_link_score"))),
                "rs_rank": _round_or_none(_safe_float(candidate.get("rs_rank"))),
                "delta_rs_rank_qoq": _round_or_none(_safe_float(candidate.get("delta_rs_rank_qoq"))),
                "rs_line_20d_slope": _round_or_none(_safe_float(candidate.get("rs_line_20d_slope"))),
                "mansfield_rs_slope": _round_or_none(_safe_float(candidate.get("mansfield_rs_slope"))),
                "leader_gap_20d": _round_or_none(_safe_float(best_payload.get("leader_gap_20d")) * 100.0 if best_payload.get("leader_gap_20d") is not None else None),
                "leader_gap_60d": _round_or_none(_safe_float(best_payload.get("leader_gap_60d")) * 100.0 if best_payload.get("leader_gap_60d") is not None else None),
                "propagation_ratio_20d": _round_or_none(_safe_float(best_payload.get("propagation_ratio_20d"))),
                "top_reason_1": reasons[0] if reasons else "",
                "top_reason_2": reasons[1] if len(reasons) > 1 else "",
                "reason_codes": _unique(reasons),
                "underreaction_score": _round_or_none(_safe_float(best_payload.get("underreaction_score"))),
                "rs_inflection_score": _round_or_none(_safe_float(best_payload.get("rs_inflection_score"))),
                "group_strength_score": _round_or_none(_safe_float(candidate.get("group_strength_score"))),
                "trend_integrity_score": _round_or_none(_safe_float(candidate.get("trend_integrity_score"))),
                "volume_demand_score": _round_or_none(_safe_float(candidate.get("volume_demand_score"))),
                "liquidity_quality_score": _round_or_none(_safe_float(candidate.get("liquidity_quality_score"))),
                "as_of_ts": candidate.get("as_of_ts"),
                "hard_precondition_pass": hard_precondition,
                "hygiene_pass": hygiene_pass,
                "risk_flag": "",
            }
            follower_rows.append(follower_row)
            pair_rows.append(
                {
                    "leader_symbol": best_payload.get("linked_leader"),
                    "follower_symbol": symbol,
                    "group_name": candidate.get("group_name"),
                    "pair_link_score": _round_or_none(_safe_float(best_payload.get("pair_link_score"))),
                    "follower_score": _round_or_none(_safe_float(best_payload.get("follower_score"))),
                    "leader_gap_20d": follower_row["leader_gap_20d"],
                    "leader_gap_60d": follower_row["leader_gap_60d"],
                    "propagation_ratio_20d": follower_row["propagation_ratio_20d"],
                    "corr_60d": _round_or_none(_safe_float(best_payload.get("corr"))),
                    "label": "",
                }
            )

        followers = pd.DataFrame(follower_rows)
        pairs = pd.DataFrame(pair_rows)
        if not followers.empty:
            followers, calibrated = self._assign_follower_labels(followers, calibration=calibration_map)
            label_map = followers.set_index("symbol")["label"].to_dict()
            if not pairs.empty:
                pairs["label"] = pairs["follower_symbol"].map(label_map).fillna("")
            followers = followers.sort_values(["follower_score", "pair_link_score"], ascending=[False, False]).reset_index(drop=True)
        if not pairs.empty:
            pairs = pairs.sort_values(["follower_score", "pair_link_score"], ascending=[False, False]).reset_index(drop=True)
        return followers, pairs


class LeaderLaggingScreener:
    def __init__(self, *, market: str = "us") -> None:
        self.market = market_key(market)
        ensure_market_dirs(self.market)
        from utils.market_runtime import get_leader_lagging_results_dir

        self.results_dir = get_leader_lagging_results_dir(self.market)
        ensure_dir(self.results_dir)
        self.analyzer = LeaderLaggingAnalyzer()

    def _load_metadata_map(self) -> dict[str, dict[str, Any]]:
        metadata_path = get_stock_metadata_path(self.market)
        if not os.path.exists(metadata_path):
            return {}
        frame = pd.read_csv(metadata_path)
        if frame.empty or "symbol" not in frame.columns:
            return {}
        frame["symbol"] = frame["symbol"].astype(str).str.upper()
        return {
            row["symbol"]: row.dropna().to_dict()
            for _, row in frame.iterrows()
        }

    def _load_frames(self) -> dict[str, pd.DataFrame]:
        data_dir = get_market_data_dir(self.market)
        if not os.path.isdir(data_dir):
            return {}
        frames: dict[str, pd.DataFrame] = {}
        candidate_files = [name for name in sorted(os.listdir(data_dir)) if name.endswith(".csv")]
        interval = progress_interval(len(candidate_files), target_updates=8, min_interval=50)
        print(f"[LeaderLagging] Frame load started ({self.market}) - files={len(candidate_files)}")
        for index, name in enumerate(candidate_files, start=1):
            if not name.endswith(".csv"):
                continue
            symbol = os.path.splitext(name)[0].upper()
            if not symbol or is_index_symbol(self.market, symbol):
                if is_progress_tick(index, len(candidate_files), interval):
                    print(
                        f"[LeaderLagging] Frame load progress ({self.market}) - "
                        f"processed={index}/{len(candidate_files)}, loaded={len(frames)}"
                    )
                continue
            frame = load_local_ohlcv_frame(self.market, symbol)
            if not frame.empty:
                frames[symbol] = frame
            if is_progress_tick(index, len(candidate_files), interval):
                print(
                    f"[LeaderLagging] Frame load progress ({self.market}) - "
                    f"processed={index}/{len(candidate_files)}, loaded={len(frames)}"
                )
        return frames

    def _persist(
        self,
        pattern_excluded_pool: pd.DataFrame,
        pattern_included_candidates: pd.DataFrame,
        leaders: pd.DataFrame,
        followers: pd.DataFrame,
        pairs: pd.DataFrame,
        group_table: pd.DataFrame,
        market_context: MarketContext,
        actual_data_calibration: dict[str, Any],
    ) -> None:
        outputs = {
            "pattern_excluded_pool": pattern_excluded_pool,
            "pattern_included_candidates": pattern_included_candidates,
            "leaders": leaders,
            "followers": followers,
            "leader_follower_pairs": pairs,
            "group_dashboard": group_table,
        }
        for stem, frame in outputs.items():
            csv_path = os.path.join(self.results_dir, f"{stem}.csv")
            json_path = os.path.join(self.results_dir, f"{stem}.json")
            frame.to_csv(csv_path, index=False)
            frame.to_json(json_path, orient="records", indent=2, force_ascii=False)

        summary = {
            "market": self.market.upper(),
            "benchmark_symbol": market_context.benchmark_symbol,
            "regime_state": market_context.regime_state,
            "regime_score": market_context.regime_score,
            "breadth_50": market_context.breadth_50,
            "breadth_200": market_context.breadth_200,
            "high_low_ratio": market_context.high_low_ratio,
            "top_group_share": market_context.top_group_share,
            "score_multiplier": market_context.score_multiplier,
            "reason_codes": list(market_context.reason_codes),
            "actual_data_calibration": actual_data_calibration,
            "counts": {
                "pattern_excluded_pool": int(len(pattern_excluded_pool)),
                "pattern_included_candidates": int(len(pattern_included_candidates)),
                "leaders": int(len(leaders)),
                "confirmed_leaders": int((leaders["label"] == "Confirmed Leader").sum()) if not leaders.empty else 0,
                "followers": int(len(followers)),
                "high_quality_followers": int((followers["label"] == "High-Quality Follower").sum()) if not followers.empty else 0,
                "pairs": int(len(pairs)),
            },
        }
        with open(os.path.join(self.results_dir, "market_summary.json"), "w", encoding="utf-8") as handle:
            json.dump(summary, handle, ensure_ascii=False, indent=2)
        with open(os.path.join(self.results_dir, "actual_data_calibration.json"), "w", encoding="utf-8") as handle:
            json.dump(actual_data_calibration, handle, ensure_ascii=False, indent=2)

    def run(self) -> dict[str, Any]:
        metadata_map = self._load_metadata_map()
        frames = self._load_frames()
        print(
            f"[LeaderLagging] Inputs ready ({self.market}) - "
            f"metadata={len(metadata_map)}, frames={len(frames)}"
        )
        benchmark_symbol, benchmark_daily = load_benchmark_data(
            self.market,
            get_benchmark_candidates(self.market),
            allow_yfinance_fallback=True,
        )
        benchmark_symbol = benchmark_symbol or get_primary_benchmark_symbol(self.market)
        benchmark_daily = self.analyzer.normalize_daily_frame(benchmark_daily)
        print(f"[LeaderLagging] Feature analysis started ({self.market}) - benchmark={benchmark_symbol}")

        feature_rows: list[dict[str, Any]] = []
        total_symbols = len(frames)
        interval = progress_interval(total_symbols, target_updates=8, min_interval=50)
        for index, (symbol, frame) in enumerate(frames.items(), start=1):
            feature_rows.append(
                self.analyzer.compute_symbol_features(
                    symbol=symbol,
                    market=self.market,
                    daily_frame=frame,
                    benchmark_daily=benchmark_daily,
                    metadata=metadata_map.get(symbol),
                )
            )
            if is_progress_tick(index, total_symbols, interval):
                print(
                    f"[LeaderLagging] Feature analysis progress ({self.market}) - "
                    f"processed={index}/{total_symbols}, features={len(feature_rows)}"
                )
        feature_table = self.analyzer.finalize_feature_table(pd.DataFrame(feature_rows))
        group_table = self.analyzer.compute_group_table(feature_table)
        actual_data_calibration = self.analyzer.build_actual_data_calibration(
            feature_table=feature_table,
            group_table=group_table,
        )
        print(
            f"[LeaderLagging] Relationship analysis started ({self.market}) - "
            f"features={len(feature_table)}, groups={len(group_table)}"
        )
        market_context = self.analyzer.compute_market_context(
            market=self.market,
            benchmark_symbol=benchmark_symbol,
            benchmark_daily=benchmark_daily,
            feature_table=feature_table,
            group_table=group_table,
        )
        leaders = self.analyzer.analyze_leaders(
            feature_table=feature_table,
            group_table=group_table,
            market_context=market_context,
            calibration=actual_data_calibration,
        )
        followers, pairs = self.analyzer.analyze_followers(
            feature_table=feature_table,
            leaders=leaders,
            group_table=group_table,
            market_context=market_context,
            frames=frames,
            calibration=actual_data_calibration,
        )

        pool_table = (
            feature_table.merge(
                group_table[["group_name", "group_strength_score", "group_rank"]],
                on="group_name",
                how="left",
            )
            if not feature_table.empty and not group_table.empty
            else feature_table.copy()
        )
        broad_pool = pd.DataFrame()
        if not pool_table.empty:
            for column in ("group_strength_score", "group_rank", "delta_rs_rank_qoq", "mansfield_rs_slope"):
                if column not in pool_table.columns:
                    pool_table[column] = pd.NA
            broad_pool = pool_table[
                (pool_table["close_gt_50"].fillna(False))
                & (pool_table["rs_rank"].fillna(0.0) >= actual_data_calibration["broad_pool_rs_rank_min"])
                & (pool_table["traded_value_20d"].fillna(0.0) >= self.analyzer.market_profile(self.market).traded_value_floor)
            ][
                [
                    "symbol",
                    "market",
                    "sector",
                    "industry",
                    "group_name",
                    "rs_rank",
                    "delta_rs_rank_qoq",
                    "group_strength_score",
                    "group_rank",
                    "trend_integrity_score",
                    "structure_quality_score",
                    "volume_demand_score",
                    "liquidity_quality_score",
                    "distance_to_52w_high",
                    "rs_line_20d_slope",
                    "mansfield_rs_slope",
                    "as_of_ts",
                ]
            ].copy()
            broad_pool["setup_scope"] = np.where(
                broad_pool["rs_rank"].fillna(0.0) >= actual_data_calibration["leader_rs_rank_min"],
                "LEADER_CANDIDATE_POOL",
                "FOLLOWER_CANDIDATE_POOL",
            )
            broad_pool["phase_bucket"] = "FORMING"
            broad_pool["distance_to_52w_high"] = broad_pool["distance_to_52w_high"].apply(
                lambda value: _round_or_none(_coalesce(_safe_float(value), 0.0) * 100.0)
            )
            broad_pool = broad_pool.sort_values(
                ["rs_rank", "group_strength_score", "trend_integrity_score"],
                ascending=[False, False, False],
            ).reset_index(drop=True)

        if not leaders.empty:
            leaders = leaders[leaders["label"] != "Too Weak, Reject"].reset_index(drop=True)
        if not followers.empty:
            followers = followers[followers["label"] != "Too Weak, Reject"].reset_index(drop=True)
        if not pairs.empty:
            valid_follower_symbols = set(followers["symbol"].astype(str))
            pairs = pairs[pairs["follower_symbol"].astype(str).isin(valid_follower_symbols)].reset_index(drop=True)

        pattern_included_candidates = pd.concat(
            [
                leaders.assign(candidate_family="LEADER"),
                followers.assign(candidate_family="FOLLOWER"),
            ],
            ignore_index=True,
            sort=False,
        ) if not leaders.empty or not followers.empty else pd.DataFrame()
        if not pattern_included_candidates.empty:
            pattern_included_candidates["_sort_score"] = (
                pd.to_numeric(pattern_included_candidates.get("leader_score"), errors="coerce")
                .fillna(pd.to_numeric(pattern_included_candidates.get("follower_score"), errors="coerce"))
            )
            pattern_included_candidates = pattern_included_candidates.sort_values(
                ["phase_bucket", "_sort_score", "symbol"],
                ascending=[True, False, True],
            ).drop(columns=["_sort_score"]).reset_index(drop=True)

        self._persist(
            broad_pool,
            pattern_included_candidates,
            leaders,
            followers,
            pairs,
            group_table,
            market_context,
            actual_data_calibration,
        )
        print(
            f"[LeaderLagging] Outputs saved ({self.market}) - "
            f"leaders={len(leaders)}, followers={len(followers)}, pairs={len(pairs)}"
        )
        return {
            "pattern_excluded_pool": broad_pool.to_dict(orient="records"),
            "pattern_included_candidates": pattern_included_candidates.to_dict(orient="records"),
            "leaders": leaders.to_dict(orient="records"),
            "followers": followers.to_dict(orient="records"),
            "pairs": pairs.to_dict(orient="records"),
            "group_dashboard": group_table.to_dict(orient="records"),
            "actual_data_calibration": actual_data_calibration,
            "market_summary": {
                "market": self.market.upper(),
                "benchmark_symbol": market_context.benchmark_symbol,
                "regime_state": market_context.regime_state,
                "regime_score": market_context.regime_score,
                "reason_codes": list(market_context.reason_codes),
            },
        }


def run_leader_lagging_screening(*, market: str = "us") -> dict[str, Any]:
    return LeaderLaggingScreener(market=market).run()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Leader / lagging follower screener")
    parser.add_argument("--market", default="us", help="Target market (us|kr)")
    args = parser.parse_args()
    run_leader_lagging_screening(market=args.market)


if __name__ == "__main__":
    main()
