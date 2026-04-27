from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Mapping

import numpy as np
import pandas as pd

from utils.indicator_helpers import (
    normalize_indicator_frame,
    rolling_atr as _helper_rolling_atr,
    rolling_average_volume,
    rolling_max,
    rolling_min,
    rolling_sma,
    rolling_traded_value,
    traded_value_series,
)
from utils.actual_data_calibration import bounded_quantile_value
from utils.io_utils import ensure_dir, write_dataframe_csv_with_fallback, write_dataframe_json_with_fallback, write_json_with_fallback
from utils.market_data_contract import (
    OhlcvFreshnessSummary,
    PricePolicy,
    SCREENING_OHLCV_READ_COLUMNS,
    _runtime_worker_count,
    describe_ohlcv_freshness,
    load_benchmark_data,
    load_local_ohlcv_frame,
    load_local_ohlcv_frames_ordered,
)
from utils.screening_cache import feature_row_cache_get_or_compute, resolve_ohlcv_source_path
from utils.market_runtime import (
    ensure_market_dirs,
    get_benchmark_candidates,
    get_market_data_dir,
    get_primary_benchmark_symbol,
    get_stock_metadata_path,
    is_index_symbol,
    limit_runtime_symbols,
    market_key,
)
from utils.progress_runtime import is_progress_tick, progress_interval
from utils.runtime_context import RuntimeContext, runtime_context_has_explicit_as_of
from utils.typing_utils import frame_keyed_records, series_to_str_text_dict
from screeners.leader_lagging import algorithms as leader_algorithms
from screeners.leader_lagging import followers as follower_algorithms
from screeners.leader_lagging import quality as leader_quality
from screeners.leader_lagging import tuning as leader_tuning
from screeners.leader_core_bridge import (
    MarketTruthSnapshot,
    annotate_frame_with_leader_core,
    build_industry_key,
    empty_leader_core_snapshot,
    load_market_truth_snapshot,
    shared_market_alias_to_leader_lagging_state,
)


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


def _safe_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return text if text else ""


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


def _safe_divide(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _max_drawdown(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    rolling_peak = numeric.cummax().replace(0, np.nan)
    drawdowns = 1.0 - (numeric / rolling_peak)
    if drawdowns.dropna().empty:
        return None
    return _safe_float(drawdowns.max())


def _coalesce(value: float | None, default: float) -> float:
    return default if value is None else float(value)


def _as_date_str(value: Any) -> str | None:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.strftime("%Y-%m-%d")


def _unique(items: list[str]) -> list[str]:
    return list(dict.fromkeys([item for item in items if item]))


def _tag_csv(items: list[str]) -> str:
    return ",".join(_unique(items))


def _leader_phase_bucket(label: str) -> str:
    if label in {"emerging_leader", "Emerging Leader"}:
        return "FORMING"
    if label in {"strong_leader", "Confirmed Leader"}:
        return "RECENT_OR_ACTIONABLE"
    if label in {"extended_leader", "Extended Leader"}:
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


def _numeric_frame_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column in frame.columns:
        return pd.to_numeric(frame[column], errors="coerce")
    return pd.Series(np.nan, index=frame.index, dtype=float)


def _rolling_atr(frame: pd.DataFrame, window: int) -> pd.Series:
    return _helper_rolling_atr(frame, window, close_col="adj_close", min_periods=max(5, window // 3))


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
    market_alignment_score: float | None = None
    breadth_support_score: float | None = None
    rotation_support_score: float | None = None
    leader_health_score: float | None = None
    market_alias: str = ""


class LeaderLaggingAnalyzer:
    DEFAULT_FOLLOWER_MAX_LEADERS_PER_INDUSTRY = 5
    DEFAULT_FOLLOWER_MAX_PAIRS_PER_CANDIDATE = 3
    DEFAULT_FOLLOWER_ANALYSIS_WORKER_CAP = 4

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

    def __init__(self) -> None:
        self.last_follower_lag_pruning: dict[str, Any] = {}

    def market_profile(self, market: str) -> MarketProfile:
        return self.MARKET_PROFILES.get(market_key(market), self.MARKET_PROFILES["us"])

    @staticmethod
    def _env_positive_int(env_var: str, default: int) -> int:
        raw_value = str(os.environ.get(env_var) or "").strip()
        if raw_value:
            try:
                value = int(raw_value)
            except ValueError:
                value = int(default)
            if value > 0:
                return value
        return int(default)

    @staticmethod
    def _follower_analysis_mode() -> str:
        mode = str(os.environ.get("INVEST_PROTO_FOLLOWER_ANALYSIS_MODE") or "balanced").strip().lower()
        return "full" if mode == "full" else "balanced"

    @staticmethod
    def _leader_follower_sort_key(record: Mapping[str, Any]) -> tuple[float, float, float, float, float, str]:
        return (
            -(_safe_float(record.get("leader_sort_score")) or 0.0),
            -(_safe_float(record.get("leader_score")) or 0.0),
            -(_safe_float(record.get("rs_rank")) or 0.0),
            -(_safe_float(record.get("event_proxy_score")) or 0.0),
            -(_safe_float(record.get("traded_value_20d")) or 0.0),
            _safe_text(record.get("symbol")),
        )

    def _follower_hard_precondition(
        self,
        candidate: Mapping[str, Any],
        calibration: Mapping[str, float],
    ) -> tuple[bool, list[str]]:
        candidate_industry_rs_pct = _safe_float(candidate.get("industry_rs_pct")) or 0.0
        candidate_distance_to_high = _coalesce(_safe_float(candidate.get("distance_to_52w_high")), 1.0)
        candidate_rs_rank = _safe_float(candidate.get("rs_rank")) or 0.0
        candidate_delta_rs_rank = _safe_float(candidate.get("delta_rs_rank_qoq")) or -999.0
        candidate_traded_value_20d = _safe_float(candidate.get("traded_value_20d")) or 0.0
        traded_value_floor = self.market_profile(candidate.get("market", "us")).traded_value_floor

        checks = [
            ("weak_group_rs", candidate_industry_rs_pct >= calibration["follower_group_rs_min"]),
            ("below_50dma", bool(candidate.get("close_gt_50"))),
            ("too_far_from_high", candidate_distance_to_high <= calibration["follower_distance_to_high_max"]),
            (
                "outside_follower_rs_rank_band",
                calibration["follower_rs_rank_min"] <= candidate_rs_rank < calibration["follower_rs_rank_max"],
            ),
            ("no_positive_delta_rs_rank", candidate_delta_rs_rank > 0.0),
            ("insufficient_traded_value", candidate_traded_value_20d >= traded_value_floor),
        ]
        failed = [reason for reason, passed in checks if not passed]
        return not failed, failed

    def _prefilter_reject_follower_row(
        self,
        candidate: Mapping[str, Any],
        *,
        reasons: list[str],
    ) -> dict[str, Any]:
        symbol = _safe_text(candidate.get("symbol"))
        reject_reasons = _tag_csv(["prefilter_fail", *reasons])
        return {
            "ticker": symbol,
            "symbol": symbol,
            "linked_leader": "",
            "market": candidate.get("market"),
            "sector": candidate.get("sector"),
            "industry": candidate.get("industry"),
            "industry_key": candidate.get("industry_key"),
            "group_name": candidate.get("group_name"),
            "follower_score": 0.0,
            "pair_link_score": 0.0,
            "peer_lead_score": 0.0,
            "best_lag_days": None,
            "lagged_corr": None,
            "lag_profile_sample_count": 0,
            "lag_profile_stability_score": 0.0,
            "pair_evidence_confidence": 0.0,
            "follower_confidence_score": 0.0,
            "rs_rank": _round_or_none(_safe_float(candidate.get("rs_rank"))),
            "delta_rs_rank_qoq": _round_or_none(_safe_float(candidate.get("delta_rs_rank_qoq"))),
            "rs_line_20d_slope": _round_or_none(_safe_float(candidate.get("rs_line_20d_slope"))),
            "leader_gap_20d": None,
            "leader_gap_60d": None,
            "propagation_ratio": None,
            "propagation_state": "unknown",
            "catchup_room_score": 0.0,
            "top_reason_1": "PREFILTER_FAIL",
            "top_reason_2": "",
            "reason_codes": ["PREFILTER_FAIL"],
            "follower_reject_reason_codes": reject_reasons,
            "underreaction_score": 0.0,
            "rs_inflection_score": 0.0,
            "structure_preservation_score": 0.0,
            "sympathy_freshness_score": 0.0,
            "link_evidence_tags": "",
            "group_strength_score": _round_or_none(_safe_float(candidate.get("group_strength_score"))),
            "trend_integrity_score": _round_or_none(_safe_float(candidate.get("trend_integrity_score"))),
            "volume_demand_score": _round_or_none(_safe_float(candidate.get("volume_demand_score"))),
            "liquidity_quality_score": _round_or_none(_safe_float(candidate.get("liquidity_quality_score"))),
            "as_of_ts": candidate.get("as_of_ts"),
            "hard_precondition_pass": False,
            "hygiene_pass": False,
            "risk_flag": "PREFILTER_FAIL",
        }

    def _follower_pair_pre_score(self, candidate: Mapping[str, Any], leader: Mapping[str, Any]) -> float:
        same_industry = 100.0 if str(candidate.get("industry") or "") == str(leader.get("industry") or "") and str(candidate.get("industry") or "") else 0.0
        same_group = 100.0 if str(candidate.get("group_name") or "") == str(leader.get("group_name") or "") and str(candidate.get("group_name") or "") else 0.0
        leader_ret_20d = _safe_float(leader.get("ret_20d")) or 0.0
        leader_ret_60d = _safe_float(leader.get("ret_60d")) or 0.0
        candidate_ret_20d = _safe_float(candidate.get("ret_20d")) or 0.0
        candidate_ret_60d = _safe_float(candidate.get("ret_60d")) or 0.0
        return _weighted_mean(
            [
                (same_industry, 0.20),
                (same_group, 0.12),
                ((_score_ratio(leader_ret_20d - candidate_ret_20d, 0.08, 0.00) * 100.0), 0.24),
                ((_score_ratio(leader_ret_60d - candidate_ret_60d, 0.18, 0.00) * 100.0), 0.18),
                ((_score_ratio(_safe_float(candidate.get("delta_rs_rank_qoq")) or 0.0, 8.0, 0.0) * 100.0), 0.12),
                (_safe_float(leader.get("event_proxy_score")), 0.14),
            ]
        )

    def _rank_follower_peers(
        self,
        candidate: Mapping[str, Any],
        peers: list[dict[str, Any]],
        *,
        max_pairs: int,
        balanced: bool,
    ) -> list[dict[str, Any]]:
        if not balanced:
            return peers
        scored = sorted(
            peers,
            key=lambda leader: (
                -self._follower_pair_pre_score(candidate, leader),
                *self._leader_follower_sort_key(leader),
            ),
        )
        return scored[:max_pairs]

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
        daily = normalize_indicator_frame(frame, price_policy=PricePolicy.SPLIT_ADJUSTED)
        if daily.empty:
            return pd.DataFrame(columns=["date", "open", "high", "low", "close", "adj_close", "volume"])
        # Keep the existing analyzer logic on a single, split-adjusted technical close.
        daily["adj_close"] = pd.to_numeric(daily["close"], errors="coerce")
        return daily

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
        if start is None or start == 0 or end is None:
            return None
        ratio = _safe_divide(end, start)
        if ratio is None:
            return None
        return ratio - 1.0

    def _benchmark_relative_weighted_rs(
        self,
        stock_series: pd.Series,
        benchmark_series: pd.Series,
        *,
        end_offset: int = 0,
    ) -> float | None:
        return leader_algorithms.benchmark_relative_weighted_rs(
            stock_series,
            benchmark_series,
            end_offset=end_offset,
        )

    def _estimate_structure(self, daily: pd.DataFrame) -> dict[str, Any]:
        return leader_algorithms.estimate_structure(daily)

    def compute_symbol_features(
        self,
        *,
        symbol: str,
        market: str,
        daily_frame: pd.DataFrame,
        benchmark_daily: pd.DataFrame,
        metadata: dict[str, Any] | None = None,
        benchmark_is_normalized: bool = False,
    ) -> dict[str, Any]:
        profile = self.market_profile(market)
        metadata = dict(metadata or {})
        daily = self.normalize_daily_frame(daily_frame)
        benchmark = benchmark_daily if benchmark_is_normalized else self.normalize_daily_frame(benchmark_daily)
        if daily.empty or benchmark.empty:
            return {
                "symbol": symbol,
                "market": profile.market_code,
                "bars": 0,
            }

        aligned = self._align_relative_strength(daily, benchmark)
        aligned["ma50"] = rolling_sma(aligned["adj_close"], 50, min_periods=20)
        aligned["ma150"] = rolling_sma(aligned["adj_close"], 150, min_periods=60)
        aligned["ma200"] = rolling_sma(aligned["adj_close"], 200, min_periods=80)
        aligned["atr20"] = _rolling_atr(aligned, 20)
        aligned["atr60"] = _rolling_atr(aligned, 60)
        aligned["adv20"] = rolling_average_volume(aligned, 20, min_periods=5)
        aligned["adv50"] = rolling_average_volume(aligned, 50, min_periods=10)
        aligned["traded_value"] = traded_value_series(aligned, close_col="adj_close")
        aligned["traded_value_20d"] = rolling_traded_value(aligned, 20, close_col="adj_close", min_periods=5)
        aligned["traded_value_50d"] = rolling_traded_value(aligned, 50, close_col="adj_close", min_periods=10)
        aligned["daily_return"] = aligned["adj_close"].pct_change()
        aligned["benchmark_return"] = aligned["benchmark_adj_close"].pct_change()
        aligned["rs_line_sma200"] = rolling_sma(aligned["rs_line"], 200, min_periods=80)
        aligned["hhv_252"] = rolling_max(aligned["adj_close"], 252, min_periods=60)
        aligned["llv_252"] = rolling_min(aligned["adj_close"], 252, min_periods=60)

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
        rs_line_sma200 = _safe_float(latest["rs_line_sma200"])
        benchmark_relative_strength = _safe_divide(rs_line_last, rs_line_sma200)
        benchmark_relative_series = rs_line / aligned["rs_line_sma200"].replace(0, np.nan)
        benchmark_relative_strength_slope = self._compute_subperiod_return(benchmark_relative_series.ffill(), 0, 20)
        weighted_rs_raw = self._benchmark_relative_weighted_rs(
            aligned["adj_close"],
            aligned["benchmark_adj_close"],
            end_offset=0,
        )
        weighted_rs_prev_raw = self._benchmark_relative_weighted_rs(
            aligned["adj_close"],
            aligned["benchmark_adj_close"],
            end_offset=63,
        )
        rs_proxy_profile = leader_algorithms.rs_rank_proxy_profile_from_history(
            aligned["adj_close"],
            aligned["benchmark_adj_close"],
        )
        mom_12_1 = None
        if len(aligned) > 273:
            start = _safe_float(aligned["adj_close"].iloc[-273])
            end = _safe_float(aligned["adj_close"].iloc[-22])
            mom_ratio = _safe_divide(end, start)
            if mom_ratio is not None:
                mom_12_1 = mom_ratio - 1.0

        rs_line_distance_to_high = None
        rs_250_high = _safe_float(rs_line.tail(250).max())
        rs_line_high_ratio = _safe_divide(rs_line_last, rs_250_high)
        if rs_line_high_ratio is not None:
            rs_line_distance_to_high = 1.0 - rs_line_high_ratio
        price_250_high = _safe_float(aligned["adj_close"].tail(250).max())
        rs_line_65d_high_flag = bool(rs_line_last is not None and rs_line_last >= (_safe_float(rs_line.tail(65).max()) or 0.0))
        rs_line_250d_high_flag = bool(rs_line_last is not None and rs_line_last >= (rs_250_high or 0.0))
        rs_new_high_before_price_flag = bool(
            rs_line_250d_high_flag
            and close is not None
            and price_250_high is not None
            and price_250_high != 0
            and close < price_250_high
        )

        hhv_252 = _safe_float(latest["hhv_252"]) or _safe_float(aligned["adj_close"].max())
        llv_252 = _safe_float(latest["llv_252"]) or _safe_float(aligned["adj_close"].min())
        distance_to_52w_high = None
        distance_from_52w_low = None
        high_ratio = _safe_divide(close, hhv_252)
        if high_ratio is not None:
            distance_to_52w_high = 1.0 - high_ratio
        low_ratio = _safe_divide(close, llv_252)
        if low_ratio is not None:
            distance_from_52w_low = low_ratio - 1.0

        hidden_rs_profile = leader_algorithms.hidden_rs_profile_from_aligned(aligned)

        ma200_slope_20d = None
        if len(aligned) >= 221:
            prev_ma200 = _safe_float(aligned["ma200"].iloc[-21])
            ma200_ratio = _safe_divide(ma200, prev_ma200)
            if ma200_ratio is not None:
                ma200_slope_20d = ma200_ratio - 1.0

        distance_from_ma50 = None
        if close is not None and ma50 is not None and ma50 != 0:
            close_to_ma50 = _safe_divide(close, ma50)
            if close_to_ma50 is not None:
                distance_from_ma50 = abs(close_to_ma50 - 1.0)

        trend_integrity_score = _weighted_mean(
            [
                (100.0 if close is not None and ma50 is not None and close > ma50 else 0.0, 0.20),
                (100.0 if ma50 is not None and ma150 is not None and ma50 > ma150 else 0.0, 0.20),
                (100.0 if ma150 is not None and ma200 is not None and ma150 > ma200 else 0.0, 0.20),
                ((_score_ratio(ma200_slope_20d, 0.01, -0.01) * 100.0), 0.20),
                (
                    (
                        _score_inverse(distance_from_ma50, 0.12, 0.30)
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
        latest_volume = _safe_float(latest["volume"])
        if latest_volume is not None and adv50 is not None and adv50 != 0:
            rvol = latest_volume / adv50
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
            gap_ratio = _safe_divide(open_price, prev_close)
            if gap_ratio is not None:
                gap_pct = gap_ratio - 1.0
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
        atr20_ratio = _safe_divide(atr20, close)
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
            "rs_rank_proxy_raw": rs_proxy_profile.get("rs_rank_proxy"),
            "rs_proxy_sample_count": rs_proxy_profile.get("rs_proxy_sample_count"),
            "rs_proxy_component_coverage": rs_proxy_profile.get("rs_proxy_component_coverage"),
            "rs_proxy_confidence": rs_proxy_profile.get("rs_proxy_confidence"),
            "mom_12_1": mom_12_1,
            "rs_line": rs_line_last,
            "rs_line_slope": rs_line_20d_slope,
            "rs_line_20d_slope": rs_line_20d_slope,
            "rs_line_65d_high_flag": rs_line_65d_high_flag,
            "rs_line_250d_high_flag": rs_line_250d_high_flag,
            "rs_new_high_before_price_flag": rs_new_high_before_price_flag,
            "rs_line_distance_to_high": rs_line_distance_to_high,
            "benchmark_relative_strength": benchmark_relative_strength,
            "benchmark_relative_strength_slope": benchmark_relative_strength_slope,
            "hidden_rs_raw": hidden_rs_profile.get("hidden_rs_raw"),
            "hidden_rs_weak_day_count": hidden_rs_profile.get("hidden_rs_weak_day_count"),
            "hidden_rs_down_day_excess_return": hidden_rs_profile.get("hidden_rs_down_day_excess_return"),
            "hidden_rs_drawdown_resilience": hidden_rs_profile.get("hidden_rs_drawdown_resilience"),
            "hidden_rs_weak_window_excess_return": hidden_rs_profile.get("hidden_rs_weak_window_excess_return"),
            "hidden_rs_confidence": hidden_rs_profile.get("hidden_rs_confidence"),
            "distance_to_52w_high": distance_to_52w_high,
            "distance_from_52w_low": distance_from_52w_low,
            "distance_from_ma50": distance_from_ma50,
            "ret_20d": self._compute_subperiod_return(aligned["adj_close"], 0, 20),
            "ret_60d": self._compute_subperiod_return(aligned["adj_close"], 0, 60),
            "ret_120d": self._compute_subperiod_return(aligned["adj_close"], 0, 120),
            "ret_252d": self._compute_subperiod_return(aligned["adj_close"], 0, 252),
            "adv20": adv20,
            "adv50": adv50,
            "traded_value": traded_value,
            "traded_value_20d": traded_value_20d,
            "traded_value_50d": _safe_float(latest["traded_value_50d"]),
            "atr20_pct": (atr20_ratio * 100.0) if atr20_ratio is not None else None,
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
            "box_high": structure["box_high"],
            "box_low": structure["box_low"],
            "box_valid": bool(structure["box_valid"]),
            "breakout_confirmed": bool(structure["breakout_confirmed"]),
            "structure_readiness_score": structure["structure_readiness_score"],
            "breakout_confirmation_score": structure["breakout_confirmation_score"],
            "dry_volume_ratio": structure.get("dry_volume_ratio"),
            "box_touch_count": structure.get("box_touch_count"),
            "support_hold_count": structure.get("support_hold_count"),
            "dry_volume_score": structure.get("dry_volume_score"),
            "failed_breakout_risk_score": structure.get("failed_breakout_risk_score"),
            "breakout_quality_score": structure.get("breakout_quality_score"),
            "structure_confidence": structure.get("structure_confidence"),
            "base_depth_pct": structure.get("base_depth_pct"),
            "loose_base_risk_score": structure.get("loose_base_risk_score"),
            "support_violation_count": structure.get("support_violation_count"),
            "breakout_failure_count": structure.get("breakout_failure_count"),
            "breakout_volume_quality_score": structure.get("breakout_volume_quality_score"),
            "structure_reject_reason_codes": structure.get("structure_reject_reason_codes"),
            "event_gap_up_flag": event_gap_up_flag,
            "event_proxy_score": event_proxy_score,
            "market_cap": _safe_float(metadata.get("market_cap")),
            "industry_key": build_industry_key(sector, industry),
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

    def _source_evidence_tags_from_row(self, row: pd.Series) -> str:
        return leader_algorithms.source_evidence_tags_from_row(row)

    def finalize_feature_table(self, feature_table: pd.DataFrame) -> pd.DataFrame:
        if feature_table.empty:
            return feature_table
        table = self._apply_group_fallback(feature_table)
        for column in (
            "weighted_rs_raw",
            "weighted_rs_prev_raw",
            "rs_rank_proxy_raw",
            "rs_proxy_sample_count",
            "rs_proxy_component_coverage",
            "rs_proxy_confidence",
            "mom_12_1",
            "traded_value_20d",
            "illiq",
            "ret_20d",
            "ret_60d",
            "ret_120d",
            "ret_252d",
            "trend_integrity_score",
            "structure_quality_score",
            "structure_readiness_score",
            "breakout_confirmation_score",
            "box_touch_count",
            "support_hold_count",
            "dry_volume_score",
            "failed_breakout_risk_score",
            "breakout_quality_score",
            "structure_confidence",
            "base_depth_pct",
            "loose_base_risk_score",
            "support_violation_count",
            "breakout_failure_count",
            "breakout_volume_quality_score",
            "event_proxy_score",
            "benchmark_relative_strength",
            "benchmark_relative_strength_slope",
            "hidden_rs_raw",
            "hidden_rs_weak_day_count",
            "hidden_rs_down_day_excess_return",
            "hidden_rs_drawdown_resilience",
            "hidden_rs_weak_window_excess_return",
            "hidden_rs_confidence",
            "distance_from_ma50",
            "distance_to_52w_high",
            "rvol",
        ):
            table[column] = _numeric_frame_column(table, column)

        table["rs_rank"] = _percentile_series(table["weighted_rs_raw"])
        table["rs_rank_true"] = table["rs_rank"]
        table["prev_rs_rank"] = _percentile_series(table["weighted_rs_prev_raw"])
        rank_delta = table["rs_rank"] - table["prev_rs_rank"]
        raw_delta = table["weighted_rs_raw"].fillna(0.0) - table["weighted_rs_prev_raw"].fillna(0.0)
        table["delta_rs_rank_qoq"] = np.where(rank_delta.abs() >= 0.5, rank_delta, raw_delta)
        table["weighted_rs_score"] = table["weighted_rs_raw"]
        table["rs_rank_proxy"] = table["rs_rank_proxy_raw"].where(
            table["rs_rank_proxy_raw"].notna(),
            table["weighted_rs_raw"].apply(
                lambda value: 1.0 + (98.0 * _clamp(((_safe_float(value) or 0.0) + 30.0) / 60.0))
            ),
        )
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
        table["momentum_persistence_score"] = table["norm_momentum_score"]
        table["near_high_leadership_score"] = table["distance_to_52w_high"].apply(
            lambda value: _clamp(1.0 - _coalesce(_safe_float(value), 1.0)) * 100.0
        )
        table["hidden_rs_score"] = _zscore_to_percent(_winsorized_zscore(table["hidden_rs_raw"]), higher_is_better=True)
        table["rs_line_score"] = table.apply(
            lambda row: _weighted_mean(
                [
                    ((_score_ratio(_safe_float(row.get("rs_line_20d_slope")), 0.05, -0.05) * 100.0), 0.30),
                    ((_score_inverse(_safe_float(row.get("rs_line_distance_to_high")), 0.02, 0.20) * 100.0), 0.20),
                    ((_score_ratio((_safe_float(row.get("benchmark_relative_strength")) or 0.0) - 1.0, 0.05, -0.10) * 100.0), 0.20),
                    (100.0 if bool(row.get("rs_line_250d_high_flag")) else 0.0, 0.15),
                    (100.0 if bool(row.get("rs_new_high_before_price_flag")) else 0.0, 0.15),
                ]
            ),
            axis=1,
        )
        table["rs_quality_score"] = table.apply(
            lambda row: _weighted_mean(
                [
                    (_safe_float(row.get("rs_rank_true")), 0.45),
                    (_safe_float(row.get("rs_line_score")), 0.35),
                    ((_score_ratio((_safe_float(row.get("benchmark_relative_strength")) or 0.0) - 1.0, 0.05, -0.10) * 100.0), 0.20),
                ]
            ),
            axis=1,
        )
        table["leadership_freshness_score"] = table.apply(
            lambda row: _weighted_mean(
                [
                    (100.0 if bool(row.get("rs_new_high_before_price_flag")) else 20.0, 0.45),
                    ((_score_ratio(_safe_float(row.get("rs_line_slope")), 0.04, -0.02) * 100.0), 0.30),
                    ((_score_ratio(_safe_float(row.get("delta_rs_rank_qoq")), 8.0, -5.0) * 100.0), 0.25),
                ]
            ),
            axis=1,
        )
        table["extension_risk_score"] = table.apply(
            lambda row: _weighted_mean(
                [
                    ((_score_ratio(_safe_float(row.get("ret_20d")), 0.25, 0.05) * 100.0), 0.30),
                    ((_score_ratio(_safe_float(row.get("distance_from_ma50")), 0.18, 0.04) * 100.0), 0.30),
                    ((_score_inverse(_safe_float(row.get("distance_to_52w_high")), 0.02, 0.15) * 100.0), 0.20),
                    ((_score_ratio(_safe_float(row.get("rvol")), 2.50, 1.00) * 100.0), 0.20),
                ]
            ),
            axis=1,
        )
        table["early_leader_score"] = table.apply(
            lambda row: _weighted_mean(
                [
                    (_safe_float(row.get("rs_quality_score")), 0.35),
                    (_safe_float(row.get("leadership_freshness_score")), 0.35),
                    (_safe_float(row.get("hidden_rs_score")), 0.15),
                    (100.0 - (_safe_float(row.get("extension_risk_score")) or 0.0), 0.15),
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
        table["leader_rs_state"] = table.apply(leader_algorithms.leader_rs_state_from_row, axis=1)
        table["fading_risk_score"] = table.apply(
            lambda row: _weighted_mean(
                [
                    ((_score_ratio(-(_safe_float(row.get("delta_rs_rank_qoq")) or 0.0), 8.0, -2.0) * 100.0), 0.35),
                    ((_score_ratio(-(_safe_float(row.get("rs_line_slope")) or 0.0), 0.03, -0.01) * 100.0), 0.30),
                    (100.0 - (_safe_float(row.get("structure_readiness_score")) or 0.0), 0.20),
                    (_safe_float(row.get("extension_risk_score")), 0.15),
                ]
            ),
            axis=1,
        )
        table["source_evidence_tags"] = table.apply(self._source_evidence_tags_from_row, axis=1)
        table["risk_flag"] = table.apply(
            lambda row: "EXTENDED"
            if (_safe_float(row.get("extension_risk_score")) or 0.0) >= 75.0
            else "",
            axis=1,
        )
        return table

    def compute_group_table(self, feature_table: pd.DataFrame) -> pd.DataFrame:
        if feature_table.empty:
            return pd.DataFrame()

        table = feature_table.copy()
        if "industry_key" not in table.columns:
            table["industry_key"] = table.apply(
                lambda row: build_industry_key(row.get("sector"), row.get("industry")),
                axis=1,
            )

        group_table = (
            table.groupby("industry_key", dropna=False)
            .agg(
                sector=("sector", lambda values: next((text for item in values if (text := _safe_text(item))), "")),
                industry=("industry", lambda values: next((text for item in values if (text := _safe_text(item))), "")),
                group_name=("group_name", lambda values: next((text for item in values if (text := _safe_text(item))), "")),
                group_member_count=("symbol", "count"),
                group_return_20d=("ret_20d", "mean"),
                group_return_60d=("ret_60d", "mean"),
                group_benchmark_relative_strength=("benchmark_relative_strength", "mean"),
                group_rs_slope_20d=("rs_line_20d_slope", "mean"),
                group_pct_above_50dma=("close_gt_50", "mean"),
                group_new_high_share=("distance_to_52w_high", lambda s: float((pd.to_numeric(s, errors="coerce") <= 0.08).mean())),
                core_group_strength_score=("core_group_strength_score", lambda values: next((value for value in (_safe_float(item) for item in values) if value is not None), None)),
                core_group_rank=("core_group_rank", lambda values: next((value for value in (_safe_float(item) for item in values) if value is not None), None)),
                core_group_state=("core_group_state", lambda values: next((text for item in values if (text := _safe_text(item))), "")),
            )
            .reset_index()
        )
        group_table["group_return_20d_score"] = _zscore_to_percent(_winsorized_zscore(group_table["group_return_20d"]), higher_is_better=True)
        group_table["group_return_60d_score"] = _zscore_to_percent(_winsorized_zscore(group_table["group_return_60d"]), higher_is_better=True)
        group_table["group_relative_strength_score"] = _zscore_to_percent(_winsorized_zscore(group_table["group_benchmark_relative_strength"]), higher_is_better=True)
        group_table["group_rs_slope_score"] = _zscore_to_percent(_winsorized_zscore(group_table["group_rs_slope_20d"]), higher_is_better=True)
        group_table["group_overlay_score"] = group_table.apply(
            lambda row: _weighted_mean(
                [
                    (_safe_float(row.get("group_relative_strength_score")), 0.25),
                    (_safe_float(row.get("group_return_20d_score")), 0.20),
                    (_safe_float(row.get("group_return_60d_score")), 0.15),
                    (_safe_float(row.get("group_rs_slope_score")), 0.15),
                    (((_safe_float(row.get("group_pct_above_50dma")) or 0.0) * 100.0), 0.15),
                    (((_safe_float(row.get("group_new_high_share")) or 0.0) * 100.0), 0.10),
                ]
            ),
            axis=1,
        )
        group_table["group_strength_score"] = pd.to_numeric(
            group_table["core_group_strength_score"],
            errors="coerce",
        ).fillna(0.0)
        group_table["industry_rs_pct"] = group_table["group_strength_score"]
        group_table["sector_rs_pct"] = group_table["group_overlay_score"]
        group_table["group_state"] = group_table["core_group_state"].map(_safe_text)
        group_table["group_rank"] = pd.to_numeric(
            group_table["core_group_rank"],
            errors="coerce",
        ).fillna(999.0)
        group_table = group_table.sort_values(
            ["group_strength_score", "group_overlay_score", "group_return_60d"],
            ascending=[False, False, False],
        ).reset_index(drop=True)
        missing_rank_mask = pd.to_numeric(group_table["group_rank"], errors="coerce").fillna(999.0) >= 999.0
        if bool(missing_rank_mask.any()):
            fallback_ranks = pd.Series(
                np.arange(1, int(missing_rank_mask.sum()) + 1, dtype=float),
                index=group_table.index[missing_rank_mask],
            )
            group_table.loc[missing_rank_mask, "group_rank"] = fallback_ranks
        return group_table

    def compute_market_context(
        self,
        *,
        market: str,
        benchmark_symbol: str,
        benchmark_daily: pd.DataFrame,
        feature_table: pd.DataFrame,
        group_table: pd.DataFrame,
        market_truth: MarketTruthSnapshot | None = None,
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
                top_groups = set(group_table.head(top_cutoff)["industry_key"].astype(str))
                top_group_share = float(feature_table["industry_key"].astype(str).isin(top_groups).mean() * 100.0)

        if market_truth is not None:
            regime_state = shared_market_alias_to_leader_lagging_state(market_truth.market_alias)
            score_multiplier = 1.0 if market_truth.market_alias == "RISK_ON" else 0.88 if market_truth.market_alias == "NEUTRAL" else 0.70
            reasons = [f"CORE_{market_truth.market_alias}"]
            if market_truth.market_state:
                reasons.append(f"MARKET_{market_truth.market_state.upper()}")
            if market_truth.breadth_state:
                reasons.append(f"BREADTH_{market_truth.breadth_state.upper()}")
            if market_truth.rotation_support_score is not None and market_truth.rotation_support_score >= 70.0:
                reasons.append("CORE_ROTATION_SUPPORTIVE")
            if market_truth.leader_health_score is not None and market_truth.leader_health_score >= 70.0:
                reasons.append("CORE_LEADERS_HEALTHY")
            return MarketContext(
                benchmark_symbol=str(benchmark_symbol or "").upper(),
                regime_state=regime_state,
                regime_score=round(market_truth.market_alignment_score or 55.0, 2),
                breadth_50=_round_or_none(breadth_50),
                breadth_200=_round_or_none(breadth_200),
                high_low_ratio=_round_or_none(high_low_ratio),
                top_group_share=_round_or_none(top_group_share),
                score_multiplier=score_multiplier,
                reason_codes=tuple(_unique(reasons)),
                market_alignment_score=_round_or_none(market_truth.market_alignment_score),
                breadth_support_score=_round_or_none(market_truth.breadth_support_score),
                rotation_support_score=_round_or_none(market_truth.rotation_support_score),
                leader_health_score=_round_or_none(market_truth.leader_health_score),
                market_alias=market_truth.market_alias,
            )

        trend_score = 50.0
        if not benchmark.empty and len(benchmark) >= 220:
            benchmark["ma50"] = rolling_sma(benchmark["adj_close"], 50, min_periods=20)
            benchmark["ma200"] = rolling_sma(benchmark["adj_close"], 200, min_periods=80)
            close = _safe_float(benchmark["adj_close"].iloc[-1])
            ma50 = _safe_float(benchmark["ma50"].iloc[-1])
            ma200 = _safe_float(benchmark["ma200"].iloc[-1])
            slope200 = None
            if len(benchmark) >= 221:
                prev_ma200 = _safe_float(benchmark["ma200"].iloc[-21])
                slope_ratio = _safe_divide(ma200, prev_ma200)
                if slope_ratio is not None:
                    slope200 = slope_ratio - 1.0
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
            market_alias = "RISK_ON"
        elif regime_score >= 55.0:
            regime_state = "Neutral"
            score_multiplier = 0.88
            market_alias = "NEUTRAL"
        else:
            regime_state = "Risk-Off"
            score_multiplier = 0.70
            market_alias = "RISK_OFF"

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
            market_alignment_score=_round_or_none(regime_score),
            breadth_support_score=_round_or_none(breadth_score),
            rotation_support_score=_round_or_none(trend_score),
            leader_health_score=_round_or_none(top_group_share if top_group_share is not None else breadth_score),
            market_alias=market_alias,
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
                    "industry_key",
                    "industry_rs_pct",
                    "sector_rs_pct",
                    "group_strength_score",
                    "group_rank",
                    "group_state",
                    "group_overlay_score",
                    "group_new_high_share",
                ]
            ],
            on="industry_key",
            how="left",
        ).copy()
        breadth_context_score = 100.0 if market_context.regime_state == "Risk-On" else 65.0 if market_context.regime_state == "Neutral" else 35.0
        leader_rows: list[dict[str, Any]] = []
        for _, row in table.iterrows():
            reasons: list[str] = []
            rs_rank_value = _safe_float(row.get("rs_rank")) or 0.0
            tier1_boosts = sum(
                [
                    rs_rank_value >= 92.0,
                    bool(row.get("rs_line_250d_high_flag")),
                    bool(row.get("rs_new_high_before_price_flag")),
                    (_safe_float(row.get("rvol")) or 0.0) >= 1.5,
                    (_safe_float(row.get("group_new_high_share")) or 0.0) >= 0.25,
                ]
            )
            leader_score = leader_algorithms.leader_score_v2(
                row,
                breadth_context_score=breadth_context_score,
                score_multiplier=market_context.score_multiplier,
            )
            classification = leader_algorithms.classify_leader(
                row,
                calibration=calibration_map,
                traded_value_floor=self.market_profile(row.get("market", "us")).traded_value_floor,
                leader_score=leader_score,
            )
            hard_gate = classification.hard_gate_pass
            extended = classification.extended_flag
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
            if (_safe_float(row.get("hidden_rs_score")) or 0.0) >= 65.0:
                reasons.append("HIDDEN_RS")
            if bool(row.get("breakout_confirmed")):
                reasons.append("BREAKOUT_CONFIRMED")

            leader_rows.append(
                {
                    "ticker": row.get("symbol"),
                    "symbol": row.get("symbol"),
                    "market": row.get("market"),
                    "sector": row.get("sector"),
                    "industry": row.get("industry"),
                    "industry_key": row.get("industry_key"),
                    "group_name": row.get("group_name"),
                    "leader_score": _round_or_none(leader_score),
                    "rs_rank": _round_or_none(_safe_float(row.get("rs_rank"))),
                    "weighted_rs_score": _round_or_none(_safe_float(row.get("weighted_rs_score"))),
                    "rs_rank_true": _round_or_none(_safe_float(row.get("rs_rank_true"))),
                    "rs_rank_proxy": _round_or_none(_safe_float(row.get("rs_rank_proxy"))),
                    "rs_proxy_sample_count": int(sample_count) if (sample_count := _safe_float(row.get("rs_proxy_sample_count"))) is not None else None,
                    "rs_proxy_component_coverage": int(component_coverage) if (component_coverage := _safe_float(row.get("rs_proxy_component_coverage"))) is not None else None,
                    "rs_proxy_confidence": _round_or_none(_safe_float(row.get("rs_proxy_confidence"))),
                    "rs_line_20d_slope": _round_or_none(_safe_float(row.get("rs_line_20d_slope"))),
                    "rs_new_high_before_price_flag": bool(row.get("rs_new_high_before_price_flag")),
                    "distance_to_52w_high": _round_or_none(_coalesce(_safe_float(row.get("distance_to_52w_high")), 0.0) * 100.0),
                    "group_rank": int(group_rank) if (group_rank := _safe_float(row.get("group_rank"))) is not None else None,
                    "top_reason_1": reasons[0] if reasons else "",
                    "top_reason_2": reasons[1] if len(reasons) > 1 else "",
                    "reason_codes": _unique(reasons),
                    "hard_gate_pass": hard_gate,
                    "hybrid_gate_pass": classification.hybrid_gate_pass,
                    "strict_rs_gate_pass": classification.strict_rs_gate_pass,
                    "extended_flag": extended,
                    "leader_tier": classification.leader_tier,
                    "entry_suitability": classification.entry_suitability,
                    "label": classification.label,
                    "legacy_label": classification.legacy_label,
                    "leader_sort_score": _round_or_none(classification.leader_sort_score),
                    "phase_bucket": classification.phase_bucket,
                    "tier1_boost_count": int(tier1_boosts),
                    "group_strength_score": _round_or_none(_safe_float(row.get("group_strength_score"))),
                    "trend_integrity_score": _round_or_none(_safe_float(row.get("trend_integrity_score"))),
                    "structure_quality_score": _round_or_none(_safe_float(row.get("structure_quality_score"))),
                    "structure_readiness_score": _round_or_none(_safe_float(row.get("structure_readiness_score"))),
                    "breakout_confirmation_score": _round_or_none(_safe_float(row.get("breakout_confirmation_score"))),
                    "rs_line_score": _round_or_none(_safe_float(row.get("rs_line_score"))),
                    "rs_quality_score": _round_or_none(_safe_float(row.get("rs_quality_score"))),
                    "leadership_freshness_score": _round_or_none(_safe_float(row.get("leadership_freshness_score"))),
                    "early_leader_score": _round_or_none(_safe_float(row.get("early_leader_score"))),
                    "momentum_persistence_score": _round_or_none(_safe_float(row.get("momentum_persistence_score"))),
                    "near_high_leadership_score": _round_or_none(_safe_float(row.get("near_high_leadership_score"))),
                    "hidden_rs_score": _round_or_none(_safe_float(row.get("hidden_rs_score"))),
                    "hidden_rs_weak_day_count": int(weak_day_count) if (weak_day_count := _safe_float(row.get("hidden_rs_weak_day_count"))) is not None else None,
                    "hidden_rs_down_day_excess_return": _round_or_none(_safe_float(row.get("hidden_rs_down_day_excess_return")), 6),
                    "hidden_rs_drawdown_resilience": _round_or_none(_safe_float(row.get("hidden_rs_drawdown_resilience")), 6),
                    "hidden_rs_weak_window_excess_return": _round_or_none(_safe_float(row.get("hidden_rs_weak_window_excess_return")), 6),
                    "hidden_rs_confidence": _round_or_none(_safe_float(row.get("hidden_rs_confidence"))),
                    "leader_rs_state": row.get("leader_rs_state"),
                    "fading_risk_score": _round_or_none(_safe_float(row.get("fading_risk_score"))),
                    "extension_risk_score": _round_or_none(_safe_float(row.get("extension_risk_score"))),
                    "source_evidence_tags": row.get("source_evidence_tags"),
                    "box_touch_count": int(box_touch_count) if (box_touch_count := _safe_float(row.get("box_touch_count"))) is not None else None,
                    "support_hold_count": int(support_hold_count) if (support_hold_count := _safe_float(row.get("support_hold_count"))) is not None else None,
                    "dry_volume_score": _round_or_none(_safe_float(row.get("dry_volume_score"))),
                    "failed_breakout_risk_score": _round_or_none(_safe_float(row.get("failed_breakout_risk_score"))),
                    "breakout_quality_score": _round_or_none(_safe_float(row.get("breakout_quality_score"))),
                    "structure_confidence": _round_or_none(_safe_float(row.get("structure_confidence"))),
                    "base_depth_pct": _round_or_none(_safe_float(row.get("base_depth_pct"))),
                    "loose_base_risk_score": _round_or_none(_safe_float(row.get("loose_base_risk_score"))),
                    "support_violation_count": int(support_violation_count) if (support_violation_count := _safe_float(row.get("support_violation_count"))) is not None else None,
                    "breakout_failure_count": int(breakout_failure_count) if (breakout_failure_count := _safe_float(row.get("breakout_failure_count"))) is not None else None,
                    "breakout_volume_quality_score": _round_or_none(_safe_float(row.get("breakout_volume_quality_score"))),
                    "structure_reject_reason_codes": row.get("structure_reject_reason_codes"),
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
        return leaders.sort_values(
            ["leader_sort_score", "leader_score", "rs_rank"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

    def _pair_link_score(
        self,
        candidate: Mapping[str, Any],
        leader: Mapping[str, Any],
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

    def _lagged_return_profile(
        self,
        candidate_frame: pd.DataFrame,
        leader_frame: pd.DataFrame,
        *,
        frames_are_normalized: bool = False,
    ) -> dict[str, Any]:
        return follower_algorithms.lagged_return_profile(
            candidate_frame if frames_are_normalized else self.normalize_daily_frame(candidate_frame),
            leader_frame if frames_are_normalized else self.normalize_daily_frame(leader_frame),
            frames_are_normalized=frames_are_normalized,
        )

    def filter_leaders_for_follower_analysis(self, leaders: pd.DataFrame) -> pd.DataFrame:
        if leaders.empty:
            return leaders.copy()
        leader_pool = leaders.copy()
        mask = pd.Series(True, index=leader_pool.index)
        if "label" in leader_pool.columns:
            mask &= leader_pool["label"].fillna("").astype(str) != "reject"
        if "leader_tier" in leader_pool.columns:
            mask &= leader_pool["leader_tier"].fillna("").astype(str).isin({"strong", "emerging"})
        if "entry_suitability" in leader_pool.columns:
            mask &= leader_pool["entry_suitability"].fillna("").astype(str) != "avoid"
        return leader_pool.loc[mask].copy()

    def analyze_followers(
        self,
        *,
        feature_table: pd.DataFrame,
        leaders: pd.DataFrame,
        group_table: pd.DataFrame,
        market_context: MarketContext,
        frames: dict[str, pd.DataFrame],
        calibration: dict[str, float] | None = None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
        runtime_context: RuntimeContext | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        self.last_follower_lag_pruning = {}
        if feature_table.empty or leaders.empty:
            return pd.DataFrame(), pd.DataFrame()

        calibration_map = dict(calibration or self.build_actual_data_calibration(feature_table=feature_table, group_table=group_table))
        mode = self._follower_analysis_mode()
        balanced_mode = mode == "balanced"
        max_leaders_per_industry = self._env_positive_int(
            "INVEST_PROTO_FOLLOWER_MAX_LEADERS_PER_INDUSTRY",
            self.DEFAULT_FOLLOWER_MAX_LEADERS_PER_INDUSTRY,
        )
        max_pairs_per_candidate = self._env_positive_int(
            "INVEST_PROTO_FOLLOWER_MAX_PAIRS_PER_CANDIDATE",
            self.DEFAULT_FOLLOWER_MAX_PAIRS_PER_CANDIDATE,
        )

        leader_pool = self.filter_leaders_for_follower_analysis(leaders)
        if leader_pool.empty:
            return pd.DataFrame(), pd.DataFrame()

        table = feature_table.merge(
            group_table[
                ["industry_key", "industry_rs_pct", "group_strength_score", "group_rank", "group_state", "group_overlay_score"]
            ],
            on="industry_key",
            how="left",
        ).copy()
        confirmed_symbols = set(leader_pool["symbol"].astype(str))
        follower_rows: list[dict[str, Any]] = []
        pair_rows: list[dict[str, Any]] = []
        breadth_context_score = 100.0 if market_context.regime_state == "Risk-On" else 65.0 if market_context.regime_state == "Neutral" else 35.0
        leader_records_by_industry: dict[str, list[dict[str, Any]]] = {}
        leader_records = sorted(leader_pool.to_dict(orient="records"), key=self._leader_follower_sort_key)
        for leader_record in leader_records:
            industry_key = _safe_text(leader_record.get("industry_key"))
            if not industry_key:
                continue
            leader_records_by_industry.setdefault(industry_key, []).append(leader_record)
        leader_pool_before_cap = sum(len(records) for records in leader_records_by_industry.values())
        if balanced_mode:
            leader_records_by_industry = {
                industry_key: records[:max_leaders_per_industry]
                for industry_key, records in leader_records_by_industry.items()
            }
        leader_pool_after_cap = sum(len(records) for records in leader_records_by_industry.values())
        candidate_records = table.to_dict(orient="records")
        selected_leader_records = [
            record
            for records in leader_records_by_industry.values()
            for record in records
        ]
        relevant_symbols = {
            _safe_text(record.get("symbol"))
            for record in [*candidate_records, *selected_leader_records]
            if _safe_text(record.get("symbol"))
        }
        lag_frame_lock = threading.Lock()
        lag_price_frames: dict[str, pd.DataFrame] = {}
        lag_frame_prepared_symbols: set[str] = set()
        lag_frame_cache_hits = 0
        lag_frame_precompute_seconds = 0.0

        def _prepare_lag_price_frame(symbol: str) -> pd.DataFrame:
            nonlocal lag_frame_cache_hits, lag_frame_precompute_seconds
            symbol_key = _safe_text(symbol).upper()
            if not symbol_key:
                return pd.DataFrame()
            with lag_frame_lock:
                cached = lag_price_frames.get(symbol_key)
                if cached is not None:
                    lag_frame_cache_hits += 1
                    return cached
                started = time.perf_counter()
                normalized = self.normalize_daily_frame(frames.get(symbol_key, pd.DataFrame()))
                prepared = follower_algorithms.prepare_lag_price_frame(
                    normalized,
                    frame_is_normalized=True,
                )
                lag_price_frames[symbol_key] = prepared
                lag_frame_prepared_symbols.add(symbol_key)
                lag_frame_precompute_seconds += time.perf_counter() - started
                return prepared

        if not balanced_mode:
            for symbol in sorted(relevant_symbols):
                _prepare_lag_price_frame(symbol)
        total_candidates = len(candidate_records)
        interval = progress_interval(total_candidates, target_updates=8, min_interval=50)
        pair_evaluations = 0
        pair_candidates = 0
        eligible_candidates = 0
        skipped_by_prefilter = 0
        worker_count = _runtime_worker_count(
            total_candidates,
            env_var="INVEST_PROTO_FOLLOWER_ANALYSIS_WORKERS",
            cap=self.DEFAULT_FOLLOWER_ANALYSIS_WORKER_CAP,
            runtime_context=runtime_context,
            scope="leader_lagging.follower_analysis",
        )

        def _emit_follower_progress(index: int, symbol: str) -> None:
            if progress_callback is None or not is_progress_tick(index, total_candidates, interval):
                return
            progress_callback(
                {
                    "processed": index,
                    "total": total_candidates,
                    "current_symbol": symbol,
                    "pair_evaluations": pair_evaluations,
                    "pair_candidates": pair_candidates,
                    "eligible_candidates": eligible_candidates,
                    "skipped_by_prefilter": skipped_by_prefilter,
                    "leader_pool_after_cap": leader_pool_after_cap,
                    "workers": worker_count,
                }
            )

        def _empty_candidate_result(symbol: str) -> dict[str, Any]:
            return {
                "symbol": symbol,
                "follower_rows": [],
                "pair_rows": [],
                "pair_candidates": 0,
                "pair_evaluations": 0,
                "eligible_candidates": 0,
                "skipped_by_prefilter": 0,
            }

        def _analyze_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
            symbol = _safe_text(candidate.get("symbol"))
            result = _empty_candidate_result(symbol)
            if symbol in confirmed_symbols:
                return result
            peers = leader_records_by_industry.get(_safe_text(candidate.get("industry_key")), [])
            if not peers:
                return result

            hard_precondition, prefilter_reasons = self._follower_hard_precondition(candidate, calibration_map)
            if balanced_mode and not hard_precondition:
                result["follower_rows"].append(
                    self._prefilter_reject_follower_row(candidate, reasons=prefilter_reasons)
                )
                result["skipped_by_prefilter"] = 1
                return result

            selected_peers = self._rank_follower_peers(
                candidate,
                peers,
                max_pairs=max_pairs_per_candidate,
                balanced=balanced_mode,
            )
            if not selected_peers:
                return result

            best_payload: dict[str, Any] | None = None
            candidate_frame = _prepare_lag_price_frame(symbol)
            result["pair_candidates"] = len(selected_peers)
            result["eligible_candidates"] = 1
            for leader in selected_peers:
                leader_symbol = _safe_text(leader.get("symbol"))
                leader_frame = _prepare_lag_price_frame(leader_symbol)
                lag_profile = follower_algorithms.lagged_return_profile_from_price_frames(
                    candidate_frame,
                    leader_frame,
                )
                result["pair_evaluations"] += 1
                lagged_corr = _safe_float(lag_profile.get("lagged_corr"))
                corr_score = _score_ratio(lagged_corr, 0.70, 0.20) * 100.0
                pair_link_score = self._pair_link_score(candidate, leader, corr_score)
                lag_evidence_confidence = _safe_float(lag_profile.get("pair_evidence_confidence")) or 0.0
                pair_evidence_confidence = _weighted_mean(
                    [
                        (lag_evidence_confidence, 0.55),
                        (pair_link_score, 0.30),
                        (corr_score, 0.15),
                    ]
                )
                peer_lead_score = max(lagged_corr or 0.0, 0.0) * pair_link_score
                leader_ret_20d = _safe_float(leader.get("ret_20d")) or 0.0
                candidate_ret_20d = _safe_float(candidate.get("ret_20d")) or 0.0
                leader_ret_60d = _safe_float(leader.get("ret_60d")) or 0.0
                candidate_ret_60d = _safe_float(candidate.get("ret_60d")) or 0.0
                leader_gap_20d = leader_ret_20d - candidate_ret_20d
                leader_gap_60d = leader_ret_60d - candidate_ret_60d
                propagation_ratio = None
                if leader_ret_20d > 0:
                    propagation_ratio = candidate_ret_20d / leader_ret_20d
                propagation_state = follower_algorithms.propagation_state(propagation_ratio)
                structure_preservation_score = _weighted_mean(
                    [
                        (_safe_float(candidate.get("trend_integrity_score")), 0.35),
                        (_safe_float(candidate.get("structure_readiness_score")), 0.30),
                        (100.0 if bool(candidate.get("close_gt_50")) else 20.0, 0.20),
                        ((100.0 - (_safe_float(candidate.get("extension_risk_score")) or 0.0)), 0.15),
                    ]
                )
                catchup_score = follower_algorithms.catchup_room_score(
                    leader_gap_20d,
                    leader_gap_60d,
                    propagation_ratio,
                )
                rs_inflection_score = _weighted_mean(
                    [
                        ((_score_ratio(_safe_float(candidate.get("rs_line_20d_slope")), 0.03, -0.03) * 100.0), 0.35),
                        ((_score_ratio(_safe_float(candidate.get("benchmark_relative_strength_slope")), 0.03, -0.03) * 100.0), 0.30),
                        ((_score_ratio(_safe_float(candidate.get("delta_rs_rank_qoq")), 8.0, -5.0) * 100.0), 0.15),
                        (100.0 if bool(candidate.get("rs_line_65d_high_flag")) else 35.0, 0.10),
                        (100.0 if bool(candidate.get("rs_new_high_before_price_flag")) else 35.0, 0.10),
                    ]
                )
                underreaction_score = _weighted_mean(
                    [
                        (catchup_score, 0.55),
                        (structure_preservation_score, 0.25),
                        (pair_link_score, 0.20),
                    ]
                )
                catalyst_sympathy_score = _weighted_mean(
                    [
                        (_safe_float(leader.get("event_proxy_score")), 0.60),
                        ((_score_ratio(_safe_float(candidate.get("ret_20d")), 0.05, -0.05) * 100.0), 0.40),
                    ]
                )
                sympathy_freshness_score = _weighted_mean(
                    [
                        (100.0 if leader_ret_20d >= 0.08 or (_safe_float(leader.get("event_proxy_score")) or 0.0) >= 65.0 else 35.0, 0.35),
                        (rs_inflection_score, 0.35),
                        (100.0 - (_safe_float(candidate.get("extension_risk_score")) or 0.0), 0.30),
                    ]
                )
                follower_score = _weighted_mean(
                    [
                        (pair_link_score, 0.22),
                        (peer_lead_score, 0.03),
                        (_safe_float(candidate.get("group_strength_score")), 0.18),
                        (underreaction_score, 0.20),
                        (structure_preservation_score, 0.12),
                        (rs_inflection_score, 0.11),
                        (sympathy_freshness_score, 0.08),
                        (_safe_float(candidate.get("volume_demand_score")), 0.02),
                        (_safe_float(candidate.get("liquidity_quality_score")), 0.02),
                        (breadth_context_score, 0.02),
                    ]
                ) * market_context.score_multiplier

                payload = {
                    "linked_leader": leader.get("symbol"),
                    "leader_label": leader.get("label"),
                    "pair_link_score": pair_link_score,
                    "peer_lead_score": peer_lead_score,
                    "leader_gap_20d": leader_gap_20d,
                    "leader_gap_60d": leader_gap_60d,
                    "leader_event_return": leader_ret_20d,
                    "follower_event_return": candidate_ret_20d,
                    "propagation_ratio": propagation_ratio,
                    "underreaction_score": underreaction_score,
                    "rs_inflection_score": rs_inflection_score,
                    "structure_preservation_score": structure_preservation_score,
                    "catalyst_sympathy_score": catalyst_sympathy_score,
                    "sympathy_freshness_score": sympathy_freshness_score,
                    "follower_score": follower_score,
                    "best_lag_days": lag_profile.get("lag_days"),
                    "lagged_corr": lagged_corr,
                    "lead_lag_profile": lag_profile.get("lead_lag_profile"),
                    "lag_profile_sample_count": lag_profile.get("lag_profile_sample_count"),
                    "lag_profile_stability_score": lag_profile.get("lag_profile_stability_score"),
                    "pair_evidence_confidence": pair_evidence_confidence,
                    "catchup_room_score": catchup_score,
                    "propagation_state": propagation_state,
                    "follower_reject_reason_codes": lag_profile.get("follower_reject_reason_codes"),
                }
                if best_payload is None or (payload["follower_score"] or 0.0) > (best_payload["follower_score"] or 0.0):
                    best_payload = payload

            if best_payload is None:
                return result

            candidate_industry_rs_pct = _safe_float(candidate.get("industry_rs_pct")) or 0.0
            candidate_distance_to_high = _coalesce(_safe_float(candidate.get("distance_to_52w_high")), 1.0)
            candidate_rs_rank = _safe_float(candidate.get("rs_rank")) or 0.0
            candidate_delta_rs_rank = _safe_float(candidate.get("delta_rs_rank_qoq")) or -999.0
            candidate_traded_value_20d = _safe_float(candidate.get("traded_value_20d")) or 0.0
            candidate_ma200_slope_20d = _safe_float(candidate.get("ma200_slope_20d")) or -1.0
            candidate_illiq_score = _safe_float(candidate.get("illiq_score")) or 0.0
            best_leader_gap_60d = _safe_float(best_payload.get("leader_gap_60d")) or 0.0
            best_pair_link_score = _safe_float(best_payload.get("pair_link_score")) or 0.0

            hard_precondition, _ = self._follower_hard_precondition(candidate, calibration_map)
            hygiene_pass = all(
                [
                    bool(candidate.get("close_gt_200")) or candidate_ma200_slope_20d > 0.0,
                    candidate_distance_to_high <= calibration_map["follower_hygiene_distance_to_high_max"],
                    candidate_illiq_score >= 20.0,
                    best_leader_gap_60d <= calibration_map["follower_leader_gap_60d_max"],
                ]
            )

            reasons: list[str] = []
            warnings: list[str] = []
            if best_pair_link_score >= calibration_map["follower_pair_link_min"]:
                reasons.append("STRONG_PAIR_LINK")
            if (_safe_float(best_payload.get("underreaction_score")) or 0.0) >= calibration_map["follower_underreaction_min"]:
                reasons.append("CATCH_UP_POTENTIAL")
            if (_safe_float(best_payload.get("rs_inflection_score")) or 0.0) >= calibration_map["follower_rs_inflection_min"]:
                reasons.append("RS_INFLECTING")
            if (_safe_float(candidate.get("group_strength_score")) or 0.0) >= 75.0:
                reasons.append("STRONG_GROUP")
            if (_safe_float(best_payload.get("catalyst_sympathy_score")) or 0.0) >= 65.0:
                reasons.append("SYMPATHY_SETUP")
            link_tags = ["same_industry"]
            if best_payload.get("best_lag_days") is not None:
                link_tags.append(f"best_lag_{int(best_payload['best_lag_days'])}")
            if (_safe_float(best_payload.get("peer_lead_score")) or 0.0) >= 50.0:
                link_tags.append("positive_peer_lead")
            if (_safe_float(best_payload.get("pair_evidence_confidence")) or 0.0) >= 50.0:
                link_tags.append("stable_lag_profile")
            if (_safe_float(best_payload.get("underreaction_score")) or 0.0) >= calibration_map["follower_underreaction_min"]:
                link_tags.append("underreaction")
            if str(best_payload.get("propagation_state") or "") == "early_response":
                link_tags.append("early_response")

            follower_diagnostics = {
                "pair_evidence_confidence": best_payload.get("pair_evidence_confidence"),
                "lag_profile_stability_score": best_payload.get("lag_profile_stability_score"),
                "catchup_room_score": best_payload.get("catchup_room_score"),
                "propagation_state": best_payload.get("propagation_state"),
                "structure_preservation_score": best_payload.get("structure_preservation_score"),
                "rs_inflection_score": best_payload.get("rs_inflection_score"),
                "liquidity_quality_score": candidate.get("liquidity_quality_score"),
                "pair_link_score": best_payload.get("pair_link_score"),
            }
            follower_confidence = follower_algorithms.follower_confidence_score(follower_diagnostics)
            follower_reject_reasons = follower_algorithms.follower_reject_reason_codes(
                follower_diagnostics,
                str(best_payload.get("follower_reject_reason_codes") or ""),
            )

            follower_row = {
                "ticker": symbol,
                "symbol": symbol,
                "linked_leader": best_payload.get("linked_leader"),
                "market": candidate.get("market"),
                "sector": candidate.get("sector"),
                "industry": candidate.get("industry"),
                "industry_key": candidate.get("industry_key"),
                "group_name": candidate.get("group_name"),
                "follower_score": _round_or_none(_safe_float(best_payload.get("follower_score"))),
                "pair_link_score": _round_or_none(_safe_float(best_payload.get("pair_link_score"))),
                "peer_lead_score": _round_or_none(_safe_float(best_payload.get("peer_lead_score"))),
                "best_lag_days": int(best_lag) if (best_lag := _safe_float(best_payload.get("best_lag_days"))) is not None else None,
                "lagged_corr": _round_or_none(_safe_float(best_payload.get("lagged_corr")), digits=4),
                "lag_profile_sample_count": int(lag_sample_count) if (lag_sample_count := _safe_float(best_payload.get("lag_profile_sample_count"))) is not None else None,
                "lag_profile_stability_score": _round_or_none(_safe_float(best_payload.get("lag_profile_stability_score"))),
                "pair_evidence_confidence": _round_or_none(_safe_float(best_payload.get("pair_evidence_confidence"))),
                "follower_confidence_score": _round_or_none(follower_confidence),
                "rs_rank": _round_or_none(_safe_float(candidate.get("rs_rank"))),
                "delta_rs_rank_qoq": _round_or_none(_safe_float(candidate.get("delta_rs_rank_qoq"))),
                "rs_line_20d_slope": _round_or_none(_safe_float(candidate.get("rs_line_20d_slope"))),
                "leader_gap_20d": _round_or_none((leader_gap_20d * 100.0) if (leader_gap_20d := _safe_float(best_payload.get("leader_gap_20d"))) is not None else None),
                "leader_gap_60d": _round_or_none((leader_gap_60d * 100.0) if (leader_gap_60d := _safe_float(best_payload.get("leader_gap_60d"))) is not None else None),
                "propagation_ratio": _round_or_none(_safe_float(best_payload.get("propagation_ratio"))),
                "propagation_state": best_payload.get("propagation_state"),
                "catchup_room_score": _round_or_none(_safe_float(best_payload.get("catchup_room_score"))),
                "top_reason_1": reasons[0] if reasons else "",
                "top_reason_2": reasons[1] if len(reasons) > 1 else "",
                "reason_codes": _unique(reasons),
                "follower_reject_reason_codes": follower_reject_reasons,
                "underreaction_score": _round_or_none(_safe_float(best_payload.get("underreaction_score"))),
                "rs_inflection_score": _round_or_none(_safe_float(best_payload.get("rs_inflection_score"))),
                "structure_preservation_score": _round_or_none(_safe_float(best_payload.get("structure_preservation_score"))),
                "sympathy_freshness_score": _round_or_none(_safe_float(best_payload.get("sympathy_freshness_score"))),
                "link_evidence_tags": _tag_csv(link_tags),
                "group_strength_score": _round_or_none(_safe_float(candidate.get("group_strength_score"))),
                "trend_integrity_score": _round_or_none(_safe_float(candidate.get("trend_integrity_score"))),
                "volume_demand_score": _round_or_none(_safe_float(candidate.get("volume_demand_score"))),
                "liquidity_quality_score": _round_or_none(_safe_float(candidate.get("liquidity_quality_score"))),
                "as_of_ts": candidate.get("as_of_ts"),
                "hard_precondition_pass": hard_precondition,
                "hygiene_pass": hygiene_pass,
                "risk_flag": "",
            }
            result["follower_rows"].append(follower_row)
            result["pair_rows"].append(
                {
                    "leader_symbol": best_payload.get("linked_leader"),
                    "follower_symbol": symbol,
                    "group_name": candidate.get("group_name"),
                    "pair_link_score": _round_or_none(_safe_float(best_payload.get("pair_link_score"))),
                    "peer_lead_score": follower_row["peer_lead_score"],
                    "follower_score": _round_or_none(_safe_float(best_payload.get("follower_score"))),
                    "leader_gap_20d": follower_row["leader_gap_20d"],
                    "leader_gap_60d": follower_row["leader_gap_60d"],
                    "lag_days": follower_row["best_lag_days"],
                    "lead_lag_profile": best_payload.get("lead_lag_profile"),
                    "lag_profile_sample_count": follower_row["lag_profile_sample_count"],
                    "lag_profile_stability_score": follower_row["lag_profile_stability_score"],
                    "leader_event_return": _round_or_none((_safe_float(best_payload.get("leader_event_return")) or 0.0) * 100.0),
                    "follower_event_return": _round_or_none((_safe_float(best_payload.get("follower_event_return")) or 0.0) * 100.0),
                    "propagation_ratio": follower_row["propagation_ratio"],
                    "propagation_state": follower_row["propagation_state"],
                    "catchup_room_score": follower_row["catchup_room_score"],
                    "lagged_corr": follower_row["lagged_corr"],
                    "connection_type": follower_row["link_evidence_tags"],
                    "pair_evidence_confidence": follower_row["pair_evidence_confidence"],
                    "pair_confidence": _round_or_none(
                        _weighted_mean(
                            [
                                (_safe_float(best_payload.get("pair_evidence_confidence")), 0.20),
                                (_safe_float(best_payload.get("pair_link_score")), 0.45),
                                (_safe_float(best_payload.get("peer_lead_score")), 0.20),
                                (_safe_float(best_payload.get("underreaction_score")), 0.15),
                            ]
                        )
                    ),
                    "label": "",
                }
            )
            return result

        def _accept_candidate_result(index: int, result: dict[str, Any]) -> None:
            nonlocal pair_evaluations, pair_candidates, eligible_candidates, skipped_by_prefilter
            follower_rows.extend(result["follower_rows"])
            pair_rows.extend(result["pair_rows"])
            pair_evaluations += int(result["pair_evaluations"])
            pair_candidates += int(result["pair_candidates"])
            eligible_candidates += int(result["eligible_candidates"])
            skipped_by_prefilter += int(result["skipped_by_prefilter"])
            _emit_follower_progress(index, str(result.get("symbol") or ""))

        if worker_count <= 1:
            for index, candidate in enumerate(candidate_records, start=1):
                _accept_candidate_result(index, _analyze_candidate(candidate))
        else:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [
                    executor.submit(_analyze_candidate, candidate)
                    for candidate in candidate_records
                ]
                for index, future in enumerate(futures, start=1):
                    _accept_candidate_result(index, future.result())

        followers = pd.DataFrame(follower_rows)
        pairs = pd.DataFrame(pair_rows)
        if not followers.empty:
            followers, calibrated = self._assign_follower_labels(followers, calibration=calibration_map)
            label_map = series_to_str_text_dict(followers.set_index("symbol")["label"])
            if not pairs.empty:
                pairs["label"] = pairs["follower_symbol"].map(label_map).fillna("")
            followers = followers.sort_values(["follower_score", "pair_link_score"], ascending=[False, False]).reset_index(drop=True)
        if not pairs.empty:
            pairs = pairs.sort_values(["follower_score", "pair_link_score"], ascending=[False, False]).reset_index(drop=True)
        self.last_follower_lag_pruning = {
            "mode": mode,
            "total_candidates": int(total_candidates),
            "eligible_candidates": int(eligible_candidates),
            "skipped_by_prefilter": int(skipped_by_prefilter),
            "leader_pool_before_cap": int(leader_pool_before_cap),
            "leader_pool_after_cap": int(leader_pool_after_cap),
            "max_leaders_per_industry": int(max_leaders_per_industry) if balanced_mode else None,
            "max_pairs_per_candidate": int(max_pairs_per_candidate) if balanced_mode else None,
            "pair_candidates": int(pair_candidates),
            "pair_evaluations": int(pair_evaluations),
            "workers": int(worker_count),
            "lag_frame_precompute_symbols": int(len(lag_frame_prepared_symbols)),
            "lag_frame_precompute_seconds": round(float(lag_frame_precompute_seconds), 6),
            "lag_frame_cache_hits": int(lag_frame_cache_hits),
        }
        if runtime_context is not None:
            runtime_context.add_runtime_metric(
                "feature_analysis",
                "leader_lagging_lag_frame_precompute_seconds",
                float(lag_frame_precompute_seconds),
            )
            runtime_context.set_runtime_metric(
                "feature_analysis",
                "leader_lagging_lag_frame_precompute_symbols",
                int(len(lag_frame_prepared_symbols)),
            )
        return followers, pairs


class LeaderLaggingScreener:
    def __init__(
        self,
        *,
        market: str = "us",
        standalone: bool = False,
        runtime_context: RuntimeContext | None = None,
    ) -> None:
        self.market = market_key(market)
        self.standalone = bool(standalone)
        self.runtime_context = runtime_context
        self._active_as_of_date: str | None = None
        self._explicit_replay_as_of = False
        ensure_market_dirs(self.market)
        from utils.market_runtime import get_leader_lagging_results_dir

        self.results_dir = get_leader_lagging_results_dir(self.market)
        ensure_dir(self.results_dir)
        self.analyzer = LeaderLaggingAnalyzer()

    def _emit_progress(
        self,
        *,
        current_symbol: str = "",
        current_chunk: str = "",
    ) -> None:
        if self.runtime_context is None:
            return
        self.runtime_context.update_runtime_state(
            current_stage="Leader / lagging",
            current_symbol=current_symbol,
            current_chunk=current_chunk,
            status="running",
        )

    def _load_metadata_map(self) -> dict[str, dict[str, Any]]:
        metadata_path = get_stock_metadata_path(self.market)
        if not os.path.exists(metadata_path):
            return {}
        frame = pd.read_csv(metadata_path)
        if frame.empty or "symbol" not in frame.columns:
            return {}
        frame["symbol"] = frame["symbol"].astype(str).str.upper()
        return frame_keyed_records(frame, key_column="symbol", uppercase_keys=True, drop_na=True)

    def _load_frames(self) -> dict[str, pd.DataFrame]:
        data_dir = get_market_data_dir(self.market)
        if not os.path.isdir(data_dir):
            return {}
        frames: dict[str, pd.DataFrame] = {}
        active_as_of = self._active_as_of_date
        candidate_files = limit_runtime_symbols(
            [
                name
                for name in sorted(os.listdir(data_dir))
                if name.endswith(".csv")
                and not is_index_symbol(self.market, os.path.splitext(name)[0].upper())
            ]
        )
        interval = progress_interval(len(candidate_files), target_updates=8, min_interval=50)
        print(f"[LeaderLagging] Frame load started ({self.market}) - files={len(candidate_files)}")
        freshness_reports = []
        explicit_replay = bool(self._explicit_replay_as_of)
        frame_symbols = [
            os.path.splitext(name)[0].strip().upper()
            for name in candidate_files
            if os.path.splitext(name)[0].strip()
        ]
        frame_load_started = time.perf_counter()
        frame_map = load_local_ohlcv_frames_ordered(
            self.market,
            frame_symbols,
            as_of=active_as_of,
            price_policy=PricePolicy.SPLIT_ADJUSTED,
            runtime_context=self.runtime_context,
            required_columns=SCREENING_OHLCV_READ_COLUMNS,
            worker_scope="leader_lagging.frame_load",
            load_frame_fn=load_local_ohlcv_frame,
        )
        if self.runtime_context is not None:
            self.runtime_context.add_timing(
                "leader_lagging.frame_load_seconds",
                time.perf_counter() - frame_load_started,
            )
        for index, name in enumerate(candidate_files, start=1):
            symbol = os.path.splitext(name)[0].upper()
            if not symbol:
                if is_progress_tick(index, len(candidate_files), interval):
                    print(
                        f"[LeaderLagging] Frame load progress ({self.market}) - "
                        f"processed={index}/{len(candidate_files)}, loaded={len(frames)}"
                    )
                continue
            frame = frame_map.get(symbol, pd.DataFrame())
            freshness_reports.append(
                describe_ohlcv_freshness(
                    frame,
                    market=self.market,
                    symbol=symbol,
                    as_of=active_as_of,
                    latest_completed_session=active_as_of,
                    explicit_as_of=explicit_replay,
                )
            )
            if not frame.empty:
                frames[symbol] = frame
            if is_progress_tick(index, len(candidate_files), interval):
                self._emit_progress(
                    current_symbol=symbol,
                    current_chunk=f"frame_load:{index}/{len(candidate_files)}",
                )
                print(
                    f"[LeaderLagging] Frame load progress ({self.market}) - "
                    f"processed={index}/{len(candidate_files)}, loaded={len(frames)}"
                )
        if self.runtime_context is not None:
            self.runtime_context.update_data_freshness(
                "leader_lagging",
                OhlcvFreshnessSummary.from_reports(freshness_reports),
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
        leader_quality_diagnostics: pd.DataFrame,
        leader_quality_summary: dict[str, Any],
        leader_candidate_quality_diagnostics: pd.DataFrame,
        leader_candidate_quality_summary: dict[str, Any],
        leader_threshold_tuning_report: pd.DataFrame,
        leader_threshold_tuning_summary: dict[str, Any],
        market_context: MarketContext,
        actual_data_calibration: dict[str, Any],
        *,
        market_truth_source: str,
        core_overlay_applied: bool,
    ) -> None:
        outputs = {
            "pattern_excluded_pool": pattern_excluded_pool,
            "pattern_included_candidates": pattern_included_candidates,
            "leaders": leaders,
            "followers": followers,
            "leader_follower_pairs": pairs,
            "leader_quality_diagnostics": leader_quality_diagnostics,
            "leader_candidate_quality_diagnostics": leader_candidate_quality_diagnostics,
            "leader_threshold_tuning_report": leader_threshold_tuning_report,
            "group_dashboard": group_table[[
                "industry_key",
                "sector",
                "industry",
                "group_name",
                "group_member_count",
                "group_strength_score",
                "group_state",
                "group_rank",
            ]].copy(),
        }
        for stem, frame in outputs.items():
            csv_path = os.path.join(self.results_dir, f"{stem}.csv")
            json_path = os.path.join(self.results_dir, f"{stem}.json")
            write_dataframe_csv_with_fallback(
                frame,
                csv_path,
                index=False,
                runtime_context=self.runtime_context,
                metric_label=f"leader_lagging.{stem}.csv",
            )
            write_dataframe_json_with_fallback(
                frame,
                json_path,
                orient="records",
                indent=2,
                force_ascii=False,
                runtime_context=self.runtime_context,
                metric_label=f"leader_lagging.{stem}.json",
            )

        runtime_state = (
            dict(self.runtime_context.runtime_state)
            if self.runtime_context is not None and isinstance(self.runtime_context.runtime_state, dict)
            else {}
        )
        market_truth_mode = str(runtime_state.get("market_truth_mode") or "").strip()
        if not market_truth_mode:
            market_truth_mode = "standalone_manual" if market_truth_source == "local_standalone" else "compat"
        fallback_reason = str(runtime_state.get("fallback_reason") or "").strip()
        follower_lag_pruning = dict(getattr(self.analyzer, "last_follower_lag_pruning", {}) or {})
        summary = {
            "market": self.market.upper(),
            "benchmark_symbol": market_context.benchmark_symbol,
            "market_truth_source": market_truth_source,
            "core_overlay_applied": core_overlay_applied,
            "market_truth_mode": market_truth_mode,
            "fallback_reason": fallback_reason,
            "regime_state": market_context.regime_state,
            "market_alias": market_context.market_alias,
            "market_alignment_score": market_context.market_alignment_score,
            "breadth_support_score": market_context.breadth_support_score,
            "rotation_support_score": market_context.rotation_support_score,
            "leader_health_score": market_context.leader_health_score,
            "reason_codes": list(market_context.reason_codes),
            "actual_data_calibration": actual_data_calibration,
            "leader_quality": leader_quality_summary,
            "leader_candidate_quality": leader_candidate_quality_summary,
            "leader_threshold_tuning": leader_threshold_tuning_summary,
            "follower_lag_pruning": follower_lag_pruning,
            "counts": {
                "pattern_excluded_pool": int(len(pattern_excluded_pool)),
                "pattern_included_candidates": int(len(pattern_included_candidates)),
                "leaders": int(len(leaders)),
                "confirmed_leaders": int((leaders["leader_tier"] == "strong").sum()) if not leaders.empty and "leader_tier" in leaders.columns else 0,
                "followers": int(len(followers)),
                "high_quality_followers": int((followers["label"] == "High-Quality Follower").sum()) if not followers.empty else 0,
                "pairs": int(len(pairs)),
            },
        }
        write_json_with_fallback(
            summary,
            os.path.join(self.results_dir, "market_summary.json"),
            ensure_ascii=False,
            indent=2,
            runtime_context=self.runtime_context,
            metric_label="leader_lagging.market_summary.json",
        )
        write_json_with_fallback(
            actual_data_calibration,
            os.path.join(self.results_dir, "actual_data_calibration.json"),
            ensure_ascii=False,
            indent=2,
            runtime_context=self.runtime_context,
            metric_label="leader_lagging.actual_data_calibration.json",
        )
        write_json_with_fallback(
            leader_quality_summary,
            os.path.join(self.results_dir, "leader_quality_summary.json"),
            ensure_ascii=False,
            indent=2,
            runtime_context=self.runtime_context,
            metric_label="leader_lagging.leader_quality_summary.json",
        )
        write_json_with_fallback(
            leader_threshold_tuning_report.to_dict(orient="records"),
            os.path.join(self.results_dir, "leader_threshold_tuning_report.json"),
            ensure_ascii=False,
            indent=2,
            runtime_context=self.runtime_context,
            metric_label="leader_lagging.leader_threshold_tuning_report.json",
        )
        write_json_with_fallback(
            leader_candidate_quality_summary,
            os.path.join(self.results_dir, "leader_candidate_quality_summary.json"),
            ensure_ascii=False,
            indent=2,
            runtime_context=self.runtime_context,
            metric_label="leader_lagging.leader_candidate_quality_summary.json",
        )

    def run(self) -> dict[str, Any]:
        metadata_map = self._load_metadata_map()
        requested_as_of = (
            _safe_text(self.runtime_context.as_of_date)
            if self.runtime_context is not None
            else ""
        )
        benchmark_started = time.perf_counter()
        benchmark_symbol, benchmark_daily = load_benchmark_data(
            self.market,
            get_benchmark_candidates(self.market),
            as_of=requested_as_of or None,
            allow_yfinance_fallback=True,
            price_policy=PricePolicy.SPLIT_ADJUSTED,
        )
        if self.runtime_context is not None:
            self.runtime_context.add_timing(
                "leader_lagging.benchmark_load_seconds",
                time.perf_counter() - benchmark_started,
            )
        benchmark_symbol = benchmark_symbol or get_primary_benchmark_symbol(self.market)
        benchmark_daily = self.analyzer.normalize_daily_frame(benchmark_daily)
        benchmark_as_of = (
            _as_date_str(benchmark_daily["date"].iloc[-1]) if not benchmark_daily.empty else None
        ) or None
        self._explicit_replay_as_of = runtime_context_has_explicit_as_of(self.runtime_context)
        self._active_as_of_date = requested_as_of or benchmark_as_of
        if self.runtime_context is not None and self._active_as_of_date:
            self.runtime_context.set_as_of_date(self._active_as_of_date)
        frames = self._load_frames()
        print(
            f"[LeaderLagging] Inputs ready ({self.market}) - "
            f"metadata={len(metadata_map)}, frames={len(frames)}"
        )
        print(f"[LeaderLagging] Feature analysis started ({self.market}) - benchmark={benchmark_symbol}")

        feature_rows: list[dict[str, Any]] = []
        total_symbols = len(frames)
        interval = progress_interval(total_symbols, target_updates=8, min_interval=50)
        symbols_in_order = list(frames.keys())
        worker_count = _runtime_worker_count(
            total_symbols,
            env_var="INVEST_PROTO_SYMBOL_ANALYSIS_WORKERS",
            runtime_context=self.runtime_context,
            scope="leader_lagging.feature_analysis",
        )

        def _compute_features(symbol: str) -> dict[str, Any]:
            metadata = metadata_map.get(symbol)

            def _compute() -> dict[str, Any]:
                return self.analyzer.compute_symbol_features(
                    symbol=symbol,
                    market=self.market,
                    daily_frame=frames[symbol],
                    benchmark_daily=benchmark_daily,
                    metadata=metadata,
                    benchmark_is_normalized=True,
                )

            return feature_row_cache_get_or_compute(
                namespace="leader_lagging_features",
                market=self.market,
                symbol=symbol,
                as_of=self._active_as_of_date or "",
                feature_version="leader_lagging_features_v1",
                source_path=resolve_ohlcv_source_path(self.market, symbol),
                compute_fn=_compute,
                runtime_context=self.runtime_context,
                extra_key={
                    "benchmark_symbol": benchmark_symbol,
                    "benchmark_as_of": benchmark_as_of,
                    "metadata": metadata,
                },
            )

        feature_started = time.perf_counter()
        if self.runtime_context is not None:
            self.runtime_context.add_runtime_metric("feature_analysis", "symbols", total_symbols)
        if worker_count <= 1:
            features_by_symbol = {
                symbol: _compute_features(symbol)
                for symbol in symbols_in_order
            }
        else:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_by_symbol = {
                    symbol: executor.submit(_compute_features, symbol)
                    for symbol in symbols_in_order
                }
                features_by_symbol = {
                    symbol: future_by_symbol[symbol].result()
                    for symbol in symbols_in_order
                }
        if self.runtime_context is not None:
            self.runtime_context.add_timing(
                "leader_lagging.feature_analysis_seconds",
                time.perf_counter() - feature_started,
            )

        for index, symbol in enumerate(symbols_in_order, start=1):
            feature_rows.append(features_by_symbol[symbol])
            if is_progress_tick(index, total_symbols, interval):
                self._emit_progress(
                    current_symbol=symbol,
                    current_chunk=f"feature_analysis:{index}/{total_symbols}",
                )
                print(
                    f"[LeaderLagging] Feature analysis progress ({self.market}) - "
                    f"processed={index}/{total_symbols}, features={len(feature_rows)}"
                )
        feature_table = self.analyzer.finalize_feature_table(pd.DataFrame(feature_rows))
        as_of_date = self._active_as_of_date or ""
        if self.standalone:
            market_truth = None
            leader_core = empty_leader_core_snapshot(self.market, as_of_date)
            market_truth_source = "local_standalone"
            core_overlay_applied = False
        else:
            market_truth = load_market_truth_snapshot(self.market, as_of_date=as_of_date)
            leader_core = market_truth.leader_core
            market_truth_source = "market_intel_compat"
            core_overlay_applied = True
        runtime_state = (
            dict(self.runtime_context.runtime_state)
            if self.runtime_context is not None and isinstance(self.runtime_context.runtime_state, dict)
            else {}
        )
        market_truth_mode = str(runtime_state.get("market_truth_mode") or "").strip()
        if not market_truth_mode:
            market_truth_mode = "standalone_manual" if market_truth_source == "local_standalone" else "compat"
        fallback_reason = str(runtime_state.get("fallback_reason") or "").strip()
        feature_table = annotate_frame_with_leader_core(feature_table, leader_core)
        group_table = self.analyzer.compute_group_table(feature_table)
        actual_data_calibration = self.analyzer.build_actual_data_calibration(
            feature_table=feature_table,
            group_table=group_table,
        )
        print(
            f"[LeaderLagging] Relationship analysis started ({self.market}) - "
            f"features={len(feature_table)}, groups={len(group_table)}"
        )
        relationship_started = time.perf_counter()
        market_context = self.analyzer.compute_market_context(
            market=self.market,
            benchmark_symbol=benchmark_symbol,
            benchmark_daily=benchmark_daily,
            feature_table=feature_table,
            group_table=group_table,
            market_truth=market_truth,
        )
        leaders = self.analyzer.analyze_leaders(
            feature_table=feature_table,
            group_table=group_table,
            market_context=market_context,
            calibration=actual_data_calibration,
        )
        leader_candidates = leaders.copy()
        leader_candidate_quality_diagnostics, leader_candidate_quality_summary = leader_quality.build_leader_quality_artifacts(
            feature_table=feature_table,
            leaders=leader_candidates,
            group_table=group_table,
            calibration=actual_data_calibration,
        )
        tuned_calibration, leader_threshold_tuning_report, leader_threshold_tuning_summary = (
            leader_tuning.build_leader_threshold_tuning(
                base_calibration=actual_data_calibration,
                candidate_quality_diagnostics=leader_candidate_quality_diagnostics,
                standalone=self.standalone,
                enabled=leader_tuning.leader_tuning_runtime_enabled(),
            )
        )
        actual_data_calibration = dict(tuned_calibration)
        actual_data_calibration["leader_tuning_enabled"] = bool(
            leader_threshold_tuning_summary.get("policy_enabled")
        )
        actual_data_calibration["leader_tuning_applied"] = bool(
            leader_threshold_tuning_summary.get("leader_tuning_applied")
        )
        actual_data_calibration["leader_tuning_eligible"] = bool(
            leader_threshold_tuning_summary.get("eligible")
        )
        actual_data_calibration["leader_tuning_reason_codes"] = str(
            leader_threshold_tuning_summary.get("reason_codes") or ""
        )
        actual_data_calibration["leader_tuning_adjustments"] = dict(
            leader_threshold_tuning_summary.get("adjustments") or {}
        )
        if bool(leader_threshold_tuning_summary.get("leader_tuning_applied")):
            leaders = self.analyzer.analyze_leaders(
                feature_table=feature_table,
                group_table=group_table,
                market_context=market_context,
                calibration=actual_data_calibration,
            )
            leader_candidates = leaders.copy()
            leader_candidate_quality_diagnostics, leader_candidate_quality_summary = leader_quality.build_leader_quality_artifacts(
                feature_table=feature_table,
                leaders=leader_candidates,
                group_table=group_table,
                calibration=actual_data_calibration,
            )
        if not leaders.empty and not leader_candidate_quality_diagnostics.empty:
            quality_columns = [
                "symbol",
                "leader_confidence_score",
                "confidence_bucket",
                "low_confidence_reason_codes",
                "reject_reason_codes",
                "extended_reason_codes",
                "threshold_proximity_codes",
                "threshold_margin_min",
                "rs_rank_true_threshold_distance",
                "structure_threshold_distance",
                "extension_threshold_distance",
            ]
            available_quality_columns = [
                column for column in quality_columns if column in leader_candidate_quality_diagnostics.columns
            ]
            leaders = leaders.merge(
                leader_candidate_quality_diagnostics[available_quality_columns].drop_duplicates(subset=["symbol"]),
                on="symbol",
                how="left",
            )
        if not leaders.empty:
            overlay_columns = feature_table[[
                "symbol",
                "core_group_state",
                "core_group_strength_score",
                "core_group_rank",
                "core_leader_state",
                "core_breakdown_status",
                "core_leader_score",
            ]].drop_duplicates(subset=["symbol"])
            leaders = leaders.merge(overlay_columns, on="symbol", how="left")
            if self.standalone:
                leaders["leader_state"] = leaders.get("core_leader_state", pd.Series("", index=leaders.index)).fillna("")
                leaders["breakdown_status"] = leaders.get("core_breakdown_status", pd.Series("", index=leaders.index)).fillna("")
                leaders = leaders.drop(columns=[
                    "core_group_state",
                    "core_group_strength_score",
                    "core_group_rank",
                    "core_leader_state",
                    "core_breakdown_status",
                    "core_leader_score",
                ])
                leaders = leaders.sort_values(["leader_sort_score", "leader_score", "rs_rank"], ascending=[False, False, False]).reset_index(drop=True)
            else:
                leaders["leader_overlay_score"] = leaders["leader_score"]
                leaders["leader_score"] = leaders["core_leader_score"]
                leaders["group_state"] = leaders["core_group_state"].fillna("")
                leaders["group_strength_score"] = leaders["core_group_strength_score"].where(leaders["core_group_strength_score"].notna(), leaders["group_strength_score"])
                leaders["group_rank"] = leaders["core_group_rank"].where(leaders["core_group_rank"].notna(), leaders["group_rank"])
                leaders["leader_state"] = leaders["core_leader_state"].fillna("")
                leaders["breakdown_status"] = leaders["core_breakdown_status"].fillna("")
                leaders = leaders[
                    leaders["leader_state"].isin(["CONFIRMED", "EMERGING"])
                    & (leaders["breakdown_status"] == "OK")
                ].copy()
                leaders = leaders.drop(columns=[
                    "core_group_state",
                    "core_group_strength_score",
                    "core_group_rank",
                    "core_leader_state",
                    "core_breakdown_status",
                    "core_leader_score",
                ])
                leaders = leaders.sort_values(["leader_sort_score", "leader_overlay_score", "leader_score", "rs_rank"], ascending=[False, False, False, False]).reset_index(drop=True)

        follower_leader_pool = self.analyzer.filter_leaders_for_follower_analysis(leaders)
        follower_analysis_started = time.perf_counter()

        def _follower_progress_callback(payload: dict[str, Any]) -> None:
            processed = int(payload.get("processed") or 0)
            total = int(payload.get("total") or 0)
            pair_evaluations = int(payload.get("pair_evaluations") or 0)
            pair_candidates = int(payload.get("pair_candidates") or 0)
            eligible_candidates = int(payload.get("eligible_candidates") or 0)
            skipped_by_prefilter = int(payload.get("skipped_by_prefilter") or 0)
            workers = int(payload.get("workers") or 1)
            current_symbol = _safe_text(payload.get("current_symbol"))
            self._emit_progress(
                current_symbol=current_symbol,
                current_chunk=f"follower_analysis:{processed}/{total}",
            )
            print(
                f"[LeaderLagging] Follower analysis progress ({self.market}) - "
                f"processed={processed}/{total}, eligible={eligible_candidates}, "
                f"skipped_prefilter={skipped_by_prefilter}, pair_candidates={pair_candidates}, "
                f"pair_evals={pair_evaluations}, workers={workers}"
            )

        followers, pairs = self.analyzer.analyze_followers(
            feature_table=feature_table,
            leaders=follower_leader_pool,
            group_table=group_table,
            market_context=market_context,
            frames=frames,
            calibration=actual_data_calibration,
            progress_callback=_follower_progress_callback,
            runtime_context=self.runtime_context,
        )
        if self.runtime_context is not None:
            self.runtime_context.add_timing(
                "leader_lagging.follower_analysis_seconds",
                time.perf_counter() - follower_analysis_started,
            )
        if self.runtime_context is not None:
            self.runtime_context.add_timing(
                "leader_lagging.relationship_analysis_seconds",
                time.perf_counter() - relationship_started,
            )
        if not followers.empty:
            overlay_columns = feature_table[[
                "symbol",
                "core_group_state",
                "core_group_strength_score",
                "core_group_rank",
                "core_leader_state",
                "core_breakdown_status",
                "core_leader_score",
            ]].drop_duplicates(subset=["symbol"])
            followers = followers.merge(overlay_columns, on="symbol", how="left")
            if self.standalone:
                followers["leader_state"] = followers["core_leader_state"].fillna("")
                followers["breakdown_status"] = followers["core_breakdown_status"].fillna("")
            else:
                followers["group_state"] = followers["core_group_state"].fillna("")
                followers["group_strength_score"] = followers["core_group_strength_score"].where(followers["core_group_strength_score"].notna(), followers["group_strength_score"])
                existing_group_rank = (
                    followers["group_rank"]
                    if "group_rank" in followers.columns
                    else pd.Series(np.nan, index=followers.index, dtype="float64")
                )
                followers["group_rank"] = followers["core_group_rank"].where(
                    followers["core_group_rank"].notna(),
                    existing_group_rank,
                )
                followers["leader_state"] = followers["core_leader_state"].fillna("")
                followers["breakdown_status"] = followers["core_breakdown_status"].fillna("")
                followers["leader_score"] = followers["core_leader_score"]
            followers = followers.drop(columns=[
                "core_group_state",
                "core_group_strength_score",
                "core_group_rank",
                "core_leader_state",
                "core_breakdown_status",
                "core_leader_score",
            ])

        pool_table = (
            feature_table.merge(
                group_table[["industry_key", "group_strength_score", "group_rank", "group_state", "group_overlay_score"]],
                on="industry_key",
                how="left",
            )
            if not feature_table.empty and not group_table.empty
            else feature_table.copy()
        )
        broad_pool = pd.DataFrame()
        if not pool_table.empty:
            pool_table["leader_state"] = pool_table.get("core_leader_state", pd.Series("", index=pool_table.index)).fillna("")
            pool_table["breakdown_status"] = pool_table.get("core_breakdown_status", pd.Series("", index=pool_table.index)).fillna("")
            pool_table["leader_score"] = pool_table.get("core_leader_score", pd.Series(pd.NA, index=pool_table.index))
            for column in ("group_strength_score", "group_rank", "delta_rs_rank_qoq", "benchmark_relative_strength_slope"):
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
                    "industry_key",
                    "group_name",
                    "group_state",
                    "rs_rank",
                    "delta_rs_rank_qoq",
                    "group_strength_score",
                    "group_rank",
                    "leader_state",
                    "breakdown_status",
                    "leader_score",
                    "trend_integrity_score",
                    "structure_quality_score",
                    "volume_demand_score",
                    "liquidity_quality_score",
                    "distance_to_52w_high",
                    "rs_line_20d_slope",
                    "benchmark_relative_strength_slope",
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
            leaders = leaders[leaders["label"] != "reject"].reset_index(drop=True)
        if not followers.empty:
            followers = followers[followers["label"] != "Too Weak, Reject"].reset_index(drop=True)
        if not pairs.empty:
            valid_follower_symbols = set(followers["symbol"].astype(str))
            pairs = pairs[pairs["follower_symbol"].astype(str).isin(valid_follower_symbols)].reset_index(drop=True)

        leader_quality_diagnostics, leader_quality_summary = leader_quality.build_leader_quality_artifacts(
            feature_table=feature_table,
            leaders=leaders,
            group_table=group_table,
            calibration=actual_data_calibration,
        )
        if not leaders.empty and not leader_quality_diagnostics.empty:
            quality_columns = [
                "symbol",
                "leader_confidence_score",
                "confidence_bucket",
                "low_confidence_reason_codes",
                "reject_reason_codes",
                "extended_reason_codes",
                "threshold_proximity_codes",
                "threshold_margin_min",
                "rs_rank_true_threshold_distance",
                "structure_threshold_distance",
                "extension_threshold_distance",
            ]
            available_quality_columns = [
                column for column in quality_columns if column in leader_quality_diagnostics.columns
            ]
            leaders = (
                leaders.drop(
                    columns=[
                        column
                        for column in available_quality_columns
                        if column in leaders.columns and column != "symbol"
                    ],
                    errors="ignore",
                )
                .merge(
                    leader_quality_diagnostics[available_quality_columns].drop_duplicates(subset=["symbol"]),
                    on="symbol",
                    how="left",
                )
            )

        pattern_included_candidates = pd.concat(
            [
                leaders.assign(candidate_family="LEADER"),
                followers.assign(candidate_family="FOLLOWER"),
            ],
            ignore_index=True,
            sort=False,
        ) if not leaders.empty or not followers.empty else pd.DataFrame()
        if not pattern_included_candidates.empty:
            leader_scores = _numeric_frame_column(pattern_included_candidates, "leader_overlay_score")
            follower_scores = _numeric_frame_column(pattern_included_candidates, "follower_score")
            pattern_included_candidates["_sort_score"] = leader_scores.fillna(follower_scores)
            pattern_included_candidates = pattern_included_candidates.sort_values(
                ["phase_bucket", "_sort_score", "symbol"],
                ascending=[True, False, True],
            ).drop(columns=["_sort_score"]).reset_index(drop=True)

        persist_started = time.perf_counter()
        self._persist(
            broad_pool,
            pattern_included_candidates,
            leaders,
            followers,
            pairs,
            group_table,
            leader_quality_diagnostics,
            leader_quality_summary,
            leader_candidate_quality_diagnostics,
            leader_candidate_quality_summary,
            leader_threshold_tuning_report,
            leader_threshold_tuning_summary,
            market_context,
            actual_data_calibration,
            market_truth_source=market_truth_source,
            core_overlay_applied=core_overlay_applied,
        )
        if self.runtime_context is not None:
            self.runtime_context.add_timing(
                "leader_lagging.persist_outputs_seconds",
                time.perf_counter() - persist_started,
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
            "leader_quality_diagnostics": leader_quality_diagnostics.to_dict(orient="records"),
            "leader_candidate_quality_diagnostics": leader_candidate_quality_diagnostics.to_dict(orient="records"),
            "leader_threshold_tuning_report": leader_threshold_tuning_report.to_dict(orient="records"),
            "group_dashboard": group_table[[
                "industry_key",
                "sector",
                "industry",
                "group_name",
                "group_member_count",
                "group_strength_score",
                "group_state",
                "group_rank",
            ]].to_dict(orient="records"),
            "actual_data_calibration": actual_data_calibration,
            "leader_quality_summary": leader_quality_summary,
            "leader_candidate_quality_summary": leader_candidate_quality_summary,
            "leader_threshold_tuning_summary": leader_threshold_tuning_summary,
            "market_summary": {
                "market": self.market.upper(),
                "benchmark_symbol": market_context.benchmark_symbol,
                "market_truth_source": market_truth_source,
                "core_overlay_applied": core_overlay_applied,
                "market_truth_mode": market_truth_mode,
                "fallback_reason": fallback_reason,
                "regime_state": market_context.regime_state,
                "market_alias": market_context.market_alias,
                "market_alignment_score": market_context.market_alignment_score,
                "breadth_support_score": market_context.breadth_support_score,
                "rotation_support_score": market_context.rotation_support_score,
                "leader_health_score": market_context.leader_health_score,
                "reason_codes": list(market_context.reason_codes),
                "leader_quality": leader_quality_summary,
                "leader_candidate_quality": leader_candidate_quality_summary,
                "leader_threshold_tuning": leader_threshold_tuning_summary,
                "follower_lag_pruning": dict(getattr(self.analyzer, "last_follower_lag_pruning", {}) or {}),
            },
        }


def run_leader_lagging_screening(
    *,
    market: str = "us",
    standalone: bool = False,
    runtime_context: RuntimeContext | None = None,
) -> dict[str, Any]:
    return LeaderLaggingScreener(
        market=market,
        standalone=standalone,
        runtime_context=runtime_context,
    ).run()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Leader / lagging follower screener")
    parser.add_argument("--market", default="us", help="Target market (us|kr)")
    args = parser.parse_args()
    run_leader_lagging_screening(market=args.market)


if __name__ == "__main__":
    main()
