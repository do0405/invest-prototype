from __future__ import annotations

from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd


def safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
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


def score_ratio(value: float | None, good_min: float, bad_min: float) -> float:
    if value is None:
        return 0.0
    if value >= good_min:
        return 1.0
    if value <= bad_min:
        return 0.0
    return clamp((value - bad_min) / max(good_min - bad_min, 1e-9))


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


def tag_csv(items: Sequence[str]) -> str:
    return ",".join(dict.fromkeys([item for item in items if item]))


def _empty_lag_profile(*, sample_count: int, min_sample: int) -> dict[str, Any]:
    reasons = ["unstable_lag_profile"]
    if sample_count < min_sample:
        reasons.insert(0, "low_lag_sample")
    return {
        "lag_days": None,
        "lagged_corr": None,
        "lead_lag_profile": "",
        "lag_profile_sample_count": int(sample_count),
        "lag_profile_stability_score": 0.0,
        "pair_evidence_confidence": 0.0,
        "follower_reject_reason_codes": tag_csv(reasons),
    }


def _normal_price_frame(frame: pd.DataFrame, *, frame_is_normalized: bool = False) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["date", "adj_close"])
    data = frame.copy()
    if frame_is_normalized and {"date", "adj_close"}.issubset(data.columns):
        data = data[["date", "adj_close"]].copy()
        if not pd.api.types.is_datetime64_any_dtype(data["date"]):
            data["date"] = pd.to_datetime(data["date"], errors="coerce")
        if not pd.api.types.is_numeric_dtype(data["adj_close"]):
            data["adj_close"] = pd.to_numeric(data["adj_close"], errors="coerce")
        data = data.dropna().sort_values("date")
        return data.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    if "date" not in data.columns:
        data = data.reset_index().rename(columns={"index": "date"})
    close_col = "adj_close" if "adj_close" in data.columns else "close"
    if close_col not in data.columns:
        return pd.DataFrame(columns=["date", "adj_close"])
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data["adj_close"] = pd.to_numeric(data[close_col], errors="coerce")
    data = data[["date", "adj_close"]].dropna().sort_values("date")
    return data.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)


def prepare_lag_price_frame(frame: pd.DataFrame, *, frame_is_normalized: bool = False) -> pd.DataFrame:
    return _normal_price_frame(frame, frame_is_normalized=frame_is_normalized)


def _safe_corrcoef(left: np.ndarray, right: np.ndarray) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    left_std = float(np.std(left))
    right_std = float(np.std(right))
    if (
        not np.isfinite(left_std)
        or not np.isfinite(right_std)
        or left_std <= 1e-12
        or right_std <= 1e-12
    ):
        return None
    return safe_float(float(np.corrcoef(left, right)[0, 1]))


def lagged_return_profile_from_price_frames(
    candidate: pd.DataFrame,
    leader: pd.DataFrame,
    *,
    lags: Sequence[int] = (1, 2, 3, 5),
    lookback: int = 60,
    min_sample: int = 20,
) -> dict[str, Any]:
    required_columns = {"date", "adj_close"}
    if (
        candidate.empty
        or leader.empty
        or not required_columns.issubset(candidate.columns)
        or not required_columns.issubset(leader.columns)
    ):
        return _empty_lag_profile(sample_count=0, min_sample=min_sample)
    merged = candidate.merge(leader, on="date", suffixes=("_candidate", "_leader"))
    if len(merged) < min_sample + 5:
        return _empty_lag_profile(sample_count=len(merged), min_sample=min_sample)

    leader_ret = (
        pd.to_numeric(merged["adj_close_leader"], errors="coerce")
        .pct_change()
        .to_numpy(dtype=float, copy=False)
    )
    follower_ret = (
        pd.to_numeric(merged["adj_close_candidate"], errors="coerce")
        .pct_change()
        .to_numpy(dtype=float, copy=False)
    )
    lag_scores: dict[int, tuple[float, int]] = {}
    best_attempted_sample = 0
    for lag in lags:
        lag = int(lag)
        if lag <= 0:
            aligned_leader = leader_ret
            aligned_follower = follower_ret
        elif len(leader_ret) <= lag or len(follower_ret) <= lag:
            continue
        else:
            aligned_leader = leader_ret[:-lag]
            aligned_follower = follower_ret[lag:]

        valid = np.isfinite(aligned_leader) & np.isfinite(aligned_follower)
        if not valid.any():
            continue
        leader_window = aligned_leader[valid]
        follower_window = aligned_follower[valid]
        if lookback > 0 and len(leader_window) > lookback:
            leader_window = leader_window[-lookback:]
            follower_window = follower_window[-lookback:]
        sample_size = int(len(leader_window))
        best_attempted_sample = max(best_attempted_sample, sample_size)
        if sample_size < min_sample:
            continue
        corr = _safe_corrcoef(leader_window, follower_window)
        if corr is not None:
            lag_scores[lag] = (corr, sample_size)

    if not lag_scores:
        return _empty_lag_profile(sample_count=best_attempted_sample, min_sample=min_sample)

    ordered = sorted(lag_scores.items(), key=lambda item: item[1][0], reverse=True)
    best_lag, (best_corr, best_sample) = ordered[0]
    second_corr = ordered[1][1][0] if len(ordered) > 1 else -1.0
    corr_margin = best_corr - second_corr
    positive_lag_share = sum(1 for _lag, (corr, _sample) in lag_scores.items() if corr > 0.0) / max(len(lag_scores), 1)
    stability_score = 100.0 * weighted_mean(
        [
            (score_ratio(best_corr, 0.55, 0.15), 0.45),
            (score_ratio(corr_margin, 0.25, 0.02), 0.35),
            (positive_lag_share, 0.20),
        ]
    )
    confidence = 100.0 * weighted_mean(
        [
            (score_ratio(float(best_sample), 45.0, float(min_sample)), 0.30),
            (score_ratio(best_corr, 0.55, 0.15), 0.40),
            (stability_score / 100.0, 0.30),
        ]
    )
    reasons: list[str] = []
    if best_sample < min_sample:
        reasons.append("low_lag_sample")
    if best_corr < 0.20 or stability_score < 35.0:
        reasons.append("unstable_lag_profile")
    if best_corr <= 0.0:
        reasons.append("no_positive_lag")

    profile = ",".join(f"{lag}:{round(corr, 4)}" for lag, (corr, _sample) in sorted(lag_scores.items()))
    return {
        "lag_days": best_lag,
        "lagged_corr": best_corr,
        "lead_lag_profile": profile,
        "lag_profile_sample_count": int(best_sample),
        "lag_profile_stability_score": round(stability_score, 2),
        "pair_evidence_confidence": round(confidence, 2),
        "follower_reject_reason_codes": tag_csv(reasons),
    }


def lagged_return_profile(
    candidate_frame: pd.DataFrame,
    leader_frame: pd.DataFrame,
    *,
    lags: Sequence[int] = (1, 2, 3, 5),
    lookback: int = 60,
    min_sample: int = 20,
    frames_are_normalized: bool = False,
) -> dict[str, Any]:
    candidate = prepare_lag_price_frame(candidate_frame, frame_is_normalized=frames_are_normalized)
    leader = prepare_lag_price_frame(leader_frame, frame_is_normalized=frames_are_normalized)
    return lagged_return_profile_from_price_frames(
        candidate,
        leader,
        lags=lags,
        lookback=lookback,
        min_sample=min_sample,
    )


def propagation_state(propagation_ratio: float | None) -> str:
    value = safe_float(propagation_ratio)
    if value is None:
        return "unknown"
    if value < 0.15:
        return "no_response"
    if value <= 0.80:
        return "early_response"
    if value <= 1.10:
        return "already_caught_up"
    return "overreacted"


def catchup_room_score(
    leader_gap_20d: float | None,
    leader_gap_60d: float | None,
    propagation_ratio: float | None,
) -> float:
    state = propagation_state(propagation_ratio)
    propagation_component = score_range(propagation_ratio, 0.25, 0.80, 0.10, 1.00) * 100.0
    if state == "already_caught_up":
        propagation_component *= 0.45
    elif state == "overreacted":
        propagation_component *= 0.15
    return weighted_mean(
        [
            (score_range(leader_gap_20d, 0.05, 0.25, 0.0, 0.40) * 100.0, 0.35),
            (score_range(leader_gap_60d, 0.10, 0.35, 0.0, 0.50) * 100.0, 0.35),
            (propagation_component, 0.30),
        ]
    )


def follower_confidence_score(row: Mapping[str, Any]) -> float:
    propagation = str(row.get("propagation_state") or "")
    propagation_quality = 100.0
    if propagation == "already_caught_up":
        propagation_quality = 45.0
    elif propagation == "overreacted":
        propagation_quality = 20.0
    elif propagation == "no_response":
        propagation_quality = 35.0
    return weighted_mean(
        [
            (safe_float(row.get("pair_evidence_confidence")), 0.28),
            (safe_float(row.get("pair_link_score")), 0.18),
            (safe_float(row.get("catchup_room_score")), 0.18),
            (safe_float(row.get("structure_preservation_score")), 0.16),
            (safe_float(row.get("rs_inflection_score")), 0.12),
            (safe_float(row.get("liquidity_quality_score")), 0.04),
            (propagation_quality, 0.04),
        ]
    )


def follower_reject_reason_codes(row: Mapping[str, Any], existing_reasons: str | None = None) -> str:
    reasons = [item for item in str(existing_reasons or "").split(",") if item]
    if (safe_float(row.get("pair_evidence_confidence")) or 0.0) < 35.0:
        reasons.append("weak_pair_evidence")
    if (safe_float(row.get("lag_profile_stability_score")) or 0.0) < 35.0:
        reasons.append("unstable_lag_profile")
    if (safe_float(row.get("catchup_room_score")) or 0.0) < 30.0:
        reasons.append("low_catchup_room")
    propagation = str(row.get("propagation_state") or "")
    if propagation in {"already_caught_up", "overreacted"}:
        reasons.append(propagation)
    if (safe_float(row.get("structure_preservation_score")) or 0.0) < 35.0:
        reasons.append("structure_not_preserved")
    if (safe_float(row.get("rs_inflection_score")) or 0.0) < 30.0:
        reasons.append("weak_rs_inflection")
    if (safe_float(row.get("liquidity_quality_score")) or 0.0) < 35.0:
        reasons.append("low_liquidity")
    return tag_csv(reasons)
