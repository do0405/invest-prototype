from __future__ import annotations

import os
from typing import Any, Mapping

import pandas as pd


MIN_CANDIDATE_COUNT = 50
MIN_HIGH_MEDIUM_CONFIDENCE_SHARE = 0.80
MAX_COMPONENT_NULL_SHARE = 0.30


def leader_tuning_runtime_enabled() -> bool:
    raw_value = str(os.environ.get("INVEST_PROTO_ENABLE_LEADER_TUNING") or "").strip().lower()
    return raw_value in {"1", "true", "yes", "on"}


def _safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        casted = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(casted):
        return None
    return casted


def _clamp(value: float, low: float, high: float) -> float:
    return float(min(max(value, low), high))


def _count_codes(series: pd.Series) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in series.fillna("").astype(str):
        for code in [item.strip() for item in value.split(",") if item.strip()]:
            counts[code] = counts.get(code, 0) + 1
    return counts


def _component_null_share(diagnostics: pd.DataFrame) -> dict[str, float]:
    columns = [
        "rs_rank_true",
        "rs_proxy_confidence",
        "hidden_rs_confidence",
        "structure_confidence",
        "liquidity_quality_score",
        "trend_integrity_score",
        "structure_readiness_score",
        "extension_risk_score",
    ]
    return {
        column: round(float(diagnostics[column].isna().mean()), 4)
        for column in columns
        if column in diagnostics.columns
    }


def _is_leader_label(value: Any) -> bool:
    return str(value or "").strip() in {"strong_leader", "emerging_leader", "extended_leader"}


def _adjust(
    tuned: dict[str, Any],
    adjustments: dict[str, dict[str, float | str | bool]],
    key: str,
    delta: float,
    *,
    low: float,
    high: float,
    reason: str,
) -> None:
    base = _safe_float(tuned.get(key))
    if base is None:
        return
    next_value = _clamp(base + delta, low, high)
    if next_value == base:
        return
    tuned[key] = round(next_value, 4)
    adjustments[key] = {
        "base": round(base, 4),
        "tuned": round(next_value, 4),
        "delta": round(next_value - base, 4),
        "applied": True,
        "reason_codes": reason,
    }


def _build_report(
    base_calibration: Mapping[str, Any],
    tuned: Mapping[str, Any],
    adjustments: Mapping[str, Mapping[str, Any]],
    reason_codes: list[str],
) -> pd.DataFrame:
    keys = [
        "leader_rs_rank_min",
        "leader_group_strength_min",
        "leader_distance_to_high_max",
        "leader_confirmed_score_min",
        "leader_emerging_score_min",
        "leader_extended_distance_to_high_max",
        "leader_extended_pivot_proximity_max",
    ]
    rows: list[dict[str, Any]] = []
    for key in keys:
        base = _safe_float(base_calibration.get(key))
        tuned_value = _safe_float(tuned.get(key))
        adjustment = dict(adjustments.get(key) or {})
        rows.append(
            {
                "threshold_key": key,
                "base_value": base,
                "tuned_value": tuned_value,
                "delta": None if base is None or tuned_value is None else round(tuned_value - base, 4),
                "applied": bool(adjustment.get("applied")),
                "reason_codes": str(adjustment.get("reason_codes") or ",".join(reason_codes)),
            }
        )
    return pd.DataFrame(rows)


def build_leader_threshold_tuning(
    *,
    base_calibration: Mapping[str, Any],
    candidate_quality_diagnostics: pd.DataFrame,
    standalone: bool,
    enabled: bool = True,
) -> tuple[dict[str, Any], pd.DataFrame, dict[str, Any]]:
    tuned: dict[str, Any] = dict(base_calibration)
    diagnostics = candidate_quality_diagnostics.copy()
    if not enabled:
        summary = {
            "eligible": False,
            "leader_tuning_applied": False,
            "reason_codes": "disabled_by_policy",
            "candidate_count": int(len(diagnostics)),
            "leader_count": 0,
            "leader_ratio": 0.0,
            "high_medium_confidence_share": 0.0,
            "low_confidence_leader_share": 0.0,
            "component_null_share": {},
            "adjustments": {},
            "policy_enabled": False,
        }
        return tuned, _build_report(base_calibration, tuned, {}, ["disabled_by_policy"]), summary

    candidate_count = int(len(diagnostics))
    if diagnostics.empty:
        summary = {
            "eligible": False,
            "leader_tuning_applied": False,
            "reason_codes": "empty_candidate_diagnostics",
            "candidate_count": 0,
            "leader_count": 0,
            "leader_ratio": 0.0,
            "high_medium_confidence_share": 0.0,
            "low_confidence_leader_share": 0.0,
            "component_null_share": {},
            "adjustments": {},
            "policy_enabled": True,
        }
        return tuned, _build_report(base_calibration, tuned, {}, ["empty_candidate_diagnostics"]), summary

    confidence = diagnostics.get("confidence_bucket", pd.Series("", index=diagnostics.index)).fillna("").astype(str)
    high_medium_share = float(confidence.isin(["high", "medium"]).mean())
    null_share = _component_null_share(diagnostics)
    reason_codes: list[str] = []
    if candidate_count < MIN_CANDIDATE_COUNT:
        reason_codes.append("insufficient_candidate_sample")
    if high_medium_share < MIN_HIGH_MEDIUM_CONFIDENCE_SHARE:
        reason_codes.append("low_confidence_distribution")
    if any(value >= MAX_COMPONENT_NULL_SHARE for value in null_share.values()):
        reason_codes.append("high_component_null_share")

    labels = diagnostics.get("label", pd.Series("", index=diagnostics.index)).fillna("").astype(str)
    leader_mask = labels.map(_is_leader_label)
    leader_count = int(leader_mask.sum())
    leader_ratio = float(leader_count / max(candidate_count, 1))
    leader_confidence = confidence[leader_mask]
    low_confidence_leader_share = float((leader_confidence == "low").mean()) if leader_count else 0.0

    reject_counts = _count_codes(diagnostics.get("reject_reason_codes", pd.Series("", index=diagnostics.index)))
    extended_counts = _count_codes(diagnostics.get("extended_reason_codes", pd.Series("", index=diagnostics.index)))
    reject_count = max(int((labels == "reject").sum()), 1)
    rs_fail_share = reject_counts.get("rs_fail", 0) / reject_count
    near_high_fail_share = reject_counts.get("near_high_fail", 0) / reject_count
    extended_share = (
        int((labels == "extended_leader").sum()) / max(leader_count, 1)
        if leader_count
        else 0.0
    )

    eligible = not reason_codes
    adjustments: dict[str, dict[str, float | str | bool]] = {}
    if eligible:
        if leader_ratio < 0.01 and (rs_fail_share >= 0.40 or near_high_fail_share >= 0.30):
            reason_codes.append("over_reject_relaxation")
            if rs_fail_share >= 0.40:
                _adjust(tuned, adjustments, "leader_rs_rank_min", -3.0, low=75.0, high=95.0, reason="over_reject_relaxation")
                _adjust(tuned, adjustments, "leader_confirmed_score_min", -2.0, low=68.0, high=90.0, reason="over_reject_relaxation")
                _adjust(tuned, adjustments, "leader_emerging_score_min", -2.0, low=62.0, high=86.0, reason="over_reject_relaxation")
            if near_high_fail_share >= 0.30:
                _adjust(tuned, adjustments, "leader_distance_to_high_max", 0.03, low=0.08, high=0.25, reason="over_reject_relaxation")
        if leader_ratio > 0.15 or low_confidence_leader_share > 0.35:
            reason_codes.append("over_selection_tightening")
            _adjust(tuned, adjustments, "leader_rs_rank_min", 3.0, low=75.0, high=95.0, reason="over_selection_tightening")
            _adjust(tuned, adjustments, "leader_distance_to_high_max", -0.03, low=0.08, high=0.25, reason="over_selection_tightening")
            _adjust(tuned, adjustments, "leader_confirmed_score_min", 2.0, low=68.0, high=90.0, reason="over_selection_tightening")
            _adjust(tuned, adjustments, "leader_emerging_score_min", 2.0, low=62.0, high=86.0, reason="over_selection_tightening")
            if not standalone:
                _adjust(tuned, adjustments, "leader_group_strength_min", 3.0, low=60.0, high=85.0, reason="over_selection_tightening")
        if extended_share > 0.35 or extended_counts:
            reason_codes.append("extension_suitability_tightening")
            _adjust(tuned, adjustments, "leader_extended_distance_to_high_max", 0.01, low=0.01, high=0.08, reason="extension_suitability_tightening")
            _adjust(tuned, adjustments, "leader_extended_pivot_proximity_max", 3.0, low=10.0, high=35.0, reason="extension_suitability_tightening")

    if not reason_codes:
        reason_codes.append("no_tuning_needed")

    summary = {
        "eligible": bool(eligible),
        "leader_tuning_applied": bool(adjustments),
        "reason_codes": ",".join(dict.fromkeys(reason_codes)),
        "candidate_count": candidate_count,
        "leader_count": leader_count,
        "leader_ratio": round(leader_ratio, 4),
        "high_medium_confidence_share": round(high_medium_share, 4),
        "low_confidence_leader_share": round(low_confidence_leader_share, 4),
        "component_null_share": null_share,
        "reject_reason_counts": reject_counts,
        "extended_reason_counts": extended_counts,
        "adjustments": adjustments,
        "policy_enabled": True,
    }
    report = _build_report(base_calibration, tuned, adjustments, reason_codes)
    return tuned, report, summary
