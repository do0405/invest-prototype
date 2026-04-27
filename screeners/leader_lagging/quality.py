from __future__ import annotations

from typing import Any, Mapping

import numpy as np
import pandas as pd


RS_STRICT_THRESHOLD_DEFAULT = 85.0
GROUP_STRENGTH_THRESHOLD_DEFAULT = 70.0
NEAR_HIGH_MAX_DEFAULT = 0.20
STRUCTURE_HYBRID_MIN = 35.0
STRUCTURE_STRICT_MIN = 45.0
EXTENSION_RISK_MIN = 75.0


def _safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        casted = float(value)
    except (TypeError, ValueError):
        return None
    if np.isnan(casted) or np.isinf(casted):
        return None
    return casted


def _coalesce(value: float | None, default: float) -> float:
    return default if value is None else float(value)


def _clamp(value: float | None, low: float = 0.0, high: float = 1.0) -> float:
    if value is None or np.isnan(value) or np.isinf(value):
        return low
    return float(min(max(value, low), high))


def _weighted_mean(items: list[tuple[float | None, float]]) -> float:
    active = [(float(value), float(weight)) for value, weight in items if value is not None]
    if not active:
        return 0.0
    total_weight = sum(weight for _, weight in active)
    if total_weight <= 0:
        return 0.0
    return sum(value * weight for value, weight in active) / total_weight


def _tag_csv(items: list[str]) -> str:
    return ",".join(dict.fromkeys([item for item in items if item]))


def _value_counts(series: pd.Series) -> dict[str, int]:
    counts = series.fillna("").astype(str).replace("", "unknown").value_counts(dropna=False)
    return {str(key): int(value) for key, value in counts.items()}


def _threshold(calibration: Mapping[str, Any], key: str, default: float) -> float:
    return _coalesce(_safe_float(calibration.get(key)), default)


def _bars_coverage_score(row: Mapping[str, Any]) -> float:
    bars = _safe_float(row.get("bars")) or 0.0
    component_coverage = _safe_float(row.get("rs_proxy_component_coverage")) or 0.0
    return 100.0 * _weighted_mean(
        [
            (_clamp(bars / 280.0), 0.65),
            (_clamp(component_coverage / 4.0), 0.35),
        ]
    )


def leader_confidence_score_from_row(row: Mapping[str, Any]) -> float:
    return _weighted_mean(
        [
            (_safe_float(row.get("rs_proxy_confidence")), 0.25),
            (_safe_float(row.get("hidden_rs_confidence")), 0.15),
            (_safe_float(row.get("structure_confidence")), 0.25),
            (_bars_coverage_score(row), 0.15),
            (_safe_float(row.get("liquidity_quality_score")), 0.20),
        ]
    )


def confidence_bucket(score: float | None) -> str:
    value = _safe_float(score) or 0.0
    if value >= 75.0:
        return "high"
    if value >= 45.0:
        return "medium"
    return "low"


def low_confidence_reason_codes(row: Mapping[str, Any]) -> str:
    reasons: list[str] = []
    bars = _safe_float(row.get("bars")) or 0.0
    component_coverage = _safe_float(row.get("rs_proxy_component_coverage")) or 0.0
    rs_sample_count = _safe_float(row.get("rs_proxy_sample_count")) or 0.0
    if bars < 280.0 or component_coverage < 4.0:
        reasons.append("short_history")
    if rs_sample_count < 20.0 or (_safe_float(row.get("rs_proxy_confidence")) or 0.0) < 45.0:
        reasons.append("weak_rs_proxy_sample")
    if (_safe_float(row.get("hidden_rs_confidence")) or 0.0) <= 0.0 or (_safe_float(row.get("hidden_rs_weak_day_count")) or 0.0) < 3.0:
        reasons.append("no_weak_market_sample")
    if (_safe_float(row.get("structure_confidence")) or 0.0) < 45.0:
        reasons.append("low_structure_confidence")
    if (_safe_float(row.get("liquidity_quality_score")) or 0.0) < 40.0:
        reasons.append("low_liquidity")
    return _tag_csv(reasons)


def reject_reason_codes(row: Mapping[str, Any], calibration: Mapping[str, Any]) -> str:
    label = str(row.get("label") or "")
    tier = str(row.get("leader_tier") or "")
    if label != "reject" and tier != "reject":
        return ""

    reasons: list[str] = []
    if (_safe_float(row.get("liquidity_quality_score")) or 0.0) < 40.0 or (_safe_float(row.get("traded_value_20d")) or 0.0) <= 0.0:
        reasons.append("liquidity_fail")
    if not bool(row.get("close_gt_50")) or (_safe_float(row.get("trend_integrity_score")) or 0.0) < 45.0:
        reasons.append("trend_fail")
    if (
        (_safe_float(row.get("rs_rank_true")) or 0.0) < _threshold(calibration, "leader_rs_rank_min", RS_STRICT_THRESHOLD_DEFAULT)
        and (_safe_float(row.get("rs_line_score")) or 0.0) < 62.0
    ):
        reasons.append("rs_fail")
    if (_safe_float(row.get("group_strength_score")) or 0.0) < _threshold(calibration, "leader_group_strength_min", GROUP_STRENGTH_THRESHOLD_DEFAULT):
        reasons.append("group_fail")
    if (_safe_float(row.get("structure_readiness_score")) or 0.0) < STRUCTURE_HYBRID_MIN:
        reasons.append("structure_fail")
    if _coalesce(_safe_float(row.get("distance_to_52w_high")), 1.0) > _threshold(calibration, "leader_distance_to_high_max", NEAR_HIGH_MAX_DEFAULT):
        reasons.append("near_high_fail")
    return _tag_csv(reasons)


def extended_reason_codes(row: Mapping[str, Any], calibration: Mapping[str, Any]) -> str:
    label = str(row.get("label") or "")
    entry_suitability = str(row.get("entry_suitability") or "")
    if label != "extended_leader" and entry_suitability != "extended":
        return ""

    reasons: list[str] = []
    if (_safe_float(row.get("ret_20d")) or 0.0) >= 0.25:
        reasons.append("short_term_return_extension")
    if (_safe_float(row.get("distance_from_ma50")) or 0.0) >= 0.18:
        reasons.append("ma50_distance_chase")
    if _coalesce(_safe_float(row.get("distance_to_52w_high")), 1.0) <= _threshold(calibration, "leader_extended_distance_to_high_max", 0.03):
        reasons.append("near_high_chase")
    if (_safe_float(row.get("rvol")) or 0.0) >= 2.5:
        reasons.append("high_rvol_chase")
    if not reasons and (_safe_float(row.get("extension_risk_score")) or 0.0) >= EXTENSION_RISK_MIN:
        reasons.append("composite_extension_risk")
    return _tag_csv(reasons)


def threshold_distances(row: Mapping[str, Any], calibration: Mapping[str, Any]) -> dict[str, float | None]:
    rs_distance = None
    rs_value = _safe_float(row.get("rs_rank_true"))
    if rs_value is not None:
        rs_distance = rs_value - _threshold(calibration, "leader_rs_rank_min", RS_STRICT_THRESHOLD_DEFAULT)
    structure_distance = None
    structure_value = _safe_float(row.get("structure_readiness_score"))
    if structure_value is not None:
        structure_distance = structure_value - STRUCTURE_STRICT_MIN
    extension_distance = None
    extension_value = _safe_float(row.get("extension_risk_score"))
    if extension_value is not None:
        extension_distance = extension_value - EXTENSION_RISK_MIN
    return {
        "rs_rank_true_threshold_distance": rs_distance,
        "structure_threshold_distance": structure_distance,
        "extension_threshold_distance": extension_distance,
    }


def threshold_proximity_codes(row: Mapping[str, Any], calibration: Mapping[str, Any]) -> str:
    distances = threshold_distances(row, calibration)
    codes: list[str] = []
    if (distance := _safe_float(distances.get("rs_rank_true_threshold_distance"))) is not None and abs(distance) <= 5.0:
        codes.append("near_rs_threshold")
    if (distance := _safe_float(distances.get("structure_threshold_distance"))) is not None and abs(distance) <= 5.0:
        codes.append("near_structure_threshold")
    if (distance := _safe_float(distances.get("extension_threshold_distance"))) is not None and abs(distance) <= 5.0:
        codes.append("near_extension_threshold")
    return _tag_csv(codes)


def _merge_feature_context(feature_table: pd.DataFrame, leaders: pd.DataFrame) -> pd.DataFrame:
    if leaders.empty:
        return leaders.copy()
    feature_columns = [
        "symbol",
        "bars",
        "rs_proxy_sample_count",
        "rs_proxy_component_coverage",
        "rs_proxy_confidence",
        "hidden_rs_confidence",
        "hidden_rs_weak_day_count",
        "structure_confidence",
        "liquidity_quality_score",
        "traded_value_20d",
        "trend_integrity_score",
        "rs_rank_true",
        "rs_line_score",
        "group_strength_score",
        "structure_readiness_score",
        "distance_to_52w_high",
        "extension_risk_score",
        "ret_20d",
        "distance_from_ma50",
        "rvol",
        "close_gt_50",
    ]
    available = [column for column in feature_columns if column in feature_table.columns]
    if "symbol" not in available:
        return leaders.copy()
    feature_context = feature_table[available].drop_duplicates(subset=["symbol"])
    merged = leaders.merge(feature_context, on="symbol", how="left", suffixes=("", "_feature"))
    for column in feature_columns:
        feature_column = f"{column}_feature"
        if feature_column in merged.columns:
            if column in merged.columns:
                merged[column] = merged[column].where(merged[column].notna(), merged[feature_column])
            else:
                merged[column] = merged[feature_column]
            merged = merged.drop(columns=[feature_column])
    return merged


def build_leader_quality_artifacts(
    *,
    feature_table: pd.DataFrame,
    leaders: pd.DataFrame,
    group_table: pd.DataFrame,
    calibration: Mapping[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    del group_table
    diagnostics = _merge_feature_context(feature_table, leaders)
    if diagnostics.empty:
        return diagnostics, {
            "leader_count": 0,
            "label_counts": {},
            "leader_tier_counts": {},
            "entry_suitability_counts": {},
            "confidence_bucket_counts": {},
            "component_null_share": {},
            "top_10_leader_score_spread": None,
            "threshold_near_count": 0,
            "low_confidence_count": 0,
            "reject_reason_counts": {},
            "extended_reason_counts": {},
        }

    confidence_scores: list[float] = []
    confidence_buckets: list[str] = []
    low_confidence_reasons: list[str] = []
    reject_reasons: list[str] = []
    extended_reasons: list[str] = []
    threshold_codes: list[str] = []
    rs_distances: list[float | None] = []
    structure_distances: list[float | None] = []
    extension_distances: list[float | None] = []
    threshold_margin_min: list[float | None] = []

    for _, row in diagnostics.iterrows():
        row_dict = row.to_dict()
        confidence = leader_confidence_score_from_row(row_dict)
        distances = threshold_distances(row_dict, calibration)
        available_distances = [
            abs(distance)
            for distance in (
                _safe_float(distances.get("rs_rank_true_threshold_distance")),
                _safe_float(distances.get("structure_threshold_distance")),
                _safe_float(distances.get("extension_threshold_distance")),
            )
            if distance is not None
        ]
        confidence_scores.append(round(confidence, 2))
        confidence_buckets.append(confidence_bucket(confidence))
        low_confidence_reasons.append(low_confidence_reason_codes(row_dict))
        reject_reasons.append(reject_reason_codes(row_dict, calibration))
        extended_reasons.append(extended_reason_codes(row_dict, calibration))
        threshold_codes.append(threshold_proximity_codes(row_dict, calibration))
        rs_distances.append(_safe_float(distances.get("rs_rank_true_threshold_distance")))
        structure_distances.append(_safe_float(distances.get("structure_threshold_distance")))
        extension_distances.append(_safe_float(distances.get("extension_threshold_distance")))
        threshold_margin_min.append(round(min(available_distances), 2) if available_distances else None)

    diagnostics = diagnostics.copy()
    diagnostics["leader_confidence_score"] = confidence_scores
    diagnostics["confidence_bucket"] = confidence_buckets
    diagnostics["low_confidence_reason_codes"] = low_confidence_reasons
    diagnostics["reject_reason_codes"] = reject_reasons
    diagnostics["extended_reason_codes"] = extended_reasons
    diagnostics["threshold_proximity_codes"] = threshold_codes
    diagnostics["rs_rank_true_threshold_distance"] = rs_distances
    diagnostics["structure_threshold_distance"] = structure_distances
    diagnostics["extension_threshold_distance"] = extension_distances
    diagnostics["threshold_margin_min"] = threshold_margin_min

    diagnostic_columns = [
        "symbol",
        "label",
        "leader_tier",
        "entry_suitability",
        "leader_score",
        "leader_sort_score",
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
        "rs_rank_true",
        "rs_proxy_confidence",
        "hidden_rs_confidence",
        "structure_confidence",
        "liquidity_quality_score",
        "bars",
        "group_strength_score",
        "trend_integrity_score",
        "structure_readiness_score",
        "extension_risk_score",
    ]
    diagnostics = diagnostics[[column for column in diagnostic_columns if column in diagnostics.columns]].copy()

    component_columns = [
        "rs_rank_true",
        "rs_proxy_confidence",
        "hidden_rs_confidence",
        "structure_confidence",
        "liquidity_quality_score",
        "trend_integrity_score",
        "structure_readiness_score",
        "extension_risk_score",
    ]
    component_null_share = {
        column: round(float(diagnostics[column].isna().mean()), 4)
        for column in component_columns
        if column in diagnostics.columns
    }
    sorted_scores = pd.to_numeric(diagnostics.get("leader_sort_score", pd.Series(dtype=float)), errors="coerce").dropna().sort_values(ascending=False).head(10)
    top_spread = None
    if len(sorted_scores) >= 2:
        top_spread = round(float(sorted_scores.max() - sorted_scores.min()), 2)

    reject_reason_counts: dict[str, int] = {}
    extended_reason_counts: dict[str, int] = {}
    for value in diagnostics["reject_reason_codes"].fillna("").astype(str):
        for code in [item for item in value.split(",") if item]:
            reject_reason_counts[code] = reject_reason_counts.get(code, 0) + 1
    for value in diagnostics["extended_reason_codes"].fillna("").astype(str):
        for code in [item for item in value.split(",") if item]:
            extended_reason_counts[code] = extended_reason_counts.get(code, 0) + 1

    summary = {
        "leader_count": int(len(diagnostics)),
        "label_counts": _value_counts(diagnostics.get("label", pd.Series(dtype=str))),
        "leader_tier_counts": _value_counts(diagnostics.get("leader_tier", pd.Series(dtype=str))),
        "entry_suitability_counts": _value_counts(diagnostics.get("entry_suitability", pd.Series(dtype=str))),
        "confidence_bucket_counts": _value_counts(diagnostics["confidence_bucket"]),
        "component_null_share": component_null_share,
        "top_10_leader_score_spread": top_spread,
        "threshold_near_count": int((diagnostics["threshold_proximity_codes"].fillna("").astype(str) != "").sum()),
        "low_confidence_count": int((diagnostics["confidence_bucket"] == "low").sum()),
        "reject_reason_counts": reject_reason_counts,
        "extended_reason_counts": extended_reason_counts,
    }
    return diagnostics, summary
