from __future__ import annotations

from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd

from utils.market_data_contract import PricePolicy, load_local_ohlcv_frame

DEFAULT_QUANTILE_LEVELS: tuple[float, ...] = (
    0.05,
    0.10,
    0.20,
    0.30,
    0.40,
    0.50,
    0.60,
    0.70,
    0.80,
    0.90,
    0.95,
)
DEFAULT_HORIZONS: tuple[int, ...] = (5, 10, 20)
TSFM_MODEL_FAMILY = "TSFM"


def round_or_none(value: float | None, digits: int = 4) -> float | None:
    if value is None or not np.isfinite(value):
        return None
    return round(float(value), digits)


def _clamp(value: float, *, lower: float = 0.0, upper: float = 1.0) -> float:
    return float(max(lower, min(upper, value)))


def score_pct(
    value: float,
    *,
    slope: float,
    midpoint: float = 0.0,
    inverse: bool = False,
) -> float:
    normalized = 50.0 + ((midpoint - value) if inverse else (value - midpoint)) * slope
    return float(max(0.0, min(100.0, normalized)))


def resolve_support_anchor(close_series: np.ndarray) -> tuple[float | None, str]:
    series = pd.Series(close_series, dtype=float)
    ema20 = (
        round_or_none(float(series.ewm(span=20, adjust=False).mean().iloc[-1]))
        if len(series) >= 20
        else None
    )
    low20 = round_or_none(float(series.tail(20).min())) if len(series) >= 20 else None
    sma50 = round_or_none(float(series.tail(50).mean())) if len(series) >= 50 else None

    if ema20 is not None and low20 is not None:
        return max(float(ema20), float(low20)), "EMA20_LOW20_MAX"
    if ema20 is not None:
        return float(ema20), "EMA20_ONLY"
    if low20 is not None:
        return float(low20), "LOW20_ONLY"
    if sma50 is not None:
        return float(sma50), "SMA50"
    return None, "NONE"


def _base_output_row(
    row: Mapping[str, Any],
    *,
    market: str,
    model_id: str,
    model_family: str,
    fm_status: str = "OK",
) -> dict[str, Any]:
    output = {
        "symbol": str(row.get("symbol") or "").strip().upper(),
        "market": str(row.get("market") or market).upper(),
        "source_tags": list(row.get("source_tags", [])),
        "primary_source_tag": str(row.get("primary_source_tag") or ""),
        "support_anchor_price": None,
        "support_anchor_type": "",
        "fm_model_id": str(model_id or "").strip(),
        "fm_model_family": str(model_family or TSFM_MODEL_FAMILY).strip(),
        "fm_rerank_score": None,
        "fm_rank": None,
        "fm_status": str(fm_status or "RUNTIME_SKIP").strip().upper(),
    }
    for horizon in DEFAULT_HORIZONS:
        output[f"fm_upside_{horizon}d_pct"] = None
        output[f"fm_breach_margin_{horizon}d_pct"] = None
        output[f"fm_dispersion_{horizon}d_pct"] = None
        output[f"up_close_prob_proxy_{horizon}d"] = None
        output[f"down_close_prob_proxy_{horizon}d"] = None
        output[f"support_breach_risk_proxy_{horizon}d"] = None
        output[f"follow_through_quality_{horizon}d"] = None
        output[f"fragility_score_{horizon}d"] = None
    return output


def build_soft_skip_rows(
    *,
    merged_candidate_rows: list[dict[str, Any]],
    market: str,
    fm_status: str,
    model_id: str,
    model_family: str = TSFM_MODEL_FAMILY,
) -> list[dict[str, Any]]:
    return [
        _base_output_row(
            row,
            market=market,
            model_id=model_id,
            model_family=model_family,
            fm_status=fm_status,
        )
        for row in [dict(item) for item in merged_candidate_rows]
    ]


def _prediction_path(
    quantiles: Mapping[float, np.ndarray],
    level: float,
    *,
    horizon: int,
) -> np.ndarray | None:
    normalized: list[tuple[float, np.ndarray]] = []
    for raw_level, raw_values in quantiles.items():
        try:
            float_level = float(raw_level)
        except Exception:
            continue
        values = np.asarray(raw_values, dtype=float)
        if values.ndim != 1 or len(values) < horizon or not np.all(np.isfinite(values[:horizon])):
            continue
        normalized.append((float_level, values[:horizon]))
    if not normalized:
        return None
    for float_level, values in normalized:
        if abs(float_level - float(level)) < 1e-9:
            return values
    nearest_level, nearest_values = min(
        normalized,
        key=lambda item: abs(item[0] - float(level)),
    )
    if abs(nearest_level - float(level)) <= 0.051:
        return nearest_values
    return None


def _terminal_cdf_probability(
    quantiles: Mapping[float, np.ndarray],
    *,
    horizon: int,
    threshold: float,
) -> float | None:
    pairs: list[tuple[float, float]] = []
    for raw_level, raw_values in quantiles.items():
        values = np.asarray(raw_values, dtype=float)
        if values.ndim != 1 or len(values) < horizon or not np.isfinite(values[horizon - 1]):
            continue
        try:
            pairs.append((float(values[horizon - 1]), float(raw_level)))
        except Exception:
            continue
    if len(pairs) < 2:
        return None
    pairs.sort(key=lambda item: item[0])
    terminal_values = np.asarray([pair[0] for pair in pairs], dtype=float)
    quantile_levels = np.asarray([pair[1] for pair in pairs], dtype=float)
    return _clamp(float(np.interp(threshold, terminal_values, quantile_levels, left=0.0, right=1.0)))


def _support_breach_risk_proxy(
    quantiles: Mapping[float, np.ndarray],
    *,
    support_anchor: float,
    horizon: int,
) -> float | None:
    q10_path = _prediction_path(quantiles, 0.10, horizon=horizon)
    q05_path = _prediction_path(quantiles, 0.05, horizon=horizon)
    if q10_path is None or q05_path is None:
        return None

    q10_path = np.asarray(q10_path, dtype=float)
    q05_path = np.asarray(q05_path, dtype=float)
    core_depth = max(0.0, (float(support_anchor) - float(np.min(q10_path))) / max(float(support_anchor), 1e-9))
    tail_depth = max(0.0, (float(support_anchor) - float(np.min(q05_path))) / max(float(support_anchor), 1e-9))

    risk = (
        (0.65 * min(1.0, core_depth * 8.0))
        + (0.35 * min(1.0, tail_depth * 6.0))
    )
    return round_or_none(_clamp(risk), digits=4)


def compute_horizon_metrics(
    *,
    current_close: float,
    support_anchor: float,
    quantiles: Mapping[float, np.ndarray],
    horizon: int,
) -> dict[str, float] | None:
    q05 = _prediction_path(quantiles, 0.05, horizon=horizon)
    q10 = _prediction_path(quantiles, 0.10, horizon=horizon)
    q50 = _prediction_path(quantiles, 0.50, horizon=horizon)
    q90 = _prediction_path(quantiles, 0.90, horizon=horizon)
    q95 = _prediction_path(quantiles, 0.95, horizon=horizon)
    if any(path is None for path in (q05, q10, q50, q90, q95)):
        return None

    q05 = np.asarray(q05, dtype=float)
    q10 = np.asarray(q10, dtype=float)
    q50 = np.asarray(q50, dtype=float)
    q90 = np.asarray(q90, dtype=float)
    q95 = np.asarray(q95, dtype=float)

    upside_pct = ((float(q50[horizon - 1]) / current_close) - 1.0) * 100.0
    breach_margin_pct = ((float(np.min(q10)) / support_anchor) - 1.0) * 100.0
    dispersion_pct = ((float(q90[horizon - 1]) - float(q10[horizon - 1])) / current_close) * 100.0

    down_close_prob = _terminal_cdf_probability(
        quantiles,
        horizon=horizon,
        threshold=current_close,
    )
    if down_close_prob is None:
        return None
    up_close_prob = _clamp(1.0 - down_close_prob)
    support_breach_risk = _support_breach_risk_proxy(
        quantiles,
        support_anchor=support_anchor,
        horizon=horizon,
    )
    if support_breach_risk is None:
        support_breach_risk = _clamp(max(0.0, -breach_margin_pct) / 10.0)

    median_gain_score = score_pct(upside_pct, slope=5.0) / 100.0
    follow_through_quality = 100.0 * (
        (0.45 * up_close_prob)
        + (0.35 * (1.0 - support_breach_risk))
        + (0.20 * median_gain_score)
    )

    lower_tail = max(0.0, (current_close - float(q05[horizon - 1])) / current_close)
    upper_tail = max(0.0, (float(q95[horizon - 1]) - current_close) / current_close)
    dispersion_norm = _clamp(((float(q95[horizon - 1]) - float(q05[horizon - 1])) / current_close) / 0.20)
    downside_asymmetry = _clamp((lower_tail - upper_tail + 0.02) / 0.12)
    fragility_score = 100.0 * ((0.45 * dispersion_norm) + (0.55 * downside_asymmetry))

    return {
        "fm_upside_pct": round_or_none(upside_pct) or 0.0,
        "fm_breach_margin_pct": round_or_none(breach_margin_pct) or 0.0,
        "fm_dispersion_pct": round_or_none(dispersion_pct) or 0.0,
        "up_close_prob_proxy": round_or_none(up_close_prob) or 0.0,
        "down_close_prob_proxy": round_or_none(down_close_prob) or 0.0,
        "support_breach_risk_proxy": round_or_none(support_breach_risk) or 0.0,
        "follow_through_quality": round_or_none(follow_through_quality) or 0.0,
        "fragility_score": round_or_none(fragility_score) or 0.0,
    }


def generate_tsfm_rerank_rows(
    *,
    merged_candidate_rows: list[dict[str, Any]],
    market: str,
    model_id: str,
    model_family: str = TSFM_MODEL_FAMILY,
    as_of_date: str | None = None,
    load_ohlcv_frame_fn: Callable[..., pd.DataFrame] = load_local_ohlcv_frame,
    forecast_fn: Callable[
        [dict[str, np.ndarray], int, list[float]],
        dict[str, dict[float, np.ndarray]],
    ]
    | None = None,
) -> list[dict[str, Any]]:
    ordered_rows = [dict(row) for row in merged_candidate_rows]
    if not ordered_rows:
        return []

    history_map: dict[str, np.ndarray] = {}
    for row in ordered_rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        frame = load_ohlcv_frame_fn(
            symbol=symbol,
            market=market,
            as_of=as_of_date,
            price_policy=PricePolicy.SPLIT_ADJUSTED,
        )
        if frame.empty or len(frame) < 252:
            continue
        closes = pd.to_numeric(frame["close"], errors="coerce").dropna().tail(252).to_numpy(dtype=float)
        if len(closes) == 252:
            history_map[symbol] = closes

    predictions: dict[str, dict[float, np.ndarray]] = {}
    if history_map and forecast_fn is not None:
        predictions = forecast_fn(history_map, 20, list(DEFAULT_QUANTILE_LEVELS))

    scored_rows: list[dict[str, Any]] = []
    ok_rows: list[dict[str, Any]] = []
    for row in ordered_rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        output = _base_output_row(
            row,
            market=market,
            model_id=model_id,
            model_family=model_family,
        )
        if symbol not in history_map:
            output["fm_status"] = "INSUFFICIENT_HISTORY"
            scored_rows.append(output)
            continue
        if symbol not in predictions:
            output["fm_status"] = "MISSING_MODEL"
            scored_rows.append(output)
            continue

        close_series = history_map[symbol]
        current_close = float(close_series[-1])
        support_anchor, support_type = resolve_support_anchor(close_series)
        output["support_anchor_price"] = round_or_none(support_anchor)
        output["support_anchor_type"] = support_type
        if support_anchor is None or current_close <= support_anchor:
            output["fm_status"] = "UNSCORABLE"
            scored_rows.append(output)
            continue

        quantiles = predictions[symbol]
        horizon_scores: dict[int, float] = {}
        for horizon in DEFAULT_HORIZONS:
            metrics = compute_horizon_metrics(
                current_close=current_close,
                support_anchor=float(support_anchor),
                quantiles=quantiles,
                horizon=horizon,
            )
            if metrics is None:
                output["fm_status"] = "UNSCORABLE"
                break
            output[f"fm_upside_{horizon}d_pct"] = metrics["fm_upside_pct"]
            output[f"fm_breach_margin_{horizon}d_pct"] = metrics["fm_breach_margin_pct"]
            output[f"fm_dispersion_{horizon}d_pct"] = metrics["fm_dispersion_pct"]
            output[f"up_close_prob_proxy_{horizon}d"] = metrics["up_close_prob_proxy"]
            output[f"down_close_prob_proxy_{horizon}d"] = metrics["down_close_prob_proxy"]
            output[f"support_breach_risk_proxy_{horizon}d"] = metrics["support_breach_risk_proxy"]
            output[f"follow_through_quality_{horizon}d"] = metrics["follow_through_quality"]
            output[f"fragility_score_{horizon}d"] = metrics["fragility_score"]
            horizon_scores[horizon] = (
                (0.55 * float(metrics["follow_through_quality"]))
                + (0.25 * (100.0 * (1.0 - float(metrics["support_breach_risk_proxy"]))))
                + (0.20 * (100.0 - float(metrics["fragility_score"])))
            )
        if output["fm_status"] != "OK":
            scored_rows.append(output)
            continue

        final_score = (
            (0.20 * horizon_scores[5])
            + (0.60 * horizon_scores[10])
            + (0.20 * horizon_scores[20])
        )
        output["fm_rerank_score"] = round_or_none(final_score)
        scored_rows.append(output)
        ok_rows.append(output)

    ranked_ok = sorted(
        (row for row in ok_rows if row["fm_status"] == "OK"),
        key=lambda item: (-float(item["fm_rerank_score"] or 0.0), str(item["symbol"] or "")),
    )
    for index, row in enumerate(ranked_ok, start=1):
        row["fm_rank"] = index
    return scored_rows
