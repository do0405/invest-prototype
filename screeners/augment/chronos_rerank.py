from __future__ import annotations

import importlib
from typing import Any, Callable

import numpy as np
import pandas as pd

from utils.market_data_contract import PricePolicy, load_local_ohlcv_frame


def _round_or_none(value: float | None, digits: int = 4) -> float | None:
    if value is None or not np.isfinite(value):
        return None
    return round(float(value), digits)


def _score_pct(
    value: float,
    *,
    slope: float,
    midpoint: float = 0.0,
    inverse: bool = False,
) -> float:
    normalized = 50.0 + ((midpoint - value) if inverse else (value - midpoint)) * slope
    return float(max(0.0, min(100.0, normalized)))


def _resolve_support_anchor(close_series: np.ndarray) -> tuple[float | None, str]:
    series = pd.Series(close_series, dtype=float)
    ema20 = (
        _round_or_none(float(series.ewm(span=20, adjust=False).mean().iloc[-1]))
        if len(series) >= 20
        else None
    )
    low20 = _round_or_none(float(series.tail(20).min())) if len(series) >= 20 else None
    sma50 = _round_or_none(float(series.tail(50).mean())) if len(series) >= 50 else None

    if ema20 is not None and low20 is not None:
        return max(float(ema20), float(low20)), "EMA20_LOW20_MAX"
    if ema20 is not None:
        return float(ema20), "EMA20_ONLY"
    if low20 is not None:
        return float(low20), "LOW20_ONLY"
    if sma50 is not None:
        return float(sma50), "SMA50"
    return None, "NONE"


def _default_forecast_fn(
    series_map: dict[str, np.ndarray],
    prediction_length: int,
    quantile_levels: list[float],
) -> dict[str, dict[float, np.ndarray]]:
    import pandas as pd

    chronos_module = importlib.import_module("chronos")
    Chronos2Pipeline = getattr(chronos_module, "Chronos2Pipeline")
    pipeline = Chronos2Pipeline.from_pretrained("amazon/chronos-2", device_map="cpu")
    context_rows: list[dict[str, Any]] = []
    for symbol, values in series_map.items():
        dates = pd.date_range("2000-01-01", periods=len(values), freq="B")
        context_rows.extend(
            {
                "id": symbol,
                "timestamp": timestamp,
                "target": float(value),
            }
            for timestamp, value in zip(dates, values, strict=False)
        )
    context_df = pd.DataFrame(context_rows)
    pred_df = pipeline.predict_df(
        context_df,
        prediction_length=prediction_length,
        quantile_levels=quantile_levels,
        id_column="id",
        timestamp_column="timestamp",
        target="target",
    )
    result: dict[str, dict[float, np.ndarray]] = {}
    for symbol, symbol_frame in pred_df.groupby("id", sort=False):
        result[symbol] = {}
        for quantile in quantile_levels:
            column = "predictions" if quantile == 0.5 and "predictions" in symbol_frame.columns else str(quantile)
            result[symbol][quantile] = pd.to_numeric(
                symbol_frame[column],
                errors="coerce",
            ).to_numpy(dtype=float)
    return result


def generate_chronos_rerank_rows(
    *,
    merged_candidate_rows: list[dict[str, Any]],
    market: str,
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
            as_of=None,
            price_policy=PricePolicy.SPLIT_ADJUSTED,
        )
        if frame.empty or len(frame) < 252:
            continue
        closes = (
            pd.to_numeric(frame["close"], errors="coerce").dropna().tail(252).to_numpy(dtype=float)
        )
        if len(closes) == 252:
            history_map[symbol] = closes

    predictions: dict[str, dict[float, np.ndarray]] = {}
    if history_map:
        resolved_forecast = forecast_fn or _default_forecast_fn
        predictions = resolved_forecast(history_map, 20, [0.1, 0.5, 0.9])

    scored_rows: list[dict[str, Any]] = []
    ok_rows: list[dict[str, Any]] = []
    for row in ordered_rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        output = {
            "symbol": symbol,
            "market": str(row.get("market") or market).upper(),
            "source_tags": list(row.get("source_tags", [])),
            "primary_source_tag": str(row.get("primary_source_tag") or ""),
            "support_anchor_price": None,
            "support_anchor_type": "",
            "fm_upside_5d_pct": None,
            "fm_upside_10d_pct": None,
            "fm_upside_20d_pct": None,
            "fm_breach_margin_5d_pct": None,
            "fm_breach_margin_10d_pct": None,
            "fm_breach_margin_20d_pct": None,
            "fm_dispersion_5d_pct": None,
            "fm_dispersion_10d_pct": None,
            "fm_dispersion_20d_pct": None,
            "fm_rerank_score": None,
            "fm_rank": None,
            "fm_status": "OK",
        }
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
        support_anchor, support_type = _resolve_support_anchor(close_series)
        output["support_anchor_price"] = _round_or_none(support_anchor)
        output["support_anchor_type"] = support_type
        if support_anchor is None or current_close <= support_anchor:
            output["fm_status"] = "UNSCORABLE"
            scored_rows.append(output)
            continue

        quantiles = predictions[symbol]
        horizon_scores: dict[int, float] = {}
        for horizon in (5, 10, 20):
            q10 = np.asarray(quantiles[0.1], dtype=float)[:horizon]
            q50 = np.asarray(quantiles[0.5], dtype=float)[:horizon]
            q90 = np.asarray(quantiles[0.9], dtype=float)[:horizon]
            upside_pct = ((q50[horizon - 1] / current_close) - 1.0) * 100.0
            breach_margin_pct = ((float(np.min(q10)) / support_anchor) - 1.0) * 100.0
            dispersion_pct = ((q90[horizon - 1] - q10[horizon - 1]) / current_close) * 100.0
            output[f"fm_upside_{horizon}d_pct"] = _round_or_none(upside_pct)
            output[f"fm_breach_margin_{horizon}d_pct"] = _round_or_none(breach_margin_pct)
            output[f"fm_dispersion_{horizon}d_pct"] = _round_or_none(dispersion_pct)

            upside_score = _score_pct(upside_pct, slope=5.0)
            breach_score = _score_pct(breach_margin_pct, slope=12.0)
            dispersion_score = _score_pct(
                dispersion_pct,
                slope=8.0,
                inverse=True,
            )
            horizon_scores[horizon] = (
                (0.45 * upside_score)
                + (0.35 * breach_score)
                + (0.20 * dispersion_score)
            )

        final_score = (
            (0.2 * horizon_scores[5])
            + (0.6 * horizon_scores[10])
            + (0.2 * horizon_scores[20])
        )
        output["fm_rerank_score"] = _round_or_none(final_score)
        scored_rows.append(output)
        ok_rows.append(output)

    ranked_ok = sorted(
        (row for row in ok_rows if row["fm_status"] == "OK"),
        key=lambda item: (-float(item["fm_rerank_score"] or 0.0), str(item["symbol"] or "")),
    )
    for index, row in enumerate(ranked_ok, start=1):
        row["fm_rank"] = index
    return scored_rows
