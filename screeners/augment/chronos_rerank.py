from __future__ import annotations

import importlib
from typing import Any, Callable

import numpy as np
import pandas as pd

from utils.market_data_contract import load_local_ohlcv_frame

from .tsfm_metrics import (
    DEFAULT_QUANTILE_LEVELS,
    TSFM_MODEL_FAMILY,
    build_soft_skip_rows as build_tsfm_soft_skip_rows,
    generate_tsfm_rerank_rows,
)

CHRONOS_MODEL_ID = "chronos2"


def _default_forecast_fn(
    series_map: dict[str, np.ndarray],
    prediction_length: int,
    quantile_levels: list[float],
) -> dict[str, dict[float, np.ndarray]]:
    chronos_module = importlib.import_module("chronos")
    Chronos2Pipeline = getattr(chronos_module, "Chronos2Pipeline")
    try:
        pipeline = Chronos2Pipeline.from_pretrained("amazon/chronos-2", device_map="cpu")
    except Exception as exc:
        raise RuntimeError(f"MISSING_MODEL: {exc}") from exc

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
    try:
        pred_df = pipeline.predict_df(
            context_df,
            prediction_length=prediction_length,
            quantile_levels=quantile_levels,
            id_column="id",
            timestamp_column="timestamp",
            target="target",
        )
    except Exception as exc:
        raise RuntimeError(f"FORECAST_RUNTIME: {exc}") from exc

    result: dict[str, dict[float, np.ndarray]] = {}
    for symbol, symbol_frame in pred_df.groupby("id", sort=False):
        result[symbol] = {}
        for quantile in quantile_levels:
            column = "predictions" if abs(float(quantile) - 0.5) < 1e-9 and "predictions" in symbol_frame.columns else str(quantile)
            result[symbol][float(quantile)] = pd.to_numeric(
                symbol_frame[column],
                errors="coerce",
            ).to_numpy(dtype=float)
    return result


def build_soft_skip_rows(
    *,
    merged_candidate_rows: list[dict[str, Any]],
    market: str,
    fm_status: str,
) -> list[dict[str, Any]]:
    return build_tsfm_soft_skip_rows(
        merged_candidate_rows=merged_candidate_rows,
        market=market,
        fm_status=fm_status,
        model_id=CHRONOS_MODEL_ID,
        model_family=TSFM_MODEL_FAMILY,
    )


def generate_chronos_rerank_rows(
    *,
    merged_candidate_rows: list[dict[str, Any]],
    market: str,
    as_of_date: str | None = None,
    load_ohlcv_frame_fn: Callable[..., pd.DataFrame] = load_local_ohlcv_frame,
    forecast_fn: Callable[
        [dict[str, np.ndarray], int, list[float]],
        dict[str, dict[float, np.ndarray]],
    ]
    | None = None,
) -> list[dict[str, Any]]:
    return generate_tsfm_rerank_rows(
        merged_candidate_rows=merged_candidate_rows,
        market=market,
        model_id=CHRONOS_MODEL_ID,
        model_family=TSFM_MODEL_FAMILY,
        as_of_date=as_of_date,
        load_ohlcv_frame_fn=load_ohlcv_frame_fn,
        forecast_fn=forecast_fn or _default_forecast_fn,
    )


__all__ = [
    "CHRONOS_MODEL_ID",
    "DEFAULT_QUANTILE_LEVELS",
    "build_soft_skip_rows",
    "generate_chronos_rerank_rows",
]
