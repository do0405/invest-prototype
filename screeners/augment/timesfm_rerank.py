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

TIMESFM_MODEL_ID = "timesfm2p5"


def _default_forecast_fn(
    series_map: dict[str, np.ndarray],
    prediction_length: int,
    quantile_levels: list[float],
) -> dict[str, dict[float, np.ndarray]]:
    timesfm_module = importlib.import_module("timesfm")
    model_cls = getattr(timesfm_module, "TimesFM_2p5_200M_torch")
    forecast_config_cls = getattr(timesfm_module, "ForecastConfig")
    try:
        model = model_cls.from_pretrained("google/timesfm-2.5-200m-pytorch")
        model.compile(
            forecast_config_cls(
                max_context=1024,
                max_horizon=256,
                normalize_inputs=True,
                use_continuous_quantile_head=True,
                force_flip_invariance=True,
                infer_is_positive=True,
                fix_quantile_crossing=True,
            )
        )
    except Exception as exc:
        raise RuntimeError(f"MISSING_MODEL: {exc}") from exc

    symbols = list(series_map.keys())
    inputs = [np.asarray(series_map[symbol], dtype=float) for symbol in symbols]
    try:
        _point_forecast, quantile_forecast = model.forecast(
            horizon=prediction_length,
            inputs=inputs,
        )
    except Exception as exc:
        raise RuntimeError(f"FORECAST_RUNTIME: {exc}") from exc

    quantile_forecast = np.asarray(quantile_forecast, dtype=float)
    if quantile_forecast.ndim != 3 or quantile_forecast.shape[0] != len(symbols):
        raise RuntimeError("FORECAST_RUNTIME: unexpected TimesFM quantile forecast shape")

    native_levels = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]
    result: dict[str, dict[float, np.ndarray]] = {}
    for index, symbol in enumerate(symbols):
        symbol_quantiles = quantile_forecast[index]
        if symbol_quantiles.shape[0] < prediction_length or symbol_quantiles.shape[1] < 10:
            raise RuntimeError("FORECAST_RUNTIME: TimesFM quantile head returned incomplete output")

        mapped: dict[float, np.ndarray] = {}
        for native_index, native_level in enumerate(native_levels, start=1):
            mapped[native_level] = np.asarray(symbol_quantiles[:prediction_length, native_index], dtype=float)

        q10 = mapped[0.10]
        q20 = mapped[0.20]
        q80 = mapped[0.80]
        q90 = mapped[0.90]
        mapped[0.05] = np.asarray(q10 - (0.5 * (q20 - q10)), dtype=float)
        mapped[0.95] = np.asarray(q90 + (0.5 * (q90 - q80)), dtype=float)

        normalized: dict[float, np.ndarray] = {}
        for level in quantile_levels:
            float_level = float(level)
            if float_level in mapped:
                normalized[float_level] = mapped[float_level]
        for missing_level in DEFAULT_QUANTILE_LEVELS:
            normalized.setdefault(float(missing_level), mapped.get(float(missing_level), mapped[0.50]))
        result[symbol] = normalized
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
        model_id=TIMESFM_MODEL_ID,
        model_family=TSFM_MODEL_FAMILY,
    )


def generate_timesfm_rerank_rows(
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
        model_id=TIMESFM_MODEL_ID,
        model_family=TSFM_MODEL_FAMILY,
        as_of_date=as_of_date,
        load_ohlcv_frame_fn=load_ohlcv_frame_fn,
        forecast_fn=forecast_fn or _default_forecast_fn,
    )


__all__ = [
    "TIMESFM_MODEL_ID",
    "DEFAULT_QUANTILE_LEVELS",
    "build_soft_skip_rows",
    "generate_timesfm_rerank_rows",
]
