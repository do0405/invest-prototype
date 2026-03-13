from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import numpy as np
import pandas as pd


def clamp_float(value: float | None, lower: float | None = None, upper: float | None = None) -> float:
    result = float(value if value is not None else 0.0)
    if np.isnan(result) or np.isinf(result):
        result = 0.0
    if lower is not None:
        result = max(result, float(lower))
    if upper is not None:
        result = min(result, float(upper))
    return float(result)


def numeric_series(values: pd.Series | Sequence[Any] | None) -> pd.Series:
    if values is None:
        return pd.Series(dtype=float)
    series = values if isinstance(values, pd.Series) else pd.Series(list(values))
    numeric = pd.to_numeric(series, errors="coerce")
    numeric = numeric.replace([np.inf, -np.inf], np.nan).dropna()
    return numeric.astype(float)


def bounded_quantile_value(
    values: pd.Series | Sequence[Any] | None,
    quantile: float,
    default: float,
    *,
    lower: float | None = None,
    upper: float | None = None,
    positive_only: bool = False,
    min_count: int = 8,
) -> float:
    numeric = numeric_series(values)
    if positive_only:
        numeric = numeric[numeric > 0]
    if len(numeric) < int(min_count):
        return clamp_float(default, lower, upper)

    q = min(max(float(quantile), 0.0), 1.0)
    try:
        value = float(numeric.quantile(q))
    except Exception:
        value = float(default)
    return clamp_float(value, lower, upper)


def percentile_rank_series(values: pd.Series | Sequence[Any] | None) -> pd.Series:
    numeric = numeric_series(values)
    if numeric.empty:
        return pd.Series(dtype=float)
    if numeric.nunique(dropna=True) <= 1:
        return pd.Series(100.0, index=numeric.index, dtype=float)
    return numeric.rank(pct=True, method="average").fillna(0.5) * 100.0
