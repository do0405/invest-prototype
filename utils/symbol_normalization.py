from __future__ import annotations

import re
from collections.abc import Iterable

import pandas as pd

from utils.typing_utils import is_na_like


_KR_NUMERIC_PATTERN = re.compile(r"^\d+(?:\.0+)?$")


def _coerce_text(value: object) -> str:
    if is_na_like(value):
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "<na>"}:
        return ""
    return text


def normalize_symbol_value(value: object, market: str = "us") -> str:
    text = _coerce_text(value).upper()
    if not text:
        return ""
    if str(market or "").strip().lower() == "kr" and _KR_NUMERIC_PATTERN.fullmatch(text):
        return text.split(".", 1)[0].zfill(6)
    return text


def normalize_provider_symbol_value(value: object) -> str:
    return _coerce_text(value).upper()


def normalize_symbol_columns(
    frame: pd.DataFrame,
    market: str = "us",
    *,
    columns: Iterable[str] = ("symbol",),
    provider_columns: Iterable[str] = (),
) -> pd.DataFrame:
    normalized = frame.copy()
    for column in columns:
        if column not in normalized.columns:
            continue
        series = normalized[column].map(lambda value: normalize_symbol_value(value, market))
        typed_series = pd.Series(series, index=normalized.index, dtype="string")
        normalized[column] = typed_series.mask(typed_series == "")
    for column in provider_columns:
        if column not in normalized.columns:
            continue
        series = normalized[column].map(normalize_provider_symbol_value)
        typed_series = pd.Series(series, index=normalized.index, dtype="string")
        normalized[column] = typed_series.mask(typed_series == "")
    return normalized


def validate_symbol_merge_frame(
    frame: pd.DataFrame,
    *,
    market: str,
    frame_name: str,
    columns: Iterable[str] = ("symbol",),
    provider_columns: Iterable[str] = (),
) -> pd.DataFrame:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        missing_columns = ", ".join(missing)
        raise ValueError(f"{frame_name} is missing required merge key column(s): {missing_columns}")

    normalized = normalize_symbol_columns(
        frame,
        market,
        columns=columns,
        provider_columns=provider_columns,
    )

    for column in columns:
        usable = normalized[column].dropna()
        if normalized.empty:
            continue
        if usable.empty:
            raise ValueError(f"{frame_name} has no usable '{column}' values after symbol normalization")
        if not pd.api.types.is_string_dtype(normalized[column]):
            raise ValueError(f"{frame_name}.{column} is not string-typed after symbol normalization")
    return normalized
