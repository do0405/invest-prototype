from __future__ import annotations

from typing import Any, TypeAlias

import pandas as pd


Record: TypeAlias = dict[str, Any]


def is_na_like(value: Any) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def to_float_or_none(value: Any) -> float | None:
    if is_na_like(value):
        return None
    try:
        casted = float(value)
    except (TypeError, ValueError):
        return None
    return None if is_na_like(casted) else casted


def row_to_record(row: pd.Series, *, drop_na: bool = False) -> Record:
    record: Record = {}
    for key, value in row.items():
        if drop_na and is_na_like(value):
            continue
        record[str(key)] = value
    return record


def frame_keyed_records(
    frame: pd.DataFrame,
    *,
    key_column: str,
    uppercase_keys: bool = False,
    drop_na: bool = False,
) -> dict[str, Record]:
    keyed: dict[str, Record] = {}
    if frame.empty or key_column not in frame.columns:
        return keyed

    for _, row in frame.iterrows():
        key_raw = row.get(key_column)
        if is_na_like(key_raw):
            continue
        key = str(key_raw).strip()
        if not key:
            continue
        if uppercase_keys:
            key = key.upper()
        keyed[key] = row_to_record(row, drop_na=drop_na)
    return keyed


def series_to_str_float_dict(series: pd.Series) -> dict[str, float]:
    mapping: dict[str, float] = {}
    numeric = pd.to_numeric(series, errors="coerce")
    for key, value in numeric.items():
        if is_na_like(value):
            continue
        mapping[str(key)] = float(value)
    return mapping


def series_to_str_text_dict(series: pd.Series) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for key, value in series.fillna("").astype(str).items():
        mapping[str(key)] = str(value)
    return mapping


def series_value_counts_to_int_dict(series: pd.Series) -> dict[str, int]:
    counts: dict[str, int] = {}
    for key, value in series.value_counts(dropna=False).items():
        counts[str(key)] = int(value)
    return counts
