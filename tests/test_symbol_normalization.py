from __future__ import annotations

import pandas as pd
import pytest

from utils.symbol_normalization import (
    normalize_provider_symbol_value,
    normalize_symbol_columns,
    normalize_symbol_value,
    validate_symbol_merge_frame,
)


def test_normalize_symbol_value_zero_pads_kr_numeric_codes() -> None:
    assert normalize_symbol_value("5930", "kr") == "005930"
    assert normalize_symbol_value(5930, "kr") == "005930"
    assert normalize_symbol_value("005930.0", "kr") == "005930"


def test_normalize_provider_symbol_value_preserves_provider_suffixes() -> None:
    assert normalize_provider_symbol_value(" 005930.ks ") == "005930.KS"
    assert normalize_provider_symbol_value("AAPL") == "AAPL"


def test_normalize_symbol_columns_handles_symbol_and_provider_columns() -> None:
    frame = pd.DataFrame(
        {
            "symbol": ["5930", "", None, "AAPL"],
            "provider_symbol": ["005930.ks", " ", None, "aapl"],
        }
    )

    normalized = normalize_symbol_columns(frame, "kr", columns=("symbol",), provider_columns=("provider_symbol",))

    assert list(normalized["symbol"].astype("string")) == ["005930", pd.NA, pd.NA, "AAPL"]
    assert list(normalized["provider_symbol"].astype("string")) == ["005930.KS", pd.NA, pd.NA, "AAPL"]


def test_validate_symbol_merge_frame_rejects_missing_symbol_column() -> None:
    frame = pd.DataFrame({"ticker": ["AAPL"]})

    with pytest.raises(ValueError, match="missing required merge key column"):
        validate_symbol_merge_frame(frame, market="us", frame_name="sample")


def test_validate_symbol_merge_frame_rejects_empty_symbol_values() -> None:
    frame = pd.DataFrame({"symbol": [" ", None]})

    with pytest.raises(ValueError, match="has no usable 'symbol' values"):
        validate_symbol_merge_frame(frame, market="us", frame_name="sample")
