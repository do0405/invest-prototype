from __future__ import annotations

import pytest
import pandas as pd

from utils.indicator_helpers import adr_percent


def test_adr_percent_returns_none_when_window_contains_zero_close() -> None:
    frame = pd.DataFrame(
        {
            "high": [10.0, 12.0, 14.0],
            "low": [8.0, 10.0, 12.0],
            "close": [9.0, 0.0, 13.0],
        }
    )

    assert adr_percent(frame, length=3) is None


def test_adr_percent_returns_expected_average_for_valid_window() -> None:
    frame = pd.DataFrame(
        {
            "high": [12.0, 15.0, 18.0],
            "low": [6.0, 9.0, 12.0],
            "close": [9.0, 12.0, 15.0],
        }
    )

    expected = (((12.0 - 6.0) / 9.0) * 100.0 + ((15.0 - 9.0) / 12.0) * 100.0 + ((18.0 - 12.0) / 15.0) * 100.0) / 3.0

    assert adr_percent(frame, length=3) == pytest.approx(expected)