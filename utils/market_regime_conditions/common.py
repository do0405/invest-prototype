from typing import Dict, Tuple
import pandas as pd
from config import MARKET_REGIME_CRITERIA
from ..market_regime_helpers import (
    calculate_high_low_index,
    calculate_advance_decline_trend,
    calculate_put_call_ratio,
    calculate_ma_distance,
    count_consecutive_below_ma,
)

__all__ = [
    "_strength_above",
    "_strength_below",
    "MARKET_REGIME_CRITERIA",
    "calculate_high_low_index",
    "calculate_advance_decline_trend",
    "calculate_put_call_ratio",
    "calculate_ma_distance",
    "count_consecutive_below_ma",
]


def _strength_above(value: float, threshold: float) -> float:
    """Return normalized strength for values above ``threshold``."""
    if threshold == 0:
        return 0.0
    return max(0.0, min(1.0, (value - threshold) / abs(threshold)))


def _strength_below(value: float, threshold: float) -> float:
    """Return normalized strength for values below ``threshold``."""
    if threshold == 0:
        return 0.0
    return max(0.0, min(1.0, (threshold - value) / abs(threshold)))
