import pandas as pd

__all__ = ["calculate_percentile_rank"]


def calculate_percentile_rank(series: pd.Series) -> pd.Series:
    """Return percentile rank in percentage."""
    return series.rank(pct=True) * 100
