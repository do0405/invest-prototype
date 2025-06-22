"""Common technical indicator calculations used across screeners."""

from __future__ import annotations

import pandas as pd
import numpy as np

__all__ = ["calculate_macd", "calculate_stochastic"]


def calculate_macd(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    include_hist: bool = False,
) -> pd.DataFrame:
    """Calculate MACD indicators and optionally histogram."""
    df.loc[:, "ema_fast"] = df["close"].ewm(span=fast, adjust=False).mean()
    df.loc[:, "ema_slow"] = df["close"].ewm(span=slow, adjust=False).mean()
    df.loc[:, "macd"] = df["ema_fast"] - df["ema_slow"]
    df.loc[:, "macd_signal"] = df["macd"].ewm(span=signal, adjust=False).mean()
    if include_hist:
        df.loc[:, "macd_hist"] = df["macd"] - df["macd_signal"]
    return df


def calculate_stochastic(
    df: pd.DataFrame,
    k_period: int = 14,
    d_period: int = 3,
) -> pd.DataFrame:
    """Calculate stochastic oscillator."""
    df.loc[:, "lowest_low"] = df["low"].rolling(window=k_period).min()
    df.loc[:, "highest_high"] = df["high"].rolling(window=k_period).max()
    df.loc[:, "stoch_k"] = (
        (df["close"] - df["lowest_low"]) / (df["highest_high"] - df["lowest_low"])
    ) * 100
    df.loc[:, "stoch_d"] = df["stoch_k"].rolling(window=d_period).mean()
    return df
