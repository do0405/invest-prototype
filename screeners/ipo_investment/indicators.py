"""Indicator utilities for IPOInvestmentScreener."""
import pandas as pd
from utils.calc_utils import calculate_rsi, calculate_atr
from utils.technical_indicators import calculate_macd, calculate_stochastic

__all__ = [
    "calculate_base_pattern",
    "calculate_macd",
    "calculate_stochastic",
    "calculate_track2_indicators",
]

def calculate_base_pattern(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate base pattern indicators for IPO stocks."""
    df['sma_10'] = df['close'].rolling(window=10).mean()
    df['sma_20'] = df['close'].rolling(window=20).mean()
    df['sma_50'] = df['close'].rolling(window=50).mean()

    df['std_20'] = df['close'].rolling(window=20).std()
    df['upper_band'] = df['sma_20'] + (df['std_20'] * 2)
    df['lower_band'] = df['sma_20'] - (df['std_20'] * 2)

    df = calculate_atr(df)
    df = calculate_rsi(df)

    df['volume_sma_20'] = df['volume'].rolling(window=20).mean()
    df['volume_ratio'] = df['volume'] / df['volume_sma_20']

    df['rolling_high'] = df['high'].rolling(window=20).max()
    df['rolling_low'] = df['low'].rolling(window=20).min()
    df['price_range'] = (df['rolling_high'] / df['rolling_low'] - 1) * 100
    return df

def calculate_track2_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df['ema_5'] = df['close'].ewm(span=5, adjust=False).mean()
    df['rsi_7'] = calculate_rsi(df, window=7)
    df['roc_5'] = df['close'].pct_change(periods=5) * 100
    df = calculate_macd(df)
    df = calculate_stochastic(df, k_period=7, d_period=3)
    df['volume_sma_20'] = df['volume'].rolling(window=20).mean()
    return df
