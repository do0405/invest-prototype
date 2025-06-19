"""Indicator utilities for IPOInvestmentScreener."""
import pandas as pd
from utils.calc_utils import calculate_rsi, calculate_atr

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

def calculate_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    df['ema_fast'] = df['close'].ewm(span=fast, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=slow, adjust=False).mean()
    df['macd'] = df['ema_fast'] - df['ema_slow']
    df['macd_signal'] = df['macd'].ewm(span=signal, adjust=False).mean()
    return df

def calculate_stochastic(df: pd.DataFrame, k_period: int = 14, d_period: int = 3) -> pd.DataFrame:
    df['lowest_low'] = df['low'].rolling(window=k_period).min()
    df['highest_high'] = df['high'].rolling(window=k_period).max()
    df['stoch_k'] = ((df['close'] - df['lowest_low']) / (df['highest_high'] - df['lowest_low'])) * 100
    df['stoch_d'] = df['stoch_k'].rolling(window=d_period).mean()
    return df

def calculate_track2_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df['ema_5'] = df['close'].ewm(span=5, adjust=False).mean()
    df['rsi_7'] = calculate_rsi(df, window=7)
    df['roc_5'] = df['close'].pct_change(periods=5) * 100
    df = calculate_macd(df)
    df = calculate_stochastic(df, k_period=7, d_period=3)
    df['volume_sma_20'] = df['volume'].rolling(window=20).mean()
    return df
