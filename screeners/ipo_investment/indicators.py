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
    # DataFrame 복사본 생성
    result_df = df.copy()
    
    result_df['sma_10'] = result_df['close'].rolling(window=10).mean()
    result_df['sma_20'] = result_df['close'].rolling(window=20).mean()
    result_df['sma_50'] = result_df['close'].rolling(window=50).mean()

    result_df['std_20'] = result_df['close'].rolling(window=20).std()
    result_df['upper_band'] = result_df['sma_20'] + (result_df['std_20'] * 2)
    result_df['lower_band'] = result_df['sma_20'] - (result_df['std_20'] * 2)

    # ATR 계산 (Series 반환되므로 컬럼으로 할당)
    result_df['atr'] = calculate_atr(result_df)
    
    # RSI 계산 (DataFrame 반환)
    result_df = calculate_rsi(result_df)

    result_df['volume_sma_20'] = result_df['volume'].rolling(window=20).mean()
    result_df['volume_ratio'] = result_df['volume'] / result_df['volume_sma_20']

    result_df['rolling_high'] = result_df['high'].rolling(window=20).max()
    result_df['rolling_low'] = result_df['low'].rolling(window=20).min()
    result_df['price_range'] = (result_df['rolling_high'] / result_df['rolling_low'] - 1) * 100
    return result_df

def calculate_track2_indicators(df: pd.DataFrame) -> pd.DataFrame:
    # DataFrame 복사본 생성
    result_df = df.copy()
    
    result_df['ema_5'] = result_df['close'].ewm(span=5, adjust=False).mean()
    result_df = calculate_rsi(result_df, window=7)
    result_df['roc_5'] = result_df['close'].pct_change(periods=5) * 100
    result_df = calculate_macd(result_df)
    result_df = calculate_stochastic(result_df, k_period=7, d_period=3)
    result_df['volume_sma_20'] = result_df['volume'].rolling(window=20).mean()
    return result_df
