import numpy as np
import pandas as pd
from scipy.signal import find_peaks
from utils.technical_indicators import calculate_macd, calculate_stochastic

__all__ = [
"calculate_macd",
"calculate_stochastic",
    "calculate_adx",
    "calculate_bollinger_bands",
    "calculate_moving_averages",
    "calculate_volume_indicators",
    "calculate_vwap",
    "calculate_obv",
    "calculate_ad",
    "detect_cup_and_handle",
]

def calculate_adx(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    df['tr1'] = abs(df['high'] - df['low'])
    df['tr2'] = abs(df['high'] - df['close'].shift(1))
    df['tr3'] = abs(df['low'] - df['close'].shift(1))
    df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
    df['up_move'] = df['high'] - df['high'].shift(1)
    df['down_move'] = df['low'].shift(1) - df['low']
    df['+dm'] = np.where((df['up_move'] > df['down_move']) & (df['up_move'] > 0), df['up_move'], 0)
    df['-dm'] = np.where((df['down_move'] > df['up_move']) & (df['down_move'] > 0), df['down_move'], 0)
    df['atr'] = df['tr'].rolling(window=period).mean()
    df['+di'] = 100 * (df['+dm'].rolling(window=period).mean() / df['atr'])
    df['-di'] = 100 * (df['-dm'].rolling(window=period).mean() / df['atr'])
    df['dx'] = 100 * (abs(df['+di'] - df['-di']) / (df['+di'] + df['-di']))
    df['adx'] = df['dx'].rolling(window=period).mean()
    return df


def calculate_bollinger_bands(df: pd.DataFrame, window: int = 20, num_std: int = 2) -> pd.DataFrame:
    df['sma_20'] = df['close'].rolling(window=window).mean()
    df['std_20'] = df['close'].rolling(window=window).std()
    df['upper_band'] = df['sma_20'] + (df['std_20'] * num_std)
    df['lower_band'] = df['sma_20'] - (df['std_20'] * num_std)
    df['bandwidth'] = (df['upper_band'] - df['lower_band']) / df['sma_20'] * 100
    return df


def calculate_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    df['sma_5'] = df['close'].rolling(window=5).mean()
    df['sma_10'] = df['close'].rolling(window=10).mean()
    df['sma_20'] = df['close'].rolling(window=20).mean()
    df['sma_50'] = df['close'].rolling(window=50).mean()
    df['sma_100'] = df['close'].rolling(window=100).mean()
    df['sma_200'] = df['close'].rolling(window=200).mean()
    df['sma_30w'] = df['close'].rolling(window=150).mean()
    return df


def calculate_volume_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df['volume_sma_20'] = df['volume'].rolling(window=20).mean()
    df['volume_sma_50'] = df['volume'].rolling(window=50).mean()
    df['volume_ratio'] = df['volume'] / df['volume_sma_20']
    df['volume_change'] = df['volume'].pct_change() * 100
    return df


def calculate_vwap(df: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    tp = (df['high'] + df['low'] + df['close']) / 3
    vol = df['volume']
    df['vwap'] = (tp * vol).rolling(window=window).sum() / vol.rolling(window=window).sum()
    return df


def calculate_obv(df: pd.DataFrame) -> pd.DataFrame:
    direction = np.sign(df['close'].diff()).fillna(0)
    df['obv'] = (direction * df['volume']).cumsum()
    return df


def calculate_ad(df: pd.DataFrame) -> pd.DataFrame:
    mfm = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
    mfm = mfm.replace([np.inf, -np.inf], 0).fillna(0)
    df['ad'] = (mfm * df['volume']).cumsum()
    return df


def detect_cup_and_handle(df: pd.DataFrame, window: int = 180) -> bool:
    try:
        if len(df) < window:
            return False
        data = df.tail(window)
        prices = data['close'].values
        peaks, _ = find_peaks(prices)
        troughs, _ = find_peaks(-prices)
        if len(peaks) < 2 or len(troughs) == 0:
            return False
        left = peaks[0]
        right = peaks[-1]
        bottom = troughs[troughs > left]
        bottom = bottom[bottom < right]
        if len(bottom) == 0:
            return False
        bottom = bottom[prices[bottom].argmin()]
        left_high = prices[left]
        right_high = prices[right]
        bottom_low = prices[bottom]
        depth = min(left_high, right_high) - bottom_low
        if depth <= 0:
            return False
        depth_pct = depth / min(left_high, right_high) * 100
        handle_low = prices[right:].min()
        handle_pct = (right_high - handle_low) / depth * 100
        return (
            abs(left_high - right_high) / min(left_high, right_high) <= 0.1
            and depth_pct >= 20
            and handle_pct <= 30
        )
    except Exception:
        return False
