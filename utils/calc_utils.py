# -*- coding: utf-8 -*-
"""Financial calculation helper functions."""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from pytz import timezone

__all__ = [
    "get_us_market_today",
    "clean_tickers",
    "get_date_range",
    "calculate_atr",
    "calculate_rsi",
    "calculate_adx",
    "calculate_historical_volatility",
    "check_sp500_condition",
]


def get_us_market_today():
    """Return today's date in US/Eastern time zone."""
    et_now = datetime.now(timezone('US/Eastern'))
    return et_now.date()


def clean_tickers(tickers):
    """Clean ticker list by filtering unusual strings."""
    if not tickers:
        return []
    cleaned = [t for t in tickers if t is not None and not pd.isna(t)]
    cleaned = [str(t).strip() for t in cleaned]
    filtered = []
    for ticker in cleaned:
        if not ticker or ticker.isspace():
            continue
        if not all(c.isalnum() or c in '.-$^' for c in ticker):
            log_msg = f"⚠️ 비정상적인 티커 제외: {ticker}"
            reasons = []
            if '<' in ticker or '>' in ticker:
                reasons.append("HTML 태그 포함")
            css_patterns = ['{', '}', ':', ';']
            css_keywords = ['width', 'height', 'position', 'margin', 'padding', 'color', 'background', 'font', 'display', 'style']
            if any(p in ticker for p in css_patterns):
                reasons.append("CSS 구문 포함")
            elif any(kw in ticker.lower() for kw in css_keywords):
                reasons.append("CSS 속성 포함")
            js_patterns = ['=', '(', ')', '[', ']', '&&', '||', '!', '?', '.']
            js_keywords = ['function', 'var', 'let', 'const', 'return', 'if', 'else', 'for', 'while', 'class', 'new', 'this', 'document', 'window']
            if any(p in ticker for p in js_patterns):
                reasons.append("JS 구문 포함")
            elif any(kw in ticker.lower() for kw in js_keywords):
                reasons.append("JS 키워드 포함")
            elif '.className' in ticker or 'RegExp' in ticker:
                reasons.append("JS API 포함")
            if ticker.strip().startswith('//') or ticker.strip().startswith('/*') or ticker.strip().endswith('*/'):
                reasons.append("JS 주석 포함")
            if not reasons:
                reasons.append("비정상 문자 포함")
            print(f"{log_msg} - 이유: {', '.join(reasons)}")
            continue
        if len(ticker) > 8:
            print(f"⚠️ 너무 긴 티커 제외 ({len(ticker)}자): {ticker}")
            continue
        filtered.append(ticker)
    cleaned = filtered
    cleaned = [t for t in cleaned if t]
    filtered = []
    for ticker in cleaned:
        if '<' in ticker or '>' in ticker:
            print(f"⚠️ HTML 태그가 포함된 티커 제외: {ticker}")
            continue
        if '=' in ticker or '(' in ticker or ')' in ticker or '.className' in ticker or 'RegExp' in ticker:
            print(f"⚠️ JavaScript 코드로 추정되는 티커 제외: {ticker}")
            continue
        if ticker.strip().startswith('//') or ticker.strip().startswith('/*') or ticker.strip().endswith('*/'):
            print(f"⚠️ JavaScript 주석으로 추정되는 티커 제외: {ticker}")
            continue
        filtered.append(ticker)
    filtered = list(set(filtered))
    return filtered


def get_date_range(end_date=None, days=30):
    """Return (start, end) date range from end_date backwards."""
    if end_date is None:
        end_date = datetime.now().date()
    elif isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    start_date = end_date - timedelta(days=days)
    return start_date, end_date


def calculate_atr(df, window=10):
    """Calculate Average True Range."""
    try:
        required_cols = ['high', 'low', 'close']
        for col in required_cols:
            if col not in df.columns:
                print(f"⚠️ ATR 계산에 필요한 '{col}' 컬럼이 없습니다.")
                return pd.Series(index=df.index)
        df = df.copy()
        df['prev_close'] = df['close'].shift(1)
        df['tr1'] = abs(df['high'] - df['low'])
        df['tr2'] = abs(df['high'] - df['prev_close'])
        df['tr3'] = abs(df['low'] - df['prev_close'])
        df['true_range'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
        df['atr'] = df['true_range'].rolling(window=window).mean()
        return df['atr']
    except Exception as e:
        print(f"❌ ATR 계산 오류: {e}")
        return pd.Series(index=df.index)


def calculate_rsi(df, window=14):
    """Calculate Relative Strength Index."""
    try:
        if 'close' not in df.columns:
            print(f"⚠️ RSI 계산에 필요한 'close' 컬럼이 없습니다.")
            return pd.Series(index=df.index)
        delta = df['close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=window).mean()
        avg_loss = loss.rolling(window=window).mean()
        rs = avg_gain / avg_loss.where(avg_loss != 0, 1)
        rsi = 100 - (100 / (1 + rs))
        return rsi
    except Exception as e:
        print(f"❌ RSI 계산 오류: {e}")
        return pd.Series(index=df.index)


def calculate_adx(df, window=14):
    """Calculate Average Directional Index."""
    try:
        required_cols = ['high', 'low', 'close']
        for col in required_cols:
            if col not in df.columns:
                print(f"⚠️ ADX 계산에 필요한 '{col}' 컬럼이 없습니다.")
                return pd.Series(index=df.index)
        df = df.copy()
        df['prev_high'] = df['high'].shift(1)
        df['prev_low'] = df['low'].shift(1)
        df['prev_close'] = df['close'].shift(1)
        df['up_move'] = df['high'] - df['prev_high']
        df['down_move'] = df['prev_low'] - df['low']
        df['+dm'] = np.where((df['up_move'] > df['down_move']) & (df['up_move'] > 0), df['up_move'], 0)
        df['-dm'] = np.where((df['down_move'] > df['up_move']) & (df['down_move'] > 0), df['down_move'], 0)
        df['tr1'] = abs(df['high'] - df['low'])
        df['tr2'] = abs(df['high'] - df['prev_close'])
        df['tr3'] = abs(df['low'] - df['prev_close'])
        df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
        df['+dm_avg'] = df['+dm'].rolling(window=window).mean()
        df['-dm_avg'] = df['-dm'].rolling(window=window).mean()
        df['tr_avg'] = df['tr'].rolling(window=window).mean()
        df['+di'] = 100 * df['+dm_avg'] / df['tr_avg']
        df['-di'] = 100 * df['-dm_avg'] / df['tr_avg']
        df['dx'] = 100 * abs(df['+di'] - df['-di']) / (df['+di'] + df['-di'])
        df['adx'] = df['dx'].rolling(window=window).mean()
        return df['adx']
    except Exception as e:
        print(f"❌ ADX 계산 오류: {e}")
        return pd.Series(index=df.index)


def calculate_historical_volatility(df, window=60, annualize=True):
    """Calculate historical volatility."""
    try:
        if 'close' not in df.columns:
            print(f"⚠️ 변동성 계산에 필요한 'close' 컬럼이 없습니다.")
            return pd.Series(index=df.index)
        df = df.copy()
        df['log_return'] = np.log(df['close'] / df['close'].shift(1))
        volatility = df['log_return'].rolling(window=window).std()
        if annualize:
            volatility = volatility * np.sqrt(252) * 100
        else:
            volatility = volatility * 100
        return volatility
    except Exception as e:
        print(f"❌ 변동성 계산 오류: {e}")
        return pd.Series(index=df.index)


def check_sp500_condition(data_dir, ma_days=100):
    """Check if SPY close is above moving average."""
    try:
        spy_file = os.path.join(data_dir, 'SPY.csv')
        if not os.path.exists(spy_file):
            print("⚠️ SPY 데이터 파일을 찾을 수 없습니다.")
            return False
        spy_df = pd.read_csv(spy_file)
        spy_df.columns = [col.lower() for col in spy_df.columns]
        if 'date' in spy_df.columns:
            spy_df['date'] = pd.to_datetime(spy_df['date'], utc=True)
            spy_df = spy_df.sort_values('date')
        else:
            print("⚠️ SPY 데이터에 날짜 컬럼이 없습니다.")
            return False
        if len(spy_df) < ma_days:
            print("⚠️ SPY 데이터가 충분하지 않습니다.")
            return False
        spy_df[f'ma{ma_days}'] = spy_df['close'].rolling(window=ma_days).mean()
        latest_spy = spy_df.iloc[-1]
        spy_condition = latest_spy['close'] > latest_spy[f'ma{ma_days}']
        if not spy_condition:
            print(f"⚠️ SPY 종가가 {ma_days}일 이동평균선 아래에 있습니다.")
            return False
        return True
    except Exception as e:
        print(f"❌ SPY 조건 확인 오류: {e}")
        return False
