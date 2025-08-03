"""Helper functions for market regime indicator."""

import os
import pandas as pd
import numpy as np
from typing import Dict, Optional

from config import DATA_US_DIR, BREADTH_DATA_DIR, OPTION_DATA_DIR

__all__ = [
    "load_index_data",
    "calculate_high_low_index",
    "calculate_advance_decline_trend",
    "calculate_ma_distance",
    "count_consecutive_below_ma",
    "INDEX_TICKERS",
    "MARKET_REGIMES",
]


INDEX_TICKERS = {
    "SPY": "S&P 500 (대형주)",
    "QQQ": "나스닥 100 (기술주)",
    "IWM": "Russell 2000 (소형주)",
    "MDY": "S&P 400 MidCap (중형주)",
    "IBB": "바이오텍 ETF",
    "XBI": "바이오텍 ETF",
    "VIX": "변동성 지수",
}

MARKET_REGIMES = {
    "aggressive_bull": {
        "name": "공격적 상승장 (Aggressive Bull Market)",
        "score_range": (80, 100),
        "description": "모든 주요 지수가 강세를 보이며 시장 심리가 매우 낙관적인 상태입니다.",
        "strategy": "소형주, 성장주 비중 확대",
    },
    "bull": {
        "name": "상승장 (Bull Market)",
        "score_range": (60, 79),
        "description": "대형주 중심의 상승세가 유지되나 일부 섹터에서 약세가 나타나기 시작합니다.",
        "strategy": "대형주 중심, 리더주 선별 투자",
    },
    "correction": {
        "name": "조정장 (Correction Market)",
        "score_range": (40, 59),
        "description": "주요 지수가 단기 이동평균선 아래로 하락하며 조정이 진행중입니다.",
        "strategy": "현금 비중 증대, 방어적 포지션",
    },
    "risk_management": {
        "name": "위험 관리장 (Risk Management Market)",
        "score_range": (20, 39),
        "description": "주요 지수가 장기 이동평균선 아래로 하락하며 위험이 증가하고 있습니다.",
        "strategy": "신규 투자 중단, 손절매 기준 엄격 적용",
    },
    "bear": {
        "name": "완전한 약세장 (Full Bear Market)",
        "score_range": (0, 19),
        "description": "모든 주요 지수가 장기 이동평균선 아래에서 지속적인 하락세를 보입니다.",
        "strategy": "현금 보유, 적립식 투자 외 투자 자제",
    },
}


def load_index_data(ticker: str, days: int = 200) -> Optional[pd.DataFrame]:
    """Load index price data with moving averages."""
    try:
        if ticker == 'VIX':
            file_path = os.path.join(OPTION_DATA_DIR, 'vix.csv')
            if not os.path.exists(file_path):
                from data_collectors.market_breadth_collector import MarketBreadthCollector
                collector = MarketBreadthCollector()
                collector.collect_vix_data(days)
        else:
            file_path = os.path.join(DATA_US_DIR, f"{ticker}.csv")
        if not os.path.exists(file_path):
            print(f"⚠️ {ticker} 데이터 파일을 찾을 수 없습니다.")
            return None

        from utils.screener_utils import read_csv_flexible
        df = read_csv_flexible(file_path, required_columns=['date', 'close'])
        if df is None:
            print(f"⚠️ {ticker} 데이터 파일 읽기 실패.")
            return None
        
        # VIX 데이터의 경우 컬럼명 매핑
        if ticker == 'VIX':
            if 'vix_close' in df.columns:
                df['close'] = df['vix_close']
            if 'vix_high' in df.columns:
                df['high'] = df['vix_high']
            if 'vix_low' in df.columns:
                df['low'] = df['vix_low']
            if 'vix_volume' in df.columns:
                df['volume'] = df['vix_volume']
        
        if 'date' not in df.columns:
            print(f"⚠️ {ticker} 데이터에 날짜 컬럼이 없습니다.")
            return None

        df['date'] = pd.to_datetime(df['date'], utc=True)
        df = df.sort_values('date')
        if len(df) < days:
            print(f"⚠️ {ticker} 데이터가 충분하지 않습니다. (필요: {days}, 실제: {len(df)})")
            return None

        df = df.iloc[-days:].copy()
        df['ma50'] = df['close'].rolling(window=50).mean()
        df['ma200'] = df['close'].rolling(window=200).mean()
        return df
    except Exception as e:
        print(f"❌ {ticker} 데이터 로드 오류: {e}")
        return None


def calculate_high_low_index(index_data: Dict[str, pd.DataFrame]) -> float:
    """Calculate High-Low Index from breadth data."""
    file_path = os.path.join(BREADTH_DATA_DIR, "high_low.csv")
    try:
        if not os.path.exists(file_path):
            from data_collectors.market_breadth_collector import MarketBreadthCollector
            collector = MarketBreadthCollector()
            collector.collect_high_low_index(days=200)
        if not os.path.exists(file_path):
            print(f"⚠️ High-Low 데이터 파일을 찾을 수 없습니다: {file_path}")
            return 50

        df = pd.read_csv(file_path)
        df.columns = [c.lower() for c in df.columns]
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], utc=True)
            df = df.sort_values('date')

        high_col = next((c for c in df.columns if 'high' in c), None)
        low_col = next((c for c in df.columns if 'low' in c), None)
        if not high_col or not low_col:
            print("⚠️ High-Low 데이터에 필요한 컬럼이 없습니다.")
            return 50

        highs = float(df[high_col].iloc[-1])
        lows = float(df[low_col].iloc[-1])
        total = highs + lows
        if total == 0:
            return 50
        return max(min(highs / total * 100, 100), 0)
    except Exception as e:
        print(f"❌ High-Low Index 계산 오류: {e}")
        return 50


def calculate_advance_decline_trend(index_data: Dict[str, pd.DataFrame]) -> float:
    """Calculate trend of Advance-Decline Line."""
    file_path = os.path.join(BREADTH_DATA_DIR, "advance_decline.csv")
    try:
        if not os.path.exists(file_path):
            from data_collectors.market_breadth_collector import MarketBreadthCollector
            collector = MarketBreadthCollector()
            collector.collect_advance_decline_data(days=200)
        if not os.path.exists(file_path):
            print(f"⚠️ Advance-Decline 데이터 파일을 찾을 수 없습니다: {file_path}")
            return 0

        df = pd.read_csv(file_path)
        df.columns = [c.lower() for c in df.columns]
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], utc=True)
            df = df.sort_values('date')

        # 정확한 컬럼명으로 확인
        required_columns = ['advancing', 'declining']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"⚠️ Advance-Decline 데이터에 필요한 컬럼이 없습니다: {missing_columns}")
            print(f"📋 현재 컬럼: {list(df.columns)}")
            return 0
            
        adv_col = 'advancing'
        dec_col = 'declining'
        
        # 데이터 타입 변환 (숫자가 아닌 값들을 0으로 처리)
        df[adv_col] = pd.to_numeric(df[adv_col], errors='coerce').fillna(0)
        df[dec_col] = pd.to_numeric(df[dec_col], errors='coerce').fillna(0)

        df['ad_line'] = (df[adv_col] - df[dec_col]).cumsum()
        if len(df) < 50:
            return 0

        short_ma = df['ad_line'].rolling(window=20).mean().iloc[-1]
        long_ma = df['ad_line'].rolling(window=50).mean().iloc[-1]
        if long_ma == 0:
            return 0
        return ((short_ma - long_ma) / abs(long_ma)) * 100
    except Exception as e:
        print(f"❌ Advance-Decline 추세 계산 오류: {e}")
        return 0


# Put/Call Ratio 계산 기능 제거됨


def calculate_ma_distance(df: pd.DataFrame, ma_column: str = "ma200", price_column: str = "close") -> float:
    """Return latest close price distance (%) from given moving average.

    If the moving average column or price column is missing, 0 is returned.
    """
    try:
        if df is None or price_column not in df.columns or ma_column not in df.columns:
            return 0.0

        latest = df.iloc[-1]
        ma_value = latest[ma_column]
        price = latest[price_column]

        if ma_value == 0 or pd.isna(ma_value) or pd.isna(price):
            return 0.0

        return (price - ma_value) / ma_value * 100
    except Exception:
        return 0.0


def count_consecutive_below_ma(df: pd.DataFrame, ma_column: str = "ma50", price_column: str = "close") -> int:
    """Return number of consecutive days the price closed below the given MA.

    The check starts from the most recent date and stops when the price
    closes above the moving average. Missing columns return 0.
    """
    try:
        if df is None or price_column not in df.columns or ma_column not in df.columns:
            return 0

        closes = df[price_column].values[::-1]
        mas = df[ma_column].values[::-1]
        count = 0

        for close, ma in zip(closes, mas):
            if pd.isna(close) or pd.isna(ma):
                break
            if close <= ma:
                count += 1
            else:
                break

        return int(count)
    except Exception:
        return 0
