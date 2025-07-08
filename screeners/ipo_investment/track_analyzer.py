# -*- coding: utf-8 -*-
"""IPO Track 분석 모듈"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple
from .indicators import calculate_base_pattern, calculate_track2_indicators


class IPOTrackAnalyzer:
    """IPO Track 분석 클래스"""
    
    def __init__(self, ipo_data: pd.DataFrame, vix: float, sector_rs: Dict):
        """초기화"""
        self.ipo_data = ipo_data
        self.vix = vix
        self.sector_rs = sector_rs
    
    def check_track1(self, ticker: str, df: pd.DataFrame) -> Tuple[bool, Dict]:
        """Track 1 조건 확인"""
        ipo_row = self.ipo_data[self.ipo_data['ticker'] == ticker]
        if ipo_row.empty:
            return False, {}

        ipo_info = ipo_row.iloc[0]
        df = calculate_base_pattern(df)
        recent = df.iloc[-1]

        price_cond = ipo_info['ipo_price'] * 0.7 <= recent['close'] <= ipo_info['ipo_price'] * 0.9
        rsi_cond = recent['rsi_14'] < 30
        support_touch = recent['close'] <= recent['rolling_low'] * 1.02
        volume_cond = recent['volume'] < recent['volume_sma_20'] * 0.5

        sector_rs = self.sector_rs.get(ipo_info['sector'], {}).get('percentile', 0)
        environment_cond = self.vix < 25 and sector_rs >= 50

        fundamental_cond = (
            ipo_info.get('ps_ratio', np.inf) < ipo_info.get('industry_ps_ratio', np.inf) and
            ipo_info.get('revenue_growth', 0) > 20 and
            ipo_info.get('equity_ratio', 0) > 30 and
            ipo_info.get('cash_to_sales', 0) > 15
        )

        info = {
            'price_cond': price_cond,
            'rsi_cond': rsi_cond,
            'support_touch': support_touch,
            'volume_cond': volume_cond,
            'environment_cond': environment_cond,
            'fundamental_cond': fundamental_cond,
            'sector_rs': sector_rs,
            'vix': self.vix,
            'current_price': recent['close'],
            'ipo_price': ipo_info['ipo_price']
        }

        return all([price_cond, rsi_cond and support_touch, volume_cond, environment_cond, fundamental_cond]), info

    def check_track2(self, ticker: str, df: pd.DataFrame) -> Tuple[bool, Dict]:
        """Track 2 조건 확인"""
        ipo_row = self.ipo_data[self.ipo_data['ticker'] == ticker]
        if ipo_row.empty:
            return False, {}

        ipo_info = ipo_row.iloc[0]
        df = calculate_track2_indicators(df)
        recent = df.iloc[-1]

        price_momentum = len(df) >= 5 and (df['close'].iloc[4] / df['close'].iloc[0] - 1) >= 0.20
        macd_signal = recent['macd'] > recent['macd_signal']
        volume_surge = recent['volume'] >= recent['volume_sma_20'] * 3
        institutional_buy = False

        ema_break = df.iloc[-2]['close'] > df.iloc[-2]['ema_5'] and recent['close'] > recent['ema_5']
        rsi_strong = recent['rsi_7'] > 70
        stoch_cond = recent['stoch_k'] > recent['stoch_d'] and recent['stoch_k'] > 80
        roc_cond = recent['roc_5'] > 15

        sector_rs = self.sector_rs.get(ipo_info['sector'], {}).get('percentile', 0)
        environment_cond = self.vix < 25 and sector_rs >= 50

        info = {
            'price_momentum': price_momentum,
            'macd_signal': macd_signal,
            'volume_surge': volume_surge,
            'ema_break': ema_break,
            'rsi_strong': rsi_strong,
            'stoch_cond': stoch_cond,
            'roc_cond': roc_cond,
            'environment_cond': environment_cond,
            'sector_rs': sector_rs,
            'vix': self.vix,
            'current_price': recent['close']
        }

        return all([price_momentum, macd_signal, volume_surge,
                    ema_break, rsi_strong, stoch_cond, roc_cond, environment_cond]), info