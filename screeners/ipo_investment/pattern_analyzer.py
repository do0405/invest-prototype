# -*- coding: utf-8 -*-
"""IPO 패턴 분석기 - README.md 기반 구현"""

import pandas as pd
import numpy as np
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def calculate_rsi(prices, period=14):
    """RSI 계산 함수"""
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1]

class IPOPatternAnalyzer:
    """IPO 패턴 분석기 클래스"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def check_ipo_base_pattern(self, df, ipo_price):
        """베이스 패턴 확인 - README.md 기반
        
        베이스 형성 조건:
        - 시간: 상장 후 5~25일 내
        - 하락폭: 현재가 < IPO 첫날 종가 × 0.70 (30% 하락)
        - 횡보 범위: (구간최고가 - 구간최저가) ÷ 구간평균 < 0.20
        - 거래량 감소: 베이스 기간 평균거래량 < 전체 기간 평균거래량
        """
        try:
            if len(df) < 25:
                return False, {}
            
            current_price = df['close'].iloc[-1]
            ipo_first_close = ipo_price  # IPO 첫날 종가로 가정
            
            # 30% 하락 확인
            decline_check = current_price < ipo_first_close * 0.70
            
            # 횡보 범위 확인 (최근 5~25일 구간)
            period_high = df['high'].iloc[-25:].max()
            period_low = df['low'].iloc[-25:].min()
            period_avg = df['close'].iloc[-25:].mean()
            range_check = (period_high - period_low) / period_avg < 0.20
            
            # 거래량 감소 확인
            base_period_volume = df['volume'].iloc[-25:].mean()
            total_period_volume = df['volume'].mean()
            volume_decrease = base_period_volume < total_period_volume
            
            base_pattern = decline_check and range_check and volume_decrease
            
            base_info = {
                'current_price': current_price,
                'ipo_first_close': ipo_first_close,
                'decline_check': decline_check,
                'range_check': range_check,
                'volume_decrease': volume_decrease,
                'period_range_ratio': (period_high - period_low) / period_avg,
                'pattern_formation_date': df.index[-1].strftime('%Y-%m-%d') if hasattr(df.index[-1], 'strftime') else str(df.index[-1])
            }
            
            return base_pattern, base_info
            
        except Exception as e:
            self.logger.error(f"베이스 패턴 분석 중 오류: {e}")
            return False, {}
    
    def check_ipo_breakout(self, df):
        """브레이크아웃 패턴 확인 - README.md 기반
        
        브레이크아웃 조건:
        - 돌파: 현재가 > 구간 최고가 × 1.025 (2.5% 이상)
        - 거래량: 당일거래량 > 10일 평균거래량 × 2.0
        - 종가 확인: 종가 > 돌파수준 × 0.975 (돌파수준 -2.5% 이상)
        - RSI: 50 < RSI < 85
        """
        try:
            if len(df) < 25:
                return False, {}
            
            current_price = df['close'].iloc[-1]
            current_volume = df['volume'].iloc[-1]
            avg_10_volume = df['volume'].iloc[-10:].mean()
            
            # 구간 최고가 (최근 25일)
            base_high = df['high'].iloc[-25:].max()
            
            # 2.5% 돌파 확인
            breakout_level = base_high * 1.025
            breakout_check = current_price > breakout_level
            
            # 거래량 2배 이상 증가 확인
            volume_check = current_volume > avg_10_volume * 2.0
            
            # 종가가 돌파 수준 근처에서 마감 확인
            close_check = current_price > breakout_level * 0.975
            
            # RSI 확인
            rsi = calculate_rsi(df['close'], 14)
            rsi_check = 50 < rsi < 85
            
            breakout_pattern = breakout_check and volume_check and close_check and rsi_check
            
            breakout_info = {
                'current_price': current_price,
                'base_high': base_high,
                'breakout_level': breakout_level,
                'breakout_check': breakout_check,
                'volume_check': volume_check,
                'close_check': close_check,
                'rsi_check': rsi_check,
                'current_rsi': rsi,
                'pattern_formation_date': df.index[-1].strftime('%Y-%m-%d') if hasattr(df.index[-1], 'strftime') else str(df.index[-1])
            }
            
            return breakout_pattern, breakout_info
            
        except Exception as e:
            self.logger.error(f"브레이크아웃 패턴 분석 중 오류: {e}")
            return False, {}
    
    def check_track1_pattern(self, df, ipo_price):
        """Track 1 패턴 확인 - README.md 기반
        
        Track 1 조건:
        - 하락폭: 현재가 < IPO 발행가 × 0.50 (50% 이상 하락)
        - 거래량 증가: 당일거래량 > 5일 평균 × 1.8
        - RSI 반전: RSI가 30 이하에서 35 이상으로 상승
        - 지지 확인: 피보나치 61.8% 또는 50% 수준에서 지지
        """
        try:
            if len(df) < 15:
                return False, {}
            
            current_price = df['close'].iloc[-1]
            current_volume = df['volume'].iloc[-1]
            avg_5_volume = df['volume'].iloc[-5:].mean()
            
            # 50% 이상 하락 확인
            decline_check = current_price < ipo_price * 0.50
            
            # 거래량 증가 확인
            volume_check = current_volume > avg_5_volume * 1.8
            
            # RSI 반전 확인
            rsi = calculate_rsi(df['close'], 14)
            prev_rsi = calculate_rsi(df['close'].iloc[:-1], 14)
            rsi_check = prev_rsi <= 30 and rsi >= 35
            
            track1_pattern = decline_check and volume_check and rsi_check
            
            track1_info = {
                'current_price': current_price,
                'ipo_price': ipo_price,
                'decline_check': decline_check,
                'volume_check': volume_check,
                'rsi_check': rsi_check,
                'current_rsi': rsi,
                'prev_rsi': prev_rsi,
                'pattern_formation_date': df.index[-1].strftime('%Y-%m-%d') if hasattr(df.index[-1], 'strftime') else str(df.index[-1])
            }
            
            return track1_pattern, track1_info
            
        except Exception as e:
            self.logger.error(f"Track 1 패턴 분석 중 오류: {e}")
            return False, {}
    
    def check_track2_pattern(self, df, ipo_price):
        """Track 2 패턴 확인 - README.md 기반
        
        강한 모멘텀 조건:
        - 상승폭: 현재가 > IPO 발행가 × 1.50 (50% 이상 상승)
        - 승률: 최근 10일 중 7일 이상 상승
        - 평균 상승: 최근 5일 평균 일일수익률 > 2%
        - 이동평균: 10일 MA > 21일 MA > 50일 MA
        - 거래량: 최근 5일 평균 > 전체기간 평균 × 1.3
        - RSI: 60 < RSI < 85
        """
        try:
            if len(df) < 50:
                return False, {}
            
            current_price = df['close'].iloc[-1]
            
            # 50% 이상 상승 확인
            if current_price > ipo_price * 1.50:
                recent_10_days = df.iloc[-10:]
                returns = recent_10_days['close'].pct_change().dropna()
                up_days = sum(returns > 0)
                
                # 10일 중 7일 이상 상승 확인
                if up_days >= 7:
                    # 최근 5일 평균 수익률 2% 이상 확인
                    recent_5_returns = returns.iloc[-5:]
                    avg_return = recent_5_returns.mean()
                    
                    if avg_return > 0.02:
                        # 이동평균 정렬 확인
                        ma_10 = df['close'].iloc[-10:].mean()
                        ma_21 = df['close'].iloc[-21:].mean()
                        ma_50 = df['close'].iloc[-50:].mean()
                        
                        if ma_10 > ma_21 > ma_50:
                            # 거래량 확인
                            recent_5_volume = df['volume'].iloc[-5:].mean()
                            total_volume = df['volume'].mean()
                            
                            if recent_5_volume > total_volume * 1.3:
                                # RSI 확인
                                rsi = calculate_rsi(df['close'], 14)
                                if 60 < rsi < 85:
                                    track2_info = {
                                        'current_price': current_price,
                                        'ipo_price': ipo_price,
                                        'up_days': up_days,
                                        'avg_return': avg_return,
                                        'ma_10': ma_10,
                                        'ma_21': ma_21,
                                        'ma_50': ma_50,
                                        'recent_5_volume': recent_5_volume,
                                        'total_volume': total_volume,
                                        'current_rsi': rsi,
                                        'pattern_formation_date': df.index[-1].strftime('%Y-%m-%d') if hasattr(df.index[-1], 'strftime') else str(df.index[-1])
                                    }
                                    return True, track2_info
            
            return False, {}
            
        except Exception as e:
            self.logger.error(f"Track 2 패턴 분석 중 오류: {e}")
            return False, {}