# -*- coding: utf-8 -*-
"""IPO Track 분석기 - README.md 기반 구현"""

import pandas as pd
import numpy as np
from datetime import datetime
import logging
from utils.calc_utils import calculate_rsi

logger = logging.getLogger(__name__)

class IPOTrackAnalyzer:
    """IPO Track 분석기 클래스"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def check_track1(self, df, ipo_price):
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
            df_with_rsi = calculate_rsi(df, 14)
            rsi = df_with_rsi['rsi_14'].iloc[-1]
            prev_df_with_rsi = calculate_rsi(df.iloc[:-1], 14)
            prev_rsi = prev_df_with_rsi['rsi_14'].iloc[-1]
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
                'volume_ratio': current_volume / avg_5_volume if avg_5_volume > 0 else 0,
                'pattern_formation_date': df.index[-1].strftime('%Y-%m-%d') if hasattr(df.index[-1], 'strftime') else str(df.index[-1])
            }
            
            return track1_pattern, track1_info
            
        except Exception as e:
            self.logger.error(f"Track 1 패턴 분석 중 오류: {e}")
            return False, {}
    
    def check_track2(self, df, ipo_price):
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
                                df_with_rsi = calculate_rsi(df, 14)
                                rsi = df_with_rsi['rsi_14'].iloc[-1]
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
                                        'volume_ratio': recent_5_volume / total_volume if total_volume > 0 else 0,
                                        'price_vs_ipo': (current_price / ipo_price - 1) * 100,
                                        'pattern_formation_date': df.index[-1].strftime('%Y-%m-%d') if hasattr(df.index[-1], 'strftime') else str(df.index[-1])
                                    }
                                    return True, track2_info
            
            return False, {}
            
        except Exception as e:
            self.logger.error(f"Track 2 패턴 분석 중 오류: {e}")
            return False, {}