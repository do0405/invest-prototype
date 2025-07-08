# -*- coding: utf-8 -*-
"""IPO 패턴 분석 모듈"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple
from .indicators import calculate_base_pattern, calculate_track2_indicators


class IPOPatternAnalyzer:
    """IPO 패턴 분석 클래스"""
    
    def check_ipo_base_pattern(self, df: pd.DataFrame, min_days: int = 30, max_days: int = 200) -> Tuple[bool, Dict]:
        """IPO 베이스 패턴 확인"""
        # 데이터가 충분하지 않은 경우
        if len(df) < min_days:
            return False, {}
            
        # 너무 오래된 IPO인 경우
        if len(df) > max_days:
            return False, {}
        
        # 기술적 지표 계산
        df = calculate_base_pattern(df)
        
        # 최근 데이터
        recent = df.iloc[-1]
        
        # 베이스 패턴 조건 확인
        # 1. 최근 20일 동안 가격 변동이 15% 이내 (베이스 형성)
        recent_range = df.iloc[-20:]['price_range'].max()
        base_formed = recent_range <= 15
        
        # 2. 거래량 감소 (베이스 형성 중 거래량 감소)
        volume_declining = df.iloc[-20:]['volume'].mean() < df.iloc[-40:-20]['volume'].mean()
        
        # 3. 종가가 20일 이동평균선 위에 있음
        above_20_sma = recent['close'] > recent['sma_20']
        
        # 4. RSI가 과매도 구간에서 벗어남 (RSI > 40)
        healthy_rsi = recent['rsi_14'] > 40
        
        # 5. 최근 가격이 IPO 이후 고점의 70% 이상
        ipo_high = df['high'].max()
        price_strength = recent['close'] >= ipo_high * 0.7
        
        # 베이스 패턴 점수 계산
        base_score = sum([base_formed, volume_declining, above_20_sma, healthy_rsi, price_strength])
        
        # 베이스 패턴 정보
        base_info = {
            'base_score': base_score,
            'base_formed': base_formed,
            'volume_declining': volume_declining,
            'above_20_sma': above_20_sma,
            'healthy_rsi': healthy_rsi,
            'price_strength': price_strength,
            'recent_range': recent_range,
            'ipo_high': ipo_high,
            'current_price': recent['close'],
            'price_to_high_ratio': recent['close'] / ipo_high
        }
        
        # 베이스 패턴 확인 (점수가 3점 이상이면 베이스 패턴으로 간주)
        return base_score >= 3, base_info
    
    def check_ipo_breakout(self, df: pd.DataFrame) -> Tuple[bool, Dict]:
        """IPO 브레이크아웃 확인"""
        # 데이터가 충분하지 않은 경우
        if len(df) < 30:
            return False, {}
        
        # 기술적 지표 계산
        df = calculate_base_pattern(df)
        
        # 최근 데이터
        recent = df.iloc[-1]
        prev = df.iloc[-2]
        
        # 브레이크아웃 조건 확인
        # 1. 종가가 20일 고점 돌파
        breakout_20d_high = recent['close'] > df.iloc[-21:-1]['high'].max()
        
        # 2. 거래량 급증 (20일 평균 대비 2배 이상)
        volume_surge = recent['volume_ratio'] >= 2.0
        
        # 3. 종가가 상단 밴드 돌파
        breakout_upper_band = recent['close'] > recent['upper_band']
        
        # 4. 당일 가격 상승률 2% 이상
        daily_gain = (recent['close'] / prev['close'] - 1) * 100 >= 2
        
        # 5. RSI가 50 이상 (상승 모멘텀)
        strong_rsi = recent['rsi_14'] >= 50
        
        # 브레이크아웃 점수 계산
        breakout_score = sum([breakout_20d_high, volume_surge, breakout_upper_band, daily_gain, strong_rsi])
        
        # 브레이크아웃 정보
        breakout_info = {
            'breakout_score': breakout_score,
            'breakout_20d_high': breakout_20d_high,
            'volume_surge': volume_surge,
            'breakout_upper_band': breakout_upper_band,
            'daily_gain': daily_gain,
            'strong_rsi': strong_rsi,
            'current_price': recent['close'],
            'volume_ratio': recent['volume_ratio'],
            'rsi': recent['rsi_14']
        }
        
        # 브레이크아웃 확인 (점수가 3점 이상이면 브레이크아웃으로 간주)
        return breakout_score >= 3, breakout_info