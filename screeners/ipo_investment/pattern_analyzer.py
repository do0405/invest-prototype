# -*- coding: utf-8 -*-
"""IPO 패턴 분석 모듈"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple
from .indicators import calculate_base_pattern, calculate_track2_indicators


class IPOPatternAnalyzer:
    """IPO 패턴 분석 클래스"""
    
    def check_ipo_base_pattern(self, df: pd.DataFrame, min_days: int = 30, max_days: int = 200) -> Tuple[bool, Dict]:
        """IPO 베이스 패턴 확인 및 패턴 형성 시점 추적"""
        # 데이터가 충분하지 않은 경우
        if len(df) < min_days:
            return False, {}
            
        # 너무 오래된 IPO인 경우
        if len(df) > max_days:
            return False, {}
        
        # 기술적 지표 계산
        df = calculate_base_pattern(df)
        
        # 베이스 패턴 형성 시점 찾기 (최근 20일 내에서)
        pattern_formation_date = None
        best_base_score = 0
        best_base_info = {}
        
        # 최근 20일 내에서 베이스 패턴 형성 시점 탐색
        search_days = min(20, len(df))
        for i in range(search_days):
            current_idx = -(search_days - i)
            
            # 해당 시점에서 충분한 데이터가 있는지 확인
            if current_idx + len(df) < min_days:
                continue
                
            current_data = df.iloc[current_idx]
            
            # 해당 시점까지의 데이터로 베이스 패턴 조건 확인
            end_idx = current_idx + len(df) if current_idx < 0 else current_idx + 1
            analysis_df = df.iloc[:end_idx]
            
            if len(analysis_df) < 20:
                continue
            
            # 베이스 패턴 조건 확인
            # 1. 최근 20일 동안 가격 변동이 15% 이내 (베이스 형성)
            recent_20_data = analysis_df.iloc[-20:]
            recent_range = recent_20_data['price_range'].max()
            base_formed = recent_range <= 15
            
            # 2. 거래량 감소 (베이스 형성 중 거래량 감소)
            if len(analysis_df) >= 40:
                volume_declining = recent_20_data['volume'].mean() < analysis_df.iloc[-40:-20]['volume'].mean()
            else:
                volume_declining = True  # 데이터 부족 시 통과
            
            # 3. 종가가 20일 이동평균선 위에 있음
            above_20_sma = current_data['close'] > current_data['sma_20']
            
            # 4. RSI가 과매도 구간에서 벗어남 (RSI > 40)
            healthy_rsi = current_data['rsi_14'] > 40
            
            # 5. 최근 가격이 IPO 이후 고점의 70% 이상
            ipo_high = analysis_df['high'].max()
            price_strength = current_data['close'] >= ipo_high * 0.7
            
            # 베이스 패턴 점수 계산
            base_score = sum([base_formed, volume_declining, above_20_sma, healthy_rsi, price_strength])
            
            # 더 높은 점수의 패턴을 찾으면 업데이트
            if base_score >= 3 and base_score > best_base_score:
                best_base_score = base_score
                
                # 패턴 형성 날짜 추출
                if hasattr(current_data, 'name') and hasattr(current_data.name, 'strftime'):
                    pattern_formation_date = current_data.name.strftime('%Y-%m-%d')
                elif 'date' in df.columns:
                    date_val = current_data.get('date')
                    if pd.notna(date_val):
                        if isinstance(date_val, str):
                            pattern_formation_date = date_val
                        else:
                            pattern_formation_date = pd.to_datetime(date_val).strftime('%Y-%m-%d')
                
                best_base_info = {
                    'base_score': base_score,
                    'base_formed': base_formed,
                    'volume_declining': volume_declining,
                    'above_20_sma': above_20_sma,
                    'healthy_rsi': healthy_rsi,
                    'price_strength': price_strength,
                    'recent_range': recent_range,
                    'ipo_high': ipo_high,
                    'current_price': current_data['close'],
                    'price_to_high_ratio': current_data['close'] / ipo_high,
                    'pattern_formation_date': pattern_formation_date
                }
        
        # 패턴이 발견되지 않았으면 최신 데이터로 분석
        if best_base_score == 0:
            recent = df.iloc[-1]
            
            # 베이스 패턴 조건 확인
            recent_range = df.iloc[-20:]['price_range'].max()
            base_formed = recent_range <= 15
            
            volume_declining = df.iloc[-20:]['volume'].mean() < df.iloc[-40:-20]['volume'].mean() if len(df) >= 40 else True
            above_20_sma = recent['close'] > recent['sma_20']
            healthy_rsi = recent['rsi_14'] > 40
            
            ipo_high = df['high'].max()
            price_strength = recent['close'] >= ipo_high * 0.7
            
            base_score = sum([base_formed, volume_declining, above_20_sma, healthy_rsi, price_strength])
            
            # 최신 데이터의 날짜 사용
            if hasattr(recent, 'name') and hasattr(recent.name, 'strftime'):
                pattern_formation_date = recent.name.strftime('%Y-%m-%d')
            elif 'date' in df.columns:
                date_val = recent.get('date')
                if pd.notna(date_val):
                    if isinstance(date_val, str):
                        pattern_formation_date = date_val
                    else:
                        pattern_formation_date = pd.to_datetime(date_val).strftime('%Y-%m-%d')
            
            best_base_info = {
                'base_score': base_score,
                'base_formed': base_formed,
                'volume_declining': volume_declining,
                'above_20_sma': above_20_sma,
                'healthy_rsi': healthy_rsi,
                'price_strength': price_strength,
                'recent_range': recent_range,
                'ipo_high': ipo_high,
                'current_price': recent['close'],
                'price_to_high_ratio': recent['close'] / ipo_high,
                'pattern_formation_date': pattern_formation_date
            }
            
            best_base_score = base_score
        
        # 베이스 패턴 확인 (점수가 3점 이상이면 베이스 패턴으로 간주)
        return best_base_score >= 3, best_base_info
    
    def check_ipo_breakout(self, df: pd.DataFrame) -> Tuple[bool, Dict]:
        """IPO 브레이크아웃 확인 및 패턴 형성 시점 추적"""
        # 데이터가 충분하지 않은 경우
        if len(df) < 30:
            return False, {}
        
        # 기술적 지표 계산
        df = calculate_base_pattern(df)
        
        # 브레이크아웃 패턴 형성 시점 찾기 (최근 10일 내에서)
        pattern_formation_date = None
        best_breakout_score = 0
        best_breakout_info = {}
        
        # 최근 10일 내에서 브레이크아웃 패턴 형성 시점 탐색
        search_days = min(10, len(df))
        for i in range(search_days):
            current_idx = -(search_days - i)
            
            # 해당 시점에서 충분한 데이터가 있는지 확인
            if current_idx + len(df) < 30:
                continue
                
            current_data = df.iloc[current_idx]
            
            # 이전 데이터 (당일 상승률 계산용)
            if current_idx == -len(df):
                continue  # 첫 번째 데이터는 이전 데이터가 없으므로 건너뜀
            prev_data = df.iloc[current_idx - 1]
            
            # 해당 시점까지의 데이터로 브레이크아웃 조건 확인
            end_idx = current_idx + len(df) if current_idx < 0 else current_idx + 1
            analysis_df = df.iloc[:end_idx]
            
            if len(analysis_df) < 21:
                continue
            
            # 브레이크아웃 조건 확인
            # 1. 종가가 20일 고점 돌파
            breakout_20d_high = current_data['close'] > analysis_df.iloc[-21:-1]['high'].max()
            
            # 2. 거래량 급증 (20일 평균 대비 2배 이상)
            volume_surge = current_data['volume_ratio'] >= 2.0
            
            # 3. 종가가 상단 밴드 돌파
            breakout_upper_band = current_data['close'] > current_data['upper_band']
            
            # 4. 당일 가격 상승률 2% 이상
            daily_gain = (current_data['close'] / prev_data['close'] - 1) * 100 >= 2
            
            # 5. RSI가 50 이상 (상승 모멘텀)
            strong_rsi = current_data['rsi_14'] >= 50
            
            # 브레이크아웃 점수 계산
            breakout_score = sum([breakout_20d_high, volume_surge, breakout_upper_band, daily_gain, strong_rsi])
            
            # 더 높은 점수의 패턴을 찾으면 업데이트
            if breakout_score >= 3 and breakout_score > best_breakout_score:
                best_breakout_score = breakout_score
                
                # 패턴 형성 날짜 추출
                if hasattr(current_data, 'name') and hasattr(current_data.name, 'strftime'):
                    pattern_formation_date = current_data.name.strftime('%Y-%m-%d')
                elif 'date' in df.columns:
                    date_val = current_data.get('date')
                    if pd.notna(date_val):
                        if isinstance(date_val, str):
                            pattern_formation_date = date_val
                        else:
                            pattern_formation_date = pd.to_datetime(date_val).strftime('%Y-%m-%d')
                
                best_breakout_info = {
                    'breakout_score': breakout_score,
                    'breakout_20d_high': breakout_20d_high,
                    'volume_surge': volume_surge,
                    'breakout_upper_band': breakout_upper_band,
                    'daily_gain': daily_gain,
                    'strong_rsi': strong_rsi,
                    'current_price': current_data['close'],
                    'volume_ratio': current_data['volume_ratio'],
                    'rsi': current_data['rsi_14'],
                    'pattern_formation_date': pattern_formation_date
                }
        
        # 패턴이 발견되지 않았으면 최신 데이터로 분석
        if best_breakout_score == 0:
            recent = df.iloc[-1]
            prev = df.iloc[-2]
            
            # 브레이크아웃 조건 확인
            breakout_20d_high = recent['close'] > df.iloc[-21:-1]['high'].max()
            volume_surge = recent['volume_ratio'] >= 2.0
            breakout_upper_band = recent['close'] > recent['upper_band']
            daily_gain = (recent['close'] / prev['close'] - 1) * 100 >= 2
            strong_rsi = recent['rsi_14'] >= 50
            
            breakout_score = sum([breakout_20d_high, volume_surge, breakout_upper_band, daily_gain, strong_rsi])
            
            # 최신 데이터의 날짜 사용
            if hasattr(recent, 'name') and hasattr(recent.name, 'strftime'):
                pattern_formation_date = recent.name.strftime('%Y-%m-%d')
            elif 'date' in df.columns:
                date_val = recent.get('date')
                if pd.notna(date_val):
                    if isinstance(date_val, str):
                        pattern_formation_date = date_val
                    else:
                        pattern_formation_date = pd.to_datetime(date_val).strftime('%Y-%m-%d')
            
            best_breakout_info = {
                'breakout_score': breakout_score,
                'breakout_20d_high': breakout_20d_high,
                'volume_surge': volume_surge,
                'breakout_upper_band': breakout_upper_band,
                'daily_gain': daily_gain,
                'strong_rsi': strong_rsi,
                'current_price': recent['close'],
                'volume_ratio': recent['volume_ratio'],
                'rsi': recent['rsi_14'],
                'pattern_formation_date': pattern_formation_date
            }
            
            best_breakout_score = breakout_score
        
        # 브레이크아웃 확인 (점수가 3점 이상이면 브레이크아웃으로 간주)
        return best_breakout_score >= 3, best_breakout_info