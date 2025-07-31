#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mark Minervini 고급 수학적 패턴 분석기
VCP와 Cup & Handle 패턴을 학술 논문 기반 수학적 알고리즘으로 감지
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Union
import logging

from .mathematical_functions import (
    kernel_smoothing,
    extract_peaks_troughs,
    calculate_amplitude_contraction,
    quadratic_fit_cup,
    bezier_curve_correlation
)

logger = logging.getLogger(__name__)


class PatternAnalyzer:
    """고급 수학적 패턴 분석기"""
    
    def __init__(self):
        pass
    
    def detect_vcp_pattern(self, prices: np.ndarray, volumes: np.ndarray, symbol: str) -> Tuple[bool, float]:
        """VCP (Volatility Contraction Pattern) 패턴을 학술 논문 기반 수학적 알고리즘으로 감지
        
        Reference: Lo, Mamaysky & Wang (2000), Suh, Li & Gao (2008)
        
        Args:
            prices: 가격 시계열 데이터
            volumes: 거래량 시계열 데이터
            symbol: 주식 심볼
            
        Returns:
            Tuple[bool, float]: (패턴 감지 여부, 신뢰도 점수)
        """
        try:
            if len(prices) < 60:
                return False, 0.0
            
            # 최근 90일 데이터 사용
            recent_prices = prices[-90:] if len(prices) >= 90 else prices
            recent_volumes = volumes[-90:] if len(volumes) >= 90 else volumes
            
            confidence_score = 0.0
            
            # 1. 커널 회귀를 이용한 가격 곡선 스무딩
            smoothed_prices = kernel_smoothing(recent_prices)
            
            # 2. 피크와 골 추출
            peaks, troughs = extract_peaks_troughs(smoothed_prices)
            
            if len(peaks) < 3:
                return False, 0.0
            
            # 3. 연속적 변동성 수축 검출 (세밀한 점수 계산)
            amplitudes = calculate_amplitude_contraction(peaks, troughs, smoothed_prices)
            
            if len(amplitudes) >= 2:
                # 진폭 감소 패턴 확인 (연속성에 따른 가변 점수)
                decreasing_count = 0
                for i in range(len(amplitudes)-1):
                    if amplitudes[i] >= amplitudes[i+1]:
                        decreasing_count += 1
                
                # 연속 감소 비율에 따른 점수 (0.1 ~ 0.35)
                decrease_ratio = decreasing_count / (len(amplitudes) - 1)
                confidence_score += 0.1 + (decrease_ratio * 0.25)
                    
                # 진폭 수축 정도 평가 (수축 비율에 따른 연속 점수)
                if len(amplitudes) >= 3:
                    contraction_ratio = amplitudes[-1] / amplitudes[0] if amplitudes[0] > 0 else 1
                    # 수축 비율에 따른 점수 (0.05 ~ 0.25)
                    if contraction_ratio < 0.3:  # 70% 이상 수축
                        confidence_score += 0.25
                    elif contraction_ratio < 0.5:  # 50% 이상 수축
                        confidence_score += 0.20
                    elif contraction_ratio < 0.7:  # 30% 이상 수축
                        confidence_score += 0.15
                    elif contraction_ratio < 0.85:  # 15% 이상 수축
                        confidence_score += 0.10
                    else:
                        confidence_score += 0.05
            
            # 4. 거래량 패턴 확인 (세밀한 거래량 수축 분석)
            if len(recent_volumes) >= 30:
                # 최근 30일을 3개 구간으로 나누어 거래량 수축 확인
                vol_sections = np.array_split(recent_volumes[-30:], 3)
                vol_means = [np.mean(section) for section in vol_sections]
                
                # 거래량 감소 정도에 따른 점수 계산
                vol_decrease_score = 0.0
                for i in range(len(vol_means)-1):
                    if vol_means[i] > vol_means[i+1]:
                        decrease_ratio = (vol_means[i] - vol_means[i+1]) / vol_means[i]
                        vol_decrease_score += min(decrease_ratio * 0.15, 0.08)  # 최대 0.08씩
                
                confidence_score += min(vol_decrease_score, 0.15)  # 최대 0.15
            
            # 5. 최종 브레이크아웃 확인 (근접도에 따른 세밀한 점수)
            if len(recent_prices) >= 5:
                recent_high = np.max(recent_prices[-20:]) if len(recent_prices) >= 20 else np.max(recent_prices)
                current_price = recent_prices[-1]
                
                # 최근 고점 근처 정도에 따른 점수 (0.02 ~ 0.12)
                if current_price >= recent_high * 0.98:  # 98% 이상
                    confidence_score += 0.12
                elif current_price >= recent_high * 0.95:  # 95% 이상
                    confidence_score += 0.08
                elif current_price >= recent_high * 0.90:  # 90% 이상
                    confidence_score += 0.05
                elif current_price >= recent_high * 0.85:  # 85% 이상
                    confidence_score += 0.02
            
            # VCP 패턴 감지 기준: 신뢰도 0.5 이상
            vcp_detected = confidence_score >= 0.5
            
            return vcp_detected, confidence_score
            
        except Exception as e:
            logger.warning(f"{symbol}: VCP 패턴 감지 중 오류 - {str(e)}")
            return False, 0.0
    
    def detect_cup_handle_pattern(self, prices: np.ndarray, volumes: np.ndarray, symbol: str) -> Tuple[bool, float]:
        """Cup & Handle 패턴을 학술 논문 기반 수학적 알고리즘으로 감지
        
        Reference: Suh, Li & Gao (2008) - 베지어 곡선 이용 컵 앤 핸들 역방향 스크리닝
        
        Args:
            prices: 가격 시계열 데이터
            volumes: 거래량 시계열 데이터
            symbol: 주식 심볼
            
        Returns:
            Tuple[bool, float]: (패턴 감지 여부, 신뢰도 점수)
        """
        try:
            if len(prices) < 40:
                return False, 0.0
            
            # 최근 120일 데이터 사용
            recent_prices = prices[-120:] if len(prices) >= 120 else prices
            recent_volumes = volumes[-120:] if len(volumes) >= 120 else volumes
            
            confidence_score = 0.0
            
            # 1. 커널 회귀를 이용한 가격 곡선 스무딩
            smoothed_prices = kernel_smoothing(recent_prices)
            
            # 2. 피크와 골 추출
            peaks, troughs = extract_peaks_troughs(smoothed_prices)
            
            if len(peaks) < 2 or len(troughs) < 1:
                return False, 0.0
            
            # 3. 컵 패턴 검증 (U자형 구조)
            # 왼쪽 림(A), 저점(B), 오른쪽 림(C) 찾기
            n = len(smoothed_prices)
            
            # A점: 초기 1/3 구간에서 최고점
            A_region_end = n // 3
            A_candidates = peaks[peaks < A_region_end]
            if len(A_candidates) == 0:
                return False, 0.0
            A_idx = A_candidates[np.argmax(smoothed_prices[A_candidates])]
            A_price = smoothed_prices[A_idx]
            
            # B점: 중간 구간에서 최저점
            B_region_start = A_idx + 5
            B_region_end = min(n - 10, A_idx + 60)
            if B_region_end <= B_region_start:
                return False, 0.0
            
            B_candidates = troughs[(troughs >= B_region_start) & (troughs <= B_region_end)]
            if len(B_candidates) == 0:
                B_idx = B_region_start + np.argmin(smoothed_prices[B_region_start:B_region_end])
            else:
                B_idx = B_candidates[np.argmin(smoothed_prices[B_candidates])]
            B_price = smoothed_prices[B_idx]
            
            # C점: 마지막 구간에서 고점
            C_region_start = B_idx + 5
            C_region_end = min(n - 3, B_idx + 40)
            if C_region_end <= C_region_start:
                return False, 0.0
            
            C_candidates = peaks[(peaks >= C_region_start) & (peaks <= C_region_end)]
            if len(C_candidates) == 0:
                C_idx = C_region_start + np.argmax(smoothed_prices[C_region_start:C_region_end])
            else:
                C_idx = C_candidates[np.argmax(smoothed_prices[C_candidates])]
            C_price = smoothed_prices[C_idx]
            
            # 4. 컵 패턴 수학적 검증 (점수 정규화)
            
            # 깊이 조건: 10-50% 하락
            if A_price > 0:
                depth_ratio = (A_price - B_price) / A_price
                if 0.10 <= depth_ratio <= 0.50:
                    confidence_score += 0.25
                elif 0.05 <= depth_ratio <= 0.60:
                    confidence_score += 0.15
            
            # 대칭성 조건: C가 A의 85% 이상
            if A_price > 0 and C_price >= 0.85 * A_price:
                confidence_score += 0.20
                if C_price >= 0.95 * A_price:
                    confidence_score += 0.05  # 추가 점수
            
            # 컵 폭 조건: 7주-65주 (35-325일)
            cup_width = B_idx - A_idx
            if 20 <= cup_width <= 200:  # 데이터 제한으로 조정
                confidence_score += 0.15
            
            # 5. 2차 다항식 근사를 이용한 U자형 검증
            cup_indices = np.arange(A_idx, C_idx + 1)
            cup_prices = smoothed_prices[A_idx:C_idx + 1]
            
            if len(cup_indices) >= 10:
                r_squared, curvature = quadratic_fit_cup(cup_indices, cup_prices)
                if r_squared >= 0.7:  # 높은 적합도
                    confidence_score += 0.10
                elif r_squared >= 0.5:
                    confidence_score += 0.05
            
            # 6. 베지어 곡선 상관계수 검증
            if len(cup_prices) >= 5:
                control_points = np.array([cup_prices[0], np.min(cup_prices), cup_prices[-1]])
                bezier_corr = bezier_curve_correlation(control_points, cup_prices)
                if bezier_corr >= 0.8:
                    confidence_score += 0.08
            
            # 7. 핸들 패턴 검증 (선택적)
            if C_idx < n - 5:
                handle_region = smoothed_prices[C_idx:]
                if len(handle_region) >= 5:
                    D_price = np.min(handle_region)
                    
                    # 핸들 깊이: C의 85% 이상
                    if C_price > 0 and D_price >= 0.85 * C_price:
                        confidence_score += 0.07
                    
                    # 브레이크아웃 신호
                    if len(handle_region) > 2 and handle_region[-1] >= 0.98 * C_price:
                        confidence_score += 0.03
            
            # 8. 거래량 패턴 확인
            if len(recent_volumes) >= len(recent_prices):
                # 컵 형성 중 거래량 감소
                if B_idx > 10 and C_idx < len(recent_volumes) - 5:
                    early_volume = np.mean(recent_volumes[A_idx:A_idx+10])
                    cup_volume = np.mean(recent_volumes[B_idx-5:B_idx+5])
                    
                    if early_volume > 0 and cup_volume < 0.8 * early_volume:
                        confidence_score += 0.07
            
            # 점수 정규화 (최대 1.0으로 제한)
            confidence_score = min(confidence_score, 1.0)
            
            # Cup & Handle 패턴 감지 기준: 신뢰도 0.6 이상
            cup_handle_detected = confidence_score >= 0.6
            
            return cup_handle_detected, confidence_score
            
        except Exception as e:
            logger.warning(f"{symbol}: Cup & Handle 패턴 감지 중 오류 - {str(e)}")
            return False, 0.0
    
    def analyze_patterns_from_data(self, symbol: str, stock_data: pd.DataFrame) -> Dict[str, Dict[str, Union[bool, float]]]:
        """주식 데이터로부터 고급 수학적 패턴 분석 수행
        
        Args:
            symbol: 주식 심볼
            stock_data: 주식 데이터 (OHLCV)
            
        Returns:
            Dict: 패턴 분석 결과
        """
        try:
            if stock_data.empty or len(stock_data) < 40:
                return {
                    'vcp': {'detected': False, 'confidence': 0.0},
                    'cup_handle': {'detected': False, 'confidence': 0.0}
                }
            
            # 가격과 거래량 데이터 추출
            prices = stock_data['Close'].values
            volumes = stock_data['Volume'].values if 'Volume' in stock_data.columns else np.ones(len(prices))
            
            # VCP 패턴 분석
            vcp_detected, vcp_confidence = self.detect_vcp_pattern(prices, volumes, symbol)
            
            # Cup & Handle 패턴 분석
            cup_handle_detected, cup_handle_confidence = self.detect_cup_handle_pattern(prices, volumes, symbol)
            
            return {
                'vcp': {'detected': vcp_detected, 'confidence': round(vcp_confidence, 3)},
                'cup_handle': {'detected': cup_handle_detected, 'confidence': round(cup_handle_confidence, 3)}
            }
            
        except Exception as e:
            logger.error(f"{symbol}: 패턴 분석 오류: {e}")
            return {
                'vcp': {'detected': False, 'confidence': 0.0},
                'cup_handle': {'detected': False, 'confidence': 0.0}
            }