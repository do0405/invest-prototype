#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mark Minervini 향상된 다차원 패턴 분석기
VCP와 Cup & Handle 패턴을 다차원 평가 시스템과 정규화된 신뢰도로 감지
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Union
import logging
from dataclasses import dataclass

from .mathematical_functions import (
    kernel_smoothing,
    extract_peaks_troughs,
    calculate_amplitude_contraction,
    quadratic_fit_cup,
    bezier_curve_correlation
)

logger = logging.getLogger(__name__)


@dataclass
class DimensionalScores:
    """다차원 평가 점수 구조체"""
    technical_quality: float = 0.0
    volume_confirmation: float = 0.0
    temporal_validity: float = 0.0
    market_context: float = 0.0
    
    def to_dict(self) -> Dict[str, float]:
        return {
            'technical_quality': float(self.technical_quality),
            'volume_confirmation': float(self.volume_confirmation),
            'temporal_validity': float(self.temporal_validity),
            'market_context': float(self.market_context)
        }


class EnhancedPatternAnalyzer:
    """향상된 다차원 패턴 분석기"""
    
    def __init__(self):
        self.DETECTION_THRESHOLD = 0.6
        self.DIMENSION_WEIGHTS = {
            'technical_quality': 0.35,
            'volume_confirmation': 0.25,
            'temporal_validity': 0.25,
            'market_context': 0.15
        }
        
        # 통일된 임계값
        self.HIGH_CONFIDENCE_THRESHOLD = 0.8  # 고신뢰도 기준
    
    def _normalize_score(self, score: float, max_score: float = 1.0) -> float:
        """점수를 0-1 범위로 정규화"""
        if max_score <= 0 or np.isnan(max_score) or np.isinf(max_score):
            return 0.0
        
        if np.isnan(score) or np.isinf(score):
            return 0.0
        
        normalized = score / max_score
        
        if np.isnan(normalized) or np.isinf(normalized):
            return 0.0
        
        return min(max(normalized, 0.0), 1.0)
    
    def _calculate_technical_quality_vcp(self, prices: np.ndarray, peaks: np.ndarray, 
                                        troughs: np.ndarray, amplitudes: List[float]) -> float:
        """VCP 기술적 품질 평가 (0-1 정규화)"""
        score = 0.0
        
        # 1. 수축 패턴 품질 (0.4)
        if len(amplitudes) >= 2:
            decreasing_count = sum(1 for i in range(len(amplitudes)-1) 
                                 if amplitudes[i] >= amplitudes[i+1])
            decrease_ratio = decreasing_count / (len(amplitudes) - 1)
            score += decrease_ratio * 0.4
        
        # 2. 수축 강도 (0.4)
        if len(amplitudes) >= 3:
            contraction_ratio = amplitudes[-1] / amplitudes[0] if amplitudes[0] > 0 else 1
            if contraction_ratio < 0.3:      # 70% 이상 수축
                score += 0.4
            elif contraction_ratio < 0.5:    # 50% 이상 수축
                score += 0.3
            elif contraction_ratio < 0.7:    # 30% 이상 수축
                score += 0.2
            elif contraction_ratio < 0.85:   # 15% 이상 수축
                score += 0.1
        
        # 3. 패턴 일관성 (0.2)
        if len(peaks) >= 3:
            peak_consistency = 1.0 - (np.std(prices[peaks]) / np.mean(prices[peaks]))
            score += max(peak_consistency, 0) * 0.2
        
        return self._normalize_score(score)
    
    def _calculate_volume_confirmation_vcp(self, volumes: np.ndarray) -> float:
        """VCP 거래량 확인 평가 (0-1 정규화)"""
        score = 0.0
        
        if len(volumes) >= 30:
            # 1. 거래량 수축 패턴 (0.6)
            vol_sections = np.array_split(volumes[-30:], 3)
            vol_means = [np.mean(section) for section in vol_sections]
            
            vol_decrease_score = 0.0
            for i in range(len(vol_means)-1):
                if vol_means[i] > vol_means[i+1]:
                    decrease_ratio = (vol_means[i] - vol_means[i+1]) / vol_means[i]
                    vol_decrease_score += decrease_ratio
            
            score += min(vol_decrease_score / 2, 0.6)  # 정규화
            
            # 2. 거래량 변동성 감소 (0.4)
            early_vol_std = np.std(volumes[-30:-20]) if len(volumes) >= 30 else 0
            late_vol_std = np.std(volumes[-10:]) if len(volumes) >= 10 else 0
            
            if early_vol_std > 0:
                volatility_reduction = 1 - (late_vol_std / early_vol_std)
                score += max(volatility_reduction, 0) * 0.4
        
        return self._normalize_score(score)
    
    def _calculate_temporal_validity_vcp(self, prices: np.ndarray, peaks: np.ndarray) -> float:
        """VCP 시간적 유효성 평가 (0-1 정규화)"""
        score = 0.0
        
        # 1. 형성 기간 적절성 (0.5)
        data_length = len(prices)
        if 60 <= data_length <= 120:  # 이상적 기간
            score += 0.5
        elif 45 <= data_length < 60 or 120 < data_length <= 150:
            score += 0.3
        elif data_length >= 30:
            score += 0.1
        
        # 2. 패턴 진행 속도 (0.3)
        if len(peaks) >= 2:
            avg_peak_interval = np.mean(np.diff(peaks))
            if 5 <= avg_peak_interval <= 20:  # 적절한 간격
                score += 0.3
            elif avg_peak_interval <= 30:
                score += 0.2
        
        # 3. 최근성 (0.2)
        if len(peaks) > 0:
            last_peak_recency = (len(prices) - peaks[-1]) / len(prices)
            if last_peak_recency <= 0.1:  # 최근 10% 내
                score += 0.2
            elif last_peak_recency <= 0.2:
                score += 0.1
        
        return self._normalize_score(score)
    
    def _calculate_market_context(self, prices: np.ndarray) -> float:
        """시장 맥락 평가 (0-1 정규화)"""
        score = 0.0
        
        # 1. 상대적 강도 (0.5)
        if len(prices) >= 20:
            recent_performance = (prices[-1] - prices[-20]) / prices[-20]
            if recent_performance > 0.05:  # 5% 이상 상승
                score += 0.5
            elif recent_performance > 0:
                score += 0.3
            elif recent_performance > -0.05:
                score += 0.1
        
        # 2. 가격 위치 (0.3)
        if len(prices) >= 50:
            price_percentile = np.percentile(prices[-50:], 90)
            if prices[-1] >= price_percentile:
                score += 0.3
            elif prices[-1] >= np.percentile(prices[-50:], 70):
                score += 0.2
        
        # 3. 추세 일관성 (0.2)
        if len(prices) >= 10:
            ma_short = np.mean(prices[-5:])
            ma_long = np.mean(prices[-10:])
            if ma_short > ma_long:
                score += 0.2
            elif ma_short >= ma_long * 0.98:
                score += 0.1
        
        return self._normalize_score(score)
    
    def detect_vcp_pattern_enhanced(self, prices: np.ndarray, volumes: np.ndarray, 
                                  symbol: str) -> Tuple[bool, float, DimensionalScores]:
        """향상된 VCP 패턴 감지 (다차원 평가)"""
        try:
            if len(prices) < 60:
                return False, 0.0, DimensionalScores()
            
            # 최근 90일 데이터 사용
            recent_prices = prices[-90:] if len(prices) >= 90 else prices
            recent_volumes = volumes[-90:] if len(volumes) >= 90 else volumes
            
            # 1. 커널 회귀를 이용한 가격 곡선 스무딩
            smoothed_prices = kernel_smoothing(recent_prices)
            
            # 2. 피크와 골 추출
            peaks, troughs = extract_peaks_troughs(smoothed_prices)
            
            if len(peaks) < 3:
                return False, 0.0, DimensionalScores()
            
            # 3. 연속적 변동성 수축 검출
            amplitudes = calculate_amplitude_contraction(peaks, troughs, smoothed_prices)
            
            if len(amplitudes) < 2:
                return False, 0.0, DimensionalScores()
            
            # 4. 다차원 평가
            dimensional_scores = DimensionalScores(
                technical_quality=self._calculate_technical_quality_vcp(
                    smoothed_prices, peaks, troughs, amplitudes),
                volume_confirmation=self._calculate_volume_confirmation_vcp(recent_volumes),
                temporal_validity=self._calculate_temporal_validity_vcp(recent_prices, peaks),
                market_context=self._calculate_market_context(recent_prices)
            )
            
            # 5. 가중 평균으로 최종 신뢰도 계산
            final_confidence = 0.0
            for dimension, score in dimensional_scores.to_dict().items():
                if dimension in self.DIMENSION_WEIGHTS:
                    weight = self.DIMENSION_WEIGHTS[dimension]
                    if not (np.isnan(score) or np.isinf(score) or np.isnan(weight) or np.isinf(weight)):
                        final_confidence += score * weight
            
            # NaN 체크
            if np.isnan(final_confidence) or np.isinf(final_confidence):
                final_confidence = 0.0
            else:
                final_confidence = max(0.0, min(1.0, final_confidence))  # 0-1 범위로 제한
            
            # 6. 패턴 감지 결정
            vcp_detected = final_confidence >= self.DETECTION_THRESHOLD
            
            return vcp_detected, final_confidence, dimensional_scores
            
        except Exception as e:
            logger.warning(f"{symbol}: VCP 패턴 감지 중 오류 - {str(e)}")
            return False, 0.0, DimensionalScores()
    
    def _calculate_technical_quality_cup(self, smoothed_prices: np.ndarray, A_idx: int, 
                                       B_idx: int, C_idx: int, r_squared: float, 
                                       bezier_corr: float) -> float:
        """Cup&Handle 기술적 품질 평가 (0-1 정규화)"""
        score = 0.0
        
        A_price = smoothed_prices[A_idx]
        B_price = smoothed_prices[B_idx]
        C_price = smoothed_prices[C_idx]
        
        # 1. 깊이 적절성 (0.3)
        if A_price > 0:
            depth_ratio = (A_price - B_price) / A_price
            if 0.12 <= depth_ratio <= 0.35:  # 이상적 깊이
                score += 0.3
            elif 0.10 <= depth_ratio <= 0.50:  # 허용 깊이
                score += 0.2
            elif 0.05 <= depth_ratio <= 0.60:  # 최소 깊이
                score += 0.1
        
        # 2. 대칭성 (0.25)
        if A_price > 0:
            symmetry_ratio = C_price / A_price
            if symmetry_ratio >= 0.95:  # 거의 완벽한 대칭
                score += 0.25
            elif symmetry_ratio >= 0.90:  # 좋은 대칭
                score += 0.20
            elif symmetry_ratio >= 0.85:  # 허용 가능한 대칭
                score += 0.15
        
        # 3. U자형 적합도 (0.25)
        if r_squared >= 0.8:  # 매우 높은 적합도
            score += 0.25
        elif r_squared >= 0.7:  # 높은 적합도
            score += 0.20
        elif r_squared >= 0.6:  # 중간 적합도
            score += 0.15
        elif r_squared >= 0.5:  # 최소 적합도
            score += 0.10
        
        # 4. 베지어 곡선 상관계수 (0.2)
        if bezier_corr >= 0.9:  # 매우 높은 상관관계
            score += 0.2
        elif bezier_corr >= 0.85:  # 높은 상관관계
            score += 0.15
        elif bezier_corr >= 0.8:  # 중간 상관관계
            score += 0.1
        
        return self._normalize_score(score)
    
    def _calculate_volume_confirmation_cup(self, volumes: np.ndarray, A_idx: int, 
                                         B_idx: int, C_idx: int) -> float:
        """Cup&Handle 거래량 확인 평가 (0-1 정규화)"""
        score = 0.0
        
        if len(volumes) > max(A_idx, B_idx, C_idx):
            # 1. 컵 형성 중 거래량 감소 (0.6)
            if B_idx > 10 and A_idx < len(volumes) - 10:
                early_volume = np.mean(volumes[A_idx:A_idx+10])
                cup_volume = np.mean(volumes[max(0, B_idx-5):B_idx+5])
                
                if early_volume > 0:
                    volume_decrease = 1 - (cup_volume / early_volume)
                    if volume_decrease >= 0.3:  # 30% 이상 감소
                        score += 0.6
                    elif volume_decrease >= 0.2:  # 20% 이상 감소
                        score += 0.4
                    elif volume_decrease >= 0.1:  # 10% 이상 감소
                        score += 0.2
            
            # 2. 거래량 패턴 일관성 (0.4)
            cup_volumes = volumes[A_idx:C_idx+1]
            if len(cup_volumes) > 10:
                early_vol = cup_volumes[:len(cup_volumes)//3]
                late_vol = cup_volumes[-len(cup_volumes)//3:]
                
                early_avg = np.mean(early_vol)
                late_avg = np.mean(late_vol)
                
                if early_avg > 0 and late_avg <= early_avg * 1.1:  # 후반부 거래량 증가 제한
                    score += 0.4
                elif late_avg <= early_avg * 1.3:
                    score += 0.2
        
        return self._normalize_score(score)
    
    def _calculate_temporal_validity_cup(self, A_idx: int, B_idx: int, C_idx: int, 
                                       total_length: int) -> float:
        """Cup&Handle 시간적 유효성 평가 (0-1 정규화)"""
        score = 0.0
        
        # 1. 컵 폭 적절성 (0.4)
        cup_width = C_idx - A_idx
        if 35 <= cup_width <= 200:  # 7주-40주 (이상적)
            score += 0.4
        elif 20 <= cup_width <= 250:  # 4주-50주 (허용)
            score += 0.3
        elif cup_width >= 15:  # 최소 3주
            score += 0.2
        
        # 2. 좌우 균형 (0.3)
        left_width = B_idx - A_idx
        right_width = C_idx - B_idx
        
        if left_width > 0 and right_width > 0:
            balance_ratio = min(left_width, right_width) / max(left_width, right_width)
            if balance_ratio >= 0.7:  # 좋은 균형
                score += 0.3
            elif balance_ratio >= 0.5:  # 허용 가능한 균형
                score += 0.2
            elif balance_ratio >= 0.3:  # 최소 균형
                score += 0.1
        
        # 3. 전체 기간 적절성 (0.3)
        if 40 <= total_length <= 180:  # 이상적 기간
            score += 0.3
        elif 30 <= total_length <= 250:  # 허용 기간
            score += 0.2
        elif total_length >= 20:  # 최소 기간
            score += 0.1
        
        return self._normalize_score(score)
    
    def detect_cup_handle_pattern_enhanced(self, prices: np.ndarray, volumes: np.ndarray, 
                                         symbol: str) -> Tuple[bool, float, DimensionalScores]:
        """향상된 Cup&Handle 패턴 감지 (다차원 평가)"""
        try:
            if len(prices) < 40:
                return False, 0.0, DimensionalScores()
            
            # 최근 120일 데이터 사용
            recent_prices = prices[-120:] if len(prices) >= 120 else prices
            recent_volumes = volumes[-120:] if len(volumes) >= 120 else volumes
            
            # 1. 커널 회귀를 이용한 가격 곡선 스무딩
            smoothed_prices = kernel_smoothing(recent_prices)
            
            # 2. 피크와 골 추출
            peaks, troughs = extract_peaks_troughs(smoothed_prices)
            
            if len(peaks) < 2 or len(troughs) < 1:
                return False, 0.0, DimensionalScores()
            
            # 3. 컵 구조 식별 (A-B-C 포인트)
            n = len(smoothed_prices)
            
            # A점: 초기 1/3 구간에서 최고점
            A_region_end = n // 3
            A_candidates = peaks[peaks < A_region_end]
            if len(A_candidates) == 0:
                return False, 0.0, DimensionalScores()
            A_idx = A_candidates[np.argmax(smoothed_prices[A_candidates])]
            
            # B점: 중간 구간에서 최저점
            B_region_start = A_idx + 5
            B_region_end = min(n - 10, A_idx + 60)
            if B_region_end <= B_region_start:
                return False, 0.0, DimensionalScores()
            
            B_candidates = troughs[(troughs >= B_region_start) & (troughs <= B_region_end)]
            if len(B_candidates) == 0:
                B_idx = B_region_start + np.argmin(smoothed_prices[B_region_start:B_region_end])
            else:
                B_idx = B_candidates[np.argmin(smoothed_prices[B_candidates])]
            
            # C점: 마지막 구간에서 고점
            C_region_start = B_idx + 5
            C_region_end = min(n - 3, B_idx + 40)
            if C_region_end <= C_region_start:
                return False, 0.0, DimensionalScores()
            
            C_candidates = peaks[(peaks >= C_region_start) & (peaks <= C_region_end)]
            if len(C_candidates) == 0:
                C_idx = C_region_start + np.argmax(smoothed_prices[C_region_start:C_region_end])
            else:
                C_idx = C_candidates[np.argmax(smoothed_prices[C_candidates])]
            
            # 4. 수학적 검증
            cup_indices = np.arange(A_idx, C_idx + 1)
            cup_prices = smoothed_prices[A_idx:C_idx + 1]
            
            r_squared, curvature = 0.0, 0.0
            if len(cup_indices) >= 10:
                r_squared, curvature = quadratic_fit_cup(cup_indices, cup_prices)
            
            bezier_corr = 0.0
            if len(cup_prices) >= 5:
                control_points = np.array([cup_prices[0], np.min(cup_prices), cup_prices[-1]])
                bezier_corr = bezier_curve_correlation(control_points, cup_prices)
            
            # 5. 다차원 평가
            dimensional_scores = DimensionalScores(
                technical_quality=self._calculate_technical_quality_cup(
                    smoothed_prices, A_idx, B_idx, C_idx, r_squared, bezier_corr),
                volume_confirmation=self._calculate_volume_confirmation_cup(
                    recent_volumes, A_idx, B_idx, C_idx),
                temporal_validity=self._calculate_temporal_validity_cup(
                    A_idx, B_idx, C_idx, len(recent_prices)),
                market_context=self._calculate_market_context(recent_prices)
            )
            
            # 6. 가중 평균으로 최종 신뢰도 계산
            final_confidence = 0.0
            for dimension, score in dimensional_scores.to_dict().items():
                if dimension in self.DIMENSION_WEIGHTS:
                    weight = self.DIMENSION_WEIGHTS[dimension]
                    if not (np.isnan(score) or np.isinf(score) or np.isnan(weight) or np.isinf(weight)):
                        final_confidence += score * weight
            
            # NaN 체크
            if np.isnan(final_confidence) or np.isinf(final_confidence):
                final_confidence = 0.0
            else:
                final_confidence = max(0.0, min(1.0, final_confidence))  # 0-1 범위로 제한
            
            # 7. 패턴 감지 결정
            cup_handle_detected = final_confidence >= self.DETECTION_THRESHOLD
            
            return cup_handle_detected, final_confidence, dimensional_scores
            
        except Exception as e:
            logger.warning(f"{symbol}: Cup&Handle 패턴 감지 중 오류 - {str(e)}")
            return False, 0.0, DimensionalScores()
    
    def analyze_patterns_enhanced(self, symbol: str, stock_data: pd.DataFrame) -> Dict[str, Dict]:
        """향상된 패턴 분석 (다차원 평가 결과 포함)"""
        try:
            if stock_data.empty or len(stock_data) < 40:
                return {
                    'vcp': {
                        'detected': False, 
                        'confidence': 0.0,
                        'dimensional_scores': DimensionalScores().to_dict()
                    },
                    'cup_handle': {
                        'detected': False, 
                        'confidence': 0.0,
                        'dimensional_scores': DimensionalScores().to_dict()
                    }
                }
            
            # 가격과 거래량 데이터 추출
            prices = stock_data['Close'].values
            volumes = stock_data['Volume'].values if 'Volume' in stock_data.columns else np.ones(len(prices))
            
            # VCP 패턴 분석
            vcp_detected, vcp_confidence, vcp_dimensions = self.detect_vcp_pattern_enhanced(
                prices, volumes, symbol)
            
            # Cup & Handle 패턴 분석
            cup_detected, cup_confidence, cup_dimensions = self.detect_cup_handle_pattern_enhanced(
                prices, volumes, symbol)
            
            return {
                'vcp': {
                    'detected': vcp_detected,
                    'confidence': round(vcp_confidence, 3),
                    'dimensional_scores': vcp_dimensions.to_dict(),
                    'confidence_level': self._get_confidence_level(vcp_confidence)
                },
                'cup_handle': {
                    'detected': cup_detected,
                    'confidence': round(cup_confidence, 3),
                    'dimensional_scores': cup_dimensions.to_dict(),
                    'confidence_level': self._get_confidence_level(cup_confidence)
                }
            }
            
        except Exception as e:
            logger.error(f"{symbol}: 향상된 패턴 분석 오류: {e}")
            return {
                'vcp': {
                    'detected': False, 
                    'confidence': 0.0,
                    'dimensional_scores': DimensionalScores().to_dict(),
                    'confidence_level': 'None'
                },
                'cup_handle': {
                    'detected': False, 
                    'confidence': 0.0,
                    'dimensional_scores': DimensionalScores().to_dict(),
                    'confidence_level': 'None'
                }
            }
    
    def _get_confidence_level(self, confidence: float) -> str:
        """신뢰도 수준 분류"""
        # NaN이나 무한대 값을 0으로 처리
        if np.isnan(confidence) or np.isinf(confidence):
            confidence = 0.0
            
        if confidence >= self.HIGH_CONFIDENCE_THRESHOLD:
            return 'High'
        elif confidence >= self.DETECTION_THRESHOLD:
            return 'Medium'
        elif confidence >= 0.4:
            return 'Low'
        elif confidence == 0.0:
            return 'Low'  # 0.0인 경우 "low"로 변경
        else:
            return 'None'