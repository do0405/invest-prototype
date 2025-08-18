#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mark Minervini 고급 수학적 패턴 인식을 위한 수학 함수 모듈
학술 논문 기반 수학적 알고리즘 구현

Reference Papers:
- Lo, Mamaysky & Wang (2000): 비모수 커널 회귀 기반 차트 패턴 인식
- Suh, Li & Gao (2008): 베지어 곡선 이용 컵 앤 핸들 역방향 스크리닝
"""

import numpy as np
from typing import List, Tuple
from scipy.signal import find_peaks
from scipy.stats import pearsonr
import logging

logger = logging.getLogger(__name__)


def kernel_smoothing(prices: np.ndarray, bandwidth: float = None) -> np.ndarray:
    """비모수 커널 회귀를 이용한 가격 곡선 스무딩 (Lo, Mamaysky & Wang 2000)
    
    Args:
        prices: 가격 시계열 데이터
        bandwidth: 커널 대역폭 (None이면 자동 계산)
        
    Returns:
        np.ndarray: 스무딩된 가격 곡선
    """
    n = len(prices)
    if n < 10:
        return prices
    
    # NaN 값 처리
    if np.any(np.isnan(prices)) or np.any(np.isinf(prices)):
        prices = np.nan_to_num(prices, nan=np.nanmean(prices), posinf=np.nanmax(prices[np.isfinite(prices)]), neginf=np.nanmin(prices[np.isfinite(prices)]))
    
    # 최적 대역폭 계산 (CV 최적값의 30% 수준으로 조정)
    if bandwidth is None:
        # Silverman's rule of thumb 기반 대역폭
        std_prices = np.std(prices)
        if std_prices == 0 or np.isnan(std_prices):
            std_prices = 1.0  # 기본값 설정
        bandwidth = 1.06 * std_prices * (n ** (-1/5)) * 0.3
        if bandwidth <= 0 or np.isnan(bandwidth):
            bandwidth = 1.0  # 기본값 설정
    
    smoothed = np.zeros_like(prices)
    x_points = np.arange(n)
    
    for i in range(n):
        # 가우시안 커널 가중치 계산
        weights = np.exp(-0.5 * ((x_points - i) / bandwidth) ** 2)
        weight_sum = np.sum(weights)
        if weight_sum == 0 or np.isnan(weight_sum):
            smoothed[i] = prices[i]  # 원본 값 사용
        else:
            weights /= weight_sum
            # 가중 평균으로 스무딩 값 계산
            smoothed[i] = np.sum(weights * prices)
            # NaN 체크
            if np.isnan(smoothed[i]) or np.isinf(smoothed[i]):
                smoothed[i] = prices[i]  # 원본 값 사용
    
    return smoothed


def extract_peaks_troughs(smoothed_prices: np.ndarray, min_distance: int = 5) -> Tuple[np.ndarray, np.ndarray]:
    """스무딩된 곡선에서 피크와 골 추출
    
    Args:
        smoothed_prices: 스무딩된 가격 데이터
        min_distance: 피크 간 최소 거리
        
    Returns:
        Tuple[np.ndarray, np.ndarray]: (피크 인덱스, 골 인덱스)
    """
    # 피크 찾기
    peaks, _ = find_peaks(smoothed_prices, distance=min_distance)
    
    # 골 찾기 (음수로 변환 후 피크 찾기)
    troughs, _ = find_peaks(-smoothed_prices, distance=min_distance)
    
    return peaks, troughs


def calculate_amplitude_contraction(peaks: np.ndarray, troughs: np.ndarray, prices: np.ndarray) -> List[float]:
    """연속 피크 간 진폭 수축 계산 (Suh, Li & Gao 2008)
    
    Args:
        peaks: 피크 인덱스 배열
        troughs: 골 인덱스 배열  
        prices: 가격 데이터
        
    Returns:
        List[float]: 각 구간의 진폭 비율
    """
    if len(peaks) < 2:
        return []
    
    # NaN 값 처리
    if np.any(np.isnan(prices)) or np.any(np.isinf(prices)):
        prices = np.nan_to_num(prices, nan=np.nanmean(prices), posinf=np.nanmax(prices[np.isfinite(prices)]), neginf=np.nanmin(prices[np.isfinite(prices)]))
    
    amplitudes = []
    
    for i in range(len(peaks) - 1):
        try:
            peak1_idx = peaks[i]
            peak2_idx = peaks[i + 1]
            
            # 인덱스 유효성 검사
            if peak1_idx >= len(prices) or peak2_idx >= len(prices):
                continue
            
            # 두 피크 사이의 최저점 찾기
            between_troughs = troughs[(troughs > peak1_idx) & (troughs < peak2_idx)]
            if len(between_troughs) > 0:
                # 유효한 인덱스만 필터링
                valid_troughs = between_troughs[between_troughs < len(prices)]
                if len(valid_troughs) > 0:
                    trough_idx = valid_troughs[np.argmin(prices[valid_troughs])]
                    
                    # 진폭 계산 (피크에서 골까지의 최대 하락폭)
                    amplitude1 = prices[peak1_idx] - prices[trough_idx]
                    amplitude2 = prices[peak2_idx] - prices[trough_idx]
                    
                    # NaN 체크
                    if not (np.isnan(amplitude1) or np.isnan(amplitude2) or np.isinf(amplitude1) or np.isinf(amplitude2)):
                        amplitude = max(amplitude1, amplitude2)
                        if amplitude > 0:  # 양수인 진폭만 추가
                            amplitudes.append(amplitude)
        except Exception as e:
            logger.warning(f"진폭 계산 중 오류: {e}")
            continue
    
    return amplitudes


def quadratic_fit_cup(cup_indices: np.ndarray, prices: np.ndarray) -> Tuple[float, float]:
    """2차 다항식 근사를 이용한 U자형 컵 검증 (Suh, Li & Gao 2008)
    
    Args:
        cup_indices: 컵 구간의 인덱스
        prices: 해당 구간의 가격 데이터
        
    Returns:
        Tuple[float, float]: (R-squared, 곡률)
    """
    if len(cup_indices) < 3 or len(prices) < 3:
        return 0.0, 0.0
    
    try:
        # NaN 값 처리
        if np.any(np.isnan(prices)) or np.any(np.isinf(prices)):
            prices = np.nan_to_num(prices, nan=np.nanmean(prices), posinf=np.nanmax(prices[np.isfinite(prices)]), neginf=np.nanmin(prices[np.isfinite(prices)]))
        
        # 2차 다항식 피팅: f(t) = at^2 + bt + c
        coeffs = np.polyfit(cup_indices, prices, 2)
        
        # 계수에 NaN이 있는지 확인
        if np.any(np.isnan(coeffs)) or np.any(np.isinf(coeffs)):
            return 0.0, 0.0
        
        fitted_prices = np.polyval(coeffs, cup_indices)
        
        # R-squared 계산
        ss_res = np.sum((prices - fitted_prices) ** 2)
        ss_tot = np.sum((prices - np.mean(prices)) ** 2)
        
        # NaN 체크 및 안전한 계산
        if ss_tot > 0 and not (np.isnan(ss_res) or np.isnan(ss_tot) or np.isinf(ss_res) or np.isinf(ss_tot)):
            r_squared = 1 - (ss_res / ss_tot)
            r_squared = max(0.0, min(1.0, r_squared))  # 0-1 범위로 제한
        else:
            r_squared = 0.0
        
        # 곡률 계산 (2차 계수의 절댓값)
        curvature = abs(coeffs[0]) if not (np.isnan(coeffs[0]) or np.isinf(coeffs[0])) else 0.0
        
        return r_squared, curvature
    except Exception as e:
        logger.warning(f"2차 다항식 피팅 중 오류: {e}")
        return 0.0, 0.0


def bezier_curve_correlation(control_points: np.ndarray, actual_prices: np.ndarray) -> float:
    """베지어 곡선과 실제 가격의 상관계수 계산 (Suh, Li & Gao 2008)
    
    Args:
        control_points: 베지어 곡선 제어점
        actual_prices: 실제 가격 데이터
        
    Returns:
        float: 피어슨 상관계수
    """
    if len(control_points) < 3 or len(actual_prices) < 3:
        return 0.0
    
    try:
        # NaN 값 처리
        if np.any(np.isnan(control_points)) or np.any(np.isinf(control_points)):
            control_points = np.nan_to_num(control_points, nan=np.nanmean(control_points), posinf=np.nanmax(control_points[np.isfinite(control_points)]), neginf=np.nanmin(control_points[np.isfinite(control_points)]))
        
        if np.any(np.isnan(actual_prices)) or np.any(np.isinf(actual_prices)):
            actual_prices = np.nan_to_num(actual_prices, nan=np.nanmean(actual_prices), posinf=np.nanmax(actual_prices[np.isfinite(actual_prices)]), neginf=np.nanmin(actual_prices[np.isfinite(actual_prices)]))
        
        # 3차 베지어 곡선 생성
        t = np.linspace(0, 1, len(actual_prices))
        bezier_curve = np.zeros_like(t)
        
        n = len(control_points) - 1
        for i in range(n + 1):
            try:
                # 베르누이 다항식 계산
                bernstein = np.math.comb(n, i) * (t ** i) * ((1 - t) ** (n - i))
                
                # NaN 체크
                if not (np.any(np.isnan(bernstein)) or np.any(np.isinf(bernstein))):
                    bezier_curve += control_points[i] * bernstein
            except Exception:
                continue
        
        # NaN 체크
        if np.any(np.isnan(bezier_curve)) or np.any(np.isinf(bezier_curve)):
            return 0.0
        
        # 피어슨 상관계수 계산
        if len(bezier_curve) == len(actual_prices) and np.std(bezier_curve) > 0 and np.std(actual_prices) > 0:
            correlation, _ = pearsonr(bezier_curve, actual_prices)
            return abs(correlation) if not (np.isnan(correlation) or np.isinf(correlation)) else 0.0
        else:
            return 0.0
    except Exception as e:
        logger.warning(f"베지어 곡선 상관계수 계산 중 오류: {e}")
        return 0.0