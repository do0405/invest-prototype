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
    
    # 최적 대역폭 계산 (CV 최적값의 30% 수준으로 조정)
    if bandwidth is None:
        # Silverman's rule of thumb 기반 대역폭
        std_prices = np.std(prices)
        bandwidth = 1.06 * std_prices * (n ** (-1/5)) * 0.3
    
    smoothed = np.zeros_like(prices)
    x_points = np.arange(n)
    
    for i in range(n):
        # 가우시안 커널 가중치 계산
        weights = np.exp(-0.5 * ((x_points - i) / bandwidth) ** 2)
        weights /= np.sum(weights)
        
        # 가중 평균으로 스무딩 값 계산
        smoothed[i] = np.sum(weights * prices)
    
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
    
    amplitudes = []
    
    for i in range(len(peaks) - 1):
        peak1_idx = peaks[i]
        peak2_idx = peaks[i + 1]
        
        # 두 피크 사이의 최저점 찾기
        between_troughs = troughs[(troughs > peak1_idx) & (troughs < peak2_idx)]
        if len(between_troughs) > 0:
            trough_idx = between_troughs[np.argmin(prices[between_troughs])]
            
            # 진폭 계산 (피크에서 골까지의 최대 하락폭)
            amplitude = max(
                prices[peak1_idx] - prices[trough_idx],
                prices[peak2_idx] - prices[trough_idx]
            )
            amplitudes.append(amplitude)
    
    return amplitudes


def quadratic_fit_cup(cup_indices: np.ndarray, prices: np.ndarray) -> Tuple[float, float]:
    """2차 다항식 근사를 이용한 U자형 컵 검증 (Suh, Li & Gao 2008)
    
    Args:
        cup_indices: 컵 구간의 인덱스
        prices: 해당 구간의 가격 데이터
        
    Returns:
        Tuple[float, float]: (R-squared, 곡률)
    """
    if len(cup_indices) < 3:
        return 0.0, 0.0
    
    try:
        # 2차 다항식 피팅: f(t) = at^2 + bt + c
        coeffs = np.polyfit(cup_indices, prices, 2)
        fitted_prices = np.polyval(coeffs, cup_indices)
        
        # R-squared 계산
        ss_res = np.sum((prices - fitted_prices) ** 2)
        ss_tot = np.sum((prices - np.mean(prices)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
        
        # 곡률 계산 (2차 계수의 절댓값)
        curvature = abs(coeffs[0])
        
        return r_squared, curvature
    except:
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
        # 3차 베지어 곡선 생성
        t = np.linspace(0, 1, len(actual_prices))
        bezier_curve = np.zeros_like(t)
        
        n = len(control_points) - 1
        for i in range(n + 1):
            # 베르누이 다항식 계산
            bernstein = np.math.comb(n, i) * (t ** i) * ((1 - t) ** (n - i))
            bezier_curve += control_points[i] * bernstein
        
        # 피어슨 상관계수 계산
        correlation, _ = pearsonr(bezier_curve, actual_prices)
        return abs(correlation) if not np.isnan(correlation) else 0.0
    except:
        return 0.0