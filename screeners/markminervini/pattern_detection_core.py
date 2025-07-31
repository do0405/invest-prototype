#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
패턴 감지 핵심 알고리즘 모듈

이 모듈은 VCP와 Cup&Handle 패턴 감지 알고리즘을 제공합니다.
학술 논문 기반의 고급 수학적 알고리즘을 사용합니다.
"""

from __future__ import annotations

import logging
from typing import List

import pandas as pd
import numpy as np

from .mathematical_functions import (
    kernel_smoothing,
    extract_peaks_troughs,
    calculate_amplitude_contraction,
    quadratic_fit_cup,
    bezier_curve_correlation
)

logger = logging.getLogger(__name__)


def detect_vcp(df: pd.DataFrame) -> bool:
    """학술 논문 기반 VCP 패턴 감지 (Lo, Mamaysky & Wang 2000; Suh, Li & Gao 2008)
    
    Args:
        df: 주가 데이터 (최소 60일 이상의 데이터 필요)
        
    Returns:
        bool: VCP 패턴 감지 여부
    """
    if df is None or len(df) < 60:
        return False

    # 최근 90일 데이터 사용
    recent = df.tail(90).copy()
    prices = recent["close"].values
    volumes = recent["volume"].values
    
    # 1. 커널 회귀를 이용한 가격 곡선 스무딩
    smoothed_prices = kernel_smoothing(prices)
    
    # 2. 스무딩된 곡선에서 피크와 골 추출
    peaks, troughs = extract_peaks_troughs(smoothed_prices)
    
    if len(peaks) < 3:  # 최소 3개의 피크 필요
        return False
    
    # 3. 연속적 변동성 수축 검출
    amplitudes = calculate_amplitude_contraction(peaks, troughs, smoothed_prices)
    
    if len(amplitudes) < 2:  # 최소 2회 수축 필요
        return False
    
    # 4. 진폭 감소 패턴 확인
    contraction_count = 0
    for i in range(1, len(amplitudes)):
        if amplitudes[i] < amplitudes[i-1] * 0.85:  # 15% 이상 감소
            contraction_count += 1
    
    if contraction_count < 2:  # 최소 2회 연속 수축
        return False
    
    # 5. 거래량 패턴 확인 (수축 시 거래량 감소)
    volume_ma = pd.Series(volumes).rolling(10).mean().values
    recent_volume_trend = volume_ma[-10:]
    
    if len(recent_volume_trend) > 5:
        volume_decrease = recent_volume_trend[-1] < recent_volume_trend[0] * 0.8
        if not volume_decrease:
            return False
    
    # 6. 최종 브레이크아웃 확인
    last_peak_price = smoothed_prices[peaks[-1]]
    current_price = prices[-1]
    
    # 현재 가격이 마지막 피크 근처에 있어야 함
    if current_price < last_peak_price * 0.95:
        return False
    
    return True


def detect_cup_and_handle(df: pd.DataFrame, window: int = 180) -> bool:
    """학술 논문 기반 Cup-with-Handle 패턴 감지 (Suh, Li & Gao 2008)
    
    Args:
        df: 주가 데이터 (최소 window일 이상의 데이터 필요)
        window: 분석할 기간 (기본값: 180일)
        
    Returns:
        bool: Cup-with-Handle 패턴 감지 여부
    """
    if df is None or len(df) < window:
        return False

    data = df.tail(window).copy()
    prices = data["close"].values
    volumes = data["volume"].values
    
    # 1. 커널 회귀를 이용한 가격 곡선 스무딩
    smoothed_prices = kernel_smoothing(prices)
    
    # 2. 피크와 골 추출
    peaks, troughs = extract_peaks_troughs(smoothed_prices)
    
    if len(peaks) < 2 or len(troughs) == 0:
        return False

    # 3. 컵 구조 식별 (좌측 고점 - 바닥 - 우측 고점)
    left_peak = peaks[0]
    right_candidates = peaks[peaks > left_peak]
    if len(right_candidates) == 0:
        return False
    
    right_peak = right_candidates[-1]
    bottom_candidates = troughs[(troughs > left_peak) & (troughs < right_peak)]
    if len(bottom_candidates) == 0:
        return False
    
    bottom = bottom_candidates[np.argmin(smoothed_prices[bottom_candidates])]

    # 4. 기본 구조 검증
    if right_peak - left_peak < 30:  # 최소 30일 컵 형성 기간
        return False
    
    if bottom - left_peak < 8 or right_peak - bottom < 8:  # 좌우 균형
        return False

    # 5. 2차 다항식 근사를 이용한 U자형 컵 검증
    cup_indices = np.arange(left_peak, right_peak + 1)
    cup_prices = smoothed_prices[left_peak:right_peak + 1]
    
    r_squared, curvature = quadratic_fit_cup(cup_indices, cup_prices)
    
    if r_squared < 0.7:  # R-squared 임계값
        return False
    
    if curvature < 0.0001:  # 충분한 곡률 필요
        return False

    # 6. 베지어 곡선 상관계수 검증
    # 7개 제어점 선정: 좌측 고점, 중간점들, 바닥, 우측 고점
    control_points = np.array([
        smoothed_prices[left_peak],
        smoothed_prices[left_peak + (bottom - left_peak) // 2],
        smoothed_prices[bottom],
        smoothed_prices[bottom + (right_peak - bottom) // 2],
        smoothed_prices[right_peak]
    ])
    
    correlation = bezier_curve_correlation(control_points, cup_prices)
    
    if correlation < 0.85:  # 논문에서 제시한 임계값
        return False

    # 7. 좌우 고점 대칭성 검증
    left_high = smoothed_prices[left_peak]
    right_high = smoothed_prices[right_peak]
    height_diff = abs(left_high - right_high) / min(left_high, right_high) * 100
    
    if height_diff > 5:  # 5% 이내 차이
        return False

    # 8. 컵 깊이 검증
    bottom_low = smoothed_prices[bottom]
    depth = (min(left_high, right_high) - bottom_low) / min(left_high, right_high) * 100
    
    if not (12 <= depth <= 50):  # 적절한 깊이
        return False

    # 9. 핸들 검증
    handle_start = right_peak
    handle_prices = smoothed_prices[handle_start:]
    
    if len(handle_prices) < 5:  # 최소 핸들 길이
        return False
    
    handle_low = np.min(handle_prices)
    handle_depth = (right_high - handle_low) / right_high * 100
    
    # 핸들 깊이가 컵 깊이의 33% 이내 (논문 기준)
    if handle_depth > depth * 0.33:
        return False
    
    if handle_depth < 2 or handle_depth > 25:  # 적절한 핸들 깊이
        return False

    # 10. 거래량 패턴 검증
    avg_volume = pd.Series(volumes).rolling(20).mean().values
    
    # 컵 바닥에서 거래량 감소
    bottom_vol = volumes[max(0, bottom - 2): bottom + 3].mean()
    if bottom_vol >= avg_volume[bottom] * 0.7:
        return False
    
    # 핸들 구간 거래량 확인
    handle_vol = volumes[handle_start:].mean() if len(volumes[handle_start:]) > 0 else volumes[handle_start]
    if handle_vol >= bottom_vol * 1.2:
        return False
    
    # 최근 브레이크아웃 거래량
    if len(volumes) > 0 and len(avg_volume) > 0:
        if volumes[-1] < avg_volume[-1] * 1.2:
            return False

    # 11. 컵 형성 중 거래량 감소 트렌드
    cup_volumes = volumes[left_peak:right_peak]
    if len(cup_volumes) > 10:
        early_vol = cup_volumes[:len(cup_volumes)//3].mean()
        late_vol = cup_volumes[-len(cup_volumes)//3:].mean()
        if late_vol >= early_vol * 1.1:  # 후반부 거래량이 너무 증가하면 안됨
            return False

    return True