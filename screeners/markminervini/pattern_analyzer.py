#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mark Minervini 패턴 분석기 (향상된 다차원 평가 시스템 적용)
VCP와 Cup & Handle 패턴을 감지하고 정규화된 신뢰도를 계산
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
from .enhanced_pattern_analyzer import EnhancedPatternAnalyzer, DimensionalScores

logger = logging.getLogger(__name__)


class PatternAnalyzer:
    """Mark Minervini 패턴 분석기 (향상된 다차원 평가 시스템)"""
    
    def __init__(self):
        # 향상된 패턴 분석기 인스턴스 생성
        self.enhanced_analyzer = EnhancedPatternAnalyzer()
        
        # 하위 호환성을 위한 기존 임계값 유지
        self.VCP_CONFIDENCE_THRESHOLD = 0.5
        self.CUP_HANDLE_CONFIDENCE_THRESHOLD = 0.6
    
    def detect_vcp_pattern(self, prices: np.ndarray, volumes: np.ndarray, symbol: str) -> Tuple[bool, float]:
        """VCP 패턴 감지 및 신뢰도 계산 (향상된 다차원 평가 적용)"""
        try:
            # 향상된 분석기 사용
            vcp_detected, vcp_confidence, vcp_dimensions = self.enhanced_analyzer.detect_vcp_pattern_enhanced(
                prices, volumes, symbol)
            
            # 하위 호환성을 위해 기존 임계값으로 재검증
            legacy_detected = vcp_confidence >= self.VCP_CONFIDENCE_THRESHOLD
            
            return legacy_detected, vcp_confidence
            
        except Exception as e:
            logger.warning(f"{symbol}: VCP 패턴 감지 중 오류 - {str(e)}")
            return False, 0.0
    
    def detect_cup_handle_pattern(self, prices: np.ndarray, volumes: np.ndarray, symbol: str) -> Tuple[bool, float]:
        """Cup & Handle 패턴 감지 및 신뢰도 계산 (향상된 다차원 평가 적용)"""
        try:
            # 향상된 분석기 사용
            cup_detected, cup_confidence, cup_dimensions = self.enhanced_analyzer.detect_cup_handle_pattern_enhanced(
                prices, volumes, symbol)
            
            # 하위 호환성을 위해 기존 임계값으로 재검증
            legacy_detected = cup_confidence >= self.CUP_HANDLE_CONFIDENCE_THRESHOLD
            
            return legacy_detected, cup_confidence
            
        except Exception as e:
            logger.warning(f"{symbol}: Cup & Handle 패턴 감지 중 오류 - {str(e)}")
            return False, 0.0
    
    def analyze_patterns_from_data(self, symbol: str, stock_data: pd.DataFrame) -> Dict[str, Dict[str, Union[bool, float]]]:
        """주식 데이터로부터 패턴 분석 (향상된 다차원 평가 결과 포함)"""
        try:
            if stock_data.empty or len(stock_data) < 40:
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
            
            # 향상된 분석기를 통한 다차원 평가
            enhanced_results = self.enhanced_analyzer.analyze_patterns_enhanced(symbol, stock_data)
            
            # 하위 호환성을 위한 기존 방식 결과도 포함
            prices = stock_data['Close'].values
            volumes = stock_data['Volume'].values if 'Volume' in stock_data.columns else np.ones(len(prices))
            
            # 기존 임계값으로 재검증
            vcp_legacy_detected, vcp_legacy_confidence = self.detect_vcp_pattern(prices, volumes, symbol)
            cup_legacy_detected, cup_legacy_confidence = self.detect_cup_handle_pattern(prices, volumes, symbol)
            
            # 향상된 결과와 기존 결과 통합
            return {
                'vcp': {
                    'detected': enhanced_results['vcp']['detected'],
                    'confidence': enhanced_results['vcp']['confidence'],
                    'dimensional_scores': enhanced_results['vcp']['dimensional_scores'],
                    'confidence_level': enhanced_results['vcp']['confidence_level'],
                    'legacy_detected': vcp_legacy_detected,
                    'legacy_confidence': round(vcp_legacy_confidence, 3)
                },
                'cup_handle': {
                    'detected': enhanced_results['cup_handle']['detected'],
                    'confidence': enhanced_results['cup_handle']['confidence'],
                    'dimensional_scores': enhanced_results['cup_handle']['dimensional_scores'],
                    'confidence_level': enhanced_results['cup_handle']['confidence_level'],
                    'legacy_detected': cup_legacy_detected,
                    'legacy_confidence': round(cup_legacy_confidence, 3)
                }
            }
            
        except Exception as e:
            logger.error(f"{symbol}: 패턴 분석 오류: {e}")
            return {
                'vcp': {
                    'detected': False, 
                    'confidence': 0.0,
                    'dimensional_scores': DimensionalScores().to_dict(),
                    'confidence_level': 'None',
                    'legacy_detected': False,
                    'legacy_confidence': 0.0
                },
                'cup_handle': {
                    'detected': False, 
                    'confidence': 0.0,
                    'dimensional_scores': DimensionalScores().to_dict(),
                    'confidence_level': 'None',
                    'legacy_detected': False,
                    'legacy_confidence': 0.0
                }
            }
    
    def analyze_patterns_enhanced(self, symbol: str, stock_data: pd.DataFrame) -> Dict[str, Dict]:
        """향상된 패턴 분석 (다차원 평가 결과 포함) - integrated_screener.py 호환성을 위한 메서드"""
        return self.enhanced_analyzer.analyze_patterns_enhanced(symbol, stock_data)