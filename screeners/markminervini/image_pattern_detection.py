#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mark Minervini 고급 수학적 패턴 인식 모듈 (리팩토링됨)

이 모듈은 새로운 모듈화된 구조를 사용하여 패턴 감지를 수행합니다.
"""

import os
import sys
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Optional

# 프로젝트 루트 경로 설정
try:
    from utils.path_utils import add_project_root
    add_project_root()
    from config import *
except ImportError:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    project_root = BASE_DIR
    MARKMINERVINI_RESULTS_DIR = os.path.join(BASE_DIR, 'results', 'screeners', 'markminervini')
    os.makedirs(MARKMINERVINI_RESULTS_DIR, exist_ok=True)

# 새로운 모듈들 import
from .pattern_analyzer import PatternAnalyzer
from .chart_generator import ChartGenerator
from .integrated_screener import run_integrated_screening

logger = logging.getLogger(__name__)

# 레거시 호환성을 위한 래퍼 클래스
class ImagePatternDetector:
    """레거시 호환성을 위한 래퍼 클래스 (새로운 모듈들 사용)"""
    
    def __init__(self):
        self.pattern_analyzer = PatternAnalyzer()
        self.chart_generator = ChartGenerator(project_root, os.path.join(project_root, 'data', 'image'))
        self.image_dir = IMAGE_OUTPUT_DIR
        self.ensure_directories()
        
    def ensure_directories(self):
        """필요한 디렉토리 생성"""
        os.makedirs(self.image_dir, exist_ok=True)
        os.makedirs(MARKMINERVINI_RESULTS_DIR, exist_ok=True)
    


    def detect_pattern_with_ai(self, symbol: str) -> Dict:
        """AI 기반 패턴 감지 (새로운 모듈 사용)"""
        try:
            # OHLCV 데이터 가져오기
            stock_data = self.chart_generator.fetch_ohlcv_data(symbol, days=120)
            if stock_data is None:
                return {
                    'symbol': symbol,
                    'data_available': False,
                    'image_generated': False,
                    'vcp_detected': False,
                    'vcp_confidence': 0.0,
                    'cup_handle_detected': False,
                    'cup_handle_confidence': 0.0,
                    'processing_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'error': 'No data available'
                }
            
            # 차트 이미지 생성
            image_success = self.chart_generator.generate_chart_image(symbol, stock_data)
            
            # 패턴 분석
            pattern_results = self.pattern_analyzer.analyze_patterns_from_data(symbol, stock_data)
            
            return {
                'symbol': symbol,
                'data_available': True,
                'image_generated': image_success,
                'vcp_detected': pattern_results['vcp']['detected'],
                'vcp_confidence': pattern_results['vcp']['confidence'],
                'cup_handle_detected': pattern_results['cup_handle']['detected'],
                'cup_handle_confidence': pattern_results['cup_handle']['confidence'],
                'processing_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            logger.error(f"{symbol} 패턴 감지 중 오류: {e}")
            return {
                'symbol': symbol,
                'data_available': False,
                'image_generated': False,
                'vcp_detected': False,
                'vcp_confidence': 0.0,
                'cup_handle_detected': False,
                'cup_handle_confidence': 0.0,
                'processing_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'error': str(e)
            }
    
    def process_symbol(self, symbol: str) -> Dict:
        """개별 심볼 처리"""
        return self.detect_pattern_with_ai(symbol)

        
# 레거시 호환성을 위한 래퍼 함수들
def run_image_pattern_detection(max_symbols: Optional[int] = None, skip_data: bool = False) -> pd.DataFrame:
    """이미지 패턴 감지 실행 함수 (통합 스크리너 사용)
    
    Args:
        max_symbols: 처리할 최대 심볼 수
        
    Returns:
        결과 DataFrame
    """
    return run_integrated_screening(max_symbols)

if __name__ == "__main__":
    # 통합 스크리너 실행 (처음 50개 심볼만)
    results = run_image_pattern_detection(max_symbols=50)
    print(f"\n처리 완료: {len(results)}개 심볼")