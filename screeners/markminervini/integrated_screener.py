#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mark Minervini 통합 스크리너
새로운 티커 찾기 메서드와 패턴 감지를 연결하여 결과를 JSON/CSV로 저장
"""

import os
import sys
import pandas as pd
import json
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
    
# project_root가 정의되지 않은 경우 기본값 설정
if 'project_root' not in globals():
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
# MARKMINERVINI_RESULTS_DIR가 정의되지 않은 경우 기본값 설정
if 'MARKMINERVINI_RESULTS_DIR' not in globals():
    MARKMINERVINI_RESULTS_DIR = os.path.join(project_root, 'results', 'screeners', 'markminervini')
    os.makedirs(MARKMINERVINI_RESULTS_DIR, exist_ok=True)

from .pattern_analyzer import PatternAnalyzer
from .chart_generator import ChartGenerator
# 표준화된 형태로 변경 - 기본 저장 기능만 사용

logger = logging.getLogger(__name__)


class IntegratedScreener:
    """통합 스크리너 클래스"""
    
    def __init__(self):
        self.results_dir = MARKMINERVINI_RESULTS_DIR
        self.image_dir = os.path.join(project_root, 'data', 'image')
        
        # 컴포넌트 초기화
        self.pattern_analyzer = PatternAnalyzer()
        self.chart_generator = ChartGenerator(project_root, self.image_dir)
        
        # 결과 파일 경로
        self.pattern_results_csv = os.path.join(self.results_dir, 'integrated_pattern_results.csv')
        self.pattern_results_json = os.path.join(self.results_dir, 'integrated_pattern_results.json')
        
        self.ensure_directories()
    
    def ensure_directories(self):
        """필요한 디렉토리 생성"""
        os.makedirs(self.results_dir, exist_ok=True)
        os.makedirs(self.image_dir, exist_ok=True)
    
    def get_ticker_list(self) -> List[str]:
        """새로운 티커 목록 가져오기
        
        Returns:
            List[str]: 티커 심볼 목록
        """
        try:
            # 1. advanced_financial_results.csv에서 티커 목록 가져오기
            advanced_results_path = os.path.join(self.results_dir, 'advanced_financial_results.csv')
            
            if os.path.exists(advanced_results_path):
                logger.info("Advanced financial results에서 티커 목록 로드")
                from utils.screener_utils import read_csv_flexible
                advanced_df = read_csv_flexible(advanced_results_path, required_columns=['symbol'])
                if advanced_df is not None:
                    return advanced_df['symbol'].tolist()
            
            # 2. 기본 스크리너 결과에서 티커 목록 가져오기
            basic_results_path = os.path.join(self.results_dir, 'screener_results.csv')
            
            if os.path.exists(basic_results_path):
                logger.info("기본 스크리너 결과에서 티커 목록 로드")
                from utils.screener_utils import read_csv_flexible
                basic_df = read_csv_flexible(basic_results_path, required_columns=['symbol'])
                if basic_df is not None:
                    return basic_df['symbol'].tolist()
            
            # 3. 로컬 데이터 디렉토리에서 직접 티커 목록 가져오기
            data_dir = os.path.join(project_root, 'data', 'us')
            if os.path.exists(data_dir):
                logger.info("로컬 데이터 디렉토리에서 티커 목록 로드")
                csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
                return [f.replace('.csv', '') for f in csv_files[:100]]  # 처음 100개만
            
            logger.warning("티커 목록을 찾을 수 없습니다.")
            return []
            
        except Exception as e:
            logger.error(f"티커 목록 가져오기 실패: {e}")
            return []
    
    def process_symbol(self, symbol: str) -> Dict:
        """개별 심볼 처리
        
        Args:
            symbol: 주식 심볼
            
        Returns:
            Dict: 처리 결과
        """
        try:
            logger.info(f"처리 시작: {symbol}")
            
            # 1. OHLCV 데이터 가져오기
            stock_data = self.chart_generator.fetch_ohlcv_data(symbol, days=120)
            if stock_data is None or len(stock_data) < 40:
                return {
                    'symbol': symbol,
                'data_available': False,
                'image_generated': False,
                'vcp_detected': False,
                'vcp_confidence': 0.0,
                'vcp_confidence_level': 'None',
                'vcp_dimensional_scores': {'technical_quality': 0.0, 'volume_confirmation': 0.0, 'temporal_validity': 0.0, 'market_context': 0.0},
                'cup_handle_detected': False,
                'cup_handle_confidence': 0.0,
                'cup_handle_confidence_level': 'None',
                'cup_handle_dimensional_scores': {'technical_quality': 0.0, 'volume_confirmation': 0.0, 'temporal_validity': 0.0, 'market_context': 0.0},
                'processing_date': datetime.now().strftime('%Y-%m-%d'),
                'error': 'Insufficient data'
                }
            
            # 2. 차트 이미지 생성
            image_success = self.chart_generator.generate_chart_image(symbol, stock_data)
            
            # 3. 향상된 다차원 패턴 분석
            pattern_results = self.pattern_analyzer.analyze_patterns_enhanced(symbol, stock_data)
            
            # 4. 결과 통합 (다차원 평가 포함)
            result = {
                'symbol': symbol,
                'data_available': True,
                'image_generated': image_success,
                'vcp_detected': pattern_results['vcp']['detected'],
                'vcp_confidence': pattern_results['vcp']['confidence'],
                'vcp_confidence_level': pattern_results['vcp']['confidence_level'],
                'vcp_dimensional_scores': pattern_results['vcp']['dimensional_scores'],
                'cup_handle_detected': pattern_results['cup_handle']['detected'],
                'cup_handle_confidence': pattern_results['cup_handle']['confidence'],
                'cup_handle_confidence_level': pattern_results['cup_handle']['confidence_level'],
                'cup_handle_dimensional_scores': pattern_results['cup_handle']['dimensional_scores'],
                'processing_date': datetime.now().strftime('%Y-%m-%d')
            }
            
            # 패턴 감지 로깅 (다차원 평가 포함)
            if result['vcp_detected'] or result['cup_handle_detected']:
                patterns = []
                if result['vcp_detected']:
                    patterns.append(f"VCP({result['vcp_confidence']:.3f}/{result['vcp_confidence_level']})")
                if result['cup_handle_detected']:
                    patterns.append(f"Cup&Handle({result['cup_handle_confidence']:.3f}/{result['cup_handle_confidence_level']})")
                logger.info(f"{symbol}: 패턴 감지됨 - {', '.join(patterns)}")
            
            return result
            
        except Exception as e:
            logger.error(f"{symbol} 처리 중 오류: {e}")
            return {
                'symbol': symbol,
                'data_available': False,
                'image_generated': False,
                'vcp_detected': False,
                'vcp_confidence': 0.0,
                'vcp_confidence_level': 'Low',  # 0.0인 경우 "Low"로 변경
                'vcp_dimensional_scores': {'technical_quality': 0.0, 'volume_confirmation': 0.0, 'temporal_validity': 0.0, 'market_context': 0.0},
                'cup_handle_detected': False,
                'cup_handle_confidence': 0.0,
                'cup_handle_confidence_level': 'Low',  # 0.0인 경우 "Low"로 변경
                'cup_handle_dimensional_scores': {'technical_quality': 0.0, 'volume_confirmation': 0.0, 'temporal_validity': 0.0, 'market_context': 0.0},
                'processing_date': datetime.now().strftime('%Y-%m-%d'),
                'error': str(e)
            }
    
    def run_integrated_screening(self, max_symbols: Optional[int] = None) -> pd.DataFrame:
        """통합 스크리닝 실행
        
        Args:
            max_symbols: 처리할 최대 심볼 수 (None이면 전체)
            
        Returns:
            pd.DataFrame: 스크리닝 결과
        """
        logger.info("🔍 Mark Minervini 통합 스크리닝 시작")
        
        # 티커 목록 가져오기
        symbols = self.get_ticker_list()
        if not symbols:
            logger.error("처리할 티커가 없습니다.")
            return pd.DataFrame()
        
        if max_symbols:
            symbols = symbols[:max_symbols]
        
        logger.info(f"처리할 심볼 수: {len(symbols)}")
        
        # 각 심볼 처리
        results = []
        total_symbols = len(symbols)
        
        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"진행률: {i}/{total_symbols} ({i/total_symbols*100:.1f}%) - {symbol}")
                result = self.process_symbol(symbol)
                
                # 둘 다 false인 경우 저장하지 않음
                if result['vcp_detected'] or result['cup_handle_detected']:
                    results.append(result)
                else:
                    logger.debug(f"{symbol}: 패턴 미감지로 인해 결과에서 제외됨")
                
            except Exception as e:
                logger.error(f"{symbol} 처리 중 오류: {e}")
                # 오류가 발생한 경우에도 둘 다 false이므로 결과에 추가하지 않음
                logger.debug(f"{symbol}: 처리 오류로 인해 결과에서 제외됨 - {e}")
        
        # 결과 DataFrame 생성
        results_df = pd.DataFrame(results)
        
        # 결과 저장
        self.save_results(results_df)
        
        # 스크리닝 완료 메시지
        if not results_df.empty:
            print(f"✅ 마크 미니버니 통합 스크리닝 완료: {len(results_df)}개 종목 발견")
        else:
            print("✅ 마크 미니버니 통합 스크리닝 완료: 조건에 맞는 종목 없음")
        
        # 통계 출력
        self.print_statistics(results_df)
        
        return results_df
    
    def save_results(self, results_df: pd.DataFrame):
        """결과를 CSV와 JSON으로 저장 (증분 업데이트 지원)
        
        Args:
            results_df: 결과 DataFrame
        """
        try:
            if results_df.empty:
                # 빈 결과 파일 생성 (완전한 헤더 포함)
                empty_df = pd.DataFrame(columns=[
                    'symbol', 'data_available', 'image_generated', 'vcp_detected', 'vcp_confidence',
                    'vcp_confidence_level', 'vcp_dimensional_scores', 'cup_handle_detected',
                    'cup_handle_confidence', 'cup_handle_confidence_level', 'cup_handle_dimensional_scores',
                    'processing_date', 'error'
                ])
                empty_df.to_csv(self.pattern_results_csv, index=False, encoding='utf-8-sig')
                with open(self.pattern_results_json, 'w', encoding='utf-8') as f:
                    json.dump([], f, indent=2, ensure_ascii=False)
                logger.info(f"빈 결과 파일 생성: {self.pattern_results_csv}")
                return
            
            # 증분 업데이트 처리
            if os.path.exists(self.pattern_results_csv):
                try:
                    existing_df = pd.read_csv(self.pattern_results_csv)
                    # 새 데이터와 기존 데이터 병합
                    combined_df = pd.concat([existing_df, results_df], ignore_index=True)
                    # 중복 제거 (symbol 기준)
                    combined_df = combined_df.drop_duplicates(subset=['symbol'], keep='last')
                    # processing_date 기준 내림차순 정렬 유지
                    combined_df = combined_df.sort_values('processing_date', ascending=False)
                    final_df = combined_df
                except Exception as e:
                    logger.warning(f"기존 파일 읽기 실패, 새 파일로 저장: {e}")
                    final_df = results_df
            else:
                final_df = results_df
            
            # CSV 저장
            final_df.to_csv(self.pattern_results_csv, index=False, encoding='utf-8-sig')
            logger.info(f"CSV 결과 저장: {self.pattern_results_csv}")
            
            # JSON 저장 (dimensional_scores를 dict로 변환 및 NaN 처리)
            results_dict = final_df.to_dict('records')
            for record in results_dict:
                # NaN 값을 0으로 처리하고 confidence level 조정
                import math
                for key, value in record.items():
                    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                        record[key] = 0.0
                        # confidence가 0.0이 된 경우 해당 confidence_level을 "Low"로 설정
                        if key in ['vcp_confidence', 'cup_handle_confidence'] and record[key] == 0.0:
                            level_key = key.replace('confidence', 'confidence_level')
                            if level_key in record:
                                record[level_key] = 'Low'
                    elif value == 'NaN' or str(value) == 'nan':
                        record[key] = 0.0
                        # confidence가 0.0이 된 경우 해당 confidence_level을 "Low"로 설정
                        if key in ['vcp_confidence', 'cup_handle_confidence'] and record[key] == 0.0:
                            level_key = key.replace('confidence', 'confidence_level')
                            if level_key in record:
                                record[level_key] = 'Low'
                
                # dimensional_scores가 문자열인 경우 dict로 변환
                for key in ['vcp_dimensional_scores', 'cup_handle_dimensional_scores']:
                    if key in record and isinstance(record[key], str):
                        try:
                            # numpy 타입이 포함된 문자열을 처리
                            import re
                            import ast
                            score_str = record[key]
                            # numpy 타입 제거 (예: np.float64(0.8) -> 0.8)
                            score_str = re.sub(r'np\.\w+\(([^)]+)\)', r'\1', score_str)
                            record[key] = ast.literal_eval(score_str)
                        except (ValueError, SyntaxError) as e:
                            # 변환 실패 시 기본값 설정
                            logger.warning(f"dimensional_scores 파싱 실패 ({key}): {e}")
                            record[key] = {'technical_quality': 0.0, 'volume_confirmation': 0.0, 'temporal_validity': 0.0, 'market_context': 0.0}
            
            with open(self.pattern_results_json, 'w', encoding='utf-8') as f:
                json.dump(results_dict, f, indent=2, ensure_ascii=False, default=str)
            logger.info(f"JSON 결과 저장: {self.pattern_results_json}")
            
        except Exception as e:
            logger.error(f"결과 저장 실패: {e}")
    
    def print_statistics(self, results_df: pd.DataFrame):
        """통계 정보 출력
        
        Args:
            results_df: 결과 DataFrame
        """
        total_count = len(results_df)
        data_available = results_df['data_available'].sum()
        image_generated = results_df['image_generated'].sum()
        vcp_detected = results_df['vcp_detected'].sum()
        cup_handle_detected = results_df['cup_handle_detected'].sum()
        both_patterns = (results_df['vcp_detected'] & results_df['cup_handle_detected']).sum()
        
        print("\n" + "="*70)
        print("🔍 Mark Minervini 통합 스크리닝 결과")
        print("="*70)
        print(f"📊 전체 처리 심볼 수: {total_count:,}")
        print(f"📈 데이터 사용 가능: {data_available:,} ({data_available/total_count*100:.1f}%)")
        print(f"🖼️ 이미지 생성 성공: {image_generated:,} ({image_generated/total_count*100:.1f}%)")
        print(f"📈 VCP 패턴 감지: {vcp_detected:,} ({vcp_detected/total_count*100:.1f}%)")
        print(f"☕ Cup&Handle 패턴 감지: {cup_handle_detected:,} ({cup_handle_detected/total_count*100:.1f}%)")
        print(f"🎯 두 패턴 모두 감지: {both_patterns:,} ({both_patterns/total_count*100:.1f}%)")
        print("="*70)
        
        # 상위 패턴 감지 결과 출력 (신뢰도 순)
        if vcp_detected > 0:
            print("\n🔍 VCP 패턴 감지된 상위 10개 심볼 (신뢰도 순):")
            vcp_symbols = results_df[results_df['vcp_detected']].nlargest(10, 'vcp_confidence')
            for i, (_, row) in enumerate(vcp_symbols.iterrows(), 1):
                print(f"  {i:2d}. {row['symbol']}: {row['vcp_confidence']:.3f}")
        
        if cup_handle_detected > 0:
            print("\n☕ Cup&Handle 패턴 감지된 상위 10개 심볼 (신뢰도 순):")
            cup_symbols = results_df[results_df['cup_handle_detected']].nlargest(10, 'cup_handle_confidence')
            for i, (_, row) in enumerate(cup_symbols.iterrows(), 1):
                print(f"  {i:2d}. {row['symbol']}: {row['cup_handle_confidence']:.3f}")


def run_integrated_screening(max_symbols: Optional[int] = None) -> pd.DataFrame:
    """통합 스크리닝 실행 함수
    
    Args:
        max_symbols: 처리할 최대 심볼 수
        
    Returns:
        pd.DataFrame: 스크리닝 결과
    """
    screener = IntegratedScreener()
    return screener.run_integrated_screening(max_symbols)


if __name__ == "__main__":
    # 테스트 실행 (처음 50개 심볼만)
    results = run_integrated_screening(max_symbols=50)
    print(f"\n처리 완료: {len(results)}개 심볼")