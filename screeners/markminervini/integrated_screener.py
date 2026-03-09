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
import yfinance as yf
from datetime import datetime, timedelta
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

from utils.screener_utils import read_csv_flexible
from utils.external_data_cache import write_csv_atomic
from .enhanced_pattern_analyzer import EnhancedPatternAnalyzer
# 표준화된 형태로 변경 - 기본 저장 기능만 사용

logger = logging.getLogger(__name__)


class IntegratedScreener:
    """통합 스크리너 클래스"""
    
    def __init__(self):
        self.results_dir = MARKMINERVINI_RESULTS_DIR
        self.image_dir = os.path.join(project_root, 'data', 'image')
        
        # 컴포넌트 초기화
        self.pattern_analyzer = EnhancedPatternAnalyzer()
        
        # 결과 파일 경로
        self.pattern_results_csv = os.path.join(self.results_dir, 'integrated_pattern_results.csv')
        self.pattern_results_json = os.path.join(self.results_dir, 'integrated_pattern_results.json')
        
        self.ensure_directories()
    
    def ensure_directories(self):
        """필요한 디렉토리 생성"""
        os.makedirs(self.results_dir, exist_ok=True)

    @staticmethod
    def _normalize_ohlcv_frame(frame: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Normalize OHLCV payload to repository CSV shape before caching."""
        if frame is None or frame.empty:
            return pd.DataFrame()

        normalized = frame.copy()
        if isinstance(normalized.columns, pd.MultiIndex):
            normalized.columns = [col[0] if isinstance(col, tuple) else col for col in normalized.columns]

        if "Date" not in normalized.columns and "date" not in normalized.columns and isinstance(normalized.index, pd.DatetimeIndex):
            normalized = normalized.reset_index()

        normalized = normalized.rename(
            columns={
                "Date": "date",
                "Datetime": "date",
                "index": "date",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "adj close": "Adj Close",
                "adj_close": "Adj Close",
                "volume": "Volume",
            }
        )

        if "date" not in normalized.columns:
            return pd.DataFrame()

        normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce", utc=True)
        normalized = normalized.dropna(subset=["date"])
        if normalized.empty:
            return normalized

        for column in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]:
            if column in normalized.columns:
                normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

        keep_columns = ["date"]
        for column in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]:
            if column in normalized.columns:
                keep_columns.append(column)
        normalized["symbol"] = symbol
        keep_columns.append("symbol")

        normalized = normalized[keep_columns]
        normalized = normalized.sort_values("date").drop_duplicates(subset=["date"], keep="last")
        return normalized

    def _persist_ohlcv_cache(self, symbol: str, frame: pd.DataFrame, csv_path: str) -> None:
        """Persist fetched OHLCV into local CSV cache for subsequent runs."""
        normalized = self._normalize_ohlcv_frame(frame, symbol=symbol)
        if normalized.empty:
            return
        write_csv_atomic(normalized, csv_path, index=False)

    def merge_technical_and_financial(self) -> pd.DataFrame:
        """기술적 스크리닝과 재무제표 스크리닝 결과를 통합"""
        logger.info("📊 기술적 + 재무제표 결과 통합 중...")
        
        try:
            # 기술적 스크리닝 결과 로드
            if not os.path.exists(US_WITH_RS_PATH):
                logger.warning(f"기술적 스크리닝 결과 없음: {US_WITH_RS_PATH}")
                return pd.DataFrame()
            
            tech_df = read_csv_flexible(US_WITH_RS_PATH)
            
            # 재무제표 스크리닝 결과 로드
            if not os.path.exists(ADVANCED_FINANCIAL_RESULTS_PATH):
                logger.warning(f"재무제표 스크리닝 결과 없음: {ADVANCED_FINANCIAL_RESULTS_PATH}")
                return pd.DataFrame()
            
            fin_df = read_csv_flexible(ADVANCED_FINANCIAL_RESULTS_PATH)
            
            # 병합 전 전처리
            if 'symbol' not in tech_df.columns and tech_df.index.name != 'symbol':
                logger.error("기술적 결과에 symbol 컬럼이 없습니다.")
                return pd.DataFrame()
            if 'symbol' not in fin_df.columns and fin_df.index.name != 'symbol':
                logger.error("재무 결과에 symbol 컬럼이 없습니다.")
                return pd.DataFrame()

            # RS 점수 중복 처리
            if 'rs_score' in tech_df.columns and 'rs_score' in fin_df.columns:
                fin_df = fin_df.rename(columns={'rs_score': 'rs_score_fin'})
            
            # 병합 (Inner Join)
            merged_df = pd.merge(tech_df, fin_df, on='symbol', how='inner')
            merged_df = merged_df.drop_duplicates(subset=['symbol'])
            
            # 필터링 및 점수 계산
            if 'met_count' in merged_df.columns and 'fin_met_count' in merged_df.columns:
                merged_df['total_met_count'] = merged_df['met_count'] + merged_df['fin_met_count']
            
            # 재무 조건 최소 1개 이상 만족 필터링
            merged_df = merged_df[merged_df['fin_met_count'] > 0]
            
            # 정렬
            sort_cols = ['total_met_count', 'rs_score']
            if all(col in merged_df.columns for col in sort_cols):
                merged_df = merged_df.sort_values(sort_cols, ascending=[False, False])
            
            # 저장
            merged_df.to_csv(INTEGRATED_RESULTS_PATH, index=False)
            json_path = INTEGRATED_RESULTS_PATH.replace('.csv', '.json')
            merged_df.to_json(json_path, orient='records', indent=2, force_ascii=False)
            
            logger.info(f"✅ 통합 결과 저장 완료: {len(merged_df)}개 종목")
            return merged_df
            
        except Exception as e:
            logger.error(f"결과 통합 중 오류 발생: {e}")
            return pd.DataFrame()
    
    def get_ticker_list(self) -> List[str]:
        """새로운 티커 목록 가져오기
        
        Returns:
            List[str]: 티커 심볼 목록
        """
        try:
            # 0. 먼저 통합 결과를 최신화
            merged_df = self.merge_technical_and_financial()
            
            # 1. 통합 결과에서 티커 목록 가져오기 (가장 우선)
            if not merged_df.empty:
                return merged_df['symbol'].tolist()
            
            # 통합 실패 시 기존 로직 유지 (백업)
            integrated_path = os.path.join(self.results_dir, 'integrated_results.csv')
            if os.path.exists(integrated_path):
                 df = read_csv_flexible(integrated_path, required_columns=['symbol'])
                 if df is not None:
                     return df['symbol'].tolist()

            # ... (나머지 백업 로직은 유지하되, 위에서 대부분 처리됨)
            
            # 2. advanced_financial_results.csv에서 티커 목록 가져오기
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
    
    def fetch_ohlcv_data(self, symbol: str, days: int = 365) -> Optional[pd.DataFrame]:
        """
        심볼에 대한 OHLCV 데이터를 가져옵니다.
        1. 로컬 CSV 파일(data/us/{symbol}.csv)을 우선적으로 확인합니다.
        2. 로컬 파일이 없거나 문제가 있으면 yfinance를 통해 다운로드합니다.
        """
        df = None
        data_dir = os.path.join(project_root, 'data', 'us')
        
        # 1. 로컬 CSV 확인
        csv_path = os.path.join(data_dir, f"{symbol}.csv")
        if os.path.exists(csv_path):
            try:
                # CSV 읽기
                df = pd.read_csv(csv_path)
                
                # 컬럼 이름 표준화 (소문자 -> 대문자 첫글자)
                rename_map = {
                    'date': 'Date',
                    'open': 'Open',
                    'high': 'High',
                    'low': 'Low',
                    'close': 'Close',
                    'volume': 'Volume'
                }
                df.rename(columns=rename_map, inplace=True)
                
                # 날짜 인덱스 설정
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'], utc=True)
                    df.set_index('Date', inplace=True)
                
                # 필요한 컬럼 확인
                required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                if not all(col in df.columns for col in required_cols):
                    # 대소문자 무시하고 확인
                    lower_cols = [c.lower() for c in df.columns]
                    if all(rc.lower() in lower_cols for rc in required_cols):
                         # 이미 있을수도 있음.
                         pass
                    else:
                        logger.warning(f"⚠️ {symbol}: CSV에 필수 컬럼이 누락되었습니다. yfinance로 대체합니다.")
                        df = None
                    
            except Exception as e:
                logger.warning(f"⚠️ {symbol}: 로컬 CSV 읽기 실패 ({e}). yfinance로 대체합니다.")
                df = None
        
        # 2. 로컬 데이터가 없으면 yfinance 사용
        if df is None or df.empty:
            try:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days + 100) # MA 계산을 위한 여유 기간
                df = yf.download(
                    symbol,
                    start=start_date,
                    end=end_date,
                    progress=False,
                    auto_adjust=False,
                )
                
                if df.empty:
                    return None
                self._persist_ohlcv_cache(symbol=symbol, frame=df, csv_path=csv_path)
            except Exception as e:
                logger.error(f"❌ {symbol}: 데이터 다운로드 실패: {e}")
                return None
        
        # 3. 데이터 필터링 (최근 N일)
        if df is not None and not df.empty:
            # 인덱스가 DatetimeIndex인지 확인
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)
            
            # 정렬
            df.sort_index(inplace=True)
            
            # 기간 필터링
            end_date = df.index.max()
            start_date = end_date - timedelta(days=days)
            df = df.loc[start_date:end_date]
            
        return df

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
            stock_data = self.fetch_ohlcv_data(symbol, days=120)
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
            
            # 2. 차트 이미지 생성 (제거됨)
            image_success = False
            
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
                # 진행률 로깅 (과다 출력 방지를 위해 100개 단위로 출력)
                if i % 100 == 0 or i == 1 or i == total_symbols:
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
