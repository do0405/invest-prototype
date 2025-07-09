#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mark Minervini 이미지 기반 패턴 인식 모듈
VCP(Volatility Contraction Pattern)와 Cup & Handle 패턴을 이미지 AI로 감지
"""

import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import mplfinance as mpf
from datetime import datetime, timedelta
import yfinance as yf
import json
import logging
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# YOLO import (ultralytics 라이브러리 필요)
try:
    from ultralytics import YOLO
except ImportError:
    YOLO = None
    print("Warning: ultralytics 라이브러리가 설치되지 않았습니다. 이미지 패턴 감지 기능이 제한됩니다.")

from utils.path_utils import add_project_root

# 프로젝트 루트 경로 설정
add_project_root()

from config import *

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 결과 파일 경로
IMAGE_PATTERN_RESULTS_CSV = os.path.join(MARKMINERVINI_RESULTS_DIR, 'image_pattern_results.csv')
IMAGE_PATTERN_RESULTS_JSON = os.path.join(MARKMINERVINI_RESULTS_DIR, 'image_pattern_results.json')
IMAGE_OUTPUT_DIR = os.path.join(project_root, 'data', 'image')

class ImagePatternDetector:
    """
    이미지 기반 차트 패턴 감지 클래스
    """
    
    def __init__(self):
        self.image_dir = IMAGE_OUTPUT_DIR
        self.ensure_directories()
        
    def ensure_directories(self):
        """필요한 디렉토리 생성"""
        os.makedirs(self.image_dir, exist_ok=True)
        os.makedirs(MARKMINERVINI_RESULTS_DIR, exist_ok=True)
        
    def fetch_ohlcv_data(self, symbol: str, days: int = 120) -> Optional[pd.DataFrame]:
        """
        로컬 CSV 파일에서 OHLCV 데이터 가져오기
        
        Args:
            symbol: 주식 심볼
            days: 조회할 일수 (기본 120일)
            
        Returns:
            OHLCV DataFrame 또는 None
        """
        try:
            # 로컬 CSV 파일 경로
            csv_path = os.path.join(project_root, 'data', 'us', f'{symbol}.csv')
            
            if not os.path.exists(csv_path):
                logger.warning(f"{symbol}: 로컬 데이터 파일이 없습니다. ({csv_path})")
                return None
            
            # CSV 파일 읽기
            data = pd.read_csv(csv_path)
            
            # 컬럼명 확인 및 정리
            if len(data.columns) >= 7:
                # 일반적인 형태: Date,Open,High,Low,Close,Adj Close,Volume,Symbol
                data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume', 'Symbol']
                data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
            else:
                logger.error(f"{symbol}: CSV 파일 형식이 올바르지 않습니다. 컬럼 수: {len(data.columns)}")
                return None
            
            # 날짜 컬럼 처리
            data['Date'] = pd.to_datetime(data['Date'], errors='coerce', utc=True)
            data = data.dropna(subset=['Date'])  # 잘못된 날짜 제거
            
            # timezone 정보 제거 (localize to None)
            data['Date'] = data['Date'].dt.tz_localize(None)
            
            data.set_index('Date', inplace=True)
            data = data.sort_index()
            
            # 인덱스가 DatetimeIndex인지 확인하고 강제 변환
            if not isinstance(data.index, pd.DatetimeIndex):
                data.index = pd.to_datetime(data.index, utc=True).tz_localize(None)
            
            if len(data) < days:
                logger.warning(f"{symbol}: 충분한 데이터가 없습니다. (조회된 데이터: {len(data)}일)")
                return None
                
            # 최근 120일 데이터만 사용
            data = data.tail(days)
            
            # 컬럼명을 mplfinance에 맞게 변경
            data.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            
            # 숫자형 데이터로 변환
            for col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')
            
            # NaN 값이 있는 행 제거
            data = data.dropna()
            
            return data
            
        except Exception as e:
            logger.error(f"{symbol} 데이터 조회 실패: {e}")
            return None
            
    def generate_chart_image(self, symbol: str, data: pd.DataFrame) -> bool:
        """
        OHLCV 데이터로 캔들차트 이미지 생성
        
        Args:
            symbol: 주식 심볼
            data: OHLCV DataFrame
            
        Returns:
            성공 여부
        """
        try:
            image_path = os.path.join(self.image_dir, f"{symbol}.png")
            
            # mplfinance 스타일 설정
            mc = mpf.make_marketcolors(
                up='g', down='r',
                edge='inherit',
                wick={'up':'green', 'down':'red'},
                volume='in'
            )
            
            s = mpf.make_mpf_style(
                marketcolors=mc,
                gridstyle='-',
                y_on_right=False
            )
            
            # 차트 생성
            mpf.plot(
                data,
                type='candle',
                volume=True,
                style=s,
                title=f'{symbol} - 120 Days Chart',
                ylabel='Price ($)',
                ylabel_lower='Volume',
                figsize=(12, 8),
                savefig=dict(fname=image_path, dpi=150, bbox_inches='tight')
            )
            
            plt.close('all')  # 메모리 정리
            
            logger.info(f"{symbol} 차트 이미지 생성 완료: {image_path}")
            return True
            
        except Exception as e:
            logger.error(f"{symbol} 차트 이미지 생성 실패: {e}")
            return False
            
    def detect_pattern_with_ai(self, symbol: str) -> Dict[str, any]:
        """
        AI 모델을 사용하여 패턴 감지
        
        Args:
            symbol: 주식 심볼
            
        Returns:
            패턴 감지 결과
        """
        try:
            image_path = os.path.join(self.image_dir, f"{symbol}.png")
            
            if not os.path.exists(image_path):
                return {
                    'symbol': symbol,
                    'has_image': False,
                    'vcp_detected': False,
                    'vcp_confidence': 0.0,
                    'cup_handle_detected': False,
                    'cup_handle_confidence': 0.0,
                    'detection_date': datetime.now().strftime('%Y-%m-%d'),
                    'error': 'Image not found'
                }
            
            # YOLOv8 모델 사용 (ultralytics 라이브러리 필요)
            try:
                if YOLO is None:
                    raise ImportError("ultralytics 라이브러리가 설치되지 않았습니다.")
                    
                # Hugging Face에서 주식 패턴 감지 전용 모델 사용
                logger.info("Hugging Face YOLOv8 패턴 감지 모델 로드 중...")
                model = YOLO('foduucom/stockmarket-pattern-detection-yolov8')
                
                # 이미지에서 패턴 감지 실행
                results = model(image_path)
                
                # 패턴 감지 결과 초기화
                vcp_detected = False
                vcp_confidence = 0.0
                cup_detected = False
                cup_confidence = 0.0
                
                # 결과 분석
                for result in results:
                    boxes = result.boxes
                    if boxes is not None:
                        for box in boxes:
                            # 클래스 이름과 신뢰도 추출
                            class_id = int(box.cls)
                            conf = float(box.conf)
                            label = model.names[class_id] if hasattr(model, 'names') else str(class_id)
                            
                            # VCP 패턴 감지
                            if 'vcp' in label.lower() and conf >= 0.5:
                                vcp_detected = True
                                vcp_confidence = max(vcp_confidence, conf)
                            
                            # Cup & Handle 패턴 감지
                            if any(keyword in label.lower() for keyword in ['cup', 'handle']) and conf >= 0.5:
                                cup_detected = True
                                cup_confidence = max(cup_confidence, conf)
                
                result = {
                    'symbol': symbol,
                    'has_image': True,
                    'vcp_detected': vcp_detected,
                    'vcp_confidence': round(vcp_confidence, 3),
                    'cup_handle_detected': cup_detected,
                    'cup_handle_confidence': round(cup_confidence, 3),
                    'detection_date': datetime.now().strftime('%Y-%m-%d')
                }
                
                logger.info(f"{symbol} 패턴 감지 완료: VCP:{vcp_detected}({vcp_confidence:.3f}), Cup&Handle:{cup_detected}({cup_confidence:.3f})")
                return result
                
            except ImportError as import_err:
                logger.error(f"ultralytics 라이브러리가 설치되지 않음: {import_err}")
                raise Exception(f"ultralytics 라이브러리 설치 필요: {import_err}")
            
            except Exception as ai_error:
                logger.error(f"AI 모델 실행 중 오류: {ai_error}")
                raise Exception(f"YOLOv8 모델 실행 중 오류 발생: {ai_error}")
            
        except Exception as e:
            logger.error(f"{symbol} 패턴 감지 실패: {e}")
            return {
                'symbol': symbol,
                'has_image': False,
                'vcp_detected': False,
                'vcp_confidence': 0.0,
                'cup_handle_detected': False,
                'cup_handle_confidence': 0.0,
                'detection_date': datetime.now().strftime('%Y-%m-%d'),
                'error': str(e)
            }
            
    def process_symbol(self, symbol: str, skip_data: bool = False) -> Dict[str, any]:
        """
        개별 심볼 처리
        
        Args:
            symbol: 주식 심볼
            skip_data: 데이터 수집 건너뛰기
            
        Returns:
            처리 결과
        """
        logger.info(f"처리 시작: {symbol}")
        
        if not skip_data:
            # 1. OHLCV 데이터 가져오기
            data = self.fetch_ohlcv_data(symbol)
            if data is None:
                return {
                    'symbol': symbol,
                    'has_image': False,
                    'vcp_detected': False,
                    'vcp_confidence': 0.0,
                    'cup_handle_detected': False,
                    'cup_handle_confidence': 0.0,
                    'detection_date': datetime.now().strftime('%Y-%m-%d'),
                    'error': 'Data fetch failed'
                }
            
            # 2. 차트 이미지 생성
            image_success = self.generate_chart_image(symbol, data)
            if not image_success:
                return {
                    'symbol': symbol,
                    'has_image': False,
                    'vcp_detected': False,
                    'vcp_confidence': 0.0,
                    'cup_handle_detected': False,
                    'cup_handle_confidence': 0.0,
                    'detection_date': datetime.now().strftime('%Y-%m-%d'),
                    'error': 'Image generation failed'
                }
        
        # 3. AI 패턴 감지
        result = self.detect_pattern_with_ai(symbol)
        return result
        
    def run_image_pattern_detection(self, skip_data: bool = False) -> pd.DataFrame:
        """
        이미지 기반 패턴 감지 실행
        
        Args:
            skip_data: 데이터 수집 건너뛰기
            
        Returns:
            결과 DataFrame
        """
        logger.info("🖼️ 이미지 기반 패턴 감지 시작")
        
        # advanced_financial_results.csv에서 티커 목록 가져오기
        advanced_results_path = os.path.join(MARKMINERVINI_RESULTS_DIR, 'advanced_financial_results.csv')
        
        if not os.path.exists(advanced_results_path):
            logger.error(f"Advanced financial results 파일을 찾을 수 없습니다: {advanced_results_path}")
            return pd.DataFrame()
            
        try:
            advanced_df = pd.read_csv(advanced_results_path)
            symbols = advanced_df['symbol'].tolist()
            logger.info(f"처리할 심볼 수: {len(symbols)}")
            
        except Exception as e:
            logger.error(f"Advanced financial results 파일 읽기 실패: {e}")
            return pd.DataFrame()
        
        # 각 심볼 처리
        results = []
        total_symbols = len(symbols)
        
        for i, symbol in enumerate(symbols, 1):
            try:
                logger.info(f"진행률: {i}/{total_symbols} ({i/total_symbols*100:.1f}%) - {symbol}")
                result = self.process_symbol(symbol, skip_data)
                results.append(result)
                
            except Exception as e:
                logger.error(f"{symbol} 처리 중 오류: {e}")
                results.append({
                    'symbol': symbol,
                    'has_image': False,
                    'vcp_detected': False,
                    'vcp_confidence': 0.0,
                    'cup_handle_detected': False,
                    'cup_handle_confidence': 0.0,
                    'detection_date': datetime.now().strftime('%Y-%m-%d'),
                    'error': str(e)
                })
        
        # 결과 DataFrame 생성
        results_df = pd.DataFrame(results)
        
        # 결과 저장
        self.save_results(results_df)
        
        # 통계 출력
        self.print_statistics(results_df)
        
        return results_df
        
    def save_results(self, results_df: pd.DataFrame):
        """
        결과를 CSV와 JSON으로 저장
        
        Args:
            results_df: 결과 DataFrame
        """
        try:
            # CSV 저장
            results_df.to_csv(IMAGE_PATTERN_RESULTS_CSV, index=False, encoding='utf-8-sig')
            logger.info(f"CSV 결과 저장: {IMAGE_PATTERN_RESULTS_CSV}")
            
            # JSON 저장
            results_dict = results_df.to_dict('records')
            with open(IMAGE_PATTERN_RESULTS_JSON, 'w', encoding='utf-8') as f:
                json.dump(results_dict, f, indent=2, ensure_ascii=False)
            logger.info(f"JSON 결과 저장: {IMAGE_PATTERN_RESULTS_JSON}")
            
        except Exception as e:
            logger.error(f"결과 저장 실패: {e}")
            
    def print_statistics(self, results_df: pd.DataFrame):
        """
        통계 정보 출력
        
        Args:
            results_df: 결과 DataFrame
        """
        total_count = len(results_df)
        image_count = results_df['has_image'].sum()
        vcp_count = results_df['vcp_detected'].sum()
        cup_count = results_df['cup_handle_detected'].sum()
        both_count = (results_df['vcp_detected'] & results_df['cup_handle_detected']).sum()
        error_count = results_df['error'].notna().sum()
        
        print("\n" + "="*60)
        print("🖼️ 이미지 기반 패턴 감지 결과 통계")
        print("="*60)
        print(f"📊 전체 처리 심볼 수: {total_count:,}")
        print(f"🖼️ 이미지 생성 성공: {image_count:,} ({image_count/total_count*100:.1f}%)")
        print(f"📈 VCP 패턴 감지: {vcp_count:,} ({vcp_count/total_count*100:.1f}%)")
        print(f"☕ Cup&Handle 패턴 감지: {cup_count:,} ({cup_count/total_count*100:.1f}%)")
        print(f"🎯 두 패턴 모두 감지: {both_count:,} ({both_count/total_count*100:.1f}%)")
        print(f"❌ 오류 발생: {error_count:,} ({error_count/total_count*100:.1f}%)")
        print("="*60)
        
        if vcp_count > 0:
            print("\n🔍 VCP 패턴 감지된 상위 10개 심볼:")
            vcp_symbols = results_df[results_df['vcp_detected']].nlargest(10, 'vcp_confidence')
            for _, row in vcp_symbols.iterrows():
                print(f"  {row['symbol']}: {row['vcp_confidence']:.3f}")
                
        if cup_count > 0:
            print("\n☕ Cup&Handle 패턴 감지된 상위 10개 심볼:")
            cup_symbols = results_df[results_df['cup_handle_detected']].nlargest(10, 'cup_handle_confidence')
            for _, row in cup_symbols.iterrows():
                print(f"  {row['symbol']}: {row['cup_handle_confidence']:.3f}")

def run_image_pattern_detection(skip_data: bool = False) -> pd.DataFrame:
    """
    이미지 기반 패턴 감지 실행 함수
    
    Args:
        skip_data: 데이터 수집 건너뛰기
        
    Returns:
        결과 DataFrame
    """
    detector = ImagePatternDetector()
    return detector.run_image_pattern_detection(skip_data)

if __name__ == "__main__":
    # 테스트 실행
    results = run_image_pattern_detection()
    print(f"\n처리 완료: {len(results)}개 심볼")