"""데이터 소스 통합 관리 모듈

모든 외부 데이터 소스를 통합적으로 관리하고 스크리너에서 사용할 수 있도록 합니다.
"""

import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple
import os
import json

from .ipo_data_collector import RealIPODataCollector

logger = logging.getLogger(__name__)

class DataManager:
    """데이터 소스 통합 관리자"""
    
    def __init__(self, cache_dir: str = "data/cache"):
        self.cache_dir = cache_dir
        self.ipo_collector = RealIPODataCollector()
        
        # 캐시 디렉토리 생성
        os.makedirs(cache_dir, exist_ok=True)
    
    def get_ipo_data(self, days_back: int = 365, use_cache: bool = True) -> pd.DataFrame:
        """IPO 데이터를 가져옵니다.
        
        Args:
            days_back: 조회할 과거 일수
            use_cache: 캐시 사용 여부
            
        Returns:
            IPO 데이터프레임
        """
        cache_file = os.path.join(self.cache_dir, f"ipo_data_{days_back}days.csv")
        
        # 캐시 확인 및 1년 이상 된 데이터 정리
        if use_cache and os.path.exists(cache_file):
            cache_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_file))
            if cache_age < timedelta(hours=6):  # 6시간 이내 캐시 사용
                logger.info("캐시된 IPO 데이터 사용")
                from utils.screener_utils import read_csv_flexible
                df = read_csv_flexible(cache_file, required_columns=['ipo_date'])
                if df is None:
                    logger.warning("캐시 파일 읽기 실패, 새로운 데이터 수집")
                else:
                    # 날짜 형식 정규화 및 UTC 변환
                    df['ipo_date'] = pd.to_datetime(df['ipo_date'], errors='coerce')
                    if df['ipo_date'].dt.tz is None:
                        df['ipo_date'] = df['ipo_date'].dt.tz_localize('UTC')
                    else:
                        df['ipo_date'] = df['ipo_date'].dt.tz_convert('UTC')
                    
                    # 1년 이상 된 데이터 제거
                    one_year_ago = pd.Timestamp.utcnow() - pd.Timedelta(days=365)
                    before_filter = len(df)
                    df = df[df['ipo_date'] >= one_year_ago]
                    after_filter = len(df)
                    
                    if before_filter > after_filter:
                        logger.info(f"캐시에서 1년 이상 된 IPO 데이터 {before_filter - after_filter}개 제거")
                        # 정리된 데이터로 캐시 업데이트
                        df.to_csv(cache_file, index=False)
                    
                    if not df.empty:
                        logger.info(f"캐시된 IPO 데이터 사용 (최근 1년 데이터 {len(df)}개)")
                return df
        
        # 새로운 데이터 수집 (실제 수집된 파일 우선 확인)
        logger.info("IPO 데이터 로드 시작")
        ipo_data = self.ipo_collector.get_recent_ipos(days_back)
        
        if ipo_data.empty:
            logger.warning("수집된 IPO 데이터가 없습니다. 새로운 데이터 수집을 시도합니다.")
            # 실제 데이터 수집 시도
            try:
                results = self.ipo_collector.collect_all_ipo_data()
                if results and results.get('recent_ipos'):
                    ipo_data = pd.DataFrame(results['recent_ipos'])
                    logger.info(f"새로운 IPO 데이터 수집 완료: {len(ipo_data)}개")
            except Exception as e:
                logger.error(f"IPO 데이터 수집 실패: {e}")
        else:
            logger.info(f"기존 IPO 데이터 로드 완료: {len(ipo_data)}개")
        if not ipo_data.empty:
            # 날짜 형식 정규화 및 UTC 변환
            ipo_data['ipo_date'] = pd.to_datetime(ipo_data['ipo_date'], errors='coerce')
            if ipo_data['ipo_date'].dt.tz is None:
                ipo_data['ipo_date'] = ipo_data['ipo_date'].dt.tz_localize('UTC')
            else:
                ipo_data['ipo_date'] = ipo_data['ipo_date'].dt.tz_convert('UTC')
        
        # 캐시 저장
        if not ipo_data.empty:
            ipo_data.to_csv(cache_file, index=False)
            logger.info(f"IPO 데이터 캐시 저장: {cache_file}")
        
        return ipo_data
    

    
    def get_ipo_analysis(self, symbol: str) -> Dict:
        """IPO 종목 분석 데이터를 가져옵니다.
        
        Args:
            symbol: IPO 종목 심볼
            
        Returns:
            IPO 분석 데이터
        """
        try:
            # IPO 데이터에서 해당 종목 찾기
            ipo_data = self.get_ipo_data()
            ipo_info = ipo_data[ipo_data['symbol'] == symbol]
            
            if ipo_info.empty:
                return {}
            
            ipo_date = ipo_info.iloc[0]['ipo_date']
            
            analysis = {
                'ipo_info': ipo_info.iloc[0].to_dict(),
                'ipo_date': ipo_date,
                'symbol': symbol
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"{symbol} IPO 분석 중 오류: {e}")
            return {}
    
