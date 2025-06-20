"""데이터 소스 통합 관리 모듈

모든 외부 데이터 소스를 통합적으로 관리하고 스크리너에서 사용할 수 있도록 합니다.
"""

import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Tuple
import os
import json

from .ipo_data_collector import IPODataCollector
from .institutional_data_collector import InstitutionalDataCollector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataManager:
    """데이터 소스 통합 관리자"""
    
    def __init__(self, cache_dir: str = "data/cache"):
        self.cache_dir = cache_dir
        self.ipo_collector = IPODataCollector()
        self.institutional_collector = InstitutionalDataCollector()
        
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
        
        # 캐시 확인
        if use_cache and os.path.exists(cache_file):
            cache_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_file))
            if cache_age < timedelta(hours=6):  # 6시간 이내 캐시 사용
                logger.info("캐시된 IPO 데이터 사용")
                return pd.read_csv(cache_file)
        
        # 새로운 데이터 수집
        logger.info("새로운 IPO 데이터 수집")
        ipo_data = self.ipo_collector.get_recent_ipos(days_back)
        
        # 캐시 저장
        if not ipo_data.empty:
            ipo_data.to_csv(cache_file, index=False)
            logger.info(f"IPO 데이터 캐시 저장: {cache_file}")
        
        return ipo_data
    
    def get_institutional_data(self, symbol: str, use_cache: bool = True) -> Tuple[pd.DataFrame, Dict]:
        """기관 투자자 데이터를 가져옵니다.
        
        Args:
            symbol: 종목 심볼
            use_cache: 캐시 사용 여부
            
        Returns:
            (기관 보유 데이터, 자금 흐름 분석)
        """
        cache_file = os.path.join(self.cache_dir, f"institutional_{symbol}.json")
        
        # 캐시 확인
        if use_cache and os.path.exists(cache_file):
            cache_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(cache_file))
            if cache_age < timedelta(hours=12):  # 12시간 이내 캐시 사용
                logger.info(f"캐시된 {symbol} 기관 데이터 사용")
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                holdings_df = pd.DataFrame(cached_data['holdings'])
                flow_data = cached_data['flow_analysis']
                return holdings_df, flow_data
        
        # 새로운 데이터 수집
        logger.info(f"{symbol} 기관 데이터 수집")
        holdings = self.institutional_collector.get_institutional_holdings(symbol)
        flow_analysis = self.institutional_collector.get_institutional_flow(symbol)
        
        # 캐시 저장
        if not holdings.empty:
            cache_data = {
                'holdings': holdings.to_dict('records'),
                'flow_analysis': flow_analysis,
                'timestamp': datetime.now().isoformat()
            }
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            logger.info(f"{symbol} 기관 데이터 캐시 저장: {cache_file}")
        
        return holdings, flow_analysis
    
    def check_institutional_buying_streak(self, symbol: str, min_days: int = 3) -> bool:
        """기관 연속 매수 확인
        
        Args:
            symbol: 종목 심볼
            min_days: 최소 연속 일수
            
        Returns:
            연속 매수 조건 만족 여부
        """
        try:
            _, flow_analysis = self.get_institutional_data(symbol)
            
            if not flow_analysis:
                return False
            
            consecutive_buying = flow_analysis.get('consecutive_buying_days', 0)
            net_flow = flow_analysis.get('net_institutional_flow', 0)
            
            return consecutive_buying >= min_days and net_flow > 0
            
        except Exception as e:
            logger.error(f"{symbol} 기관 매수 확인 중 오류: {e}")
            return False
    
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
            
            # 성과 분석
            performance = self.ipo_collector.get_ipo_performance(symbol, ipo_date)
            
            # 기관 투자자 관심도
            holdings, flow_analysis = self.get_institutional_data(symbol)
            
            analysis = {
                'ipo_info': ipo_info.iloc[0].to_dict(),
                'performance': performance,
                'institutional_interest': {
                    'total_institutions': len(holdings) if not holdings.empty else 0,
                    'institutional_ownership': holdings['Ownership_Percentage'].sum() if not holdings.empty else 0,
                    'recent_flow': flow_analysis.get('net_institutional_flow', 0)
                }
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"{symbol} IPO 분석 중 오류: {e}")
            return {}
    
    def update_all_cache(self) -> None:
        """모든 캐시를 업데이트합니다."""
        logger.info("전체 캐시 업데이트 시작")
        
        try:
            # IPO 데이터 업데이트
            self.get_ipo_data(use_cache=False)
            
            # 주요 종목들의 기관 데이터 업데이트
            major_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA']
            for symbol in major_symbols:
                self.get_institutional_data(symbol, use_cache=False)
            
            logger.info("전체 캐시 업데이트 완료")
            
        except Exception as e:
            logger.error(f"캐시 업데이트 중 오류: {e}")
    
    def clear_cache(self, older_than_hours: int = 24) -> None:
        """오래된 캐시 파일을 삭제합니다.
        
        Args:
            older_than_hours: 삭제할 캐시 파일의 최소 나이 (시간)
        """
        try:
            cutoff_time = datetime.now() - timedelta(hours=older_than_hours)
            
            for filename in os.listdir(self.cache_dir):
                file_path = os.path.join(self.cache_dir, filename)
                if os.path.isfile(file_path):
                    file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                    if file_time < cutoff_time:
                        os.remove(file_path)
                        logger.info(f"오래된 캐시 파일 삭제: {filename}")
            
        except Exception as e:
            logger.error(f"캐시 정리 중 오류: {e}")
    
    def get_data_status(self) -> Dict:
        """데이터 소스 상태를 확인합니다."""
        status = {
            'ipo_data': {
                'available': False,
                'last_update': None,
                'record_count': 0
            },
            'institutional_data': {
                'cached_symbols': [],
                'cache_files': 0
            },
            'cache_directory': self.cache_dir,
            'cache_size_mb': 0
        }
        
        try:
            # IPO 데이터 상태
            ipo_cache = os.path.join(self.cache_dir, "ipo_data_365days.csv")
            if os.path.exists(ipo_cache):
                status['ipo_data']['available'] = True
                status['ipo_data']['last_update'] = datetime.fromtimestamp(
                    os.path.getmtime(ipo_cache)
                ).isoformat()
                ipo_df = pd.read_csv(ipo_cache)
                status['ipo_data']['record_count'] = len(ipo_df)
            
            # 기관 데이터 상태
            institutional_files = [f for f in os.listdir(self.cache_dir) 
                                 if f.startswith('institutional_') and f.endswith('.json')]
            status['institutional_data']['cache_files'] = len(institutional_files)
            status['institutional_data']['cached_symbols'] = [
                f.replace('institutional_', '').replace('.json', '') 
                for f in institutional_files
            ]
            
            # 캐시 크기 계산
            total_size = sum(
                os.path.getsize(os.path.join(self.cache_dir, f))
                for f in os.listdir(self.cache_dir)
                if os.path.isfile(os.path.join(self.cache_dir, f))
            )
            status['cache_size_mb'] = round(total_size / (1024 * 1024), 2)
            
        except Exception as e:
            logger.error(f"데이터 상태 확인 중 오류: {e}")
        
        return status

# 사용 예시
if __name__ == "__main__":
    manager = DataManager()
    
    # 데이터 상태 확인
    status = manager.get_data_status()
    print("데이터 소스 상태:")
    print(json.dumps(status, indent=2, ensure_ascii=False))
    
    # IPO 데이터 가져오기
    ipo_data = manager.get_ipo_data()
    print(f"\nIPO 데이터: {len(ipo_data)}개 종목")
    
    # 기관 투자자 데이터 가져오기
    symbol = "AAPL"
    holdings, flow = manager.get_institutional_data(symbol)
    print(f"\n{symbol} 기관 보유: {len(holdings)}개 기관")
    
    # 기관 연속 매수 확인
    buying_streak = manager.check_institutional_buying_streak(symbol)
    print(f"{symbol} 기관 연속 매수: {buying_streak}")