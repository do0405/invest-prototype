"""SEC EDGAR 데이터 소스"""

from typing import List, Dict, Any
import logging
import requests
import json
from datetime import datetime, timedelta
from .base_source import BaseIPODataSource

logger = logging.getLogger(__name__)

class SecEdgarSource(BaseIPODataSource):
    """SEC EDGAR를 사용한 IPO 데이터 소스"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.base_url = "https://data.sec.gov/api/xbrl"
        self.headers = {
            'User-Agent': 'IPO Data Collector (contact@example.com)',
            'Accept': 'application/json'
        }
    
    def is_available(self) -> bool:
        """SEC EDGAR API 사용 가능 여부 확인"""
        try:
            # SEC API 연결 테스트
            test_url = "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json"
            response = requests.get(test_url, headers=self.headers, timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"SEC EDGAR API 연결 실패: {e}")
            return False
        
    def get_recent_ipos(self, months_back: int = 3) -> List[Dict[str, Any]]:
        """SEC EDGAR를 통한 최근 IPO 데이터 수집"""
        recent_ipos = []
        
        try:
            # SEC EDGAR에서 최근 S-1 및 424B4 폼 검색
            # 이는 IPO 관련 주요 폼들입니다
            search_url = "https://efts.sec.gov/LATEST/search-index"
            
            # 최근 3개월간의 IPO 관련 폼 검색
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30 * months_back)
            
            params = {
                'dateRange': 'custom',
                'startdt': start_date.strftime('%Y-%m-%d'),
                'enddt': end_date.strftime('%Y-%m-%d'),
                'forms': ['S-1', '424B4', 'S-1/A'],
                'count': 100
            }
            
            self._safe_request_delay()
            response = requests.get(search_url, params=params, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if 'hits' in data and 'hits' in data['hits']:
                    for hit in data['hits']['hits']:
                        source_data = hit.get('_source', {})
                        
                        # IPO 데이터 추출
                        ipo_data = {
                            'symbol': self._extract_ticker(source_data),
                            'company_name': source_data.get('display_names', [''])[0],
                            'ipo_date': source_data.get('file_date', ''),
                            'price_range': 'N/A',  # SEC 폼에서 추출 필요
                            'shares_offered': 'N/A',
                            'exchange': 'N/A',
                            'type': 'IPO',
                            'source': 'sec_edgar',
                            'form_type': source_data.get('form', ''),
                            'cik': source_data.get('ciks', [''])[0]
                        }
                        
                        if ipo_data['company_name']:  # 회사명이 있는 경우만 추가
                            recent_ipos.append(ipo_data)
                            
        except Exception as e:
            logger.error(f"SEC EDGAR 최근 IPO 데이터 수집 실패: {e}")
        
        logger.info(f"SEC EDGAR에서 총 {len(recent_ipos)}개 최근 IPO 데이터 수집")
        return recent_ipos
        
    def get_upcoming_ipos(self, months_ahead: int = 3) -> List[Dict[str, Any]]:
        """SEC EDGAR를 통한 예정된 IPO 데이터 수집"""
        # SEC EDGAR는 과거 데이터만 제공하므로 예정된 IPO는 제공하지 않음
        logger.info("SEC EDGAR는 예정된 IPO 데이터를 제공하지 않습니다.")
        return []
        
    def _extract_ticker(self, source_data: Dict) -> str:
        """SEC 데이터에서 티커 심볼 추출"""
        # 티커는 보통 파일명이나 회사명에서 추출해야 함
        tickers = source_data.get('tickers', [])
        if tickers:
            return tickers[0]
        return 'N/A'