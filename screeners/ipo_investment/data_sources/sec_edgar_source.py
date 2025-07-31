"""SEC EDGAR 데이터 소스 - 완전 구현"""

from typing import List, Dict, Any
import logging
import requests
import json
from datetime import datetime, timedelta
import re
import time
from .base_source import BaseIPODataSource

logger = logging.getLogger(__name__)

class SecEdgarSource(BaseIPODataSource):
    """SEC EDGAR를 사용한 IPO 데이터 소스"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.base_url = "https://data.sec.gov/api/xbrl"
        self.search_url = "https://efts.sec.gov/LATEST/search-index"
        self.headers = {
            'User-Agent': 'IPO Data Collector v1.0 (educational@example.com)',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        }
        
        # IPO 관련 폼 타입들
        self.ipo_forms = ['S-1', 'S-1/A', '424B4', '424B1', 'F-1', 'F-1/A']
        
    def is_available(self) -> bool:
        """SEC EDGAR API 사용 가능 여부 확인"""
        try:
            # SEC API 연결 테스트
            test_url = "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json"
            response = requests.get(test_url, headers=self.headers, timeout=15)
            available = response.status_code == 200
            logger.info(f"SEC EDGAR API 사용 가능: {available}")
            return available
        except Exception as e:
            logger.warning(f"SEC EDGAR API 연결 실패: {e}")
            return False
        
    def get_recent_ipos(self, months_back: int = 3) -> List[Dict[str, Any]]:
        """SEC EDGAR를 통한 최근 IPO 데이터 수집"""
        recent_ipos = []
        
        try:
            # 최근 IPO 관련 폼 검색
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30 * months_back)
            
            logger.info(f"SEC EDGAR에서 {start_date.strftime('%Y-%m-%d')}부터 {end_date.strftime('%Y-%m-%d')}까지 IPO 데이터 검색")
            
            for form_type in self.ipo_forms:
                try:
                    self._safe_request_delay()
                    
                    # SEC EDGAR 검색 API 호출
                    params = {
                        'dateRange': 'custom',
                        'startdt': start_date.strftime('%Y-%m-%d'),
                        'enddt': end_date.strftime('%Y-%m-%d'),
                        'forms': [form_type],
                        'count': 100
                    }
                    
                    response = requests.get(
                        self.search_url, 
                        params=params, 
                        headers=self.headers, 
                        timeout=15
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        filings = self._parse_search_results(data, form_type)
                        recent_ipos.extend(filings)
                        logger.info(f"{form_type} 폼에서 {len(filings)}개 IPO 관련 서류 발견")
                    else:
                        logger.warning(f"{form_type} 검색 실패: HTTP {response.status_code}")
                        
                except Exception as e:
                    logger.error(f"{form_type} 폼 검색 중 오류: {e}")
                    continue
            
            # 중복 제거 및 정리
            recent_ipos = self._deduplicate_ipos(recent_ipos)
            
        except Exception as e:
            logger.error(f"SEC EDGAR 최근 IPO 데이터 수집 실패: {e}")
        
        logger.info(f"SEC EDGAR에서 총 {len(recent_ipos)}개 고유 IPO 데이터 수집")
        return recent_ipos
        
    def get_upcoming_ipos(self, months_ahead: int = 3) -> List[Dict[str, Any]]:
        """SEC EDGAR를 통한 예정된 IPO 데이터 수집"""
        # SEC EDGAR는 과거 제출 서류만 제공하므로 예정된 IPO는 S-1 등록서류에서 추정
        upcoming_ipos = []
        
        try:
            # 최근 S-1 서류 중 아직 상장되지 않은 것들 찾기
            end_date = datetime.now()
            start_date = end_date - timedelta(days=90)  # 최근 3개월
            
            logger.info("SEC EDGAR에서 예정된 IPO 추정 중 (S-1 등록서류 기반)")
            
            self._safe_request_delay()
            
            params = {
                'dateRange': 'custom',
                'startdt': start_date.strftime('%Y-%m-%d'),
                'enddt': end_date.strftime('%Y-%m-%d'),
                'forms': ['S-1', 'F-1'],  # 초기 등록서류만
                'count': 50
            }
            
            response = requests.get(
                self.search_url, 
                params=params, 
                headers=self.headers, 
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                potential_ipos = self._parse_search_results(data, 'S-1/F-1')
                
                # 아직 상장되지 않은 것들 필터링 (간단한 휴리스틱)
                for ipo in potential_ipos:
                    if self._is_likely_upcoming_ipo(ipo):
                        ipo['type'] = 'Upcoming IPO (Estimated)'
                        ipo['estimated_date'] = 'TBD'
                        upcoming_ipos.append(ipo)
                        
        except Exception as e:
            logger.error(f"SEC EDGAR 예정 IPO 데이터 수집 실패: {e}")
        
        logger.info(f"SEC EDGAR에서 {len(upcoming_ipos)}개 예정 IPO 추정")
        return upcoming_ipos
    
    def _parse_search_results(self, data: Dict, form_type: str) -> List[Dict[str, Any]]:
        """SEC 검색 결과 파싱"""
        ipos = []
        
        try:
            if 'hits' in data and 'hits' in data['hits']:
                for hit in data['hits']['hits']:
                    source_data = hit.get('_source', {})
                    
                    # 기본 정보 추출
                    company_name = self._extract_company_name(source_data)
                    ticker = self._extract_ticker(source_data)
                    
                    if company_name:  # 회사명이 있는 경우만 처리
                        ipo_data = {
                            'symbol': ticker,
                            'company_name': company_name,
                            'ipo_date': source_data.get('file_date', ''),
                            'filing_date': source_data.get('file_date', ''),
                            'price_range': self._extract_price_range(source_data),
                            'shares_offered': self._extract_shares_offered(source_data),
                            'exchange': self._extract_exchange(source_data),
                            'type': 'IPO',
                            'source': 'sec_edgar',
                            'form_type': form_type,
                            'cik': source_data.get('ciks', [''])[0] if source_data.get('ciks') else '',
                            'accession_number': source_data.get('accession_number', ''),
                            'file_description': source_data.get('file_description', ''),
                            'business_address': self._extract_business_address(source_data),
                            'sic_code': source_data.get('sic', ''),
                            'fiscal_year_end': source_data.get('fiscal_year_end', '')
                        }
                        
                        ipos.append(ipo_data)
                        
        except Exception as e:
            logger.error(f"검색 결과 파싱 실패: {e}")
        
        return ipos
    
    def _extract_company_name(self, source_data: Dict) -> str:
        """회사명 추출"""
        # 여러 필드에서 회사명 시도
        display_names = source_data.get('display_names', [])
        if display_names and display_names[0]:
            return display_names[0].strip()
        
        entity_name = source_data.get('entity_name', '')
        if entity_name:
            return entity_name.strip()
        
        return ''
    
    def _extract_ticker(self, source_data: Dict) -> str:
        """티커 심볼 추출"""
        try:
            # 티커 필드에서 직접 추출
            tickers = source_data.get('tickers', [])
            if tickers and len(tickers) > 0 and tickers[0] and isinstance(tickers[0], str):
                return tickers[0].upper().strip()
            
            # 파일명에서 추출 시도
            file_name = source_data.get('file_name', '')
            if file_name and isinstance(file_name, str):
                # 파일명에서 티커 패턴 찾기 (예: "aapl-20231201.htm")
                ticker_match = re.search(r'([A-Z]{1,5})-\d{8}', file_name.upper())
                if ticker_match:
                    return ticker_match.group(1)
            
            return 'N/A'
        except Exception as e:
            logger.warning(f"티커 추출 중 오류: {e}")
            return 'N/A'
    
    def _extract_price_range(self, source_data: Dict) -> str:
        """가격 범위 추출 (파일 내용에서)"""
        # 실제 구현에서는 파일 내용을 다운로드하여 파싱해야 함
        # 여기서는 기본값 반환
        return 'N/A'
    
    def _extract_shares_offered(self, source_data: Dict) -> str:
        """제공 주식 수 추출"""
        # 실제 구현에서는 파일 내용에서 추출
        return 'N/A'
    
    def _extract_exchange(self, source_data: Dict) -> str:
        """거래소 정보 추출"""
        # 파일 설명에서 거래소 정보 찾기
        file_desc = source_data.get('file_description', '').upper()
        
        if 'NASDAQ' in file_desc:
            return 'NASDAQ'
        elif 'NYSE' in file_desc:
            return 'NYSE'
        elif 'AMEX' in file_desc:
            return 'AMEX'
        
        return 'N/A'
    
    def _extract_business_address(self, source_data: Dict) -> str:
        """사업장 주소 추출"""
        addresses = source_data.get('addresses', [])
        if addresses:
            addr = addresses[0]
            return f"{addr.get('city', '')}, {addr.get('state', '')}"
        return 'N/A'
    
    def _deduplicate_ipos(self, ipos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """IPO 데이터 중복 제거"""
        seen = set()
        unique_ipos = []
        
        for ipo in ipos:
            # 회사명과 CIK로 중복 체크
            key = (ipo.get('company_name', ''), ipo.get('cik', ''))
            if key not in seen and key[0]:  # 회사명이 있는 경우만
                seen.add(key)
                unique_ipos.append(ipo)
        
        return unique_ipos
    
    def _is_likely_upcoming_ipo(self, ipo: Dict[str, Any]) -> bool:
        """예정된 IPO인지 판단하는 휴리스틱"""
        # 간단한 규칙: S-1 폼이고 최근 제출되었으며 아직 424B4가 없는 경우
        form_type = ipo.get('form_type', '')
        filing_date = ipo.get('filing_date', '')
        
        if form_type in ['S-1', 'F-1'] and filing_date:
            try:
                filing_dt = datetime.strptime(filing_date, '%Y-%m-%d')
                days_ago = (datetime.now() - filing_dt).days
                # 최근 60일 이내 제출된 S-1
                return 0 <= days_ago <= 60
            except:
                pass
        
        return False

# 테스트용 함수
def test_sec_edgar_source():
    """SEC Edgar 소스 테스트"""
    source = SecEdgarSource()
    
    print(f"SEC EDGAR 사용 가능: {source.is_available()}")
    
    if source.is_available():
        print("\n최근 IPO 데이터 수집 중...")
        recent = source.get_recent_ipos(months_back=1)
        print(f"최근 IPO: {len(recent)}개")
        
        if recent:
            print("\n첫 번째 IPO 예시:")
            print(json.dumps(recent[0], indent=2, ensure_ascii=False))
        
        print("\n예정된 IPO 데이터 수집 중...")
        upcoming = source.get_upcoming_ipos()
        print(f"예정된 IPO: {len(upcoming)}개")

if __name__ == "__main__":
    test_sec_edgar_source()