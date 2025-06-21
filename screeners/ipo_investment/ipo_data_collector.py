#!/usr/bin/env python3
"""
고급 IPO 데이터 수집기 - 실제 데이터 수집

이 스크립트는 finance_calendars와 investpy 라이브러리를 사용하여
실제 IPO 데이터를 수집합니다.

주요 기능:
- NASDAQ API를 통한 실제 IPO 데이터 수집 (finance_calendars)
- Investing.com을 통한 추가 IPO 정보 수집 (investpy)
- 과거 및 예정된 IPO 데이터 모두 수집
- CSV 및 JSON 형식으로 저장
- 재시도 로직 및 오류 처리
"""

import logging
import os
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import pandas as pd
import requests
from pathlib import Path

# 외부 라이브러리 import
try:
    import finance_calendars.finance_calendars as fc
except ImportError:
    fc = None
    logging.warning("finance_calendars 라이브러리를 찾을 수 없습니다.")

try:
    import warnings
    # pkg_resources 경고 억제
    warnings.filterwarnings("ignore", message="pkg_resources is deprecated")
    import investpy
except ImportError:
    investpy = None
    logging.warning("investpy 라이브러리를 찾을 수 없습니다.")

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RealIPODataCollector:
    """실제 IPO 데이터 수집기"""
    
    def __init__(self, data_dir: str = "../../data/IPO"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # API 호출 제한
        self.request_delay = 1.0  # 초
        self.max_retries = 3
        
        logger.info("실제 IPO 데이터 수집기 초기화 완료")
    
    def _safe_request_delay(self):
        """API 호출 간 안전한 지연"""
        time.sleep(self.request_delay)
    
    def get_recent_ipos_finance_calendars(self, months_back: int = 3) -> List[Dict[str, Any]]:
        """finance_calendars를 사용하여 최근 IPO 데이터 수집"""
        if not fc:
            logger.warning("finance_calendars 라이브러리가 없어 건너뜁니다.")
            return []
        
        recent_ipos = []
        current_date = datetime.now()
        
        try:
            # Get recent IPOs from finance_calendars
            recent_ipos_df = fc.get_priced_ipos_this_month()
            
            if not recent_ipos_df.empty:
                for _, ipo in recent_ipos_df.iterrows():
                    ipo_data = {
                        'ticker': '',  # ticker not available in this API
                        'company_name': ipo.get('companyName', ''),
                        'date': ipo.get('pricedDate', ''),
                        'price_range': f"${ipo.get('proposedSharePrice', '')}",
                        'volume': ipo.get('sharesOffered', ''),
                        'exchange': ipo.get('proposedExchange', ''),
                        'sector': '',  # sector not available in this API
                        'source': 'finance_calendars'
                    }
                    recent_ipos.append(ipo_data)
                
        except Exception as e:
            print(f"Error getting recent IPOs from finance_calendars: {e}")
            
        try:
            # Get filed IPOs from finance_calendars (as additional recent data)
            filed_ipos_df = fc.get_filed_ipos_this_month()
            
            if not filed_ipos_df.empty:
                for _, ipo in filed_ipos_df.iterrows():
                    ipo_data = {
                        'ticker': '',  # ticker not available in this API
                        'company_name': ipo.get('companyName', ''),
                        'date': ipo.get('filedDate', ''),
                        'price_range': f"${ipo.get('proposedSharePrice', '')}",
                        'volume': ipo.get('sharesOffered', ''),
                        'exchange': ipo.get('proposedExchange', ''),
                        'sector': '',  # sector not available in this API
                        'source': 'finance_calendars_filed'
                    }
                    recent_ipos.append(ipo_data)
                
        except Exception as e:
            print(f"Error getting filed IPOs from finance_calendars: {e}")
        
        logger.info(f"finance_calendars에서 총 {len(recent_ipos)}개 최근 IPO 수집")
        return recent_ipos
    
    def get_upcoming_ipos_finance_calendars(self, months_ahead: int = 3) -> List[Dict[str, Any]]:
        """finance_calendars를 사용하여 예정된 IPO 데이터 수집"""
        if not fc:
            logger.warning("finance_calendars 라이브러리가 없어 건너뜁니다.")
            return []
        
        upcoming_ipos = []
        current_date = datetime.now()
        
        try:
            # Get upcoming IPOs from finance_calendars
            upcoming_ipos_df = fc.get_upcoming_ipos_this_month()
            
            if not upcoming_ipos_df.empty:
                for _, ipo in upcoming_ipos_df.iterrows():
                    ipo_data = {
                        'ticker': '',  # ticker not available in this API
                        'company_name': ipo.get('companyName', ''),
                        'date': ipo.get('expectedPriceDate', ''),
                        'price_range': f"${ipo.get('proposedSharePrice', '')}",
                        'volume': ipo.get('sharesOffered', ''),
                        'exchange': ipo.get('proposedExchange', ''),
                        'underwriters': ipo.get('underwriters', ''),
                        'source': 'finance_calendars'
                    }
                    upcoming_ipos.append(ipo_data)
                
        except Exception as e:
            print(f"Error getting upcoming IPOs from finance_calendars: {e}")
            
        try:
            # Get next month's upcoming IPOs as well
            next_month = datetime.now() + timedelta(days=30)
            upcoming_ipos_next_df = fc.get_upcoming_ipos_by_month(next_month)
            
            if not upcoming_ipos_next_df.empty:
                for _, ipo in upcoming_ipos_next_df.iterrows():
                    ipo_data = {
                        'ticker': '',  # ticker not available in this API
                        'company_name': ipo.get('companyName', ''),
                        'date': ipo.get('expectedPriceDate', ''),
                        'price_range': f"${ipo.get('proposedSharePrice', '')}",
                        'volume': ipo.get('sharesOffered', ''),
                        'exchange': ipo.get('proposedExchange', ''),
                        'underwriters': ipo.get('underwriters', ''),
                        'source': 'finance_calendars_next_month'
                    }
                    upcoming_ipos.append(ipo_data)
                
        except Exception as e:
            print(f"Error getting next month upcoming IPOs from finance_calendars: {e}")
        
        logger.info(f"finance_calendars에서 총 {len(upcoming_ipos)}개 upcoming IPO 수집")
        return upcoming_ipos
    
    def get_ipos_investpy(self) -> List[Dict[str, Any]]:
        """investpy를 사용하여 IPO 데이터 수집 (보조적)"""
        if not investpy:
            logger.warning("investpy 라이브러리가 없어 건너뜁니다.")
            return []
        
        ipos_data = []
        
        try:
            # investpy는 주로 주식 데이터에 특화되어 있어 IPO 전용 기능이 제한적
            # 대신 최근 상장된 주식들을 검색하여 IPO 정보를 추론
            self._safe_request_delay()
            
            # 미국 주식 중 최근 상장된 것들 검색
            search_results = investpy.search_quotes(
                text='IPO', 
                products=['stocks'], 
                countries=['united states'], 
                n_results=10
            )
            
            if isinstance(search_results, list):
                for result in search_results:
                    try:
                        info = result.retrieve_information()
                        ipo_data = {
                            'symbol': result.symbol,
                            'company_name': result.name,
                            'ipo_date': 'N/A',  # investpy에서 직접 IPO 날짜 제공 안함
                            'price_range': 'N/A',
                            'shares_offered': 0,
                            'estimated_market_cap': 0,
                            'exchange': getattr(result, 'exchange', 'N/A'),
                            'sector': info.get('Sector', 'N/A') if info else 'N/A',
                            'source': 'investpy'
                        }
                        ipos_data.append(ipo_data)
                    except Exception as e:
                        logger.warning(f"investpy 개별 데이터 처리 실패: {e}")
                        continue
            
        except Exception as e:
            logger.error(f"investpy IPO 데이터 수집 실패: {e}")
        
        logger.info(f"investpy에서 총 {len(ipos_data)}개 IPO 관련 데이터 수집")
        return ipos_data
    
    def _clean_and_deduplicate(self, ipos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """IPO 데이터 정리 및 중복 제거"""
        if not ipos:
            return []
        
        # 심볼 기준으로 중복 제거 (우선순위: finance_calendars > investpy)
        seen_symbols = set()
        cleaned_ipos = []
        
        # finance_calendars 데이터 우선 처리
        for ipo in ipos:
            symbol = ipo.get('symbol', 'N/A')
            if symbol != 'N/A' and symbol not in seen_symbols:
                seen_symbols.add(symbol)
                cleaned_ipos.append(ipo)
            elif symbol == 'N/A':
                # 심볼이 없는 경우 회사명으로 중복 체크
                company_name = ipo.get('company_name', 'N/A')
                if company_name not in [c.get('company_name') for c in cleaned_ipos]:
                    cleaned_ipos.append(ipo)
        
        logger.info(f"중복 제거 후 {len(cleaned_ipos)}개 IPO 데이터")
        return cleaned_ipos
    
    def _save_to_files(self, data: List[Dict[str, Any]], file_prefix: str) -> Dict[str, str]:
        """데이터를 CSV와 JSON 파일로 저장"""
        if not data:
            logger.warning(f"{file_prefix} 데이터가 비어있어 파일을 저장하지 않습니다.")
            return {}
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # CSV 파일 저장
        csv_filename = f"{file_prefix}_{timestamp}.csv"
        csv_path = self.data_dir / csv_filename
        
        df = pd.DataFrame(data)
        df.to_csv(csv_path, index=False, encoding='utf-8')
        logger.info(f"CSV 파일 저장 완료: {csv_path} ({len(data)}개 IPO)")
        
        # JSON 파일 저장
        json_filename = f"{file_prefix}_{timestamp}.json"
        json_path = self.data_dir / json_filename
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"JSON 파일 저장 완료: {json_path} ({len(data)}개 IPO)")
        
        return {
            'csv': str(csv_path),
            'json': str(json_path)
        }
    
    def collect_all_ipo_data(self) -> Dict[str, Any]:
        """모든 IPO 데이터 수집 및 저장"""
        logger.info("실제 IPO 데이터 수집 시작")
        
        # 최근 IPO 데이터 수집
        recent_ipos = []
        recent_ipos.extend(self.get_recent_ipos_finance_calendars())
        recent_ipos.extend(self.get_ipos_investpy())
        
        # 예정된 IPO 데이터 수집
        upcoming_ipos = self.get_upcoming_ipos_finance_calendars()
        
        # 데이터 정리
        recent_ipos = self._clean_and_deduplicate(recent_ipos)
        upcoming_ipos = self._clean_and_deduplicate(upcoming_ipos)
        
        # 파일 저장
        recent_files = self._save_to_files(recent_ipos, 'recent_ipos')
        upcoming_files = self._save_to_files(upcoming_ipos, 'upcoming_ipos')
        
        logger.info("전체 IPO 데이터 수집 및 저장 완료")
        
        return {
            'recent_ipos': recent_ipos,
            'upcoming_ipos': upcoming_ipos,
            'files': {
                'recent': recent_files,
                'upcoming': upcoming_files
            }
        }

    def get_recent_ipos(self, days_back: int = 365) -> pd.DataFrame:
        """최근 IPO 데이터를 파일에서 로드"""
        csv_files = sorted(self.data_dir.glob('recent_ipos_*.csv'))
        if not csv_files:
            return pd.DataFrame()

        df_list = []
        for file in csv_files:
            df = pd.read_csv(file)
            df.columns = [c.lower() for c in df.columns]
            if 'ticker' in df.columns and 'symbol' not in df.columns:
                df.rename(columns={'ticker': 'symbol'}, inplace=True)
            if 'date' in df.columns and 'ipo_date' not in df.columns:
                df.rename(columns={'date': 'ipo_date'}, inplace=True)
            if 'price_range' in df.columns and 'ipo_price' not in df.columns:
                df['ipo_price'] = (
                    df['price_range']
                    .astype(str)
                    .str.replace('$', '')
                    .str.split('-')
                    .str[0]
                    .str.replace(',', '')
                )
            df_list.append(df)

        df_all = pd.concat(df_list, ignore_index=True)
        if 'ipo_date' in df_all.columns:
            df_all['ipo_date'] = pd.to_datetime(df_all['ipo_date'], errors='coerce')
            cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=days_back)
            df_all = df_all[df_all['ipo_date'] >= cutoff]
        return df_all

def main():
    """메인 실행 함수"""
    collector = RealIPODataCollector()
    
    try:
        results = collector.collect_all_ipo_data()
        
        print("\n=== 수집 결과 ===")
        print(f"과거 IPO 데이터: {len(results['recent_ipos'])}개")
        print(f"예정된 IPO 데이터: {len(results['upcoming_ipos'])}개")
        
        # 샘플 데이터 출력
        if results['recent_ipos']:
            print("\n=== 최근 IPO 샘플 ===")
            for ipo in results['recent_ipos'][:3]:
                symbol = ipo.get('symbol', 'N/A')
                company = ipo.get('company_name', 'N/A')
                date = ipo.get('ipo_date', 'N/A')
                print(f"- {symbol}: {company} ({date})")
        
        if results['upcoming_ipos']:
            print("\n=== 예정된 IPO 샘플 ===")
            for ipo in results['upcoming_ipos'][:3]:
                symbol = ipo.get('symbol', 'N/A')
                company = ipo.get('company_name', 'N/A')
                date = ipo.get('expected_ipo_date', 'N/A')
                print(f"- {symbol}: {company} ({date})")
        
        # 저장된 파일 정보
        print("\n=== 저장된 파일 ===")
        files = results['files']
        if files.get('recent'):
            print(f"- recent_csv: {files['recent']['csv']}")
            print(f"- recent_json: {files['recent']['json']}")
        if files.get('upcoming'):
            print(f"- upcoming_csv: {files['upcoming']['csv']}")
            print(f"- upcoming_json: {files['upcoming']['json']}")
            
    except Exception as e:
        logger.error(f"IPO 데이터 수집 중 오류 발생: {e}")
        raise
