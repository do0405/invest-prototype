# -*- coding: utf-8 -*-
"""
IPO 데이터 수집기 - 개선된 버전
Finnhub API, Financial Modeling Prep API, SEC EDGAR API를 통해 IPO 데이터를 수집합니다.
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional, Union
import time
import os
import json
from config import IPO_DATA_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IPODataCollector:
    """개선된 IPO 데이터 수집기"""

    def __init__(self, finnhub_api_key: Optional[str] = None, fmp_api_key: Optional[str] = None):
        """
        Args:
            finnhub_api_key: Finnhub API 키 (무료 계정 가능)
            fmp_api_key: Financial Modeling Prep API 키 (무료 계정 가능)
        """
        self.finnhub_api_key = finnhub_api_key or os.getenv('FINNHUB_API_KEY')
        self.fmp_api_key = fmp_api_key or os.getenv('FMP_API_KEY')
        
        self.headers = {
            'User-Agent': 'Investment Research Tool (contact@example.com)'
        }
        
        # API 엔드포인트
        self.finnhub_base_url = "https://finnhub.io/api/v1"
        self.fmp_base_url = "https://financialmodelingprep.com/api/v3"
        self.sec_base_url = "https://www.sec.gov/files/company_tickers.json"

    def _safe_request(self, url: str, params: Optional[Dict] = None,
                      retries: int = 3, delay: float = 1.0) -> Optional[requests.Response]:
        """안전한 HTTP 요청 래퍼 (재시도 및 지연 포함)"""
        for attempt in range(1, retries + 1):
            try:
                resp = requests.get(url, headers=self.headers, params=params, timeout=10)
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                logger.warning(f"HTTP 요청 실패 (시도 {attempt}/{retries}): {e}")
                time.sleep(delay)
        return None
    
    def get_recent_ipos(self, days_back: int = 365) -> pd.DataFrame:
        """최근 IPO 목록을 가져옵니다.
        
        Args:
            days_back: 조회할 과거 일수
            
        Returns:
            IPO 데이터프레임 (symbol, company_name, ipo_date, ipo_price, market_cap, sector)
        """
        try:
            logger.info(f"최근 {days_back}일간 IPO 데이터 수집 시작")
            
            # 여러 소스에서 IPO 데이터 수집
            ipo_data = []
            
            # 1. NASDAQ 공식 API 또는 finance_calendars
            nasdaq_data = self._get_nasdaq_ipo_data(days_back)
            ipo_data.extend(nasdaq_data)
            logger.info(f"NASDAQ에서 {len(nasdaq_data)}개 IPO 수집")

            # 2. Investpy (Investing.com)
            investpy_data = self._get_investpy_ipo_data(days_back)
            ipo_data.extend(investpy_data)
            logger.info(f"Investing.com에서 {len(investpy_data)}개 IPO 수집")

            # 3. Finnhub IPO 캘린더
            if self.finnhub_api_key:
                finnhub_data = self._get_finnhub_ipo_data(days_back)
                ipo_data.extend(finnhub_data)
                logger.info(f"Finnhub에서 {len(finnhub_data)}개 IPO 수집")

            # 4. Financial Modeling Prep IPO 캘린더
            if self.fmp_api_key:
                fmp_data = self._get_fmp_ipo_data(days_back)
                ipo_data.extend(fmp_data)
                logger.info(f"FMP에서 {len(fmp_data)}개 IPO 수집")

            # 5. SEC EDGAR 데이터
            sec_data = self._get_sec_edgar_ipo_data(days_back)
            ipo_data.extend(sec_data)
            logger.info(f"SEC EDGAR에서 {len(sec_data)}개 IPO 수집")

            # 6. Yahoo Finance 데이터
            yahoo_data = self._get_yahoo_finance_ipo_data(days_back)
            ipo_data.extend(yahoo_data)
            logger.info(f"Yahoo Finance에서 {len(yahoo_data)}개 IPO 수집")
            
            # 중복 제거 및 데이터 정리
            if ipo_data:
                df = self._clean_and_deduplicate(ipo_data)
                logger.info(f"총 {len(df)}개의 고유 IPO 데이터 수집 완료")
                return df
            else:
                logger.warning("IPO 데이터 수집 실패, 대체 데이터 사용")
                return self._get_fallback_ipo_data()
                
        except Exception as e:
            logger.error(f"IPO 데이터 수집 중 오류: {e}")
            return self._get_fallback_ipo_data()
    
    def _get_finnhub_ipo_data(self, days_back: int) -> List[Dict]:
        """Finnhub API에서 IPO 캘린더 데이터 수집"""
        if not self.finnhub_api_key:
            return []
            
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            url = f"{self.finnhub_base_url}/calendar/ipo"
            params = {
                'token': self.finnhub_api_key,
                'from': start_date.strftime('%Y-%m-%d'),
                'to': end_date.strftime('%Y-%m-%d')
            }
            
            response = self._safe_request(url, params)
            if not response:
                return []
                
            data = response.json()
            ipos = []
            
            if 'ipoCalendar' in data:
                for ipo in data['ipoCalendar']:
                    try:
                        ipos.append({
                            'symbol': ipo.get('symbol', '').strip(),
                            'company_name': ipo.get('name', '').strip(),
                            'ipo_date': ipo.get('date', ''),
                            'ipo_price': self._parse_price_range(ipo.get('price', '0')),
                            'shares': ipo.get('numberOfShares', 0),
                            'market_cap': ipo.get('totalSharesValue', 0),
                            'source': 'finnhub'
                        })
                    except Exception as e:
                        logger.warning(f"Finnhub IPO 데이터 파싱 오류: {e}")
                        continue
            
            return ipos
            
        except Exception as e:
            logger.warning(f"Finnhub IPO 데이터 수집 실패: {e}")
            return []
    
    def _get_fmp_ipo_data(self, days_back: int) -> List[Dict]:
        """Financial Modeling Prep API에서 IPO 캘린더 데이터 수집"""
        if not self.fmp_api_key:
            return []
            
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            url = f"{self.fmp_base_url}/ipo_calendar"
            params = {
                'apikey': self.fmp_api_key,
                'from': start_date.strftime('%Y-%m-%d'),
                'to': end_date.strftime('%Y-%m-%d')
            }
            
            response = self._safe_request(url, params)
            if not response:
                return []
                
            data = response.json()
            ipos = []
            
            if isinstance(data, list):
                for ipo in data:
                    try:
                        ipos.append({
                            'symbol': ipo.get('symbol', '').strip(),
                            'company_name': ipo.get('company', '').strip(),
                            'ipo_date': ipo.get('date', ''),
                            'ipo_price': self._parse_price_range(ipo.get('price', '0')),
                            'shares': ipo.get('numberOfShares', 0),
                            'market_cap': 0,  # FMP에서 직접 제공하지 않음
                            'source': 'fmp'
                        })
                    except Exception as e:
                        logger.warning(f"FMP IPO 데이터 파싱 오류: {e}")
                        continue
            
            return ipos
            
        except Exception as e:
            logger.warning(f"FMP IPO 데이터 수집 실패: {e}")
            return []
    
    
        def _get_nasdaq_ipo_data(self, days_back: int) -> List[Dict]:
            """NASDAQ IPO 캘린더에서 데이터 수집."""
            try:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=days_back)

                try:
                    from finance_calendars import nasdaq as nasdaq_cal
                except Exception:
                    nasdaq_cal = None

                ipo_list: List[Dict] = []

                if nasdaq_cal and hasattr(nasdaq_cal, "get_ipo_calendar"):
                    try:
                        df = nasdaq_cal.get_ipo_calendar(start=start_date.date(), end=end_date.date())
                        for _, row in df.iterrows():
                            ipo_list.append({
                                'symbol': row.get('symbol', ''),
                                'company_name': row.get('company', ''),
                                'ipo_date': row.get('date', ''),
                                'ipo_price': self._parse_number(row.get('price', '0')),
                                'market_cap': 0,
                                'sector': row.get('sector', ''),
                                'industry': row.get('industry', ''),
                                'volume': 0,
                                'source': 'nasdaq_official'
                            })
                        return ipo_list
                    except Exception as e:
                        logger.warning(f"finance_calendars 사용 실패: {e}")

                url = "https://api.nasdaq.com/api/ipo/calendar"
                params = {'date': end_date.strftime('%Y-%m-%d')}
                resp = self._safe_request(url, params, retries=3, delay=2)
                if resp:
                    data = resp.json()
                    if 'data' in data and 'rows' in data['data']:
                        for row in data['data']['rows']:
                            try:
                                ipo_date = datetime.strptime(row.get('expectedPriceDate', ''), '%m/%d/%Y')
                                if start_date <= ipo_date <= end_date:
                                    ipo_list.append({
                                        'symbol': row.get('symbol', ''),
                                        'company_name': row.get('companyName', ''),
                                        'ipo_date': ipo_date.strftime('%Y-%m-%d'),
                                        'ipo_price': self._parse_number(row.get('proposedSharePrice', '0')),
                                        'market_cap': 0,
                                        'sector': '',
                                        'industry': '',
                                        'volume': 0,
                                        'source': 'nasdaq'
                                    })
                            except (ValueError, KeyError) as e:
                                logger.debug(f"NASDAQ 데이터 파싱 오류: {e}")
                                continue

                return ipo_list
            except Exception as e:
                logger.warning(f"NASDAQ IPO 데이터 수집 실패: {e}")

                return []
    def _get_investpy_ipo_data(self, days_back: int) -> List[Dict]:
        """Investing.com 데이터를 사용한 IPO 수집"""
        try:
            import investpy

            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days_back)

            df = investpy.ipo.get_ipo_calendar(from_date=start_date.strftime('%d/%m/%Y'),
                                               to_date=end_date.strftime('%d/%m/%Y'))
            ipo_list: List[Dict] = []
            for _, row in df.iterrows():
                ipo_list.append({
                    'symbol': row.get('symbol', ''),
                    'company_name': row.get('name', ''),
                    'ipo_date': row.get('date', ''),
                    'ipo_price': self._parse_number(row.get('price', 0)),
                    'market_cap': 0,
                    'sector': row.get('sector', ''),
                    'industry': row.get('industry', ''),
                    'volume': 0,
                    'source': 'investpy'
                })
            time.sleep(0.5)
            return ipo_list
        except Exception as e:
            logger.warning(f"Investpy IPO 데이터 수집 실패: {e}")
            return []
        """SEC EDGAR에서 최근 상장 기업 데이터 수집"""
        try:
            # SEC의 최근 등록 기업 목록
            url = "https://www.sec.gov/files/company_tickers.json"
            resp = self._safe_request(url)
            if not resp:
                return []
            
            companies = resp.json()
            ipo_list = []
            
            # 최근 등록된 기업들 중 일부를 IPO로 간주 (실제로는 더 정교한 필터링 필요)
            recent_companies = list(companies.values())[-50:]  # 최근 50개 기업
            
            for company in recent_companies:
                try:
                    ipo_list.append({
                        'symbol': company.get('ticker', ''),
                        'company_name': company.get('title', ''),
                        'ipo_date': (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'),  # 추정
                        'ipo_price': 0,  # 알 수 없음
                        'market_cap': 0,
                        'sector': '',
                        'industry': '',
                        'volume': 0,
                        'source': 'sec_edgar'
                    })
                except Exception as e:
                    logger.debug(f"SEC 데이터 파싱 오류: {e}")
                    continue
            
            return ipo_list[:10]  # 최대 10개만 반환
            
        except Exception as e:
            logger.warning(f"SEC EDGAR 데이터 수집 실패: {e}")
            return []
    
    def _get_yahoo_finance_ipo_data(self, days_back: int) -> List[Dict]:
        """Yahoo Finance에서 최근 상장 종목 추정"""
        try:
            import yfinance as yf
            
            # 알려진 최근 IPO 종목들
            recent_ipo_symbols = ['CRCL', 'VOYG', 'SOLV', 'KKVS', 'ARM', 'FRHC', 'CAVA']
            ipo_list = []
            
            for symbol in recent_ipo_symbols:
                try:
                    ticker = yf.Ticker(symbol)
                    info = ticker.info
                    
                    if info:
                        ipo_list.append({
                            'symbol': symbol,
                            'company_name': info.get('longName', symbol),
                            'ipo_date': '2024-01-01',  # 추정값
                            'ipo_price': info.get('regularMarketPrice', 0),
                            'market_cap': info.get('marketCap', 0),
                            'sector': info.get('sector', ''),
                            'industry': info.get('industry', ''),
                            'volume': info.get('volume', 0),
                            'source': 'yahoo_finance'
                        })
                        time.sleep(0.1)  # API 제한 방지
                except Exception as e:
                    logger.debug(f"Yahoo Finance {symbol} 데이터 수집 실패: {e}")
                    continue
            
            return ipo_list
            
        except ImportError:
            logger.warning("yfinance 라이브러리가 설치되지 않음")
            return []
        except Exception as e:
            logger.warning(f"Yahoo Finance IPO 데이터 수집 실패: {e}")
            return []
    
    def _parse_price_range(self, price_str: Union[str, float, int]) -> float:
        """가격 범위 문자열을 평균 가격으로 변환"""
        try:
            if isinstance(price_str, (int, float)):
                return float(price_str)
                
            import re
            # $15.00-$17.00 형태에서 숫자 추출
            numbers = re.findall(r'[\d.]+', str(price_str))
            if len(numbers) >= 2:
                return (float(numbers[0]) + float(numbers[1])) / 2
            elif len(numbers) == 1:
                return float(numbers[0])
        except Exception:
            pass
        return 0.0
    
    def _parse_number(self, num_str: Union[str, int, float]) -> int:
        """숫자 문자열을 정수로 변환"""
        try:
            if isinstance(num_str, (int, float)):
                return int(num_str)
                
            import re
            # 1,000,000 형태에서 숫자 추출
            clean_str = re.sub(r'[^\d.]', '', str(num_str))
            if clean_str:
                return int(float(clean_str))
        except Exception:
            pass
        return 0
    
    def _clean_and_deduplicate(self, ipo_data: List[Dict]) -> pd.DataFrame:
        """IPO 데이터 정리 및 중복 제거"""
        if not ipo_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(ipo_data)
        
        # 필수 컬럼 확인
        required_columns = ['symbol', 'company_name', 'ipo_date', 'ipo_price']
        for col in required_columns:
            if col not in df.columns:
                df[col] = ''
        
        # 빈 심볼 제거
        df = df[df['symbol'].str.strip() != '']
        
        # 날짜 형식 통일
        df['ipo_date'] = pd.to_datetime(df['ipo_date'], errors='coerce')
        df = df.dropna(subset=['ipo_date'])
        
        # 중복 제거 (심볼 기준)
        df = df.drop_duplicates(subset=['symbol'], keep='first')
        
        # 날짜순 정렬
        df = df.sort_values('ipo_date', ascending=False)
        
        # 추가 컬럼 생성
        if 'sector' not in df.columns:
            df['sector'] = 'Unknown'
        if 'industry' not in df.columns:
            df['industry'] = 'Unknown'
        
        return df.reset_index(drop=True)
    
    def _get_fallback_ipo_data(self) -> pd.DataFrame:
        """대체 IPO 데이터 - 빈 데이터프레임 반환"""
        logger.warning("모든 IPO 데이터 소스에서 데이터 수집 실패")
        
        # 빈 데이터프레임을 올바른 컬럼 구조로 반환
        columns = ['symbol', 'company_name', 'ipo_date', 'ipo_price', 'market_cap', 'sector', 'industry', 'volume', 'source']
        return pd.DataFrame(columns=columns)
    
    def save_ipo_data(self, ipo_data: pd.DataFrame, base_filename: str = "ipo_data") -> Dict[str, str]:
        """IPO 데이터를 JSON과 CSV 형태로 저장합니다.
        
        Args:
            ipo_data: 저장할 IPO 데이터프레임
            base_filename: 기본 파일명 (확장자 제외)
            
        Returns:
            저장된 파일 경로들을 담은 딕셔너리
        """
        try:
            # 저장 디렉터리 생성
            save_dir = IPO_DATA_DIR
            os.makedirs(save_dir, exist_ok=True)
            
            # 타임스탬프 추가
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # 파일 경로 설정
            csv_filename = f"{base_filename}_{timestamp}.csv"
            json_filename = f"{base_filename}_{timestamp}.json"
            
            csv_path = os.path.join(save_dir, csv_filename)
            json_path = os.path.join(save_dir, json_filename)
            
            # CSV 저장
            ipo_data.to_csv(csv_path, index=False, encoding='utf-8-sig')
            logger.info(f"CSV 파일 저장 완료: {csv_path}")
            
            # JSON 저장
            # 날짜 컬럼을 문자열로 변환
            json_data = ipo_data.copy()
            if 'ipo_date' in json_data.columns:
                json_data['ipo_date'] = json_data['ipo_date'].astype(str)
            
            # JSON 형태로 변환
            json_dict = {
                'metadata': {
                    'collection_date': datetime.now().isoformat(),
                    'total_records': len(ipo_data),
                    'data_sources': list(ipo_data['source'].unique()) if 'source' in ipo_data.columns else [],
                    'date_range': {
                        'earliest_ipo': str(ipo_data['ipo_date'].min()) if 'ipo_date' in ipo_data.columns and not ipo_data.empty else None,
                        'latest_ipo': str(ipo_data['ipo_date'].max()) if 'ipo_date' in ipo_data.columns and not ipo_data.empty else None
                    }
                },
                'data': json_data.to_dict('records')
            }
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(json_dict, f, ensure_ascii=False, indent=2)
            logger.info(f"JSON 파일 저장 완료: {json_path}")
            
            return {
                'csv_path': csv_path,
                'json_path': json_path,
                'records_count': len(ipo_data)
            }
            
        except Exception as e:
            logger.error(f"IPO 데이터 저장 중 오류: {e}")
            return {}
    
    def collect_and_save_ipo_data(self, days_back: int = 365, filename: str = "recent_ipos") -> Dict[str, str]:
        """IPO 데이터를 수집하고 JSON/CSV로 저장하는 통합 메서드
        
        Args:
            days_back: 조회할 과거 일수
            filename: 저장할 파일명 (확장자 제외)
            
        Returns:
            저장된 파일 정보
        """
        try:
            # IPO 데이터 수집
            ipo_data = self.get_recent_ipos(days_back)
            
            if ipo_data.empty:
                logger.warning("수집된 IPO 데이터가 없습니다.")
                return {}
            
            # 데이터 저장
            save_result = self.save_ipo_data(ipo_data, filename)
            
            if save_result:
                logger.info(f"IPO 데이터 수집 및 저장 완료: {save_result['records_count']}개 레코드")
                return save_result
            else:
                logger.error("IPO 데이터 저장 실패")
                return {}
                
        except Exception as e:
            logger.error(f"IPO 데이터 수집 및 저장 중 오류: {e}")
            return {}
    
    def get_ipo_performance(self, symbol: str, ipo_date: str) -> Dict:
        """특정 IPO의 성과를 분석합니다 (yfinance 대신 더 안정적인 방법 사용)"""
        try:
            # Finnhub 또는 FMP를 사용하여 주가 데이터 수집
            if self.finnhub_api_key:
                return self._get_finnhub_performance(symbol, ipo_date)
            elif self.fmp_api_key:
                return self._get_fmp_performance(symbol, ipo_date)
            else:
                # 대체 방법: Alpha Vantage 무료 API 사용
                return self._get_alphavantage_performance(symbol, ipo_date)
                
        except Exception as e:
            logger.error(f"{symbol} 성과 분석 중 오류: {e}")
            return {}
    
    def _get_finnhub_performance(self, symbol: str, ipo_date: str) -> Dict:
        """Finnhub API를 사용한 성과 분석"""
        try:
            start_date = datetime.strptime(ipo_date, '%Y-%m-%d')
            end_date = datetime.now()
            
            url = f"{self.finnhub_base_url}/stock/candle"
            params = {
                'symbol': symbol,
                'resolution': 'D',
                'from': int(start_date.timestamp()),
                'to': int(end_date.timestamp()),
                'token': self.finnhub_api_key
            }
            
            response = self._safe_request(url, params)
            if not response:
                return {}
                
            data = response.json()
            
            if data.get('s') == 'ok' and data.get('c'):
                closes = data['c']
                highs = data['h']
                lows = data['l']
                volumes = data['v']
                
                if closes:
                    ipo_price = closes[0]
                    current_price = closes[-1]
                    
                    return {
                        'symbol': symbol,
                        'ipo_price': ipo_price,
                        'current_price': current_price,
                        'total_return': (current_price - ipo_price) / ipo_price * 100,
                        'max_price': max(highs),
                        'min_price': min(lows),
                        'max_return': (max(highs) - ipo_price) / ipo_price * 100,
                        'max_drawdown': (min(lows) - ipo_price) / ipo_price * 100,
                        'avg_volume': sum(volumes) / len(volumes) if volumes else 0
                    }
            
            return {}
            
        except Exception as e:
            logger.error(f"Finnhub 성과 분석 오류: {e}")
            return {}
    
    def _get_fmp_performance(self, symbol: str, ipo_date: str) -> Dict:
        """FMP API를 사용한 성과 분석"""
        try:
            url = f"{self.fmp_base_url}/historical-price-full/{symbol}"
            params = {
                'apikey': self.fmp_api_key,
                'from': ipo_date
            }
            
            response = self._safe_request(url, params)
            if not response:
                return {}
                
            data = response.json()
            
            if 'historical' in data and data['historical']:
                hist_data = data['historical']
                hist_data.reverse()  # 날짜순 정렬
                
                ipo_price = hist_data[0]['close']
                current_price = hist_data[-1]['close']
                
                highs = [d['high'] for d in hist_data]
                lows = [d['low'] for d in hist_data]
                volumes = [d['volume'] for d in hist_data]
                
                return {
                    'symbol': symbol,
                    'ipo_price': ipo_price,
                    'current_price': current_price,
                    'total_return': (current_price - ipo_price) / ipo_price * 100,
                    'max_price': max(highs),
                    'min_price': min(lows),
                    'max_return': (max(highs) - ipo_price) / ipo_price * 100,
                    'max_drawdown': (min(lows) - ipo_price) / ipo_price * 100,
                    'avg_volume': sum(volumes) / len(volumes) if volumes else 0
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"FMP 성과 분석 오류: {e}")
            return {}
    
    def _get_alphavantage_performance(self, symbol: str, ipo_date: str) -> Dict:
        """Alpha Vantage API를 사용한 성과 분석 (무료 대안)"""
        try:
            api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
            if not api_key:
                return {}
                
            url = "https://www.alphavantage.co/query"
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': symbol,
                'apikey': api_key,
                'outputsize': 'full'
            }
            
            response = self._safe_request(url, params)
            if not response:
                return {}
                
            data = response.json()
            
            if 'Time Series (Daily)' in data:
                time_series = data['Time Series (Daily)']
                dates = sorted(time_series.keys())
                
                # IPO 날짜 이후 데이터만 필터링
                filtered_dates = [d for d in dates if d >= ipo_date]
                
                if filtered_dates:
                    first_data = time_series[filtered_dates[0]]
                    last_data = time_series[filtered_dates[-1]]
                    
                    ipo_price = float(first_data['4. close'])
                    current_price = float(last_data['4. close'])
                    
                    highs = [float(time_series[d]['2. high']) for d in filtered_dates]
                    lows = [float(time_series[d]['3. low']) for d in filtered_dates]
                    volumes = [int(time_series[d]['5. volume']) for d in filtered_dates]
                    
                    return {
                        'symbol': symbol,
                        'ipo_price': ipo_price,
                        'current_price': current_price,
                        'total_return': (current_price - ipo_price) / ipo_price * 100,
                        'max_price': max(highs),
                        'min_price': min(lows),
                        'max_return': (max(highs) - ipo_price) / ipo_price * 100,
                        'max_drawdown': (min(lows) - ipo_price) / ipo_price * 100,
                        'avg_volume': sum(volumes) / len(volumes) if volumes else 0
                    }
            
            return {}
            
        except Exception as e:
            logger.error(f"Alpha Vantage 성과 분석 오류: {e}")
            return {}

# 사용 예시
if __name__ == "__main__":
    # API 키 설정 (환경변수 또는 직접 입력)
    # export FINNHUB_API_KEY="your_key_here"
    # export FMP_API_KEY="your_key_here"
    
    collector = IPODataCollector()
    
    # 최근 1년간 IPO 데이터 수집 및 저장
    print("최근 IPO 데이터 수집 및 저장 중...")
    save_result = collector.collect_and_save_ipo_data(365, "recent_ipos")
    
    if save_result:
        print(f"\n데이터 저장 완료:")
        print(f"- CSV 파일: {save_result['csv_path']}")
        print(f"- JSON 파일: {save_result['json_path']}")
        print(f"- 레코드 수: {save_result['records_count']}개")
        
        # 저장된 데이터 미리보기
        ipo_data = collector.get_recent_ipos(365)
        if not ipo_data.empty:
            print(f"\n수집된 IPO 데이터 미리보기:")
            print(ipo_data.head())
            
            # 특정 IPO 성과 분석
            symbol = ipo_data.iloc[0]['symbol']
            ipo_date = ipo_data.iloc[0]['ipo_date'].strftime('%Y-%m-%d')
            print(f"\n{symbol} 성과 분석 중...")
            performance = collector.get_ipo_performance(symbol, ipo_date)
            if performance:
                print(f"IPO 가격: ${performance['ipo_price']:.2f}")
                print(f"현재 가격: ${performance['current_price']:.2f}")
                print(f"총 수익률: {performance['total_return']:.2f}%")
            else:
                print("성과 분석 데이터를 가져올 수 없습니다.")
    else:
        print("IPO 데이터 수집 및 저장 실패")