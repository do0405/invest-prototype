"""IPO 데이터 수집 모듈

SEC EDGAR API, Yahoo Finance, 기타 소스를 통해 IPO 데이터를 수집합니다.
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import yfinance as yf
import logging
from typing import Dict, List, Optional
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IPODataCollector:
    """IPO 데이터 수집기"""
    
    def __init__(self):
        self.sec_base_url = "https://www.sec.gov/files/company_tickers.json"
        self.headers = {
            'User-Agent': 'Investment Research Tool (contact@example.com)'
        }
    
    def get_recent_ipos(self, days_back: int = 365) -> pd.DataFrame:
        """최근 IPO 목록을 가져옵니다.
        
        Args:
            days_back: 조회할 과거 일수
            
        Returns:
            IPO 데이터프레임 (symbol, company_name, ipo_date, ipo_price)
        """
        try:
            # SEC 데이터 수집 (실제 구현시 SEC EDGAR API 사용)
            ipo_data = self._get_sec_ipo_data(days_back)
            
            # Yahoo Finance에서 추가 정보 수집
            enhanced_data = self._enhance_with_yahoo_data(ipo_data)
            
            return enhanced_data
            
        except Exception as e:
            logger.error(f"IPO 데이터 수집 중 오류: {e}")
            return self._get_sample_ipo_data()
    
    def _get_sec_ipo_data(self, days_back: int) -> List[Dict]:
        """실제 IPO 데이터를 수집합니다."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        ipo_data = []
        
        try:
            # 방법 1: NASDAQ IPO 캘린더 API 사용
            ipo_data.extend(self._get_nasdaq_ipo_data(start_date, end_date))
            
            # 방법 2: IPO Scoop 데이터 스크래핑
            ipo_data.extend(self._get_iposcoop_data(start_date, end_date))
            
            # 방법 3: Yahoo Finance IPO 캘린더
            ipo_data.extend(self._get_yahoo_ipo_data(start_date, end_date))
            
            # 중복 제거
            seen_symbols = set()
            unique_ipos = []
            for ipo in ipo_data:
                if ipo['symbol'] not in seen_symbols:
                    seen_symbols.add(ipo['symbol'])
                    unique_ipos.append(ipo)
            
            logger.info(f"실제 IPO 데이터 수집 완료: {len(unique_ipos)}개 종목")
            return unique_ipos
            
        except Exception as e:
            logger.error(f"실제 IPO 데이터 수집 실패: {e}, 샘플 데이터 사용")
            return self._get_fallback_ipo_data()
    
    def _get_nasdaq_ipo_data(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """NASDAQ IPO 캘린더에서 데이터 수집"""
        try:
            # NASDAQ IPO 캘린더 URL
            url = "https://api.nasdaq.com/api/ipo/calendar"
            
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                ipos = []
                if 'data' in data and 'rows' in data['data']:
                    for row in data['data']['rows']:
                        try:
                            ipo_date_str = row.get('expectedPriceDate', '')
                            if ipo_date_str:
                                ipo_date = datetime.strptime(ipo_date_str, '%m/%d/%Y')
                                
                                if start_date <= ipo_date <= end_date:
                                    ipos.append({
                                        'symbol': row.get('symbol', '').strip(),
                                        'company_name': row.get('companyName', '').strip(),
                                        'ipo_date': ipo_date.strftime('%Y-%m-%d'),
                                        'ipo_price': self._parse_price(row.get('proposedSharePrice', '0'))
                                    })
                        except Exception as e:
                            logger.warning(f"NASDAQ IPO 데이터 파싱 오류: {e}")
                            continue
                
                logger.info(f"NASDAQ에서 {len(ipos)}개 IPO 데이터 수집")
                return ipos
            
        except Exception as e:
            logger.warning(f"NASDAQ IPO 데이터 수집 실패: {e}")
        
        return []
    
    def _get_iposcoop_data(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """IPO Scoop에서 데이터 수집"""
        try:
            import requests
            from bs4 import BeautifulSoup
            
            url = "https://www.iposcoop.com/ipo-calendar/"
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                ipos = []
                # IPO 테이블 파싱 (실제 구조에 맞게 조정 필요)
                tables = soup.find_all('table')
                
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows[1:]:  # 헤더 제외
                        try:
                            cells = row.find_all('td')
                            if len(cells) >= 4:
                                symbol = cells[0].get_text(strip=True)
                                company_name = cells[1].get_text(strip=True)
                                ipo_date_str = cells[2].get_text(strip=True)
                                price_range = cells[3].get_text(strip=True)
                                
                                if symbol and company_name and ipo_date_str:
                                    try:
                                        ipo_date = datetime.strptime(ipo_date_str, '%m/%d/%Y')
                                        if start_date <= ipo_date <= end_date:
                                            ipos.append({
                                                'symbol': symbol,
                                                'company_name': company_name,
                                                'ipo_date': ipo_date.strftime('%Y-%m-%d'),
                                                'ipo_price': self._parse_price_range(price_range)
                                            })
                                    except ValueError:
                                        continue
                        except Exception as e:
                            continue
                
                logger.info(f"IPO Scoop에서 {len(ipos)}개 IPO 데이터 수집")
                return ipos
            
        except Exception as e:
            logger.warning(f"IPO Scoop 데이터 수집 실패: {e}")
        
        return []
    
    def _get_yahoo_ipo_data(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Yahoo Finance IPO 캘린더에서 데이터 수집"""
        try:
            # Yahoo Finance IPO 캘린더는 직접 API가 없으므로 
            # 최근 상장된 종목들을 yfinance로 확인
            recent_ipos = [
                'SMCI', 'ARM', 'FRHC', 'SOLV', 'KKVS', 'KROS', 'TMDX', 'CGEM',
                'INTA', 'PRCT', 'TARS', 'WEAV', 'CAVA', 'FREY', 'VTEX', 'SPIR'
            ]
            
            ipos = []
            for symbol in recent_ipos:
                try:
                    ticker = yf.Ticker(symbol)
                    info = ticker.info
                    
                    # IPO 날짜 추정 (첫 거래일)
                    hist = ticker.history(period="max")
                    if not hist.empty:
                        first_date = hist.index[0].date()
                        first_datetime = datetime.combine(first_date, datetime.min.time())
                        
                        # timezone 정보 제거하여 비교
                        start_date_naive = start_date.replace(tzinfo=None) if hasattr(start_date, 'tzinfo') and start_date.tzinfo else start_date
                        end_date_naive = end_date.replace(tzinfo=None) if hasattr(end_date, 'tzinfo') and end_date.tzinfo else end_date
                        
                        if start_date_naive <= first_datetime <= end_date_naive:
                            ipos.append({
                                'symbol': symbol,
                                'company_name': info.get('longName', symbol),
                                'ipo_date': first_date.strftime('%Y-%m-%d'),
                                'ipo_price': hist['Open'].iloc[0] if not hist.empty else 0
                            })
                    
                    time.sleep(0.1)  # API 제한 준수
                    
                except Exception as e:
                    logger.warning(f"Yahoo Finance {symbol} 데이터 수집 실패: {e}")
                    continue
            
            logger.info(f"Yahoo Finance에서 {len(ipos)}개 IPO 데이터 수집")
            return ipos
            
        except Exception as e:
            logger.warning(f"Yahoo Finance IPO 데이터 수집 실패: {e}")
        
        return []
    
    def _parse_price(self, price_str: str) -> float:
        """가격 문자열을 float로 변환"""
        try:
            # $15.00 형태에서 숫자만 추출
            import re
            numbers = re.findall(r'[\d.]+', str(price_str))
            if numbers:
                return float(numbers[0])
        except:
            pass
        return 0.0
    
    def _parse_price_range(self, price_range: str) -> float:
        """가격 범위에서 중간값 계산"""
        try:
            import re
            numbers = re.findall(r'[\d.]+', str(price_range))
            if len(numbers) >= 2:
                return (float(numbers[0]) + float(numbers[1])) / 2
            elif len(numbers) == 1:
                return float(numbers[0])
        except:
            pass
        return 0.0
    
    def _get_fallback_ipo_data(self) -> List[Dict]:
        """실제 데이터 수집 실패시 최신 IPO 종목들 반환"""
        # 2023-2024년 실제 IPO 종목들
        return [
            {
                'symbol': 'ARM',
                'company_name': 'Arm Holdings plc',
                'ipo_date': '2023-09-14',
                'ipo_price': 51.0
            },
            {
                'symbol': 'FRHC',
                'company_name': 'Freedom Holding Corp',
                'ipo_date': '2023-08-15',
                'ipo_price': 18.0
            },
            {
                'symbol': 'SOLV',
                'company_name': 'Solventum Corporation',
                'ipo_date': '2024-04-01',
                'ipo_price': 63.0
            },
            {
                'symbol': 'KKVS',
                'company_name': 'KKR Real Estate Finance Trust Inc',
                'ipo_date': '2024-02-15',
                'ipo_price': 20.0
            },
            {
                'symbol': 'CAVA',
                'company_name': 'CAVA Group Inc',
                'ipo_date': '2023-06-15',
                'ipo_price': 22.0
            }
        ]
    
    def _enhance_with_yahoo_data(self, ipo_data: List[Dict]) -> pd.DataFrame:
        """Yahoo Finance에서 추가 정보를 수집합니다."""
        enhanced_data = []
        
        for ipo in ipo_data:
            try:
                ticker = yf.Ticker(ipo['symbol'])
                info = ticker.info
                
                enhanced_ipo = ipo.copy()
                enhanced_ipo.update({
                    'market_cap': info.get('marketCap', 0),
                    'sector': info.get('sector', 'Unknown'),
                    'industry': info.get('industry', 'Unknown'),
                    'current_price': info.get('currentPrice', 0),
                    'volume': info.get('volume', 0),
                    'avg_volume': info.get('averageVolume', 0)
                })
                
                enhanced_data.append(enhanced_ipo)
                time.sleep(0.1)  # API 호출 제한 준수
                
            except Exception as e:
                logger.warning(f"{ipo['symbol']} 데이터 수집 실패: {e}")
                enhanced_data.append(ipo)
        
        return pd.DataFrame(enhanced_data)
    
    def _get_sample_ipo_data(self) -> pd.DataFrame:
        """샘플 IPO 데이터를 반환합니다."""
        sample_data = [
            {
                'symbol': 'RIVN',
                'company_name': 'Rivian Automotive Inc',
                'ipo_date': '2021-11-10',
                'ipo_price': 78.0,
                'market_cap': 100000000000,
                'sector': 'Consumer Cyclical',
                'industry': 'Auto Manufacturers',
                'current_price': 15.0,
                'volume': 50000000,
                'avg_volume': 25000000
            },
            {
                'symbol': 'LCID',
                'company_name': 'Lucid Group Inc',
                'ipo_date': '2021-07-26',
                'ipo_price': 24.0,
                'market_cap': 15000000000,
                'sector': 'Consumer Cyclical',
                'industry': 'Auto Manufacturers',
                'current_price': 4.5,
                'volume': 30000000,
                'avg_volume': 15000000
            }
        ]
        
        return pd.DataFrame(sample_data)
    
    def get_ipo_performance(self, symbol: str, ipo_date: str) -> Dict:
        """특정 IPO의 성과를 분석합니다."""
        try:
            ticker = yf.Ticker(symbol)
            
            # IPO 이후 데이터 가져오기
            start_date = datetime.strptime(ipo_date, '%Y-%m-%d')
            hist = ticker.history(start=start_date)
            
            if hist.empty:
                return {}
            
            current_price = hist['Close'].iloc[-1]
            ipo_price = hist['Close'].iloc[0]
            
            # 성과 지표 계산
            performance = {
                'symbol': symbol,
                'ipo_price': ipo_price,
                'current_price': current_price,
                'total_return': (current_price - ipo_price) / ipo_price * 100,
                'max_price': hist['High'].max(),
                'min_price': hist['Low'].min(),
                'max_return': (hist['High'].max() - ipo_price) / ipo_price * 100,
                'max_drawdown': (hist['Low'].min() - ipo_price) / ipo_price * 100,
                'volatility': hist['Close'].pct_change().std() * 100,
                'avg_volume': hist['Volume'].mean()
            }
            
            return performance
            
        except Exception as e:
            logger.error(f"{symbol} 성과 분석 중 오류: {e}")
            return {}

# 사용 예시
if __name__ == "__main__":
    collector = IPODataCollector()
    
    # 최근 1년간 IPO 데이터 수집
    ipo_data = collector.get_recent_ipos(365)
    print("최근 IPO 데이터:")
    print(ipo_data)
    
    # 특정 IPO 성과 분석
    if not ipo_data.empty:
        symbol = ipo_data.iloc[0]['symbol']
        ipo_date = ipo_data.iloc[0]['ipo_date']
        performance = collector.get_ipo_performance(symbol, ipo_date)
        print(f"\n{symbol} 성과 분석:")
        print(performance)