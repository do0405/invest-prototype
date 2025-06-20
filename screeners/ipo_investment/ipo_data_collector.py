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
        """SEC에서 IPO 데이터를 수집합니다."""
        # 실제 구현에서는 SEC EDGAR API를 사용
        # 현재는 샘플 데이터 반환
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # 샘플 IPO 데이터 (실제로는 SEC API에서 가져옴)
        sample_ipos = [
            {
                'symbol': 'RIVN',
                'company_name': 'Rivian Automotive Inc',
                'ipo_date': '2021-11-10',
                'ipo_price': 78.0
            },
            {
                'symbol': 'LCID',
                'company_name': 'Lucid Group Inc',
                'ipo_date': '2021-07-26',
                'ipo_price': 24.0
            },
            {
                'symbol': 'HOOD',
                'company_name': 'Robinhood Markets Inc',
                'ipo_date': '2021-07-29',
                'ipo_price': 38.0
            }
        ]
        
        return sample_ipos
    
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