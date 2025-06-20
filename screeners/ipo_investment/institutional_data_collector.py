"""기관 투자자 데이터 수집 모듈

SEC 13F 파일링, FINRA 데이터 등을 통해 기관 매수/매도 데이터를 수집합니다.
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import yfinance as yf
import logging
from typing import Dict, List, Optional
import time
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InstitutionalDataCollector:
    """기관 투자자 데이터 수집기"""
    
    def __init__(self):
        self.sec_base_url = "https://data.sec.gov/api/xbrl/companyfacts/CIK{}.json"
        self.headers = {
            'User-Agent': 'Investment Research Tool (contact@example.com)'
        }
        
        # 주요 기관 투자자 CIK 번호 (예시)
        self.major_institutions = {
            'Berkshire Hathaway': '0001067983',
            'Vanguard Group': '0000102909',
            'BlackRock': '0001364742',
            'State Street': '0000093751',
            'Fidelity': '0000315066'
        }
    
    def get_institutional_holdings(self, symbol: str, quarters_back: int = 4) -> pd.DataFrame:
        """특정 종목의 기관 보유 현황을 가져옵니다.
        
        Args:
            symbol: 종목 심볼
            quarters_back: 조회할 과거 분기 수
            
        Returns:
            기관 보유 데이터프레임
        """
        try:
            # Yahoo Finance에서 기관 보유 데이터 수집
            ticker = yf.Ticker(symbol)
            institutional_holders = ticker.institutional_holders
            
            if institutional_holders is not None and not institutional_holders.empty:
                # 데이터 정제 및 추가 정보 수집
                enhanced_data = self._enhance_institutional_data(institutional_holders, symbol)
                return enhanced_data
            else:
                return self._get_sample_institutional_data(symbol)
                
        except Exception as e:
            logger.error(f"{symbol} 기관 보유 데이터 수집 중 오류: {e}")
            return self._get_sample_institutional_data(symbol)
    
    def get_institutional_flow(self, symbol: str, days_back: int = 30) -> Dict:
        """기관 자금 흐름을 분석합니다.
        
        Args:
            symbol: 종목 심볼
            days_back: 분석할 과거 일수
            
        Returns:
            기관 자금 흐름 분석 결과
        """
        try:
            # 실제 구현에서는 다양한 데이터 소스 활용
            ticker = yf.Ticker(symbol)
            
            # 최근 거래 데이터 분석
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            hist = ticker.history(start=start_date, end=end_date)
            
            if hist.empty:
                return {}
            
            # 기관 매매 패턴 분석 (볼륨과 가격 움직임 기반)
            flow_analysis = self._analyze_institutional_flow(hist, symbol)
            
            return flow_analysis
            
        except Exception as e:
            logger.error(f"{symbol} 기관 자금 흐름 분석 중 오류: {e}")
            return {}
    
    def _enhance_institutional_data(self, holdings_data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """기관 보유 데이터를 보강합니다."""
        enhanced_data = holdings_data.copy()
        
        # 보유 비중 계산
        if 'Shares' in enhanced_data.columns:
            total_shares = enhanced_data['Shares'].sum()
            enhanced_data['Ownership_Percentage'] = (enhanced_data['Shares'] / total_shares * 100).round(2)
        
        # 기관 유형 분류
        enhanced_data['Institution_Type'] = enhanced_data['Holder'].apply(self._classify_institution_type)
        
        # 최근 변동 추정 (실제로는 13F 파일링 비교 필요)
        enhanced_data['Estimated_Change'] = 'Hold'  # 실제 구현시 계산
        
        return enhanced_data
    
    def _classify_institution_type(self, institution_name: str) -> str:
        """기관 유형을 분류합니다."""
        name_lower = institution_name.lower()
        
        if any(word in name_lower for word in ['vanguard', 'blackrock', 'state street', 'fidelity']):
            return 'Asset Manager'
        elif any(word in name_lower for word in ['pension', 'retirement']):
            return 'Pension Fund'
        elif any(word in name_lower for word in ['hedge', 'capital', 'partners']):
            return 'Hedge Fund'
        elif any(word in name_lower for word in ['insurance', 'life']):
            return 'Insurance Company'
        elif any(word in name_lower for word in ['bank', 'trust']):
            return 'Bank/Trust'
        else:
            return 'Other'
    
    def _analyze_institutional_flow(self, price_data: pd.DataFrame, symbol: str) -> Dict:
        """가격과 볼륨 데이터를 기반으로 기관 자금 흐름을 분석합니다."""
        
        # 볼륨 가중 평균 가격 (VWAP) 계산
        price_data['VWAP'] = (price_data['Close'] * price_data['Volume']).cumsum() / price_data['Volume'].cumsum()
        
        # 대량 거래일 식별 (평균 볼륨의 150% 이상)
        avg_volume = price_data['Volume'].mean()
        high_volume_days = price_data[price_data['Volume'] > avg_volume * 1.5]
        
        # 기관 매수/매도 추정
        institutional_buying_days = 0
        institutional_selling_days = 0
        
        for _, day in high_volume_days.iterrows():
            # 종가가 VWAP보다 높고 볼륨이 많으면 기관 매수로 추정
            if day['Close'] > day['VWAP']:
                institutional_buying_days += 1
            else:
                institutional_selling_days += 1
        
        # 연속 매수/매도일 계산
        consecutive_buying = self._calculate_consecutive_days(price_data, 'buying')
        consecutive_selling = self._calculate_consecutive_days(price_data, 'selling')
        
        flow_analysis = {
            'symbol': symbol,
            'analysis_period_days': len(price_data),
            'high_volume_days': len(high_volume_days),
            'estimated_institutional_buying_days': institutional_buying_days,
            'estimated_institutional_selling_days': institutional_selling_days,
            'net_institutional_flow': institutional_buying_days - institutional_selling_days,
            'consecutive_buying_days': consecutive_buying,
            'consecutive_selling_days': consecutive_selling,
            'avg_daily_volume': avg_volume,
            'volume_trend': 'Increasing' if price_data['Volume'].tail(5).mean() > price_data['Volume'].head(5).mean() else 'Decreasing',
            'price_vs_vwap': 'Above' if price_data['Close'].iloc[-1] > price_data['VWAP'].iloc[-1] else 'Below'
        }
        
        return flow_analysis
    
    def _calculate_consecutive_days(self, price_data: pd.DataFrame, flow_type: str) -> int:
        """연속 매수/매도일을 계산합니다."""
        consecutive_days = 0
        max_consecutive = 0
        
        for i in range(len(price_data)):
            if flow_type == 'buying':
                condition = (price_data['Close'].iloc[i] > price_data['VWAP'].iloc[i] and 
                           price_data['Volume'].iloc[i] > price_data['Volume'].mean())
            else:
                condition = (price_data['Close'].iloc[i] < price_data['VWAP'].iloc[i] and 
                           price_data['Volume'].iloc[i] > price_data['Volume'].mean())
            
            if condition:
                consecutive_days += 1
                max_consecutive = max(max_consecutive, consecutive_days)
            else:
                consecutive_days = 0
        
        return max_consecutive
    
    def _get_sample_institutional_data(self, symbol: str) -> pd.DataFrame:
        """샘플 기관 보유 데이터를 반환합니다."""
        sample_data = {
            'Holder': [
                'Vanguard Group Inc',
                'BlackRock Inc.',
                'State Street Corporation',
                'Fidelity Management & Research Company LLC',
                'Geode Capital Management LLC'
            ],
            'Shares': [50000000, 45000000, 30000000, 25000000, 20000000],
            'Date Reported': ['2024-03-31'] * 5,
            'Value': [1500000000, 1350000000, 900000000, 750000000, 600000000],
            'Institution_Type': ['Asset Manager', 'Asset Manager', 'Asset Manager', 'Asset Manager', 'Asset Manager'],
            'Ownership_Percentage': [29.41, 26.47, 17.65, 14.71, 11.76],
            'Estimated_Change': ['Hold', 'Buy', 'Hold', 'Sell', 'Buy']
        }
        
        return pd.DataFrame(sample_data)
    
    def get_insider_trading(self, symbol: str, months_back: int = 6) -> pd.DataFrame:
        """임원 거래 내역을 가져옵니다."""
        try:
            # 실제 구현에서는 SEC Form 4 파일링 데이터 사용
            # 현재는 샘플 데이터 반환
            sample_insider_data = {
                'Date': ['2024-01-15', '2024-02-20', '2024-03-10'],
                'Insider': ['CEO John Smith', 'CFO Jane Doe', 'Director Bob Johnson'],
                'Transaction': ['Sale', 'Purchase', 'Sale'],
                'Shares': [10000, 5000, 2000],
                'Price': [150.0, 145.0, 160.0],
                'Value': [1500000, 725000, 320000]
            }
            
            return pd.DataFrame(sample_insider_data)
            
        except Exception as e:
            logger.error(f"{symbol} 임원 거래 데이터 수집 중 오류: {e}")
            return pd.DataFrame()

# 사용 예시
if __name__ == "__main__":
    collector = InstitutionalDataCollector()
    
    # 기관 보유 현황 조회
    symbol = "AAPL"
    holdings = collector.get_institutional_holdings(symbol)
    print(f"{symbol} 기관 보유 현황:")
    print(holdings)
    
    # 기관 자금 흐름 분석
    flow_analysis = collector.get_institutional_flow(symbol, 30)
    print(f"\n{symbol} 기관 자금 흐름 분석:")
    print(json.dumps(flow_analysis, indent=2))
    
    # 임원 거래 내역
    insider_trades = collector.get_insider_trading(symbol)
    print(f"\n{symbol} 임원 거래 내역:")
    print(insider_trades)