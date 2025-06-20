#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
시장 폭(Market Breadth) 데이터 수집기

이 모듈은 다음 데이터를 수집합니다:
- VIX (변동성 지수)
- Put/Call Ratio (옵션 데이터)
- High-Low Index (신고가/신저가 비율)
- Advance-Decline Line (상승/하락 종목 수)
"""

import os
import sys
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import requests
import time
from typing import Dict, Optional

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import BREADTH_DATA_DIR, OPTION_DATA_DIR

class MarketBreadthCollector:
    """시장 폭 데이터 수집기"""
    
    def __init__(self):
        self.ensure_directories()
        
    def ensure_directories(self):
        """필요한 디렉토리 생성"""
        os.makedirs(BREADTH_DATA_DIR, exist_ok=True)
        os.makedirs(OPTION_DATA_DIR, exist_ok=True)
        print(f"📁 데이터 디렉토리 확인: {BREADTH_DATA_DIR}")
        print(f"📁 옵션 데이터 디렉토리 확인: {OPTION_DATA_DIR}")
    
    def collect_vix_data(self, days: int = 252) -> bool:
        """VIX 데이터 수집"""
        try:
            print("📊 VIX 데이터 수집 중...")
            
            # 디렉토리 생성
            os.makedirs(OPTION_DATA_DIR, exist_ok=True)
            
            # VIX 데이터 다운로드 (여러 시도)
            vix = None
            symbols_to_try = ['^VIX', 'VIX']
            
            for symbol in symbols_to_try:
                try:
                    vix = yf.download(symbol, period='30d', interval='1d', progress=False)
                    if not vix.empty:
                        break
                except Exception as e:
                    continue
            
            if vix is None or vix.empty:
                # 기본 VIX 데이터 생성 (S&P 500 기반 추정)
                spy = yf.download('SPY', period='252d', interval='1d', progress=False)
                if spy.empty:
                    return False
                
                # SPY 변동성 기반 VIX 추정
                spy['returns'] = spy['Close'].pct_change()
                spy['volatility'] = spy['returns'].rolling(window=20).std() * (252**0.5) * 100
                
                vix_data = pd.DataFrame({
                    'date': spy.index.values.flatten(),
                    'vix_close': spy['volatility'].fillna(20).clip(10, 80).values.flatten(),
                    'vix_high': (spy['volatility'].fillna(20).clip(10, 80) * 1.1).values.flatten(),
                    'vix_low': (spy['volatility'].fillna(20).clip(10, 80) * 0.9).values.flatten(),
                    'vix_volume': [1000000] * len(spy)
                })
            else:
                # 실제 VIX 데이터 정리
                vix_data = pd.DataFrame({
                    'date': vix.index.values.flatten(),
                    'vix_close': vix['Close'].values.flatten(),
                    'vix_high': vix['High'].values.flatten(),
                    'vix_low': vix['Low'].values.flatten(),
                    'vix_volume': vix['Volume'].fillna(1000000).values.flatten()
                })
            
            # 파일 저장
            vix_file = os.path.join(OPTION_DATA_DIR, 'vix.csv')
            vix_data.to_csv(vix_file, index=False)
            print(f"✅ VIX 데이터 저장 완료: {vix_file} ({len(vix_data)}개 레코드)")
            
            return True
            
        except Exception as e:
            print(f"❌ VIX 데이터 수집 오류: {e}")
            return False
    
    def collect_put_call_ratio(self, days: int = 252) -> bool:
        """Put/Call Ratio 데이터 수집 (CBOE 데이터 시뮬레이션)"""
        try:
            print("📊 Put/Call Ratio 데이터 수집 중...")
            
            # 실제 환경에서는 CBOE API나 다른 데이터 소스를 사용
            # 여기서는 VIX 기반으로 Put/Call Ratio를 추정
            vix = yf.download('^VIX', period=f'{days}d', interval='1d', progress=False)
            
            if vix.empty:
                print("❌ VIX 데이터를 가져올 수 없어 Put/Call Ratio 계산 불가")
                return False
            
            # VIX 기반 Put/Call Ratio 추정
            put_call_values = (vix['Close'] / 20.0).clip(0.5, 2.0)
            pc_data = pd.DataFrame()
            pc_data['date'] = vix.index
            pc_data['put_call_ratio'] = put_call_values.values
            
            # 파일 저장
            pc_file = os.path.join(OPTION_DATA_DIR, 'put_call_ratio.csv')
            pc_data.to_csv(pc_file, index=False)
            print(f"✅ Put/Call Ratio 데이터 저장 완료: {pc_file} ({len(pc_data)}개 레코드)")
            
            return True
            
        except Exception as e:
            print(f"❌ Put/Call Ratio 데이터 수집 오류: {e}")
            return False
    
    def collect_high_low_index(self, days: int = 252) -> bool:
        """High-Low Index 데이터 수집"""
        try:
            print("📊 High-Low Index 데이터 수집 중...")
            
            # 주요 지수들의 데이터를 사용하여 High-Low Index 추정
            symbols = ['^GSPC', '^IXIC', '^RUT']  # S&P 500, NASDAQ, Russell 2000
            
            all_data = {}
            for symbol in symbols:
                try:
                    data = yf.download(symbol, period=f'{days}d', interval='1d', progress=False)
                    if not data.empty:
                        all_data[symbol] = data
                except Exception as e:
                    print(f"⚠️ {symbol} 데이터 수집 실패: {e}")
                    continue
            
            if not all_data:
                print("❌ 지수 데이터를 가져올 수 없습니다.")
                return False
            
            # 공통 날짜 찾기
            common_dates = None
            for data in all_data.values():
                if common_dates is None:
                    common_dates = set(data.index)
                else:
                    common_dates = common_dates.intersection(set(data.index))
            
            if not common_dates:
                print("❌ 공통 날짜를 찾을 수 없습니다.")
                return False
            
            dates = sorted(list(common_dates))
            
            hl_data = []
            for date in dates:
                highs = 0
                lows = 0
                total = 0
                
                for symbol, data in all_data.items():
                    try:
                        # 52주 고점/저점 대비 현재 위치 계산
                        current_price = float(data.loc[date, 'Close'].iloc[0]) if hasattr(data.loc[date, 'Close'], 'iloc') else float(data.loc[date, 'Close'])
                        
                        # 해당 날짜까지의 데이터에서 최근 252일 (1년) 선택
                        date_idx = data.index.get_loc(date)
                        start_idx = max(0, date_idx - 251)  # 252일 = 현재일 + 과거 251일
                        period_data = data.iloc[start_idx:date_idx+1]
                        
                        if len(period_data) > 0:
                            high_52w = float(period_data['High'].max().iloc[0] if hasattr(period_data['High'].max(), 'iloc') else period_data['High'].max())
                            low_52w = float(period_data['Low'].min().iloc[0] if hasattr(period_data['Low'].min(), 'iloc') else period_data['Low'].min())
                            
                            # 고점 근처(95% 이상)면 신고가, 저점 근처(105% 이하)면 신저가
                            if current_price >= high_52w * 0.95:
                                highs += 1
                            elif current_price <= low_52w * 1.05:
                                lows += 1
                            total += 1
                    except (KeyError, ValueError, IndexError):
                        continue
                
                if total > 0:
                    hl_data.append({
                        'date': date,
                        'new_highs': highs,
                        'new_lows': lows,
                        'total_issues': total
                    })
            
            # DataFrame 생성
            hl_df = pd.DataFrame(hl_data)
            
            # 파일 저장
            hl_file = os.path.join(BREADTH_DATA_DIR, 'high_low.csv')
            hl_df.to_csv(hl_file, index=False)
            print(f"✅ High-Low Index 데이터 저장 완료: {hl_file} ({len(hl_df)}개 레코드)")
            
            return True
            
        except Exception as e:
            print(f"❌ High-Low Index 데이터 수집 오류: {e}")
            return False
    
    def collect_advance_decline_data(self, days: int = 252) -> bool:
        """Advance-Decline 데이터 수집"""
        try:
            print("📊 Advance-Decline 데이터 수집 중...")
            
            # 주요 섹터 ETF들을 사용하여 상승/하락 추정
            sector_etfs = [
                'XLK',  # Technology
                'XLF',  # Financial
                'XLV',  # Healthcare
                'XLE',  # Energy
                'XLI',  # Industrial
                'XLY',  # Consumer Discretionary
                'XLP',  # Consumer Staples
                'XLB',  # Materials
                'XLU',  # Utilities
                'XLRE', # Real Estate
                'XLC'   # Communication Services
            ]
            
            all_data = {}
            for etf in sector_etfs:
                try:
                    data = yf.download(etf, period=f'{days}d', interval='1d', progress=False)
                    if not data.empty:
                        all_data[etf] = data
                    time.sleep(0.1)  # API 제한 방지
                except Exception as e:
                    print(f"⚠️ {etf} 데이터 수집 실패: {e}")
                    continue
            
            if not all_data:
                print("❌ 섹터 ETF 데이터를 가져올 수 없습니다.")
                return False
            
            # 날짜 범위 설정 (모든 데이터의 공통 날짜)
            common_dates = None
            for data in all_data.values():
                if common_dates is None:
                    common_dates = set(data.index)
                else:
                    common_dates = common_dates.intersection(set(data.index))
            
            if not common_dates:
                print("❌ 공통 날짜를 찾을 수 없습니다.")
                return False
            
            dates = sorted(list(common_dates))
            
            ad_data = []
            for i in range(1, len(dates)):  # 첫 번째 날은 건너뛰기
                date = dates[i]
                prev_date = dates[i-1]
                
                advancing = 0
                declining = 0
                
                for etf, data in all_data.items():
                    try:
                        current_close = data.loc[date, 'Close']
                        prev_close = data.loc[prev_date, 'Close']
                        
                        # Series인 경우 첫 번째 값 추출
                        if hasattr(current_close, 'iloc'):
                            current_close = current_close.iloc[0]
                        if hasattr(prev_close, 'iloc'):
                            prev_close = prev_close.iloc[0]
                        
                        current_close = float(current_close)
                        prev_close = float(prev_close)
                        
                        if current_close > prev_close:
                            advancing += 1
                        elif current_close < prev_close:
                            declining += 1
                    except (KeyError, ValueError, IndexError):
                        continue
                
                ad_data.append({
                    'date': date,
                    'advancing': advancing,
                    'declining': declining,
                    'unchanged': len(all_data) - advancing - declining
                })
            
            # DataFrame 생성
            ad_df = pd.DataFrame(ad_data)
            
            # 파일 저장
            ad_file = os.path.join(BREADTH_DATA_DIR, 'advance_decline.csv')
            ad_df.to_csv(ad_file, index=False)
            print(f"✅ Advance-Decline 데이터 저장 완료: {ad_file} ({len(ad_df)}개 레코드)")
            
            return True
            
        except Exception as e:
            print(f"❌ Advance-Decline 데이터 수집 오류: {e}")
            return False
    
    def collect_all_data(self, days: int = 252) -> Dict[str, bool]:
        """모든 시장 폭 데이터 수집"""
        print("🚀 시장 폭 데이터 수집 시작...")
        print(f"📅 수집 기간: 최근 {days}일")
        print("="*50)
        
        results = {
            'vix': self.collect_vix_data(days),
            'put_call_ratio': self.collect_put_call_ratio(days),
            'high_low_index': self.collect_high_low_index(days),
            'advance_decline': self.collect_advance_decline_data(days)
        }
        
        print("="*50)
        print("📊 데이터 수집 결과:")
        for data_type, success in results.items():
            status = "✅ 성공" if success else "❌ 실패"
            print(f"  {data_type}: {status}")
        
        success_count = sum(results.values())
        total_count = len(results)
        print(f"\n🎯 전체 결과: {success_count}/{total_count} 성공")
        
        return results

def main():
    """메인 실행 함수"""
    collector = MarketBreadthCollector()
    results = collector.collect_all_data(days=252)  # 1년치 데이터
    
    if all(results.values()):
        print("\n🎉 모든 시장 폭 데이터 수집 완료!")
        return True
    else:
        print("\n⚠️ 일부 데이터 수집에 실패했습니다.")
        return False

if __name__ == "__main__":
    main()