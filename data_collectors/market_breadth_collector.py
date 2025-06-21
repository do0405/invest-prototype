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
from io import StringIO
from typing import Dict, Optional

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import (
    BREADTH_DATA_DIR,
    OPTION_DATA_DIR,
    DATA_US_DIR,
    STOCK_METADATA_PATH,
)

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
                    vix = yf.download(symbol, period=f'{days}d', interval='1d', progress=False)
                    if not vix.empty:
                        break
                except Exception:
                    continue

            if vix is None or vix.empty:
                print('❌ VIX 데이터를 가져오지 못했습니다.')
                return False

            # 실제 VIX 데이터 정리
            vix_data = pd.DataFrame({
                'date': vix.index.values.flatten(),
                'vix_close': vix['Close'].values.flatten(),
                'vix_high': vix['High'].values.flatten(),
                'vix_low': vix['Low'].values.flatten(),
                'vix_volume': vix['Volume'].fillna(0).values.flatten(),
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
        """Put/Call Ratio 데이터를 FRED에서 수집"""
        try:
            print("📊 Put/Call Ratio 데이터 수집 중...")

            url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=PUTCALL"
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                print("❌ Put/Call Ratio 데이터를 가져오지 못했습니다.")
                return False

            df = pd.read_csv(StringIO(resp.text))
            df.columns = [c.lower() for c in df.columns]
            df['date'] = pd.to_datetime(df['date'])
            df.rename(columns={df.columns[1]: 'put_call_ratio'}, inplace=True)
            df = df.dropna().tail(days)

            pc_data = df
            
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
            
            # 전 종목 데이터를 활용하여 High-Low Index 계산
            csv_files = [
                os.path.join(DATA_US_DIR, f)
                for f in os.listdir(DATA_US_DIR)
                if f.endswith('.csv')
            ]

            if not csv_files:
                print('❌ 종목 데이터를 찾을 수 없습니다.')
                return False

            date_map: Dict[pd.Timestamp, Dict[str, int]] = {}

            for file in csv_files:
                try:
                    df = pd.read_csv(file)
                    df.columns = [c.lower() for c in df.columns]
                    if 'date' not in df.columns or 'high' not in df.columns or 'low' not in df.columns or 'close' not in df.columns:
                        continue
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.sort_values('date')
                    df = df.tail(252 + days)

                    for i in range(-days, 0):
                        row = df.iloc[i]
                        date = row['date']
                        window = df.iloc[: i + 252] if i != -days else df.iloc[:252]
                        high_52w = window['high'].max()
                        low_52w = window['low'].min()
                        record = date_map.setdefault(date, {'highs': 0, 'lows': 0, 'total': 0})
                        if row['close'] >= high_52w:
                            record['highs'] += 1
                        elif row['close'] <= low_52w:
                            record['lows'] += 1
                        record['total'] += 1
                except Exception:
                    continue

            hl_data = [
                {
                    'date': d,
                    'new_highs': v['highs'],
                    'new_lows': v['lows'],
                    'total_issues': v['total'],
                }
                for d, v in sorted(date_map.items())
            ]
            
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
        """Advance-Decline 데이터를 실제 종목 데이터를 사용해 계산"""
        try:
            print("📊 Advance-Decline 데이터 수집 중...")

            csv_files = [
                os.path.join(DATA_US_DIR, f)
                for f in os.listdir(DATA_US_DIR)
                if f.endswith('.csv')
            ]

            if not csv_files:
                print('❌ 종목 데이터를 찾을 수 없습니다.')
                return False

            date_map: Dict[pd.Timestamp, Dict[str, int]] = {}

            for file in csv_files:
                try:
                    df = pd.read_csv(file)
                    df.columns = [c.lower() for c in df.columns]
                    if 'date' not in df.columns or 'close' not in df.columns:
                        continue
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.sort_values('date')
                    df = df.tail(days + 1)
                    for i in range(1, len(df)):
                        cur = df.iloc[i]
                        prev = df.iloc[i - 1]
                        date = cur['date']
                        rec = date_map.setdefault(date, {'advancing': 0, 'declining': 0, 'total': 0})
                        if pd.isna(cur['close']) or pd.isna(prev['close']):
                            continue
                        rec['total'] += 1
                        if cur['close'] > prev['close']:
                            rec['advancing'] += 1
                        elif cur['close'] < prev['close']:
                            rec['declining'] += 1
                except Exception:
                    continue

            ad_data = [
                {
                    'date': d,
                    'advancing': v['advancing'],
                    'declining': v['declining'],
                    'unchanged': v['total'] - v['advancing'] - v['declining'],
                }
                for d, v in sorted(date_map.items())
            ]
            
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
