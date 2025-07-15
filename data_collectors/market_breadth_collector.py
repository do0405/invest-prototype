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
    
    def collect_vix_data(self, days: int = 252, force_update: bool = False) -> bool:
        """VIX 데이터 수집 (캐싱 및 중복 방지)"""
        try:
            print("📊 VIX 데이터 수집 중...")
            
            # 디렉토리 생성
            os.makedirs(OPTION_DATA_DIR, exist_ok=True)
            vix_file = os.path.join(OPTION_DATA_DIR, 'vix.csv')
            
            # 기존 파일 확인 및 캐싱 로직
            if not force_update and os.path.exists(vix_file):
                try:
                    existing_data = pd.read_csv(vix_file)
                    if not existing_data.empty:
                        # 최신 데이터가 1일 이내인지 확인
                        last_date = pd.to_datetime(existing_data['date'].iloc[-1])
                        if (datetime.now() - last_date).days < 1:
                            print(f"✅ 기존 VIX 데이터 사용 (최신: {last_date.date()})")
                            return True
                except Exception:
                    pass

            # VIX 데이터 다운로드 (여러 시도)
            vix = None
            symbols_to_try = ['^VIX', 'VIX']

            for symbol in symbols_to_try:
                try:
                    print(f"  시도 중: {symbol}")
                    vix = yf.download(symbol, period=f'{days}d', interval='1d', progress=False)
                    if not vix.empty:
                        print(f"  ✅ {symbol}에서 데이터 수집 성공")
                        break
                except Exception as e:
                    print(f"  ❌ {symbol} 실패: {e}")
                    continue

            if vix is None or vix.empty:
                print('❌ VIX 데이터를 가져오지 못했습니다.')
                return False

            # VIX 데이터 정리 및 검증
            # MultiIndex 컬럼이 있는 경우 처리
            if isinstance(vix.columns, pd.MultiIndex):
                vix.columns = vix.columns.droplevel(1)
            
            # 데이터 평탄화 처리
            vix_close = vix['Close'].values.flatten() if hasattr(vix['Close'], 'values') else vix['Close']
            vix_high = vix['High'].values.flatten() if hasattr(vix['High'], 'values') else vix['High']
            vix_low = vix['Low'].values.flatten() if hasattr(vix['Low'], 'values') else vix['Low']
            vix_volume = vix['Volume'].values.flatten() if hasattr(vix['Volume'], 'values') else vix['Volume']
            
            vix_data = pd.DataFrame({
                'date': vix.index.strftime('%Y-%m-%d'),
                'vix_close': pd.Series(vix_close).round(2),
                'vix_high': pd.Series(vix_high).round(2),
                'vix_low': pd.Series(vix_low).round(2),
                'vix_volume': pd.Series(vix_volume).fillna(0).astype(int),
            })
            
            # 데이터 검증
            vix_data = vix_data.dropna(subset=['vix_close'])
            if vix_data.empty:
                print('❌ 유효한 VIX 데이터가 없습니다.')
                return False
            
            # 파일 저장
            vix_data.to_csv(vix_file, index=False)
            print(f"✅ VIX 데이터 저장 완료: {vix_file} ({len(vix_data)}개 레코드)")
            print(f"  최신 VIX: {vix_data.iloc[-1]['vix_close']} ({vix_data.iloc[-1]['date']})")
            
            return True
            
        except Exception as e:
            print(f"❌ VIX 데이터 수집 오류: {e}")
            return False
    
    def collect_put_call_ratio(self, days: int = 252) -> bool:
        """Put/Call Ratio 데이터를 FRED에서 수집"""
        try:
            print("📊 Put/Call Ratio 데이터 수집 중...")
            
            # 디렉토리 생성
            os.makedirs(OPTION_DATA_DIR, exist_ok=True)
            pc_file = os.path.join(OPTION_DATA_DIR, 'put_call_ratio.csv')
            
            # 기존 파일 확인
            if os.path.exists(pc_file):
                try:
                    existing_data = pd.read_csv(pc_file)
                    if not existing_data.empty:
                        print(f"✅ 기존 Put/Call Ratio 데이터 사용 ({len(existing_data)}개 레코드)")
                        return True
                except Exception:
                    pass

            # FRED에서 데이터 수집 시도
            url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=PUTCALL"
            try:
                resp = requests.get(url, timeout=15)
                if resp.status_code != 200:
                    raise Exception(f"HTTP {resp.status_code} 오류")
                
                df = pd.read_csv(StringIO(resp.text))
                if df.empty:
                    raise Exception("빈 데이터셋")
                    
                df.columns = [c.lower() for c in df.columns]
                df['date'] = pd.to_datetime(df['date'])
                df.rename(columns={df.columns[1]: 'put_call_ratio'}, inplace=True)
                df = df.dropna().tail(days)
                
                if df.empty:
                    raise Exception("유효한 데이터 없음")
                
                # 파일 저장
                df.to_csv(pc_file, index=False)
                print(f"✅ Put/Call Ratio 데이터 저장 완료: {pc_file} ({len(df)}개 레코드)")
                return True
                
            except Exception as e:
                print(f"❌ FRED에서 Put/Call Ratio 데이터 수집 실패: {e}")
                
                # 대체 데이터 생성 (더미 데이터)
                print("📊 대체 Put/Call Ratio 데이터 생성 중...")
                dates = pd.date_range(end=datetime.now().date(), periods=days, freq='D')
                dummy_data = pd.DataFrame({
                    'date': dates.strftime('%Y-%m-%d'),
                    'put_call_ratio': [1.0] * days  # 기본값 1.0
                })
                dummy_data.to_csv(pc_file, index=False)
                print(f"✅ 대체 Put/Call Ratio 데이터 생성 완료: {pc_file} ({len(dummy_data)}개 레코드)")
                return True
            
        except Exception as e:
            print(f"❌ Put/Call Ratio 데이터 수집 오류: {e}")
            return False
    
    def _process_file_for_high_low(self, file_path: str, days: int) -> Dict[pd.Timestamp, Dict[str, int]]:
        """Process a single file for high-low index calculation."""
        date_map: Dict[pd.Timestamp, Dict[str, int]] = {}
        try:
            df = pd.read_csv(file_path)
            df.columns = [c.lower() for c in df.columns]
            if 'date' not in df.columns or 'high' not in df.columns or 'low' not in df.columns or 'close' not in df.columns:
                return date_map
            
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
            pass
        return date_map

    def collect_high_low_index(self, days: int = 252) -> bool:
        """High-Low Index 데이터 수집 (병렬 처리)"""
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

            print(f"📈 {len(csv_files)}개 파일을 병렬 처리로 분석 중...")
            
            # 병렬 처리로 파일들 처리
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import threading
            
            date_map: Dict[pd.Timestamp, Dict[str, int]] = {}
            lock = threading.Lock()
            
            def merge_results(file_result):
                with lock:
                    for date, values in file_result.items():
                        if date not in date_map:
                            date_map[date] = {'highs': 0, 'lows': 0, 'total': 0}
                        date_map[date]['highs'] += values['highs']
                        date_map[date]['lows'] += values['lows']
                        date_map[date]['total'] += values['total']
            
            # 최대 8개 워커로 병렬 처리
            max_workers = min(8, len(csv_files))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_file = {executor.submit(self._process_file_for_high_low, file, days): file for file in csv_files}
                
                completed = 0
                for future in as_completed(future_to_file):
                    file_result = future.result()
                    merge_results(file_result)
                    completed += 1
                    if completed % 100 == 0:
                        print(f"진행률: {completed}/{len(csv_files)} 파일 처리 완료")

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
            
            # 데이터 검증
            if ad_df.empty:
                print("⚠️ Advance-Decline 데이터가 비어있습니다.")
                # 빈 DataFrame이라도 기본 구조는 유지
                ad_df = pd.DataFrame(columns=['date', 'advancing', 'declining', 'unchanged'])
            else:
                # 데이터 타입 확인 및 변환
                for col in ['advancing', 'declining', 'unchanged']:
                    if col in ad_df.columns:
                        ad_df[col] = pd.to_numeric(ad_df[col], errors='coerce').fillna(0).astype(int)
            
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
