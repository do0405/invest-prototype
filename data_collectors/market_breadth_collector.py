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

# 유틸리티 함수 import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.screener_utils import read_csv_flexible

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
                    vix = yf.download(symbol, period=f'{days}d', interval='1d', progress=False, auto_adjust=False)
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
    
    # Put/Call Ratio 데이터 수집 기능 제거됨
    
    def _process_file_for_high_low(self, file_path: str, days: int, start_date: pd.Timestamp = None) -> Dict[pd.Timestamp, Dict[str, int]]:
        """Process a single file for high-low index calculation with incremental update support."""
        date_map: Dict[pd.Timestamp, Dict[str, int]] = {}
        try:
            df = read_csv_flexible(file_path, ['date', 'high', 'low', 'close'])
            if df is None:
                return date_map
            
            df['date'] = pd.to_datetime(df['date'], utc=True)
            df = df.sort_values('date')
            
            # 증분 업데이트인 경우 start_date 이후 데이터만 처리
            if start_date is not None:
                df_recent = df[df['date'] > start_date]
                if len(df_recent) == 0:
                    return date_map
                # 52주 계산을 위해 충분한 과거 데이터 확보
                df_full = df.tail(252 + len(df_recent))
                process_days = len(df_recent)
            else:
                df_full = df.tail(252 + days)
                process_days = days

            for i in range(-process_days, 0):
                row = df_full.iloc[i]
                date = row['date']
                # 증분 업데이트인 경우 start_date 이후만 처리
                if start_date is not None and date <= start_date:
                    continue
                    
                window = df_full.iloc[: i + 252] if i != -process_days else df_full.iloc[:252]
                if len(window) < 50:  # 최소 데이터 요구사항
                    continue
                    
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
            
            # 기존 데이터 확인
            hl_file = os.path.join(BREADTH_DATA_DIR, 'high_low_index.csv')
            existing_data = None
            last_date = None
            
            if os.path.exists(hl_file):
                existing_data = read_csv_flexible(hl_file, ['date'])
                if existing_data is not None:
                    try:
                        existing_data['date'] = pd.to_datetime(existing_data['date'], utc=True)
                        last_date = existing_data['date'].max()
                        print(f"✅ 기존 High-Low Index 데이터 확인 (최신: {last_date.strftime('%Y-%m-%d')})")
                    except Exception as e:
                        print(f"⚠️ 날짜 처리 오류: {e}, 전체 재수집 진행")
                        existing_data = None
                        last_date = None
                else:
                    print(f"⚠️ 기존 데이터 로드 실패, 전체 재수집 진행")
                    existing_data = None
                    last_date = None
            
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
            
            # 증분 업데이트 방식 개선: 누락된 데이터부터 처리
            from utils.incremental_update_helper import incremental_helper
            
            if last_date is not None:
                # 누락된 기간 계산 (거래일 기준으로 개선)
                from utils.incremental_update_helper import incremental_helper
                
                # 현재 시간을 UTC로 설정
                today = pd.Timestamp.now(tz='UTC')
                
                # 거래일 기준으로 누락된 기간 확인
                last_date_dt = last_date.to_pydatetime().replace(tzinfo=None)
                today_dt = today.to_pydatetime().replace(tzinfo=None)
                
                # 오늘이 거래일인지 확인
                if incremental_helper.is_trading_day(today_dt):
                    # 오늘이 거래일이면 오늘까지 데이터가 있어야 함
                    target_date = today_dt.date()
                else:
                    # 오늘이 거래일이 아니면 이전 거래일까지 데이터가 있어야 함
                    target_date = incremental_helper.get_previous_trading_day(today_dt).date()
                
                # 최신 데이터가 목표 날짜와 같거나 이후면 최신 상태
                if last_date_dt.date() >= target_date:
                    print(f"📈 최신 상태: {last_date.strftime('%Y-%m-%d')} (목표: {target_date})")
                    return True
                
                # 누락된 일수 계산
                missing_days = (target_date - last_date_dt.date()).days
                update_days = min(missing_days + 2, days)  # 누락된 일수 + 2일 여유
                print(f"📈 증분 업데이트: {missing_days}일 누락, {update_days}일 처리 중... (목표: {target_date})")
            else:
                update_days = days
                print(f"📈 전체 업데이트: {len(csv_files)}개 파일 처리 중...")
            
            # 병렬 처리로 파일들 처리 (증분 업데이트 지원)
            from concurrent.futures import ThreadPoolExecutor, as_completed
            
            date_map: Dict[pd.Timestamp, Dict[str, int]] = {}
            all_file_results = []  # 모든 파일 결과를 임시 저장
            
            # 증분 업데이트를 위한 시작 날짜 설정
            start_date = last_date if last_date is not None else None
            
            # 최대 8개 워커로 병렬 처리
            max_workers = min(8, len(csv_files))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_file = {executor.submit(self._process_file_for_high_low, file, update_days, start_date): file for file in csv_files}
                
                completed = 0
                for future in as_completed(future_to_file):
                    file_result = future.result()
                    all_file_results.append(file_result)
                    completed += 1
                    if completed % 100 == 0 or (last_date is None and completed % 100 == 0):
                        print(f"진행률: {completed}/{len(csv_files)} 파일 처리 완료")
            
            # 결과 병합 (메인 스레드에서 안전하게 처리)
            for file_result in all_file_results:
                for date, values in file_result.items():
                    if date not in date_map:
                        date_map[date] = {'highs': 0, 'lows': 0, 'total': 0}
                    date_map[date]['highs'] += values['highs']
                    date_map[date]['lows'] += values['lows']
                    date_map[date]['total'] += values['total']

            # 새로운 데이터 생성
            new_hl_data = [
                {
                    'date': d,
                    'new_highs': v['highs'],
                    'new_lows': v['lows'],
                    'total_issues': v['total'],
                }
                for d, v in sorted(date_map.items())
            ]
            
            # 기존 데이터와 병합
            if existing_data is not None and len(new_hl_data) > 0:
                # 기존 데이터에서 중복 날짜 제거
                new_dates = set(pd.to_datetime([item['date'] for item in new_hl_data]))
                existing_filtered = existing_data[~existing_data['date'].isin(new_dates)]
                
                # 새 데이터와 기존 데이터 결합
                new_df = pd.DataFrame(new_hl_data)
                new_df['date'] = pd.to_datetime(new_df['date'])
                hl_df = pd.concat([existing_filtered, new_df], ignore_index=True)
                hl_df = hl_df.sort_values('date').reset_index(drop=True)
                
                print(f"✅ 증분 업데이트: {len(new_hl_data)}개 새 레코드 추가")
            else:
                # 전체 업데이트
                hl_df = pd.DataFrame(new_hl_data)
                if len(hl_df) > 0:
                    hl_df['date'] = pd.to_datetime(hl_df['date'])
            
            # 파일 저장
            if len(hl_df) > 0:
                hl_df.to_csv(hl_file, index=False)
                print(f"✅ High-Low Index 데이터 저장 완료: {hl_file} (총 {len(hl_df)}개 레코드)")
            else:
                print("⚠️ 저장할 새로운 데이터가 없습니다.")
            
            return True
            
        except Exception as e:
            print(f"❌ High-Low Index 데이터 수집 오류: {e}")
            return False
    
    def collect_advance_decline_data(self, days: int = 252) -> bool:
        """Advance-Decline 데이터를 실제 종목 데이터를 사용해 계산 (증분 업데이트)"""
        try:
            print("📊 Advance-Decline 데이터 수집 중...")
            
            # 기존 데이터 확인
            ad_file = os.path.join(BREADTH_DATA_DIR, 'advance_decline.csv')
            existing_data = None
            last_date = None
            
            if os.path.exists(ad_file):
                existing_data = read_csv_flexible(ad_file, ['date'])
                if existing_data is not None:
                    try:
                        existing_data['date'] = pd.to_datetime(existing_data['date'], utc=True)
                        last_date = existing_data['date'].max()
                        print(f"✅ 기존 Advance-Decline 데이터 확인 (최신: {last_date.strftime('%Y-%m-%d')})")
                    except Exception as e:
                        print(f"⚠️ 날짜 처리 오류: {e}, 전체 재수집 진행")
                        existing_data = None
                        last_date = None
                else:
                    print(f"⚠️ 기존 데이터 로드 실패, 전체 재수집 진행")
                    existing_data = None
                    last_date = None

            csv_files = [
                os.path.join(DATA_US_DIR, f)
                for f in os.listdir(DATA_US_DIR)
                if f.endswith('.csv')
            ]

            if not csv_files:
                print('❌ 종목 데이터를 찾을 수 없습니다.')
                return False

            # 증분 업데이트 방식 개선: 누락된 데이터부터 처리
            if last_date is not None:
                # 누락된 기간 계산 (거래일 기준으로 개선)
                from utils.incremental_update_helper import incremental_helper
                
                # 현재 시간을 UTC로 설정
                today = pd.Timestamp.now(tz='UTC')
                
                # 거래일 기준으로 누락된 기간 확인
                last_date_dt = last_date.to_pydatetime().replace(tzinfo=None)
                today_dt = today.to_pydatetime().replace(tzinfo=None)
                
                # 오늘이 거래일인지 확인
                if incremental_helper.is_trading_day(today_dt):
                    # 오늘이 거래일이면 오늘까지 데이터가 있어야 함
                    target_date = today_dt.date()
                else:
                    # 오늘이 거래일이 아니면 이전 거래일까지 데이터가 있어야 함
                    target_date = incremental_helper.get_previous_trading_day(today_dt).date()
                
                # 최신 데이터가 목표 날짜와 같거나 이후면 최신 상태
                if last_date_dt.date() >= target_date:
                    print(f"📈 최신 상태: {last_date.strftime('%Y-%m-%d')} (목표: {target_date})")
                    return True
                
                # 누락된 일수 계산
                missing_days = (target_date - last_date_dt.date()).days
                update_days = min(missing_days + 2, days)  # 누락된 일수 + 2일 여유
                print(f"📈 증분 업데이트: {missing_days}일 누락, {update_days}일 처리 중... (목표: {target_date})")
            else:
                update_days = days
                print(f"📈 전체 업데이트: {len(csv_files)}개 파일 처리 중...")

            date_map: Dict[pd.Timestamp, Dict[str, int]] = {}

            for file in csv_files:
                try:
                    df = read_csv_flexible(file, ['date', 'close'])
                    if df is None:
                        continue
                    
                    df['date'] = pd.to_datetime(df['date'], utc=True)
                    df = df.sort_values('date')
                    
                    # 증분 업데이트인 경우 필요한 데이터만 처리
                    if last_date is not None:
                        # 최신 날짜 이후 데이터만 처리하되, 이전 날짜 하나는 포함 (비교용)
                        cutoff_date = last_date - pd.Timedelta(days=2)  # 2일 여유를 둠
                        df_filtered = df[df['date'] > cutoff_date]
                        if len(df_filtered) < 2:  # 비교할 데이터가 없으면 스킵
                            continue
                        df = df_filtered
                    else:
                        df = df.tail(update_days + 1)
                    
                    for i in range(1, len(df)):
                        cur = df.iloc[i]
                        prev = df.iloc[i - 1]
                        date = cur['date']
                        
                        # 증분 업데이트인 경우 last_date 이후만 처리
                        if last_date is not None and date <= last_date:
                            continue
                            
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

            # 새로운 데이터 생성
            new_ad_data = [
                {
                    'date': d,
                    'advancing': v['advancing'],
                    'declining': v['declining'],
                    'unchanged': v['total'] - v['advancing'] - v['declining'],
                }
                for d, v in sorted(date_map.items())
            ]
            
            # 기존 데이터와 병합
            if existing_data is not None and len(new_ad_data) > 0:
                # 기존 데이터에서 중복 날짜 제거
                new_dates = set(pd.to_datetime([item['date'] for item in new_ad_data]))
                existing_filtered = existing_data[~existing_data['date'].isin(new_dates)]
                
                # 새 데이터와 기존 데이터 결합
                new_df = pd.DataFrame(new_ad_data)
                new_df['date'] = pd.to_datetime(new_df['date'])
                ad_df = pd.concat([existing_filtered, new_df], ignore_index=True)
                ad_df = ad_df.sort_values('date').reset_index(drop=True)
                
                print(f"✅ 증분 업데이트: {len(new_ad_data)}개 새 레코드 추가")
            else:
                # 전체 업데이트
                ad_df = pd.DataFrame(new_ad_data)
                if len(ad_df) > 0:
                    ad_df['date'] = pd.to_datetime(ad_df['date'])
            
            # 데이터 검증 및 처리
            if len(new_ad_data) == 0:
                print("⚠️ 새로운 Advance-Decline 데이터가 없습니다.")
                if existing_data is not None:
                    print(f"✅ 기존 데이터 유지: {len(existing_data)}개 레코드")
                    return True
                else:
                    # 빈 DataFrame이라도 기본 구조는 유지
                    ad_df = pd.DataFrame(columns=['date', 'advancing', 'declining', 'unchanged'])
            else:
                # 데이터 타입 확인 및 변환
                if 'ad_df' in locals() and not ad_df.empty:
                    for col in ['advancing', 'declining', 'unchanged']:
                        if col in ad_df.columns:
                            ad_df[col] = pd.to_numeric(ad_df[col], errors='coerce').fillna(0).astype(int)
            
            # 파일 저장
            ad_file = os.path.join(BREADTH_DATA_DIR, 'advance_decline.csv')
            if 'ad_df' in locals() and len(ad_df) > 0:
                ad_df.to_csv(ad_file, index=False)
                print(f"✅ Advance-Decline 데이터 저장 완료: {ad_file} (총 {len(ad_df)}개 레코드)")
            else:
                print("⚠️ 저장할 새로운 데이터가 없습니다.")
            
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
