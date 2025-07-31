#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Mark Minervini Screener - 데이터 수집 모듈

import os
import csv
import time
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from pytz import timezone
from concurrent.futures import ThreadPoolExecutor
from typing import List, Set

from utils import (
    ensure_dir, get_us_market_today, clean_tickers, safe_filename
)

from config import (
    DATA_DIR, DATA_US_DIR, RESULTS_DIR
)

# 주식 심볼 수집 (NASDAQ API 제거됨 - 타임아웃 문제로 인해)
# 대신 Yahoo Finance나 다른 안정적인 소스를 사용하는 것을 권장

# 크라켄 관련 함수 제거됨

def get_sp500_symbols() -> List[str]:
    """S&P 500 구성 종목 가져오기"""
    try:
        print("📈 S&P 500 종목 리스트 업데이트 중...")
        sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(sp500_url)
        sp500_df = tables[0]
        symbols = sp500_df['Symbol'].tolist()
        
        # 일부 기호 정리 (예: BRK.B -> BRK-B)
        cleaned_symbols = []
        for symbol in symbols:
            if '.' in symbol:
                symbol = symbol.replace('.', '-')
            cleaned_symbols.append(symbol)
        
        print(f"✅ S&P 500 종목 {len(cleaned_symbols)}개 수집 완료")
        return cleaned_symbols
    except Exception as e:
        print(f"⚠️ S&P 500 목록 가져오기 실패: {e}")
        return []

def get_nasdaq_100_symbols() -> List[str]:
    """NASDAQ 100 구성 종목 가져오기"""
    try:
        print("📈 NASDAQ 100 종목 리스트 업데이트 중...")
        nasdaq_url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        tables = pd.read_html(nasdaq_url)
        nasdaq_df = tables[4]  # NASDAQ 100 구성 종목 테이블
        symbols = nasdaq_df['Ticker'].tolist()
        
        print(f"✅ NASDAQ 100 종목 {len(symbols)}개 수집 완료")
        return symbols
    except Exception as e:
        print(f"⚠️ NASDAQ 100 목록 가져오기 실패: {e}")
        return []

def get_ipo_symbols() -> List[str]:
    """최근 IPO 종목에서 심볼 추출"""
    try:
        print("🏢 최근 IPO 종목 확인 중...")
        from screeners.ipo_investment.ipo_data_collector import RealIPODataCollector
        
        collector = RealIPODataCollector()
        result = collector.collect_all_ipo_data()
        
        symbols = []
        # 최근 IPO에서 심볼 추출
        for ipo in result.get('recent_ipos', []):
            symbol = ipo.get('symbol', ipo.get('ticker', ''))
            if symbol and symbol.strip():
                symbols.append(symbol.strip())
        
        # 예정된 IPO에서 심볼 추출
        for ipo in result.get('upcoming_ipos', []):
            symbol = ipo.get('symbol', ipo.get('ticker', ''))
            if symbol and symbol.strip():
                symbols.append(symbol.strip())
        
        # 중복 제거
        symbols = list(set(symbols))
        print(f"✅ IPO 종목 {len(symbols)}개 수집 완료")
        return symbols
    except Exception as e:
        print(f"⚠️ IPO 종목 가져오기 실패: {e}")
        return []

def update_symbol_list() -> Set[str]:
    """새로운 종목 리스트 업데이트"""
    print("\n🔄 종목 리스트 업데이트 시작...")
    
    # 기존 종목 목록
    try:
        from data_collectors.stock_metadata_collector import get_symbols
        existing_symbols = set(get_symbols())
        print(f"📊 기존 종목 수: {len(existing_symbols)}개")
    except Exception as e:
        print(f"⚠️ 기존 종목 로드 실패: {e}")
        existing_symbols = set()
    
    # 새로운 종목 수집
    new_symbols = set()
    
    # S&P 500 종목 추가
    sp500_symbols = get_sp500_symbols()
    new_symbols.update(sp500_symbols)
    
    # NASDAQ 100 종목 추가
    nasdaq_symbols = get_nasdaq_100_symbols()
    new_symbols.update(nasdaq_symbols)
    
    # IPO 종목 추가
    ipo_symbols = get_ipo_symbols()
    new_symbols.update(ipo_symbols)
    
    # 기존에 없는 새로운 종목만 필터링
    truly_new_symbols = new_symbols - existing_symbols
    
    if truly_new_symbols:
        print(f"🆕 새로 발견된 종목: {len(truly_new_symbols)}개")
        print(f"   예시: {list(truly_new_symbols)[:10]}")
        
        # 새로운 종목들의 빈 CSV 파일 생성 (다음 수집 시 포함되도록)
        for symbol in truly_new_symbols:
            try:
                safe_symbol = safe_filename(symbol)
                csv_path = os.path.join(DATA_US_DIR, f"{safe_symbol}.csv")
                if not os.path.exists(csv_path):
                    # 빈 CSV 파일 생성 (헤더만)
                    empty_df = pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume"])
                    empty_df.to_csv(csv_path, index=False)
                    print(f"📝 새 종목 파일 생성: {symbol}")
            except Exception as e:
                print(f"⚠️ {symbol} 파일 생성 실패: {e}")
    else:
        print("✅ 새로운 종목이 없습니다.")
    
    # 전체 종목 목록 반환
    all_symbols = existing_symbols.union(new_symbols)
    print(f"📈 총 종목 수: {len(all_symbols)}개")
    return all_symbols

# 미국 주식 단일 종목 데이터 가져오기
def fetch_us_single(ticker, start, end):
    """yfinance API를 사용하여 단일 종목의 주가 데이터를 가져오는 함수
    
    Args:
        ticker: 주식 티커 심볼
        start: 시작 날짜
        end: 종료 날짜
        
    Returns:
        DataFrame: 주가 데이터 또는 None(오류 발생 시)
    """
    try:
        # 요청 전 짧은 대기 추가 (API 제한 방지)
        time.sleep(0.5)
        
        # 타임아웃 설정 추가
        print(f"[US] 📊 데이터 요청 중: {ticker} ({start} ~ {end})")
        ticker_obj = yf.Ticker(ticker)
        
        # income_stmt에서 Net Income 가져오기
        try:
            income_stmt = ticker_obj.income_stmt
            if income_stmt is not None and not income_stmt.empty:
                net_income = income_stmt.loc['Net Income'] if 'Net Income' in income_stmt.index else None
                if net_income is not None:
                    print(f"[US] ✅ Net Income 데이터 수신 성공: {ticker}")
        except Exception as e:
            print(f"[US] ⚠️ Net Income 데이터 수집 실패: {ticker} - {str(e)}")
        
        # 주가 데이터 가져오기
        df = ticker_obj.history(start=start, end=end, interval="1d",
                               auto_adjust=False, actions=False, timeout=10)
        
        if df.empty:
            print(f"[US] ❌ 빈 데이터 반환됨: {ticker}")
            # 빈 데이터도 상장 폐지 종목으로 처리 (데이터가 없는 경우)
            return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume"])
            
        print(f"[US] ✅ 데이터 수신 성공: {ticker} ({len(df)} 행)")
        df = df.rename_axis("date").reset_index()
        df["symbol"] = ticker
        return df
    except requests.exceptions.ReadTimeout:
        print(f"[US] ⏱️ 타임아웃 발생: {ticker} - API 응답 지연 (재시도 필요)")
        return None
    except requests.exceptions.ConnectionError:
        print(f"[US] 🌐 연결 오류: {ticker} - 네트워크 문제 발생 (재시도 필요)")
        return None
    except Exception as e:
        error_msg = str(e).lower()
        # 상장 폐지 종목 감지 및 처리 (감지 조건 확장)
        delisted_keywords = ["delisted", "no timezone found", "possibly delisted", "not found", 
                            "invalid ticker", "symbol may be delisted", "404"]
        if any(keyword in error_msg for keyword in delisted_keywords):
            print(f"[US] 🚫 상장 폐지 종목 감지됨: {ticker}")
            # 빈 파일 생성하여 다음에 다시 시도하지 않도록 함
            return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume"])
        elif "rate limit" in error_msg or "429" in error_msg:
            print(f"[US] 🚫 API 제한 도달: {ticker} - 잠시 후 다시 시도하세요")
        else:
            print(f"[US] ❌ 오류 발생 ({ticker}): {error_msg[:100]}")
        return None

# 미국 주식 데이터 수집 및 저장 (병렬 처리 버전)
def fetch_and_save_us_ohlcv_chunked(tickers, save_dir=DATA_US_DIR, chunk_size=5, pause=5.0, start_chunk=0, max_chunks=None, max_workers=3):
    ensure_dir(save_dir)
    today = get_us_market_today()
    
    # 진행 상황 저장 파일
    progress_file = os.path.join(DATA_DIR, "fetch_progress.txt")
    
    # 총 청크 수 계산
    total_chunks = (len(tickers) + chunk_size - 1) // chunk_size
    if max_chunks is not None:
        total_chunks = min(total_chunks, start_chunk + max_chunks)

    # 시작 청크 설정
    if start_chunk > 0:
        print(f"🔄 청크 {start_chunk}부터 다시 시작합니다 (총 {total_chunks} 청크)")
    
    # 티커 처리 함수 정의 (병렬 처리용)
    def process_ticker(ticker):
        # Windows 예약 파일명 처리
        safe_ticker = safe_filename(ticker)
        path = os.path.join(save_dir, f"{safe_ticker}.csv")
        if os.path.exists(path):
            try:
                existing = pd.read_csv(path)
                existing["date"] = pd.to_datetime(existing["date"], utc=True)
                if "date" not in existing.columns:
                    raise ValueError("❌ 'date' 컬럼 없음")

                # 날짜 데이터를 UTC로 변환
                existing["date"] = pd.to_datetime(existing["date"], utc=True)

                if len(existing) == 0:
                    # 헤더만 존재하는 빈 파일의 경우 새로 수집
                    print(f"[US] 📊 빈 파일 감지, 새로 데이터 수집: {ticker}")
                    existing = None
                    start_date = today - timedelta(days=450)
                else:
                    # 날짜 컬럼이 UTC 시간대로 변환되었는지 확인
                    if not pd.api.types.is_datetime64tz_dtype(existing["date"]):
                        existing["date"] = pd.to_datetime(existing["date"], utc=True)

                    # 330 영업일 제한 적용 (데이터가 330일 이상인 경우 오래된 데이터 제거)
                    if len(existing) > 330:
                        print(f"[US] ✂️ {ticker}: 330 영업일 초과 데이터 정리 중 ({len(existing)} → 330)")
                        existing = existing.sort_values("date", ascending=False).head(330).reset_index(drop=True)
                        # 오래된 데이터가 위에 오도록 다시 정렬
                        existing = existing.sort_values("date", ascending=True).reset_index(drop=True)

                    last_date = existing["date"].dropna().max().date()
                    start_date = last_date + timedelta(days=1)
            except Exception as e:
                print(f"⚠️ {ticker} 기존 파일 오류: {e}")
                existing = None
                start_date = today - timedelta(days=450)
        else:
            existing = None
            start_date = today - timedelta(days=450)

        if start_date >= today:
            print(f"[US] ⏩ 최신 상태 (또는 오늘은 거래일 아님): {ticker}")
            return False

        print(f"[DEBUG] {ticker}: 수집 시작일 {start_date}, 종료일 {today}")

        df_new = None
        for j in range(5):  # 재시도 횟수 증가
            try:
                df_new = fetch_us_single(ticker, start=start_date, end=today)
                
                # 상장 폐지 종목 처리 (빈 DataFrame이 반환된 경우)
                if df_new is not None and df_new.empty:
                    # 기존 데이터가 있으면 상장폐지로 처리하지 않음
                    if existing is not None and len(existing) > 0:
                        print(f"[US] ⏩ 새 데이터 없음, 기존 데이터 유지: {ticker}")
                        return False
                    else:
                        print(f"[US] 🚫 상장 폐지 종목 감지됨: {ticker}")
                        # 빈 DataFrame에 컬럼이 있으면 그대로 저장, 없으면 기본 컬럼 추가
                        if len(df_new.columns) == 0:
                            df_new = pd.DataFrame(columns=['date', 'symbol', 'open', 'high', 'low', 'close', 'volume'])
                        df_new.to_csv(path, index=False)
                        return True
                elif df_new is not None and not df_new.empty:
                    # 정상 데이터 획득
                    break
                    
                print(f"[US] ⚠️ {ticker} 빈 데이터 반환, 재시도 {j+1}/5")
            except Exception as e:
                error_msg = str(e).lower()
                # 상장 폐지 관련 오류인지 확인
                delisted_keywords = ["delisted", "no timezone", "possibly delisted", "not found", "invalid ticker", "404"]
                if any(keyword in error_msg for keyword in delisted_keywords):
                    print(f"[US] 🚫 상장 폐지 종목 감지됨: {ticker}")
                    # 빈 파일 생성
                    empty_df = pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume"])
                    empty_df.to_csv(path, index=False)
                    return True
                    
                wait = 2 ** j + 2  # 대기 시간 증가
                print(f"[US] ⚠️ {ticker} 재시도 {j+1}/5 → {wait}s 대기: {error_msg[:100]}")
                time.sleep(wait)
            
            # 마지막 시도가 아니면 추가 대기
            if j < 4:  # 마지막 시도 전까지
                time.sleep(1)  # API 요청 사이에 추가 대기

        if df_new is None or df_new.empty:
            print(f"[US] ❌ 빈 데이터: {ticker}")
            # 기존 데이터가 있으면 상장폐지로 처리하지 않음
            if existing is not None and len(existing) > 0:
                print(f"[US] ⏩ 새 데이터 없음, 기존 데이터 유지: {ticker}")
                return False
            else:
                # 여러 번 시도해도 데이터가 없으면 상장 폐지로 간주
                empty_df = pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume"])
                empty_df.to_csv(path, index=False)
                print(f"[US] 🚫 데이터 없음 - 상장 폐지로 처리: {ticker}")
                return True

        if existing is not None:
            before_len = len(existing)
            
            # 날짜 형식 통일 (모든 날짜를 UTC 시간대로 변환)
            if not pd.api.types.is_datetime64tz_dtype(existing["date"]):
                existing["date"] = pd.to_datetime(existing["date"], utc=True)
            if not pd.api.types.is_datetime64tz_dtype(df_new["date"]):
                df_new["date"] = pd.to_datetime(df_new["date"], utc=True)
                
            # 날짜 문자열 형식으로 변환하여 중복 제거 (시간대 문제 해결)
            existing["date_str"] = existing["date"].dt.strftime("%Y-%m-%d")
            df_new["date_str"] = df_new["date"].dt.strftime("%Y-%m-%d")
            
            # 기존 데이터에서 새 데이터와 중복되는 날짜 제거
            existing_filtered = existing[~existing["date_str"].isin(df_new["date_str"])]
            
            # 데이터 병합
            df_combined = pd.concat([existing_filtered, df_new], ignore_index=True)
            
            # 임시 컬럼 제거
            df_combined.drop("date_str", axis=1, inplace=True)
            
            # 330 영업일 제한 적용 (데이터 병합 후 다시 확인)
            if len(df_combined) > 330:
                print(f"[US] ✂️ {ticker}: 병합 후 330 영업일 초과 데이터 정리 중 ({len(df_combined)} → 330)")
                df_combined = df_combined.sort_values("date", ascending=False).head(330).reset_index(drop=True)
            
            # 최종 저장 전 오래된 데이터가 위에 오도록 정렬
            df_combined = df_combined.sort_values("date", ascending=True).reset_index(drop=True)
            
            after_len = len(df_combined)

            # 항상 저장하여 데이터 업데이트 보장
            df_combined.to_csv(path, index=False)
            if after_len > before_len:
                print(f"[US] ✅ 저장됨: {ticker} ({after_len} rows, +{after_len - before_len})")
            else:
                print(f"[US] 🔄 데이터 업데이트됨: {ticker} ({after_len} rows)")
            return True
        else:
            # 신규 데이터도 330 영업일 제한 적용
            if len(df_new) > 330:
                print(f"[US] ✂️ {ticker}: 신규 데이터 330 영업일 초과 정리 중 ({len(df_new)} → 330)")
                df_new = df_new.sort_values("date", ascending=False).head(330).reset_index(drop=True)
            
            # 최종 저장 전 오래된 데이터가 위에 오도록 정렬
            df_new = df_new.sort_values("date", ascending=True).reset_index(drop=True)
                
            df_new.to_csv(path, index=False)
            print(f"[US] ✅ 신규 저장: {ticker} ({len(df_new)} rows)")
            return True
    
    # 청크별 처리 시작
    for i in range(start_chunk * chunk_size, len(tickers), chunk_size):
        chunk_num = i // chunk_size
        
        # 최대 청크 수 제한 확인
        if max_chunks is not None and chunk_num >= start_chunk + max_chunks:
            print(f"🛑 최대 청크 수 ({max_chunks}) 도달. 작업 중단.")
            break
            
        chunk = tickers[i:i+chunk_size]
        
        # NaN 값 필터링
        chunk = [t for t in chunk if isinstance(t, str) or (isinstance(t, (int, float)) and not pd.isna(t))]
        
        print(f"\n⏱️ Chunk {chunk_num + 1}/{total_chunks} 시작 ({len(chunk)}개): {chunk}")
        
        # 진행 상황 저장
        with open(progress_file, 'w') as f:
            f.write(str(chunk_num))
        
        # 청크 내 티커 병렬 처리
        success_count = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 병렬로 티커 처리 실행
            results = list(executor.map(process_ticker, chunk))
            success_count = sum(1 for result in results if result)
        
        # 청크 완료 후 상태 출력 및 대기
        print(f"✅ 청크 {chunk_num + 1}/{total_chunks} 완료: {success_count}/{len(chunk)} 성공")
        
        # API 제한 방지를 위한 대기
        if chunk_num + 1 < total_chunks:  # 마지막 청크가 아니면 대기
            print(f"⏳ {pause}초 대기 중...")
            time.sleep(pause)

# 크라켄 관련 함수 제거됨

# 메인 데이터 수집 함수
def collect_data(max_us_chunks=None, start_chunk=0, update_symbols=True):
    # 필요한 디렉토리 생성
    for directory in [DATA_DIR, DATA_US_DIR, RESULTS_DIR]:
        ensure_dir(directory)
        
    print("\n🇺🇸 미국 주식 데이터 수집 시작...")
    
    # 종목 리스트 업데이트 (선택적)
    if update_symbols:
        try:
            print("\n🔄 1단계: 종목 리스트 업데이트")
            all_symbols = update_symbol_list()
            us_tickers = list(all_symbols)
        except Exception as e:
            print(f"⚠️ 종목 리스트 업데이트 실패: {e}")
            print("📊 기존 CSV 파일 기반으로 진행합니다.")
            # 기존 방식으로 폴백
            from data_collectors.stock_metadata_collector import get_symbols
            us_tickers = get_symbols()
    else:
        print("\n📊 기존 CSV 파일들을 기반으로 종목 목록 생성")
        from data_collectors.stock_metadata_collector import get_symbols
        us_tickers = get_symbols()
    
    if not us_tickers:
        print("⚠️ 종목을 찾을 수 없습니다. 데이터 수집을 중단합니다.")
        return
    
    print(f"\n📊 2단계: 총 {len(us_tickers)}개 종목의 OHLCV 데이터를 업데이트합니다.")
    
    # OHLCV 데이터 수집 실행
    try:
        fetch_and_save_us_ohlcv_chunked(
            tickers=us_tickers,
            save_dir=DATA_US_DIR,
            chunk_size=10,
            pause=2.0,
            start_chunk=start_chunk,
            max_chunks=max_us_chunks,
            max_workers=5
        )
        
    except Exception as e:
        print(f"❌ OHLCV 데이터 수집 중 오류 발생: {e}")
        print("⚠️ 데이터 수집을 중단합니다.")

# 명령행 인터페이스
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Mark Minervini 스크리너 - 데이터 수집")
    parser.add_argument("--max-us-chunks", type=int, help="최대 미국 주식 청크 수 제한")
    parser.add_argument("--start-chunk", type=int, default=0, help="시작할 청크 번호")
    parser.add_argument("--no-symbol-update", action="store_true", help="종목 리스트 업데이트 건너뛰기")
    
    args = parser.parse_args()
    
    collect_data(
        max_us_chunks=args.max_us_chunks,
        start_chunk=args.start_chunk,
        update_symbols=not args.no_symbol_update
    )