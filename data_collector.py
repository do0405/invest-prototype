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

from utils import (
    ensure_dir, get_us_market_today, clean_tickers, safe_filename
)

from config import (
    DATA_DIR, DATA_US_DIR, RESULTS_DIR
)

# NASDAQ, NYSE, ETF 티커 수집
def load_nasdaq_ftp_symbols():
    base_url = "https://www.nasdaqtrader.com/dynamic/SymDir"

    files = {
        "nasdaq": {
            "file": "nasdaqlisted.txt",
            "symbol_col": "Symbol",
            "test_col": "Test Issue"
        },
        "nyse": {
            "file": "otherlisted.txt",
            "symbol_col": "ACT Symbol",
            "test_col": "Test Issue"
        },
        "etf": {
            "file": "etf.txt",
            "symbol_col": None,
            "test_col": None
        }
    }

    all_symbols = []

    for name, meta in files.items():
        url = f"{base_url}/{meta['file']}"
        try:
            res = requests.get(url)
            content = res.text.strip().splitlines()
            if "File Creation Time" in content[-1]:
                content = content[:-1]
            reader = csv.reader(content, delimiter="|")
            rows = list(reader)

            if name == "etf":
                df = pd.DataFrame(rows[1:], columns=["Symbol", "Name", "IsEnabled"])
                symbols = df["Symbol"].dropna().astype(str).tolist()
            else:
                header, *data = rows
                df = pd.DataFrame(data, columns=header)
                symbol_col = meta["symbol_col"]
                test_col = meta["test_col"]
                if test_col in df.columns:
                    df = df[df[test_col] != "Y"]
                symbols = df[symbol_col].dropna().astype(str).tolist()

            # HTML 태그 및 JavaScript/CSS 코드가 포함된 티커 필터링
            filtered_symbols = []
            for symbol in symbols:
                # 빈 문자열이나 공백만 있는 경우 제외
                if not symbol or symbol.isspace():
                    continue
                    
                # 유효한 티커 심볼 패턴 검사 (알파벳, 숫자, 일부 특수문자만 허용)
                if not all(c.isalnum() or c in '.-$^' for c in symbol):
                    # 로그 시작 부분
                    log_msg = f"⚠️ 비정상적인 티커 제외: {symbol}"
                    reasons = []
                    
                    # HTML 태그 패턴 감지
                    if '<' in symbol or '>' in symbol:
                        reasons.append("HTML 태그 포함")
                    
                    # CSS 스타일 코드 패턴 감지
                    css_patterns = ['{', '}', ':', ';']
                    css_keywords = ['width', 'height', 'position', 'margin', 'padding', 'color', 'background', 'font', 'display', 'style']
                    
                    if any(p in symbol for p in css_patterns):
                        reasons.append("CSS 구문 포함")
                    elif any(kw in symbol.lower() for kw in css_keywords):
                        reasons.append("CSS 속성 포함")
                    
                    # JavaScript 코드 패턴 감지
                    js_patterns = ['=', '(', ')', '[', ']', '&&', '||', '!', '?', '.']
                    js_keywords = ['function', 'var', 'let', 'const', 'return', 'if', 'else', 'for', 'while', 'class', 'new', 'this', 'document', 'window']
                    
                    if any(p in symbol for p in js_patterns):
                        reasons.append("JS 구문 포함")
                    elif any(kw in symbol.lower() for kw in js_keywords):
                        reasons.append("JS 키워드 포함")
                    elif '.className' in symbol or 'RegExp' in symbol:
                        reasons.append("JS API 포함")
                    
                    # JavaScript 주석 패턴 감지
                    if symbol.strip().startswith('//') or symbol.strip().startswith('/*') or symbol.strip().endswith('*/'):
                        reasons.append("JS 주석 포함")
                    
                    # 기타 비정상 패턴
                    if not reasons:
                        reasons.append("비정상 문자 포함")
                    
                    # 로그 출력
                    print(f"{log_msg} - 이유: {', '.join(reasons)}")
                    continue
                
                # 티커 길이 제한 (일반적으로 1-5자)
                if len(symbol) > 8:
                    print(f"⚠️ 너무 긴 티커 제외 ({len(symbol)}자): {symbol}")
                    continue
                    
                filtered_symbols.append(symbol)
            
            all_symbols.extend(filtered_symbols)
            print(f"✅ {name.upper()} 종목 수: {len(filtered_symbols)}")
        except Exception as e:
            print(f"❌ {name.upper()} 로딩 실패: {e}")

    unique_cleaned = clean_tickers(all_symbols)
    print(f"🎯 최종 클린 티커 수: {len(unique_cleaned)}")
    return unique_cleaned

# 캐시된 NASDAQ 심볼 가져오기
def get_or_load_cached_nasdaq_symbols():
    nasdaq_cache_path = os.path.join(DATA_DIR, "nasdaq_symbols.csv")
    ensure_dir(os.path.dirname(nasdaq_cache_path))
    if os.path.exists(nasdaq_cache_path):
        # 캐시 파일이 24시간 이상 지났는지 확인
        file_time = datetime.fromtimestamp(os.path.getmtime(nasdaq_cache_path), tz=timezone('UTC'))
        if datetime.now(timezone('UTC')) - file_time < timedelta(hours=24):
            print("📂 NASDAQ 캐시 로드 중...")
            df = pd.read_csv(nasdaq_cache_path)
            return df['symbol'].tolist()
    
    print("🌐 NASDAQ 실시간 수집 중...")
    symbols = load_nasdaq_ftp_symbols()
    pd.DataFrame({'symbol': symbols}).to_csv(nasdaq_cache_path, index=False)
    return symbols

# 크라켄 관련 함수 제거됨

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
                existing = pd.read_csv(path, parse_dates=["date"])
                if "date" not in existing.columns:
                    raise ValueError("❌ 'date' 컬럼 없음")
                
                # 날짜 데이터를 UTC로 변환
                existing["date"] = pd.to_datetime(existing["date"], utc=True)
                
                # 빈 파일 확인 (상장 폐지 종목 표시용)
                if len(existing) == 0 and all(col in existing.columns for col in ["date", "symbol", "open", "high", "low", "close", "volume"]):
                    print(f"[US] 🚫 상장 폐지 종목 (이전에 확인됨): {ticker}")
                    return False
                
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
                if df_new is not None and df_new.empty and len(df_new.columns) > 0:
                    print(f"[US] 🚫 상장 폐지 종목 감지됨: {ticker}")
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
def collect_data(max_us_chunks=None, start_chunk=0):
    # 필요한 디렉토리 생성
    for directory in [DATA_DIR, DATA_US_DIR, RESULTS_DIR]:
        ensure_dir(directory)
        
    print("\n🇺🇸 미국 주식 데이터 수집 시작...")
    us_tickers = get_or_load_cached_nasdaq_symbols()
    fetch_and_save_us_ohlcv_chunked(
        tickers=us_tickers,
        save_dir=DATA_US_DIR,
        chunk_size=5,
        pause=5.0,
        start_chunk=start_chunk,
        max_chunks=max_us_chunks,
        max_workers=3
    )

# 명령행 인터페이스
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Mark Minervini 스크리너 - 데이터 수집")
    parser.add_argument("--max-us-chunks", type=int, help="최대 미국 주식 청크 수 제한")
    parser.add_argument("--start-chunk", type=int, default=0, help="시작할 청크 번호")
    
    args = parser.parse_args()
    
    collect_data(
        max_us_chunks=args.max_us_chunks,
        start_chunk=args.start_chunk
    )