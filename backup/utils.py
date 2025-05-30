# -*- coding: utf-8 -*-
# 투자 스크리너 유틸리티 모듈

import os
import pandas as pd
import numpy as np
import glob
import concurrent.futures
import re
from datetime import datetime, timedelta
from scipy.stats import rankdata
from pytz import timezone

# 디렉토리 생성 함수
def ensure_dir(directory):
    """디렉토리가 없으면 생성하는 함수"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"📁 디렉토리 생성됨: {directory}")

# 필요한 디렉토리 생성 함수
def create_required_dirs(directories=None):
    """필요한 모든 디렉토리를 생성하는 함수
    
    Args:
        directories: 생성할 디렉토리 목록 (기본값: None, 이 경우 config.py에서 정의된 디렉토리 사용)
    """
    if directories is None:
        # config.py에서 정의된 디렉토리 가져오기
        from config import (
            DATA_DIR, DATA_US_DIR, RESULTS_DIR,
            RESULTS2_DIR, RESULTS_VER2_DIR, BACKUP_DIR, MARKMINERVINI_DIR
        )
        directories = [
            DATA_DIR, DATA_US_DIR, RESULTS_DIR,
            RESULTS2_DIR, RESULTS_VER2_DIR, BACKUP_DIR, MARKMINERVINI_DIR
        ]
    
    for directory in directories:
        ensure_dir(directory)

# CSV 파일 병렬 로드 함수
def load_csvs_parallel(file_paths, max_workers=4):
    """CSV 파일들을 병렬로 로드하는 함수
    
    Args:
        file_paths: CSV 파일 경로 리스트
        max_workers: 최대 병렬 작업자 수 (기본값: 4)
        
    Returns:
        dict: {파일명: DataFrame} 형태의 딕셔너리
    """
    results = {}
    
    def load_csv(file_path):
        try:
            df = pd.read_csv(file_path)
            return os.path.basename(file_path), df
        except Exception as e:
            print(f"❌ {file_path} 로드 오류: {e}")
            return os.path.basename(file_path), None
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {executor.submit(load_csv, file_path): file_path for file_path in file_paths}
        for future in concurrent.futures.as_completed(future_to_file):
            file_name, df = future.result()
            if df is not None:
                results[file_name] = df
    
    return results

# 미국 시장 날짜 가져오기 함수
def get_us_market_today():
    """미국 시장 기준 오늘 날짜를 반환하는 함수
    
    Returns:
        datetime: 미국 동부 시간대 기준 오늘 날짜
    """
    # 미국 동부 시간대 (ET) 사용
    et_now = datetime.now(timezone('US/Eastern'))
    return et_now.date()

# 티커 정리 함수
def clean_tickers(tickers):
    """티커 목록을 정리하는 함수
    
    Args:
        tickers: 정리할 티커 목록
        
    Returns:
        list: 정리된 티커 목록
    """
    if not tickers:
        return []
    
    # None, NaN 값 제거
    cleaned = [t for t in tickers if t is not None and not pd.isna(t)]
    
    # 문자열 변환 및 공백 제거
    cleaned = [str(t).strip() for t in cleaned]
    
    # 비정상적인 티커 필터링
    filtered = []
    for ticker in cleaned:
        # 빈 문자열이나 공백만 있는 경우 제외
        if not ticker or ticker.isspace():
            continue
            
        # 유효한 티커 심볼 패턴 검사 (알파벳, 숫자, 일부 특수문자만 허용)
        if not all(c.isalnum() or c in '.-$^' for c in ticker):
            # 로그 시작 부분
            log_msg = f"⚠️ 비정상적인 티커 제외: {ticker}"
            reasons = []
            
            # HTML 태그 패턴 감지
            if '<' in ticker or '>' in ticker:
                reasons.append("HTML 태그 포함")
            
            # CSS 스타일 코드 패턴 감지
            css_patterns = ['{', '}', ':', ';']
            css_keywords = ['width', 'height', 'position', 'margin', 'padding', 'color', 'background', 'font', 'display', 'style']
            
            if any(p in ticker for p in css_patterns):
                reasons.append("CSS 구문 포함")
            elif any(kw in ticker.lower() for kw in css_keywords):
                reasons.append("CSS 속성 포함")
            
            # JavaScript 코드 패턴 감지
            js_patterns = ['=', '(', ')', '[', ']', '&&', '||', '!', '?', '.']
            js_keywords = ['function', 'var', 'let', 'const', 'return', 'if', 'else', 'for', 'while', 'class', 'new', 'this', 'document', 'window']
            
            if any(p in ticker for p in js_patterns):
                reasons.append("JS 구문 포함")
            elif any(kw in ticker.lower() for kw in js_keywords):
                reasons.append("JS 키워드 포함")
            elif '.className' in ticker or 'RegExp' in ticker:
                reasons.append("JS API 포함")
            
            # JavaScript 주석 패턴 감지
            if ticker.strip().startswith('//') or ticker.strip().startswith('/*') or ticker.strip().endswith('*/'):
                reasons.append("JS 주석 포함")
            
            # 기타 비정상 패턴
            if not reasons:
                reasons.append("비정상 문자 포함")
            
            # 로그 출력
            print(f"{log_msg} - 이유: {', '.join(reasons)}")
            continue
        
        # 티커가 너무 길면 거부 (일반적인 티커는 1-5자)
        if len(ticker) > 8:
            print(f"⚠️ 너무 긴 티커 제외 ({len(ticker)}자): {ticker}")
            continue
            
        filtered.append(ticker)
    
    cleaned = filtered
    
    # 빈 문자열 제거
    cleaned = [t for t in cleaned if t]
    
    # HTML 태그가 포함된 티커 제거
    filtered = []
    for ticker in cleaned:
        if '<' in ticker or '>' in ticker:
            print(f"⚠️ HTML 태그가 포함된 티커 제외: {ticker}")
            continue
        # JavaScript 코드 패턴 감지 (함수 호출, 변수 할당 등)
        if '=' in ticker or '(' in ticker or ')' in ticker or '.className' in ticker or 'RegExp' in ticker:
            print(f"⚠️ JavaScript 코드로 추정되는 티커 제외: {ticker}")
            continue
        # JavaScript 주석 패턴 감지
        if ticker.strip().startswith('//') or ticker.strip().startswith('/*') or ticker.strip().endswith('*/'):
            print(f"⚠️ JavaScript 주석으로 추정되는 티커 제외: {ticker}")
            continue
        filtered.append(ticker)
    
    # 중복 제거
    filtered = list(set(filtered))
    
    return filtered

# 날짜 범위 생성 함수
def get_date_range(end_date=None, days=30):
    """지정된 종료일로부터 일정 기간의 날짜 범위를 생성하는 함수
    
    Args:
        end_date: 종료일 (기본값: 오늘)
        days: 일수 (기본값: 30일)
        
    Returns:
        tuple: (시작일, 종료일) 형태의 튜플
    """
    if end_date is None:
        end_date = datetime.now().date()
    elif isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    start_date = end_date - timedelta(days=days)
    
    return start_date, end_date

# 티커 추출 함수
def extract_ticker_from_filename(filename):
    """파일명에서 티커 심볼을 추출하는 함수
    
    Args:
        filename: 파일명 (예: 'AAPL.csv' 또는 'AAPL_data.csv')
        
    Returns:
        str: 티커 심볼
    """
    # 확장자 제거
    base_name = os.path.splitext(filename)[0]
    
    # 언더스코어(_) 이후 부분 제거
    ticker = base_name.split('_')[0]
    
    return ticker

# S&P 500 조건 확인 함수는 아래에 통합된 버전으로 정의되어 있습니다.

# 주식 데이터 처리 함수
def process_stock_data(file, data_dir, min_days=200, recent_days=200):
    """주식 데이터를 로드하고 처리하는 함수
    
    Args:
        file: 파일명
        data_dir: 데이터 디렉토리 경로
        min_days: 최소 필요 데이터 일수 (기본값: 200일)
        recent_days: 최근 데이터 추출 일수 (기본값: 200일)
        
    Returns:
        tuple: (심볼, 데이터프레임, 최근 데이터) 또는 처리 실패 시 (None, None, None)
    """
    try:
        file_path = os.path.join(data_dir, file)
        
        # Windows 예약 파일명 처리 - 파일명에서 원래 티커 추출
        symbol = extract_ticker_from_filename(file)
        
        # 개별 파일 로드
        df = pd.read_csv(file_path)
        
        # 컬럼명 소문자로 변환
        df.columns = [col.lower() for col in df.columns]
        
        # 날짜 컬럼 처리
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], utc=True)
            df = df.sort_values('date')
        else:
            return None, None, None
            
        # 최소 데이터 길이 확인
        if len(df) < min_days:
            return None, None, None
        
        # 최근 데이터 추출
        recent_data = df.iloc[-recent_days:].copy()
        
        return symbol, df, recent_data
        
    except Exception as e:
        print(f"❌ {file} 처리 오류: {e}")
        return None, None, None

# Windows 파일명 안전성 확보 함수
def safe_filename(filename):
    """Windows에서 사용할 수 없는 문자를 제거하여 안전한 파일명을 만드는 함수
    
    Args:
        filename: 원본 파일명
        
    Returns:
        str: 안전한 파일명
    """
    # Windows에서 파일명으로 사용할 수 없는 문자: < > : \\ / | ? *
    invalid_chars = r'[<>:\\\/|?*"]'
    safe_name = re.sub(invalid_chars, '_', filename)
    
    # Windows 예약 파일명 처리
    reserved_names = [
        'CON', 'PRN', 'AUX', 'NUL',
        'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
        'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
    ]
    
    name_without_ext = os.path.splitext(safe_name)[0]
    extension = os.path.splitext(safe_name)[1]
    
    if name_without_ext.upper() in reserved_names:
        safe_name = name_without_ext + '_file' + extension
    
    return safe_name

# ATR(Average True Range) 계산 함수
def calculate_atr(df, window=10):
    """ATR(Average True Range)를 계산하는 함수
    
    Args:
        df: 가격 데이터가 포함된 DataFrame (high, low, close 컬럼 필요)
        window: ATR 계산 기간 (기본값: 10일)
        
    Returns:
        pandas.Series: ATR 값 시리즈
    """
    try:
        # 필요한 컬럼 확인
        required_cols = ['high', 'low', 'close']
        for col in required_cols:
            if col not in df.columns:
                print(f"⚠️ ATR 계산에 필요한 '{col}' 컬럼이 없습니다.")
                return pd.Series(index=df.index)
        
        # 데이터 복사
        df = df.copy()
        
        # True Range 계산
        df['prev_close'] = df['close'].shift(1)
        df['tr1'] = abs(df['high'] - df['low'])
        df['tr2'] = abs(df['high'] - df['prev_close'])
        df['tr3'] = abs(df['low'] - df['prev_close'])
        df['true_range'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
        
        # ATR 계산 (단순 이동평균 사용)
        df['atr'] = df['true_range'].rolling(window=window).mean()
        
        return df['atr']
    except Exception as e:
        print(f"❌ ATR 계산 오류: {e}")
        return pd.Series(index=df.index)

# RSI(Relative Strength Index) 계산 함수
def calculate_rsi(df, window=14):
    """RSI(Relative Strength Index)를 계산하는 함수
    
    Args:
        df: 가격 데이터가 포함된 DataFrame (close 컬럼 필요)
        window: RSI 계산 기간 (기본값: 14일)
        
    Returns:
        pandas.Series: RSI 값 시리즈 (0-100 사이)
    """
    try:
        # 필요한 컬럼 확인
        if 'close' not in df.columns:
            print(f"⚠️ RSI 계산에 필요한 'close' 컬럼이 없습니다.")
            return pd.Series(index=df.index)
        
        # 종가 변화량 계산
        delta = df['close'].diff()
        
        # 상승/하락 구분
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        # 평균 상승/하락 계산
        avg_gain = gain.rolling(window=window).mean()
        avg_loss = loss.rolling(window=window).mean()
        
        # 상대적 강도(RS) 계산
        rs = avg_gain / avg_loss.where(avg_loss != 0, 1)  # 0으로 나누기 방지
        
        # RSI 계산
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    except Exception as e:
        print(f"❌ RSI 계산 오류: {e}")
        return pd.Series(index=df.index)

# ADX(Average Directional Index) 계산 함수
def calculate_adx(df, window=14):
    """ADX(Average Directional Index)를 계산하는 함수
    
    Args:
        df: 가격 데이터가 포함된 DataFrame (high, low, close 컬럼 필요)
        window: ADX 계산 기간 (기본값: 14일)
        
    Returns:
        pandas.Series: ADX 값 시리즈
    """
    try:
        # 필요한 컬럼 확인
        required_cols = ['high', 'low', 'close']
        for col in required_cols:
            if col not in df.columns:
                print(f"⚠️ ADX 계산에 필요한 '{col}' 컬럼이 없습니다.")
                return pd.Series(index=df.index)
        
        # 데이터 복사
        df = df.copy()
        
        # 가격 변화 계산
        df['prev_high'] = df['high'].shift(1)
        df['prev_low'] = df['low'].shift(1)
        df['prev_close'] = df['close'].shift(1)
        
        # +DM, -DM 계산
        df['up_move'] = df['high'] - df['prev_high']
        df['down_move'] = df['prev_low'] - df['low']
        
        # +DM 조건: 상승폭이 하락폭보다 크고, 상승폭이 양수일 때
        df['+dm'] = np.where(
            (df['up_move'] > df['down_move']) & (df['up_move'] > 0),
            df['up_move'],
            0
        )
        
        # -DM 조건: 하락폭이 상승폭보다 크고, 하락폭이 양수일 때
        df['-dm'] = np.where(
            (df['down_move'] > df['up_move']) & (df['down_move'] > 0),
            df['down_move'],
            0
        )
        
        # True Range 계산
        df['tr1'] = abs(df['high'] - df['low'])
        df['tr2'] = abs(df['high'] - df['prev_close'])
        df['tr3'] = abs(df['low'] - df['prev_close'])
        df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
        
        # 초기 평균값 계산
        df['+dm_avg'] = df['+dm'].rolling(window=window).mean()
        df['-dm_avg'] = df['-dm'].rolling(window=window).mean()
        df['tr_avg'] = df['tr'].rolling(window=window).mean()
        
        # +DI, -DI 계산
        df['+di'] = 100 * df['+dm_avg'] / df['tr_avg']
        df['-di'] = 100 * df['-dm_avg'] / df['tr_avg']
        
        # DX 계산
        df['dx'] = 100 * abs(df['+di'] - df['-di']) / (df['+di'] + df['-di'])
        
        # ADX 계산 (DX의 이동평균)
        df['adx'] = df['dx'].rolling(window=window).mean()
        
        return df['adx']
    except Exception as e:
        print(f"❌ ADX 계산 오류: {e}")
        return pd.Series(index=df.index)

# 역사적 변동성 계산 함수
def calculate_historical_volatility(df, window=60, annualize=True):
    """역사적 변동성을 계산하는 함수
    
    Args:
        df: 가격 데이터가 포함된 DataFrame (close 컬럼 필요)
        window: 변동성 계산 기간 (기본값: 60일)
        annualize: 연간화 여부 (기본값: True)
        
    Returns:
        float: 변동성 값 (백분율)
    """
    try:
        # 필요한 컬럼 확인
        if 'close' not in df.columns:
            print(f"⚠️ 변동성 계산에 필요한 'close' 컬럼이 없습니다.")
            return pd.Series(index=df.index)
        
        # 로그 수익률 계산
        df = df.copy()
        df['log_return'] = np.log(df['close'] / df['close'].shift(1))
        
        # 변동성 계산 (표준편차)
        volatility = df['log_return'].rolling(window=window).std()
        
        # 연간화 (252 거래일 기준)
        if annualize:
            volatility = volatility * np.sqrt(252) * 100
        else:
            volatility = volatility * 100
        
        return volatility
    except Exception as e:
        print(f"❌ 변동성 계산 오류: {e}")
        return pd.Series(index=df.index)

# S&P 500 조건 확인 함수
def check_sp500_condition(data_dir, ma_days=100):
    """SPY 종가가 지정된 이동평균선 위에 있는지 확인하는 함수
    
    Args:
        data_dir: 데이터 디렉토리 경로
        ma_days: 이동평균 기간 (기본값: 100일)
        
    Returns:
        bool: SPY 조건 충족 여부
    """
    try:
        # SPY 데이터 파일 경로
        spy_file = os.path.join(data_dir, 'SPY.csv')
        if not os.path.exists(spy_file):
            print("⚠️ SPY 데이터 파일을 찾을 수 없습니다.")
            return False  # SPY 데이터가 없으면 진행하지 않음
        
        # SPY 데이터 로드
        spy_df = pd.read_csv(spy_file)
        spy_df.columns = [col.lower() for col in spy_df.columns]
        
        if 'date' in spy_df.columns:
            spy_df['date'] = pd.to_datetime(spy_df['date'], utc=True)
            spy_df = spy_df.sort_values('date')
        else:
            print("⚠️ SPY 데이터에 날짜 컬럼이 없습니다.")
            return False  # 날짜 컬럼이 없으면 진행하지 않음
        
        # 최소 데이터 길이 확인
        if len(spy_df) < ma_days:
            print("⚠️ SPY 데이터가 충분하지 않습니다.")
            return False  # 데이터가 충분하지 않으면 진행하지 않음
        
        # 이동평균 계산
        spy_df[f'ma{ma_days}'] = spy_df['close'].rolling(window=ma_days).mean()
        
        # 최신 데이터 확인
        latest_spy = spy_df.iloc[-1]
        
        # 조건: SPY 종가가 이동평균선 위에 있음
        spy_condition = latest_spy['close'] > latest_spy[f'ma{ma_days}']
        
        if not spy_condition:
            print(f"⚠️ SPY 종가가 {ma_days}일 이동평균선 아래에 있습니다.")
            return False  # SPY 조건을 충족하지 않으면 진행하지 않음
        
        return True
        
    except Exception as e:
        print(f"❌ SPY 조건 확인 오류: {e}")
        return False  # 오류가 발생하면 진행하지 않음