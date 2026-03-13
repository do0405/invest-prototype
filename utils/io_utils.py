# -*- coding: utf-8 -*-
"""File and data loading utilities."""

import os
import pandas as pd
import concurrent.futures
import re
from datetime import datetime, timedelta

from config import (
    DATA_DIR,
    DATA_KR_DIR,
    DATA_US_DIR,
    RESULTS_DIR,
    QULLAMAGGIE_RESULTS_DIR,
    OPTION_RESULTS_DIR,
    BACKUP_DIR,
    MARKMINERVINI_DIR,
)

__all__ = [
    "ensure_dir",
    "ensure_directory_exists",
    "create_required_dirs",
    "load_csvs_parallel",
    "extract_ticker_from_filename",
    "process_stock_data",
    "safe_filename",
]


def ensure_dir(directory: str) -> None:
    """Create directory if it does not exist."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"📁 디렉토리 생성됨: {directory}")

# Alias for backward compatibility
ensure_directory_exists = ensure_dir


def create_required_dirs(directories=None) -> None:
    """Create a set of directories used in the project."""
    if directories is None:
        directories = [
            DATA_DIR,
            DATA_US_DIR,
            DATA_KR_DIR,
            RESULTS_DIR,
            QULLAMAGGIE_RESULTS_DIR,
            OPTION_RESULTS_DIR,
            BACKUP_DIR,
            MARKMINERVINI_DIR,
        ]

    for directory in directories:
        ensure_dir(directory)


def load_csvs_parallel(file_paths, max_workers=4):
    """Load multiple CSV files in parallel (thread-safe)."""
    results = {}
    temp_results = []  # 임시 결과 저장

    def load_csv(file_path):
        try:
            df = pd.read_csv(file_path)
            
            # 컬럼명을 소문자로 변환
            df.columns = [c.lower() for c in df.columns]
            
            # 필수 컬럼 존재 여부 확인
            required_columns = ['close', 'volume', 'date']
            if not all(col in df.columns for col in required_columns):
                print(f"⚠️ {os.path.basename(file_path)}: 필수 컬럼 누락 - {[col for col in required_columns if col not in df.columns]}")
                return os.path.basename(file_path), None
            
            return os.path.basename(file_path), df
        except Exception as e:
            print(f"❌ {file_path} 로드 오류: {e}")
            return os.path.basename(file_path), None

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {executor.submit(load_csv, file_path): file_path for file_path in file_paths}
        for future in concurrent.futures.as_completed(future_to_file):
            file_name, df = future.result()
            if df is not None:
                temp_results.append((file_name, df))
    
    # 결과 병합 (메인 스레드에서 안전하게 처리)
    for file_name, df in temp_results:
        results[file_name] = df

    return results


def extract_ticker_from_filename(filename: str) -> str:
    """Extract ticker symbol from file name."""
    base_name = os.path.splitext(filename)[0]
    ticker = base_name.split('_')[0]
    return ticker


def process_stock_data(file, data_dir, min_days=200, recent_days=200):
    """Load and preprocess stock data."""
    try:
        file_path = os.path.join(data_dir, file)
        symbol = extract_ticker_from_filename(file)
        from utils.screener_utils import read_csv_flexible
        df = read_csv_flexible(file_path, required_columns=['date', 'close'])
        if df is None:
            return None, None, None
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], utc=True)
            df = df.sort_values('date')
        else:
            return None, None, None
        if len(df) < min_days:
            return None, None, None
        recent_data = df.iloc[-recent_days:].copy()
        return symbol, df, recent_data
    except Exception as e:
        print(f"❌ {file} 처리 오류: {e}")
        return None, None, None


def safe_filename(filename: str) -> str:
    """Return a filesystem safe filename."""
    invalid_chars = r'[<>:\\/?*"|]'
    safe_name = re.sub(invalid_chars, '_', filename)
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
