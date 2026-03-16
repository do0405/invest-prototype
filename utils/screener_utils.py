#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
스크리너 공통 유틸리티 함수들
새로운 티커 추적, JSON/CSV 저장 등의 공통 기능 제공
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import glob
import re

from utils.io_utils import ensure_dir
from utils.typing_utils import Record


def convert_numpy_types(obj: Any) -> Any:
    """numpy 타입과 pandas Timestamp를 JSON 직렬화 가능한 Python native 타입으로 변환"""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Timestamp):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    elif hasattr(obj, 'timestamp'):  # pandas Timestamp 객체 처리
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    return obj


def find_latest_file(directory: str, prefix: str, extension: str) -> str | None:
    """
    디렉토리에서 특정 접두사를 가진 가장 최신 파일을 찾기
    시간 정보가 제거된 파일명도 처리
    
    Args:
        directory: 검색할 디렉토리
        prefix: 파일명 접두사
        extension: 파일 확장자 (점 제외)
    
    Returns:
        최신 파일의 전체 경로 또는 None
    """
    if not os.path.exists(directory):
        return None
    
    matching_files = []
    for file in os.listdir(directory):
        # 정확한 접두사 매칭: 접두사로 시작하고, 그 다음이 '_', '.', 또는 파일 끝
        if (file.startswith(prefix) and file.endswith(f'.{extension}') and
            (len(file) == len(prefix) + len(extension) + 1 or  # prefix.ext
             file[len(prefix)] in ['_', '.'])):
            file_path = os.path.join(directory, file)
            matching_files.append((file_path, os.path.getmtime(file_path)))
    
    if not matching_files:
        return None
    
    # 수정 시간 기준으로 가장 최신 파일 반환
    latest_file = max(matching_files, key=lambda x: x[1])[0]
    print(f"[Utils] find_latest_file: {os.path.basename(latest_file)} (총 {len(matching_files)}개 중)")
    return latest_file


def read_csv_flexible(file_path: str, required_columns: list[str] | None = None) -> pd.DataFrame | None:
    """
    CSV 파일을 유연하게 읽기 - 컬럼명 변화에 대응
    
    Args:
        file_path: CSV 파일 경로
        required_columns: 필수 컬럼 리스트 (없으면 모든 컬럼 허용)
    
    Returns:
        DataFrame 또는 None (읽기 실패 시)
    """
    if not os.path.exists(file_path):
        return None
    
    try:
        df = pd.read_csv(file_path)
        
        # 컬럼명 정규화 (소문자, 공백 제거)
        df.columns = [col.lower().strip() for col in df.columns]
        
        # VIX 데이터 특별 처리: vix_close -> close 매핑
        if 'vix_close' in df.columns and 'close' not in df.columns:
            df['close'] = df['vix_close']
            print(f"📊 VIX 데이터 매핑: vix_close -> close")
        
        # 기타 컬럼 매핑 처리
        column_mappings = {
            'vix_high': 'high',
            'vix_low': 'low',
            'vix_volume': 'volume'
        }
        
        for old_col, new_col in column_mappings.items():
            if old_col in df.columns and new_col not in df.columns:
                df[new_col] = df[old_col]
                print(f"📊 컬럼 매핑: {old_col} -> {new_col}")
        
        # 필수 컬럼 확인
        if required_columns:
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                print(f"⚠️ 필수 컬럼 누락: {missing_cols} in {file_path}")
                return None
        
        # 날짜 컬럼 처리 (다양한 형식 지원)
        date_columns = ['date', 'processing_date', '청산일시', 'added_date']
        for col in date_columns:
            if col in df.columns and not df[col].empty and not df[col].isna().all():
                try:
                    # 시간 정보가 포함된 경우 날짜만 추출
                    if df[col].dtype == 'object':
                        df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
                        if col in ['processing_date', '청산일시']:  # 시간 정보 제거가 필요한 컬럼
                            df[col] = df[col].dt.strftime('%Y-%m-%d')
                except Exception as e:
                    print(f"⚠️ 날짜 컬럼 '{col}' 처리 실패: {e}")
        
        return df
        
    except Exception as e:
        print(f"❌ CSV 파일 읽기 실패 ({file_path}): {e}")
        return None


def _write_records_with_optional_snapshot(records: list[Record], output_dir: str, filename_prefix: str, *, include_snapshot: bool = False) -> dict[str, str]:
    ensure_dir(output_dir)
    payload = convert_numpy_types(records)
    frame = pd.DataFrame(records)

    latest_csv_path = os.path.join(output_dir, f"{filename_prefix}.csv")
    latest_json_path = os.path.join(output_dir, f"{filename_prefix}.json")

    frame.to_csv(latest_csv_path, index=False, encoding="utf-8-sig")
    with open(latest_json_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)

    paths: dict[str, str] = {
        "csv_path": latest_csv_path,
        "json_path": latest_json_path,
    }

    if include_snapshot:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        snapshot_csv_path = os.path.join(output_dir, f"{filename_prefix}_{timestamp}.csv")
        snapshot_json_path = os.path.join(output_dir, f"{filename_prefix}_{timestamp}.json")
        frame.to_csv(snapshot_csv_path, index=False, encoding="utf-8-sig")
        with open(snapshot_json_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
        paths["snapshot_csv_path"] = snapshot_csv_path
        paths["snapshot_json_path"] = snapshot_json_path

    return paths


def save_screening_results(
    results: list[Record],
    output_dir: str,
    filename_prefix: str,
    include_timestamp: bool = False,
    incremental_update: bool = True,
) -> dict[str, str]:
    """
    Save screening results as latest JSON/CSV, with optional per-run snapshots.

    `incremental_update` is kept for backward compatibility; the latest files now
    always reflect the current run, while timestamped snapshot files preserve
    historical outputs when requested.
    """
    _ = incremental_update
    paths = _write_records_with_optional_snapshot(
        results,
        output_dir,
        filename_prefix,
        include_snapshot=include_timestamp,
    )

    print(f"[Results] Saved latest output: rows={len(results)}, prefix={filename_prefix}")
    print(f"   latest CSV: {paths['csv_path']}")
    print(f"   latest JSON: {paths['json_path']}")
    if paths.get("snapshot_csv_path"):
        print(f"   snapshot CSV: {paths['snapshot_csv_path']}")
        print(f"   snapshot JSON: {paths['snapshot_json_path']}")

    return paths

def track_new_tickers(
    current_results: list[Record],
    tracker_file: str,
    symbol_key: str = 'symbol',
    retention_days: int = 14,
) -> list[Record]:
    """
    새로운 티커를 추적하고 관리
    
    Args:
        current_results: 현재 스크리닝 결과
        tracker_file: 추적 파일 경로 (CSV)
        symbol_key: 심볼을 나타내는 키
        retention_days: 데이터 보존 기간 (일)
    
    Returns:
        새로 발견된 티커들의 리스트
    """
    current_symbols = {item[symbol_key] for item in current_results if symbol_key in item}
    
    # 기존 추적 데이터 로드
    if os.path.exists(tracker_file):
        existing_df = read_csv_flexible(tracker_file, [symbol_key])
        if existing_df is not None:
            existing_symbols = set(existing_df[symbol_key].tolist()) if symbol_key in existing_df.columns else set()
        else:
            print(f"⚠️  추적 파일 로드 실패: {tracker_file}")
            existing_symbols = set()
            existing_df = pd.DataFrame()
    else:
        existing_symbols = set()
        existing_df = pd.DataFrame()
    
    # 새로운 티커 식별
    new_symbols = current_symbols - existing_symbols
    
    if new_symbols:
        print(f"🆕 새로운 티커 발견: {len(new_symbols)}개")
        
        # 새로운 티커 데이터 생성
        new_ticker_data = []
        for symbol in new_symbols:
            # 현재 결과에서 해당 심볼의 데이터 찾기
            symbol_data = next((item for item in current_results if item.get(symbol_key) == symbol), {})
            
            new_ticker_data.append({
                symbol_key: symbol,
                'added_date': datetime.now().strftime('%Y-%m-%d'),
                'added_timestamp': datetime.now().timestamp(),
                **{k: v for k, v in symbol_data.items() if k != symbol_key}
            })
        
        # 기존 데이터와 병합
        if not existing_df.empty:
            # 오래된 데이터 제거 (retention_days 이상)
            cutoff_date = datetime.now().timestamp() - (retention_days * 24 * 3600)
            if 'added_timestamp' in existing_df.columns:
                existing_df = existing_df[existing_df['added_timestamp'] >= cutoff_date]
        
        # 새로운 데이터 추가
        new_df = pd.DataFrame(new_ticker_data)
        if not existing_df.empty:
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        else:
            combined_df = new_df
        
        # 추적 파일 저장
        ensure_dir(os.path.dirname(tracker_file))
        combined_df.to_csv(tracker_file, index=False, encoding='utf-8-sig')
        
        # JSON 파일도 저장
        json_file = tracker_file.replace('.csv', '.json')
        with open(json_file, 'w', encoding='utf-8') as f:
            # pandas Timestamp 등을 JSON 직렬화 가능한 형태로 변환
            data_dict = convert_numpy_types(combined_df.to_dict('records'))
            json.dump(data_dict, f, ensure_ascii=False, indent=2)
        
        print(f"   📄 추적 파일 업데이트: {tracker_file}")
        return new_ticker_data
    else:
        print("🔍 새로운 티커 없음")
        return []


def create_screener_summary(
    screener_name: str,
    total_candidates: int,
    new_tickers: int,
    results_paths: dict[str, str],
) -> dict[str, Any]:
    """
    스크리너 실행 요약 정보 생성
    
    Args:
        screener_name: 스크리너 이름
        total_candidates: 총 후보 종목 수
        new_tickers: 새로운 티커 수
        results_paths: 결과 파일 경로들
    
    Returns:
        요약 정보 딕셔너리
    """
    return {
        'screener_name': screener_name,
        'execution_time': datetime.now().isoformat(),
        'total_candidates': total_candidates,
        'new_tickers_found': new_tickers,
        'results_files': results_paths,
        'status': 'completed'
    }
