#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
스크리너 공통 유틸리티 함수들
새로운 티커 추적, JSON/CSV 저장 등의 공통 기능 제공
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional
from utils import ensure_dir
import glob
import re


def convert_numpy_types(obj):
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


def find_latest_file(directory: str, prefix: str, extension: str) -> Optional[str]:
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


def read_csv_flexible(file_path: str, required_columns: List[str] = None) -> Optional[pd.DataFrame]:
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


def save_screening_results(results: List[Dict[str, Any]], 
                          output_dir: str, 
                          filename_prefix: str,
                          include_timestamp: bool = False,  # 기본값을 False로 변경
                          incremental_update: bool = True) -> Dict[str, str]:
    """
    스크리닝 결과를 JSON과 CSV 형태로 저장 (증분 업데이트 지원)
    
    Args:
        results: 저장할 결과 리스트
        output_dir: 출력 디렉토리
        filename_prefix: 파일명 접두사
        include_timestamp: 타임스탬프 포함 여부 (날짜만 포함)
        incremental_update: 증분 업데이트 여부
    
    Returns:
        저장된 파일 경로들 (csv_path, json_path)
    """
    ensure_dir(output_dir)
    
    # 파일명 생성 (시간 정보 없이)
    if include_timestamp:
        timestamp = datetime.now().strftime('%Y%m%d')
        base_filename = f"{filename_prefix}_{timestamp}"
    else:
        base_filename = filename_prefix
    
    csv_path = os.path.join(output_dir, f"{base_filename}.csv")
    json_path = os.path.join(output_dir, f"{base_filename}.json")
    
    # 증분 업데이트 시 기존 파일 찾기
    if incremental_update and not os.path.exists(csv_path):
        existing_csv = find_latest_file(output_dir, filename_prefix, 'csv')
        if existing_csv:
            csv_path = existing_csv
        existing_json = find_latest_file(output_dir, filename_prefix, 'json')
        if existing_json:
            json_path = existing_json
    
    if len(results) > 0:
        new_df = pd.DataFrame(results)
        
        # 증분 업데이트 처리
        if incremental_update and os.path.exists(csv_path):
            try:
                existing_df = read_csv_flexible(csv_path)
                if existing_df is None:
                    raise Exception("파일 읽기 실패")
                
                # 기본 키 컬럼 확인 (symbol 또는 첫 번째 컬럼)
                key_col = 'symbol' if 'symbol' in new_df.columns else new_df.columns[0]
                
                if key_col in existing_df.columns:
                    # 기존 데이터에서 새 데이터와 중복되는 항목 제거
                    existing_df = existing_df[~existing_df[key_col].isin(new_df[key_col])]
                    
                    # 기존 데이터와 새 데이터 병합 (빈 데이터프레임 처리)
                    if existing_df.empty and not new_df.empty:
                        combined_df = new_df.copy()
                    elif not existing_df.empty and new_df.empty:
                        combined_df = existing_df.copy()
                    elif not existing_df.empty and not new_df.empty:
                        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                    else:
                        combined_df = pd.DataFrame()
                    
                    # 정렬 유지 (기존 파일의 정렬 방식 확인)
                    if len(existing_df) > 1:
                        # 첫 번째 정렬 가능한 컬럼으로 정렬 방향 확인
                        sort_col = None
                        for col in combined_df.columns:
                            if combined_df[col].dtype in ['int64', 'float64', 'datetime64[ns]'] or col == key_col:
                                sort_col = col
                                break
                        
                        if sort_col:
                            # 기존 데이터의 정렬 방향 확인
                            if len(existing_df) >= 2:
                                is_ascending = existing_df[sort_col].iloc[0] <= existing_df[sort_col].iloc[1]
                                combined_df = combined_df.sort_values(sort_col, ascending=is_ascending)
                    
                    final_df = combined_df
                    print(f"🔄 증분 업데이트: 기존 {len(existing_df)}개 + 신규 {len(new_df)}개 = 총 {len(final_df)}개")
                else:
                    final_df = new_df
                    print(f"⚠️ 키 컬럼 '{key_col}' 불일치, 전체 교체: {len(new_df)}개")
            except Exception as e:
                print(f"⚠️ 기존 파일 읽기 실패 ({e}), 전체 교체: {len(new_df)}개")
                final_df = new_df
        else:
            final_df = new_df
            print(f"✅ 신규 저장: {len(new_df)}개 종목")
        
        # CSV 저장
        final_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        # JSON 저장 (numpy 타입 변환)
        if incremental_update and os.path.exists(json_path):
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    existing_json = json.load(f)
                
                # JSON도 동일하게 증분 업데이트
                key_col = 'symbol' if 'symbol' in results[0] else list(results[0].keys())[0]
                existing_keys = {item.get(key_col) for item in existing_json if key_col in item}
                new_items = [item for item in results if item.get(key_col) not in existing_keys]
                
                # 기존 항목에서 업데이트된 항목 제거
                updated_existing = [item for item in existing_json 
                                  if item.get(key_col) not in {r.get(key_col) for r in results}]
                
                combined_json = updated_existing + convert_numpy_types(results)
            except Exception:
                combined_json = convert_numpy_types(results)
        else:
            combined_json = convert_numpy_types(results)
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(combined_json, f, ensure_ascii=False, indent=2)
        
        print(f"   📄 CSV: {csv_path}")
        print(f"   📄 JSON: {json_path}")
    else:
        # 빈 결과 파일 생성
        pd.DataFrame().to_csv(csv_path, index=False)
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump([], f)
        
        print(f"⚠️  빈 결과 파일 생성: {csv_path}")
    
    return {
        'csv_path': csv_path,
        'json_path': json_path
    }


def track_new_tickers(current_results: List[Dict[str, Any]], 
                     tracker_file: str,
                     symbol_key: str = 'symbol',
                     retention_days: int = 14) -> List[Dict[str, Any]]:
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


def create_screener_summary(screener_name: str, 
                          total_candidates: int,
                          new_tickers: int,
                          results_paths: Dict[str, str]) -> Dict[str, Any]:
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