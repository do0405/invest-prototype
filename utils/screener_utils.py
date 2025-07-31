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


def convert_numpy_types(obj):
    """numpy 타입을 JSON 직렬화 가능한 Python native 타입으로 변환"""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    return obj


def save_screening_results(results: List[Dict[str, Any]], 
                          output_dir: str, 
                          filename_prefix: str,
                          include_timestamp: bool = True) -> Dict[str, str]:
    """
    스크리닝 결과를 JSON과 CSV 형태로 저장
    
    Args:
        results: 저장할 결과 리스트
        output_dir: 출력 디렉토리
        filename_prefix: 파일명 접두사
        include_timestamp: 타임스탬프 포함 여부
    
    Returns:
        저장된 파일 경로들 (csv_path, json_path)
    """
    ensure_dir(output_dir)
    
    # 파일명 생성
    if include_timestamp:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_filename = f"{filename_prefix}_{timestamp}"
    else:
        base_filename = filename_prefix
    
    csv_path = os.path.join(output_dir, f"{base_filename}.csv")
    json_path = os.path.join(output_dir, f"{base_filename}.json")
    
    if len(results) > 0:
        # DataFrame 생성 및 CSV 저장
        df = pd.DataFrame(results)
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        # JSON 저장 (numpy 타입 변환)
        converted_results = convert_numpy_types(results)
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(converted_results, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 결과 저장 완료: {len(results)}개 종목")
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
        try:
            existing_df = pd.read_csv(tracker_file)
            existing_symbols = set(existing_df[symbol_key].tolist()) if symbol_key in existing_df.columns else set()
        except Exception as e:
            print(f"⚠️  추적 파일 로드 실패: {e}")
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
            json.dump(combined_df.to_dict('records'), f, ensure_ascii=False, indent=2)
        
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


def enhance_screener_with_tracking(screener_func):
    """
    기존 스크리너 함수에 티커 추적 기능을 추가하는 데코레이터
    
    Args:
        screener_func: 원본 스크리너 함수
    
    Returns:
        향상된 스크리너 함수
    """
    def wrapper(*args, **kwargs):
        # 원본 스크리너 실행
        results = screener_func(*args, **kwargs)
        
        # 추가 처리 로직이 필요한 경우 여기에 구현
        return results
    
    return wrapper