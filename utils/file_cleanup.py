# -*- coding: utf-8 -*-
"""File cleanup utilities for managing timestamped files."""

import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

__all__ = [
    "extract_timestamp_from_filename",
    "cleanup_old_timestamped_files",
    "get_timestamped_files"
]


def extract_timestamp_from_filename(filename: str) -> Optional[datetime]:
    """파일명에서 타임스탬프 추출 (_YYYYMMDD 또는 _YYYY-MM-DD 형식)"""
    # _YYYYMMDD 형식 매칭
    match = re.search(r'_(\d{8})(?:\.[^.]+)?$', filename)
    if match:
        try:
            return datetime.strptime(match.group(1), '%Y%m%d')
        except ValueError:
            pass
    
    # _YYYY-MM-DD 형식 매칭
    match = re.search(r'_(\d{4}-\d{2}-\d{2})(?:\.[^.]+)?$', filename)
    if match:
        try:
            return datetime.strptime(match.group(1), '%Y-%m-%d')
        except ValueError:
            pass
    
    return None


def get_timestamped_files(directory: str, extensions: List[str] = None) -> List[tuple]:
    """디렉터리에서 타임스탬프가 포함된 파일들을 찾아 반환
    
    Args:
        directory: 검색할 디렉터리 경로
        extensions: 검색할 파일 확장자 리스트 (기본값: ['.csv', '.json'])
    
    Returns:
        List of tuples: (file_path, timestamp)
    """
    if extensions is None:
        extensions = ['.csv', '.json']
    
    timestamped_files = []
    
    if not os.path.exists(directory):
        return timestamped_files
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            # 확장자 확인
            if not any(file.lower().endswith(ext) for ext in extensions):
                continue
            
            # 타임스탬프 추출
            timestamp = extract_timestamp_from_filename(file)
            if timestamp:
                file_path = os.path.join(root, file)
                timestamped_files.append((file_path, timestamp))
    
    return timestamped_files


def cleanup_old_timestamped_files(directory: str, days_threshold: int = 30, 
                                 extensions: List[str] = None, dry_run: bool = False) -> dict:
    """지정된 기간보다 오래된 타임스탬프 파일들을 삭제
    
    Args:
        directory: 정리할 디렉터리 경로
        days_threshold: 삭제 기준 일수 (기본값: 30일)
        extensions: 대상 파일 확장자 (기본값: ['.csv', '.json'])
        dry_run: True면 실제 삭제하지 않고 목록만 반환
    
    Returns:
        dict: 삭제된 파일 정보
    """
    if extensions is None:
        extensions = ['.csv', '.json']
    
    cutoff_date = datetime.now() - timedelta(days=days_threshold)
    timestamped_files = get_timestamped_files(directory, extensions)
    
    deleted_files = []
    errors = []
    
    for file_path, timestamp in timestamped_files:
        if timestamp < cutoff_date:
            try:
                if not dry_run:
                    os.remove(file_path)
                    print(f"🗑️ 삭제됨: {os.path.relpath(file_path)} (날짜: {timestamp.strftime('%Y-%m-%d')})")
                else:
                    print(f"🔍 삭제 대상: {os.path.relpath(file_path)} (날짜: {timestamp.strftime('%Y-%m-%d')})")
                
                deleted_files.append({
                    'path': file_path,
                    'timestamp': timestamp,
                    'relative_path': os.path.relpath(file_path)
                })
            except Exception as e:
                error_msg = f"삭제 실패: {os.path.relpath(file_path)} - {str(e)}"
                errors.append(error_msg)
                print(f"❌ {error_msg}")
    
    result = {
        'deleted_count': len(deleted_files),
        'deleted_files': deleted_files,
        'errors': errors,
        'cutoff_date': cutoff_date,
        'dry_run': dry_run
    }
    
    if deleted_files:
        if dry_run:
            print(f"\n📋 총 {len(deleted_files)}개 파일이 삭제 대상입니다 ({days_threshold}일 이전)")
        else:
            print(f"\n✅ 총 {len(deleted_files)}개 파일을 삭제했습니다 ({days_threshold}일 이전)")
    else:
        print(f"\n📂 삭제할 오래된 파일이 없습니다 ({days_threshold}일 이전)")
    
    if errors:
        print(f"\n⚠️ {len(errors)}개 파일 삭제 중 오류 발생")
    
    return result