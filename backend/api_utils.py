# -*- coding: utf-8 -*-
import pandas as pd
import os
import sys
import glob
from typing import Dict, List, Optional

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import RESULTS_DIR, RESULTS_VER2_DIR

class DataManager:
    """JSON 데이터 관리 클래스"""
    
    @staticmethod
    def get_latest_json_data(file_pattern: str) -> Optional[pd.DataFrame]:
        """패턴에 맞는 가장 최근 JSON 파일 데이터 반환"""
        files = glob.glob(file_pattern)
        if not files:
            return None
        
        latest_file = max(files, key=os.path.getctime)
        try:
            return pd.read_json(latest_file)
        except Exception:
            return None
    
    @staticmethod
    def get_json_data(file_path: str) -> Optional[pd.DataFrame]:
        """특정 JSON 파일 데이터 반환"""
        if not os.path.exists(file_path):
            return None
        
        try:
            return pd.read_json(file_path)
        except Exception:
            return None
    
    @staticmethod
    def save_json_data(df: pd.DataFrame, file_path: str) -> bool:
        """DataFrame을 JSON 파일로 저장"""
        try:
            # 디렉토리 생성
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            df.to_json(file_path, orient='records', indent=2, force_ascii=False)
            return True
        except Exception:
            return False
    
    @staticmethod
    def get_all_strategy_results() -> Dict[str, pd.DataFrame]:
        """모든 전략 결과 반환"""
        results = {}
        strategies = ['strategy1', 'strategy2', 'strategy3', 'strategy4', 'strategy5', 'strategy6']
        
        for strategy in strategies:
            json_file = os.path.join(RESULTS_VER2_DIR, f'{strategy}_results.json')
            data = DataManager.get_json_data(json_file)
            if data is not None:
                results[strategy] = data
        
        return results