# -*- coding: utf-8 -*-

import pandas as pd
import os
import sys
from typing import Dict, Any, List

from utils.path_utils import add_project_root

# 프로젝트 루트 디렉토리를 Python 경로에 추가
add_project_root()

from .api_utils import DataManager

class BackendWrapper:
    """기존 기능을 백엔드 API 형태로 래핑"""
    
    def __init__(self):
        self.data_manager = DataManager()
    
    def get_screening_summary(self) -> Dict[str, Any]:
        """전체 스크리닝 요약 정보 반환"""
        summary = {
            'technical_screening': None,
            'financial_screening': None,
            'integrated_screening': None,
            'volatility_skew': None,
            'strategies': {}
        }
        
        # 기술적 스크리닝
        tech_data = self.data_manager.get_json_data('results/us_with_rs.json')
        if tech_data is not None:
            summary['technical_screening'] = {
                'count': len(tech_data),
                'top_5': tech_data.head(5).to_dict('records')
            }
        
        # 재무제표 스크리닝
        fin_data = self.data_manager.get_json_data('results/advanced_financial_results.json')
        if fin_data is not None:
            summary['financial_screening'] = {
                'count': len(fin_data),
                'top_5': fin_data.head(5).to_dict('records')
            }
        
        # 통합 스크리닝
        int_data = self.data_manager.get_json_data('results/screeners/markminervini/integrated_results.json')
        if int_data is not None:
            summary['integrated_screening'] = {
                'count': len(int_data),
                'top_5': int_data.head(5).to_dict('records')
            }
        
        # 전략별 결과
        strategy_results = self.data_manager.get_all_strategy_results()
        for strategy, data in strategy_results.items():
            summary['strategies'][strategy] = {
                'count': len(data),
                'active_positions': len(data[data['롱여부'] == 'True']) if '롱여부' in data.columns else 0
            }
        
        return summary