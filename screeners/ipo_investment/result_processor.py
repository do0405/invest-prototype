# -*- coding: utf-8 -*-
"""IPO 결과 처리 모듈"""

import pandas as pd
import os
import logging
from typing import List, Dict
from datetime import datetime
from config import IPO_INVESTMENT_RESULTS_DIR

logger = logging.getLogger(__name__)


class IPOResultProcessor:
    """IPO 결과 처리 클래스"""
    
    def __init__(self, today: datetime):
        """초기화"""
        self.today = today
    
    def create_empty_result_files(self):
        """skip_data 모드에서 빈 결과 파일들을 생성"""
        # 빈 데이터프레임 칼럼 정의
        base_columns = ['ticker', 'company_name', 'ipo_date', 'ipo_price', 'days_since_ipo', 
                       'pattern_type', 'current_price', 'score', 'date']
        track_columns = ['ticker', 'company_name', 'ipo_date', 'ipo_price', 'track', 
                        'days_since_ipo', 'current_price', 'price_vs_ipo', 'date']
        
        # 파일 경로 정의 (타임스탬프 포함 파일명 사용)
        timestamp = self.today.strftime('%Y%m%d')
        files_to_create = [
            (f"ipo_base_{timestamp}.csv", base_columns),
            (f"ipo_breakout_{timestamp}.csv", base_columns),
            (f"ipo_track1_{timestamp}.csv", track_columns),
            (f"ipo_track2_{timestamp}.csv", track_columns)
        ]
        
        # 빈 파일들 생성
        for filename, columns in files_to_create:
            file_path = os.path.join(IPO_INVESTMENT_RESULTS_DIR, filename)
            empty_df = pd.DataFrame(columns=columns)
            empty_df.to_csv(file_path, index=False)
            empty_df.to_json(file_path.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            logger.info(f"빈 결과 파일 생성: {file_path}")
    
    def save_results(self, base_results: List[Dict], breakout_results: List[Dict], 
                    track1_results: List[Dict], track2_results: List[Dict]) -> pd.DataFrame:
        """결과 저장"""
        # 결과를 데이터프레임으로 변환
        base_df = pd.DataFrame(base_results) if base_results else pd.DataFrame()
        breakout_df = pd.DataFrame(breakout_results) if breakout_results else pd.DataFrame()
        track1_df = pd.DataFrame(track1_results) if track1_results else pd.DataFrame()
        track2_df = pd.DataFrame(track2_results) if track2_results else pd.DataFrame()
        
        # 베이스 패턴 결과 저장
        self._save_base_results(base_df)
        
        # 브레이크아웃 결과 저장
        self._save_breakout_results(breakout_df)
        
        # Track1 결과 저장
        self._save_track1_results(track1_df)
        
        # Track2 결과 저장
        self._save_track2_results(track2_df)
        
        # 통합 결과 반환
        dfs = [base_df, breakout_df, track1_df, track2_df]
        dfs = [d for d in dfs if not d.empty]
        combined_results = pd.concat(dfs) if dfs else pd.DataFrame()
        
        return combined_results
    
    def _save_base_results(self, base_df: pd.DataFrame):
        """베이스 패턴 결과 저장"""
        base_output_file = os.path.join(IPO_INVESTMENT_RESULTS_DIR, 
                                       f"ipo_base_{self.today.strftime('%Y%m%d')}.csv")
        if not base_df.empty:
            base_df = base_df.sort_values('score', ascending=False)
            base_df.to_csv(base_output_file, index=False)
            base_df.to_json(base_output_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            logger.info(f"IPO 베이스 패턴 결과 저장 완료: {base_output_file} ({len(base_df)}개 종목)")
        else:
            # 빈 데이터프레임에 완전한 칼럼명 추가 (코드와 일치)
            empty_base_df = pd.DataFrame(columns=[
                'ticker', 'company_name', 'ipo_date', 'ipo_price', 'days_since_ipo', 
                'pattern_type', 'current_price', 'score', 'date', 'screening_date',
                'base_score', 'pattern_formation_date', 'base_depth', 'base_length',
                'volume_dry_up', 'rs_rating', 'sector_strength'
            ])
            empty_base_df.to_csv(base_output_file, index=False)
            empty_base_df.to_json(base_output_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            logger.info(f"IPO 베이스 패턴 조건을 만족하는 종목이 없습니다. 빈 파일 생성: {base_output_file}")
    
    def _save_breakout_results(self, breakout_df: pd.DataFrame):
        """브레이크아웃 결과 저장"""
        breakout_output_file = os.path.join(IPO_INVESTMENT_RESULTS_DIR,
                                          f"ipo_breakout_{self.today.strftime('%Y%m%d')}.csv")
        if not breakout_df.empty:
            breakout_df = breakout_df.sort_values('score', ascending=False)
            breakout_df.to_csv(breakout_output_file, index=False)
            breakout_df.to_json(breakout_output_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            logger.info(f"IPO 브레이크아웃 결과 저장 완료: {breakout_output_file} ({len(breakout_df)}개 종목)")
        else:
            # 빈 데이터프레임에 완전한 칼럼명 추가 (코드와 일치)
            empty_breakout_df = pd.DataFrame(columns=[
                'ticker', 'company_name', 'ipo_date', 'ipo_price', 'days_since_ipo', 
                'pattern_type', 'current_price', 'score', 'date', 'screening_date',
                'breakout_score', 'pattern_formation_date', 'breakout_volume',
                'price_strength', 'rs_rating', 'sector_strength'
            ])
            empty_breakout_df.to_csv(breakout_output_file, index=False)
            empty_breakout_df.to_json(breakout_output_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            logger.info(f"IPO 브레이크아웃 조건을 만족하는 종목이 없습니다. 빈 파일 생성: {breakout_output_file}")
    
    def _save_track1_results(self, track1_df: pd.DataFrame):
        """Track1 결과 저장"""
        track1_output_file = os.path.join(IPO_INVESTMENT_RESULTS_DIR,
                                         f"ipo_track1_{self.today.strftime('%Y%m%d')}.csv")
        if not track1_df.empty:
            track1_df.to_csv(track1_output_file, index=False)
            track1_df.to_json(track1_output_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            logger.info(f"Track1 결과 저장 완료: {track1_output_file} ({len(track1_df)}개 종목)")
        else:
            # 빈 데이터프레임에 완전한 칼럼명 추가 (코드와 일치)
            empty_track1_df = pd.DataFrame(columns=[
                'ticker', 'company_name', 'ipo_date', 'ipo_price', 'days_since_ipo', 
                'pattern_type', 'current_price', 'score', 'date', 'screening_date',
                'track1_score', 'price_cond', 'rsi_cond', 'support_touch',
                'volume_cond', 'environment_cond', 'fundamental_cond'
            ])
            empty_track1_df.to_csv(track1_output_file, index=False)
            empty_track1_df.to_json(track1_output_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            logger.info(f"Track1 조건을 만족하는 종목이 없습니다. 빈 파일 생성: {track1_output_file}")
    
    def _save_track2_results(self, track2_df: pd.DataFrame):
        """Track2 결과 저장"""
        track2_output_file = os.path.join(IPO_INVESTMENT_RESULTS_DIR,
                                         f"ipo_track2_{self.today.strftime('%Y%m%d')}.csv")
        if not track2_df.empty:
            track2_df.to_csv(track2_output_file, index=False)
            track2_df.to_json(track2_output_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            logger.info(f"Track2 결과 저장 완료: {track2_output_file} ({len(track2_df)}개 종목)")
        else:
            # 빈 데이터프레임에 완전한 칼럼명 추가 (코드와 일치)
            empty_track2_df = pd.DataFrame(columns=[
                'ticker', 'company_name', 'ipo_date', 'ipo_price', 'days_since_ipo', 
                'pattern_type', 'current_price', 'score', 'date', 'screening_date',
                'track2_score', 'momentum_score', 'volume_surge', 'price_action',
                'technical_strength', 'rs_rating', 'sector_strength'
            ])
            empty_track2_df.to_csv(track2_output_file, index=False)
            empty_track2_df.to_json(track2_output_file.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            logger.info(f"Track2 조건을 만족하는 종목이 없습니다. 빈 파일 생성: {track2_output_file}")