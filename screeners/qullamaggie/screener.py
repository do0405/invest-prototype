# -*- coding: utf-8 -*-
# 쿨라매기 매매법 알고리즘 - 스크리너 모듈

import os
import sys
import pandas as pd
import json

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  

# 설정 및 유틸리티 임포트
from config import DATA_US_DIR, QULLAMAGGIE_RESULTS_DIR
from utils import ensure_dir, load_csvs_parallel
from .core import (
    apply_basic_filters,
    screen_breakout_setup,
    check_vcp_pattern,
    screen_episode_pivot_setup,
    screen_parabolic_short_setup,
)

# 결과 저장 경로 설정
BREAKOUT_RESULTS_PATH = os.path.join(QULLAMAGGIE_RESULTS_DIR, 'breakout_results.csv')
EPISODE_PIVOT_RESULTS_PATH = os.path.join(QULLAMAGGIE_RESULTS_DIR, 'episode_pivot_results.csv')
PARABOLIC_SHORT_RESULTS_PATH = os.path.join(QULLAMAGGIE_RESULTS_DIR, 'parabolic_short_results.csv')

# 기본 스크리닝 조건 함수
def run_qullamaggie_screening(setup_type=None):
    """
    쿨라매기 매매법 스크리닝 실행 함수
    
    Args:
        setup_type: 스크리닝할 셋업 타입 ('breakout', 'episode_pivot', 'parabolic_short', None=모두)
        
    Returns:
        dict: 각 셋업별 스크리닝 결과
    """
    print("\n🔍 쿨라매기 매매법 스크리닝 시작...")
    
    # 결과 디렉토리 생성
    ensure_dir(QULLAMAGGIE_RESULTS_DIR)
    
    # 데이터 디렉토리에서 모든 CSV 파일 경로 가져오기
    csv_files = [os.path.join(DATA_US_DIR, f) for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
    
    # 데이터 로드
    print(f"📊 총 {len(csv_files)}개 종목 데이터 로드 중...")
    stock_data = load_csvs_parallel(csv_files)
    print(f"✅ {len(stock_data)}개 종목 데이터 로드 완료")
    
    # 결과 저장용 딕셔너리
    results = {
        'breakout': [],
        'episode_pivot': [],
        'parabolic_short': []
    }
    
    # 각 종목에 대해 스크리닝 실행
    print("\n🔍 스크리닝 실행 중...")
    for i, (file_name, df) in enumerate(stock_data.items(), 1):
        ticker = os.path.splitext(file_name)[0]
        
        # 진행 상황 출력 (100개 단위)
        if i % 100 == 0 or i == len(stock_data):
            print(f"  진행률: {i}/{len(stock_data)} ({i/len(stock_data)*100:.1f}%)")
        
        # 셋업별 스크리닝 실행
        if setup_type is None or setup_type == 'breakout':
            breakout_result = screen_breakout_setup(ticker, df)
            if breakout_result['passed']:
                results['breakout'].append(breakout_result)
        
        if setup_type is None or setup_type == 'episode_pivot':
            episode_pivot_result = screen_episode_pivot_setup(ticker, df)
            if episode_pivot_result['passed']:
                results['episode_pivot'].append(episode_pivot_result)
        
        if setup_type is None or setup_type == 'parabolic_short':
            parabolic_short_result = screen_parabolic_short_setup(ticker, df)
            if parabolic_short_result['passed']:
                results['parabolic_short'].append(parabolic_short_result)
    
    # 결과 저장
    print("\n💾 스크리닝 결과 저장 중...")
    
    # 브레이크아웃 셋업 결과 저장
    if setup_type is None or setup_type == 'breakout':
        breakout_df = pd.DataFrame(results['breakout'])
        if not breakout_df.empty:
            # 점수 기준 내림차순 정렬
            breakout_df = breakout_df.sort_values('score', ascending=False)
            breakout_df.to_csv(BREAKOUT_RESULTS_PATH, index=False)
            # JSON 파일 생성
            breakout_df.to_json(BREAKOUT_RESULTS_PATH.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            print(f"✅ 브레이크아웃 셋업 결과 저장 완료: {len(breakout_df)}개 종목")
        else:
            print("⚠️ 브레이크아웃 셋업 결과 없음")
    
    # 에피소드 피벗 셋업 결과 저장
    if setup_type is None or setup_type == 'episode_pivot':
        episode_pivot_df = pd.DataFrame(results['episode_pivot'])
        if not episode_pivot_df.empty:
            # 점수 기준 내림차순 정렬
            episode_pivot_df = episode_pivot_df.sort_values('score', ascending=False)
            episode_pivot_df.to_csv(EPISODE_PIVOT_RESULTS_PATH, index=False)
            # JSON 파일 생성
            episode_pivot_df.to_json(EPISODE_PIVOT_RESULTS_PATH.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            print(f"✅ 에피소드 피벗 셋업 결과 저장 완료: {len(episode_pivot_df)}개 종목")
        else:
            print("⚠️ 에피소드 피벗 셋업 결과 없음")
    
    # 파라볼릭 숏 셋업 결과 저장
    if setup_type is None or setup_type == 'parabolic_short':
        parabolic_short_df = pd.DataFrame(results['parabolic_short'])
        if not parabolic_short_df.empty:
            # 점수 기준 내림차순 정렬
            parabolic_short_df = parabolic_short_df.sort_values('score', ascending=False)
            parabolic_short_df.to_csv(PARABOLIC_SHORT_RESULTS_PATH, index=False)
            # JSON 파일 생성
            parabolic_short_df.to_json(PARABOLIC_SHORT_RESULTS_PATH.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            print(f"✅ 파라볼릭 숏 셋업 결과 저장 완료: {len(parabolic_short_df)}개 종목")
        else:
            print("⚠️ 파라볼릭 숏 셋업 결과 없음")
    
    # 결과 요약
    print("\n📊 스크리닝 결과 요약:")
    print(f"  브레이크아웃 셋업: {len(results['breakout'])}개 종목")
    print(f"  에피소드 피벗 셋업: {len(results['episode_pivot'])}개 종목")
    print(f"  파라볼릭 숏 셋업: {len(results['parabolic_short'])}개 종목")
    
    return results

# 메인 함수
def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='쿨라매기 매매법 스크리너')
    parser.add_argument('--setup', choices=['breakout', 'episode_pivot', 'parabolic_short'], 
                        help='스크리닝할 셋업 타입')
    
    args = parser.parse_args()
    
    # 스크리닝 실행
    run_qullamaggie_screening(args.setup)

