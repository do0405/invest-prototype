# -*- coding: utf-8 -*-
# 쿨라매기 매매법 알고리즘 - 스크리너 모듈

import os
import sys
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.path_utils import add_project_root

# 프로젝트 루트 디렉토리를 Python 경로에 추가
add_project_root()

# 설정 및 유틸리티 임포트
from config import DATA_US_DIR, QULLAMAGGIE_RESULTS_DIR
from utils import ensure_dir, load_csvs_parallel
from utils.screener_utils import save_screening_results, track_new_tickers, create_screener_summary
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
def run_qullamaggie_screening(setup_type=None, enable_earnings_filter=True):
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
    
    # 각 종목에 대해 병렬 스크리닝 실행 (스레드 안전성 개선)
    print("\n🔍 병렬 스크리닝 실행 중...")
    
    def process_stock(item):
        """개별 종목 처리 함수"""
        file_name, df = item
        ticker = os.path.splitext(file_name)[0]
        stock_results = {'breakout': [], 'episode_pivot': [], 'parabolic_short': []}
        
        try:
            # 셋업별 스크리닝 실행
            if setup_type is None or setup_type == 'breakout':
                breakout_result = screen_breakout_setup(ticker, df)
                if breakout_result['passed']:
                    stock_results['breakout'].append(breakout_result)
            
            if setup_type is None or setup_type == 'episode_pivot':
                episode_pivot_result = screen_episode_pivot_setup(ticker, df, enable_earnings_filter)
                if episode_pivot_result['passed']:
                    stock_results['episode_pivot'].append(episode_pivot_result)
            
            if setup_type is None or setup_type == 'parabolic_short':
                parabolic_short_result = screen_parabolic_short_setup(ticker, df)
                if parabolic_short_result['passed']:
                    stock_results['parabolic_short'].append(parabolic_short_result)
                    
        except Exception as e:
            print(f"⚠️ {ticker} 처리 중 오류: {e}")
            
        return stock_results
    
    # 병렬 처리 실행 (스레드 안전성 보장)
    max_workers = min(4, len(stock_data))  # 최대 4개 워커
    completed_count = 0
    all_results = []  # 모든 결과를 임시로 저장
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 작업 제출
        future_to_stock = {executor.submit(process_stock, item): item[0] for item in stock_data.items()}
        
        # 결과 수집 (스레드 안전)
        for future in as_completed(future_to_stock):
            completed_count += 1
            
            # 진행 상황 출력 (100개 단위)
            if completed_count % 100 == 0 or completed_count == len(stock_data):
                print(f"  진행률: {completed_count}/{len(stock_data)} ({completed_count/len(stock_data)*100:.1f}%)")
            
            try:
                stock_results = future.result()
                all_results.append(stock_results)
            except Exception as e:
                stock_name = future_to_stock[future]
                print(f"⚠️ {stock_name} 결과 처리 중 오류: {e}")
    
    # 결과 병합 (메인 스레드에서 안전하게 처리)
    for stock_results in all_results:
        for setup_key in results.keys():
            results[setup_key].extend(stock_results[setup_key])
    
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
            # 빈 결과일 때도 칼럼명이 있는 빈 파일 생성
            empty_breakout_df = pd.DataFrame(columns=['ticker', 'score', 'passed', 'setup_type', 'date'])
            empty_breakout_df.to_csv(BREAKOUT_RESULTS_PATH, index=False)
            empty_breakout_df.to_json(BREAKOUT_RESULTS_PATH.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            print(f"⚠️ 브레이크아웃 셋업 결과 없음. 빈 파일 생성: {BREAKOUT_RESULTS_PATH}")
    
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
            # 빈 결과일 때도 칼럼명이 있는 빈 파일 생성
            empty_episode_df = pd.DataFrame(columns=['ticker', 'score', 'passed', 'setup_type', 'date'])
            empty_episode_df.to_csv(EPISODE_PIVOT_RESULTS_PATH, index=False)
            empty_episode_df.to_json(EPISODE_PIVOT_RESULTS_PATH.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            print(f"⚠️ 에피소드 피벗 셋업 결과 없음. 빈 파일 생성: {EPISODE_PIVOT_RESULTS_PATH}")
    
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
            # 빈 결과일 때도 칼럼명이 있는 빈 파일 생성
            empty_parabolic_df = pd.DataFrame(columns=['ticker', 'score', 'passed', 'setup_type', 'date'])
            empty_parabolic_df.to_csv(PARABOLIC_SHORT_RESULTS_PATH, index=False)
            empty_parabolic_df.to_json(PARABOLIC_SHORT_RESULTS_PATH.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
            print(f"⚠️ 파라볼릭 숏 셋업 결과 없음. 빈 파일 생성: {PARABOLIC_SHORT_RESULTS_PATH}")
    
    # 새로운 티커 추적
    print("\n🔍 새로운 티커 추적 중...")
    
    # 각 셋업별로 새로운 티커 추적
    new_tickers_summary = {}
    
    if setup_type is None or setup_type == 'breakout':
        tracker_file = os.path.join(QULLAMAGGIE_RESULTS_DIR, "new_breakout_tickers.csv")
        new_breakout_tickers = track_new_tickers(
            current_results=results['breakout'],
            tracker_file=tracker_file,
            symbol_key='ticker',
            retention_days=14
        )
        new_tickers_summary['breakout'] = len(new_breakout_tickers)
    
    if setup_type is None or setup_type == 'episode_pivot':
        tracker_file = os.path.join(QULLAMAGGIE_RESULTS_DIR, "new_episode_pivot_tickers.csv")
        new_episode_tickers = track_new_tickers(
            current_results=results['episode_pivot'],
            tracker_file=tracker_file,
            symbol_key='ticker',
            retention_days=14
        )
        new_tickers_summary['episode_pivot'] = len(new_episode_tickers)
    
    if setup_type is None or setup_type == 'parabolic_short':
        tracker_file = os.path.join(QULLAMAGGIE_RESULTS_DIR, "new_parabolic_short_tickers.csv")
        new_parabolic_tickers = track_new_tickers(
            current_results=results['parabolic_short'],
            tracker_file=tracker_file,
            symbol_key='ticker',
            retention_days=14
        )
        new_tickers_summary['parabolic_short'] = len(new_parabolic_tickers)
    
    # 결과 요약
    print("\n📊 스크리닝 결과 요약:")
    print(f"  브레이크아웃 셋업: {len(results['breakout'])}개 종목 (신규: {new_tickers_summary.get('breakout', 0)}개)")
    print(f"  에피소드 피벗 셋업: {len(results['episode_pivot'])}개 종목 (신규: {new_tickers_summary.get('episode_pivot', 0)}개)")
    print(f"  파라볼릭 숏 셋업: {len(results['parabolic_short'])}개 종목 (신규: {new_tickers_summary.get('parabolic_short', 0)}개)")
    
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

