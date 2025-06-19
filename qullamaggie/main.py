# -*- coding: utf-8 -*-
# 쿨라매기 매매법 알고리즘 - 메인 실행 모듈

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import argparse

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  

# 설정 및 유틸리티 임포트
from config import RESULTS_DIR, RESULTS_VER2_DIR, DATA_US_DIR
from utils import ensure_dir

# 쿨라매기 모듈 임포트
from qullamaggie.screener import (
    screen_breakout_setup,
    screen_episode_pivot_setup,
    screen_parabolic_short_setup
)
from qullamaggie.signal_generator import (
    generate_buy_signals,
    generate_sell_signals,
    manage_positions
)

# 결과 저장 경로 설정
QULLAMAGGIE_RESULTS_DIR = os.path.join(RESULTS_VER2_DIR, 'qullamaggie')

# 필요한 디렉토리 생성
def create_directories():
    """
    필요한 디렉토리 구조 생성
    """
    dirs = [
        QULLAMAGGIE_RESULTS_DIR,
        os.path.join(QULLAMAGGIE_RESULTS_DIR, 'buy'),
        os.path.join(QULLAMAGGIE_RESULTS_DIR, 'sell')
    ]
    
    for dir_path in dirs:
        ensure_dir(dir_path)
    
    print("✅ 디렉토리 구조 생성 완료")

# 스크리닝 실행 함수
def run_screening(args):
    """
    쿨라매기 매매법 스크리닝 실행
    
    Args:
        args: 명령행 인자
    """
    print("\n🔍 쿨라매기 매매법 스크리닝 시작...")
    
    # 디렉토리 생성
    create_directories()
    
    # 브레이크아웃 셋업 스크리닝
    if args.all or args.breakout:
        print("\n🔍 브레이크아웃 셋업 스크리닝 중...")
        breakout_results = screen_breakout_setup()
        if not breakout_results.empty:
            # 결과를 JSON으로 저장
            breakout_results_json = breakout_results.to_json(orient='records')
            breakout_results_list = json.loads(breakout_results_json)
            
            with open(os.path.join(QULLAMAGGIE_RESULTS_DIR, 'breakout_results.json'), 'w', encoding='utf-8') as f:
                json.dump(breakout_results_list, f, indent=2, ensure_ascii=False)
            
            # CSV로도 저장
            breakout_results.to_csv(os.path.join(QULLAMAGGIE_RESULTS_DIR, 'breakout_results.csv'), index=False)
            
            print(f"✅ 브레이크아웃 셋업 스크리닝 완료: {len(breakout_results)}개 종목 발견")
        else:
            print("⚠️ 브레이크아웃 셋업 조건을 만족하는 종목이 없습니다.")
    
    # 에피소드 피벗 셋업 스크리닝
    if args.all or args.episode_pivot:
        print("\n🔍 에피소드 피벗 셋업 스크리닝 중...")
        episode_pivot_results = screen_episode_pivot_setup()
        if not episode_pivot_results.empty:
            # 결과를 JSON으로 저장
            episode_pivot_results_json = episode_pivot_results.to_json(orient='records')
            episode_pivot_results_list = json.loads(episode_pivot_results_json)
            
            with open(os.path.join(QULLAMAGGIE_RESULTS_DIR, 'episode_pivot_results.json'), 'w', encoding='utf-8') as f:
                json.dump(episode_pivot_results_list, f, indent=2, ensure_ascii=False)
            
            # CSV로도 저장
            episode_pivot_results.to_csv(os.path.join(QULLAMAGGIE_RESULTS_DIR, 'episode_pivot_results.csv'), index=False)
            
            print(f"✅ 에피소드 피벗 셋업 스크리닝 완료: {len(episode_pivot_results)}개 종목 발견")
        else:
            print("⚠️ 에피소드 피벗 셋업 조건을 만족하는 종목이 없습니다.")
    
    # 파라볼릭 숏 셋업 스크리닝
    if args.all or args.parabolic_short:
        print("\n🔍 파라볼릭 숏 셋업 스크리닝 중...")
        parabolic_short_results = screen_parabolic_short_setup()
        if not parabolic_short_results.empty:
            # 결과를 JSON으로 저장
            parabolic_short_results_json = parabolic_short_results.to_json(orient='records')
            parabolic_short_results_list = json.loads(parabolic_short_results_json)
            
            with open(os.path.join(QULLAMAGGIE_RESULTS_DIR, 'parabolic_short_results.json'), 'w', encoding='utf-8') as f:
                json.dump(parabolic_short_results_list, f, indent=2, ensure_ascii=False)
            
            # CSV로도 저장
            parabolic_short_results.to_csv(os.path.join(QULLAMAGGIE_RESULTS_DIR, 'parabolic_short_results.csv'), index=False)
            
            print(f"✅ 파라볼릭 숏 셋업 스크리닝 완료: {len(parabolic_short_results)}개 종목 발견")
        else:
            print("⚠️ 파라볼릭 숏 셋업 조건을 만족하는 종목이 없습니다.")
    
    print("\n✅ 쿨라매기 매매법 스크리닝 완료")

# 시그널 생성 함수
def run_signal_generation(args):
    """
    쿨라매기 매매법 시그널 생성 실행
    
    Args:
        args: 명령행 인자
    """
    print("\n🔍 쿨라매기 매매법 시그널 생성 시작...")
    
    # 매수 시그널 생성
    if args.all or args.buy_signals:
        buy_signals = generate_buy_signals()
    
    # 매도 시그널 생성
    if args.all or args.sell_signals:
        sell_signals = generate_sell_signals()
    
    # 포지션 관리
    if args.all or args.manage_positions:
        updated_buy_positions, updated_sell_positions = manage_positions()
    
    print("\n✅ 쿨라매기 매매법 시그널 생성 완료")

# 외부에서 호출 가능한 함수
def run_qullamaggie_strategy(setups=None):
    """
    쿨라매기 매매법 전략 실행 - 외부에서 호출 가능한 인터페이스
    
    Args:
        setups (list): 실행할 셋업 목록 ['breakout', 'episode_pivot', 'parabolic_short']
                      None인 경우 모든 셋업 실행
    """
    # 디렉토리 생성
    create_directories()
    
    # 기본값 설정
    if setups is None:
        setups = ['breakout', 'episode_pivot', 'parabolic_short']
    
    # 가상 인자 생성
    class Args:
        def __init__(self):
            self.all = False
            self.screen = True
            self.signals = True
            self.breakout = False
            self.episode_pivot = False
            self.parabolic_short = False
            self.buy_signals = True
            self.sell_signals = True
            self.manage_positions = True
    
    args = Args()
    
    # 셋업에 따라 인자 설정
    if 'breakout' in setups:
        args.breakout = True
    if 'episode_pivot' in setups:
        args.episode_pivot = True
    if 'parabolic_short' in setups:
        args.parabolic_short = True
    
    # 스크리닝 실행
    run_screening(args)
    
    # 시그널 생성 실행
    run_signal_generation(args)
    
    return True

# 메인 함수
def main():
    parser = argparse.ArgumentParser(description='쿨라매기 매매법 알고리즘 실행')
    
    # 스크리닝 관련 인자
    parser.add_argument('--screen', action='store_true', help='스크리닝 실행')
    parser.add_argument('--breakout', action='store_true', help='브레이크아웃 셋업 스크리닝')
    parser.add_argument('--episode_pivot', action='store_true', help='에피소드 피벗 셋업 스크리닝')
    parser.add_argument('--parabolic_short', action='store_true', help='파라볼릭 숏 셋업 스크리닝')
    
    # 시그널 생성 관련 인자
    parser.add_argument('--signals', action='store_true', help='시그널 생성 실행')
    parser.add_argument('--buy_signals', action='store_true', help='매수 시그널 생성')
    parser.add_argument('--sell_signals', action='store_true', help='매도 시그널 생성')
    parser.add_argument('--manage_positions', action='store_true', help='포지션 관리')
    
    # 모든 기능 실행 인자
    parser.add_argument('--all', action='store_true', help='모든 기능 실행')
    
    args = parser.parse_args()
    
    # 기본적으로 모든 기능 실행
    if not any([args.screen, args.signals, args.breakout, args.episode_pivot, 
                args.parabolic_short, args.buy_signals, args.sell_signals, 
                args.manage_positions, args.all]):
        args.all = True
    
    # 스크리닝 실행
    if args.all or args.screen or args.breakout or args.episode_pivot or args.parabolic_short:
        run_screening(args)
    
    # 시그널 생성 실행
    if args.all or args.signals or args.buy_signals or args.sell_signals or args.manage_positions:
        run_signal_generation(args)
    
    print("\n✅ 쿨라매기 매매법 알고리즘 실행 완료")

if __name__ == '__main__':
    main()