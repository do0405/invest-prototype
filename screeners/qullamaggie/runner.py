# -*- coding: utf-8 -*-
# 쿨라매기 매매법 알고리즘 - 메인 실행 모듈

import os
import sys


# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  

# 설정 및 유틸리티 임포트
from config import QULLAMAGGIE_RESULTS_DIR
from utils import ensure_dir

# 쿨라매기 모듈 임포트
from .screener import run_qullamaggie_screening
from .signal_generator import (
    generate_buy_signals,
    generate_sell_signals,
    manage_positions
)

# 결과 저장 경로는 config에서 가져옴

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
    
    # 실행할 셋업 결정
    if args.all:
        setup_type = None
    elif args.breakout:
        setup_type = 'breakout'
    elif args.episode_pivot:
        setup_type = 'episode_pivot'
    elif args.parabolic_short:
        setup_type = 'parabolic_short'
    else:
        setup_type = None

    # screener 모듈의 통합 함수 호출
    run_qullamaggie_screening(setup_type)

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
    
    # 포지션 관리는 요청된 경우에만 수행
    if args.manage_positions:
        updated_buy_positions, updated_sell_positions = manage_positions()
    
    print("\n✅ 쿨라매기 매매법 시그널 생성 완료")

# 외부에서 호출 가능한 함수
def run_qullamaggie_strategy(setups=None, skip_data=False):
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
            self.manage_positions = False
    
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

