# -*- coding: utf-8 -*-
# 쿨라매기 매매법 알고리즘 - 매수/매도 시그널 생성 모듈

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  

# 설정 및 유틸리티 임포트
from config import RESULTS_DIR, RESULTS_VER2_DIR, DATA_US_DIR
from utils import ensure_dir

# 결과 저장 경로 설정
QULLAMAGGIE_RESULTS_DIR = os.path.join(RESULTS_VER2_DIR, 'qullamaggie')
BUY_SIGNALS_PATH = os.path.join(QULLAMAGGIE_RESULTS_DIR, 'buy', 'qullamaggie_buy_signals.csv')
SELL_SIGNALS_PATH = os.path.join(QULLAMAGGIE_RESULTS_DIR, 'sell', 'qullamaggie_sell_signals.csv')

# 매수 시그널 생성 함수
def generate_buy_signals():
    """
    스크리닝 결과를 바탕으로 매수 시그널 생성
    
    Returns:
        DataFrame: 매수 시그널 데이터프레임
    """
    print("\n🔍 쿨라매기 매매법 매수 시그널 생성 중...")
    
    # 결과 디렉토리 생성
    ensure_dir(os.path.join(QULLAMAGGIE_RESULTS_DIR, 'buy'))
    
    # 스크리닝 결과 파일 경로
    breakout_results_path = os.path.join(QULLAMAGGIE_RESULTS_DIR, 'breakout_results.json')
    episode_pivot_results_path = os.path.join(QULLAMAGGIE_RESULTS_DIR, 'episode_pivot_results.json')
    
    # 결과 저장용 리스트
    buy_signals = []
    
    # 브레이크아웃 셋업 결과 로드
    if os.path.exists(breakout_results_path):
        try:
            with open(breakout_results_path, 'r') as f:
                breakout_results = json.load(f)
            print(f"✅ 브레이크아웃 셋업 결과 로드 완료: {len(breakout_results)}개 종목")
            
            # 매수 시그널 생성
            for result in breakout_results:
                # 기본 정보 추출
                signal = {
                    'symbol': result['symbol'],
                    'setup_type': 'Breakout',
                    'signal_date': datetime.now().strftime('%Y-%m-%d'),
                    'current_price': result['current_price'],
                    'entry_price': result['breakout_level'],  # 돌파 레벨을 매수가로 설정
                    'stop_loss': result['stop_loss'],
                    'risk_percent': (result['breakout_level'] - result['stop_loss']) / result['breakout_level'] * 100,
                    'target_price_1': result['current_price'] * 1.1,  # 10% 상승 목표
                    'target_price_2': result['current_price'] * 1.2,  # 20% 상승 목표
                    'score': result['score'],
                    'volume_ratio': result['volume_ratio'],
                    'adr': result['adr'],
                    'vcp_pattern': result['vcp_pattern'],
                    'risk_reward_ratio': result['risk_reward_ratio'],
                    'position_sizing': 0.0,  # 포지션 사이징은 별도 계산
                    'status': 'New'
                }
                
                # 포지션 사이징 계산 (계좌의 1% 리스크 기준)
                risk_per_share = signal['entry_price'] - signal['stop_loss']
                if risk_per_share > 0:
                    # 계좌 크기를 $100,000으로 가정
                    account_size = 100000
                    risk_amount = account_size * 0.01  # 계좌의 1% 리스크
                    shares = int(risk_amount / risk_per_share)
                    signal['position_sizing'] = shares
                
                buy_signals.append(signal)
        except Exception as e:
            print(f"❌ 브레이크아웃 셋업 결과 처리 오류: {e}")
    else:
        print("⚠️ 브레이크아웃 셋업 결과 파일이 없습니다.")
    
    # 에피소드 피벗 셋업 결과 로드
    if os.path.exists(episode_pivot_results_path):
        try:
            with open(episode_pivot_results_path, 'r') as f:
                episode_pivot_results = json.load(f)
            print(f"✅ 에피소드 피벗 셋업 결과 로드 완료: {len(episode_pivot_results)}개 종목")
            
            # 매수 시그널 생성
            for result in episode_pivot_results:
                # 기본 정보 추출
                signal = {
                    'symbol': result['symbol'],
                    'setup_type': 'Episode Pivot',
                    'signal_date': datetime.now().strftime('%Y-%m-%d'),
                    'current_price': result['current_price'],
                    'entry_price': result['current_price'],  # 현재가를 매수가로 설정
                    'stop_loss': result['stop_loss'],
                    'risk_percent': (result['current_price'] - result['stop_loss']) / result['current_price'] * 100,
                    'target_price_1': result['current_price'] * 1.1,  # 10% 상승 목표
                    'target_price_2': result['current_price'] * 1.2,  # 20% 상승 목표
                    'score': result['score'],
                    'volume_ratio': result['volume_ratio'],
                    'gap_percent': result['gap_percent'],
                    'ma50_relation': result['ma50_relation'],
                    'risk_reward_ratio': result['risk_reward_ratio'],
                    'position_sizing': 0.0,  # 포지션 사이징은 별도 계산
                    'status': 'New'
                }
                
                # 포지션 사이징 계산 (계좌의 1% 리스크 기준)
                risk_per_share = signal['entry_price'] - signal['stop_loss']
                if risk_per_share > 0:
                    # 계좌 크기를 $100,000으로 가정
                    account_size = 100000
                    risk_amount = account_size * 0.01  # 계좌의 1% 리스크
                    shares = int(risk_amount / risk_per_share)
                    signal['position_sizing'] = shares
                
                buy_signals.append(signal)
        except Exception as e:
            print(f"❌ 에피소드 피벗 셋업 결과 처리 오류: {e}")
    else:
        print("⚠️ 에피소드 피벗 셋업 결과 파일이 없습니다.")
    
    # 매수 시그널 데이터프레임 생성
    if buy_signals:
        buy_signals_df = pd.DataFrame(buy_signals)
        
        # 점수 기준 내림차순 정렬
        buy_signals_df = buy_signals_df.sort_values('score', ascending=False)
        
        # 결과 저장
        buy_signals_df.to_csv(BUY_SIGNALS_PATH, index=False)
        # JSON 파일 생성
        buy_signals_df.to_json(BUY_SIGNALS_PATH.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
        
        print(f"✅ 매수 시그널 생성 완료: {len(buy_signals_df)}개 종목")
        return buy_signals_df
    else:
        print("⚠️ 매수 시그널이 없습니다.")
        return pd.DataFrame()

# 매도 시그널 생성 함수
def generate_sell_signals():
    """
    스크리닝 결과를 바탕으로 매도 시그널 생성
    
    Returns:
        DataFrame: 매도 시그널 데이터프레임
    """
    print("\n🔍 쿨라매기 매매법 매도 시그널 생성 중...")
    
    # 결과 디렉토리 생성
    ensure_dir(os.path.join(QULLAMAGGIE_RESULTS_DIR, 'sell'))
    
    # 스크리닝 결과 파일 경로
    parabolic_short_results_path = os.path.join(QULLAMAGGIE_RESULTS_DIR, 'parabolic_short_results.json')
    
    # 결과 저장용 리스트
    sell_signals = []
    
    # 파라볼릭 숏 셋업 결과 로드
    if os.path.exists(parabolic_short_results_path):
        try:
            with open(parabolic_short_results_path, 'r') as f:
                parabolic_short_results = json.load(f)
            print(f"✅ 파라볼릭 숏 셋업 결과 로드 완료: {len(parabolic_short_results)}개 종목")
            
            # 매도 시그널 생성
            for result in parabolic_short_results:
                # 기본 정보 추출
                signal = {
                    'symbol': result['symbol'],
                    'setup_type': 'Parabolic Short',
                    'signal_date': datetime.now().strftime('%Y-%m-%d'),
                    'current_price': result['current_price'],
                    'entry_price': result['current_price'] * 0.9,  # 현재가의 90%를 매도가로 설정 (10% 하락 후 진입)
                    'stop_loss': result['stop_loss'],
                    'risk_percent': (result['stop_loss'] - result['current_price'] * 0.9) / (result['current_price'] * 0.9) * 100,
                    'target_price_1': result['current_price'] * 0.7,  # 30% 하락 목표
                    'target_price_2': result['current_price'] * 0.5,  # 50% 하락 목표
                    'score': result['score'],
                    'volume_ratio': result['volume_ratio'],
                    'short_term_rise': result['short_term_rise'],
                    'consecutive_up_days': result['consecutive_up_days'],
                    'rsi14': result['rsi14'],
                    'ma20_deviation': result['ma20_deviation'],
                    'first_down_candle': result['first_down_candle'],
                    'position_sizing': 0.0,  # 포지션 사이징은 별도 계산
                    'status': 'New'
                }
                
                # 포지션 사이징 계산 (계좌의 1% 리스크 기준)
                risk_per_share = signal['stop_loss'] - signal['entry_price']
                if risk_per_share > 0:
                    # 계좌 크기를 $100,000으로 가정
                    account_size = 100000
                    risk_amount = account_size * 0.01  # 계좌의 1% 리스크
                    shares = int(risk_amount / risk_per_share)
                    signal['position_sizing'] = shares
                
                sell_signals.append(signal)
        except Exception as e:
            print(f"❌ 파라볼릭 숏 셋업 결과 처리 오류: {e}")
    else:
        print("⚠️ 파라볼릭 숏 셋업 결과 파일이 없습니다.")
    
    # 매도 시그널 데이터프레임 생성
    if sell_signals:
        sell_signals_df = pd.DataFrame(sell_signals)
        
        # 점수 기준 내림차순 정렬
        sell_signals_df = sell_signals_df.sort_values('score', ascending=False)
        
        # 결과 저장
        sell_signals_df.to_csv(SELL_SIGNALS_PATH, index=False)
        # JSON 파일 생성
        sell_signals_df.to_json(SELL_SIGNALS_PATH.replace('.csv', '.json'), orient='records', indent=2, force_ascii=False)
        
        print(f"✅ 매도 시그널 생성 완료: {len(sell_signals_df)}개 종목")
        return sell_signals_df
    else:
        print("⚠️ 매도 시그널이 없습니다.")
        return pd.DataFrame()

# 포지션 관리 함수
def manage_positions():
    """
    기존 포지션 관리 및 업데이트
    
    Returns:
        tuple: (업데이트된 매수 포지션, 업데이트된 매도 포지션)
    """
    print("\n🔍 쿨라매기 매매법 포지션 관리 중...")
    
    # 매수/매도 시그널 파일 경로
    buy_signals_path = os.path.join(QULLAMAGGIE_RESULTS_DIR, 'buy', 'qullamaggie_buy_signals.json')
    sell_signals_path = os.path.join(QULLAMAGGIE_RESULTS_DIR, 'sell', 'qullamaggie_sell_signals.json')
    
    # 업데이트된 포지션 저장용 변수
    updated_buy_positions = None
    updated_sell_positions = None
    
    # 매수 포지션 관리
    if os.path.exists(buy_signals_path):
        try:
            with open(buy_signals_path, 'r') as f:
                buy_positions = json.load(f)
            print(f"✅ 매수 포지션 로드 완료: {len(buy_positions)}개 종목")
            
            # 각 포지션 업데이트
            updated_positions = []
            for position in buy_positions:
                # 현재 상태가 'Closed'인 경우 건너뛰기
                if position['status'] == 'Closed':
                    updated_positions.append(position)
                    continue
                
                # 현재 가격 업데이트 (실제로는 API 호출 등으로 최신 가격 가져와야 함)
                # 여기서는 예시로 랜덤한 가격 변동 적용
                current_price = position['current_price'] * (1 + np.random.uniform(-0.05, 0.1))
                
                # 손절 조건 확인
                if current_price < position['stop_loss']:
                    position['status'] = 'Stopped'
                    position['exit_price'] = position['stop_loss']
                    position['exit_date'] = datetime.now().strftime('%Y-%m-%d')
                    position['profit_loss'] = (position['exit_price'] - position['entry_price']) / position['entry_price'] * 100
                
                # 1차 목표가 도달 확인
                elif current_price >= position['target_price_1'] and position['status'] == 'Active':
                    position['status'] = 'Partial Exit'
                    position['partial_exit_price'] = position['target_price_1']
                    position['partial_exit_date'] = datetime.now().strftime('%Y-%m-%d')
                    # 손절가를 진입가로 상향 조정
                    position['stop_loss'] = position['entry_price']
                
                # 2차 목표가 도달 확인
                elif current_price >= position['target_price_2'] and position['status'] == 'Partial Exit':
                    position['status'] = 'Closed'
                    position['exit_price'] = position['target_price_2']
                    position['exit_date'] = datetime.now().strftime('%Y-%m-%d')
                    position['profit_loss'] = (position['exit_price'] - position['entry_price']) / position['entry_price'] * 100
                
                # 현재 가격 업데이트
                position['current_price'] = current_price
                
                updated_positions.append(position)
            
            # 업데이트된 포지션 저장
            updated_buy_positions = pd.DataFrame(updated_positions)
            updated_buy_positions.to_json(buy_signals_path, orient='records', indent=2, force_ascii=False)
            updated_buy_positions.to_csv(buy_signals_path.replace('.json', '.csv'), index=False)
            
            print(f"✅ 매수 포지션 업데이트 완료: {len(updated_positions)}개 종목")
        except Exception as e:
            print(f"❌ 매수 포지션 관리 오류: {e}")
    else:
        print("⚠️ 매수 포지션 파일이 없습니다.")
    
    # 매도 포지션 관리
    if os.path.exists(sell_signals_path):
        try:
            with open(sell_signals_path, 'r') as f:
                sell_positions = json.load(f)
            print(f"✅ 매도 포지션 로드 완료: {len(sell_positions)}개 종목")
            
            # 각 포지션 업데이트
            updated_positions = []
            for position in sell_positions:
                # 현재 상태가 'Closed'인 경우 건너뛰기
                if position['status'] == 'Closed':
                    updated_positions.append(position)
                    continue
                
                # 현재 가격 업데이트 (실제로는 API 호출 등으로 최신 가격 가져와야 함)
                # 여기서는 예시로 랜덤한 가격 변동 적용
                current_price = position['current_price'] * (1 + np.random.uniform(-0.1, 0.05))
                
                # 손절 조건 확인 (숏 포지션은 가격이 상승하면 손절)
                if current_price > position['stop_loss']:
                    position['status'] = 'Stopped'
                    position['exit_price'] = position['stop_loss']
                    position['exit_date'] = datetime.now().strftime('%Y-%m-%d')
                    position['profit_loss'] = (position['entry_price'] - position['exit_price']) / position['entry_price'] * 100
                
                # 1차 목표가 도달 확인 (숏 포지션은 가격이 하락하면 이익)
                elif current_price <= position['target_price_1'] and position['status'] == 'Active':
                    position['status'] = 'Partial Exit'
                    position['partial_exit_price'] = position['target_price_1']
                    position['partial_exit_date'] = datetime.now().strftime('%Y-%m-%d')
                    # 손절가를 진입가로 하향 조정
                    position['stop_loss'] = position['entry_price']
                
                # 2차 목표가 도달 확인
                elif current_price <= position['target_price_2'] and position['status'] == 'Partial Exit':
                    position['status'] = 'Closed'
                    position['exit_price'] = position['target_price_2']
                    position['exit_date'] = datetime.now().strftime('%Y-%m-%d')
                    position['profit_loss'] = (position['entry_price'] - position['exit_price']) / position['entry_price'] * 100
                
                # 현재 가격 업데이트
                position['current_price'] = current_price
                
                updated_positions.append(position)
            
            # 업데이트된 포지션 저장
            updated_sell_positions = pd.DataFrame(updated_positions)
            updated_sell_positions.to_json(sell_signals_path, orient='records', indent=2, force_ascii=False)
            updated_sell_positions.to_csv(sell_signals_path.replace('.json', '.csv'), index=False)
            
            print(f"✅ 매도 포지션 업데이트 완료: {len(updated_positions)}개 종목")
        except Exception as e:
            print(f"❌ 매도 포지션 관리 오류: {e}")
    else:
        print("⚠️ 매도 포지션 파일이 없습니다.")
    
    return updated_buy_positions, updated_sell_positions

# 메인 함수
def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='쿨라매기 매매법 시그널 생성기')
    parser.add_argument('--buy', action='store_true', help='매수 시그널 생성')
    parser.add_argument('--sell', action='store_true', help='매도 시그널 생성')
    parser.add_argument('--manage', action='store_true', help='포지션 관리')
    
    args = parser.parse_args()
    
    # 기본적으로 모든 기능 실행
    if not (args.buy or args.sell or args.manage):
        generate_buy_signals()
        generate_sell_signals()
        manage_positions()
    else:
        # 선택적 기능 실행
        if args.buy:
            generate_buy_signals()
        if args.sell:
            generate_sell_signals()
        if args.manage:
            manage_positions()

if __name__ == '__main__':
    main()