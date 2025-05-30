# -*- coding: utf-8 -*-
# 투자 스크리너 Ver3 - 추가 스크리닝 전략 모듈

import os
import traceback
import sys
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from scipy.stats import rankdata

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  

# 설정 파일 임포트
from config import (
    DATA_DIR, DATA_US_DIR, 
    RESULTS_DIR, RESULTS_VER2_DIR
)

# 유티리티 함수 임포트
from utils import (ensure_dir, load_csvs_parallel, extract_ticker_from_filename,
                  calculate_atr, calculate_rsi, calculate_adx, calculate_historical_volatility,
                  check_sp500_condition, process_stock_data)

# 필요한 디렉토리 생성 함수
def create_required_dirs():
    """필요한 디렉토리를 생성하는 함수"""
    # RESULTS_VER2_DIR은 config.py에서 루트 디렉토리의 'results_ver2'로 설정됨
    ensure_dir(RESULTS_VER2_DIR)
    # 매수/매도 결과 저장 디렉토리 생성
    buy_dir = os.path.join(RESULTS_VER2_DIR, 'buy')
    sell_dir = os.path.join(RESULTS_VER2_DIR, 'sell')
    ensure_dir(buy_dir)
    ensure_dir(sell_dir)

# 참고: 변동률 계산 함수는 utils.py의 calculate_historical_volatility로 이동됨
# 참고: ATR(Average True Range) 계산 함수는 utils.py의 calculate_atr로 이동됨
# 참고: process_stock_data 함수는 utils.py로 이동됨

# 3일 하락률 계산 함수
def calculate_decline_percentage(df, days=3):
    """최근 n일 동안의 하락률을 계산하는 함수
    
    Args:
        df: 가격 데이터가 포함된 DataFrame (close 컬럼 필요)
        days: 하락률 계산 기간 (기본값: 3일)
        
    Returns:
        float: 하락률 (%) - 양수 값이 하락을 의미
    """
    try:
        if len(df) < days + 1:
            return 0.0
            
        # n일 전 종가와 현재 종가
        current_close = df.iloc[-1]['close']
        past_close = df.iloc[-(days+1)]['close']
        
        # 하락률 계산 (양수 값이 하락을 의미)
        decline_pct = ((past_close - current_close) / past_close) * 100
        
        return decline_pct
    except Exception as e:
        import traceback
        print(f"❌ 하락률 계산 오류: {e}")
        print(traceback.format_exc())
        return 0.0

# 참고: RSI(Relative Strength Index) 계산 함수는 utils.py로 이동됨

# 참고: ADX(Average Directional Index) 계산 함수는 utils.py로 이동됨

# 전략 3: 하락 후 반등 가능성이 있는 주식 스크리닝
def run_strategy3():
    """세 번째 전략 실행 함수 - 하락 후 반등 가능성이 있는 주식 식별
    
    필터:
    1. 주가는 최소 1달러 이상이어야 한다.
    2. 지난 50일 동안 평균 거래량이 100만 주 이상이어야 한다.
    3. 지난 10일 동안 ATR이 5% 또는 그 이상이어야 한다.
    
    설정:
    1. 종가가 150일 단순이동평균 위에 있어야 한다.
    2. 지난 3일 동안 12.5% 또는 그 이상 하락했어야 한다.
    
    순위: 지난 3일 동안 큰 폭의 하락이 발생한 주식에 우선순위를 두어 상위 20개만 선택
    
    시장 진입: 직전 종가보다 7% 낮게 지정가로 주문
    손절매: 매수 당일, 체결 가격을 기준으로 최근 10일 ATR의 2.5배 아래 지점에 설정
    시장 재진입: 가능
    수익 보호: 없음
    차익 실현: 종가 기준 4% 이상 수익이 발생하면 다음 날 장 마감 때 시장가로 매도, 매수 3일 후에도 목표 주가에 도달하지 못하면 다음 날 장 마감 때 시장가로 매도
    포지션 크기: 포지션별 총자산 대비 2%의 위험, 총자산 대비 최대 10% 자산 배분
    """
    print("\n🔍 전략 3: 하락 후 반등 가능성이 있는 주식 스크리닝 시작...")
    
    # 결과 파일 경로 (매수 전략이므로 buy 폴더에 저장)
    buy_dir = os.path.join(RESULTS_VER2_DIR, 'buy')
    ensure_dir(buy_dir)
    result_file = os.path.join(buy_dir, 'strategy3_results.csv')
    
    # S&P 500 조건 확인
    sp500_condition = check_sp500_condition(DATA_US_DIR)
    if not sp500_condition:
        print("❌ S&P 500 조건을 충족하지 않습니다. 스크리닝을 중단합니다.")
        # 빈 결과 파일 생성
        pd.DataFrame(columns=[
            'symbol', 'entry_price', 'stop_loss', 'profit_protection', 'position_size_percentage'
        ]).to_csv(result_file, index=False, mode='w')
        return
    
    try:
        # 개별 CSV 파일 로드
        us_files = [f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
        if not us_files:
            print("❌ 미국 주식 데이터 파일이 없습니다.")
            return
            
        print(f"📊 {len(us_files)}개 미국 주식 파일 처리 중...")
        
        # 개별 종목 처리
        results = []
        for i, file in enumerate(us_files):
            if i % 100 == 0 and i > 0:
                print(f"⏳ 진행 중: {i}/{len(us_files)} 종목 처리됨")
                
            try:
                # 데이터 처리
                symbol, df, recent_data = process_stock_data(file, DATA_US_DIR, min_days=150, recent_days=150)
                if symbol is None or df is None or recent_data is None:
                    continue
                
                # 필터 1: 주가는 최소 1달러 이상
                latest_close = recent_data.iloc[-1]['close']
                if latest_close < 1.0:
                    continue
                
                # 필터 2: 지난 50일 동안 평균 거래량이 100만 주 이상
                recent_50d = recent_data.iloc[-50:]
                avg_volume_50d = recent_50d['volume'].mean()
                if avg_volume_50d < 1000000:  # 100만주
                    continue
                
                # 필터 3: 지난 10일 동안 ATR이 5% 또는 그 이상
                recent_10d = recent_data.iloc[-10:]
                atr_10d = calculate_atr(recent_10d, window=10).iloc[-1]
                atr_percentage = (atr_10d / latest_close) * 100
                if atr_percentage < 5.0:
                    continue
                
                # 설정 1: 종가가 150일 단순이동평균 위에 있어야 한다
                recent_data['ma150'] = recent_data['close'].rolling(window=150).mean()
                latest = recent_data.iloc[-1]
                latest_close = float(latest['close'])
                latest_ma150 = float(latest['ma150'])
                if latest_close <= latest_ma150:
                    continue
                
                # 설정 2: 지난 3일 동안 12.5% 또는 그 이상 하락했어야 한다
                decline_pct = calculate_decline_percentage(recent_data, days=3)
                if decline_pct < 12.5:  # 12.5% 이상 하락
                    continue
                
                # 시장 진입: 직전 종가보다 7% 낮게 지정가로 주문
                entry_price = latest_close * 0.93  # 7% 낮은 가격
                
                # 손절매: 매수 당일, 체결 가격을 기준으로 최근 10일 ATR의 2.5배 아래 지점에 설정
                stop_loss = entry_price - (atr_10d * 2.5)
                
                # 차익 실현: 종가 기준 4% 이상 수익이 발생하면 다음 날 장 마감 때 시장가로 매도
                target_price = entry_price * 1.04  # 4% 상승 목표
                
                # 최대 보유 기간: 3일 (매수 3일 후에도 목표 주가에 도달하지 못하면 다음 날 장 마감 때 시장가로 매도)
                max_hold_days = 3
                
                # 포지션 크기 계산 (총자산 대비 2%의 위험, 총자산 대비 최대 10% 자산 배분
                # 위험 금액 = 진입가 - 손절가
                risk_per_share = entry_price - stop_loss
                
                # 모든 조건을 충족하는 종목 결과에 추가
                results.append({
                    'symbol': symbol,
                    'price': latest_close,
                    'avg_volume': avg_volume_50d,
                    'atr_percentage': atr_percentage,
                    'decline_percentage': decline_pct,
                    'entry_price': round(entry_price, 2),
                    'stop_loss': round(stop_loss, 2),
                    'target_price': round(target_price, 2),
                    'max_hold_days': max_hold_days,
                    'risk_per_share': round(risk_per_share, 2)
                })
                
            except Exception as e:
                import traceback
                print(f"❌ {file} 처리 오류: {e}")
                print(traceback.format_exc())
        
        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=[
                'symbol', 'entry_price', 'stop_loss', 'profit_protection', 'position_size_percentage'
            ]).to_csv(result_file, index=False)
            return
        
        # 결과 데이터프레임 생성
        result_df = pd.DataFrame(results)
        
        # 하락폭 기준으로 내림차순 정렬 (큰 하락폭이 위에 오도록)
        result_df = result_df.sort_values('decline_percentage', ascending=False)
        
        # 상위 20개 종목만 선택
        result_df = result_df.head(20)
        
        # 포지션 크기 계산 (총자산 대비 2%의 위험, 총자산 대비 최대 10% 자산 배분)
        # 가정: 총자산 = 100,000 달러
        total_assets = 100000
        risk_per_position = total_assets * 0.02  # 포지션당 2% 위험
        max_position_size = total_assets * 0.10  # 최대 10% 자산 배분
        
        # 각 종목별 포지션 크기 계산
        result_df['position_size'] = (risk_per_position / result_df['risk_per_share']).round(0)
        result_df['position_value'] = result_df['position_size'] * result_df['entry_price']
        
        # 최대 포지션 크기 제한 적용
        result_df.loc[result_df['position_value'] > max_position_size, 'position_size'] = \
            (max_position_size / result_df['entry_price']).round(0)
        
        # 최종 포지션 가치 재계산
        result_df['position_value'] = result_df['position_size'] * result_df['entry_price']
        
        # 포지션 크기 백분율 계산
        # result_df['position_value']는 이미 계산되어 있음 (총 자산 100,000 가정)
        total_assets_for_calc = 100000 # 임시 총 자산
        result_df['position_size_percentage'] = (result_df['position_value'] / total_assets_for_calc) * 100
        result_df['position_size_percentage'] = result_df['position_size_percentage'].round(2)

        # 결과 CSV에 포함할 컬럼 선택 및 이름 변경
        strategy_result_columns = ['symbol', 'entry_price', 'stop_loss', 'target_price', 'position_size_percentage']
        result_df_to_save = result_df[strategy_result_columns].rename(columns={'target_price': 'profit_protection'})
        
        # 결과 저장 (buy_dir은 이미 생성되어 있음)
        result_df_to_save.to_csv(result_file, index=False, mode='w')
        print(f"✅ 전략 3 스크리닝 결과 저장 완료: {len(result_df_to_save)}개 종목, 경로: {result_file}")
        
        # 상위 20개 종목 출력 (요청된 형식으로)
        print("\n🏆 전략 3 상위 20개 종목 (스크리닝 결과):")
        print(result_df_to_save.head(20))

        # 포트폴리오 생성
        create_portfolio_strategy3(result_df, total_capital=total_assets, update_existing=False) # update_existing는 필요에 따라 True로 설정 가능
        
    except Exception as e:
        print(f"❌ 전략 3 스크리닝 오류: {e}")

# 최신 가격 데이터 가져오기 함수 (저가 포함)
def get_latest_price_data_low_strategy3(symbol):
    """특정 종목의 최신 가격 데이터를 가져오는 함수 (저가 포함)
    
    Args:
        symbol: 종목 심볼
        
    Returns:
        tuple: (현재가, 당일 저가) 또는 데이터가 없는 경우 (None, None)
    """
    try:
        # 종목 데이터 파일 경로
        file_path = os.path.join(DATA_US_DIR, f'{symbol}.csv')
        
        if not os.path.exists(file_path):
            print(f"⚠️ {symbol} 데이터 파일을 찾을 수 없습니다.")
            return None, None
        
        # 데이터 로드
        df = pd.read_csv(file_path)
        df.columns = [col.lower() for col in df.columns]
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], utc=True)
            df = df.sort_values('date')
        else:
            print(f"⚠️ {symbol} 데이터에 날짜 컬럼이 없습니다.")
            return None, None
        
        # 최신 데이터 확인
        if df.empty:
            return None, None
        
        latest = df.iloc[-1]
        
        return latest['close'], latest['low']
        
    except Exception as e:
        print(f"❌ {symbol} 가격 데이터 가져오기 오류: {e}")
        return None, None

# 전략 3 포트폴리오 생성 함수
def create_portfolio_strategy3(screened_stocks, total_capital=100000, update_existing=False):
    """전략 3 포트폴리오 생성 함수
    
    Args:
        screened_stocks: 스크리닝된 종목 DataFrame
        total_capital: 총 자본금 (기본값: 10만 달러)
        update_existing: 기존 포트폴리오 업데이트 여부 (기본값: False)
    """
    try:
        # 포트폴리오 파일 경로
        portfolio_file = os.path.join(RESULTS_VER2_DIR, 'portfolio_strategy3.csv')
        
        # 기존 포트폴리오 확인
        existing_portfolio = None
        if update_existing and os.path.exists(portfolio_file):
            try:
                existing_portfolio = pd.read_csv(portfolio_file)
                print(f"📊 기존 포트폴리오 로드: {len(existing_portfolio)}개 종목")
            except Exception as e:
                print(f"⚠️ 기존 포트폴리오 로드 오류: {e}")
                existing_portfolio = None
        
        # 포트폴리오 계산
        portfolio = []
        available_capital = total_capital
        existing_symbols = set()
        
        # 기존 포트폴리오 종목 처리
        if existing_portfolio is not None and not existing_portfolio.empty:
            for _, position in existing_portfolio.iterrows():
                symbol = position['symbol']
                existing_symbols.add(symbol)
                
                # 최신 가격 데이터 가져오기
                current_price, low_price = get_latest_price_data_low_strategy3(symbol)
                
                if current_price is None:
                    # 데이터를 가져올 수 없는 경우 기존 포지션 유지
                    portfolio.append(position.to_dict())
                    available_capital -= position['position_amount']
                    continue
                
                # 손절매 확인
                stop_loss = position['stop_loss']
                if low_price <= stop_loss:
                    print(f"🔴 손절매 실행: {symbol} (매수가: ${position['price']:.2f}, 손절매가: ${stop_loss:.2f})")
                    continue
                
                # 수익 목표 달성 확인 (4% 이상)
                profit_pct = (current_price - position['price']) / position['price'] * 100
                if profit_pct >= 4.0:
                    print(f"🟢 수익 목표 달성: {symbol} (매수가: ${position['price']:.2f}, 현재가: ${current_price:.2f}, 수익률: {profit_pct:.2f}%)")
                    continue
                
                # 보유 기간 확인 (최대 3일)
                entry_date = pd.to_datetime(position['entry_date'])
                current_date = datetime.now()
                holding_days = (current_date - entry_date).days
                
                if holding_days >= position.get('max_hold_days', 3): # max_hold_days 컬럼 사용, 없으면 3일
                    print(f"🟡 보유 기간 초과: {symbol} (매수가: ${position['price']:.2f}, 현재가: ${current_price:.2f}, 보유일: {holding_days}일)")
                    continue
                
                # 업데이트된 포지션 정보
                updated_position = position.to_dict()
                updated_position['current_price'] = current_price
                updated_position['current_value'] = current_price * position['shares']
                updated_position['profit_loss'] = (current_price - position['price']) * position['shares']
                updated_position['profit_loss_pct'] = profit_pct
                updated_position['holding_days'] = holding_days
                
                portfolio.append(updated_position)
                available_capital -= updated_position['current_value']
        
        # 새로운 종목 추가 (상위 20개)
        for _, stock in screened_stocks.head(20).iterrows():
            # 이미 포트폴리오에 있는 종목 건너뛰기
            if stock['symbol'] in existing_symbols:
                continue
                
            # 가용 자본 확인
            if available_capital <= 0 or len(portfolio) >= 20: # 최대 20개 포지션
                break
            
            # 포지션 계산
            entry_price = stock['entry_price']
            stop_loss = stock['stop_loss']
            risk_per_share = stock['risk_per_share'] # 스크리닝 결과에서 가져옴
            
            if risk_per_share <= 0:
                continue
            
            # 위험 금액 계산 (총 자본의 2%)
            risk_capital_per_position = total_capital * 0.02
            
            # 주식 수량 계산
            shares = int(risk_capital_per_position / risk_per_share)
            
            # 포지션 금액 계산
            position_amount = shares * entry_price
            
            # 최대 배분 금액 확인 (총 자본의 10%)
            max_capital_per_position = total_capital * 0.1
            if position_amount > max_capital_per_position:
                shares = int(max_capital_per_position / entry_price)
                position_amount = shares * entry_price
            
            # 가용 자본 확인
            if position_amount > available_capital:
                shares = int(available_capital / entry_price)
                position_amount = shares * entry_price
                
                if shares <= 0:
                    continue
            
            # 포트폴리오에 추가
            portfolio.append({
                'symbol': stock['symbol'],
                'price': entry_price,
                'shares': shares,
                'position_amount': position_amount,
                'stop_loss': stop_loss,
                'profit_protection': stock['target_price'], # target_price를 profit_protection으로 변경
                'max_hold_days': stock['max_hold_days'],
                'entry_date': datetime.now().strftime('%Y-%m-%d'),
                'current_price': entry_price,
                'current_value': position_amount,
                'profit_loss': 0,
                'profit_loss_pct': 0,
                'holding_days': 0
            })
            
            available_capital -= position_amount
            print(f"🟢 새 종목 추가: {stock['symbol']} (매수가: ${entry_price:.2f}, 수량: {shares}주)")
        
        # 포트폴리오 저장
        if portfolio:
            portfolio_df = pd.DataFrame(portfolio)
            # 비중(%) 컬럼 추가
            total_portfolio_value = portfolio_df['position_amount'].sum()
            if total_portfolio_value > 0:
                portfolio_df['비중(%)'] = (portfolio_df['position_amount'] / total_portfolio_value) * 100
            else:
                portfolio_df['비중(%)'] = 0
            
            portfolio_df.to_csv(portfolio_file, index=False, mode='w')
            
            # 포트폴리오 요약 출력
            total_value = portfolio_df['current_value'].sum()
            total_profit_loss = portfolio_df['profit_loss'].sum()
            
            print(f"\n💼 포트폴리오 생성 완료: {len(portfolio_df)}개 종목, 총 가치: ${total_value:.2f}")
            print(f"💰 총 수익/손실: ${total_profit_loss:.2f}")
            print(f"💵 남은 현금: ${available_capital:.2f}")
            
            # 포트폴리오 상세 출력
            print("\n📊 포트폴리오 상세:")
            summary_cols = ['symbol', 'price', 'current_price', 'shares', 'current_value', 'profit_loss_pct', 'stop_loss', 'profit_protection', 'max_hold_days', '비중(%)'] # target_price를 profit_protection으로 변경
            print(portfolio_df[summary_cols])
            
            return portfolio_df
        else:
            print("❌ 포트폴리오에 추가할 종목이 없습니다.")
            # 빈 포트폴리오 파일 생성 (기존 파일 덮어쓰기)
            pd.DataFrame(columns=[
                'symbol', 'price', 'shares', 'position_amount', 'stop_loss', 'profit_protection', 'max_hold_days',
                'entry_date', 'current_price', 'current_value', 'profit_loss', 'profit_loss_pct', 'holding_days', '비중(%)'
            ]).to_csv(portfolio_file, index=False, mode='w')
            return None
            
    except Exception as e:
        import traceback
        print(f"❌ 포트폴리오 생성 오류 (전략 3): {e}")
        print(traceback.format_exc())
        return None

# 전략 3 포트폴리오 추적 및 업데이트 함수
def track_portfolio_strategy3(total_capital=100000):
    """전략 3 포트폴리오 추적 및 업데이트 함수
    
    Args:
        total_capital: 총 자본금 (기본값: 10만 달러)
    """
    print("\n🔍 전략 3 포트폴리오 추적 및 업데이트 시작...")
    
    # 스크리닝 실행 및 포트폴리오 업데이트
    # run_strategy3 함수 내에서 create_portfolio_strategy3가 호출되므로, 
    # 여기서는 screened_stocks를 다시 생성하지 않고, 기존 포트폴리오 업데이트 로직만 수행
    portfolio_file = os.path.join(RESULTS_VER2_DIR, 'portfolio_strategy3.csv')
    if not os.path.exists(portfolio_file):
        print(f"⚠️ {portfolio_file} 파일이 없어 추적할 포트폴리오가 없습니다. 먼저 스크리닝을 실행하세요.")
        # 스크리닝을 먼저 실행하도록 유도하거나, 빈 포트폴리오로 시작할 수 있음
        # 여기서는 스크리닝을 다시 실행하여 포트폴리오를 생성/업데이트하도록 함
        run_strategy3() # 이 경우, run_strategy3 내의 create_portfolio_strategy3가 update_existing=True로 호출되어야 함
                      # 또는, 아래처럼 직접 호출
        # screened_stocks_df = run_strategy3() # run_strategy3가 screened_stocks_df를 반환하도록 수정 필요
        # if screened_stocks_df is not None:
        #    create_portfolio_strategy3(screened_stocks_df, total_capital=total_capital, update_existing=True)
        return

    # 기존 포트폴리오가 있다면, 해당 포트폴리오를 기준으로 업데이트
    # 이 부분은 create_portfolio_strategy3(screened_stocks=pd.DataFrame(), total_capital=total_capital, update_existing=True) 와 유사하게 동작
    # screened_stocks가 비어있으면, 기존 포트폴리오만 업데이트하고 새로운 종목은 추가하지 않음.
    # 또는, run_strategy3()를 호출하여 새로운 스크리닝 결과를 바탕으로 업데이트 할 수 있음.
    # 여기서는 명시적으로 update_existing=True로 create_portfolio_strategy3를 호출하는 방식을 가정
    # (run_strategy3가 screened_stocks를 반환하고, create_portfolio_strategy3가 update_existing를 고려하도록 수정 필요)
    
    # 현재 구조에서는 run_strategy3()를 호출하면 그 안에서 create_portfolio_strategy3(..., update_existing=False)가 호출됨.
    # 추적 기능을 제대로 구현하려면, run_strategy3()가 스크리닝 결과만 반환하고,
    # track_portfolio_strategy3에서 create_portfolio_strategy3(screened_results, update_existing=True)를 호출하는 것이 좋음.
    # 또는 run_strategy3에 update_portfolio 파라미터를 추가하여 제어할 수 있음.

    # 임시 해결: 스크리닝을 다시 실행하고, 그 결과를 바탕으로 포트폴리오를 '업데이트' 모드로 생성
    # 참고: run_strategy3가 screened_df를 반환하도록 수정하고, create_portfolio_strategy3의 update_existing=True로 호출해야 함.
    # 현재 run_strategy3는 screened_df를 반환하지 않으므로, 이 방식은 바로 동작하지 않음.
    # 가장 간단한 방법은 run_strategy3 내부의 create_portfolio_strategy3 호출 시 update_existing=True로 변경하는 것임.
    # 하지만 이는 run_strategy3의 기본 동작을 변경하므로, 별도의 추적 로직이 필요.

    # 여기서는 단순히 메시지만 출력하고, 실제 업데이트는 create_portfolio_strategy3에서 처리하도록 함
    # 사용자가 run_investment_strategies.bat 등을 통해 실행할 때, 
    # run_strategy3가 호출되면서 포트폴리오가 생성/갱신될 것으로 기대.
    print(f"ℹ️ 전략 3 포트폴리오 파일 ({portfolio_file})은 run_strategy3 실행 시 생성/업데이트됩니다.")
    # 실제 업데이트 로직을 원한다면, create_portfolio_strategy3를 적절한 screened_stocks와 함께 호출해야 함.
    # 예: create_portfolio_strategy3(pd.DataFrame(), total_capital=total_capital, update_existing=True)
    # 위 코드는 새로운 스크리닝 없이 기존 포트폴리오만 업데이트 (손절/익절/기간만료 처리)

# 전략 4: 과매도된 주식 스크리닝 (RSI 기반)
def run_strategy4():
    """네 번째 전략 실행 함수 - 과매도된 주식 식별 및 추세 추종 전략
    
    필터:
    1. 일평균 거래 금액이 지난 50일 동안 1억 달러 이상이어야 한다.
    2. 변동성 메트릭스에서 낮은 쪽에 위치하는 역사적 변동성이 10%에서 40% 사이여야 한다.
    
    설정:
    1. S&P500 종가가 200일 단순이동평균보다 위에 있어야 한다. 이것은 시장이 상승세임을 의미한다.
    2. 해당 주식의 종가가 200일 단순이동평균보다 위에 있어야 한다.
    
    순위: 4일간 RSI가 가장 낮은 순(과매도의 정도가 가장 심한 것을 의미함)으로 순위를 매긴다.
    
    시장 진입: 장 시작 때 시장가로 매수한다. 슬리피지와 상관없이 반드시 매수한다.
    손절매: 매수 당일, 체결 가격을 기준으로 최근 40일 ATR의 1.5배 아래에 손절매를 설정한다.
    시장 재진입: 가능
    수익 보호: 20%의 추격 역지정가 주문을 설정한다. 주가가 지속적으로 상승할 때 수익을 보호해준다.
    차익실현: 추세가 지속될 때까지 차익을 실현하지 않고 계속 보유한다.
    포지션 크기: 포지션별 총자산 대비 2%의 위험, 총자산 대비 최대 10%의 자산 배분
    """
    print("\n🔍 전략 4: 과매도된 주식 스크리닝 시작...")
    
    # 결과 파일 경로 (매수 전략이므로 buy 폴더에 저장)
    buy_dir = os.path.join(RESULTS_VER2_DIR, 'buy')
    ensure_dir(buy_dir)
    result_file = os.path.join(buy_dir, 'strategy4_results.csv')
    
    # S&P 500 조건 확인
    sp500_condition = check_sp500_condition(DATA_US_DIR)
    if not sp500_condition:
        print("❌ S&P 500 조건을 충족하지 않습니다. 스크리닝을 중단합니다.")
        # 빈 결과 파일 생성
        pd.DataFrame(columns=[
            'symbol', 'entry_price', 'stop_loss', 'profit_protection', 'position_size_percentage'
        ]).to_csv(result_file, index=False)
        return
    
    try:
        # 개별 CSV 파일 로드
        us_files = [f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
        if not us_files:
            print("❌ 미국 주식 데이터 파일이 없습니다.")
            return
            
        print(f"📊 {len(us_files)}개 미국 주식 파일 처리 중...")
        
        # 개별 종목 처리
        results = []
        for i, file in enumerate(us_files):
            if i % 100 == 0 and i > 0:
                print(f"⏳ 진행 중: {i}/{len(us_files)} 종목 처리됨")
                
            try:
                # 데이터 처리
                symbol, df, recent_data = process_stock_data(file, DATA_US_DIR, min_days=200, recent_days=200)
                if symbol is None or df is None or recent_data is None:
                    continue
                
                # 필터 1: 일평균 거래 금액이 지난 50일 동안 1억 달러 이상
                recent_50d = recent_data.iloc[-50:]
                avg_dollar_volume = (recent_50d['volume'] * recent_50d['close']).mean()
                if avg_dollar_volume < 100000000:  # 1억 달러
                    continue
                
                # 필터 2: 변동성 메트릭스에서 낮은 쪽에 위치하는 역사적 변동성이 10%에서 40% 사이
                volatility = calculate_historical_volatility(recent_data)
                if volatility < 10.0 or volatility > 40.0:
                    continue
                
                # 설정 1: S&P500 종가가 200일 단순이동평균보다 위에 있어야 한다 (이미 check_sp500_condition()에서 확인됨)
                
                # 설정 2: 해당 주식의 종가가 200일 단순이동평균보다 위에 있어야 한다
                recent_data['ma200'] = recent_data['close'].rolling(window=200).mean()
                latest = recent_data.iloc[-1]
                latest_close = float(latest['close'])
                latest_ma200 = float(latest['ma200'])
                if latest_close <= latest_ma200:
                    continue
                
                # 순위: 4일간 RSI가 가장 낮은 순으로 순위를 매긴다
                recent_data['rsi_4d'] = calculate_rsi(recent_data, window=4)
                latest_rsi = float(recent_data.iloc[-1]['rsi_4d'])
                
                # RSI 값이 없는 경우 건너뛰기
                if pd.isna(latest_rsi):
                    continue
                
                # 시장 진입: 장 시작 때 시장가로 매수
                entry_price = latest['close']  # 현재 종가를 진입가로 설정 (장 시작 시 시장가 매수 가정)
                
                # 손절매: 매수 당일, 체결 가격을 기준으로 최근 40일 ATR의 1.5배 아래에 손절매를 설정
                atr_40d = calculate_atr(recent_data.iloc[-40:], window=40)
                stop_loss = entry_price - (atr_40d * 1.5)
                
                # 수익 보호: 20%의 추격 역지정가 주문을 설정
                trailing_stop_pct = 20.0  # 20% 추적 손절매
                
                # 위험 금액 = 진입가 - 손절가
                risk_per_share = entry_price - stop_loss
                
                # 모든 조건을 충족하는 종목 결과에 추가
                results.append({
                    'symbol': symbol,
                    'price': latest['close'],
                    'avg_volume': recent_50d['volume'].mean(),
                    'avg_dollar_volume': avg_dollar_volume,
                    'volatility': volatility,
                    'rsi_4d': latest_rsi,
                    'entry_price': round(entry_price, 2),
                    'stop_loss': round(stop_loss, 2),
                    'trailing_stop_pct': trailing_stop_pct,
                    'risk_per_share': round(risk_per_share, 2)
                })
                
            except Exception as e:
                import traceback
                print(f"❌ {file} 처리 오류: {e}")
                print(traceback.format_exc())
        
        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=[
                'symbol', 'entry_price', 'stop_loss', 'profit_protection', 'position_size_percentage'
            ]).to_csv(result_file, index=False, mode='w')
            return
        
        # 결과 데이터프레임 생성
        result_df = pd.DataFrame(results)
        
        # RSI 기준으로 오름차순 정렬 (낮은 RSI가 위에 오도록)
        result_df = result_df.sort_values('rsi_4d', ascending=True)
        
        # 상위 20개 종목만 선택
        result_df = result_df.head(20)
        
        # 포지션 크기 계산 (총자산 대비 2%의 위험, 총자산 대비 최대 10% 자산 배분)
        # 가정: 총자산 = 100,000 달러
        total_assets = 100000
        risk_per_position = total_assets * 0.02  # 포지션당 2% 위험
        max_position_size = total_assets * 0.10  # 최대 10% 자산 배분
        
        # 각 종목별 포지션 크기 계산
        result_df['position_size'] = (risk_per_position / result_df['risk_per_share']).round(0)
        result_df['position_value'] = result_df['position_size'] * result_df['entry_price']
        
        # 최대 포지션 크기 제한 적용
        result_df.loc[result_df['position_value'] > max_position_size, 'position_size'] = \
            (max_position_size / result_df['entry_price']).round(0)
        
        # 최종 포지션 가치 재계산
        result_df['position_value'] = result_df['position_size'] * result_df['entry_price']
        
        # 포지션 크기 백분율 계산
        total_assets_for_calc = 100000 # 임시 총 자산 (run_strategy4 내 total_assets와 동일하게)
        result_df['position_size_percentage'] = (result_df['position_value'] / total_assets_for_calc) * 100
        result_df['position_size_percentage'] = result_df['position_size_percentage'].round(2)

        # 결과 CSV에 포함할 컬럼 선택 및 이름 변경
        # 'trailing_stop_pct'를 'profit_protection'으로 간주하고 컬럼명 변경
        strategy_result_columns = ['symbol', 'entry_price', 'stop_loss', 'trailing_stop_pct', 'position_size_percentage']
        result_df_to_save = result_df[strategy_result_columns].rename(columns={'trailing_stop_pct': 'profit_protection'})
        
        # 결과 저장 (buy_dir은 이미 생성되어 있음)
        result_df_to_save.to_csv(result_file, index=False)
        print(f"✅ 전략 4 스크리닝 결과 저장 완료: {len(result_df_to_save)}개 종목, 경로: {result_file}")
        
        # 상위 20개 종목 출력 (요청된 형식으로)
        print("\n🏆 전략 4 상위 20개 종목 (스크리닝 결과):")
        print(result_df_to_save.head(20))
        
    except Exception as e:
        print(f"❌ 전략 4 스크리닝 오류: {e}")

# 전략 5: ADX와 RSI 기반 추세 추종 전략
def run_strategy5(total_assets=100000, update_existing=False):
    """다섯 번째 전략 실행 함수 - ADX와 RSI 기반 추세 추종 전략
    
    필터:
    1. 최근 50일 기준 일평균 거래량이 최소 50만 주 이상이어야 하고, 최근 50일 기준 일평균 거래 금액이 최소 250만 달러 이상이어야 한다.
    2. ATR은 4 이상이어야 한다.
    
    설정:
    1. 종가는 100일 단순이동평균과 최근 10일의 1ATR보다 위에 있어야 한다.
    2. 7일 ADX가 55보다 커야 한다.
    3. 3일 RSI가 50보다 작아야 한다.
    
    순위: 7일 ADX가 가장 높은 순으로 순위를 부여하여 상위 20개만 선택
    
    시장 진입: 직전 종가보다 최대 3% 낮은 가격으로 매수한다.
    손절매: 매수 당일, 체결 가격을 기준으로 최근 10일 ATR의 3배 아래에 손절매를 설정한다.
    시장 재진입: 가능
    수익 보호: 없음
    차익 실현: 종가가 최근 10일의 1ATR보다 높으면 다음 날 장 시작 때 시장가로 매도한다.
    시간 기준: 6거래일 후에도 아직 매도하지 않았고 목표 수익도 달성하지 못했을 경우, 그다음 날 장 시작 때 시장가로 매도한다.
    포지션 크기: 포지션별 총자산 대비 2%의 위험, 총자산 대비 최대 10%의 자산 배분
    """
    print("\n🔍 전략 5: ADX와 RSI 기반 추세 추종 전략 스크리닝 시작...")
    
    # 결과 파일 경로 (매수 전략이므로 buy 폴더에 저장)
    buy_dir = os.path.join(RESULTS_VER2_DIR, 'buy')
    ensure_dir(buy_dir)
    result_file = os.path.join(buy_dir, 'strategy5_results.csv')
    
    try:
        # 개별 CSV 파일 로드
        us_files = [f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
        if not us_files:
            print("❌ 미국 주식 데이터 파일이 없습니다.")
            return
            
        print(f"📊 {len(us_files)}개 미국 주식 파일 처리 중...")
        
        # 개별 종목 처리
        results = []
        for i, file in enumerate(us_files):
            if i % 100 == 0 and i > 0:
                print(f"⏳ 진행 중: {i}/{len(us_files)} 종목 처리됨")
                
            try:
                # 데이터 처리
                symbol, df, recent_data = process_stock_data(file, DATA_US_DIR, min_days=100, recent_days=100)
                if symbol is None or df is None or recent_data is None:
                    continue
                
                # 필터 1: 최근 50일 기준 일평균 거래량이 최소 50만 주 이상이어야 하고, 최근 50일 기준 일평균 거래 금액이 최소 250만 달러 이상
                recent_50d = recent_data.iloc[-50:]
                avg_volume_50d = recent_50d['volume'].mean()
                avg_dollar_volume_50d = (recent_50d['volume'] * recent_50d['close']).mean()
                
                if avg_volume_50d < 500000 or avg_dollar_volume_50d < 2500000:  # 50만주, 250만 달러
                    continue
                
                # 필터 2: ATR은 4 이상이어야 한다
                recent_10d = recent_data.iloc[-10:]
                atr_10d = calculate_atr(recent_10d, window=10).iloc[-1]
                if atr_10d < 4.0:
                    continue
                
                # 설정 1: 종가는 100일 단순이동평균과 최근 10일의 1ATR보다 위에 있어야 한다
                recent_data['ma100'] = recent_data['close'].rolling(window=100).mean()
                latest = recent_data.iloc[-1]
                latest_close = float(latest['close'])
                latest_ma100 = float(latest['ma100'])
                atr_threshold = latest_close - atr_10d  # 종가에서 1ATR을 뺀 값
                
                if latest_close <= latest_ma100 or latest_close <= atr_threshold:
                    continue
                
                # 설정 2: 7일 ADX가 55보다 커야 한다
                recent_data['adx_7d'] = calculate_adx(recent_data, window=7)
                latest_adx = float(recent_data.iloc[-1]['adx_7d'])
                
                # ADX 값이 없는 경우 건너뛰기
                if pd.isna(latest_adx) or latest_adx <= 55:
                    continue
                
                # 설정 3: 3일 RSI가 50보다 작아야 한다
                recent_data['rsi_3d'] = calculate_rsi(recent_data, window=3)
                latest_rsi = float(recent_data.iloc[-1]['rsi_3d'])
                
                # RSI 값이 없는 경우 건너뛰기
                if pd.isna(latest_rsi) or latest_rsi >= 50:
                    continue
                
                # 시장 진입: 직전 종가보다 최대 3% 낮은 가격으로 매수
                entry_price = latest['close'] * 0.97  # 3% 낮은 가격
                
                # 손절매: 매수 당일, 체결 가격을 기준으로 최근 10일 ATR의 3배 아래에 손절매를 설정
                stop_loss = entry_price - (atr_10d * 3)
                
                # 차익 실현: 종가가 최근 10일의 1ATR보다 높으면 다음 날 장 시작 때 시장가로 매도
                target_price = entry_price + atr_10d
                
                # 최대 보유 기간: 6거래일
                max_hold_days = 6
                
                # 위험 금액 = 진입가 - 손절가
                risk_per_share = entry_price - stop_loss
                
                # 모든 조건을 충족하는 종목 결과에 추가
                results.append({
                    'symbol': symbol,
                    'price': latest['close'],
                    'avg_volume': avg_volume_50d,
                    'avg_dollar_volume': avg_dollar_volume_50d,
                    'atr': atr_10d,
                    'adx_7d': latest_adx,
                    'rsi_3d': latest_rsi,
                    'entry_price': round(entry_price, 2),
                    'stop_loss': round(stop_loss, 2),
                    'target_price': round(target_price, 2),
                    'max_hold_days': max_hold_days,
                    'risk_per_share': round(risk_per_share, 2)
                })
                
            except Exception as e:
                import traceback
                print(f"❌ {file} 처리 오류: {e}")
                print(traceback.format_exc())
        
        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=[
                'symbol', 'entry_price', 'stop_loss', 'profit_protection', 'position_size_percentage'
            ]).to_csv(result_file, index=False, mode='w')
            return
        
        # 결과 데이터프레임 생성
        result_df = pd.DataFrame(results)
        
        # ADX 기준으로 내림차순 정렬 (높은 ADX가 위에 오도록)
        result_df = result_df.sort_values('adx_7d', ascending=False)
        
        # 상위 20개 종목만 선택
        result_df = result_df.head(20)
        
        # 포지션 크기 계산 (총자산 대비 2%의 위험, 총자산 대비 최대 10%의 자산 배분)
        # total_assets는 함수 파라미터로 받음
        risk_per_position = total_assets * 0.02  # 포지션당 2% 위험
        max_position_size = total_assets * 0.10  # 최대 10% 자산 배분
        
        # 각 종목별 포지션 크기 계산
        result_df['position_size'] = (risk_per_position / result_df['risk_per_share']).round(0)
        result_df['position_value'] = result_df['position_size'] * result_df['entry_price']
        
        # 최대 포지션 크기 제한 적용
        result_df.loc[result_df['position_value'] > max_position_size, 'position_size'] = \
            (max_position_size / result_df['entry_price']).round(0)
        
        # 최종 포지션 가치 재계산
        result_df['position_value'] = result_df['position_size'] * result_df['entry_price']
        
        # 포지션 크기 백분율 계산
        # total_assets는 함수 파라미터로 받음
        result_df['position_size_percentage'] = (result_df['position_value'] / total_assets) * 100
        result_df['position_size_percentage'] = result_df['position_size_percentage'].round(2)

        # 결과 CSV에 포함할 컬럼 선택 및 이름 변경
        # 'target_price'를 'profit_protection'으로 간주하고 컬럼명 변경
        strategy_result_columns = ['symbol', 'entry_price', 'stop_loss', 'target_price', 'position_size_percentage']
        result_df_to_save = result_df[strategy_result_columns].rename(columns={'target_price': 'profit_protection'})

        # 결과 저장 (buy_dir은 이미 생성되어 있음)
        result_df_to_save.to_csv(result_file, index=False)
        print(f"✅ 전략 5 스크리닝 결과 저장 완료: {len(result_df_to_save)}개 종목, 경로: {result_file}")
        
        # 상위 20개 종목 출력 (요청된 형식으로)
        print("\n🏆 전략 5 상위 20개 종목 (스크리닝 결과):")
        print(result_df_to_save.head(20))

        # 포트폴리오 생성/업데이트
        create_portfolio_strategy5(result_df, total_assets=total_assets, update_existing=update_existing)
        
    except Exception as e:
        print(f"❌ 전략 5 스크리닝 오류: {e}")
        print(traceback.format_exc())

# 메인 실행 함수
def main():
    """메인 실행 함수"""
    start_time = time.time()
    print("🚀 투자 스크리너 Ver3 실행 중...")
    
    # 필요한 디렉토리 생성
    create_required_dirs()
    
    # 전략 선택 (기본값: 전략 5)
    strategy = 5
    if len(sys.argv) > 1:
        try:
            strategy = int(sys.argv[1])
        except ValueError:
            print(f"⚠️ 잘못된 전략 번호입니다. 기본값인 전략 {strategy}를 실행합니다.")
    
    # 선택된 전략 실행
    if strategy == 3:
        run_strategy3() # run_strategy3는 이미 내부적으로 포트폴리오 생성 로직 호출
    elif strategy == 4:
        run_strategy4() # run_strategy4는 포트폴리오 로직이 이미 내장되어 있음
    elif strategy == 5:
        run_strategy5(total_assets=100000, update_existing=False) # 기본값으로 포트폴리오 생성
    else:
        print(f"⚠️ 지원되지 않는 전략 번호입니다. 기본값인 전략 5를 실행합니다.")
        run_strategy5(total_assets=100000, update_existing=False) # 기본값으로 포트폴리오 생성
    
    # 실행 시간 출력
    elapsed_time = time.time() - start_time
    print(f"\n⏱️ 총 실행 시간: {elapsed_time:.2f}초")

# 스크립트가 직접 실행될 때만 main() 함수 호출
# 전략 5 포트폴리오 생성 함수
def create_portfolio_strategy5(screened_stocks, total_capital=100000, update_existing=False):
    """전략 5 포트폴리오 생성 함수
    
    Args:
        screened_stocks: 스크리닝된 종목 DataFrame
        total_capital: 총 자본금 (기본값: 10만 달러)
        update_existing: 기존 포트폴리오 업데이트 여부 (기본값: False)
    """
    try:
        portfolio_file = os.path.join(RESULTS_VER2_DIR, 'portfolio_strategy5.csv')
        existing_portfolio = None
        if update_existing and os.path.exists(portfolio_file):
            try:
                existing_portfolio = pd.read_csv(portfolio_file)
                print(f"📊 기존 포트폴리오 로드 (전략 5): {len(existing_portfolio)}개 종목")
            except Exception as e:
                print(f"⚠️ 기존 포트폴리오 로드 오류 (전략 5): {e}")
                print(traceback.format_exc())
                existing_portfolio = None
        
        portfolio = []
        available_capital = total_capital
        existing_symbols = set()

        if existing_portfolio is not None and not existing_portfolio.empty:
            for _, position in existing_portfolio.iterrows():
                symbol = position['symbol']
                existing_symbols.add(symbol)
                current_price, low_price = get_latest_price_data_low_strategy3(symbol) # 유사 함수 사용, 필요시 strategy5용으로 수정
                
                if current_price is None:
                    portfolio.append(position.to_dict())
                    available_capital -= position.get('position_amount', 0)
                    continue
                
                stop_loss = position['stop_loss']
                if low_price <= stop_loss:
                    print(f"🔴 손절매 실행 (전략 5): {symbol} (매수가: ${position['price']:.2f}, 손절매가: ${stop_loss:.2f})")
                    continue
                
                profit_protection_price = position['profit_protection'] # target_price를 profit_protection으로 변경
                if current_price >= profit_protection_price: # 수익보호 가격 도달 시 매도
                    print(f"🟢 수익보호 가격 도달 (전략 5): {symbol} (매수가: ${position['price']:.2f}, 현재가: ${current_price:.2f}, 수익보호가: ${profit_protection_price:.2f})")
                    continue

                entry_date = pd.to_datetime(position['entry_date'])
                current_date = datetime.now()
                holding_days = (current_date - entry_date).days
                max_hold_days = position.get('max_hold_days', 6)

                if holding_days >= max_hold_days:
                    print(f"🟡 보유 기간 초과 (전략 5): {symbol} (매수가: ${position['price']:.2f}, 현재가: ${current_price:.2f}, 보유일: {holding_days}일)")
                    continue
                
                updated_position = position.to_dict()
                updated_position['current_price'] = current_price
                updated_position['current_value'] = current_price * position['shares']
                updated_position['profit_loss'] = (current_price - position['price']) * position['shares']
                updated_position['profit_loss_pct'] = ((current_price - position['price']) / position['price']) * 100 if position['price'] > 0 else 0
                updated_position['holding_days'] = holding_days
                portfolio.append(updated_position)
                available_capital -= updated_position.get('current_value',0)

        for _, stock in screened_stocks.iterrows():
            if stock['symbol'] in existing_symbols:
                continue
            if available_capital <= 0 or len(portfolio) >= 20: # 최대 20개 포지션
                break
            
            entry_price = stock['entry_price']
            shares = stock['position_size'] # 스크리닝 결과에서 계산된 position_size 사용
            position_amount = shares * entry_price

            if position_amount > available_capital:
                shares = int(available_capital / entry_price)
                position_amount = shares * entry_price
                if shares <= 0:
                    continue
            
            portfolio.append({
                'symbol': stock['symbol'],
                'price': entry_price,
                'shares': shares,
                'position_amount': position_amount,
                'stop_loss': stock['stop_loss'],
                'profit_protection': stock['target_price'], # target_price를 profit_protection으로 변경
                'max_hold_days': stock['max_hold_days'],
                'entry_date': datetime.now().strftime('%Y-%m-%d'),
                'current_price': entry_price,
                'current_value': position_amount,
                'profit_loss': 0,
                'profit_loss_pct': 0,
                'holding_days': 0
            })
            available_capital -= position_amount
            print(f"🟢 새 종목 추가 (전략 5): {stock['symbol']} (매수가: ${entry_price:.2f}, 수량: {shares}주)")

        if portfolio:
            portfolio_df = pd.DataFrame(portfolio)
            total_portfolio_value = portfolio_df['position_amount'].sum()
            if total_portfolio_value > 0:
                portfolio_df['비중(%)'] = (portfolio_df['position_amount'] / total_portfolio_value) * 100
            else:
                portfolio_df['비중(%)'] = 0
            
            portfolio_df.to_csv(portfolio_file, index=False, mode='w', encoding='utf-8-sig')
            total_value = portfolio_df['current_value'].sum()
            total_profit_loss = portfolio_df['profit_loss'].sum()
            print(f"\n💼 포트폴리오 생성 완료 (전략 5): {len(portfolio_df)}개 종목, 총 가치: ${total_value:.2f}")
            print(f"💰 총 수익/손실 (전략 5): ${total_profit_loss:.2f}")
            print(f"💵 남은 현금 (전략 5): ${available_capital:.2f}")
            summary_cols = ['symbol', 'price', 'current_price', 'shares', 'current_value', 'profit_loss_pct', 'stop_loss', 'profit_protection', 'max_hold_days', '비중(%)'] # target_price를 profit_protection으로 변경
            print("\n📊 포트폴리오 상세 (전략 5):")
            print(portfolio_df[summary_cols])
            return portfolio_df
        else:
            print("❌ 포트폴리오에 추가할 종목이 없습니다 (전략 5).")
            pd.DataFrame(columns=[
                'symbol', 'price', 'shares', 'position_amount', 'stop_loss', 'profit_protection', 'max_hold_days',
                'entry_date', 'current_price', 'current_value', 'profit_loss', 'profit_loss_pct', 'holding_days', '비중(%)'
            ]).to_csv(portfolio_file, index=False, mode='w', encoding='utf-8-sig')
            return None
            
    except Exception as e:
        print(f"❌ 포트폴리오 생성 오류 (전략 5): {e}")
        print(traceback.format_exc())
        return None

# 참고: S&P 500 조건 확인 함수는 utils.py의 check_sp500_condition으로 이동됨

if __name__ == "__main__":
    main()