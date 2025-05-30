# -*- coding: utf-8 -*-
# 투자 스크리너 Ver6 - 추가 스크리닝 전략 모듈 (전략 6: 단기 상승 종목 공매도)

import os
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
                  calculate_atr, check_sp500_condition, process_stock_data)

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

# 참고: ATR(Average True Range) 계산 함수는 utils.py로 이동됨

# 가격 상승률 계산 함수
def calculate_price_increase(df, days=6):
    """지정된 기간 동안의 가격 상승률을 계산하는 함수
    
    Args:
        df: 가격 데이터가 포함된 DataFrame (close 컬럼 필요)
        days: 상승률 계산 기간 (기본값: 6일)
        
    Returns:
        float: 상승률 (%) - 양수 값이 상승을 의미
    """
    try:
        if len(df) < days + 1:
            return 0.0
            
        # n일 전 종가와 현재 종가
        current_close = df.iloc[-1]['close']
        past_close = df.iloc[-(days+1)]['close']
        
        # 상승률 계산 (양수 값이 상승을 의미)
        increase_pct = ((current_close - past_close) / past_close) * 100
        
        return increase_pct
    except Exception as e:
        import traceback
        print(f"❌ 상승률 계산 오류: {e}")
        print(traceback.format_exc())
        return 0.0

# 최근 2일 상승 여부 확인 함수
def check_recent_price_increase(df):
    """최근 2일 동안 가격이 상승했는지 확인하는 함수
    
    Args:
        df: 가격 데이터가 포함된 DataFrame (close 컬럼 필요)
        
    Returns:
        bool: 최근 2일 동안 가격이 상승했으면 True, 아니면 False
    """
    try:
        if len(df) < 3:  # 최소 3일 데이터 필요 (오늘, 어제, 그제)
            return False
                
        today_close = float(df.iloc[-1]['close'])
        yesterday_close = float(df.iloc[-2]['close'])
        day_before_yesterday_close = float(df.iloc[-3]['close'])
        
        # 최근 2일 동안 가격이 상승했는지 확인
        # pandas Series에 대한 불리언 연산을 피하기 위해 스칼라 값으로 비교
        condition1 = today_close > yesterday_close
        condition2 = yesterday_close > day_before_yesterday_close
        return condition1 and condition2
    except Exception as e:
        import traceback
        print(f"❌ 최근 가격 상승 확인 오류: {e}")
        print(traceback.format_exc())
        return False

# 전략 6: 단기 상승 종목 공매도 전략
def run_strategy6(create_portfolio=True, total_capital=100000, update_existing=False):
    """여섯 번째 전략 실행 함수 - 단기 상승 종목 공매도 전략
    
    조건:
    1. 최소 주가는 5달러 이상
    2. 최근 50일 기준 일평균 거래 금액이 최소 1,000만 달러 이상
    3. 주가가 최근 6거래일 동안 최소 20% 상승한 종목
    4. 직전 2일 동안 주가가 상승한 종목
    5. 6일 동안 가격이 가장 많이 오른 종목 순으로 순위를 부여해 상위 20개 선택
    
    포트폴리오 관리:
    - 매도가: 직전 종가보다 최대 5% 높은 가격에 지정가로 공매도
    - 손절매: 진입 당일, 체결 가격을 기준으로 최근 10일 ATR의 3배 위 지점에 손절매 설정
    - 수익 실현: 5%의 수익이 나면, 다음 날 장 마감 때 시장가로 환매
    - 또는 시간 기준으로 3거래일 후 장 마감 때 시장가로 환매
    - 포지션 크기: 투자자산 대비 2%의 위험, 단일 매매에 사용하는 시스템 자산의 최대 10% 배분
    
    Args:
        create_portfolio: 스크리닝 후 포트폴리오 생성 여부 (기본값: True)
        total_capital: 총 자본금 (기본값: 10만 달러)
        update_existing: 기존 포트폴리오 업데이트 여부 (기본값: False)
    """
    print("\n🔍 전략 6: 단기 상승 종목 공매도 전략 스크리닝 시작...")
    
    # 결과 파일 경로 (매도 전략이므로 sell 폴더에 저장)
    sell_dir = os.path.join(RESULTS_VER2_DIR, 'sell')
    ensure_dir(sell_dir)
    result_file = os.path.join(sell_dir, 'strategy6_results.csv')
    
    try:
        # S&P 500 조건 확인
        sp500_condition = check_sp500_condition(DATA_US_DIR)
        if not sp500_condition:
            print("❌ S&P 500 조건을 충족하지 않습니다. 스크리닝을 중단합니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=['symbol', 'price', 'avg_volume_value', 'price_increase_6d', 'atr_10d', 'entry_price', 'stop_loss', 'target_price', 'max_hold_days', 'position_size']).to_csv(result_file, index=False)
            return
            
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
                    
                # 참고: 데이터 처리는 process_stock_data 함수에서 이미 수행됨
                
                # 조건 1: 최소 주가는 5달러 이상
                latest_close = float(recent_data.iloc[-1]['close'])
                if latest_close < 5.0:
                    continue
                
                # 조건 2: 최근 50일 기준 일평균 거래 금액이 최소 1,000만 달러 이상
                recent_50d = recent_data.iloc[-50:]
                avg_volume_value = (recent_50d['close'] * recent_50d['volume']).mean()
                if avg_volume_value < 10000000:  # 1000만 달러
                    continue
                
                # 조건 3: 주가가 최근 6거래일 동안 최소 20% 상승한 종목
                price_increase = calculate_price_increase(recent_data, days=6)
                if price_increase < 20.0:
                    continue
                
                # 조건 4: 직전 2일 동안 주가가 상승한 종목
                if not check_recent_price_increase(recent_data):
                    continue
                
                # ATR 계산 (직전 10일 기준)
                atr_10d = calculate_atr(recent_data.iloc[-10:], window=10).iloc[-1]
                
                # 매도가 설정 (직전 종가보다 5% 높은 가격)
                entry_price = latest_close * 1.05
                
                # 손절매: 매도가 기준 직전 10일 ATR의 3배 위 지점
                stop_loss = entry_price + (atr_10d * 3)
                
                # 수익실현: 매도가 대비 5% 하락 시
                profit_target = entry_price * 0.95
                
                # 포지션 크기: 포지션별 총자산 대비 2%의 위험비율, 최대 10% 배분
                risk_amount = stop_loss - entry_price
                if risk_amount <= 0:  # 위험 금액이 0 이하인 경우 처리
                    position_size = 0.1  # 기본값 10%
                else:
                    position_size_by_risk = 0.02 / (risk_amount / entry_price)  # 2% 위험 비율
                    position_size = min(position_size_by_risk, 0.1)  # 10%와 비교하여 작은 값 선택
                
                # 모든 조건을 충족하는 종목 결과에 추가
                results.append({
                    'symbol': symbol,
                    'price': latest_close,
                    'avg_volume_value': avg_volume_value,
                    'price_increase': price_increase,
                    'entry_price': entry_price,
                    'stop_loss': stop_loss,
                    'profit_target': profit_target,
                    'position_size': position_size
                })
                
            except Exception as e:
                import traceback
                print(f"❌ {file} 처리 오류: {e}")
                print(traceback.format_exc())
                continue
        
        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=['symbol', 'price', 'avg_volume_value', 'price_increase', 'entry_price', 'stop_loss', 'profit_target', 'position_size']).to_csv(result_file, index=False, mode='w')
            return
        
        # 결과 데이터프레임 생성
        result_df = pd.DataFrame(results)
        
        # 가격 상승률 기준으로 내림차순 정렬
        result_df = result_df.sort_values('price_increase', ascending=False)
        
        # 상위 20개 종목만 선택
        result_df = result_df.head(20)
        
        # 결과 저장
        result_df.to_csv(result_file, index=False, mode='w')
        print(f"✅ 결과 저장 완료: {len(result_df)}개 종목, 경로: {result_file}")
        
        # 상위 10개 종목 출력
        print("\n🏆 전략 6 상위 10개 매도 대상 종목:")
        print(result_df[['symbol', 'price', 'price_increase', 'entry_price', 'stop_loss', 'profit_target']].head(10))
        
        # 포트폴리오 생성 또는 업데이트
        # create_portfolio_strategy6 함수는 screened_stocks, total_capital, update_existing를 인자로 받음
        # run_strategy6 호출 시 create_portfolio=True로 설정되어 있으면 여기서 포트폴리오가 생성/업데이트됨
        if create_portfolio: # 이 조건은 run_strategy6의 파라미터에 의해 결정됨
            create_portfolio_strategy6(result_df, total_capital=total_capital, update_existing=update_existing)
        
    except Exception as e:
        import traceback
        print(f"❌ 전략 6 스크리닝 오류: {e}")
        print(traceback.format_exc())

# 최신 가격 데이터 가져오기 함수 (고가 포함)
def get_latest_price_data_high(symbol):
    """특정 종목의 최신 가격 데이터를 가져오는 함수 (고가 포함)
    
    Args:
        symbol: 종목 심볼
        
    Returns:
        tuple: (현재가, 당일 고가) 또는 데이터가 없는 경우 (None, None)
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
        
        return latest['close'], latest['high']
        
    except Exception as e:
        print(f"❌ {symbol} 가격 데이터 가져오기 오류: {e}")
        return None, None

# 전략 6 포트폴리오 생성 함수
def create_portfolio_strategy6(screened_stocks, total_capital=100000, update_existing=False):
    """전략 6 포트폴리오 생성 함수 (공매도 전략)
    
    Args:
        screened_stocks: 스크리닝된 종목 DataFrame
        total_capital: 총 자본금 (기본값: 10만 달러)
        update_existing: 기존 포트폴리오 업데이트 여부 (기본값: False)
    """
    try:
        # 포트폴리오 파일 경로
        portfolio_file = os.path.join(RESULTS_VER2_DIR, 'portfolio_strategy6.csv')
        
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
                current_price, high_price = get_latest_price_data_high(symbol)
                
                if current_price is None:
                    # 데이터를 가져올 수 없는 경우 기존 포지션 유지
                    portfolio.append(position.to_dict())
                    available_capital -= position['position_amount']
                    continue
                
                # 손절매 확인 (공매도이므로 가격이 상승하면 손실)
                stop_loss = position['stop_loss']
                
                if high_price >= stop_loss:
                    # 손절매 실행 - 포트폴리오에서 제외
                    print(f"🔴 손절매 실행: {symbol} (매도가: ${position['price']:.2f}, 손절매가: ${stop_loss:.2f})")
                    continue
                
                # 수익 확인 (공매도이므로 가격이 하락하면 수익)
                profit_pct = (position['price'] - current_price) / position['price'] * 100
                
                # 수익 목표 달성 확인 (5% 이상)
                if profit_pct >= 5.0:
                    print(f"🟢 수익 목표 달성: {symbol} (매도가: ${position['price']:.2f}, 현재가: ${current_price:.2f}, 수익률: {profit_pct:.2f}%)")
                    continue
                
                # 보유 기간 확인
                entry_date = pd.to_datetime(position['entry_date'])
                current_date = datetime.now()
                holding_days = (current_date - entry_date).days
                
                # 3일 이상 보유 시 청산
                if holding_days >= 3:
                    print(f"🟡 보유 기간 초과: {symbol} (매도가: ${position['price']:.2f}, 현재가: ${current_price:.2f}, 보유일: {holding_days}일)")
                    continue
                
                # 업데이트된 포지션 정보
                updated_position = position.to_dict()
                updated_position['current_price'] = current_price
                updated_position['current_value'] = current_price * position['shares']
                updated_position['profit_loss'] = (position['price'] - current_price) * position['shares']
                updated_position['profit_loss_pct'] = profit_pct
                updated_position['holding_days'] = holding_days
                
                portfolio.append(updated_position)
                available_capital -= updated_position['current_value']
        
        # 새로운 종목 추가
        for _, stock in screened_stocks.iterrows():
            # 이미 포트폴리오에 있는 종목 건너뛰기
            if stock['symbol'] in existing_symbols:
                continue
                
            # 가용 자본 확인
            if available_capital <= 0 or len(portfolio) >= 20:  # 최대 20개 포지션
                break
            
            # 포지션 계산
            entry_price = stock['entry_price']  # 전일 종가보다 5% 높은 가격
            stop_loss = stock['stop_loss']      # ATR의 3배 위 가격
            risk_amount = stop_loss - entry_price
            
            if risk_amount <= 0:
                continue
            
            # 위험 금액 계산 (총 자본의 2%)
            risk_capital = total_capital * 0.02
            
            # 주식 수량 계산 (공매도)
            shares = int(risk_capital / risk_amount)
            
            # 포지션 금액 계산
            position_amount = shares * entry_price
            
            # 최대 배분 금액 확인 (총 자본의 10%)
            max_amount = total_capital * 0.1
            if position_amount > max_amount:
                shares = int(max_amount / entry_price)
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
                'profit_target': stock['profit_target'],
                'entry_date': datetime.now().strftime('%Y-%m-%d'),
                'current_price': entry_price,
                'current_value': position_amount,
                'profit_loss': 0,
                'profit_loss_pct': 0,
                'holding_days': 0
            })
            
            available_capital -= position_amount
            print(f"🟢 새 종목 추가 (공매도): {stock['symbol']} (매도가: ${entry_price:.2f}, 수량: {shares}주)")
        
        # 포트폴리오 저장
        if portfolio:
            portfolio_df = pd.DataFrame(portfolio)
            portfolio_df.to_csv(portfolio_file, index=False, mode='w')
            
            # 포트폴리오 요약 출력
            total_value = portfolio_df['current_value'].sum()
            total_profit_loss = portfolio_df['profit_loss'].sum()
            
            print(f"\n💼 포트폴리오 생성 완료: {len(portfolio_df)}개 종목, 총 가치: ${total_value:.2f}")
            print(f"💰 총 수익/손실: ${total_profit_loss:.2f}")
            print(f"💵 남은 현금: ${available_capital:.2f}")
            
            # 포트폴리오 상세 출력
            print("\n📊 포트폴리오 상세:")
            summary_cols = ['symbol', 'price', 'current_price', 'shares', 'current_value', 'profit_loss_pct', 'stop_loss', 'holding_days']
            print(portfolio_df[summary_cols])
            
            return portfolio_df
        else:
            print("❌ 포트폴리오에 추가할 종목이 없습니다.")
            return None
            
    except Exception as e:
        print(f"❌ 포트폴리오 생성 오류: {e}")
        return None

# 포트폴리오 추적 및 업데이트 함수
def track_portfolio_strategy6(total_capital=100000):
    """전략 6 포트폴리오 추적 및 업데이트 함수
    
    Args:
        total_capital: 총 자본금 (기본값: 10만 달러)
    """
    print("\n🔍 전략 6 포트폴리오 추적 및 업데이트 시작...")
    
    # 스크리닝 실행 및 포트폴리오 업데이트
    # run_strategy6를 호출하여 스크리닝 결과를 받고, 그 결과를 바탕으로 포트폴리오를 업데이트합니다.
    # run_strategy6가 screened_df를 반환하도록 수정하거나, 내부에서 update_existing=True로 create_portfolio_strategy6를 호출하도록 해야 합니다.
    # 현재 run_strategy6는 screened_df를 반환하지 않고, create_portfolio=True일 때 내부적으로 create_portfolio_strategy6를 호출합니다.
    # 따라서 track_portfolio_strategy6는 run_strategy6를 update_existing=True로 호출하여 포트폴리오를 갱신하도록 합니다.
    print(f"🔄 전략 6 포트폴리오 업데이트를 위해 run_strategy6(update_existing=True) 호출...")
    run_strategy6(create_portfolio=True, total_capital=total_capital, update_existing=True)

# 메인 실행 함수
def run_screening():
    """모든 스크리닝 전략을 실행하는 메인 함수"""
    start_time = time.time()
    print("\n📊 투자 스크리너 Ver6 실행 중...")
    
    # 필요한 디렉토리 생성
    create_required_dirs()
    
    # 전략 6 실행 (단기 상승 종목 공매도 전략) - 기본적으로 포트폴리오 생성
    run_strategy6(create_portfolio=True, total_capital=100000, update_existing=False)
    
    # 포트폴리오 추적 및 업데이트 (선택 사항, 필요시 주석 해제)
    # track_portfolio_strategy6(total_capital=100000)

    # 실행 시간 출력
    elapsed_time = time.time() - start_time
    print(f"\n⏱️ 총 실행 시간: {elapsed_time:.2f}초")
    
    print("\n✅ 모든 스크리닝 완료!")

# 스크립트가 직접 실행될 때만 메인 함수 호출
if __name__ == "__main__":
    run_screening()