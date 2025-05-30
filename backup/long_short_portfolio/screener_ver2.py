# -*- coding: utf-8 -*-
# 투자 스크리너 Ver2 - 추가 스크리닝 전략 모듈

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

# 유틀리티 함수 임포트
from utils import (ensure_dir, load_csvs_parallel, extract_ticker_from_filename, 
                  calculate_atr, calculate_rsi, calculate_adx, calculate_historical_volatility,
                  check_sp500_condition, process_stock_data)

# 필요한 디렉토리 생성 함수
def create_required_dirs():
    """필요한 디렉토리를 생성하는 함수"""
    ensure_dir(RESULTS_VER2_DIR)
    # 매수/매도 결과 저장 디렉토리 생성
    buy_dir = os.path.join(RESULTS_VER2_DIR, 'buy')
    sell_dir = os.path.join(RESULTS_VER2_DIR, 'sell')
    ensure_dir(buy_dir)
    ensure_dir(sell_dir)

# 공통 데이터 처리 함수는 utils.py의 process_stock_data로 이동됨

# 첫 번째 전략: 미국 주식 스크리닝 및 포트폴리오 관리 (변동률 기준 정렬)
def run_strategy1(create_portfolio=True, total_capital=100000, update_existing=False):
    """첫 번째 전략 실행 함수 - 스크리닝 및 포트폴리오 관리
    
    조건:
    1. 최근 10일 평균 종가가 5달러 이상
    2. 직전 20일 기준 일 평균 거래 금액이 5000만 달러 초과
    3. SPY 종가가 100일 이동평균선 위에 있음
    4. 25일 단순 이동평균의 종가가 50일 이동평균의 종가보다 높음
    5. 200거래일 동안 가장 높은 변동률 순으로 정렬
    
    포트폴리오 관리:
    - 매수가: 해당 종목이 처음 추가된 날의 시가
    - 손절매: 매수가 기준 직전 20일 ATR의 5배 위 지점에 trailing stop loss
    - 수익보호: 매수가 기준 25%의 trailing stop loss
    - 포지션 크기: 포지션별 총자산 대비 2%의 위험비율, 10% 중 min 값
    - 최대 20개 포지션, 총자산 대비 최대 10%까지만 배분
    
    Args:
        create_portfolio: 스크리닝 후 포트폴리오 생성 여부 (기본값: True)
        total_capital: 총 자본금 (기본값: 10만 달러)
        update_existing: 기존 포트폴리오 업데이트 여부 (기본값: False)
    """
    print("\n🔍 전략 1: 미국 주식 스크리닝 시작...")
    
    # 결과 파일 경로 (매수 전략이므로 buy 폴더에 저장)
    buy_dir = os.path.join(RESULTS_VER2_DIR, 'buy')
    ensure_dir(buy_dir)
    result_file = os.path.join(buy_dir, 'strategy1_results.csv')
    
    try:
        # SPY 데이터 로드 및 조건 확인
        spy_condition = check_sp500_condition(DATA_US_DIR)
        if not spy_condition:
            print("❌ SPY 조건을 충족하지 않습니다. 스크리닝을 중단합니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=['symbol', 'entry_price', 'stop_loss', 'profit_protection', 'position_size_percentage']).to_csv(result_file, index=False, mode='w')
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
                
            # 데이터 처리
            symbol, df, recent_data = process_stock_data(file, DATA_US_DIR, min_days=200, recent_days=200)
            if symbol is None or df is None:
                continue
                
            # 최근 데이터 추출
            recent_data = df.iloc[-200:].copy()
            
            # 조건 1: 최근 10일 평균 종가가 5달러 이상
            recent_10d = recent_data.iloc[-10:]
            avg_price_10d = recent_10d['close'].mean()
            if avg_price_10d < 5.0:
                continue
            
            # 조건 2: 직전 20일 기준 일 평균 거래 금액이 5000만 달러 초과
            recent_20d = recent_data.iloc[-20:]
            avg_volume_value = (recent_20d['close'] * recent_20d['volume']).mean()
            if avg_volume_value <= 50000000:  # 5000만 달러
                continue
            
            # 조건 4: 25일 단순 이동평균의 종가가 50일 이동평균의 종가보다 높음
            recent_data['ma25'] = recent_data['close'].rolling(window=25).mean()
            recent_data['ma50'] = recent_data['close'].rolling(window=50).mean()
            
            # 최신 데이터의 이동평균 비교
            latest = recent_data.iloc[-1]
            if latest['ma25'] <= latest['ma50']:
                continue
            
            # 조건 5: 200거래일 동안의 변동률 계산
            # 변동성 계산 (60일 기준)
            volatility = calculate_historical_volatility(recent_data, window=60).iloc[-1] # 마지막 변동성 값 사용
            
            # ATR 계산 (직전 20일 기준)
            atr_20d = calculate_atr(recent_data.iloc[-20:], window=20).iloc[-1]  # Get the last ATR value
            
            # 매수가 (시가) 설정
            entry_price = recent_data.iloc[-1]['open']
            
            # 손절매: 매수가 기준 직전 20일 ATR의 5배 위 지점
            stop_loss = entry_price - (atr_20d * 5)
            
            # 수익보호: 매수가 기준 25%의 trailing stop loss
            profit_protection = entry_price * 0.75  # 매수가의 75% 지점 (25% 하락)
            
            # 포지션 크기: 포지션별 총자산 대비 2%의 위험비율, 10% 중 min 값
            risk_amount = entry_price - stop_loss
            if risk_amount <= 0:  # 위험 금액이 0 이하인 경우 처리
                position_size = 0.1  # 기본값 10%
            else:
                position_size_by_risk = 0.02 / (risk_amount / entry_price)  # 2% 위험 비율
                position_size = min(position_size_by_risk, 0.1)  # 10%와 비교하여 작은 값 선택
            
            # 모든 조건을 충족하는 종목 결과에 추가
            results.append({
                'symbol': symbol,
                'price': latest['close'],
                'avg_volume_value': avg_volume_value,
                'volatility': volatility,
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'profit_protection': profit_protection,
                'position_size': position_size
            })
        
        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=['symbol', 'entry_price', 'stop_loss', 'profit_protection', 'position_size_percentage']).to_csv(result_file, index=False)
            return
        
        # 결과 데이터프레임 생성
        result_df = pd.DataFrame(results)
        
        # 변동률 기준으로 내림차순 정렬
        result_df = result_df.sort_values('volatility', ascending=False)
        
        # 상위 20개 종목만 선택
        result_df = result_df.head(20)
        
        # 포지션 크기 백분율 계산 (총 자산 100,000 가정)
        # 각 포지션은 총 자산의 2% 위험을 가정하고, 최대 10% 배분을 목표로 함.
        # 20개 종목이므로, 각 종목당 평균 10% 배분 시 총 200%가 됨.
        # 여기서는 item['position_size']가 이미 자본 대비 비율(0.1 등)로 계산되어 있다고 가정하고, 이를 %로 변환.
        result_df['position_size_percentage'] = result_df['position_size'] * 100
        result_df['position_size_percentage'] = result_df['position_size_percentage'].round(2)

        # 결과 CSV에 포함할 컬럼 선택 및 이름 변경
        strategy_result_columns = ['symbol', 'entry_price', 'stop_loss', 'profit_protection', 'position_size_percentage']
        result_df_to_save = result_df[strategy_result_columns]

        # 결과 저장
        result_df_to_save.to_csv(result_file, index=False, mode='w')
        print(f"✅ 전략 1 스크리닝 결과 저장 완료: {len(result_df_to_save)}개 종목, 경로: {result_file}")
        
        # 상위 20개 종목 출력 (요청된 형식으로)
        print("\n🏆 전략 1 상위 20개 종목 (스크리닝 결과):")
        print(result_df_to_save.head(20))
        
        # 포트폴리오 생성 또는 업데이트
        if create_portfolio:
            portfolio_file = os.path.join(RESULTS_VER2_DIR, 'portfolio_strategy1.csv')
            ensure_dir(RESULTS_VER2_DIR) # Ensure directory exists

            target_columns = [
                '종목명', '매수일', '매수가', '수량', '투자금액', '현재가', '수익률(%)',
                '목표가', '손절가', '비중(%)'
            ]

            # 기존 포트폴리오 파일 로드 또는 새로 생성
            if update_existing and os.path.exists(portfolio_file):
                try:
                    portfolio_df = pd.read_csv(portfolio_file)
                    # If CSV is empty or has no columns, treat as new
                    if portfolio_df.empty and not any(col in portfolio_df.columns for col in target_columns):
                        portfolio_df = pd.DataFrame(columns=target_columns)
                except pd.errors.EmptyDataError: # Handle CSV that is empty (no data, possibly no headers)
                    portfolio_df = pd.DataFrame(columns=target_columns)
                except Exception as e:
                    print(f"⚠️ 기존 포트폴리오 로드 오류: {e}")
                    # Fallback to creating a new one if loading fails critically
                    portfolio_df = pd.DataFrame(columns=target_columns)
            else: # Not updating or file doesn't exist
                portfolio_df = pd.DataFrame(columns=target_columns)

            # 새 포트폴리오 데이터 생성
            new_portfolio_data = []
            items_for_portfolio = result_df.to_dict('records') # result_df is from screening, top 20

            # Calculate total_investment_value for the new items based on the plan's formula
            # This sum is based on the plan's interpretation of 'position_size' and 'entry_price'.
            total_investment_value_for_new_items = sum(item['position_size'] * item['entry_price'] for item in items_for_portfolio)

            for item in items_for_portfolio:
                # Plan's calculation for investment_amount and 수량
                # Note: item['position_size'] from result_df is an allocation factor (e.g., 0.1 for 10% capital),
                # not number of shares. Using it as '수량' (quantity) and in 'investment_amount' calculation
                # as per plan's literal description.
                investment_amount = item['position_size'] * item['entry_price'] 
                수량 = item['position_size'] 

                weight_percentage = 0
                if total_investment_value_for_new_items > 0: # Avoid division by zero
                    weight_percentage = (investment_amount / total_investment_value_for_new_items) * 100
                
                new_portfolio_data.append({
                    '종목명': item['symbol'],
                    '매수일': datetime.now().strftime('%Y-%m-%d'),
                    '매수가': item['entry_price'],
                    '수량': 수량,
                    '투자금액': investment_amount,
                    '현재가': item['entry_price'],  # 초기 현재가는 매수가와 동일
                    '수익률(%)': 0.0,
                    '목표가': item['entry_price'] * 1.2,  # 예시: 20% 수익 목표
                    '손절가': item['stop_loss'],
                    '비중(%)': round(weight_percentage, 2) # 비중 계산 및 추가
                })
            
            if new_portfolio_data:
                new_df = pd.DataFrame(new_portfolio_data)
                
                # Ensure existing portfolio_df conforms to target_columns before concatenation
                # This aligns schemas, adding missing columns with NA, and ensuring order.
                aligned_portfolio_df = pd.DataFrame(columns=target_columns)
                for col in target_columns:
                    if col in portfolio_df.columns:
                        aligned_portfolio_df[col] = portfolio_df[col]
                    else:
                        aligned_portfolio_df[col] = pd.NA # Use pd.NA for missing data
                
                portfolio_df = pd.concat([aligned_portfolio_df, new_df], ignore_index=True)
                portfolio_df.drop_duplicates(subset=['종목명'], keep='last', inplace=True)
            
            # 포트폴리오 저장 (even if empty, to reflect state)
            portfolio_df.to_csv(portfolio_file, index=False, encoding='utf-8-sig')
            print(f"\n💼 포트폴리오 저장 완료 ({len(portfolio_df)} 종목): {portfolio_file}")
            if not portfolio_df.empty:
                 print("\n📊 포트폴리오 요약 (상위 5개):")
                 print(portfolio_df[['종목명', '매수가', '수량', '투자금액', '비중(%)']].head())
        
    except Exception as e:
        import traceback
        print(f"❌ 전략 1 스크리닝 오류: {e}")
        print(traceback.format_exc())

# 전략 1 포트폴리오 생성 함수
def create_portfolio_strategy1(screened_stocks, total_capital=100000, update_existing=False):
    """전략 1 포트폴리오 생성 함수
    
    Args:
        screened_stocks: 스크리닝된 종목 DataFrame
        total_capital: 총 자본금 (기본값: 10만 달러)
        update_existing: 기존 포트폴리오 업데이트 여부 (기본값: False)
    """
    try:
        # 포트폴리오 파일 경로
        portfolio_file = os.path.join(RESULTS_VER2_DIR, 'portfolio_strategy1.csv')
        
        # 기존 포트폴리오 확인
        existing_portfolio = None
        if update_existing and os.path.exists(portfolio_file):
            try:
                existing_portfolio = pd.read_csv(portfolio_file)
                print(f"📊 기존 포트폴리오 로드: {len(existing_portfolio)}개 종목")
            except Exception as e:
                import traceback
                print(f"⚠️ 기존 포트폴리오 로드 오류: {e}")
                print(traceback.format_exc())
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
                current_price, low_price = get_latest_price_data(symbol)
                
                if current_price is None:
                    # 데이터를 가져올 수 없는 경우 기존 포지션 유지
                    portfolio.append(position.to_dict())
                    available_capital -= position['position_amount']
                    continue
                
                # 손절매 확인
                stop_loss = position['stop_loss']
                trailing_stop = position.get('trailing_stop_price', position['price'] * 0.75)
                
                if low_price <= stop_loss or low_price <= trailing_stop:
                    # 손절매 실행 - 포트폴리오에서 제외
                    print(f"🔴 손절매 실행: {symbol} (매수가: ${position['price']:.2f}, 손절매가: ${min(stop_loss, trailing_stop):.2f})")
                    continue
                
                # 트레일링 스탑 업데이트
                if current_price > position['price']:
                    new_trailing_stop = current_price * 0.75  # 25% 트레일링 스탑
                    if new_trailing_stop > trailing_stop:
                        trailing_stop = new_trailing_stop
                
                # 업데이트된 포지션 정보
                updated_position = position.to_dict()
                updated_position['current_price'] = current_price
                updated_position['current_value'] = current_price * position['shares']
                updated_position['profit_loss'] = (current_price - position['price']) * position['shares']
                updated_position['profit_loss_pct'] = (current_price / position['price'] - 1) * 100
                updated_position['trailing_stop_price'] = trailing_stop
                
                portfolio.append(updated_position)
                available_capital -= updated_position['current_value']
        
        # 새로운 종목 추가
        for _, stock in screened_stocks.iterrows():
            # 이미 포트폴리오에 있는 종목 건너뛰기
            if stock['symbol'] in existing_symbols:
                continue
                
            # 가용 자본 확인
            if available_capital <= 0 or len(portfolio) >= 20:
                break
            
            # 포지션 계산
            entry_price = stock['entry_price']
            stop_loss = stock['stop_loss']
            risk_amount = entry_price - stop_loss
            
            if risk_amount <= 0:
                continue
            
            # 위험 금액 계산 (총 자본의 2%)
            risk_capital = total_capital * 0.02
            
            # 주식 수량 계산
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
                'profit_protection': stock['profit_protection'],
                'trailing_stop_price': stock['profit_protection'],
                'entry_date': datetime.now().strftime('%Y-%m-%d'),
                'current_price': entry_price,
                'current_value': position_amount,
                'profit_loss': 0,
                'profit_loss_pct': 0
            })
            
            available_capital -= position_amount
            print(f"🟢 새 종목 추가: {stock['symbol']} (매수가: ${entry_price:.2f}, 수량: {shares}주)")
        
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
            summary_cols = ['symbol', 'price', 'current_price', 'shares', 'current_value', 'profit_loss_pct', 'stop_loss', 'trailing_stop_price']
            print(portfolio_df[summary_cols])
            
            return portfolio_df
        else:
            print("❌ 포트폴리오에 추가할 종목이 없습니다.")
            return None
            
    except Exception as e:
        print(f"❌ 포트폴리오 생성 오류: {e}")
        return None

# 최신 가격 데이터 가져오기 함수
def get_latest_price_data(symbol):
    """특정 종목의 최신 가격 데이터를 가져오는 함수
    
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

# 포트폴리오 추적 및 업데이트 함수
def track_portfolio_strategy1(total_capital=100000):
    """전략1 포트폴리오 추적 및 업데이트"""
    portfolio_file = os.path.join(RESULTS_VER2_DIR, 'portfolio_strategy1.csv')
    if not os.path.exists(portfolio_file):
        print(f"\n⚠️ {portfolio_file} 파일이 없어 추적할 포트폴리오가 없습니다.")
        return

    try:
        portfolio_df = pd.read_csv(portfolio_file)
        if portfolio_df.empty:
            print(f"\nℹ️ {portfolio_file} 파일이 비어있습니다. 추적할 내용이 없습니다.")
            return

        print(f"\n🔄 {portfolio_file} 포트폴리오 추적 중...")
        updated_rows = []
        total_current_value = 0

        for index, row in portfolio_df.iterrows():
            symbol = row['종목명']
            buy_price = row['매수가']
            quantity = row['수량']
            stop_loss_price = row['손절가']
            profit_target_price = row['목표가'] # 목표가 컬럼 사용

            # 현재가 가져오기 (실제로는 API 등을 통해 가져와야 함)
            # 여기서는 단순화를 위해 최근 데이터의 종가를 사용
            stock_file = os.path.join(DATA_US_DIR, f"{symbol}.csv")
            if not os.path.exists(stock_file):
                print(f"⚠️ {symbol} 데이터 파일을 찾을 수 없어 현재가를 업데이트할 수 없습니다.")
                current_price = row['현재가'] # 기존 현재가 유지
            else:
                stock_data = pd.read_csv(stock_file)
                stock_data.columns = [col.lower() for col in stock_data.columns]
                if not stock_data.empty and 'close' in stock_data.columns:
                    current_price = stock_data['close'].iloc[-1]
                else:
                    print(f"⚠️ {symbol} 데이터에 종가 정보가 없어 현재가를 업데이트할 수 없습니다.")
                    current_price = row['현재가'] # 기존 현재가 유지
            
            # 수익률 계산
            profit_loss_percent = ((current_price - buy_price) / buy_price) * 100 if buy_price > 0 else 0
            
            # 투자금액 (매수 시점 기준)
            investment_amount = buy_price * quantity
            total_current_value += current_price * quantity

            updated_row = row.copy()
            updated_row['현재가'] = round(current_price, 2)
            updated_row['수익률(%)'] = round(profit_loss_percent, 2)
            updated_row['투자금액'] = round(investment_amount, 2) # 투자금액 업데이트 (변동 없음)
            updated_rows.append(updated_row)

            # 매도 조건 확인 (손절 또는 목표가 도달)
            if current_price <= stop_loss_price:
                print(f"🔴 {symbol}: 손절매 조건 도달 (현재가: {current_price:.2f}, 손절가: {stop_loss_price:.2f})")
                # 실제 매도 로직 추가 필요 (예: 매도 기록, 포트폴리오에서 제거)
            elif current_price >= profit_target_price:
                print(f"🟢 {symbol}: 목표가 도달 (현재가: {current_price:.2f}, 목표가: {profit_target_price:.2f})")
                # 실제 매도 로직 추가 필요

        if updated_rows:
            updated_portfolio_df = pd.DataFrame(updated_rows)
            
            # 비중(%) 재계산
            current_total_investment = updated_portfolio_df['투자금액'].sum()
            if '비중(%)' not in updated_portfolio_df.columns:
                 updated_portfolio_df['비중(%)'] = 0.0 # 컬럼이 없으면 생성

            for i, r in updated_portfolio_df.iterrows():
                individual_investment = r['투자금액']
                weight = (individual_investment / current_total_investment) * 100 if current_total_investment > 0 else 0
                updated_portfolio_df.loc[i, '비중(%)'] = round(weight, 2)

            updated_portfolio_df.to_csv(portfolio_file, index=False, encoding='utf-8-sig')
            print(f"\n✅ 포트폴리오 업데이트 완료: {portfolio_file}")
            
            # 포트폴리오 요약 출력
            if not updated_portfolio_df.empty:
                print("\n📊 업데이트된 포트폴리오 요약 (상위 5개):")
                print(updated_portfolio_df[['종목명', '현재가', '수익률(%)', '투자금액', '비중(%)']].head())
                
                total_value = updated_portfolio_df['현재가'] * updated_portfolio_df['수량']
                total_portfolio_value = total_value.sum()
                initial_investment_total = portfolio_df['매수가'] * portfolio_df['수량'] # 초기 DF 사용
                total_profit_loss = total_portfolio_value - initial_investment_total.sum()
                remaining_cash = total_capital - total_portfolio_value # 총 자본에서 현재 포트폴리오 가치 차감

                print(f"\n💼 포트폴리오 총 가치: ${total_portfolio_value:,.2f}")
                print(f"💰 총 수익/손실: ${total_profit_loss:,.2f}")
                print(f"💵 남은 현금: ${remaining_cash:,.2f}")

    except FileNotFoundError:
        print(f"\n⚠️ {portfolio_file} 파일을 찾을 수 없습니다.")
    except pd.errors.EmptyDataError:
        print(f"\nℹ️ {portfolio_file} 파일이 비어있어 처리할 데이터가 없습니다.")
    except Exception as e:
        import traceback
        print(f"\n❌ 포트폴리오 추적 중 오류 발생: {e}")
        print(traceback.format_exc())

# 참고: SPY 조건 확인 함수는 utils.py의 check_sp500_condition으로 이동됨

# 참고: 변동률 계산 함수는 utils.py의 calculate_historical_volatility로 이동됨

# 참고: ATR(Average True Range) 계산 함수는 utils.py로 이동됨

# 참고: RSI(Relative Strength Index) 계산 함수는 utils.py로 이동됨

# 참고: ADX(Average Directional Index) 계산 함수는 utils.py로 이동됨

# 두 번째 전략: 과매수 종목 공매도 전략 (ADX 기준 정렬)
def run_strategy2(total_assets=100000, update_existing=False):
    """두 번째 전략 실행 함수 - 과매수 종목 공매도 전략
    
    조건:
    1. 최근 10일 평균 종가가 5달러 이상
    2. 최근 20일간 거래대금이 2500만 달러 이상
    3. 지난 10일 동안의 ATR은 주식 종가의 13% 이상
    4. 3일 RSI는 90 이상
    5. 최근 2일간 종가는 직전일 종가보다 높아야 함
    6. 7일 ADX 기준으로 정렬
    
    포트폴리오 관리:
    - 매도가: 전일 종가 대비 4% 높은 가격에 지정가 매도
    - 손절매: 매도가 기준 직전 10일 ATR의 3배 위 지점에 stop loss
    - 수익실현: 매도가 대비 4% 하락 시 또는 2일 경과 후 청산
    - 포지션 크기: 포지션별 총자산 대비 2%의 위험비율, 최대 10개 포지션
    - 총자산 대비 최대 10%까지만 배분
    
    Args:
        total_assets: 총 자산 (기본값: 100000)
        update_existing: 기존 포트폴리오 업데이트 여부 (기본값: False)
    """
    print("\n🔍 전략 2: 과매수 종목 공매도 전략 스크리닝 시작...")
    
    # 결과 파일 경로 (매도 전략이므로 sell 폴더에 저장)
    sell_dir = os.path.join(RESULTS_VER2_DIR, 'sell')
    ensure_dir(sell_dir)
    result_file = os.path.join(sell_dir, 'strategy2_results.csv')
    
    try:
        # S&P 500 조건 확인
        sp500_condition = check_sp500_condition(DATA_US_DIR)
        if not sp500_condition:
            print("❌ S&P 500 조건을 충족하지 않습니다. 스크리닝을 중단합니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=['symbol', 'entry_price', 'stop_loss', 'profit_protection', 'position_size_percentage']).to_csv(result_file, index=False)
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
                
            # 데이터 처리
            symbol, df, recent_data = process_stock_data(file, DATA_US_DIR, min_days=20, recent_days=20)
            if symbol is None or df is None or recent_data is None:
                continue
                
            # 조건 1: 최근 10일 평균 종가가 5달러 이상
            recent_10d = recent_data.iloc[-10:]
            avg_price_10d = recent_10d['close'].mean()
            if avg_price_10d < 5.0:
                continue
            
            # 조건 2: 최근 20일간 거래대금이 2500만 달러 이상
            avg_volume_value = (recent_data['close'] * recent_data['volume']).mean()
            if avg_volume_value <= 25000000:  # 2500만 달러
                continue
            
            # 조건 3: 지난 10일 동안의 ATR은 주식 종가의 13% 이상
            atr_10d_series = calculate_atr(recent_data, window=10)
            if atr_10d_series.empty or pd.isna(atr_10d_series.iloc[-1]):
                continue
            atr_10d = atr_10d_series.iloc[-1]
            
            latest_close = recent_data.iloc[-1]['close']
            if latest_close == 0: # Avoid division by zero
                continue
            atr_percentage = (atr_10d / latest_close) * 100
            if atr_percentage < 13.0:
                continue
            
            # 조건 4: 3일 RSI는 90 이상
            rsi_3d_series = calculate_rsi(recent_data, window=3)
            if rsi_3d_series.empty or pd.isna(rsi_3d_series.iloc[-1]):
                continue
            rsi_3d = rsi_3d_series.iloc[-1]
            if rsi_3d < 90.0:
                continue
            
            # 조건 5: 최근 2일간 종가는 직전일 종가보다 높아야 함
            if len(recent_data) < 3:  # 최소 3일 데이터 필요 (오늘, 어제, 그제)
                continue
                
            today_close = recent_data.iloc[-1]['close']
            yesterday_close = recent_data.iloc[-2]['close']
            day_before_yesterday_close = recent_data.iloc[-3]['close']
            
            if not (today_close > yesterday_close and yesterday_close > day_before_yesterday_close):
                continue
            
            # 조건 6: 7일 ADX 계산
            adx_7d_series = calculate_adx(recent_data, window=7)
            if adx_7d_series.empty or pd.isna(adx_7d_series.iloc[-1]):
                continue # ADX is crucial for sorting, skip if not available
            adx_7d = adx_7d_series.iloc[-1]
            
            # 매도가 설정 (전일 종가 대비 4% 높은 가격)
            entry_price = today_close * 1.04
            
            # 손절매: 매도가 기준 직전 10일 ATR의 3배 위 지점
            stop_loss = entry_price + (atr_10d * 3)
            
            # 수익실현: 매도가 대비 4% 하락 시
            profit_target = entry_price * 0.96
            
            # 포지션 크기: 포지션별 총자산 대비 2%의 위험비율, 최대 10개 포지션
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
                'atr_percentage': atr_percentage,
                'rsi_3d': rsi_3d,
                'adx_7d': adx_7d,
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'profit_target': profit_target,
                'position_size': position_size
            })
        
        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=['symbol', 'entry_price', 'stop_loss', 'profit_protection', 'position_size_percentage']).to_csv(result_file, index=False)
            return
        
        # 결과 데이터프레임 생성
        result_df = pd.DataFrame(results)
        
        # ADX 기준으로 내림차순 정렬
        result_df = result_df.sort_values('adx_7d', ascending=False)
        
        # 상위 20개 종목만 선택 (예비 포함)
        result_df = result_df.head(20)
        
        # 포지션 크기 백분율 계산
        result_df['position_size_percentage'] = result_df['position_size'] * 100
        result_df['position_size_percentage'] = result_df['position_size_percentage'].round(2)

        # 결과 CSV에 포함할 컬럼 선택 및 이름 변경
        strategy_result_columns = ['symbol', 'entry_price', 'stop_loss', 'profit_target', 'position_size_percentage']
        result_df_to_save = result_df[strategy_result_columns].rename(columns={'profit_target': 'profit_protection'})

        # 결과 저장
        result_df_to_save.to_csv(result_file, index=False)
        print(f"✅ 전략 2 스크리닝 결과 저장 완료: {len(result_df_to_save)}개 종목, 경로: {result_file}")
        
        # 상위 20개 종목 출력 (요청된 형식으로)
        print("\n🏆 전략 2 상위 20개 매도 대상 종목 (스크리닝 결과):")
        print(result_df_to_save.head(20))
        
        # 포트폴리오 생성
        create_portfolio_strategy2(result_df, total_capital=total_assets, update_existing=update_existing)
        
    except Exception as e:
        print(f"❌ 전략 2 스크리닝 오류: {e}")
        print(traceback.format_exc())
        print(traceback.format_exc())

# 전략 2 포트폴리오 생성 함수
def create_portfolio_strategy2(screened_stocks, total_capital=100000, update_existing=False):
    """전략 2 포트폴리오 생성 함수 (공매도 전략)
    
    Args:
        screened_stocks: 스크리닝된 종목 DataFrame
        total_capital: 총 자본금 (기본값: 10만 달러)
        update_existing: 기존 포트폴리오 업데이트 여부 (기본값: False)
    """
    try:
        # 포트폴리오 파일 경로
        portfolio_file = os.path.join(RESULTS_VER2_DIR, 'portfolio_strategy2.csv')
        
        # 기존 포트폴리오 확인
        existing_portfolio = None
        if update_existing and os.path.exists(portfolio_file):
            try:
                existing_portfolio = pd.read_csv(portfolio_file)
                print(f"📊 기존 포트폴리오 로드: {len(existing_portfolio)}개 종목")
            except Exception as e:
                import traceback
                print(f"⚠️ 기존 포트폴리오 로드 오류: {e}")
                print(traceback.format_exc())
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
                
                # 수익 목표 달성 확인 (4% 이상)
                if profit_pct >= 4.0:
                    print(f"🟢 수익 목표 달성: {symbol} (매도가: ${position['price']:.2f}, 현재가: ${current_price:.2f}, 수익률: {profit_pct:.2f}%)")
                    continue
                
                # 보유 기간 확인
                entry_date = pd.to_datetime(position['entry_date'])
                current_date = datetime.now()
                holding_days = (current_date - entry_date).days
                
                # 2일 이상 보유 시 청산
                if holding_days >= 2:
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
            if available_capital <= 0 or len(portfolio) >= 10:  # 최대 10개 포지션
                break
            
            # 포지션 계산
            entry_price = stock['entry_price']  # 전일 종가보다 4% 높은 가격
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

# 포트폴리오 추적 및 업데이트 함수
def track_portfolio_strategy2(total_capital=100000):
    """전략 2 포트폴리오 추적 및 업데이트 함수
    
    Args:
        total_capital: 총 자본금 (기본값: 10만 달러)
    """
    print("\n🔍 전략 2 포트폴리오 추적 및 업데이트 시작...")
    
    # 스크리닝 실행 및 포트폴리오 업데이트
    run_strategy2(total_assets=total_capital, update_existing=True)

# 참고: SPY 조건 확인 함수는 utils.py의 check_sp500_condition으로 이동됨

# 참고: 변동률 계산 함수는 utils.py의 calculate_historical_volatility로 이동됨

# 참고: ATR(Average True Range) 계산 함수는 utils.py로 이동됨

# 참고: RSI(Relative Strength Index) 계산 함수는 utils.py로 이동됨

# 참고: ADX(Average Directional Index) 계산 함수는 utils.py로 이동됨

# 두 번째 전략: 과매수 종목 공매도 전략 (ADX 기준 정렬)
def run_strategy2(total_assets=100000, update_existing=False):
    """두 번째 전략 실행 함수 - 과매수 종목 공매도 전략
    
    조건:
    1. 최근 10일 평균 종가가 5달러 이상
    2. 최근 20일간 거래대금이 2500만 달러 이상
    3. 지난 10일 동안의 ATR은 주식 종가의 13% 이상
    4. 3일 RSI는 90 이상
    5. 최근 2일간 종가는 직전일 종가보다 높아야 함
    6. 7일 ADX 기준으로 정렬
    
    포트폴리오 관리:
    - 매도가: 전일 종가 대비 4% 높은 가격에 지정가 매도
    - 손절매: 매도가 기준 직전 10일 ATR의 3배 위 지점에 stop loss
    - 수익실현: 매도가 대비 4% 하락 시 또는 2일 경과 후 청산
    - 포지션 크기: 포지션별 총자산 대비 2%의 위험비율, 최대 10개 포지션
    - 총자산 대비 최대 10%까지만 배분
    
    Args:
        total_assets: 총 자산 (기본값: 100000)
        update_existing: 기존 포트폴리오 업데이트 여부 (기본값: False)
    """
    print("\n🔍 전략 2: 과매수 종목 공매도 전략 스크리닝 시작...")
    
    # 결과 파일 경로 (매도 전략이므로 sell 폴더에 저장)
    sell_dir = os.path.join(RESULTS_VER2_DIR, 'sell')
    ensure_dir(sell_dir)
    result_file = os.path.join(sell_dir, 'strategy2_results.csv')
    
    try:
        # S&P 500 조건 확인
        sp500_condition = check_sp500_condition(DATA_US_DIR)
        if not sp500_condition:
            print("❌ S&P 500 조건을 충족하지 않습니다. 스크리닝을 중단합니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=['symbol', 'entry_price', 'stop_loss', 'profit_protection', 'position_size_percentage']).to_csv(result_file, index=False)
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
                
            # 데이터 처리
            symbol, df, recent_data = process_stock_data(file, DATA_US_DIR, min_days=20, recent_days=20)
            if symbol is None or df is None or recent_data is None:
                continue
                
            # 조건 1: 최근 10일 평균 종가가 5달러 이상
            recent_10d = recent_data.iloc[-10:]
            avg_price_10d = recent_10d['close'].mean()
            if avg_price_10d < 5.0:
                continue
            
            # 조건 2: 최근 20일간 거래대금이 2500만 달러 이상
            avg_volume_value = (recent_data['close'] * recent_data['volume']).mean()
            if avg_volume_value <= 25000000:  # 2500만 달러
                continue
            
            # 조건 3: 지난 10일 동안의 ATR은 주식 종가의 13% 이상
            atr_10d_series = calculate_atr(recent_data, window=10)
            if atr_10d_series.empty or pd.isna(atr_10d_series.iloc[-1]):
                continue
            atr_10d = atr_10d_series.iloc[-1]
            
            latest_close = recent_data.iloc[-1]['close']
            if latest_close == 0: # Avoid division by zero
                continue
            atr_percentage = (atr_10d / latest_close) * 100
            if atr_percentage < 13.0:
                continue
            
            # 조건 4: 3일 RSI는 90 이상
            rsi_3d_series = calculate_rsi(recent_data, window=3)
            if rsi_3d_series.empty or pd.isna(rsi_3d_series.iloc[-1]):
                continue
            rsi_3d = rsi_3d_series.iloc[-1]
            if rsi_3d < 90.0:
                continue
            
            # 조건 5: 최근 2일간 종가는 직전일 종가보다 높아야 함
            if len(recent_data) < 3:  # 최소 3일 데이터 필요 (오늘, 어제, 그제)
                continue
                
            today_close = recent_data.iloc[-1]['close']
            yesterday_close = recent_data.iloc[-2]['close']
            day_before_yesterday_close = recent_data.iloc[-3]['close']
            
            if not (today_close > yesterday_close and yesterday_close > day_before_yesterday_close):
                continue
            
            # 조건 6: 7일 ADX 계산
            adx_7d_series = calculate_adx(recent_data, window=7)
            if adx_7d_series.empty or pd.isna(adx_7d_series.iloc[-1]):
                continue # ADX is crucial for sorting, skip if not available
            adx_7d = adx_7d_series.iloc[-1]
            
            # 매도가 설정 (전일 종가 대비 4% 높은 가격)
            entry_price = today_close * 1.04
            
            # 손절매: 매도가 기준 직전 10일 ATR의 3배 위 지점
            stop_loss = entry_price + (atr_10d * 3)
            
            # 수익실현: 매도가 대비 4% 하락 시
            profit_target = entry_price * 0.96
            
            # 포지션 크기: 포지션별 총자산 대비 2%의 위험비율, 최대 10개 포지션
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
                'atr_percentage': atr_percentage,
                'rsi_3d': rsi_3d,
                'adx_7d': adx_7d,
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'profit_target': profit_target,
                'position_size': position_size
            })
        
        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=['symbol', 'entry_price', 'stop_loss', 'profit_protection', 'position_size_percentage']).to_csv(result_file, index=False)
            return
        
        # 결과 데이터프레임 생성
        result_df = pd.DataFrame(results)
        
        # ADX 기준으로 내림차순 정렬
        result_df = result_df.sort_values('adx_7d', ascending=False)
        
        # 상위 20개 종목만 선택 (예비 포함)
        result_df = result_df.head(20)
        
        # 포지션 크기 백분율 계산
        result_df['position_size_percentage'] = result_df['position_size'] * 100
        result_df['position_size_percentage'] = result_df['position_size_percentage'].round(2)

        # 결과 CSV에 포함할 컬럼 선택 및 이름 변경
        strategy_result_columns = ['symbol', 'entry_price', 'stop_loss', 'profit_target', 'position_size_percentage']
        result_df_to_save = result_df[strategy_result_columns].rename(columns={'profit_target': 'profit_protection'})

        # 결과 저장
        result_df_to_save.to_csv(result_file, index=False)
        print(f"✅ 전략 2 스크리닝 결과 저장 완료: {len(result_df_to_save)}개 종목, 경로: {result_file}")
        
        # 상위 20개 종목 출력 (요청된 형식으로)
        print("\n🏆 전략 2 상위 20개 매도 대상 종목 (스크리닝 결과):")
        print(result_df_to_save.head(20))
        
        # 포트폴리오 생성
        create_portfolio_strategy2(result_df, total_capital=total_assets, update_existing=update_existing)
        
    except Exception as e:
        print(f"❌ 전략 2 스크리닝 오류: {e}")
        print(traceback.format_exc())
        print(traceback.format_exc())