# -*- coding: utf-8 -*-
# 전략 1: 트렌드 하이 모멘텀 롱 (Long Trend High Momentum)

import os
import traceback
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from utils.path_utils import add_project_root

# 프로젝트 루트 디렉토리를 Python 경로에 추가
add_project_root()

# 설정 파일 임포트
from config import (
    DATA_DIR, DATA_US_DIR,
    RESULTS_DIR, PORTFOLIO_RESULTS_DIR
)

# 유틸리티 함수 임포트
from utils import (
    ensure_dir, extract_ticker_from_filename, 
    calculate_atr, calculate_historical_volatility,
    check_sp500_condition, process_stock_data
)


def run_strategy1_screening(update_existing=True):
    """
    전략 1: 트렌드 하이 모멘텀 롱 스크리닝
    
    Args:
        update_existing: 기존 포트폴리오 업데이트 여부 (기본값: True)
    """
    print("\n🔍 전략 1: 트렌드 하이 모멘텀 롱 스크리닝 시작...")
    
    # 결과 파일 경로 - buy 폴더로 변경
    buy_dir = os.path.join(PORTFOLIO_RESULTS_DIR, 'buy')
    ensure_dir(buy_dir)
    result_file = os.path.join(buy_dir, 'strategy1_results.csv')
    
    try:
        # SPY 데이터 로드 및 조건 확인
        spy_condition = check_sp500_condition(DATA_US_DIR)
        if not spy_condition:
            print("❌ SPY 조건을 충족하지 않습니다. 스크리닝을 중단합니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']).to_csv(result_file, index=False, mode='w', encoding='utf-8-sig')
            # JSON 파일 생성 추가
            json_file = result_file.replace('.csv', '.json')
            pd.DataFrame(columns=['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']).to_json(json_file, orient='records', indent=2, force_ascii=False)
            
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
            
            # 조건 3: 25일 이동평균 > 50일 이동평균
            recent_data['ma25'] = recent_data['close'].rolling(window=25).mean()
            recent_data['ma50'] = recent_data['close'].rolling(window=50).mean()
            latest = recent_data.iloc[-1]
            if latest['ma25'] <= latest['ma50']:
                continue
            
            # 변동성 계산 (200일 기준)
            volatility = calculate_historical_volatility(recent_data, window=200).iloc[-1]
            
            # 200일 상승률 계산
            price_change_200d = ((recent_data['close'].iloc[-1] - recent_data['close'].iloc[0]) / recent_data['close'].iloc[0]) * 100
            
            # ATR 계산 (직전 20일 기준)
            atr_20d = calculate_atr(recent_data.iloc[-20:], window=20).iloc[-1]
            
            # 매수가 (시가) 설정
            entry_price = recent_data.iloc[-1]['open']
            
            # 손절매: 매수가 기준 직전 20일 ATR의 5배 위 지점
            stop_loss = entry_price - (atr_20d * 5)
            
            # 수익보호: 매수가 기준 25%의 trailing stop loss
            profit_protection_trailing_stop = entry_price * 0.75  # 매수가의 75% 지점 (25% 하락)
            
            # 포지션 크기: 포지션별 총자산 대비 2%의 위험비율, 10% 중 min 값
            risk_amount = entry_price - stop_loss
            if risk_amount <= 0:  # 위험 금액이 0 이하인 경우 처리
                position_size = 0  # 0%
            else:
                position_size_by_risk = 0.02 / (risk_amount / entry_price)  # 2% 위험 비율
                position_size = min(position_size_by_risk, 0.1)  # 10%와 비교하여 작은 값 선택
            
            # 모든 조건을 충족하는 종목 결과에 추가
            results.append({
                '종목명': symbol,
                '매수일': datetime.now().strftime('%Y-%m-%d'),
                '매수가': '시장가',  # 시장가 매수, 추후 다음날 시가로 업데이트
                '비중(%)': round(position_size * 100, 2), # % 기호 없이 숫자만 저장
                '수익률': 0.0, # 초기 수익률
                '차익실현': '없음',  # 목표 수익 없음
                '손절매': round(stop_loss, 2), # 계산된 손절매 가격
                '수익보호': round(profit_protection_trailing_stop, 2),  # 25% 트레일링 스톱 가격
                '롱여부': True,
                'volatility': volatility,  # 정렬용
                'price_change_200d': price_change_200d  # 정렬용
            })
        
        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            # 빈 결과 파일 생성
            pd.DataFrame(columns=['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']).to_csv(result_file, index=False, encoding='utf-8-sig')
            return
        
        # 기존 포트폴리오 로드 (있는 경우)
        existing_portfolio = pd.DataFrame()
        if os.path.exists(result_file) and update_existing:
            try:
                existing_portfolio = pd.read_csv(result_file, encoding='utf-8-sig')
                print(f"📂 기존 포트폴리오 로드: {len(existing_portfolio)}개 종목")
            except Exception as e:
                print(f"⚠️ 기존 포트폴리오 로드 실패: {e}")
                existing_portfolio = pd.DataFrame()
        
        # 결과 데이터프레임 생성
        result_df = pd.DataFrame(results)
        
        # 변동성 기준으로 내림차순 정렬 후, 같은 변동성은 200일 상승률 기준으로 정렬
        result_df = result_df.sort_values(['volatility', 'price_change_200d'], ascending=[False, False])
        
        # 기존 포트폴리오와 새로운 후보 종목 통합
        final_portfolio = pd.DataFrame()
        
        if not existing_portfolio.empty:
            # 기존 종목들을 우선 유지 (매수일 업데이트 안함)
            existing_tickers = set(existing_portfolio['종목명'].tolist())
            final_portfolio = existing_portfolio.copy()
            
            # 기존 종목 중에서 새로운 스크리닝 결과에도 있는 종목들의 가격 정보만 업데이트
            new_tickers_dict = {row['종목명']: row for _, row in result_df.iterrows()}
            
            for idx, row in final_portfolio.iterrows():
                ticker = row['종목명']
                if ticker in new_tickers_dict:
                    # 손절매, 수익보호 가격만 업데이트 (매수일은 유지)
                    final_portfolio.at[idx, '손절매'] = new_tickers_dict[ticker]['손절매']
                    final_portfolio.at[idx, '수익보호'] = new_tickers_dict[ticker]['수익보호']
            
            # 10개 미만인 경우 새로운 종목 추가 (조건에 맞는 종목이 있을 때만)
            current_count = len(final_portfolio)
            if current_count < 10:
                needed_count = 10 - current_count
                # 기존에 없는 새로운 종목들만 선택
                new_candidates = result_df[~result_df['종목명'].isin(existing_tickers)]
                
                if not new_candidates.empty:
                    new_additions = new_candidates.head(needed_count)
                    strategy_result_columns = ['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']
                    new_additions_to_add = new_additions[strategy_result_columns]
                    final_portfolio = pd.concat([final_portfolio, new_additions_to_add], ignore_index=True)
                    print(f"➕ 새로운 종목 {len(new_additions_to_add)}개 추가")
                else:
                    print(f"⚠️ 조건에 맞는 새로운 종목이 없어 {current_count}개로 유지합니다.")
        else:
            # 기존 포트폴리오가 없는 경우 상위 10개 선택
            result_df = result_df.head(10)
            strategy_result_columns = ['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']
            final_portfolio = result_df[strategy_result_columns]
        
        # 결과 저장
        final_portfolio.to_csv(result_file, index=False, encoding='utf-8-sig')
        
        # JSON 파일도 저장
        json_file = result_file.replace('.csv', '.json')
        final_portfolio.to_json(json_file, orient='records', force_ascii=False, indent=2)
        
        print(f"✅ 전략 1 스크리닝 결과 저장 완료: {len(final_portfolio)}개 종목, 경로: {result_file}")
        
        # 상위 종목 출력
        print("\n🏆 전략 1 상위 종목 (스크리닝 결과):")
        print(final_portfolio)
        
        # 시장가 업데이트 실행
        update_market_prices(result_file)
        
        
    except Exception as e:
        print(f"❌ 전략 1 스크리닝 오류: {e}")
        print(traceback.format_exc())


def update_market_prices(result_file):
    """
    '시장가'로 표시된 종목들의 매수가를 다음날 시가로 업데이트
    
    Args:
        result_file: 결과 파일 경로
    """
    try:
        if not os.path.exists(result_file):
            print("❌ 결과 파일이 존재하지 않습니다.")
            return
            
        # 포트폴리오 로드
        portfolio = pd.read_csv(result_file, encoding='utf-8-sig')
        
        # '시장가'로 표시된 종목들 찾기
        market_price_stocks = portfolio[portfolio['매수가'] == '시장가']
        
        if market_price_stocks.empty:
            print("📊 시장가 업데이트가 필요한 종목이 없습니다.")
            return
            
        print(f"💰 {len(market_price_stocks)}개 종목의 시장가를 업데이트합니다...")
        
        updated_count = 0
        for idx, row in market_price_stocks.iterrows():
            ticker = row['종목명']
            buy_date = pd.to_datetime(row['매수일'], utc=True)
            
            # 다음 거래일 계산 (매수일 + 1일)
            next_trading_day = buy_date + timedelta(days=1)
            
            # 주말인 경우 월요일로 조정
            while next_trading_day.weekday() >= 5:  # 5=토요일, 6=일요일
                next_trading_day += timedelta(days=1)
            
            # 해당 종목의 데이터 파일 찾기
            ticker_file = os.path.join(DATA_US_DIR, f"{ticker}.csv")
            
            if not os.path.exists(ticker_file):
                print(f"⚠️ {ticker} 데이터 파일을 찾을 수 없습니다.")
                continue
                
            try:
                # 종목 데이터 로드
                df = pd.read_csv(ticker_file)
                df['date'] = pd.to_datetime(df['date'], utc=True)
                
                # 다음 거래일의 시가 찾기
                next_day_data = df[df['date'] == next_trading_day.date()]
                
                if not next_day_data.empty:
                    open_price = next_day_data.iloc[0]['Open']  # 대문자 Open 사용
                    portfolio.at[idx, '매수가'] = round(open_price, 2)
                    updated_count += 1
                    print(f"✅ {ticker}: 시장가 → ${open_price:.2f}")
                else:
                    # 다음 거래일 데이터가 없는 경우 가장 최근 시가 사용
                    latest_data = df.iloc[-1]
                    open_price = latest_data['Open']  # 대문자 Open 사용
                    portfolio.at[idx, '매수가'] = round(open_price, 2)
                    updated_count += 1
                    print(f"✅ {ticker}: 시장가 → ${open_price:.2f} (최근 시가 사용)")
                    
            except Exception as e:
                print(f"⚠️ {ticker} 가격 업데이트 실패: {e}")
                continue
        
        if updated_count > 0:
            # 업데이트된 포트폴리오 저장
            portfolio.to_csv(result_file, index=False, encoding='utf-8-sig')
            
            # JSON 파일도 업데이트
            json_file = result_file.replace('.csv', '.json')
            portfolio.to_json(json_file, orient='records', force_ascii=False, indent=2)
            
            print(f"💾 {updated_count}개 종목의 매수가가 업데이트되었습니다.")
        else:
            print("❌ 업데이트된 종목이 없습니다.")
            
    except Exception as e:
        print(f"❌ 시장가 업데이트 오류: {e}")
        print(traceback.format_exc())


# 메인 실행 부분


def run_strategy():
    """Wrapper function for main.py compatibility"""
    return run_strategy1_screening(update_existing=True)


if __name__ == "__main__":
    # 직접 실행 시 테스트
    run_strategy1_screening()
