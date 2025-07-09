# -*- coding: utf-8 -*-
# 전략 5: 평균회귀 하이 ADX 리버설 롱 (Mean Reversion High ADX Reversal Long)

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
    calculate_atr, calculate_rsi, calculate_adx,
    check_sp500_condition, process_stock_data
)


def run_strategy5_screening():
    print("\n🔍 전략 5: 평균회귀 하이 ADX 리버설 롱 스크리닝 시작...")

    # 결과 파일 경로 - buy 폴더로 변경
    buy_dir = os.path.join(PORTFOLIO_RESULTS_DIR, 'buy')
    ensure_dir(buy_dir)
    result_file = os.path.join(buy_dir, 'strategy5_results.csv')

    try:
        # S&P500 조건은 명시되어 있지 않으므로, 개별 종목 조건만 확인
        # check_sp500_condition 함수는 사용하지 않음

        us_files = [f for f in os.listdir(DATA_US_DIR) if f.endswith('.csv')]
        if not us_files:
            print("❌ 미국 주식 데이터 파일이 없습니다.")
            return

        print(f"📊 {len(us_files)}개 미국 주식 파일 처리 중...")

        results = []
        for i, file in enumerate(us_files):
            if i % 100 == 0 and i > 0:
                print(f"⏳ 진행 중: {i}/{len(us_files)} 종목 처리됨")

            # 100일 MA, 50일 거래량/금액, 10일 ATR, 7일 ADX, 3일 RSI 필요
            symbol, df, recent_data = process_stock_data(file, DATA_US_DIR, min_days=100, recent_days=100)
            if symbol is None or df is None or recent_data is None or len(recent_data) < 50:
                continue

            latest_close = recent_data.iloc[-1]['close']
            if latest_close == 0: continue

            # 필터 1: 50일 기준 평균 거래량 ≥ 50만 주
            avg_volume_50d = recent_data.iloc[-50:]['volume'].mean()
            if avg_volume_50d < 500000:
                continue

            # 필터 2: 50일 기준 평균 거래금액 ≥ 250만 달러
            avg_value_50d = (recent_data.iloc[-50:]['close'] * recent_data.iloc[-50:]['volume']).mean()
            if avg_value_50d < 2500000:  # 250만 달러
                continue

            # 필터 3: ATR ≥ 4 (최근 10일 ATR 기준)
            atr_10d_series = calculate_atr(recent_data.iloc[-20:], window=10)
            if atr_10d_series.empty or pd.isna(atr_10d_series.iloc[-1]) or atr_10d_series.iloc[-1] < 4:
                continue
            atr_10d = atr_10d_series.iloc[-1]

            # 설정 조건 1: 종가 > 100일 이동평균, 최근 10일 1ATR보다 높은 것
            # (해석: 종가가 (100일 MA + 10일 ATR) 보다 높아야 함)
            ma_100d = recent_data['close'].rolling(window=100).mean().iloc[-1]
            if pd.isna(ma_100d) or latest_close <= (ma_100d + atr_10d):
                continue

            # 설정 조건 2: 7일 ADX ≥ 55
            # ADX 계산을 위해 high, low, close 데이터 필요
            # ADX 계산 (7일)
            adx_7d = pd.NA  # Initialize adx_7d
            if len(recent_data) >= 20:  # ADX 계산에 충분한 데이터가 있는지 확인 (일반적으로 ADX는 최소 14일 필요, 여유있게 20일)
                adx_7d_df = calculate_adx(recent_data, window=7)
                if 'adx' not in adx_7d_df.columns or adx_7d_df['adx'].empty:
                    continue
                adx_7d = adx_7d_df['adx'].iloc[-1]
            else:
                continue
            # logger.debug(f"{ticker}: 7-day ADX: {adx_7d}")
            if pd.isna(adx_7d) or adx_7d < 55:
                continue

            # 설정 조건 3: 3일 RSI ≤ 50
            rsi_3d_df = calculate_rsi(recent_data[['close']], window=3)
            if 'rsi_3' not in rsi_3d_df.columns or pd.isna(rsi_3d_df['rsi_3'].iloc[-1]) or rsi_3d_df['rsi_3'].iloc[-1] > 50:
                continue
            rsi_3d = rsi_3d_df['rsi_3'].iloc[-1]

            # 시장 진입: 직전 종가보다 최대 3% 낮은 가격에 지정가 매수
            entry_price = latest_close * 0.97

            # 손절매: 체결가 기준 10일 ATR의 3배 아래
            stop_loss_price = entry_price - (atr_10d * 3)

            # 수익보호: 없음 (마크다운 문서에 따라)
            profit_protection = '없음'
            
            # 포지션 크기 계산 (2% 리스크 기준, 총 자산의 10% 제한)
            total_capital = 100000  # 10만 달러 기준
            risk_per_share = entry_price - stop_loss_price
            if risk_per_share <= 0:
                position_allocation = 0 # 기본값
            else:
                risk_ratio_per_share = risk_per_share / entry_price
                position_allocation_by_risk = 0.02 / risk_ratio_per_share # 총 자산의 2% 리스크
                position_allocation = min(position_allocation_by_risk, 0.1) # 최대 10% 배분

            results.append({
                '종목명': symbol,
                '매수일': datetime.now().strftime('%Y-%m-%d'),
                '매수가': round(entry_price, 2),
                '비중(%)': round(position_allocation * 100, 2),
                '수익률': 0.0,
                '차익실현': f'{round(ma_100d + atr_10d, 2)} (10일 ATR 상단) 또는 6일 후 강제매도',
                '손절매': round(stop_loss_price, 2),
                '수익보호': profit_protection,
                '롱여부': True,
                # 부가 정보
                'adx_7': adx_7d,
                'rsi_3': rsi_3d,
                'atr_10': atr_10d,
                'avg_volume_50': avg_volume_50d,
                'avg_daily_value_50': avg_value_50d,
                'ma_100': ma_100d
            })

        if not results:
            print("❌ 스크리닝 결과가 없습니다.")
            # 빈 결과 파일 생성
            # 빈 결과 파일 생성 - 표준 컬럼 사용
            empty_columns = ['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']
            pd.DataFrame(columns=empty_columns).to_csv(result_file, index=False, encoding='utf-8-sig')
            # JSON 파일 생성
            json_file = result_file.replace('.csv', '.json')
            pd.DataFrame(columns=empty_columns).to_json(json_file, orient='records', indent=2, force_ascii=False)
            return
        
        # 결과 데이터프레임 생성
        result_df = pd.DataFrame(results)
        
        # ADX 내림차순, RSI 오름차순으로 정렬
        result_df = result_df.sort_values(['adx_7', 'rsi_3'], ascending=[False, True])
        
        # 기존 포트폴리오 로드 및 병합 (원칙 3: 이미 채워진 종목은 매수일을 업데이트하지 않음)
        existing_portfolio = pd.DataFrame()
        if os.path.exists(result_file):
            try:
                existing_portfolio = pd.read_csv(result_file, encoding='utf-8-sig')
                print(f"📂 기존 포트폴리오 로드: {len(existing_portfolio)}개 종목")
            except Exception as e:
                print(f"⚠️ 기존 포트폴리오 로드 실패: {e}")
                existing_portfolio = pd.DataFrame()
        
        # 새로운 후보 종목들 (기존 종목 제외)
        if not existing_portfolio.empty:
            existing_symbols = set(existing_portfolio['종목명'].tolist())
            new_candidates = result_df[~result_df['종목명'].isin(existing_symbols)]
        else:
            new_candidates = result_df
        
        # 포트폴리오 구성 (원칙 1: 항상 최대한 만족하는 10개의 종목을 채우려 노력)
        final_portfolio = existing_portfolio.copy()
        
        # 10개까지 채우기 위해 새로운 종목 추가 (조건에 맞는 종목이 있을 때만)
        needed_count = max(0, 10 - len(final_portfolio))
        if needed_count > 0:
            if not new_candidates.empty:
                additional_stocks = new_candidates.head(needed_count)
                # 결과 CSV에 포함할 컬럼 선택 (표준 컬럼)
                strategy_result_columns = ['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']
                additional_stocks_filtered = additional_stocks[strategy_result_columns]
                final_portfolio = pd.concat([final_portfolio, additional_stocks_filtered], ignore_index=True)
                print(f"➕ {len(additional_stocks_filtered)}개 새로운 종목 추가")
            else:
                print(f"⚠️ 조건에 맞는 새로운 종목이 없어 {len(final_portfolio)}개로 유지합니다.")
        
        # 원칙 2: 실행해서 csv파일이 일부라도 비어있을 경우(10개 미만일 경우) 종목을 찾는다
        if len(final_portfolio) < 10 and not result_df.empty:
            remaining_needed = 10 - len(final_portfolio)
            print(f"📋 포트폴리오가 {len(final_portfolio)}개로 부족하여 {remaining_needed}개 더 채웁니다.")
            strategy_result_columns = ['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']
            additional_fill = result_df.head(remaining_needed)[strategy_result_columns]
            final_portfolio = pd.concat([final_portfolio, additional_fill], ignore_index=True)
        
        # 최종 포트폴리오가 비어있는 경우 빈 파일 생성
        if final_portfolio.empty:
            strategy_result_columns = ['종목명', '매수일', '매수가', '비중(%)', '수익률', '차익실현', '손절매', '수익보호', '롱여부']
            final_portfolio = pd.DataFrame(columns=strategy_result_columns)
        
        # 결과 저장
        final_portfolio.to_csv(result_file, index=False, encoding='utf-8-sig')
        
        # JSON 파일도 저장
        json_file = result_file.replace('.csv', '.json')
        final_portfolio.to_json(json_file, orient='records', force_ascii=False, indent=2)
        
        print(f"✅ 전략 5 스크리닝 결과 저장 완료: {len(final_portfolio)}개 종목, 경로: {result_file}")
        print("\n🏆 전략 5 상위 종목 (스크리닝 결과):")
        print(final_portfolio)


    except Exception as e:
        print(f"❌ 전략 5 스크리닝 오류: {e}")
        print(traceback.format_exc())







def run_strategy():
    """Wrapper function for main.py compatibility"""
    return run_strategy5_screening()

