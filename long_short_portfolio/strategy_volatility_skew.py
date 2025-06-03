# -*- coding: utf-8 -*-
"""
변동성 스큐 역전 전략 (Strategy Volatility Skew)
Xing et al.(2010) 논문 기반 주간 리밸런싱 롱-숏 전략
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config import RESULTS_VER2_DIR, OPTION_VOLATILITY_DIR
from utils import ensure_dir
from option_data_based_strategy.volatility_skew_screener import run_volatility_skew_screening

def run_strategy(total_capital: float = 100000, alpha_vantage_key: Optional[str] = None):
    """
    변동성 스큐 역전 전략 실행
    
    Args:
        total_capital: 총 자본금
        alpha_vantage_key: Alpha Vantage API 키 (선택사항)
    """
    print("\n🔍 변동성 스큐 역전 전략 (Strategy Volatility Skew) 시작...")
    
    # 결과 디렉토리 설정
    strategy_dir = os.path.join(RESULTS_VER2_DIR, 'volatility_skew')
    ensure_dir(strategy_dir)
    
    try:
        # 변동성 스큐 스크리닝 실행
        portfolios, signals, portfolio_file, signals_file = run_volatility_skew_screening(alpha_vantage_key)
        
        if not portfolios or not signals:
            print("❌ 변동성 스큐 스크리닝 실패")
            return
        
        # 롱-숏 포트폴리오 결과 생성
        long_results = []
        short_results = []
        
        # 자본 배분 (50% 롱, 50% 숏)
        long_capital = total_capital * 0.5
        short_capital = total_capital * 0.5
        
        # 롱 포지션 처리
        long_stocks = signals['long_portfolio']
        if long_stocks:
            for stock in long_stocks:
                position_value = long_capital * stock['weight']
                quantity = int(position_value / stock['current_price'])
                
                if quantity > 0:
                    long_results.append({
                        '종목명': stock['symbol'],
                        '회사명': stock['company_name'],
                        '매수일': datetime.now().strftime('%Y-%m-%d'),
                        '시장 진입가': stock['current_price'],
                        '수량': quantity,
                        '비중(%)': stock['weight'] * 100,
                        '포지션가치': position_value,
                        '수익률(%)': 0.0,
                        '차익실현': '15%',  # 15% 수익 시 차익실현
                        '손절매': '-8%',   # 8% 손실 시 손절
                        '수익보호': '있음',
                        '롱여부': True,
                        '스큐지수': stock['skew_index'],
                        '신호강도': 'HIGH',
                        '전략': 'volatility_skew_long'
                    })
        
        # 숏 포지션 처리
        short_stocks = signals['short_portfolio']
        if short_stocks:
            for stock in short_stocks:
                position_value = short_capital * stock['weight']
                quantity = int(position_value / stock['current_price'])
                
                if quantity > 0:
                    short_results.append({
                        '종목명': stock['symbol'],
                        '회사명': stock['company_name'],
                        '매도일': datetime.now().strftime('%Y-%m-%d'),
                        '시장 진입가': stock['current_price'],
                        '수량': quantity,
                        '비중(%)': stock['weight'] * 100,
                        '포지션가치': position_value,
                        '수익률(%)': 0.0,
                        '차익실현': '15%',  # 15% 수익 시 차익실현 (숏의 경우 가격 하락)
                        '손절매': '-8%',   # 8% 손실 시 손절 (숏의 경우 가격 상승)
                        '수익보호': '있음',
                        '롱여부': False,
                        '스큐지수': stock['skew_index'],
                        '신호강도': 'HIGH',
                        '전략': 'volatility_skew_short'
                    })
        
        # 결과 파일 저장
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 롱 포지션 결과 저장
        if long_results:
            long_df = pd.DataFrame(long_results)
            long_file = os.path.join(strategy_dir, f'volatility_skew_long_{timestamp}.csv')
            long_df.to_csv(long_file, index=False, encoding='utf-8-sig')
            
            long_json_file = long_file.replace('.csv', '.json')
            long_df.to_json(long_json_file, orient='records', indent=2, force_ascii=False)
            
            print(f"📈 롱 포지션 결과 저장: {long_file}")
            print(f"   - 종목 수: {len(long_results)}개")
            print(f"   - 총 투자금액: ${sum(r['포지션가치'] for r in long_results):,.0f}")
        
        # 숏 포지션 결과 저장
        if short_results:
            short_df = pd.DataFrame(short_results)
            short_file = os.path.join(strategy_dir, f'volatility_skew_short_{timestamp}.csv')
            short_df.to_csv(short_file, index=False, encoding='utf-8-sig')
            
            short_json_file = short_file.replace('.csv', '.json')
            short_df.to_json(short_json_file, orient='records', indent=2, force_ascii=False)
            
            print(f"📉 숏 포지션 결과 저장: {short_file}")
            print(f"   - 종목 수: {len(short_results)}개")
            print(f"   - 총 투자금액: ${sum(r['포지션가치'] for r in short_results):,.0f}")
        
        # 통합 결과 저장
        all_results = long_results + short_results
        if all_results:
            combined_df = pd.DataFrame(all_results)
            combined_file = os.path.join(strategy_dir, f'volatility_skew_combined_{timestamp}.csv')
            combined_df.to_csv(combined_file, index=False, encoding='utf-8-sig')
            
            combined_json_file = combined_file.replace('.csv', '.json')
            combined_df.to_json(combined_json_file, orient='records', indent=2, force_ascii=False)
            
            print(f"📊 통합 결과 저장: {combined_file}")
        
        # 전략 요약 출력
        print(f"\n✅ 변동성 스큐 역전 전략 완료")
        print(f"   - 총 자본금: ${total_capital:,.0f}")
        print(f"   - 롱 포지션: {len(long_results)}개 종목")
        print(f"   - 숏 포지션: {len(short_results)}개 종목")
        print(f"   - 다음 리밸런싱: {(datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d')}")
        
    except Exception as e:
        print(f"❌ 변동성 스큐 역전 전략 실행 오류: {e}")
        import traceback
        print(traceback.format_exc())


if __name__ == "__main__":
    # 테스트 실행
    run_strategy(total_capital=100000)