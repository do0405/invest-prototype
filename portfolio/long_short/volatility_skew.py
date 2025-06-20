#!/usr/bin/env python3
"""
변동성 스큐 역전 전략 (Volatility Skew Reversal Strategy)

이 모듈은 옵션 변동성 스큐의 역전을 이용한 매수 전략을 구현합니다.
변동성 스큐가 과도하게 높을 때 매수 포지션을 취하는 전략입니다.
"""

import os
import sys
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# 프로젝트 루트 디렉토리를 Python 경로에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

from portfolio.manager.strategies.volatility_skew_strategy import VolatilitySkewPortfolioStrategy
from config import PORTFOLIO_BUY_DIR
from utils import ensure_dir

def run_volatility_skew_screening(alpha_vantage_key: Optional[str] = None) -> Tuple[List[Dict], str]:
    """
    변동성 스큐 역전 전략 스크리닝을 실행합니다.
    
    Args:
        alpha_vantage_key: Alpha Vantage API 키 (선택사항)
        
    Returns:
        Tuple[List[Dict], str]: (스크리닝 결과 리스트, 결과 파일 경로)
    """
    try:
        print("\n📊 변동성 스큐 역전 전략 스크리닝 시작...")
        
        # 전략 인스턴스 생성
        strategy = VolatilitySkewPortfolioStrategy(alpha_vantage_key=alpha_vantage_key)
        
        # 스크리닝 및 포트폴리오 생성 실행
        results, result_file = strategy.run_screening_and_portfolio_creation()
        
        if results:
            # 결과를 portfolio/buy 디렉토리에도 저장
            ensure_dir(PORTFOLIO_BUY_DIR)
            buy_result_file = os.path.join(PORTFOLIO_BUY_DIR, 'volatility_skew_results.csv')
            
            df_results = pd.DataFrame(results)
            df_results.to_csv(buy_result_file, index=False)
            
            print(f"✅ 변동성 스큐 전략 결과 저장 완료: {len(results)}개 종목")
            print(f"📁 결과 파일: {buy_result_file}")
            
            return results, buy_result_file
        else:
            print("⚠️ 조건을 만족하는 종목이 없습니다.")
            return [], ""
            
    except Exception as e:
        print(f"❌ 변동성 스큐 전략 스크리닝 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return [], ""

def main():
    """
    메인 실행 함수
    """
    print("🚀 변동성 스큐 역전 전략 실행")
    print("=" * 50)
    
    # 스크리닝 실행
    results, result_file = run_volatility_skew_screening()
    
    if results:
        print(f"\n📊 스크리닝 완료: {len(results)}개 종목 발견")
        print(f"📁 결과 파일: {result_file}")
        
        # 상위 5개 종목 출력
        print("\n🔝 상위 5개 종목:")
        for i, result in enumerate(results[:5], 1):
            symbol = result.get('symbol', 'N/A')
            score = result.get('score', 0)
            print(f"  {i}. {symbol} (점수: {score:.2f})")
    else:
        print("\n⚠️ 조건을 만족하는 종목이 없습니다.")
    
    print("\n✅ 변동성 스큐 역전 전략 실행 완료")

if __name__ == "__main__":
    main()