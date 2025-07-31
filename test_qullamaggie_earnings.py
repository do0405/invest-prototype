# -*- coding: utf-8 -*-
# 쿨라매기 스크리너 실적 서프라이즈 필터 테스트 스크립트

import sys
import os
from utils.path_utils import add_project_root

# 프로젝트 루트 디렉토리를 Python 경로에 추가
add_project_root()

from screeners.qullamaggie.runner import run_qullamaggie_strategy
from screeners.qullamaggie.earnings_data_collector import EarningsDataCollector

def test_earnings_data_collector():
    """실적 데이터 수집기 테스트"""
    print("\n🧪 실적 데이터 수집기 테스트...")
    
    collector = EarningsDataCollector()
    
    # 테스트할 종목들 (대형주 위주)
    test_symbols = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
    
    for symbol in test_symbols:
        print(f"\n📊 {symbol} 실적 데이터 수집 중...")
        earnings_data = collector.get_earnings_surprise(symbol)
        
        if earnings_data:
            print(f"  ✅ 실적 데이터 수집 성공")
            print(f"  📈 EPS 서프라이즈: {earnings_data['eps_surprise_pct']:.2f}%")
            print(f"  📈 매출 서프라이즈: {earnings_data['revenue_surprise_pct']:.2f}%")
            print(f"  📈 전년 동기 EPS 성장률: {earnings_data['yoy_eps_growth']:.2f}%")
            print(f"  📈 전년 동기 매출 성장률: {earnings_data['yoy_revenue_growth']:.2f}%")
            print(f"  🎯 쿨라매기 기준 충족: {'✅' if earnings_data['meets_criteria'] else '❌'}")
        else:
            print(f"  ❌ 실적 데이터 수집 실패")

def test_qullamaggie_with_earnings_filter():
    """실적 필터 활성화된 쿨라매기 스크리닝 테스트"""
    print("\n🔍 실적 필터 활성화 쿨라매기 스크리닝 테스트...")
    
    try:
        # 에피소드 피벗 셋업만 실행 (실적 필터 활성화)
        result = run_qullamaggie_strategy(
            setups=['episode_pivot'], 
            skip_data=False, 
            enable_earnings_filter=True
        )
        
        if result:
            print("✅ 실적 필터 활성화 스크리닝 완료")
        else:
            print("❌ 실적 필터 활성화 스크리닝 실패")
            
    except Exception as e:
        print(f"❌ 스크리닝 중 오류 발생: {str(e)}")

def test_qullamaggie_without_earnings_filter():
    """실적 필터 비활성화된 쿨라매기 스크리닝 테스트"""
    print("\n🔍 실적 필터 비활성화 쿨라매기 스크리닝 테스트...")
    
    try:
        # 에피소드 피벗 셋업만 실행 (실적 필터 비활성화)
        result = run_qullamaggie_strategy(
            setups=['episode_pivot'], 
            skip_data=False, 
            enable_earnings_filter=False
        )
        
        if result:
            print("✅ 실적 필터 비활성화 스크리닝 완료")
        else:
            print("❌ 실적 필터 비활성화 스크리닝 실패")
            
    except Exception as e:
        print(f"❌ 스크리닝 중 오류 발생: {str(e)}")

def compare_results():
    """실적 필터 활성화/비활성화 결과 비교"""
    print("\n📊 결과 비교 분석...")
    
    import pandas as pd
    from config import QULLAMAGGIE_RESULTS_DIR
    
    episode_pivot_path = os.path.join(QULLAMAGGIE_RESULTS_DIR, 'episode_pivot_results.csv')
    
    if os.path.exists(episode_pivot_path):
        df = pd.read_csv(episode_pivot_path)
        print(f"📈 에피소드 피벗 결과: {len(df)}개 종목")
        
        if not df.empty:
            print("\n🏆 상위 5개 종목:")
            top_5 = df.head(5)
            for idx, row in top_5.iterrows():
                earnings_status = "📊" if row.get('earnings_surprise') else "❌"
                print(f"  {earnings_status} {row['symbol']}: 점수 {row['score']}, 갭업 {row.get('gap_percent', 0):.1f}%")
        else:
            print("  ⚠️ 조건을 만족하는 종목이 없습니다.")
    else:
        print("❌ 결과 파일을 찾을 수 없습니다.")

def main():
    """메인 테스트 함수"""
    print("🚀 쿨라매기 스크리너 실적 서프라이즈 필터 테스트 시작")
    print("=" * 60)
    
    # 1. 실적 데이터 수집기 테스트
    test_earnings_data_collector()
    
    # 2. 실적 필터 비활성화 테스트 (기준선)
    test_qullamaggie_without_earnings_filter()
    
    # 3. 결과 확인
    print("\n📊 실적 필터 비활성화 결과:")
    compare_results()
    
    # 4. 실적 필터 활성화 테스트
    test_qullamaggie_with_earnings_filter()
    
    # 5. 결과 비교
    print("\n📊 실적 필터 활성화 결과:")
    compare_results()
    
    print("\n" + "=" * 60)
    print("✅ 테스트 완료")
    print("\n💡 사용법:")
    print("  - 실적 필터 활성화: run_qullamaggie_strategy(enable_earnings_filter=True)")
    print("  - 실적 필터 비활성화: run_qullamaggie_strategy(enable_earnings_filter=False)")

if __name__ == "__main__":
    main()