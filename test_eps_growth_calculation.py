# -*- coding: utf-8 -*-
# EPS 성장률 계산 로직 테스트 스크립트

import pandas as pd
import numpy as np
from datetime import datetime

def test_eps_growth_calculation():
    """EPS 성장률 계산 로직 테스트"""
    
    print("🧪 EPS 성장률 계산 로직 테스트")
    print("=" * 50)
    
    # 테스트 케이스 정의
    test_cases = [
        # (현재 EPS, 1년전 EPS, 예상 성장률, 설명)
        (2.0, 1.0, 100.0, "양수→양수 증가 (1.0→2.0)"),
        (1.0, 2.0, -50.0, "양수→양수 감소 (2.0→1.0)"),
        (1.0, -2.0, 200.0, "음수→양수 전환 (-2.0→1.0)"),
        (-1.0, -3.0, 66.67, "음수→음수 개선 (-3.0→-1.0)"),
        (-3.0, -1.0, -200.0, "음수→음수 악화 (-1.0→-3.0)"),
        (0.0, 1.0, -100.0, "양수→제로 (1.0→0.0)"),
        (1.0, 0.0, 0.0, "제로→양수 (0.0→1.0, 분모가 0이므로 0으로 처리)"),
        (-1.0, 0.0, 0.0, "제로→음수 (0.0→-1.0, 분모가 0이므로 0으로 처리)"),
    ]
    
    def calculate_eps_growth(eps_actual, prev_eps):
        """개선된 EPS 성장률 계산 함수"""
        if prev_eps != 0:
            if prev_eps > 0:
                # 기존 양수 EPS: 일반적인 성장률 계산
                return ((eps_actual - prev_eps) / prev_eps) * 100
            else:
                # 기존 음수 EPS: 손실 개선 여부로 판단
                if eps_actual >= 0:
                    # 손실에서 흑자 전환: 매우 긍정적 (200%로 설정)
                    return 200
                else:
                    # 여전히 손실이지만 개선: 손실 감소율로 계산
                    return ((eps_actual - prev_eps) / abs(prev_eps)) * 100
        else:
            return 0
    
    # 테스트 실행
    for i, (current_eps, prev_eps, expected_growth, description) in enumerate(test_cases, 1):
        calculated_growth = calculate_eps_growth(current_eps, prev_eps)
        
        # 결과 출력
        status = "✅" if abs(calculated_growth - expected_growth) < 0.1 else "❌"
        print(f"{status} 테스트 {i}: {description}")
        print(f"   계산된 성장률: {calculated_growth:.2f}%")
        print(f"   예상 성장률: {expected_growth:.2f}%")
        
        if abs(calculated_growth - expected_growth) >= 0.1:
            print(f"   ⚠️ 차이: {abs(calculated_growth - expected_growth):.2f}%")
        print()
    
    print("📊 특별 케이스 분석:")
    print("-" * 30)
    
    # 사용자가 언급한 케이스 (-3 → -1)
    print("🔍 사용자 언급 케이스: -3에서 -1로 개선")
    growth = calculate_eps_growth(-1.0, -3.0)
    print(f"   성장률: {growth:.2f}%")
    print(f"   해석: 손실이 66.67% 개선됨 (긍정적)")
    print()
    
    # 기존 로직과 비교
    print("🔍 기존 로직 vs 개선 로직 비교")
    old_growth = ((-1.0 - (-3.0)) / abs(-3.0)) * 100  # 기존 로직
    new_growth = calculate_eps_growth(-1.0, -3.0)  # 개선 로직
    print(f"   기존 로직: {old_growth:.2f}%")
    print(f"   개선 로직: {new_growth:.2f}%")
    print(f"   결과: {'동일' if abs(old_growth - new_growth) < 0.1 else '다름'}")
    print()
    
    print("💡 개선 사항 요약:")
    print("   1. 음수→양수 전환 시 200% 고정값으로 매우 긍정적 평가")
    print("   2. 음수→음수 개선 시 손실 감소율로 정확한 계산")
    print("   3. 양수 EPS는 기존과 동일한 일반적 성장률 계산")
    print("   4. 분모가 0인 경우 안전하게 0으로 처리")

def test_with_real_data_simulation():
    """실제 데이터 시뮬레이션 테스트"""
    print("\n📈 실제 데이터 시뮬레이션")
    print("=" * 50)
    
    # 가상의 실적 데이터 생성
    earnings_scenarios = [
        {
            'company': 'Company A (흑자 성장)',
            'data': [0.5, 0.8, 1.2, 2.0],  # 4분기 EPS 데이터
            'description': '꾸준한 흑자 성장'
        },
        {
            'company': 'Company B (적자 개선)',
            'data': [-2.0, -1.5, -0.5, 0.2],  # 적자에서 흑자 전환
            'description': '적자에서 흑자로 전환'
        },
        {
            'company': 'Company C (적자 지속)',
            'data': [-3.0, -2.5, -2.0, -1.0],  # 적자 개선
            'description': '적자 지속하지만 개선'
        },
        {
            'company': 'Company D (흑자 악화)',
            'data': [2.0, 1.5, 1.0, -0.5],  # 흑자에서 적자로
            'description': '흑자에서 적자로 악화'
        }
    ]
    
    def calculate_eps_growth_from_data(eps_data):
        """데이터 배열에서 EPS 성장률 계산"""
        current_eps = eps_data[-1]  # 최신 분기
        prev_eps = eps_data[0]      # 4분기 전
        
        if prev_eps != 0:
            if prev_eps > 0:
                return ((current_eps - prev_eps) / prev_eps) * 100
            else:
                if current_eps >= 0:
                    return 200
                else:
                    return ((current_eps - prev_eps) / abs(prev_eps)) * 100
        else:
            return 0
    
    for scenario in earnings_scenarios:
        company = scenario['company']
        data = scenario['data']
        description = scenario['description']
        
        growth = calculate_eps_growth_from_data(data)
        
        print(f"🏢 {company}")
        print(f"   EPS 추이: {' → '.join([f'{eps:.1f}' for eps in data])}")
        print(f"   설명: {description}")
        print(f"   전년 동기 대비 성장률: {growth:.1f}%")
        
        # 쿨라매기 기준 충족 여부
        meets_criteria = growth >= 100
        print(f"   쿨라매기 기준(≥100%) 충족: {'✅' if meets_criteria else '❌'}")
        print()

if __name__ == "__main__":
    test_eps_growth_calculation()
    test_with_real_data_simulation()
    
    print("\n" + "=" * 50)
    print("✅ 모든 테스트 완료")
    print("\n💡 결론:")
    print("   개선된 로직은 EPS가 음수인 상황에서도")
    print("   올바른 성장률을 계산하며, 손실 개선을")
    print("   적절히 반영합니다.")