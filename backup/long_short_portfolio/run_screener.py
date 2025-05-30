# -*- coding: utf-8 -*-
# 스크리너 실행 스크립트

import os
import sys
import subprocess

# 현재 디렉토리 확인
current_dir = os.getcwd()
print(f"\n📂 현재 작업 디렉토리: {current_dir}")

# 프로젝트 루트 디렉토리 확인
root_dir = os.path.dirname(os.path.abspath(__file__))
print(f"📂 프로젝트 루트 디렉토리: {root_dir}")

# 프로젝트 루트 디렉토리를 sys.path에 추가
parent_dir = os.path.dirname(root_dir)
sys.path.insert(0, parent_dir)
print(f"📂 상위 디렉토리를 sys.path에 추가: {parent_dir}")

# 설정 파일 임포트
try:
    from config import (
        DATA_DIR, DATA_US_DIR, 
        RESULTS_DIR, RESULTS_VER2_DIR
    )
    print("\n✅ config.py 모듈 임포트 성공")
    
    # 설정된 경로 출력
    print("\n📁 설정된 경로:")
    print(f"- DATA_DIR: {DATA_DIR}")
    print(f"- DATA_US_DIR: {DATA_US_DIR}")
    print(f"- RESULTS_DIR: {RESULTS_DIR}")
    print(f"- RESULTS_VER2_DIR: {RESULTS_VER2_DIR}")
    
    # 경로 존재 여부 확인 및 생성
    print("\n🔍 경로 존재 여부 확인 및 생성:")
    for path_name, path in [
        ("DATA_DIR", DATA_DIR),
        ("DATA_US_DIR", DATA_US_DIR),
        ("RESULTS_DIR", RESULTS_DIR),
        ("RESULTS_VER2_DIR", RESULTS_VER2_DIR),
    ]:
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
            print(f"- {path_name}: ✅ 생성 완료")
        else:
            print(f"- {path_name}: ✅ 이미 존재함")
    
    # 매수/매도 결과 디렉토리 확인 및 생성
    buy_dir = os.path.join(RESULTS_VER2_DIR, 'buy')
    sell_dir = os.path.join(RESULTS_VER2_DIR, 'sell')
    
    print("\n📊 매수/매도 결과 디렉토리 확인 및 생성:")
    if not os.path.exists(buy_dir):
        os.makedirs(buy_dir, exist_ok=True)
        print(f"- 매수 디렉토리(buy): ✅ 생성 완료")
    else:
        print(f"- 매수 디렉토리(buy): ✅ 이미 존재함")
        
    if not os.path.exists(sell_dir):
        os.makedirs(sell_dir, exist_ok=True)
        print(f"- 매도 디렉토리(sell): ✅ 생성 완료")
    else:
        print(f"- 매도 디렉토리(sell): ✅ 이미 존재함")
    
    # 결과 파일 경로 확인
    strategy1_file = os.path.join(buy_dir, 'strategy1_results.csv')
    strategy2_file = os.path.join(sell_dir, 'strategy2_results.csv')
    
    print("\n📄 결과 파일 경로:")
    print(f"- 전략1 결과 파일: {strategy1_file}")
    print(f"  존재 여부: {'✅ 존재함' if os.path.exists(strategy1_file) else '❌ 아직 생성되지 않음'}")
    print(f"- 전략2 결과 파일: {strategy2_file}")
    print(f"  존재 여부: {'✅ 존재함' if os.path.exists(strategy2_file) else '❌ 아직 생성되지 않음'}")
    
except ImportError as e:
    import traceback
    print(f"\n❌ config.py 모듈 임포트 실패: {e}")
    print(traceback.format_exc())
    print("\n💡 해결 방법: 스크립트를 프로젝트 루트 디렉토리에서 실행하세요.")
    sys.exit(1)

# long_short_portfolio 디렉토리 확인
long_short_dir = root_dir  # root_dir 자체가 long_short_portfolio 디렉토리입니다
print(f"\n📁 long_short_portfolio 디렉토리: {long_short_dir}")

if not os.path.exists(long_short_dir):
    print(f"  ❌ 존재하지 않음")
    sys.exit(1)

screener_file = os.path.join(long_short_dir, 'screener_ver2.py')
if not os.path.exists(screener_file):
    print(f"  ❌ screener_ver2.py 파일이 존재하지 않음")
    sys.exit(1)

print(f"  ✅ screener_ver2.py 파일 존재함")

# 스크리너 실행
print("\n🚀 스크리너 실행 중...")
print("="*50)
print("\n📊 모든 전략을 자동으로 실행합니다.")

try:
    # 스크리너 모듈 임포트
    sys.path.append(root_dir)
    from screener_ver2 import run_strategy1, track_portfolio_strategy1, run_strategy2, track_portfolio_strategy2
    
    # 포트폴리오 파일 존재 여부 확인
    portfolio1_file = os.path.join(RESULTS_VER2_DIR, 'portfolio_strategy1.csv')
    portfolio2_file = os.path.join(RESULTS_VER2_DIR, 'portfolio_strategy2.csv')
    
    portfolio_exists = os.path.exists(portfolio1_file) or os.path.exists(portfolio2_file)
    
    if portfolio_exists:
        print("\n📊 기존 포트폴리오가 발견되었습니다. 포트폴리오 추적 및 업데이트를 실행합니다.")
        # 포트폴리오 추적 및 업데이트
        track_portfolio_strategy1()
        track_portfolio_strategy2()
        print("\n✅ 모든 전략 포트폴리오 추적 및 업데이트 완료")
    else:
        print("\n📊 기존 포트폴리오가 없습니다. 스크리닝 후 새 포트폴리오를 생성합니다.")
        # 스크리닝 및 새 포트폴리오 생성
        run_strategy1(create_portfolio=True, update_existing=False)
        run_strategy2(create_portfolio=True, update_existing=False)
        print("\n✅ 모든 전략 스크리닝 및 새 포트폴리오 생성 완료")
    
    print("="*50)
    print("\n✅ 스크리너 실행 완료")
    
    # 결과 파일 확인
    print("\n📄 결과 파일 생성 확인:")
    print(f"- 전략1 결과 파일: {'✅ 생성됨' if os.path.exists(strategy1_file) else '❌ 생성되지 않음'}")
    print(f"- 전략2 결과 파일: {'✅ 생성됨' if os.path.exists(strategy2_file) else '❌ 생성되지 않음'}")
    
except Exception as e:  # subprocess.CalledProcessError 대신 일반 Exception 사용
    import traceback
    print("="*50)
    print(f"\n❌ 스크리너 실행 중 오류 발생: {e}")
    print(traceback.format_exc())

print("\n💡 참고: 결과 CSV 파일은 다음 경로에 생성됩니다:")
print(f"   - 매수 전략 결과: {buy_dir}")
print(f"   - 매도 전략 결과: {sell_dir}")