# -*- coding: utf-8 -*-
# RS 점수 계산 테스트 스크립트

import os
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from Markminervini.screener import run_us_screening, calculate_rs_score
from utils import ensure_dir
from config import DATA_DIR, DATA_US_DIR, RESULTS_DIR, US_WITH_RS_PATH

def main():
    print("\n🧪 RS 점수 계산 테스트 시작...")
    
    # 필요한 디렉토리 확인
    ensure_dir(DATA_DIR)
    ensure_dir(DATA_US_DIR)
    ensure_dir(RESULTS_DIR)
    
    # 미국 주식 스크리닝 실행
    print("\n🔍 미국 주식 스크리닝 실행 중...")
    run_us_screening()
    
    # 결과 확인
    if os.path.exists(US_WITH_RS_PATH):
        try:
            result_df = pd.read_csv(US_WITH_RS_PATH)
            print(f"\n✅ 결과 파일 로드 성공: {len(result_df)}개 종목")
            
            # RS 점수 분포 확인
            rs_stats = result_df['rs_score'].describe()
            print(f"\n📊 RS 점수 통계:\n{rs_stats}")
            
            # 상위 10개 종목 확인
            top_10 = result_df.sort_values('rs_score', ascending=False).head(10)
            print(f"\n🏆 RS 점수 상위 10개 종목:\n{top_10[['rs_score']]}")
            
            # 하위 10개 종목 확인
            bottom_10 = result_df.sort_values('rs_score').head(10)
            print(f"\n🔻 RS 점수 하위 10개 종목:\n{bottom_10[['rs_score']]}")
            
            # 모든 종목이 동일한 RS 점수를 가지는지 확인
            unique_rs = result_df['rs_score'].nunique()
            print(f"\n🔢 고유한 RS 점수 개수: {unique_rs}개")
            
            if unique_rs <= 1:
                print("⚠️ 모든 종목이 동일한 RS 점수를 가집니다. 문제가 있을 수 있습니다.")
            else:
                print("✅ 다양한 RS 점수가 존재합니다. 정상적으로 계산된 것으로 보입니다.")
        except Exception as e:
            print(f"❌ 결과 파일 처리 오류: {e}")
    else:
        print(f"❌ 결과 파일을 찾을 수 없습니다: {US_WITH_RS_PATH}")

if __name__ == "__main__":
    main()