import pandas as pd
import os
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# config에서 필요한 경로 임포트
from config import (
    RESULTS_DIR, US_WITH_RS_PATH, 
    DATA_DIR, DATA_US_DIR,
    ADVANCED_FINANCIAL_RESULTS_PATH
)

# utils에서 함수 import
from utils import ensure_dir, load_csvs_parallel

# 크립토 필터링 함수 제거됨

def filter_us():
    # 파일 경로 설정
    input_file = os.path.join(RESULTS_DIR, 'us_with_rs.csv')
    
    # CSV 파일 읽기
    df = pd.read_csv(input_file)
    
    # 모든 조건이 True인 종목만 필터링
    conditions = [f'cond{i}' for i in range(1, 9)]  # cond1부터 cond8까지
    filtered_df = df[df[conditions].all(axis=1)]
    
    # RS 점수 기준으로 내림차순 정렬
    sorted_df = filtered_df.sort_values(by='rs_score', ascending=False)
    
    # 결과를 원본 파일에 덮어쓰기
    sorted_df.to_csv(input_file, index=False)
    # JSON 파일 생성 추가
    json_file = input_file.replace('.csv', '.json')
    sorted_df.to_json(json_file, orient='records', indent=2, force_ascii=False)
    
    # 필터링 결과 출력
    print(f'[US] 총 {len(df)}개 중 {len(filtered_df)}개 종목이 모든 조건을 만족함')
    print('[US] 상위 5개 종목:')
    print(sorted_df[['symbol', 'rs_score']].head(5))

def run_integrated_screening():
    """
    기술적 스크리닝과 재무제표 스크리닝 결과를 통합하는 함수
    """
    print("\n🔍 통합 스크리닝 시작...")
    
    # 결과 저장 경로 (results/screeners/markminervini 폴더에 저장)
    from config import MARKMINERVINI_RESULTS_DIR
    INTEGRATED_RESULTS_PATH = os.path.join(MARKMINERVINI_RESULTS_DIR, 'integrated_results.csv')
    
    try:
        # 기술적 스크리닝 결과 로드
        if not os.path.exists(US_WITH_RS_PATH):
            print(f"⚠️ 기술적 스크리닝 결과 파일이 없습니다: {US_WITH_RS_PATH}")
            return
        
        tech_df = pd.read_csv(US_WITH_RS_PATH)
        print(f"✅ 기술적 스크리닝 결과 로드 완료: {len(tech_df)}개 종목")
        
        # tech_df에 symbol 컬럼이 없으면 인덱스에서 생성
        if 'symbol' not in tech_df.columns:
            if tech_df.index.name is None:
                tech_df = tech_df.reset_index()
                tech_df = tech_df.rename(columns={'index': 'symbol'})
            else:
                tech_df = tech_df.reset_index()
        
        # 재무제표 스크리닝 결과 로드
        if not os.path.exists(ADVANCED_FINANCIAL_RESULTS_PATH):
            print(f"⚠️ 재무제표 스크리닝 결과 파일이 없습니다: {ADVANCED_FINANCIAL_RESULTS_PATH}")
            return
        
        fin_df = pd.read_csv(ADVANCED_FINANCIAL_RESULTS_PATH)
        print(f"✅ 재무제표 스크리닝 결과 로드 완료: {len(fin_df)}개 종목")
        
        # fin_df에 symbol 컬럼이 없으면 인덱스에서 생성
        if 'symbol' not in fin_df.columns:
            if fin_df.index.name is None:
                fin_df = fin_df.reset_index()
                fin_df = fin_df.rename(columns={'index': 'symbol'})
            else:
                fin_df = fin_df.reset_index()
        
        # 두 결과 병합 (중복 컬럼 처리)
        # 'rs_score' 컬럼이 중복될 경우 tech_df의 것을 사용
        if 'rs_score' in tech_df.columns and 'rs_score' in fin_df.columns:
            fin_df = fin_df.rename(columns={'rs_score': 'rs_score_fin'})
        
        merged_df = pd.merge(tech_df, fin_df, on='symbol', how='inner')
        
        # 중복 행 제거
        merged_df = merged_df.drop_duplicates(subset=['symbol'])
        print(f"✅ 통합 결과: {len(merged_df)}개 종목")
        
        if merged_df.empty:
            print("❌ 통합 결과가 없습니다.")
            return
        
        # 기술적 조건 충족 수와 재무제표 조건 충족 수 합산
        if 'met_count' in merged_df.columns and 'fin_met_count' in merged_df.columns:
            merged_df['total_met_count'] = merged_df['met_count'] + merged_df['fin_met_count']
        else:
            print(f"⚠️ 필요한 컬럼이 없습니다. 사용 가능한 컬럼: {', '.join(merged_df.columns.tolist())}")
            if 'met_count' not in merged_df.columns:
                print("❌ 'met_count' 컬럼이 없습니다.")
            if 'fin_met_count' not in merged_df.columns:
                print("❌ 'fin_met_count' 컬럼이 없습니다.")
            return
        
        # RS 점수 열 처리
        if 'rs_score_x' in merged_df.columns:
            merged_df['rs_score'] = merged_df['rs_score_x']
            merged_df = merged_df.drop('rs_score_x', axis=1)
        if 'rs_score_y' in merged_df.columns:
            merged_df = merged_df.drop('rs_score_y', axis=1)
        
        # 결과 정렬 및 저장 (total_met_count 기준으로 먼저 정렬하고, 그 다음 rs_score로 내림차순 정렬)
        merged_df = merged_df.sort_values(['total_met_count', 'rs_score'], ascending=[False, False])
        
        # 필요한 컬럼만 선택하여 저장 (symbol, met_count, fin_met_count, total_met_count, rs_score)
        selected_columns = ['symbol', 'met_count', 'fin_met_count', 'total_met_count', 'rs_score']
        filtered_df = merged_df[selected_columns]
        
        ensure_dir(MARKMINERVINI_RESULTS_DIR)
        filtered_df.to_csv(INTEGRATED_RESULTS_PATH, index=False)
        # JSON 파일 생성 추가
        json_path = INTEGRATED_RESULTS_PATH.replace('.csv', '.json')
        filtered_df.to_json(json_path, orient='records', indent=2, force_ascii=False)
        
        print(f"✅ 통합 스크리닝 결과 저장 완료: {INTEGRATED_RESULTS_PATH}")
        print(f"✅ 저장된 컬럼: {', '.join(selected_columns)}")
        
        # 상위 10개 종목 출력
        top_10 = filtered_df.head(10).reset_index(drop=True)
        print("\n🏆 통합 스크리닝 상위 10개 종목:")
        pd.set_option('display.max_rows', None)
        print(top_10.to_string(index=True))
        
        # 통합 스크리너 실행 (패턴 감지 포함)
        try:
            print("\n🔍 통합 패턴 감지 스크리너 실행 중...")
            from .integrated_screener import run_integrated_screening
            
            # 상위 30개 심볼만 패턴 감지
            top_symbols = filtered_df.head(30)['symbol'].tolist()
            if top_symbols:
                pattern_results = run_integrated_screening(max_symbols=len(top_symbols))
                print(f"✅ 패턴 감지 완료: {len(pattern_results)}개 심볼 처리")
            else:
                print("⚠️ 패턴 감지할 심볼이 없습니다.")
        except Exception as e:
            print(f"⚠️ 통합 패턴 감지 오류: {e}")
        
    except Exception as e:
        print(f"❌ 통합 스크리닝 오류: {e}")
        import traceback
        traceback.print_exc()

def main():
    parser = argparse.ArgumentParser(description='주식 데이터 필터링 및 정렬 도구')
    parser.add_argument('--integrated', action='store_true', help='통합 스크리닝 실행 (기술적 + 재무제표)')
    args = parser.parse_args()
    
    if args.integrated:
        # 통합 스크리닝 실행
        run_integrated_screening()
        return
    
    # 기본적으로 US 주식 데이터 처리
    filter_us()
