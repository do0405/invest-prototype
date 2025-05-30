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

def ensure_dir(path):
    """디렉토리가 존재하지 않으면 생성하는 함수"""
    os.makedirs(path, exist_ok=True)

# CSV 병렬 로딩 함수
def load_csvs_parallel(directory, time_col, id_col, max_workers=6):
    """여러 CSV 파일을 병렬로 로드하는 함수
    
    Args:
        directory: CSV 파일이 있는 디렉토리 경로
        time_col: 시간 컬럼명
        id_col: ID 컬럼명
        max_workers: 최대 스레드 수
        
    Returns:
        list: 로드된 DataFrame 리스트
    """
    files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    paths = [os.path.join(directory, f) for f in files]

    def _read_csv(path):
        try:
            # CSV 파일 읽기
            df = pd.read_csv(path)
            
            # 컬럼명 소문자로 변환 (대소문자 일관성 유지)
            df.columns = [col.lower() for col in df.columns]
            
            # 날짜 컬럼 처리 (대소문자 구분 없이)
            time_col_lower = time_col.lower()
            if time_col_lower in df.columns:
                df[time_col_lower] = pd.to_datetime(df[time_col_lower])
            elif time_col.upper() in df.columns:
                # 대문자 컬럼명이 있는 경우 처리
                df.rename(columns={time_col.upper(): time_col_lower}, inplace=True)
                df[time_col_lower] = pd.to_datetime(df[time_col_lower])
            elif 'date' in df.columns and time_col_lower == 'time':
                # 크립토 데이터에서 'time' 대신 'date'가 있는 경우
                df.rename(columns={'date': time_col_lower}, inplace=True)
                df[time_col_lower] = pd.to_datetime(df[time_col_lower])
            elif 'time' in df.columns and time_col_lower == 'date':
                # 주식 데이터에서 'date' 대신 'time'이 있는 경우
                df.rename(columns={'time': time_col_lower}, inplace=True)
                df[time_col_lower] = pd.to_datetime(df[time_col_lower])
            
            # ID 컬럼 처리
            id_col_lower = id_col.lower()
            name = os.path.splitext(os.path.basename(path))[0]
            df[id_col_lower] = name
            
            # 인덱스 설정
            if time_col_lower in df.columns:
                df = df.set_index(time_col_lower)
            else:
                print(f"⚠️ 시간 컬럼 '{time_col_lower}'를 찾을 수 없습니다: {path}")
            
            return df
        except Exception as e:
            print(f"❌ Error reading {path}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        dfs = list(pool.map(_read_csv, paths))
    
    # None 값 제거
    return [df for df in dfs if df is not None]

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
    
    # 결과 저장 경로 (results 폴더에 저장)
    from config import RESULTS_DIR
    INTEGRATED_RESULTS_PATH = os.path.join(RESULTS_DIR, 'integrated_results.csv')
    
    try:
        # 기술적 스크리닝 결과 로드
        if not os.path.exists(US_WITH_RS_PATH):
            print(f"⚠️ 기술적 스크리닝 결과 파일이 없습니다: {US_WITH_RS_PATH}")
            return
        
        tech_df = pd.read_csv(US_WITH_RS_PATH)
        print(f"✅ 기술적 스크리닝 결과 로드 완료: {len(tech_df)}개 종목")
        
        # 재무제표 스크리닝 결과 로드
        if not os.path.exists(ADVANCED_FINANCIAL_RESULTS_PATH):
            print(f"⚠️ 재무제표 스크리닝 결과 파일이 없습니다: {ADVANCED_FINANCIAL_RESULTS_PATH}")
            return
        
        fin_df = pd.read_csv(ADVANCED_FINANCIAL_RESULTS_PATH)
        print(f"✅ 재무제표 스크리닝 결과 로드 완료: {len(fin_df)}개 종목")
        
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
        
        ensure_dir(RESULTS_DIR)
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

if __name__ == '__main__':
    main()