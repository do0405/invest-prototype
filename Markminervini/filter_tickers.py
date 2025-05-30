import pandas as pd
import os
import sys

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))  

# 설정 파일 임포트
from config import RESULTS_DIR, US_WITH_RS_PATH

def filter_new_tickers():
    # 파일 경로 설정
    new_tickers_path = os.path.join(RESULTS_DIR, 'new_tickers.csv')
    us_with_rs_path = US_WITH_RS_PATH
    
    # 두 CSV 파일 읽기
    print(f"1. {new_tickers_path} 파일 읽는 중...")
    new_tickers_df = pd.read_csv(new_tickers_path)
    print(f"   - 원본 데이터: {len(new_tickers_df)} 행")
    
    print(f"2. {us_with_rs_path} 파일 읽는 중...")
    us_with_rs_df = pd.read_csv(us_with_rs_path)
    
    # us_with_rs.csv에 있는 심볼 목록 생성
    valid_symbols = set(us_with_rs_df['symbol'].values)
    print(f"3. us_with_rs.csv에서 {len(valid_symbols)}개의 유효한 심볼을 찾았습니다.")
    
    # new_tickers.csv에서 us_with_rs.csv에 있는 심볼만 유지
    filtered_df = new_tickers_df[new_tickers_df['symbol'].isin(valid_symbols)]
    print(f"4. 필터링 후 데이터: {len(filtered_df)} 행 (제거됨: {len(new_tickers_df) - len(filtered_df)} 행)")
    
    # 결과를 원래 파일에 저장
    # 필터링된 결과 저장
    filtered_df.to_csv(new_tickers_path, index=False)
    # JSON 파일 생성 추가
    json_path = new_tickers_path.replace('.csv', '.json')
    filtered_df.to_json(json_path, orient='records', indent=2, force_ascii=False)
    print(f"5. 필터링된 데이터를 {new_tickers_path}에 저장했습니다.")
    
    # 제거된 심볼 목록 출력
    removed_symbols = set(new_tickers_df['symbol'].values) - valid_symbols
    if removed_symbols:
        print("\n제거된 심볼 목록:")
        print(", ".join(sorted(removed_symbols)))

if __name__ == "__main__":
    print("new_tickers.csv에서 us_with_rs.csv에 없는 데이터 제거 시작...\n")
    filter_new_tickers()
    print("\n작업 완료!")