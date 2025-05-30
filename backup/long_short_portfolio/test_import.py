import sys
import os

# 현재 디렉토리 출력
print("Current directory:", os.getcwd())

# 상위 디렉토리를 Python 경로에 추가
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)
print("Added to Python path:", parent_dir)

try:
    # utils 모듈 임포트
    import utils
    print("utils 모듈 임포트 성공")
    
    # process_stock_data 함수 확인
    if 'process_stock_data' in dir(utils):
        print("process_stock_data 함수 존재함")
    else:
        print("process_stock_data 함수가 없음")
        print("사용 가능한 함수들:", [f for f in dir(utils) if not f.startswith('_') and callable(getattr(utils, f))])
        
except Exception as e:
    import traceback
    print("오류 발생:", str(e))
    print(traceback.format_exc())