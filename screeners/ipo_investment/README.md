# 데이터 소스 연동 모듈

이 모듈은 외부 데이터 소스와의 연동을 담당하여 IPO 데이터, 기관 투자자 데이터 등을 실시간으로 수집하고 관리합니다.

## 주요 기능

### 1. IPO 데이터 수집 (`ipo_data_collector.py`)
- SEC EDGAR API를 통한 IPO 파일링 데이터 수집
- Yahoo Finance를 통한 IPO 종목 추가 정보 수집
- IPO 성과 분석 및 추적

### 2. 기관 투자자 데이터 수집 (`institutional_data_collector.py`)
- SEC 13F 파일링을 통한 기관 보유 현황 수집
- 기관 자금 흐름 분석
- 임원 거래 내역 추적
- 기관 매수/매도 패턴 분석

### 3. 통합 데이터 관리 (`data_manager.py`)
- 모든 데이터 소스의 통합 관리
- 캐싱 시스템으로 성능 최적화
- 데이터 상태 모니터링

## 사용법

### 기본 사용

```python
from data_sources.data_manager import DataManager

# 데이터 매니저 초기화
manager = DataManager()

# IPO 데이터 가져오기
ipo_data = manager.get_ipo_data(days_back=365)
print(f"수집된 IPO: {len(ipo_data)}개")

# 기관 투자자 데이터 가져오기
symbol = "AAPL"
holdings, flow_analysis = manager.get_institutional_data(symbol)
print(f"{symbol} 기관 보유: {len(holdings)}개 기관")

# 기관 연속 매수 확인
buying_streak = manager.check_institutional_buying_streak(symbol, min_days=3)
print(f"기관 연속 매수: {buying_streak}")
```

### IPO 분석

```python
# 특정 IPO 종목 분석
ipo_analysis = manager.get_ipo_analysis("RIVN")
print("IPO 분석 결과:")
print(f"- IPO 정보: {ipo_analysis['ipo_info']}")
print(f"- 성과: {ipo_analysis['performance']}")
print(f"- 기관 관심도: {ipo_analysis['institutional_interest']}")
```

### 캐시 관리

```python
# 데이터 매니저 사용 예시
manager.get_ipo_data(days_back=365)
```

## 스크리너에서 사용

### IPO Investment 스크리너

```python
from screeners.ipo_investment.screener import IPOInvestmentScreener

# 스크리너 실행 (자동으로 데이터 소스 연동)
screener = IPOInvestmentScreener()
results = screener.screen_ipo_investments()

# 결과에는 실제 기관 투자자 데이터가 포함됨
print(results[['symbol', 'institutional_interest', 'institutional_flow']])
```

### Momentum Signals 스크리너에서 기관 데이터 활용

```python
from data_sources.data_manager import DataManager

# 기관 매수 확인을 추가 필터로 사용
manager = DataManager()
for symbol in candidate_stocks:
    if manager.check_institutional_buying_streak(symbol, min_days=3):
        print(f"{symbol}: 기관 연속 매수 확인")
```

## 설정 및 환경변수

### 필요한 환경변수 (`.env` 파일)

```bash
# SEC API 설정
SEC_API_USER_AGENT="Your Company Name (your.email@company.com)"

# Alpha Vantage API (선택사항)
ALPHA_VANTAGE_API_KEY="your_api_key_here"

# Financial Modeling Prep API (선택사항)
FMP_API_KEY="your_api_key_here"

# 캐시 설정
CACHE_DIRECTORY="data/cache"
CACHE_EXPIRY_HOURS=6
```

### 데이터 소스 우선순위

1. **IPO 데이터**
   - 1순위: SEC EDGAR API
   - 2순위: Yahoo Finance

2. **기관 투자자 데이터**
   - 1순위: SEC 13F 파일링
   - 2순위: Yahoo Finance Institutional Holders
   - 3순위: 볼륨/가격 패턴 분석

## 성능 최적화

### 캐싱 전략
- IPO 데이터: 6시간 캐시
- 기관 데이터: 12시간 캐시
- 자동 캐시 정리: 24시간 이상 된 파일 삭제

### API 호출 제한
- Yahoo Finance: 0.1초 간격
- SEC API: 0.5초 간격
- 기타 API: 각 제공업체 정책 준수

## 에러 처리

### 일반적인 에러 상황
1. **API 호출 실패**: 캐시된 데이터 사용
2. **네트워크 오류**: 캐시된 데이터 사용
3. **데이터 형식 오류**: 로그 기록 후 건너뛰기

### 로그 확인
```python
import logging
logging.basicConfig(level=logging.INFO)

# 데이터 수집 과정의 로그가 출력됨
manager = DataManager()
ipo_data = manager.get_ipo_data()
```

## 확장 가능성

### 새로운 데이터 소스 추가
1. `data_sources/` 디렉토리에 새 수집기 모듈 생성
2. `DataManager` 클래스에 통합
3. 스크리너에서 활용

### 지원 예정 데이터 소스
- FINRA 데이터
- 옵션 플로우 데이터
- 소셜 미디어 센티먼트
- 뉴스 및 공시 데이터

## 문제 해결

### 자주 발생하는 문제

1. **ImportError**: `pip install -r requirements.txt` 실행
2. **API 키 오류**: `.env` 파일 설정 확인
3. **캐시 오류**: `data/cache` 디렉토리 권한 확인
4. **네트워크 오류**: 인터넷 연결 및 방화벽 설정 확인

### 디버깅
```python
# 로그 확인 예시
ipo_data = manager.get_ipo_data()
print(ipo_data.head())
```

이 모듈을 통해 실제 시장 데이터를 활용한 더욱 정확하고 실용적인 투자 스크리닝이 가능합니다.