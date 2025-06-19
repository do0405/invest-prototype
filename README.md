# 투자 스크리닝 및 포트폴리오 관리 시스템

이 프로젝트는 미국 주식에 대한 종합적인 투자 분석 도구로, 기술적 분석과 재무제표 분석을 통합하여 투자 대상을 선별하고 포트폴리오를 생성 및 추적하는 자동화된 시스템입니다.

## 🎯 주요 기능

### 1. 스크리닝 시스템
- **기술적 스크리닝**: Mark Minervini 기법 기반 상대강도(RS) 점수 계산
- **재무제표 스크리닝**: EPS, 매출, 영업이익률 등 11개 재무 지표 분석
- **통합 스크리닝**: 기술적 분석과 재무 분석을 결합한 종합 평가
- **변동성 스큐 스크리닝**: 옵션 데이터 기반 변동성 스큐 역전 전략

### 2. 포트폴리오 관리
- **6가지 투자 전략**: 트렌드 하이 모멘텀, 밸류 모멘텀, 그로스 모멘텀 등
- **롱/숏 포트폴리오**: 상승 종목 매수, 하락 종목 공매도 전략
- **포트폴리오 추적**: 일일 성과 분석 및 리밸런싱
- **리스크 관리**: 포지션 크기 조절 및 손절매 관리

### 3. 백엔드 API
- **JSON 데이터 제공**: 모든 스크리닝 결과를 JSON 형태로 제공
- **RESTful API**: Flask 기반 웹 API 서버
- **실시간 데이터**: 포트폴리오 성과 및 스크리닝 결과 실시간 조회

## 🏗️ 시스템 아키텍처

```
invest_prototype/
├── config.py                   # 전역 설정
├── main.py                     # 메인 실행 파일
├── utils/                      # 유틸리티 모듈 모음
├── data_collector.py           # 데이터 수집
├── screeners/                  # 스크리너 모듈
│   ├── markminervini/
│   ├── qullamaggie/
│   ├── us_gainer/
│   ├── us_setup/
│   └── option_volatility/
├── portfolio/
│   ├── long_short/             # 전략 스크립트
│   └── manager/                # 포트폴리오 관리 로직
├── backend/                    # Flask API 서버
├── data/
│   └── us/
└── results/
    ├── screeners/
    └── portfolio/

```



## 🚀 실행 방법

### 기본 스크리닝 실행
```bash
# 전체 프로세스 실행 (데이터 수집 + 스크리닝)
python main.py

# 기술적 스크리닝만 실행
python main.py --screen-only

# 재무제표 스크리닝만 실행
python main.py --financial-only

# 통합 스크리닝 실행
python main.py --integrated

### 포트폴리오 관리
```bash
# 포트폴리오 스크리너 실행
cd portfolio/long_short
python run_screener.py

# 개별 전략 실행
python strategy1.py  # 트렌드 하이 모멘텀
python strategy2.py  # 밸류 모멘텀
# ... strategy6.py까지

# 포트폴리오 통합 관리
python portfolio_integration.py
```

### 백엔드 API 서버
```bash
cd backend
python api_server.py
# 주요 엔드포인트
# GET http://localhost:5000/api/screening-results
# GET http://localhost:5000/api/portfolio-performance
# GET http://localhost:5000/api/strategy-results
```
## 📊 스크리닝 기준
### 기술적 분석 (Mark Minervini 기법)
- 현재가 > 150일/200일 이동평균
- 150일 이동평균 > 200일 이동평균
- 200일 이동평균 상승 추세 (1개월 전 대비)
- 현재가가 52주 최고가의 75% 이상
- 현재가가 52주 최저가의 125% 이상
- 상대강도(RS) 점수 70 이상
### 재무제표 분석 (11개 지표)
1. EPS 성장률 : 분기/연간 20% 이상
2. EPS 가속화 : 최근 분기 성장률 > 이전 분기
3. 매출 성장률 : 분기/연간 20% 이상
4. 영업이익률 개선 : 분기/연간 개선 여부
5. 순이익 성장률 : 분기/연간 20% 이상
6. ROE : 15% 이상
7. 부채비율 : 150% 이하
### 포트폴리오 전략
1. Strategy 1 : 트렌드 하이 모멘텀 롱
2. Strategy 2 : 밸류 모멘텀 롱
3. Strategy 3 : 그로스 모멘텀 롱
4. Strategy 4 : 퀄리티 모멘텀 롱
5. Strategy 5 : 스몰캡 모멘텀 롱
6. Strategy 6 : 디펜시브 모멘텀 롱
## 📈 결과 파일
### CSV & JSON 형태로 이중 저장
- results/us_with_rs.csv/.json : 기술적 스크리닝 결과
- results/advanced_financial_results.csv/.json : 재무 스크리닝 결과
- results/integrated_results.csv/.json : 통합 스크리닝 결과
- results/portfolio/buy/strategyX_results.csv/.json : 전략별 매수 신호
- results/portfolio/sell/strategyX_results.csv/.json : 전략별 매도 신호
- results/portfolio/portfolio_integration_report.csv/.json : 포트폴리오 통합 보고서
### 주요 결과 지표
- rs_score : 상대강도 점수 (0-100)
- fin_met_count : 충족한 재무 조건 수 (0-11)
- total_percentile : 종합 백분위 점수
- portfolio_weight : 포트폴리오 내 비중
- expected_return : 예상 수익률
- risk_score : 리스크 점수
## 🔧 설정 및 커스터마이징
### config.py 주요 설정

# 스크리닝 기준값
RS_THRESHOLD = 70
EPS_GROWTH_THRESHOLD = 0.20
ROE_THRESHOLD = 0.15
DEBT_RATIO_THRESHOLD = 1.50

# 포트폴리오 설정
MAX_POSITIONS = 20
POSITION_SIZE = 0.05  # 5%
STOP_LOSS = -0.08     # -8%
TAKE_PROFIT = 0.25    # 25%

📋 의존성

pip install pandas numpy yfinance requests flask flask-cors scipy pytz

## 🎯 사용 시나리오
### 1. 일일 스크리닝

# 매일 장 마감 후 실행
python main.py --integrated
cd portfolio/long_short && python run_screener.py

2. 포트폴리오 모니터링

# 포트폴리오 성과 추적
python portfolio_integration.py

# API를 통한 실시간 모니터링
python backend/api_server.py
