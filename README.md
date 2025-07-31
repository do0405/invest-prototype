# 투자 스크리닝 및 포트폴리오 관리 시스템

이 프로젝트는 미국 주식에 대한 종합적인 투자 분석 도구로, 기술적 분석과 재무제표 분석을 통합하여 투자 대상을 선별하고 포트폴리오를 생성 및 추적하는 자동화된 시스템입니다.

## 🎯 주요 기능

### 1. 스크리닝 시스템
- **기술적 스크리닝**: Mark Minervini 기법 기반 상대강도(RS) 점수 계산
- **재무제표 스크리닝**: EPS, 매출, 영업이익률 등 9개 재무 지표 분석
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

### 환경 설정

#### 1. 환경 변수 설정
```bash
# .env.example을 .env로 복사하고 설정 값을 수정하세요
cp .env.example .env
```

주요 환경 변수:
- `BACKEND_PORT`: 백엔드 서버 포트 (기본값: 5000)
- `FRONTEND_PORT`: 프론트엔드 서버 포트 (기본값: 3000)
- `BACKEND_URL`: 백엔드 서버 URL (기본값: http://localhost:5000)
- `NODE_ENV`: Node.js 환경 (development/production)
- `FLASK_ENV`: Flask 환경 (development/production)
- `SEC_API_USER_AGENT`: SEC API 사용자 에이전트
- `CACHE_DIRECTORY`: 캐시 디렉토리 경로

#### 2. 개발 환경 실행

**Windows:**
```bash
# 개발 서버 자동 시작 (백엔드 + 프론트엔드)
scripts\start-dev.bat
```

**Linux/Mac:**
```bash
# 실행 권한 부여
chmod +x scripts/start-dev.sh

# 개발 서버 자동 시작
./scripts/start-dev.sh
```

#### 3. 프로덕션 배포 (Docker)

**Windows:**
```bash
scripts\deploy.bat
```

**Linux/Mac:**
```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh
```

### 기본 스크리닝 실행
```bash
# 전체 프로세스 실행 (데이터 수집 + 스크리닝)
py main.py  # 실행 시 us_with_rs.csv 파일이 자동 갱신됩니다
# 데이터 수집을 건너뛰는 경우에도 동일
py main.py --skip-data

# 특정 스크리너 실행 예시
py main.py --task setup

# 포트폴리오 관리만 실행
py main.py --task portfolio --skip-data
```

### 스케줄러 사용
```bash
# `--skip-data` 스크리너가 끝날 때마다 1분 후 다시 실행하며,
# 한국 시각 14:30 이후 첫 실행이 완료되면 1분 뒤에 전체 모드를 한 번 수행합니다.
py main.py --schedule

# 간단한 유지용 실행을 수동으로 하려면
py main.py --task screening --skip-data
```

### 포트폴리오 관리
```bash
# 개별 전략 실행
py strategy1.py  # 트렌드 하이 모멘텀
py strategy2.py  # 밸류 모멘텀
# ... strategy6.py까지

# 포트폴리오 통합 관리
py portfolio_integration.py
```

### 백엔드 API 서버
```bash
# 개별 실행 (환경 변수 자동 로드)
cd backend
py api_server.py

# 또는 환경 변수와 함께 실행
BACKEND_PORT=5000 FLASK_ENV=development py api_server.py
```

주요 엔드포인트:
- `GET http://localhost:{BACKEND_PORT}/api/screening-results`
- `GET http://localhost:{BACKEND_PORT}/api/portfolio-performance`
- `GET http://localhost:{BACKEND_PORT}/api/strategy-results`

### 프론트엔드 웹 애플리케이션
```bash
# 개발 모드
cd frontend
npm run dev

# 프로덕션 빌드
npm run build
npm start
```

웹 인터페이스: `http://localhost:{FRONTEND_PORT}`
각 스크리너 API는 `last_updated` 필드로 데이터 파일의 수정 시간을 함께 반환하므로
프론트엔드에서 최신 여부를 쉽게 확인할 수 있습니다. 이 시간은 각 스크리닝
작업이 완료된 시각을 기준으로 합니다.
### 주식 메타데이터 수집
`leader_stock`과 `momentum_signals` 스크리너는 섹터, PER, 매출 성장률 등
기본 메타데이터가 포함된 `data/stock_metadata.csv` 파일을 사용합니다.
메인 프로그램에서 데이터 수집 시 자동으로 생성되며, 필요 시 다음
명령으로 개별 실행할 수 있습니다.

```bash
py data_collectors/stock_metadata_collector.py
```

파일 경로는 `config.STOCK_METADATA_PATH` 설정을 따릅니다.
## 📊 스크리닝 기준
### 기술적 분석 (Mark Minervini 기법)
- 현재가 > 150일/200일 이동평균
- 150일 이동평균 > 200일 이동평균
- 200일 이동평균 상승 추세 (1개월 전 대비)
- 현재가가 52주 최고가의 75% 이상
- 현재가가 52주 최저가의 125% 이상
- 상대강도(RS) 점수 70 이상
### 재무제표 분석 (9개 지표)
1. 연간 EPS 성장률 20% 이상
2. 분기별 EPS 가속화(최근 4개 중 3분기 이상 상승)
3. 연간 매출 성장률 15% 이상
4. 분기별 매출 가속화(최근 4개 중 3분기 이상 상승)
5. 순이익률(Net Margin) 개선
6. EPS 3분기 연속 가속화
7. 매출 3분기 연속 가속화
8. 순이익률 3분기 연속 가속화
9. 부채비율 150% 이하
### 포트폴리오 전략
1. Strategy 1 : 트렌드 하이 모멘텀 롱
2. Strategy 2 : 밸류 모멘텀 롱
3. Strategy 3 : 그로스 모멘텀 롱
4. Strategy 4 : 퀄리티 모멘텀 롱
5. Strategy 5 : 스몰캡 모멘텀 롱
6. Strategy 6 : 디펜시브 모멘텀 롱
## 📈 결과 파일
### CSV & JSON 형태로 이중 저장
- results/screeners/markminervini/us_with_rs.csv/.json : 기술적 스크리닝 결과
- results/screeners/markminervini/advanced_financial_results.csv/.json : 재무 스크리닝 결과
- results/screeners/markminervini/integrated_results.csv/.json : 통합 스크리닝 결과
- results/option/volatility_skew_screening_YYYYMMDD.csv/.json : 옵션 변동성 스크리닝 결과
- results/momentum_signals/momentum_signals_YYYYMMDD.csv : 상승 모멘텀 시그널 결과
- results/portfolio/buy/strategyX_results.csv/.json : 전략별 매수 신호
- results/portfolio/sell/strategyX_results.csv/.json : 전략별 매도 신호
- results/portfolio/portfolio_integration_report.csv/.json : 포트폴리오 통합 보고서
### 주요 결과 지표
- rs_score : 상대강도 점수 (0-100)
- fin_met_count : 충족한 재무 조건 수 (0-9)
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

## 📋 의존성

### Python 패키지
```bash
# requirements.txt를 통한 설치
pip install -r requirements.txt
```

주요 패키지:
- `pandas`, `numpy`: 데이터 처리
- `yfinance`: 주식 데이터 수집
- `flask`, `flask-cors`: 백엔드 API 서버
- `python-dotenv`: 환경 변수 관리
- `scipy`, `pytz`: 과학 계산 및 시간대 처리

### Node.js 패키지 (프론트엔드)
```bash
cd frontend
npm install
```

### Docker (선택사항)
- Docker Desktop 또는 Docker Engine
- Docker Compose

### 환경 요구사항
- Python 3.9+
- Node.js 18+
- npm 또는 yarn

## 🎯 사용 시나리오
### 1. 일일 스크리닝

# 매일 장 마감 후 실행
py main.py --integrated

2. 포트폴리오 모니터링

# 포트폴리오 성과 추적
py portfolio_integration.py

# API를 통한 실시간 모니터링
py backend/api_server.py
