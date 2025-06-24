
## HANDOVER_GUIDE.md 완전 업데이트

```markdown:c%3A%2FUsers%2FHOME%2FDesktop%2Finvest_prototype%2FHANDOVER_GUIDE.md
# 투자 스크리닝 및 포트폴리오 관리 시스템 인수인계 가이드

## 1. 시스템 개요

이 투자 스크리닝 및 포트폴리오 관리 시스템은 미국 주식에 대한 종합적인 투자 분석 도구입니다. Mark Minervini의 기술적 분석 기법과 재무제표 분석을 통합하여 투자 대상을 선별하고, 6가지 전략을 통해 롱/숏 포트폴리오를 구성 및 추적하는 자동화된 시스템입니다.

### 핵심 기능

1. **다층 스크리닝 시스템**
   - 기술적 분석 (Mark Minervini 기법)
   - 재무제표 분석 (9개 핵심 지표)
   - 통합 스크리닝 (기술적 + 재무적)
   - 변동성 스큐 분석 (옵션 데이터 기반)

2. **포트폴리오 관리**
   - 6가지 투자 전략 (트렌드, 밸류, 그로스, 퀄리티, 스몰캡, 디펜시브)
   - 롱/숏 포지션 관리
   - 일일 성과 추적 및 리밸런싱
   - 리스크 관리 (손절매, 포지션 사이징)

3. **백엔드 API 시스템**
   - JSON 데이터 제공
   - RESTful API 서버
   - 실시간 포트폴리오 모니터링

## 2. 시스템 아키텍처

### 모듈 구조

invest_prototype/
├── 📁 core/                    # 핵심 시스템
│   ├── main.py                 # 메인 실행 파일
│   ├── config.py               # 전역 설정
│   ├── utils.py                # 공통 유틸리티
│   ├── data_collector.py       # 데이터 수집
│   └── fill_business_days.py   # 영업일 데이터 채우기
├──
├── 📁 Markminervini/           # 스크리닝 엔진
│   ├── filter_stock.py         # 기술적 스크리닝
│   ├── advanced_financial.py   # 재무 스크리닝
│   ├── ticker_tracker.py       # 종목 추적
│   ├── pattern_detection.py    # 패턴 감지
├──
├── 📁 long_short_portfolio/    # 포트폴리오 시스템
│   ├── strategy1.py            # 트렌드 하이 모멘텀
│   ├── strategy2.py            # 밸류 모멘텀
│   ├── strategy3.py            # 그로스 모멘텀
│   ├── strategy4.py            # 퀄리티 모멘텀
│   ├── strategy5.py            # 스몰캡 모멘텀
│   ├── strategy6.py            # 디펜시브 모멘텀
│   ├── portfolio_integration.py # 포트폴리오 통합
├──
├── 📁 portfolio_management/    # 포트폴리오 관리
│   ├── portfolio_manager.py    # 포트폴리오 매니저
│   └── core/
│       └── performance_analyzer.py # 성과 분석기
├──
├── 📁 option_data_based_strategy/ # 옵션 전략
│   └── volatility_skew_screener.py # 변동성 스큐
├──
├── 📁 backend/                 # 백엔드 API
│   ├── api_server.py           # Flask API 서버
│   ├── api_utils.py            # API 유틸리티
│   └── json_backend_wrapper.py # 백엔드 래퍼
└──
└── 📁 data/                    # 데이터 저장소
├── us/                     # 미국 주식 원시 데이터
└── results/                # 분석 결과
└── ver2/               # 포트폴리오 결과


### 데이터 플로우

1. **데이터 수집** (`data_collector.py`)
   - Yahoo Finance API를 통한 주식 데이터 수집
   - 일일 가격, 거래량, 재무제표 데이터

2. **스크리닝 단계**
   - **1차**: 기술적 스크리닝 (Mark Minervini 기법)
   - **2차**: 재무제표 스크리닝 (9개 지표)
   - **3차**: 통합 스크리닝 (기술적 + 재무적)

3. **포트폴리오 구성**
   - 6가지 전략별 종목 선별
   - 롱/숏 포지션 결정
   - 포지션 사이징 및 리스크 관리

4. **성과 추적**
   - 일일 포트폴리오 성과 계산
   - 리밸런싱 신호 생성
   - JSON/CSV 형태로 결과 저장

## 3. 스크리닝 시스템 상세

### Mark Minervini 기술적 분석

**핵심 조건 (6개)**:
1. 현재가 > 150일 이동평균
2. 현재가 > 200일 이동평균
3. 150일 이동평균 > 200일 이동평균
4. 200일 이동평균 상승 추세
5. 현재가 ≥ 52주 최고가 × 0.75
6. 현재가 ≥ 52주 최저가 × 1.25

**상대강도(RS) 점수**:
- 3개월, 6개월, 9개월, 12개월 수익률 가중 평균
- S&P 500 대비 상대적 성과 측정
- 0-100 점수로 표준화

### 재무제표 분석 (9개 지표)

1. **성장성 지표**
   - 분기 EPS 성장률 ≥ 20%
   - 연간 EPS 성장률 ≥ 20%
   - EPS 성장 가속화 (최근 > 이전)
   - 분기 매출 성장률 ≥ 20%
   - 연간 매출 성장률 ≥ 20%
   - 분기 순이익 성장률 ≥ 20%
   - 연간 순이익 성장률 ≥ 20%

2. **수익성 지표**
   - 분기 영업이익률 개선
   - 연간 영업이익률 개선
   - ROE ≥ 15%

3. **안정성 지표**
   - 부채비율 ≤ 150%

### 통합 스코어링

```python
# 백분위 계산
rs_percentile = (rs_score 순위 / 전체 종목 수) × 100
fin_percentile = (재무 점수 순위 / 전체 종목 수) × 100
total_percentile = (rs_percentile + fin_percentile) / 2

# 최종 정렬 기준
1. fin_met_count (재무 조건 충족 수) 내림차순
2. total_percentile 내림차순
3. rs_score 내림차순

## 4. 포트폴리오 관리 시스템
### 6가지 투자 전략
1. Strategy 1: 트렌드 하이 모멘텀 롱
   
   - 강한 상승 추세 + 높은 모멘텀
   - RS 점수 80 이상, 기술적 조건 5개 이상
2. Strategy 2: 밸류 모멘텀 롱
   
   - 저평가 + 모멘텀 전환
   - PER < 25, PBR < 3, 재무 조건 7개 이상
3. Strategy 3: 그로스 모멘텀 롱
   
   - 고성장 + 지속 가능성
   - EPS 성장률 30% 이상, 매출 성장률 25% 이상
4. Strategy 4: 퀄리티 모멘텀 롱
   
   - 고품질 기업 + 모멘텀
   - ROE 20% 이상, 부채비율 100% 이하
5. Strategy 5: 스몰캡 모멘텀 롱
   
   - 소형주 + 강한 모멘텀
   - 시가총액 100억 달러 이하
6. Strategy 6: 디펜시브 모멘텀 롱
   
   - 방어적 섹터 + 안정적 성장


포트폴리오 구성 원칙

# 포지션 사이징
max_positions = 20          # 최대 포지션 수
position_size = 5%          # 개별 포지션 크기
max_sector_weight = 25%     # 섹터별 최대 비중

# 리스크 관리
stop_loss = -8%            # 손절매 기준
take_profit = 25%          # 익절 기준
max_drawdown = -15%        # 최대 낙폭 제한

# 리밸런싱
rebalance_frequency = 'weekly'  # 주간 리밸런싱
threshold = 2%             # 리밸런싱 임계값

## 5. 백엔드 API 시스템
### API 엔드포인트

# 스크리닝 결과
GET /api/screening-results
# 포트폴리오 성과
GET /api/portfolio-performance  
# 전략별 결과
GET /api/strategy-results
# 종목별 상세 정보
GET /api/stock-details/{symbol}

JSON 데이터 구조

{
  "screening_results": {
    "technical": {
      "count": 150,
      "top_performers": [...]
    },
    "financial": {
      "count": 89,
      "high_quality": [...]
    },
    "integrated": {
      "count": 45,
      "final_candidates": [...]
    }
  },
  "portfolio_performance": {
    "total_return": 0.1234,
    "daily_return": 0.0056,
    "sharpe_ratio": 1.45,
    "max_drawdown": -0.0823,
    "positions": [...]
  }
}

## 6. 운영 가이드
### 일일 운영 프로세스
1. 데이터 수집 (장 마감 후)

python main.py --collect-only

2. 스크리닝 실행 (저녁)

python main.py --integrated

3. 포트폴리오 업데이트 (저녁)

cd long_short_portfolio
python portfolio_integration.py

성과 모니터링 (다음날 아침)

python backend/api_server.py
# 브라우저에서 http://localhost:5000/api/portfolio-performance 확인

### 주간 운영 프로세스
1. 포트폴리오 리밸런싱
   
   - 성과 분석 및 포지션 조정
   - 신규 종목 추가/제거
   - 섹터 비중 조정
2. 전략 성과 검토
   
   - 6가지 전략별 성과 분석
   - 저성과 전략 원인 분석
   - 파라미터 조정 검토
3. 리스크 관리
   
   - 최대 낙폭 점검
   - 포지션 사이징 재검토
   - 상관관계 분석
### 월간 운영 프로세스
1. 전략 백테스팅
   
   - 과거 데이터로 전략 성과 검증
   - 파라미터 최적화
   - 새로운 전략 아이디어 테스트
2. 시스템 업데이트
   
   - 라이브러리 버전 업데이트
   - 새로운 지표 추가
   - 코드 최적화