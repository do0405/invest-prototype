# IPO 투자 전략 모듈

이 모듈은 IPO 투자 전략을 위한 데이터 수집, 분석, 스크리닝 기능을 제공합니다.

## 주요 기능

### 1. IPO 데이터 수집 (`ipo_data_collector.py`)
- SEC EDGAR API를 통한 IPO 데이터 수집
- Finance Calendars를 통한 IPO 일정 수집
- 다양한 데이터 소스 통합 및 중복 제거

### 2. 패턴 분석 (`pattern_analyzer.py`)
- IPO 베이스 패턴 분석
- IPO 브레이크아웃 패턴 분석
- 기술적 지표 기반 패턴 인식

### 3. 트랙 분석 (`track_analyzer.py`)
- Track1: 기본적 IPO 투자 조건 분석
- Track2: 고급 모멘텀 기반 분석
- 환경적 요인(VIX, 섹터 강도) 고려

### 4. 결과 처리 (`result_processor.py`)
- 스크리닝 결과 저장 (CSV, JSON)
- 빈 결과 파일 생성
- 결과 데이터 정렬 및 포맷팅

## 사용법

### IPO 데이터 수집

```python
from screeners.ipo_investment.ipo_data_collector import RealIPODataCollector

# IPO 데이터 수집기 초기화
collector = RealIPODataCollector()

# 모든 IPO 데이터 수집
result = collector.collect_all_ipo_data()
print(f"수집된 최근 IPO: {len(result.get('recent_ipos', []))}개")
print(f"수집된 예정 IPO: {len(result.get('upcoming_ipos', []))}개")
```

### IPO 스크리닝

```python
from screeners.ipo_investment.screener import run_ipo_investment_screening

# IPO 투자 전략 스크리닝 실행
results = run_ipo_investment_screening()
print(f"스크리닝 결과: {len(results)}개 종목")
```

## 데이터 소스

현재 지원되는 IPO 데이터 소스:

1. **SEC EDGAR API** - 가장 안정적인 소스
2. **Finance Calendars** - IPO 일정 정보
3. **Investpy** - 보조 데이터 소스

## 모듈 구조

```
ipo_investment/
├── screener.py           # 메인 스크리너
├── ipo_data_collector.py  # IPO 데이터 수집
├── pattern_analyzer.py    # 패턴 분석
├── track_analyzer.py      # 트랙 분석
├── result_processor.py    # 결과 처리
├── data_manager.py        # 데이터 관리
├── indicators.py          # 기술적 지표
└── data_sources/          # 데이터 소스 모듈
    ├── base_source.py
    ├── sec_edgar_source.py
    ├── finance_calendars_source.py
    └── ...
```

## 주의사항

- FMP API와 NASDAQ API는 인증 및 타임아웃 문제로 제거되었습니다
- 현재 SEC EDGAR API가 가장 안정적으로 작동합니다
- 데이터 수집 시 API 호출 제한을 준수합니다