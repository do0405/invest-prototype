# 투자 프로토타입

이 프로젝트는 주식 투자를 위한 데이터 수집 및 분석 도구입니다.

## 주요 기능

### 1. RS 점수 계산 (`rs_calculator.py`)
- 주식의 상대 강도(Relative Strength) 점수를 계산
- 3개월, 6개월, 9개월, 12개월 수익률을 고려
- 결과는 `us_with_rs.csv`에 저장

### 2. 고급 재무 스크리닝 (`advanced_financial_screener.py`)
- `us_with_rs.csv`의 종목들에 대해 재무 데이터 수집 및 분석
- 주요 재무 지표:
  - 분기/연간 EPS 성장률 (≥20%)
  - EPS 성장 가속화 여부
  - 분기/연간 매출 성장률 (≥20%)
  - 분기/연간 영업이익률 개선 여부
  - 분기/연간 순이익 성장률 (≥20%)
  - ROE (≥15%)
  - 부채비율 (≤150%)
- 각 지표별 백분위 계산 (us_with_rs.csv의 종목들끼리 비교)
- RS 점수와 재무 지표를 결합한 종합 점수 계산
- 결과는 `results2/advanced_financial_results.csv`에 저장

### 3. 메인 실행 파일 (`main.py`)
- `--rs-only`: RS 점수 계산만 실행
- `--financial-only`: 재무 스크리닝만 실행
- 옵션 없이 실행: RS 점수 계산 후 재무 스크리닝 실행

## 실행 방법

1. RS 점수 계산만 실행:
```bash
python main.py --rs-only
```

2. 재무 스크리닝만 실행:
```bash
python main.py --financial-only
```

3. 데이터 수집을 제외한 모든 과정 순차 실행:
```bash
python main.py --process-only
```

4. 전체 프로세스 실행:
```bash
python main.py
```

## 결과 파일

- `us_with_rs.csv`: RS 점수가 포함된 종목 목록
- `results2/advanced_financial_results.csv`: 재무 스크리닝 결과
  - symbol: 종목 코드
  - fin_met_count: 충족한 재무 지표 수 (최대 11개)
  - has_error: 데이터 수집 오류 여부
  - rs_score: RS 점수
  - rs_percentile: RS 점수 백분위
  - fin_percentile: 재무 지표 백분위
  - total_percentile: 종합 백분위

## 정렬 기준

결과는 다음 기준으로 정렬됩니다:
1. fin_met_count가 11인 종목 우선 (모든 재무 조건 충족)
2. total_percentile (내림차순)
3. rs_score (내림차순)

## 주의사항

- Yahoo Finance API를 사용하여 데이터를 수집하므로 인터넷 연결이 필요합니다.
- API 호출 제한으로 인해 대량의 데이터 수집 시 시간이 소요될 수 있습니다.
- 일부 종목의 경우 데이터 수집이 실패할 수 있으며, 이는 has_error 컬럼에 표시됩니다.
- 재무 데이터는 분기별/연간 성장률과 개선 여부를 중심으로 평가됩니다.