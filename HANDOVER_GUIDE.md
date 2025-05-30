# 투자 스크리닝 시스템 인수인계 가이드

## 1. 시스템 개요

이 투자 스크리닝 시스템은 미국 주식 및 암호화폐(크립토)에 대한 기술적 분석과 재무제표 분석을 통합하여 투자 대상을 선별하는 자동화된 도구입니다. 시스템은 모듈식 아키텍처로 설계되어 있어 각 기능을 독립적으로 실행하거나 통합하여 사용할 수 있습니다.

### 주요 기능

1. **데이터 수집**: 미국 주식 및 크립토 데이터를 자동으로 수집
2. **기술적 분석**: 상대강도(RS) 점수 등 기술적 지표 기반 스크리닝
3. **재무제표 분석**: EPS 성장률, 매출 성장률, 영업이익률 등 재무 지표 기반 스크리닝
4. **통합 스크리닝**: 기술적 분석과 재무제표 분석을 결합한 종합 평가
5. **자동화 스케줄링**: 정해진 시간에 자동으로 데이터 수집 및 분석 실행

## 2. 시스템 아키텍처

### 디렉토리 구조

```
invest_prototype/
├── main.py                  # 메인 실행 파일
├── config.py                # 설정 파일
├── utils.py                 # 유틸리티 함수
├── data_collector.py        # 데이터 수집 모듈
├── screener.py              # 기술적 분석 스크리닝 모듈
├── financial_screener.py    # 기본 재무제표 스크리닝 모듈
├── simple_screener.py       # 간단한 재무제표 스크리닝 모듈
├── advanced_financial_screener.py  # 고급 재무제표 스크리닝 모듈
├── data/                    # 데이터 저장 디렉토리
│   ├── us/                  # 미국 주식 데이터
│   ├── kraken/              # 크립토 데이터
│   ├── financial/           # 재무제표 데이터
│   └── results/             # 분석 결과
├── results2/                # 추가 분석 결과
└── backup/                  # 백업 데이터
```

### 주요 모듈 설명

1. **main.py**: 전체 시스템의 진입점으로, 명령행 인터페이스를 통해 다양한 모드로 시스템을 실행할 수 있습니다.

2. **config.py**: 시스템 전체에서 사용되는 경로 설정 및 스크리닝 기준값을 정의합니다.

3. **data_collector.py**: 미국 주식 및 크립토 데이터를 수집하는 모듈입니다.

4. **screener.py**: 기술적 분석 기반 스크리닝을 수행하는 모듈로, 상대강도(RS) 점수 등을 계산합니다.

5. **financial_screener.py**: 기본적인 재무제표 스크리닝을 수행하는 모듈입니다.

6. **simple_screener.py** / **advanced_financial_screener.py**: 보다 상세한 재무제표 분석을 수행하는 모듈로, yfinance API를 활용하여 실시간 재무 데이터를 수집하고 분석합니다.

## 3. 재무제표 스크리닝 기능

### 분석 지표

재무제표 스크리닝은 다음과 같은 주요 지표를 분석합니다:

1. **EPS(주당순이익) 성장률**
   - 분기별 EPS 성장률 (≥ +20%)
   - 연간 EPS 성장률 (≥ +20%)
   - EPS 성장 가속화 여부

2. **매출 성장률**
   - 분기별 매출 성장률 (≥ +20%)
   - 연간 매출 성장률 (≥ +20%)

3. **영업이익률**
   - 분기별 영업이익률 개선 여부
   - 연간 영업이익률 개선 여부

4. **순이익 성장률**
   - 분기별 순이익 성장률 (≥ +20%)
   - 연간 순이익 성장률 (≥ +20%)

5. **ROE(자기자본이익률)** (≥ 15%)

6. **부채비율** (≤ 150%)

### 데이터 수집 방식

재무제표 데이터는 `yfinance` 라이브러리를 사용하여 수집합니다. 이 라이브러리는 Yahoo Finance에서 제공하는 데이터를 파이썬으로 쉽게 가져올 수 있게 해줍니다. 주요 데이터 소스는 다음과 같습니다:

- `ticker.quarterly_income_stmt`: 분기별 손익계산서
- `ticker.income_stmt`: 연간 손익계산서
- `ticker.balance_sheet`: 대차대조표
- `ticker.quarterly_earnings`: 분기별 실적
- `ticker.earnings`: 연간 실적

## 4. 시스템 실행 방법

### 기본 실행

전체 프로세스(데이터 수집, 기술적 분석, 재무제표 분석, 통합 분석)를 실행하려면:

```bash
python main.py
```

### 특정 모듈만 실행

1. **데이터 수집만 실행**:
   ```bash
   python main.py --collect-only
   ```

2. **기술적 스크리닝만 실행**:
   ```bash
   python main.py --screen-only
   ```

3. **재무제표 스크리닝만 실행**:
   ```bash
   python main.py --financial-only
   ```

4. **통합 스크리닝 실행**:
   ```bash
   python main.py --integrated
   ```

### 특정 자산 클래스만 처리

1. **미국 주식만 처리**:
   ```bash
   python main.py --us-only
   ```

2. **크립토만 처리**:
   ```bash
   python main.py --kraken-only
   ```

### 스케줄러 설정

매일 정해진 시간에 자동으로 데이터 수집 및 분석을 실행하려면:

```bash
python main.py --schedule --collect-hour 1 --screen-hour 2
```

## 5. 고급 재무제표 스크리닝 실행

고급 재무제표 스크리닝은 별도의 모듈로 구현되어 있으며, 다음과 같이 실행할 수 있습니다:

```bash
python advanced_financial_screener.py
```

또는 간단한 버전을 실행하려면:

```bash
python simple_screener.py
```

## 6. 결과 해석

### 결과 파일

분석 결과는 다음 위치에 저장됩니다:

1. **기술적 분석 결과**: `data/results/us_with_rs.csv` 및 `data/results/kraken_with_rs.csv`
2. **재무제표 분석 결과**: `data/results/financial_results.csv`
3. **통합 분석 결과**: `data/results/integrated_results.csv`
4. **고급 재무제표 분석 결과**: `results2/advanced_financial_results.csv` 또는 `results2/financial_screening_results.csv`

### 결과 해석 방법

각 결과 파일에는 다음과 같은 정보가 포함되어 있습니다:

1. **기술적 분석 결과**:
   - `rs_score`: 상대강도 점수 (높을수록 강세)
   - `met_count`: 충족된 기술적 조건 수

2. **재무제표 분석 결과**:
   - 각 재무 지표 값 (EPS 성장률, 매출 성장률 등)
   - `cond1` ~ `cond11`: 각 조건 충족 여부 (True/False)
   - `met_count` 또는 `fin_met_count`: 충족된 재무 조건 수

3. **통합 분석 결과**:
   - `total_met_count`: 기술적 조건과 재무 조건을 합한 총 충족 조건 수

## 7. 시스템 확장 및 커스터마이징

### 스크리닝 기준 변경

스크리닝 기준을 변경하려면 `config.py` 파일의 `FINANCIAL_CRITERIA` 딕셔너리를 수정하거나, 각 스크리닝 모듈 내의 기준값을 직접 수정할 수 있습니다.

### 새로운 지표 추가

새로운 재무 지표를 추가하려면 다음 단계를 따르세요:

1. 해당 스크리닝 모듈(예: `advanced_financial_screener.py`)에서 데이터 수집 함수에 새 지표 계산 로직 추가
2. 스크리닝 함수에 새 조건 추가
3. 필요한 경우 `config.py`에 새 기준값 추가

## 8. 문제 해결

### 일반적인 오류

1. **데이터 수집 오류**: 네트워크 연결 확인 및 API 제한 고려
2. **yfinance API 오류**: 라이브러리 버전 업데이트 또는 API 변경사항 확인
3. **파일 경로 오류**: 디렉토리 구조 및 권한 확인

### 로깅 및 디버깅

각 모듈은 실행 중 상세한 로그를 출력합니다. 문제 해결을 위해 이 로그를 검토하세요.

## 9. 의존성 및 요구사항

### 필수 라이브러리

- pandas: 데이터 처리
- numpy: 수치 계산
- yfinance: 재무 데이터 수집
- schedule: 자동화 스케줄링 (선택적)

### 설치 방법

```bash
pip install pandas numpy yfinance schedule
```

## 10. 결론

이 투자 스크리닝 시스템은 기술적 분석과 재무제표 분석을 결합하여 투자 대상을 체계적으로 선별할 수 있는 강력한 도구입니다. 모듈식 설계로 인해 필요에 따라 특정 기능만 사용하거나 전체 시스템을 통합하여 사용할 수 있습니다. 또한 스크리닝 기준을 쉽게 조정하여 자신만의 투자 전략에 맞게 커스터마이징할 수 있습니다.