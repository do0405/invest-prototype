# VCP 및 Cup-with-Handle 패턴 탐지 모듈

## 🛠️ 목적
사전에 필터링된 티커들의 로컬 OHLCV 데이터(CSV, 300영업일 이상)에 대해 VCP 또는 Cup-with-Handle 패턴을 룰 기반으로 식별하고, 기계학습을 전혀 사용하지 않고 수식 기반의 신뢰도 점수를 계산하는 분석 파이프라인입니다.

## 📌 전체 분석 절차

### 1. 룰 기반 패턴 후보 탐색
- **VCP 패턴 후보**: `cookstock` 오픈소스의 `detect_vcp(df)` 함수 사용
- **Cup-with-Handle 패턴 후보**: `canslimTechnical`의 `cup_with_handle(df)` 함수 사용
- **출력**: 각 패턴별 후보 구간 (start_idx, end_idx) 리스트

### 2. 거래량 수축 판단 (볼륨 필터)
- 각 후보 구간에 대해 거래량 시계열에서 피크를 추출: `scipy.signal.find_peaks`
- 피크의 선형 회귀 기울기(slope)와 상관계수(r)를 계산: `scipy.stats.linregress`
- **수축 판단 조건**: `slope < 0` and `abs(r) > 0.5`

### 3. 신뢰도 점수 계산 (ML 없이 확률 흉내)
아래 수식으로 0~1 사이의 신뢰도 점수 반환:

```python
def score_pattern(vol, high, low, breakout_vol):
    from scipy.stats import linregress
    slope, _, r, _, _ = linregress(range(len(vol)), vol)
    contraction_ratio = (max(high) - min(low)) / (high[-1] - low[-1] + 1e-6)
    volume_spike = breakout_vol / (vol[-5:].mean() + 1e-6)

    score = (
        (abs(r) if slope < 0 else 0) * 0.4 +             # 볼륨 수축 일관성
        (min(1, contraction_ratio / 2.5)) * 0.3 +         # 수축 폭 비율
        (min(1, volume_spike / 2)) * 0.3                  # 돌파 직전 볼륨 스파이크
    )
    return round(score, 3)
```

- **기준선**: `score >= 0.75` 이면 신뢰도 높은 패턴(True)

### 4. 결과 저장
- 종목명, 패턴 종류, 날짜, 점수, 신뢰도 기준 통과 여부(True/False)를 포함한 CSV 저장
- **출력 예시 컬럼**: ['ticker', 'pattern', 'pivot_date', 'score', 'signal']

## 📦 설치 및 사용법

### 1. 의존성 설치
```bash
pip install -r requirements_pattern.txt
```

### 2. 데이터 준비
- OHLCV 데이터가 포함된 CSV 파일들을 `./data/` 폴더에 준비
- CSV 파일 형식: `open`, `high`, `low`, `close`, `volume` 컬럼 필수
- 최소 300영업일 이상의 데이터 필요

### 3. 실행
```python
from pattern_detection import main

# 기본 실행
main()

# 사용자 정의 경로
main(
    data_folder='./your_data_folder',
    output_file='./your_results.csv'
)
```

### 4. 개별 함수 사용
```python
from pattern_detection import (
    detect_vcp_candidates,
    detect_cup_candidates,
    is_contracting_volume,
    score_pattern,
    analyze_single_stock
)

# 단일 종목 분석
results = analyze_single_stock('./data/AAPL.csv')

# VCP 패턴만 탐지
import pandas as pd
df = pd.read_csv('./data/AAPL.csv')
vcp_candidates = detect_vcp_candidates(df)
```

## 📊 출력 결과 예시

```csv
ticker,pattern,pivot_date,score,signal
AAPL,VCP,2024-01-15,0.856,True
MSFT,Cup-with-Handle,2024-01-12,0.782,True
GOOGL,VCP,2024-01-10,0.734,False
TSLA,Cup-with-Handle,2024-01-08,0.691,False
```

## ⚙️ 구현 특징

- **모든 함수는 모듈화**: `detect_vcp_candidates()`, `detect_cup_candidates()`, `is_contracting_volume()`, `score_pattern()` 등
- **로컬 폴더 내 OHLCV CSV 일괄 분석**
- **사용자 입력 최소화**: 파라미터는 상단 상수 정의
- **실행 시 하나의 main() 함수에서 전체 파이프라인 수행**
- **최종 출력은 ./results.csv**

## 🎯 목표

- **기계학습 없이도 precision 약 85~90% 수준 도달**
- **종목별/패턴별 신뢰도 점수 출력**
- **추후 시각화 모듈 (예: matplotlib, plotly) 연결 가능하도록 구조화**

## 🔧 문제 해결

### cookstock 또는 canslimTechnical 라이브러리 설치 오류
```bash
# GitHub에서 직접 설치
pip install git+https://github.com/shiyu2011/cookstock.git
pip install git+https://github.com/kanwalpreet18/canslimTechnical.git
```

### 데이터 형식 오류
- CSV 파일에 `open`, `high`, `low`, `close`, `volume` 컬럼이 모두 있는지 확인
- 컬럼명은 대소문자 구분하지 않음 (자동으로 소문자로 변환)
- 최소 300행 이상의 데이터가 필요

### 메모리 부족
- 대용량 데이터셋의 경우 배치 처리로 나누어 실행
- `main()` 함수에서 파일 개수를 제한하여 테스트

## 📈 성능 최적화

- **목표**: precision 85~90% 달성을 위해 신뢰도 점수 임계값 조정 가능
- **기준선 변경**: `score_pattern()` 함수에서 0.75 대신 다른 값 사용
- **가중치 조정**: 볼륨 수축 일관성(0.4), 수축 폭 비율(0.3), 볼륨 스파이크(0.3) 비율 조정

## 🔗 확장 가능성

- **시각화 연동**: matplotlib, plotly를 사용한 패턴 차트 생성
- **백테스팅**: 탐지된 패턴의 수익률 검증
- **실시간 모니터링**: API 연동으로 실시간 패턴 탐지
- **추가 패턴**: Ascending Triangle, Flag, Pennant 등 다른 패턴 추가