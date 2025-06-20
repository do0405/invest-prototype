# 시장 국면 판단을 위한 정량적 규칙 기반 지표 (Market Regime Classification Indicator)

## 지표 구성 요소 (Core Components)

### 1. 주요 지수 모니터링
- **S&P 500 (SPY)**: 대형주 지수
- **나스닥 100 (QQQ)**: 기술주 중심 지수  
- **Russell 2000 (IWM)**: 소형주 지수
- **S&P 400 MidCap (MDY)**: 중형주 지수
- **바이오텍 ETF (IBB, XBI)**: 바이오텍 섹터 지수

### 2. 기술적 지표
- **이동평균선**: 50일, 200일 단순이동평균
- **VIX**: 시장 변동성 지수
- **Put/Call Ratio**: 옵션 거래 비율
- **High-Low Index**: 신고가/신저가 비율
- **Advance-Decline Line**: 상승주/하락주 비율

---

## 시장 국면별 정량적 판단 기준

### 1. 공격적 상승장 (Aggressive Bull Market)
**점수: 80-100**

#### 필수 조건 (모든 조건 충족 시)
- S&P 500, QQQ, IWM, MDY 모두 50일 이동평균 위에서 거래
- 4개 주요 지수 모두 200일 이동평균 대비 +5% 이상
- 바이오텍 지수 (IBB, XBI) 상승세 지속 (월간 +3% 이상)

#### 부가 조건 (70% 이상 충족 시 확인)
- VIX < 20
- Put/Call Ratio < 0.7 (강한 낙관 심리)
- High-Low Index > 70 (신고가 기록 종목 다수)
- Advance-Decline Line 상승 추세
- 소형주(IWM)가 대형주(SPY) 대비 아웃퍼폼

---

### 2. 상승장 (Bull Market)  
**점수: 60-79**

#### 필수 조건
- S&P 500, QQQ는 50일 이동평균 위 유지
- 중소형주 (IWM, MDY) 중 하나 이상이 50일 이동평균 하회 시작
- 바이오텍 지수 상승 모멘텀 둔화 (월간 수익률 0~3%)

#### 부가 조건 (60% 이상 충족 시 확인)
- VIX 20-25 구간
- Put/Call Ratio 0.7-0.9
- High-Low Index 50-70
- 대형주 > 중형주 > 소형주 순서의 상대적 강세
- Advance-Decline Line 횡보 또는 완만한 상승

---

### 3. 조정장 (Correction Market)
**점수: 40-59**

#### 필수 조건
- 주요 지수 2개 이상이 50일 이동평균선 하회
- 주요 지수 고점 대비 -5% ~ -15% 조정
- 조정이 1주일 이상 지속

#### 부가 조건 (60% 이상 충족 시 확인)
- VIX 25-35 구간
- Put/Call Ratio 0.9-1.2
- High-Low Index 30-50
- Advance-Decline Line 하락 추세
- 단기 반등 시도 후 재하락 패턴

---

### 4. 위험 관리장 (Risk Management Market)
**점수: 20-39**

#### 필수 조건
- 주요 지수 3개 이상이 200일 이동평균선 하회
- 주요 지수 고점 대비 -15% ~ -25% 조정
- 개별 주식 20-30% 조정 빈발

#### 부가 조건 (50% 이상 충족 시 확인)
- VIX > 35
- Put/Call Ratio > 1.2
- High-Low Index < 30
- Advance-Decline Line 급락
- 이동평균선 역배열 (50일 < 200일)

---

### 5. 완전한 약세장 (Full Bear Market)
**점수: 0-19**

#### 필수 조건
- 모든 주요 지수가 200일 이동평균선 하회
- 주요 지수 고점 대비 -25% 이상 하락
- 하락 추세가 2개월 이상 지속

#### 부가 조건 (확인 지표)
- VIX > 40 또는 지속적으로 30 이상 유지
- Put/Call Ratio > 1.5
- High-Low Index < 20
- Advance-Decline Line 지속적 하락
- 바이오텍 지수 급락 (-30% 이상)

---

## 지표 계산 방법

### 종합 점수 산출
1. **기본 점수** (60점 만점)
   - 각 주요 지수(5개)별 12점씩 배점
   - 50일 MA 위: +6점, 200일 MA 위: +6점

2. **기술적 지표 점수** (40점 만점)
   - VIX 수준: 8점
   - Put/Call Ratio: 8점  
   - High-Low Index: 8점
   - Advance-Decline Line: 8점
   - 바이오텍 지수 상태: 8점

### 점수별 해석
- **80-100**: 공격적 상승장
- **60-79**: 상승장
- **40-59**: 조정장  
- **20-39**: 위험 관리장
- **0-19**: 완전한 약세장

---

## 주의사항 및 활용법

### 지표 업데이트 주기
- 일일 모니터링 권장
- 주간 단위로 트렌드 확인
- 국면 변화 시 2-3일 연속 확인 후 판정

### 제한사항
- 급격한 시장 변화 시 후행성 존재
- 섹터별 차이 고려 필요
- 글로벌 이벤트 발생 시 추가 분석 요구

### 투자 전략 가이드라인
- **공격적 상승장**: 소형주, 성장주 비중 확대
- **상승장**: 대형주 중심, 리더주 선별 투자
- **조정장**: 현금 비중 증대, 방어적 포지션
- **위험 관리장**: 신규 투자 중단, 손절매 기준 엄격 적용
- **완전한 약세장**: 현금 보유, 적립식 투자 외 투자 자제