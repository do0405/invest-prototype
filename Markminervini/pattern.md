pattern_detection.py 를 조정하려고 해. 이제 패턴을 보는게 아니라 정량화 쉬운 조건들에 대해서만 필터링할 예정이야.

### 시스템 역할

당신은 패턴 인식 전용 AI IDE입니다. 다음은 미국 주식 일봉 데이터를 기반으로,

문헌 기반의 수축 신호 5가지(가격·거래량·변동성 등)를 탐지하는 알고리즘을 구현하기 위한 상세 명세입니다.

이 알고리즘은 기계학습이 아닌 **해석 가능한 룰 기반 구조**이며,

VCP(Volatility Contraction Pattern)와 컵앤핸들(Cup and Handle)의 공통 구조를 수치적으로 포착하기 위해 설계되었습니다.

---

### 목표

아래의 5가지 수축 신호 조건을 이용하여,

**조건별 점수를 합산하여 판단하는 '점수 기반(score)' 모드**

로 구현합니다.

---

### 정량 조건 – 문헌 기반 5가지 수축 신호

| 번호 | 조건명 | 수치 기준 | 출처 |

|------|--------|------------|--------|

| ① | **거래량 Dry-Up (VDU)** | 최근 10일 평균 거래량 < 50일 EMA × 0.4 | IBD 컵핸들 체크리스트, TraderLion VCP 가이드 |

| ② | **가격 범위 축소 (Price Range Contraction)** | 최근 5일간 High–Low 평균 < 이전 5일간 평균 × 0.8 | AmiBroker VCP 공식 토론 스레드 |

| ③ | **변동성 수축 (Volatility Contraction)** | ATR(14)_현재 < ATR(14)_15일 전 × 0.8 | StockCharts & Tradingsim – VCP 설명 자료 |

| ④ | **지속적인 거래량 하락 추세 (Volume Downtrend)** | log1p(volume) 최근 20일 선형회귀 기울기 < –0.001 <br>+ std/mean < 0.2 | TraderLion, DeepVue RMV 기반 스크리너 해설 |

| ⑤ | **Higher Lows 유지** | 최근 3개 저점이 연속 상승 (low₋₂ < low₋₁ < low₀) | Tradingsim – 컵핸들 핸들 구조 조건 |

---

### 구현 방식

```python

def contraction_signals(df):

vol = df['volume']; high, low = df['high'], df['low']; close = df['close']

# ① VDU: 최근 10일 평균 거래량 < EMA50 × 0.4

v10 = vol.tail(10).mean()

v50 = vol.ewm(span=50).mean().iloc[-1]

vdu = v10 < 0.4 * v50

# ② 가격 범위 수축

range_now  = (high - low).tail(5).mean()

range_prev = (high - low).tail(10).head(5).mean()

pr_contr = range_now < 0.8 * range_prev

# ③ ATR 수축

atr = ta.atr(high, low, close, length=14)

atr_contr = atr.iloc[-1] < 0.8 * atr.iloc[-15]

# ④ 거래량 하락 추세 (log scale + 안정성)

y = np.log1p(vol.tail(20).values)

slope, _ = np.polyfit(np.arange(20), y, 1)

std_ratio = y.std() / y.mean()

vol_down = (slope < -0.001) and (std_ratio < 0.2)

# ⑤ Higher Lows

lows = low.tail(3).values

higher_lows = lows[0] < lows[1] < lows[2]

return {

"VDU": vdu,

"PriceRange": pr_contr,

"ATRContract": atr_contr,

"VolDowntrend": vol_down,

"HigherLows": higher_lows,

}

```

---

### 필터 모드별 동작 방식

점수 기반 모드 — 총점 ≥ 22점 시 통과 (30점 만점)

```python

score = (

5 * flags['VDU'] +

5 * flags['PriceRange'] +

5 * flags['ATRContract'] +

10 * flags['VolDowntrend'] +

5 * flags['HigherLows']

)

score_filter = score >= 22

```

---

### 출력 예시

csv파일로 결과를 만들고,

칼럼명은 티커, rs_score, 새로 만든 점수, fin_met_count

이때, fin_met_count가 5이상인것만 csv에 쓰고, 새로 만든 점수가 가장 높은 것부터 정렬해줘. results2 에 결과 데이터를 넣어줘.

### 참고 사항

- **이 알고리즘은 반드시 해석 가능한 방식**으로 동작해야 하며, 모든 필터 조건은 문헌에 기반하여 고정된 수치를 사용합니다.

- 추후 파라미터 변경이 필요한 경우, 문헌 기반 또는 백테스트 기반의 근거를 통해 조정되어야 합니다.

이 스펙은 패턴 탐지 알고리즘의 핵심 필터 모듈로 재사용되며, VCP 및 컵앤핸들 패턴 후보 탐색기의 전처리 레이어로 설계됩니다.