# 정량적 Rule-based 스크리너 설계: VCP와 컵앤핸들 패턴을 1년 OHLCV로 스크리닝하기

## Executive summary

본 보고서는 entity["people","마크 미너비니(Mark Minervini)","stock trader and author"] 방식으로 널리 알려진 **VCP(Volatility Contraction Pattern)**와, entity["people","윌리엄 J. 오닐(William J. O'Neil)","investor and author"]이 대중화한 **컵앤핸들(Cup with Handle)**을 **일봉(가정) 1년치 다종목 OHLCV CSV**로 스크리닝하기 위한 **명확한 수치 규칙(룰셋), 알고리즘 흐름, 검증/튜닝 절차, 구현 디테일**을 제시한다. 기술적 패턴 인식은 “눈으로 보는 주관성”이 크기 때문에, **극값(피벗)·변동성·거래량을 규칙화**해 자동 탐지를 가능케 하고, 이후 **형성 중 / 완성 / 최근 5영업일 내 돌파** 상태를 분리해 결과를 반환하는 구조를 권장한다. citeturn17view0

핵심 접근은 다음과 같다.  
(1) **전처리/가격보정(가능하면 분할·배당 반영)** → (2) **보조지표(이평, ATR%, 수익률 표준편차, 거래량 평균 등) 계산** → (3) **빠른 후보 탐색(필터링)** → (4) **구조 검증(피벗/스윙 기반 패턴 규칙 체크)** → (5) **돌파 판정(최근 5영업일)** → (6) **스코어링·랭킹** → (7) **워크포워드 기반 검증/민감도 분석으로 과최적화 방지**. citeturn17view1turn17view2turn16view0

또한 실무적으로는 (a) 미너비니식 “강한 추세/스테이지2” 템플릿(가격이 주요 장기 이평 위, 52주 고점 근접 등)을 **사전 필터로 사용해 오탐을 크게 줄이고**, (b) 패턴 본체는 **“수축(변동성↓, 거래량↓) + 명확한 피벗 + 돌파 시 거래량 확장”**을 정량화하되, (c) 파라미터는 **단일 최적값**이 아니라 **안정 구간(robust region)**을 목표로 튜닝하는 것을 권장한다. citeturn8view2turn12search3turn10view0turn6view2turn8view0

## 데이터 전처리와 지표 산출

### 입력 데이터 표준화

권장 입력 스키마(일봉):  
- 필수: `ticker, date, open, high, low, close, volume`  
- 강력 권장(가능하면): `adj_close`(조정종가) 또는 분할/배당 조정계수(Adjustment factor)

정렬/무결성 기본 규칙:  
- 종목별(`ticker`)로 `date` 오름차순 정렬, 중복 날짜 제거(또는 OHLCV 합리적 병합)  
- 음수/0 가격 제거, 거래정지 등 비정상 캔들(예: high<low) 제거 또는 수정 로그 남김  
- 거래량 0이 “휴장/결측”인지 “실제 0 거래”인지 구분이 어려우면, **일봉에서는 보통 결측으로 처리**(전일 대비 수익률 왜곡 방지)

“최근 5영업일”은 실무상 **각 종목의 최신 5개 행(거래가 존재하는 5개 일봉)**으로 정의하는 것이 안전하다(국가/거래소별 휴일이 달라 단순 Business day와 다를 수 있음). 거래소 캘린더가 필요하면 `pandas_market_calendars`처럼 “거래소 개장일” 인덱스를 제공하는 라이브러리 사용을 고려할 수 있다. citeturn19search0turn19search3

### 결측치 처리(OHLCV)

일봉 스크리닝에서 결측 처리는 “왜곡 최소화”가 목표다. 다음을 권장한다.

- **가격(Open/High/Low/Close) 결측**: 해당 일자 제거(보간은 패턴 탐지에 인위적 평활을 만들어 위험)  
- **거래량 결측**: 0으로 채우지 말고 결측으로 유지 후, `volume_ma` 계산 시 `min_periods`를 충분히 주거나(예: 20일 MA는 최소 15일) 결측 많은 종목은 제외  
- **연속 장기 결측(예: 상장 직후/정지)**: 1년 창에서 패턴 구조 자체가 불완전해지므로 제외 또는 “데이터 부족” 플래그

### 분할/배당/권리락 등 가격 보정(미지정 → 제안 포함)

사용자의 CSV가 **조정주가인지 미지정**이므로, 패턴 스크리닝/백테스트 정확도를 위해 다음 우선순위를 제안한다.

1) `adj_close`가 제공되는 경우: **Adj factor = adj_close / close**로 계산해 OHLC까지 일관되게 조정  
- `adj_open = open * adj_close / close`  
- `adj_high = high * adj_close / close`  
- `adj_low  = low  * adj_close / close`  
- `adj_close = adj_close`  
- `adj_volume = volume / (adj_close / close)` (가격이 분할로 내려가면 거래량은 반대로 늘어나므로 역비례 조정) citeturn9view4turn8view3

2) `adj_close`가 없고 분할/배당 정보도 없는 경우:  
- **패턴 모양 자체가 분할/권리락으로 깨질 수 있으므로**, 가능하면 데이터 공급원에서 조정주가로 재수급을 권장한다(조정종가는 분할/배당 등을 반영해 과거가격을 비교 가능하게 만든다). citeturn4search0turn8view3

`Yahoo` 계열 데이터(참고)에서는 “Adjusted close는 분할과 배당을 반영해 조정되며, 분할/배당 승수로 조정한다”는 설명이 공식 도움말에 있다. citeturn8view3  
(데이터 소스가 entity["company","Yahoo Finance","financial data service"]가 아니더라도, “조정종가를 쓰는 이유” 자체는 대부분의 주식 데이터에 공통적으로 적용된다. citeturn4search0)

### 보조지표(필수/권장)와 계산식, 윈도우 추천

아래 지표는 “패턴의 수축(변동성↓)/거래량 건조(Volume dry-up)/추세(Trend)/돌파(Volume expansion)”를 정량화하는 데 쓰인다. VCP와 컵앤핸들 모두 “거래량 감소 + 돌파 시 확장”을 강조한다. citeturn6view2turn8view0turn6view1turn9view2turn10view0

권장 산출 지표(일봉 기준):

1) 이동평균(SMA)  
- `SMA20, SMA50, SMA150, SMA200` (미너비니식 추세 템플릿/장기 추세 확인에 자주 사용) citeturn8view2turn12search3  
- 구현: `df['close'].rolling(n).mean()`

2) True Range(TR), ATR(평균진폭) 및 ATR%  
- TR: `max(high-low, abs(high-prev_close), abs(low-prev_close))`  
- ATR(통상 14기간): TR의 이동평균(와일더 스무딩을 자주 사용) citeturn17view3turn2search2  
- ATR%: `ATR14 / close * 100` (종목 가격수준 차이를 정규화해 “수축”을 비교하기 쉬움)

3) 수익률 표준편차(변동성)  
- 로그수익률: `r_t = ln(close_t / close_{t-1})`  
- `vol20 = std(r, window=20)` (또는 10/20/50 병행)  
- “변동성 수축”은 ATR% 또는 표준편차의 하락으로 표현 가능. 패턴 자동인식 문헌에서도 “스무딩 후 극값 기반 정의”로 패턴을 수치화하는 접근이 제시된다. citeturn17view0turn16view0

4) 거래량 평균/상대거래량(Relative Volume)  
- `vol_ma20, vol_ma50`  
- `rel_vol = volume / vol_ma50` (돌파 시 1.4~2.0배 같은 임계값을 설정)

5) “타이트함” 지표(추천)  
- 최근 N일 %레인지: `range_N = (rolling_max(high,N) - rolling_min(low,N)) / close * 100`  
- 예: N=5 또는 10. “5일 범위가 작다”는 식의 타이트 레인지 스캔 아이디어가 실제 스크리너에서 사용된다. citeturn8view0

## 패턴 정량 정의: VCP

VCP는 **강한 상승 추세 중** “왼쪽에서 오른쪽으로 갈수록” **가격 변동폭(Volatility)과 거래량(Volume)이 점진적으로 줄어들며**, 여러 번의 수축(contraction)을 만든 뒤 **피벗(pivot) 위로 거래량을 동반해 돌파**하는 구조로 요약된다. citeturn6view2turn8view0turn10view0turn12search3  
특히 인터뷰 기사에서도 “베이스의 왼쪽에서 오른쪽으로 일련의 수축이 진행되고, 가장 타이트한 구간에서 거래량과 가격이 조용해진다”고 설명된다. citeturn10view0

### VCP 정량 룰셋(권장 기본값 포함)

아래는 “**빠른 후보탐색(통계 기반)** + **구조검증(스윙/피벗 기반)**”을 결합한 룰셋이다. (1년 일봉 데이터에서도 구동 가능하도록 기간을 거래일 기준으로 표기한다.)

#### VCP 룰셋 테이블

| 구분 | 규칙(수치화) | 권장 기본값 | 조정 가이드(오탐/미탐 관점) |
|---|---|---:|---|
| 전제(추세) | **상승 추세 필터**: `close > SMA50`, `SMA50 > SMA150 > SMA200`, `SMA200` 상승(예: 30거래일 전보다 큼), `close`가 52주 저점 대비 +30% 이상, 52주 고점 대비 -25% 이내(선택) | ON | 추세 필터는 미너비니식 스크리닝에 자주 포함된다. 너무 엄격하면 초기 리더를 놓칠 수 있어, 초반엔 52주 조건을 OFF하고 MA 정렬만 쓰는 타협도 가능. citeturn8view2turn12search3 |
| 베이스 탐색 창 | 최근 `BASE_LOOKBACK` 안에서 VCP 베이스를 찾음 | 120일(≈ 6개월) | 1년 데이터만 있으면 180일 이상은 정보가 부족할 수 있음. 베이스가 길면(큰 종목/대형주) 160~200일로 확장 고려. |
| 피벗 후보 | 베이스 내 저항선(피벗) = `max(high)`(또는 스윙 고점들 중 최댓값) | `pivot = max(high[-BASE_LOOKBACK:])` | “너무 이른 왼쪽 고점”이 피벗이 되면 돌파가 멀어져 미탐↑. 스윙 기반으로 “마지막 1~2개 수축의 상단”을 피벗으로 두면 실전형. |
| 수축 횟수 | 수축(contraction) 2~6회(실무/문헌에서 자주 언급) | 최소 3회 | 2회는 단순 플래그/박스와 혼동↑. 4회 이상은 드문 대신 신뢰↑. citeturn6view2turn12search3 |
| 수축 깊이(가격) | 각 수축 i의 하락폭 `depth_i = (swing_high_i - swing_low_i)/swing_high_i`가 점진적으로 감소 | `depth_{i+1} <= 0.7 * depth_i` | 0.8~0.9로 완화하면 후보↑(미탐↓) 대신 오탐↑. 변동 큰 시장(소형/바이오)은 완화 필요. “15%→10%→5%” 같은 점진 감소 예시는 전형적 설명으로 자주 제시된다. citeturn8view0 |
| 수축 기간 | 각 수축 구간 길이(거래일) 최소치 | 5일 | 너무 짧으면 노이즈. `order`(피벗 탐지 민감도)와 함께 조정. |
| 고저점 구조 | 수축이 진행되며 **저점이 높아지는(higher lows)** 경향(선택) | ON(완화 가능) | 강한 리더는 higher lows가 자주 관찰된다고 설명된다. 다만 컵/박스형은 예외가 있어 완화 옵션 제공. citeturn8view0turn8view0 |
| 변동성 수축(통계) | `ATR%` 또는 `vol20`이 베이스 왼쪽 대비 오른쪽에서 감소 | `ATR%_last10 <= 0.7*ATR%_first10` | ATR%는 방향성이 아니라 변동성만 측정한다. 수축을 수치로 빠르게 포착하는 데 유리. citeturn17view3turn6view2 |
| 거래량 건조 | 베이스가 진행될수록 거래량 감소(Volume contraction) | `vol_ma10_last <= 0.8*vol_ma50` 또는 “수축 구간별 평균 거래량 감소” | “거래량이 감소하며, 돌파에서 급증”은 VCP의 대표 특징으로 반복 언급된다. 임계값(0.6~0.9)은 종목 유동성에 맞춰 조정. citeturn8view0turn6view2turn10view0 |
| 최종 타이트 구간 | 마지막 5~10일 %레인지가 제한 범위 이내 | `range_5 <= 8%` | “타이트 레인지(5일 범위 < 8%)” 같은 규칙은 실제 스크리너 프리셋에도 등장한다. 더 엄격(예: 5%)하면 고품질만 남음. citeturn8view0 |
| 무효화(실패) | 마지막 수축 저점 이탈(또는 피벗 아래 일정 %) 시 실패 | `close < last_contraction_low` | “타이트 스톱”과 결합하는 실전 로직. 인터뷰에서도 정밀한 진입과 타이트 스톱을 사용한다고 언급된다. citeturn10view0turn6view2 |

### VCP 탐지 구현 포인트(스윙/피벗 기반)

VCP를 “수축들의 연쇄”로 보려면 **스윙 고점/저점(피벗)**을 잡아야 한다. 실무적으로는 다음 둘 중 하나(또는 결합)를 권장한다.

1) **피벗 정의(TradingView식)**: “좌우 k봉 안에서 더 큰 고/저가가 없는 국소 극값”을 피벗으로 본다. citeturn14view0  
2) **스무딩 후 극값 탐색(SciPy argrelextrema)**: 스무딩(rolling mean/EMA) 후 국소 극값을 찾고, 노이즈를 줄이기 위해 윈도우 내 최댓값/최솟값으로 재선택하는 방식이 패턴 인식 예제들에서 활용된다. citeturn14view1turn16view0

VCP는 “극값 기반 정의로 패턴을 자동화”하는 방법론과도 잘 맞는다(기술적 패턴 인식 자동화 연구에서 이 점을 강조). citeturn17view0

image_group{"layout":"carousel","aspect_ratio":"16:9","query":["volatility contraction pattern VCP chart example","Mark Minervini VCP line of least resistance chart","VCP breakout volume dry up chart","tight consolidation breakout example chart"],"num_per_query":1}

## 패턴 정량 정의: 컵앤핸들

컵앤핸들은 “상승 추세 이후” **U자형 컵(완만한 바닥)**을 만들고, 우측 고점(림) 부근에서 **짧은 핸들(약한 조정/횡보)**을 거친 뒤, **핸들 저항선을 돌파**하며 상승 재개를 기대하는 연속형 패턴이다. citeturn9view0turn9view2turn9view1  
컵앤핸들은 기간이 길 수 있으며(예: 수주~수개월, 혹은 7~65주로도 언급), 1년 데이터만으로는 “장기 컵” 일부를 놓칠 수 있다. citeturn9view0turn6view1turn9view3

### 컵앤핸들 정량 룰셋(권장 기본값 포함)

#### 컵앤핸들 룰셋 테이블

| 구분 | 규칙(수치화) | 권장 기본값 | 조정 가이드 |
|---|---|---:|---|
| 전제(추세) | 컵 형성 전 상승 추세 존재(연속형 패턴 전제) | ON | 추세가 너무 “성숙”하면(과열) 연속형 성공확률이 떨어질 수 있다는 설명이 있다. citeturn9view2turn6view1 |
| 컵 기간 | `cup_len` (좌측 림→우측 림) | 30~120 거래일(≈ 1.5~6개월) | StockCharts는 1~6개월(컵), 1~4주(핸들)를 제시하며, 더 긴 주기도 가능하다고 한다. Investopedia는 7~65주 언급. 데이터가 1년이면 30~160일 정도를 우선 탐색. citeturn6view1turn9view0turn9view3 |
| 림 높이 일치 | 좌/우 림(high 또는 close)이 유사(저항선 형성) | `abs(rim_L - rim_R)/rim_L <= 5%` | TradingView 패턴 설명에서도 컵의 양쪽 “엣지(림)가 대략 같은 수준”을 요구한다. citeturn14view0turn9view2 |
| 컵 깊이 | 컵 저점이 이전 상승분의 1/3 이하(이상적으로), 변동성 큰 시장은 1/2까지 | `cup_depth <= 33%` (완화 시 50%) | StockCharts는 이상적으로 1/3 이하, 변동 큰 경우 1/3~1/2 가능하다고 언급한다. citeturn6view1turn9view2 |
| U자형(완만함) | V자 급반전 배제(저점 부근 체류/완만한 회복) | “바닥 구간 ≥ X일” | U자형이 이상적이고 V자형은 너무 급격해 부적합하다는 설명이 반복된다. 정량화는 (a) 바닥 근처(저점±10%) 체류일수, (b) 스무딩 곡선의 곡률/기울기 변화로 근사. citeturn6view1turn9view2turn9view3 |
| 핸들 기간 | `handle_len` | 5~20 거래일(≈ 1~4주) | StockCharts는 1~4주가 이상적이라고 설명. TradingView 구현 예시에서도 “핸들이 컵보다 길면 안 됨” 같은 제약을 둔다. citeturn6view1turn14view0 |
| 핸들 깊이 | 핸들 조정폭은 컵 상승분의 최대 1/3(대개 그 이하) | `handle_depth <= 10~12%` 또는 `<= 1/3 cup_advance` | StockCharts는 “핸들이 컵 상승분의 최대 1/3까지”를 언급. (절대% 기준은 시장/종목에 따라 달라 8~12%를 시작점으로 두고 튜닝) citeturn6view1turn5search20 |
| 핸들 위치 | 핸들은 컵 상단(upper half)에서 형성 | `handle_low >= (cup_low + rim)/2` | Bulkowski는 “핸들이 컵 상단부에서 형성”을 가이드로 제시한다. citeturn9view3 |
| 거래량 | 컵/핸들 형성 중 거래량 감소, 돌파 시 증가 | “핸들 평균 거래량 < 50일 평균”, “돌파일 rel_vol 상향” | “돌파는 거래량 증가가 동반되어야 한다”는 설명이 컵앤핸들 문헌에서 반복된다. citeturn6view1turn9view2turn9view1 |

### 컵앤핸들 탐지의 실용적 단순화(1년 데이터 기준)

1년 OHLCV만 있을 때 “완벽한 U자 곡률”을 엄밀히 맞추려 하면 미탐이 급증한다. 실무적으로는 아래 단순화가 유효하다.

- 스무딩된 종가(예: 5일 이동평균)에서 **피벗 3점(좌림–바닥–우림)**을 찾고,  
- 좌림과 우림의 높이 차이가 작고,  
- 바닥이 충분히 깊되 너무 깊지 않으며(예: 12~35% 범위로 시작),  
- 우림 이후 핸들이 짧고 얕은 조정으로 형성되는지(upper half, 1/3 규칙)를 본다. citeturn14view0turn9view0turn9view2turn9view3

오픈소스 구현들도 대체로 “피벗/극값 + 기간 제약 + 깊이 제약” 조합을 사용한다(예: 커널 회귀 기반 컵 인식 후 규칙 체크). citeturn6view4turn17view0

image_group{"layout":"carousel","aspect_ratio":"16:9","query":["cup and handle pattern chart example breakout volume","cup with handle U-shaped cup and handle resistance line chart","cup and handle pattern annotated chart volume histogram","TradingView cup and handle pattern example"],"num_per_query":1}

## 돌파 판정과 최근 5영업일 필터

VCP와 컵앤핸들 모두 “**피벗(저항선) 돌파 + 거래량 확장**”을 돌파 확인의 핵심으로 둔다. VCP는 “가장 타이트한 수축 이후” 위로 튀는 구간을 특히 강조하고, 컵앤핸들은 “핸들 저항선 돌파”에 초점을 둔다. citeturn8view0turn6view2turn14view0turn6view1turn10view0

### 돌파 정의(최근 5영업일 이내)

다음 정의를 “기본값”으로 두고 종목/시장 특성에 따라 조정하는 방식을 권장한다.

**공통 돌파일(breakout day) 정의**  
- 가격 조건(보수적): `close_t > pivot * (1 + ε)`  
- 가격 조건(공격적): `high_t > pivot * (1 + ε)` AND `close_t >= pivot`  
- 권장 ε(노이즈 방지): `0.001 ~ 0.003` (0.1%~0.3%)

**거래량 확인(confirmation)**  
- `volume_t >= k * vol_ma50_t`  
- 기본값: `k = 1.4` (1.3~2.0 범위 탐색 권장)

컵앤핸들에 대해 entity["organization","Investor's Business Daily","financial news and investing"] 스타일 가이드에서는 “볼륨이 **50일 평균 대비 최소 40% 이상 증가**하는 것이 이상적”이라는 서술이 널리 인용된다. citeturn0search10  
또한 컵앤핸들 설명 자료들에서도 돌파 시 거래량 증가를 중요 조건으로 둔다. citeturn6view1turn9view2turn9view1

**최근 5영업일 조건**  
- `t_breakout`가 “각 종목의 최신 일자 기준”으로 `t_end-4` ~ `t_end` 사이에 존재하면 “최근 5영업일 내 돌파”로 판정  
- 구현은 거래소 캘린더 없이도 “종목별 최신 5개 행”으로 안정적으로 처리 가능(데이터에 실제 거래일만 존재한다는 가정). 거래소 달력이 필요하면 시장 캘린더 라이브러리 고려. citeturn19search0turn19search3

### 상태 분류 로직(형성 중 / 완성 / 최근 돌파)

패턴 탐지 결과(각 종목당 0개 이상 후보)에 대해 다음 상태를 부여하면 스크리너가 실무적으로 쓰기 좋아진다.

- **FORMING(형성 중)**: 패턴 조건의 일부(예: 수축/컵 구조)가 충족되나, “최종 타이트 구간/핸들”이 아직 조건 미달이거나 피벗이 안정적으로 고정되지 않음  
- **COMPLETED(완성)**: 패턴 구조(수축 연쇄 또는 컵+핸들)가 완성되었고 피벗이 정의 가능하지만, 아직 돌파(가격+거래량 확인)가 없음  
- **BROKEOUT_RECENT(최근 돌파)**: 완성 조건 + 최근 5영업일 내 돌파 조건 충족  
- (선택) **BROKEOUT_OLD**: 돌파는 했지만 5영업일 이전

이렇게 상태를 나누면 “지금 막 올라가는 종목”뿐 아니라 “곧 돌파할 후보(완성)”와 “아직 만드는 중(형성)”을 별도 워치리스트로 관리할 수 있다.

## 스크리너 알고리즘과 구현 설계

### 전체 흐름(탐지 → 검증 → 돌파확인 → 랭킹)

아래는 다종목 1년 OHLCV를 효율적으로 처리하기 위한 권장 파이프라인이다.

1) **전처리**: 데이터 정렬/결측 제거/가격 조정(가능 시)  
2) **지표 계산**: SMA, ATR%, range_5/10, vol_ma 등  
3) **1차 후보 탐색(빠른 필터)**  
   - (필수) 유동성 필터: 예) `median(volume*close)` 또는 `vol_ma50*close`가 일정 금액 이상  
   - (권장) 추세 템플릿: MA 정렬, 52주 고점 근접 등(미너비니식) citeturn8view2turn12search3  
   - (권장) 타이트 구간: 마지막 5~10일 range/ATR%가 낮음 citeturn8view0turn17view3  
4) **2차 구조 검증(패턴 룰)**  
   - VCP: 피벗/스윙 기반 수축 연쇄, 거래량 건조  
   - 컵앤핸들: 림·바닥·핸들 구조/기간/깊이  
   - 패턴 인식 자동화에서 흔히 쓰는 방식은 “스무딩 → 국소 극값 → 규칙 체크”이며, 이는 대규모 자동화에 적합하다. citeturn16view0turn14view1turn17view0  
5) **돌파 판정**: 최근 5영업일 내 돌파 여부/돌파 품질(볼륨 배수, 종가 위치 등)  
6) **스코어링·랭킹**: 신뢰점수/우선순위 정렬  
7) **출력**: 결과 DataFrame/CSV로 저장 + (선택) 차트 자동 생성

### 핵심 함수 설계(입력/출력 명세)

권장 입력 형태(판다스):  
- (A) long-format: `['ticker','date','open','high','low','close','volume', ...]`  
- (B) MultiIndex: index=`['ticker','date']`, columns=`OHLCV...`  

권장 출력 형태: “패턴 후보 리스트” DataFrame(종목당 여러 후보 가능)

#### 출력 필드(요구사항 반영)

| 필드명 | 타입 | 설명 |
|---|---|---|
| ticker | str | 종목코드 |
| pattern_type | str | `VCP` / `CUP_HANDLE` |
| status | str | `FORMING` / `COMPLETED` / `BROKEOUT_RECENT` / (선택)`BROKEOUT_OLD` |
| pattern_start | date | 패턴(베이스/컵) 시작일(추정) |
| pattern_end | date | 패턴 종료일(완성일 또는 최신 평가일) |
| pivot_price | float | 피벗(저항선) 가격 |
| breakout_date | date or NaN | 돌파일(없으면 NaN) |
| breakout_price | float or NaN | 돌파 기준 가격(종가 또는 피벗 상향치) |
| breakout_volume | float or NaN | 돌파일 거래량 |
| vol_ma50 | float | 돌파일(또는 최신일) 50일 평균 거래량 |
| volume_multiple | float | `breakout_volume / vol_ma50` |
| tightness_score | float | 마지막 구간 타이트함(예: 0~1 정규화) |
| volume_dryup_score | float | 거래량 건조도(예: 0~1) |
| trend_score | float | 추세 템플릿 충족도(예: 0~1) |
| confidence_score | float | 종합 신뢰점수(예: 0~100) |
| invalidation_price | float | 무효화 기준(예: last_contraction_low 또는 handle_low) |

### 의사코드(스텝별)

```text
INPUT: ohlcv_all (DataFrame; columns = ticker,date,open,high,low,close,volume[,adj_close])

PREPROCESS:
  - clean rows, sort by ticker/date
  - if adj_close exists -> build adj_factor and adjust OHLCV
  - compute indicators per ticker: SMA20/50/150/200, ATR14, ATR%, range_5/10, vol_ma20/50

FOR each ticker:
  df = ohlcv[ticker]
  if len(df) < MIN_BARS: continue

  # Stage A: fast filters
  if not liquidity_ok(df): continue
  if not trend_template_ok(df): continue  (optional but recommended)

  # Stage B: pattern candidates
  vcp_list  = detect_vcp(df, params_vcp)
  cup_list  = detect_cup_handle(df, params_cup)

  candidates = vcp_list + cup_list
  for c in candidates:
     breakout = check_breakout(df, pivot=c.pivot, last_n=5, vol_mult=k)
     status = classify(c, breakout)
     score  = score_candidate(df, c, breakout)
     output.append(build_row(ticker, c, breakout, status, score))

RANK:
  - sort by confidence_score desc, then volume_multiple desc, then liquidity desc

RETURN output_df
```

### 파이썬/판다스 코드 스니펫(핵심 뼈대)

아래 코드는 “바로 구현에 들어갈 수 있는” 형태로 **입력/출력 인터페이스**를 잡는 예시다(완전한 제품 코드는 아니며, 핵심 로직 중심).

```python
import numpy as np
import pandas as pd

def adjust_ohlcv_with_adj_close(df: pd.DataFrame) -> pd.DataFrame:
    """
    df columns: open, high, low, close, volume, adj_close
    returns: open, high, low, close, volume adjusted to adj_close scale
    """
    df = df.copy()
    f = df["adj_close"] / df["close"]
    df["open"]  = df["open"]  * f
    df["high"]  = df["high"]  * f
    df["low"]   = df["low"]   * f
    df["close"] = df["adj_close"]
    df["volume"] = df["volume"] / f
    return df

def compute_atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    high, low, close = df["high"], df["low"], df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    # Wilder-style smoothing can be approximated by ewm(alpha=1/n)
    atr = tr.ewm(alpha=1/n, adjust=False, min_periods=n).mean()
    return atr

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for n in [20, 50, 150, 200]:
        df[f"sma{n}"] = df["close"].rolling(n, min_periods=n).mean()
    df["atr14"] = compute_atr(df, 14)
    df["atrp14"] = 100.0 * df["atr14"] / df["close"]
    df["vol_ma20"] = df["volume"].rolling(20, min_periods=15).mean()
    df["vol_ma50"] = df["volume"].rolling(50, min_periods=40).mean()
    df["range_5p"] = 100.0 * (df["high"].rolling(5).max() - df["low"].rolling(5).min()) / df["close"]
    df["range_10p"] = 100.0 * (df["high"].rolling(10).max() - df["low"].rolling(10).min()) / df["close"]
    return df

def trend_template_ok(df: pd.DataFrame) -> bool:
    """
    A practical approximation of Minervini's trend template using daily MAs.
    """
    last = df.iloc[-1]
    # require indicators exist
    req_cols = ["sma50","sma150","sma200"]
    if any(pd.isna(last[c]) for c in req_cols):
        return False

    cond = (
        (last["close"] > last["sma50"]) and
        (last["close"] > last["sma150"]) and
        (last["close"] > last["sma200"]) and
        (last["sma50"] > last["sma150"] > last["sma200"]) and
        (last["sma200"] > df["sma200"].iloc[-31])  # slope up vs 30 trading days ago
    )
    return bool(cond)

def check_breakout_last_n(df: pd.DataFrame, pivot: float, n: int = 5,
                          eps: float = 0.001, vol_mult: float = 1.4):
    """
    Returns dict with breakout info if found in last n rows else None.
    Breakout uses close > pivot*(1+eps) and volume confirmation.
    """
    tail = df.iloc[-n:].copy()
    tail["cond_price"] = tail["close"] > pivot * (1.0 + eps)
    tail["cond_vol"] = tail["volume"] >= vol_mult * tail["vol_ma50"]
    hit = tail[tail["cond_price"] & tail["cond_vol"]]
    if hit.empty:
        return None
    row = hit.iloc[-1]  # most recent breakout within window
    return {
        "breakout_date": row.name if isinstance(row.name, (pd.Timestamp,)) else row.get("date", None),
        "breakout_price": float(row["close"]),
        "breakout_volume": float(row["volume"]),
        "volume_multiple": float(row["volume"] / row["vol_ma50"]) if row["vol_ma50"] else np.nan,
    }
```

- ATR의 TR 정의 및 14기간 사용, “ATR은 방향이 아니라 변동성을 측정”한다는 점은 대표 기술문헌/설명에서 공통적으로 제시된다. citeturn17view3turn2search2  
- 조정 OHLC를 `adj_close/close` 팩터로 만드는 방식과 거래량의 역비례 조정은 실무 Q&A에서도 간결히 정리되어 있다. citeturn9view4turn8view3

### 시각화 예시(캔들 + 패턴 라인 + 돌파 + 거래량)

패턴 스크리너는 “탐지 결과를 사람이 검증하는” 워크플로우가 거의 필수다(패턴 자체가 주관성이 큰 영역이기 때문). 기술적 패턴 인식 연구도 자동화가 분석가를 “대체”하기보다 “증폭”할 수 있다는 관점을 제시한다. citeturn17view0  
따라서 결과 상위 N개는 차트를 자동 저장하는 기능을 추천한다.

아래는 `mplfinance`로 (1) 캔들, (2) 거래량, (3) 피벗 수평선, (4) 돌파일 수직선, (5) 이평선을 함께 그리는 예시다. `mplfinance`는 Pandas DataFrame(OHLC + DatetimeIndex)와 잘 결합되도록 설계된 오픈소스다. citeturn17view4

```python
import mplfinance as mpf

def plot_candidate(df: pd.DataFrame, pattern: dict, fname: str = None):
    """
    df index: DatetimeIndex
    df cols: open, high, low, close, volume, sma20/sma50/sma150/sma200
    pattern dict example:
      {"pattern_type":"VCP", "start":..., "end":..., "pivot":..., "breakout_date":...}
    """
    view = df.loc[pattern["start"]:pattern["end"]].copy()

    apds = []
    for n in [20, 50, 150, 200]:
        c = f"sma{n}"
        if c in view.columns:
            apds.append(mpf.make_addplot(view[c]))

    hlines = [pattern["pivot"]]
    vlines = []
    if pattern.get("breakout_date") is not None:
        vlines.append(pattern["breakout_date"])

    kwargs = dict(
        type="candle",
        volume=True,
        addplot=apds if apds else None,
        hlines=dict(hlines=hlines, linestyle="--"),
        vlines=dict(vlines=vlines, linestyle=":") if vlines else None,
        title=f'{pattern["pattern_type"]} | pivot={pattern["pivot"]:.2f}'
    )
    if fname:
        mpf.plot(view, savefig=fname, **kwargs)
    else:
        mpf.plot(view, **kwargs)
```

## 튜닝·백테스트·검증 프레임워크

### 왜 “검증 설계”가 특히 중요한가

패턴 스크리너는 파라미터(기간/깊이/볼륨배수/피벗 민감도 등)가 많아, 무작정 그리드서치로 최적값을 고르면 **백테스트 과최적화(backtest overfitting)** 위험이 급증한다. 백테스트 과최적화의 확률(PBO)을 추정하는 연구는, 높은 인샘플 샤프가 아웃오브샘플에서 쉽게 붕괴할 수 있음을 보여준다. citeturn17view1turn3search12  
또한 금융 머신러닝/전략 개발에서 흔한 함정(누수, 잘못된 라벨링, 비IID 샘플 등)을 경고하는 문헌도 존재한다. citeturn17view2

### 테스트 데이터셋 설계(샘플 크기, 기간)

사용 데이터가 “1년”이지만, **검증은 최소 3~5년**을 권장한다. 이유는 컵앤핸들 자체가 수개월~수십 주에 걸칠 수 있고, 불리한 장세(약세·변동성 확대)까지 포함해야 하기 때문이다. citeturn9view0turn6view1turn9view3  
현실적으로 1년만 가능한 경우에는 “검증”이 아니라 “개발용 sanity check”로 격하하고, 추후 데이터 확장 시 정식 검증을 권장한다.

권장 샘플 구성(예시):
- 종목 수: 최소 300~2000(가능하면 전체 유니버스)  
- 기간: 5년 이상(상승·하락·횡보 국면 포함)  
- 편향 방지: 상폐/합병 등 “생존 편향(survivorship bias)” 최소화(가능하면 상장폐지 포함 데이터 사용)

### 라벨(정답) 정의: “돌파 성공”을 어떻게 정의할까

패턴 스크리너의 목적에 따라 라벨이 달라진다.

**A. 신호 정확도(분류) 관점 라벨**  
- 이벤트: 돌파일 t0(가격+거래량 조건 충족)  
- 성공(success) 예시:  
  - t0 이후 20거래일 내 최고종가가 `breakout_price*(1+R)` 이상 (예: R=10%) **그리고**  
  - t0 이후 최저종가가 `breakout_price*(1-S)` 아래로 크게 꺾이지 않음(예: S=7~10%, 손절 가정)  
- 이 정의는 미너비니가 “정밀 진입/타이트 스톱”을 중시한다는 설명과 결합해 실전형 성공 정의로 만들 수 있다. citeturn10view0turn6view2

**B. 수익성(트레이딩) 관점 라벨**  
- 고정 룰 기반 백테스트:  
  - 진입: 돌파일 종가(또는 다음날 시가)  
  - 손절: `handle_low` 또는 `last_contraction_low` 하회 시 청산  
  - 익절/청산: (i) 고정 보유기간(예: 20/60일), (ii) 트레일링 스톱(예: 2*ATR), (iii) 이평 이탈 등

### 평가 지표(정확도, 정밀도, 재현율, 샤프비율 등)

요구사항을 만족하기 위해 “분류 지표 + 포트폴리오 지표”를 같이 본다.

- 분류: Accuracy, Precision, Recall, F1  
  - 스크리너 목적이 “유망 후보를 좁히는 것”이면 **Precision(정밀도)** 우선  
  - “놓치면 안 되는 리더 포착”이면 Recall도 함께 관리

- 트레이딩/포트폴리오:  
  - 평균 수익률, 중앙값 수익률, 승률, Profit factor  
  - 샤프비율(Sharpe ratio), 최대낙폭(Max drawdown), Calmar 등  
  - 여러 파라미터 조합을 시험했다면 **아웃오브샘플 분포**와 “과최적화 위험”을 함께 보고, 필요 시 PBO/DSR류 관점도 참고 citeturn17view1turn3search12

### 교차검증/워크포워드(파라미터 튜닝)

패턴 스크리너 튜닝은 일반 ML과 달리 “시간 누수”가 특히 치명적이다. 권장 방식:

- **워크포워드(rolling) 방식**:  
  - (학습/튜닝 구간) 2년 → (검증 구간) 다음 6개월 → 앞으로 롤링  
  - 각 구간에서 파라미터의 “안정 구간”을 찾는다(한 점이 아니라 범위)  
- 이벤트 라벨이 미래 구간을 포함한다면, 일반 K-fold는 누수 위험이 있어 “purging/embargo” 같은 시계열 전용 검증을 고려한다는 논의가 있다. citeturn3search7turn17view2turn17view1

### 민감도 분석과 오탐/미탐 완화 전략

**민감도 분석(권장)**  
- 각 핵심 파라미터(예: VCP 수축 횟수, depth 감소율, range_5 임계값, 돌파 vol_mult)를 ±20~50% 범위로 흔들어도 상위 후보가 크게 뒤집히지 않는지 확인  
- 한두 파라미터에 결과가 과도하게 민감하면, 해당 조건을 “하드 필터”에서 “점수화(soft constraint)”로 바꾸는 것이 일반적으로 안정적

**오탐(false positive) 줄이기**  
- 유동성 필터 강화(저유동성은 스프레드/노이즈로 패턴이 쉽게 ‘그럴듯’해짐)  
- 추세 템플릿(장기 이평 정렬 등) ON citeturn8view2turn12search3  
- VCP: “최종 타이트 구간” 조건 강화(range_5, ATR% 급감) citeturn8view0turn17view3  
- 컵앤핸들: “핸들이 컵 상단부에서 형성” 및 “핸들 깊이 1/3 규칙” 강제 citeturn9view3turn6view1turn9view2  
- 거래량 확인 강화(돌파 vol_mult 상향). 컵앤핸들의 경우 50일 평균 대비 +40% 이상 가이드가 자주 인용된다. citeturn0search10turn6view1

**미탐(false negative) 줄이기**  
- VCP 수축 횟수 최소값을 2로 완화(대신 스코어에서 감점) citeturn6view2turn12search3  
- 컵 깊이/기간 범위를 넓히되, “U자형 조건”은 유지(깊이만 완화하면 잡음 패턴이 급증) citeturn6view1turn9view2turn9view0  
- 피벗 탐지 민감도(`order`/스무딩) 조정: 민감도가 너무 높으면 극값이 과다 검출되어 오탐↑, 너무 낮으면 미탐↑. 극값 기반 자동 패턴 인식 예제들이 “스무딩과 윈도우 재선정”으로 노이즈를 줄이는 이유가 여기에 있다. citeturn16view0turn14view1

### 샘플 결과 테이블 예시(형식)

| ticker | pattern_type | status | pivot_price | breakout_date | breakout_price | volume_multiple | confidence_score |
|---|---|---|---:|---|---:|---:|---:|
| AAA | VCP | BROKEOUT_RECENT | 52.30 | 2026-03-10 | 53.05 | 1.72 | 88 |
| BBB | CUP_HANDLE | COMPLETED | 118.40 | NaN | NaN | NaN | 81 |
| CCC | VCP | FORMING | 24.10 | NaN | NaN | NaN | 69 |

(위 표는 “필드 형태 예시”이며 실제 값은 데이터에 따라 산출된다.)

## 우선참조 소스

### 원전/저작(미너비니·오닐)

- entity["book","Trade Like a Stock Market Wizard","Minervini 2013"] (VCP/SEPA 맥락) citeturn2search25turn18search12  
- entity["book","Think and Trade Like a Champion","Minervini 2017"] (VCP 포함, 트레이딩 규칙/심리) citeturn18search0turn12search3  
- entity["book","How to Make Money in Stocks","O'Neil 1988"] (컵앤핸들 도입/대중화) citeturn9view0turn9view2

### VCP 설명·구현 가이드(블로그/플랫폼)

- VCP는 “수축이 진행되며 변동성과 거래량이 감소하고 피벗 돌파 시 거래량이 증가”하는 패턴으로 설명된다. citeturn6view2turn8view0turn12search3turn10view0  
- 미너비니의 “추세 템플릿(이평 정렬/52주 위치)”을 VCP 스캔의 출발점으로 사용하는 서술이 있다. citeturn8view2turn12search3

### 컵앤핸들 정의·가이드(기술적 분석 문헌/사이트)

- 컵앤핸들의 기간/구조(7~65주), 기본 개념 정리. citeturn9view0turn9view1  
- 컵 깊이/핸들 깊이/기간(컵 1~6개월, 핸들 1~4주), 돌파 시 거래량 증가, U자형 강조. citeturn6view1turn9view2turn9view3

### 오픈소스/구현체(참고용)

- entity["company","GitHub","code hosting platform"] 상의 VCP/미너비니 스크리너 구현 예시(스테이지2 필터 + VCP 탐지 + 피벗/거래량 평가 등). citeturn6view3turn0search3turn0search31  
- 컵앤핸들 패턴 인식 오픈소스(커널 회귀/극값 기반 규칙 등). citeturn6view4turn5search0turn8view4  
- entity["company","TradingView","charting platform"]의 Cup and Handle 자동 패턴 설명(피벗 정의, 컵 최소 폭, 돌파 기준 등) — 구현 규칙 설계에 직접적 힌트를 제공. citeturn14view0

### 자동 패턴 인식·검증 방법론(연구/리스크)

- 기술적 패턴 인식의 주관성, 그리고 “극값 기반 정의 + 자동 알고리즘” 접근을 제시한 연구. citeturn17view0  
- 패턴/전략 파라미터 최적화에서 백테스트 과최적화 위험(PBO) 및 아웃오브샘플 붕괴 가능성. citeturn17view1turn3search12  
- 금융 전략 개발에서 누수/라벨링/비IID 문제 등 흔한 함정 경고. citeturn17view2