# IPO 주식 패턴 분석 알고리즘 완성 문서

최근 1년 내 미국 주식 시장에 상장한 IPO 종목들의 거래 패턴을 분석하여 4가지 핵심 알고리즘을 개발했습니다. 이 문서는 신뢰할 수 있는 시장 데이터와 검증된 기술적 분석 방법론을 바탕으로 작성되었습니다.

## 시장 현황 분석

2024년 미국 IPO 시장은 328억 달러 규모를 기록했으며[1], 44개의 새로운 IPO가 2분기에만 70억 달러 이상의 가치를 창출했습니다[2]. 특히 CoreWeave(CRWV), Chime Financial(CHYM), Circle Internet Group(CRCL) 등이 주목할 만한 성과를 보였습니다[2].

## IPO 거래 패턴의 과학적 근거

연구에 따르면 IPO 주식은 상장 후 20거래일 경과 시점에서 첫 번째 국소 최고점(약 10%)을 기록하며, 105거래일 후 글로벌 최고점(약 33%)에 도달하는 패턴을 보입니다[3][4]. 이는 "조용한 기간(quiet period)" 만료와 6개월 락업 기간과 밀접한 관련이 있습니다.

# 4가지 핵심 패턴별 상세 알고리즘

## 1. 베이스 패턴 (Base Pattern) - IPO 안정화 포착

**과학적 근거**: IPO 베이스는 주식이 상장 후 25일 내에 형성되는 첫 번째 거래 가능한 지점으로, William O'Neil의 연구에 따르면 모든 베이스 패턴 중 가장 폭발적인 움직임을 보입니다[3][5].

**수학적 조건**:
```
베이스 형성 조건:
- 시간: 상장 후 5~25일 내
- 하락폭: 현재가 < IPO 첫날 종가 × 0.70 (30% 하락)
- 횡보 범위: (구간최고가 - 구간최저가) ÷ 구간평균 < 0.20
- 거래량 감소: 베이스 기간 평균거래량 < 전체 기간 평균거래량
```

**알고리즘 구현**:
```python
def check_ipo_base_pattern(df, ipo_price):
    current_price = df['close'].iloc[-1]
    ipo_first_close = ipo_price  # IPO 첫날 종가로 가정
    
    # 30% 하락 확인
    decline_check = current_price < ipo_first_close * 0.70
    
    # 횡보 범위 확인 (최근 5~25일 구간)
    period_high = df['high'].iloc[-25:].max()
    period_low = df['low'].iloc[-25:].min()
    period_avg = df['close'].iloc[-25:].mean()
    range_check = (period_high - period_low) / period_avg < 0.20
    
    # 거래량 감소 확인
    base_period_volume = df['volume'].iloc[-25:].mean()
    total_period_volume = df['volume'].mean()
    volume_decrease = base_period_volume < total_period_volume
    
    return decline_check and range_check and volume_decrease
```

## 2. 브레이크아웃 패턴 (Breakout Pattern) - 돌파 포착

**과학적 근거**: 베이스 패턴 형성 후 거래량을 동반한 가격 돌파는 강한 상승 모멘텀의 시작을 의미합니다.

**수학적 조건**:
```
브레이크아웃 조건:
- 돌파: 현재가 > 구간 최고가 × 1.025 (2.5% 이상)
- 거래량: 당일거래량 > 10일 평균거래량 × 2.0
- 종가 확인: 종가 > 돌파수준 × 0.975 (돌파수준 -2.5% 이상)
- RSI: 50 < RSI < 85
```

**알고리즘 구현**:
```python
def check_ipo_breakout(df):
    current_price = df['close'].iloc[-1]
    current_volume = df['volume'].iloc[-1]
    avg_10_volume = df['volume'].iloc[-10:].mean()
    
    # 구간 최고가 (최근 25일)
    base_high = df['high'].iloc[-25:].max()
    
    # 2.5% 돌파 확인
    breakout_level = base_high * 1.025
    breakout_check = current_price > breakout_level
    
    # 거래량 2배 이상 증가 확인
    volume_check = current_volume > avg_10_volume * 2.0
    
    # 종가가 돌파 수준 근처에서 마감 확인
    close_check = current_price > breakout_level * 0.975
    
    # RSI 확인
    rsi = calculate_rsi(df['close'], 14)
    rsi_check = 50 < rsi < 85
    
    return breakout_check and volume_check and close_check and rsi_check
```

## 3. Track 1 조건 - 반등 매수

**과학적 근거**: IPO 주식이 50% 이상 하락 후 반등 신호를 보일 때 매수하는 전략입니다.

**수학적 조건**:
```
Track 1 조건:
- 하락폭: 현재가 < IPO 발행가 × 0.50 (50% 이상 하락)
- 거래량 증가: 당일거래량 > 5일 평균 × 1.8
- RSI 반전: RSI가 30 이하에서 35 이상으로 상승
- 지지 확인: 피보나치 61.8% 또는 50% 수준에서 지지
```

**알고리즘 구현**:
```python
def check_track1(df, ipo_price):
    current_price = df['close'].iloc[-1]
    current_volume = df['volume'].iloc[-1]
    avg_5_volume = df['volume'].iloc[-5:].mean()
    
    # 50% 이상 하락 확인
    decline_check = current_price < ipo_price * 0.50
    
    # 거래량 증가 확인
    volume_check = current_volume > avg_5_volume * 1.8
    
    # RSI 반전 확인
    rsi = calculate_rsi(df['close'], 14)
    prev_rsi = calculate_rsi(df['close'].iloc[:-1], 14)
    rsi_check = prev_rsi <= 30 and rsi >= 35
    
    return decline_check and volume_check and rsi_check
```

## 4. Track 2 조건 - 강한 모멘텀 추격 매수

**과학적 근거**: 모멘텀 투자는 Jegadeesh와 Titman(1993)의 연구에서 입증된 전략으로, 12개월까지 유의한 수익을 창출합니다[4]. IPO 주식의 경우 상장가 대비 50% 이상 상승 후에도 추가 상승 가능성이 높습니다.

**수학적 조건**:
```
강한 모멘텀 조건:
- 상승폭: 현재가 > IPO 발행가 × 1.50 (50% 이상 상승)
- 승률: 최근 10일 중 7일 이상 상승
- 평균 상승: 최근 5일 평균 일일수익률 > 2%
- 이동평균: 10일 MA > 21일 MA > 50일 MA
- 거래량: 최근 5일 평균 > 전체기간 평균 × 1.3
- RSI: 60 < RSI < 85
```

**알고리즘 구현**:
```python
def check_track2(df, ipo_price):
    current_price = df['close'].iloc[-1]
    
    # 50% 이상 상승 확인
    if current_price > ipo_price * 1.50:
        recent_10_days = df.iloc[-10:]
        returns = recent_10_days['close'].pct_change().dropna()
        up_days = sum(returns > 0)
        
        # 10일 중 7일 이상 상승 확인
        if up_days >= 7:
            # 최근 5일 평균 수익률 2% 이상 확인
            recent_5_returns = returns.iloc[-5:]
            avg_return = recent_5_returns.mean()
            
            if avg_return > 0.02:
                # 이동평균 정렬 확인
                ma_10 = df['close'].iloc[-10:].mean()
                ma_21 = df['close'].iloc[-21:].mean()
                ma_50 = df['close'].iloc[-50:].mean()
                
                if ma_10 > ma_21 > ma_50:
                    # 거래량 확인
                    recent_5_volume = df['volume'].iloc[-5:].mean()
                    total_volume = df['volume'].mean()
                    
                    if recent_5_volume > total_volume * 1.3:
                        # RSI 확인
                        rsi = calculate_rsi(df['close'], 14)
                        if 60 < rsi < 85:
                            return True
    return False
```

## RSI 계산 함수

```python
def calculate_rsi(prices, period=14):
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi.iloc[-1]
```

