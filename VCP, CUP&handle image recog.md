# 이미지 기반 차트 패턴 인식 워크플로

아래는 파이썬 환경에서 OHLCV 데이터를 이용해 **거래량이 포함된 캔들차트 이미지를 생성**하고, **Hugging Face의 고성능 YOLOv8 모델**을 불러와 해당 차트에서 **VCP(Volatility Contraction Pattern)** 또는 **Cup & Handle** 패턴을 자동 인식·판단하는 전체 워크플로입니다.

---

## 1. 데이터 준비 및 기간 선정

1. **OHLCV 데이터 소스**: Yahoo Finance, CCXT API 등  
2. **패턴 인식에 적절한 기간**  
   - VCP: 6~12주(약 30~60거래일) 패턴 형성 기간 권장[1].  
   - Cup & Handle: 컵 형성 1~6개월(약 20~120거래일), 핸들 형성 1~4주(5~20거래일)[2].  
3. **권장 조회 범위**:  
   - 일간 차트 기준: 60~120거래일 (VCP와 컵앤핸들 공통 커버)  
   - 주간 차트 기준: 12~24주 (장기 패턴 검출 시)

---

## 2. 차트 이미지 생성

1. **라이브러리 설치**  
   ```bash
   pip install pandas matplotlib mplfinance
   ```
2. **데이터 로드 및 전처리**  
   ```python
   import pandas as pd
   df = pd.read_csv('ohlcv.csv', index_col=0, parse_dates=True)
   ```
3. **캔들차트 + 거래량 이미지 출력**  
   ```python
   import mplfinance as mpf
   mpf.plot(df,
            type='candle',
            volume=True,
            style='yahoo',
            savefig='chart.png')
   ```
   - 파일명: `chart.png`  
   - 기간: 60~120일 (일간 OHLCV 기준)  

---

## 3. Hugging Face 모델 선정

- **모델**: `foduucom/stockmarket-pattern-detection-yolov8`  
- **특징**:  
  - 실시간 차트 패턴(Object Detection) 인식에 특화[3].  
  - mAP@0.5(box) ≈ 0.61로 높은 정확도[4].  
  - Python용 `ultralytics` 라이브러리로 직접 로드 가능.  

---

## 4. 패턴 인식 워크플로

1. **라이브러리 설치**  
   ```bash
   pip install ultralytics==8.3.94 opencv-python
   ```
2. **모델 로드 및 이미지 추론**  
   ```python
   from ultralytics import YOLO
   model = YOLO('model.pt')                     # Hugging Face로부터 다운로드
   results = model('chart.png')                 # 차트 이미지 추론
   ```
3. **패턴 결과 해석**  
   ```python
   for box in results[0].boxes:
       label = results[0].names[int(box.cls)]
       conf = float(box.conf)
       print(f"{label}: {conf:.2f}")
   ```
   - `label`에 `"VCP"` 또는 `"Cup&Handle"` 클래스를 매핑하도록 모델 튜닝 필요.  
   - 신뢰도(`conf`) 기준으로 임계치(예: ≥ 0.5) 적용.  

---

## 5. 전체 자동화 스크립트 예시

| 단계        | 코드 스니펫                                 |
|-----------|------------------------------------------|
| 데이터 로드   | `df = pd.read_csv(...)`                  |
| 차트 생성    | `mpf.plot(..., savefig='chart.png')`     |
| 모델 추론    | `results = model('chart.png')`           |
| 패턴 필터    | `if label in ['VCP','Cup&Handle']:`     |
| 결과 출력    | `print(label, conf)`                    |

---

## 6. 신뢰성 확보 방안

- **다중 시간프레임**: 일봉·주봉 병행하여 패턴 확인[5].  
- **볼륨 필터**: 패턴 돌파 시 거래량 기준(핸들 돌파 볼륨 ≥ 140%) 적용[6].  
- **후검증**: 과거 패턴 발생 시 실제 성과 백테스트 수행.  

---

이 워크플로를 따르면 **60~120일 분량**의 OHLCV 데이터를 기반으로 한 캔들차트 이미지를 생성하고, Hugging Face의 **YOLOv8 모델**을 활용해 **VCP** 및 **Cup & Handle** 패턴을 고정밀·자동으로 인식할 수 있습니다.