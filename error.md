1. 첫번째 에러

⏳ 9단계: 주도주 투자 전략 스크리닝 실행 중...

📊 주도주 투자 전략 스크리너 시작...
ERROR - 시장 단계 결정 중 오류 발생: 'close'
INFO - 주도주 투자 전략 스크리닝 시작...
INFO - 현재 시장 단계: unknown -> 이것도 왜 unknown이라고 나오는지 확인 후 수정할 것.
INFO - 강한 섹터 수: 2
INFO -   - Technology: RS 점수 = 234.16, 백분위 = 134.26
INFO -   - Consumer Discretionary: RS 점수 = 123.16, 백분위 = 121.57
INFO - 조건을 만족하는 종목이 없습니다.
⚠️ 조건을 만족하는 종목이 없습니다.

2. 두번째 에러
2025-06-21 21:14:56,515 - ERROR - IPO 데이터 로드 중 오류: 'RealIPODataCollector' object has no attribute 'get_recent_ipos'

3. 세번째 에러

⚠️ 쿨라매기 모듈 로드 실패: No module named 'qullamaggie'

4. 네번째 에러

⚠️ VIX 데이터가 충분하지 않습니다. (필요: 200, 실제: 28)
❌ 시장 국면 분석 중 오류 발생: cannot access local variable 'vix_conditio
n' where it is not associated with a value
Traceback (most recent call last):
  File "C:\Users\HOME\Desktop\invest_prototype\orchestrator\tasks.py", line 473, in run_market_regime_analysis
    result = analyze_market_regime(save_result=True)
  File "C:\Users\HOME\Desktop\invest_prototype\utils\market_regime_calc.py", line 296, in analyze_market_regime
    condition_regime, condition_details = determine_regime_by_conditions(index_data)
                                          ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^
  File "C:\Users\HOME\Desktop\invest_prototype\utils\market_regime_conditions\determine.py", line 30, in determine_regime_by_conditions
    is_qualified, details = check_function(index_data)
                            ~~~~~~~~~~~~~~^^^^^^^^^^^^
  File "C:\Users\HOME\Desktop\invest_prototype\utils\market_regime_conditions\aggressive_bull.py", line 90, in check_aggressive_bull_conditions   
    additional_conditions.append(vix_condition)
                                 ^^^^^^^^^^^^^
UnboundLocalError: cannot access local variable 'vix_condition' where it 
is not associated with a value


5. 다섯번째 에러

❌ 전략 2 스크리닝 오류: The truth value of a Series is ambiguous. Use a.e
mpty, a.bool(), a.item(), a.any() or a.all().
Traceback (most recent call last):
  File "C:\Users\HOME\Desktop\invest_prototype\portfolio\long_short\strategy2.py", line 102, in run_strategy2_screening
    if rsi_3_series.empty or pd.isna(rsi_3_series.iloc[-1]):
                             ~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\HOME\AppData\Local\Programs\Python\Python313\Lib\site-packages\pandas\core\generic.py", line 1577, in __nonzero__
    raise ValueError(
    ...<2 lines>...
    )
ValueError: The truth value of a Series is ambiguous. Use a.empty, a.bool(), a.item(), a.any() or a.all().

6. 여섯번째 에러

❌ 전략 5 스크리닝 오류: The truth value of a Series is ambiguous. Use a.e
mpty, a.bool(), a.item(), a.any() or a.all().
Traceback (most recent call last):
  File "C:\Users\HOME\Desktop\invest_prototype\portfolio\long_short\strategy5.py", line 95, in run_strategy5_screening
    if pd.isna(adx_7d) or adx_7d < 55:
       ~~~~~~~^^^^^^^^
  File "C:\Users\HOME\AppData\Local\Programs\Python\Python313\Lib\site-packages\pandas\core\generic.py", line 1577, in __nonzero__
    raise ValueError(
    ...<2 lines>...
    )
ValueError: The truth value of a Series is ambiguous. Use a.empty, a.bool(), a.item(), a.any() or a.all().

7. 일곱번째 에러


❌ 변동성 스큐 포트폴리오 생성 오류: can't multiply sequence by non-in
t of type 'float'
⚠️ 조건을 만족하는 종목이 없습니다.


8. 여덟번째 에러

