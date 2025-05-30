"""
🎯 목적: Fred6724의 TradingView 기반 RS Rating 알고리즘을 Python으로 재현
📌 핵심 기능:
- 주어진 종목의 3/6/9/12개월 수익률 계산
- 동일 방식으로 S&P 500 수익률도 계산
- 상대 성과 (RS Score)를 계산: (종목 수익률 합산 / S&P 수익률 합산) * 100
- 전체 종목 universe에서 이 RS Score의 백분위(퍼센타일)를 구해 RS Rating 산출

✅ 입력 요구사항:
- 개별 종목: 최소 252거래일의 일간 종가 (pd.Series)
- 벤치마크: 동일 기간의 S&P 500 종가 (또는 비슷한 대형 벤치마크)
- 전체 universe: 동일 방식으로 계산된 RS Score 리스트 (ex: 6,000개 종목)

✅ 산식:
RS_score = 0.4 * perf_3m + 0.2 * perf_6m + 0.2 * perf_9m + 0.2 * perf_12m
perf_Xm = (현재가격 - Xm 전 가격) / Xm 전 가격

RS_rating = percentile_rank(RS_score, universe_scores)
"""

import numpy as np
import pandas as pd
from scipy.stats import percentileofscore

def calculate_relative_strength(df: pd.DataFrame, benchmark: pd.Series, universe_scores: list[float]) -> dict:
    """
    Parameters:
    - df: DataFrame with 'Close' column (length >= 252)
    - benchmark: Series of S&P 500 or comparable index (length >= 252)
    - universe_scores: List of RS Score values for all tradable stocks (used for percentile ranking)

    Returns:
    - dict with raw RS Score and RS Rating (percentile)
    """
    if len(df) < 252 or len(benchmark) < 252:
        raise ValueError("종목 및 벤치마크는 최소 252일 데이터가 필요합니다.")

    close = df['Close'].values
    bench = benchmark.values

    # 수익률 계산
    p3  = (close[-1] - close[-63])  / close[-63]  * 100
    p6  = (close[-1] - close[-126]) / close[-126] * 100
    p9  = (close[-1] - close[-189]) / close[-189] * 100
    p12 = (close[-1] - close[-252]) / close[-252] * 100

    b3  = (bench[-1] - bench[-63])  / bench[-63]  * 100
    b6  = (bench[-1] - bench[-126]) / bench[-126] * 100
    b9  = (bench[-1] - bench[-189]) / bench[-189] * 100
    b12 = (bench[-1] - bench[-252]) / bench[-252] * 100

    # 종목 성과 vs 벤치마크 성과
    stock_score = 0.4 * p3 + 0.2 * p6 + 0.2 * p9 + 0.2 * p12
    bench_score = 0.4 * b3 + 0.2 * b6 + 0.2 * b9 + 0.2 * b12
    rs_score = stock_score / bench_score * 100

    # 전체 universe에서 상대 백분위로 환산
    rs_rating = round(percentileofscore(universe_scores, rs_score, kind='rank'), 2)

    return {
        "RS_Score": round(rs_score, 2),
        "RS_Rating": rs_rating  # 0~100 사이 백분위 점수
    }
