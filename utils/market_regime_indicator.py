# -*- coding: utf-8 -*-
"""Market Regime Classification Indicator.

이 모듈은 시장 국면을 판단하기 위한 정량적 규칙 기반 지표를 제공합니다.
다양한 기술적 지표와 시장 지수를 분석하여 현재 시장 상태를 5가지 국면으로 분류합니다.

1. 공격적 상승장 (Aggressive Bull Market): 80-100점
2. 상승장 (Bull Market): 60-79점
3. 조정장 (Correction Market): 40-59점
4. 위험 관리장 (Risk Management Market): 20-39점
5. 완전한 약세장 (Full Bear Market): 0-19점
This module uses several daily datasets under ``DATA_DIR``:

* ``DATA_US_DIR`` - price history for major indices.
* ``BREADTH_DATA_DIR/advance_decline.csv`` - market breadth advancing/declining issues.
* ``BREADTH_DATA_DIR/high_low.csv`` - daily new highs and lows.
* ``OPTION_DATA_DIR/put_call_ratio.csv`` - option market put/call ratio.

Files must contain a ``date`` column and corresponding value columns (e.g. ``ratio``,
``highs``, ``lows``, ``advancing`` and ``declining``). Latest rows are used for
calculations.
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union

from config import (
    DATA_US_DIR,
    BREADTH_DATA_DIR,
    OPTION_DATA_DIR,
    RESULTS_DIR,
    MARKET_REGIME_DIR,
    MARKET_REGIME_CRITERIA,
)
from utils.calc_utils import get_us_market_today
from .market_regime_helpers import INDEX_TICKERS, MARKET_REGIMES, load_index_data, calculate_high_low_index, calculate_advance_decline_trend, calculate_put_call_ratio
from .market_regime_calc import calculate_market_score, get_market_regime, get_regime_description, get_investment_strategy, analyze_market_regime
__all__ = [
    "analyze_market_regime",
    "calculate_market_score",
    "get_market_regime",
    "get_regime_description",
    "get_investment_strategy",
]




if __name__ == "__main__":
    # 모듈 테스트
    result = analyze_market_regime()
    print(f"\n📊 시장 국면 분석 결과 (점수: {result['score']})")
    print(f"🔍 현재 국면: {result['regime_name']}")
    print(f"📝 설명: {result['description']}")
    print(f"💡 투자 전략: {result['strategy']}")

