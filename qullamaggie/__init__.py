# -*- coding: utf-8 -*-
# 쿨라매기 매매법 알고리즘 모듈

# 주요 모듈 임포트
from qullamaggie.screener import (
    screen_breakout_setup,
    screen_episode_pivot_setup,
    screen_parabolic_short_setup
)

from qullamaggie.signal_generator import (
    generate_buy_signals,
    generate_sell_signals,
    manage_positions
)

from qullamaggie.main import run_qullamaggie_strategy

# 버전 정보
__version__ = '1.0.0'