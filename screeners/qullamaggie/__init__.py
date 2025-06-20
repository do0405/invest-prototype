# -*- coding: utf-8 -*-
# 쿨라매기 매매법 알고리즘 모듈

# 주요 모듈 임포트
from .core import (
    screen_breakout_setup,
    screen_episode_pivot_setup,
    screen_parabolic_short_setup,
)

from .signal_generator import (
    generate_buy_signals,
    generate_sell_signals,
    manage_positions,
)

from .runner import run_qullamaggie_strategy

__all__ = [
    "screen_breakout_setup",
    "screen_episode_pivot_setup",
    "screen_parabolic_short_setup",
    "generate_buy_signals",
    "generate_sell_signals",
    "manage_positions",
    "run_qullamaggie_strategy",
]

__version__ = "1.0.0"
