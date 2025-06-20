# -*- coding: utf-8 -*-
# 쿨라매기 매매법 알고리즘 모듈

# 주요 모듈 임포트
from .core import (
    apply_basic_filters,
    screen_breakout_setup,
    check_vcp_pattern,
    screen_episode_pivot_setup,
    screen_parabolic_short_setup,
)

from .screener import run_qullamaggie_screening

from .signal_generator import (
    generate_buy_signals,
    generate_sell_signals,
    manage_positions,
)

from .runner import run_qullamaggie_strategy

__all__ = [
    "apply_basic_filters",
    "screen_breakout_setup",
    "check_vcp_pattern",
    "screen_episode_pivot_setup",
    "screen_parabolic_short_setup",
    "generate_buy_signals",
    "generate_sell_signals",
    "manage_positions",
    "run_qullamaggie_screening",
    "run_qullamaggie_strategy",
]

__version__ = "1.0.0"
