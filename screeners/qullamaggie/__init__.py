from __future__ import annotations

import importlib.util
from typing import TYPE_CHECKING

from .core import MarketRegime, QullamaggieAnalyzer
from .screener import (
    apply_basic_filters,
    check_vcp_pattern,
    run_qullamaggie_screening,
    screen_breakout_setup,
    screen_episode_pivot_setup,
    screen_parabolic_short_setup,
)

if TYPE_CHECKING:
    from screeners.signals import (
        MultiScreenerSignalEngine,
        PEGImminentScreener,
        QullamaggieSignalEngine,
        SignalEngine,
        run_multi_screener_signal_scan,
        run_peg_imminent_screen,
        run_qullamaggie_signal_scan,
        run_signal_scan,
    )

_LEGACY_SIGNAL_EXPORTS = (
    "PEGImminentScreener",
    "MultiScreenerSignalEngine",
    "QullamaggieSignalEngine",
    "SignalEngine",
    "run_signal_scan",
    "run_multi_screener_signal_scan",
    "run_peg_imminent_screen",
    "run_qullamaggie_signal_scan",
)

try:
    _HAS_LEGACY_SIGNAL_EXPORTS = importlib.util.find_spec("screeners.signals") is not None
except (ImportError, ValueError):
    _HAS_LEGACY_SIGNAL_EXPORTS = False

__all__ = [
    "MarketRegime",
    "QullamaggieAnalyzer",
    "apply_basic_filters",
    "check_vcp_pattern",
    "screen_breakout_setup",
    "screen_episode_pivot_setup",
    "screen_parabolic_short_setup",
    "run_qullamaggie_screening",
]

if _HAS_LEGACY_SIGNAL_EXPORTS:
    __all__.extend(_LEGACY_SIGNAL_EXPORTS)


def __getattr__(name: str):
    if _HAS_LEGACY_SIGNAL_EXPORTS and name in _LEGACY_SIGNAL_EXPORTS:
        from screeners import signals as _signals

        return getattr(_signals, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
