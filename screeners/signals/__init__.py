from __future__ import annotations

from typing import TYPE_CHECKING, Any

# Canonical public names are MultiScreenerSignalEngine and
# run_multi_screener_signal_scan. The shorter names below remain compatibility
# aliases for older operator scripts.
if TYPE_CHECKING:
    from .engine import (
        MultiScreenerSignalEngine,
        PEGImminentScreener,
        QullamaggieSignalEngine,
        SignalEngine,
        run_multi_screener_signal_scan,
        run_peg_imminent_screen,
        run_qullamaggie_signal_scan,
        run_signal_scan,
    )

__all__ = [
    "MultiScreenerSignalEngine",
    "PEGImminentScreener",
    "SignalEngine",
    "QullamaggieSignalEngine",
    "run_signal_scan",
    "run_multi_screener_signal_scan",
    "run_peg_imminent_screen",
    "run_qullamaggie_signal_scan",
]



def __getattr__(name: str) -> Any:
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from . import engine as _engine

    return getattr(_engine, name)
