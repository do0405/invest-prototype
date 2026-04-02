from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .engine import (
        MultiScreenerSignalEngine,
        PEGImminentScreener,
        run_multi_screener_signal_scan,
    )

__all__ = [
    "MultiScreenerSignalEngine",
    "PEGImminentScreener",
    "run_multi_screener_signal_scan",
]



def __getattr__(name: str) -> Any:
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from . import engine as _engine

    return getattr(_engine, name)
