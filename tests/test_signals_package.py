from __future__ import annotations

import importlib
import sys

from screeners.source_contracts import CANONICAL_SOURCE_SPECS


def test_signals_package_import_is_lazy() -> None:
    sys.modules.pop("screeners.signals", None)
    sys.modules.pop("screeners.signals.engine", None)

    signals = importlib.import_module("screeners.signals")

    assert "screeners.signals.engine" not in sys.modules

    _ = signals.MultiScreenerSignalEngine

    assert "screeners.signals.engine" in sys.modules


def test_signal_engine_uses_shared_source_contract() -> None:
    engine = importlib.import_module("screeners.signals.engine")

    assert engine._SOURCE_SPECS is CANONICAL_SOURCE_SPECS
