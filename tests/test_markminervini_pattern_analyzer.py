from __future__ import annotations

import numpy as np
import pandas as pd

from screeners.markminervini.enhanced_pattern_analyzer import EnhancedPatternAnalyzer


def _build_frame(
    closes: np.ndarray,
    volumes: np.ndarray,
    spreads: np.ndarray,
    start: str = "2025-01-02",
) -> pd.DataFrame:
    dates = pd.bdate_range(start=start, periods=len(closes))
    opens = np.concatenate(([closes[0]], closes[:-1]))
    highs = closes * (1.0 + spreads)
    lows = closes * (1.0 - spreads)
    return pd.DataFrame(
        {
            "Date": dates,
            "Open": opens,
            "High": highs,
            "Low": lows,
            "Close": closes,
            "Volume": volumes,
        }
    ).set_index("Date")


def _synthetic_vcp_frame() -> pd.DataFrame:
    pretrend = np.linspace(50.0, 88.0, 170)
    x = np.arange(50)
    key_x = np.array([0, 4, 9, 14, 19, 24, 29, 34, 39, 44, 49])
    key_y = np.array([90.0, 100.0, 82.0, 98.0, 89.0, 99.0, 94.0, 100.0, 98.8, 100.1, 104.0])
    base = np.interp(x, key_x, key_y)
    closes = np.concatenate([pretrend, base])

    pre_spreads = np.linspace(0.035, 0.025, len(pretrend))
    base_spreads = np.concatenate(
        [
            np.linspace(0.06, 0.045, 15),
            np.linspace(0.04, 0.025, 15),
            np.linspace(0.02, 0.012, 20),
        ]
    )
    spreads = np.concatenate([pre_spreads, base_spreads])

    pre_volumes = np.linspace(1_100_000, 1_600_000, len(pretrend))
    base_volumes = np.concatenate(
        [
            np.linspace(2_000_000, 1_500_000, 15),
            np.linspace(1_400_000, 900_000, 15),
            np.linspace(850_000, 650_000, 17),
            np.array([2_700_000, 3_000_000, 3_300_000]),
        ]
    )
    volumes = np.concatenate([pre_volumes, base_volumes])
    return _build_frame(closes, volumes, spreads)


def _synthetic_cup_handle_frame() -> pd.DataFrame:
    pretrend = np.linspace(35.0, 76.0, 150)
    t = np.linspace(-1.0, 1.0, 65)
    cup = 82.0 - 16.0 * (1.0 - np.square(t))
    handle = np.array([80.4, 80.0, 79.6, 79.2, 78.9, 79.1, 79.4, 79.8, 80.1, 80.5])
    breakout = np.array([80.8, 82.9, 84.0, 84.6])
    closes = np.concatenate([pretrend, cup, handle, breakout])

    pre_spreads = np.linspace(0.035, 0.025, len(pretrend))
    cup_spreads = np.linspace(0.04, 0.022, len(cup))
    handle_spreads = np.linspace(0.018, 0.012, len(handle))
    breakout_spreads = np.array([0.014, 0.018, 0.02, 0.02])
    spreads = np.concatenate([pre_spreads, cup_spreads, handle_spreads, breakout_spreads])

    pre_volumes = np.linspace(1_200_000, 1_700_000, len(pretrend))
    cup_volumes = np.concatenate(
        [
            np.linspace(1_700_000, 1_300_000, 30),
            np.linspace(1_250_000, 1_450_000, 35),
        ]
    )
    handle_volumes = np.linspace(900_000, 700_000, len(handle))
    breakout_volumes = np.array([1_050_000, 2_600_000, 2_900_000, 3_100_000])
    volumes = np.concatenate([pre_volumes, cup_volumes, handle_volumes, breakout_volumes])
    return _build_frame(closes, volumes, spreads)


def test_detects_structural_vcp_breakout_recent() -> None:
    analyzer = EnhancedPatternAnalyzer()
    patterns = analyzer.analyze_patterns_enhanced("VCPX", _synthetic_vcp_frame())
    vcp = patterns["vcp"]

    assert vcp["detected"] is True
    assert vcp["state_detail"] == "BREAKOUT_VCP_RECENT"
    assert vcp["pivot_price"] is not None
    assert vcp["breakout_date"] is not None
    assert vcp["volume_multiple"] is not None
    assert vcp["volume_multiple"] >= 1.4
    assert vcp["metrics"]["contractions"] >= 2


def test_detects_structural_cup_handle_breakout_recent() -> None:
    analyzer = EnhancedPatternAnalyzer()
    patterns = analyzer.analyze_patterns_enhanced("CUPX", _synthetic_cup_handle_frame())
    cup_handle = patterns["cup_handle"]

    assert cup_handle["detected"] is True
    assert cup_handle["state_detail"] == "BREAKOUT_CWH_RECENT"
    assert cup_handle["pivot_price"] is not None
    assert cup_handle["breakout_date"] is not None
    assert cup_handle["metrics"]["cup_len"] >= 30
    assert cup_handle["metrics"]["handle_len"] >= 5


def test_pattern_analyzer_requires_prd_minimum_bar_count() -> None:
    analyzer = EnhancedPatternAnalyzer()
    closes = np.linspace(20.0, 40.0, 180)
    volumes = np.full(180, 1_500_000.0)
    spreads = np.full(180, 0.02)
    patterns = analyzer.analyze_patterns_enhanced("SHORT", _build_frame(closes, volumes, spreads))

    assert patterns["vcp"]["detected"] is False
    assert patterns["cup_handle"]["detected"] is False
