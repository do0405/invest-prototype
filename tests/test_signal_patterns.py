from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import screeners.signals.patterns as signal_patterns


def test_detect_pocket_pivot_uses_recent_down_volume_max() -> None:
    frame = pd.DataFrame(
        [
            {
                "date": f"2026-03-{index + 1:02d}",
                "open": 10.0,
                "high": 10.3,
                "low": 9.8,
                "close": close,
                "volume": volume,
            }
            for index, (close, volume) in enumerate(
                [
                    (9.8, 100.0),
                    (10.2, 80.0),
                    (9.7, 140.0),
                    (10.1, 90.0),
                    (9.6, 180.0),
                    (10.0, 120.0),
                    (9.5, 210.0),
                    (10.1, 110.0),
                    (9.9, 160.0),
                    (10.0, 130.0),
                    (10.4, 211.0),
                ]
            )
        ]
    )
    frame.loc[frame.index[-1], ["open", "high", "low", "close"]] = [
        10.0,
        10.6,
        9.95,
        10.55,
    ]

    result = signal_patterns.detect_pocket_pivot(frame)

    assert result["pocket_pivot"] is True
    assert result["pocket_pivot_down_volume_max"] == pytest.approx(210.0)
    assert "POCKET_PIVOT_DOWN_VOLUME_MAX" in result["reason_codes"]


def test_detect_pocket_pivot_rejects_weak_close_or_low_volume() -> None:
    frame = pd.DataFrame(
        [
            {
                "date": f"2026-03-{index + 1:02d}",
                "open": 10.0,
                "high": 10.3,
                "low": 9.8,
                "close": close,
                "volume": volume,
            }
            for index, (close, volume) in enumerate(
                [
                    (9.8, 100.0),
                    (10.2, 80.0),
                    (9.7, 140.0),
                    (10.1, 90.0),
                    (9.6, 180.0),
                    (10.0, 120.0),
                    (9.5, 210.0),
                    (10.1, 110.0),
                    (9.9, 160.0),
                    (10.0, 130.0),
                    (10.5, 209.0),
                ]
            )
        ]
    )
    frame.loc[frame.index[-1], ["open", "high", "low", "close"]] = [
        10.0,
        10.6,
        9.95,
        10.55,
    ]

    result = signal_patterns.detect_pocket_pivot(frame)

    assert result["pocket_pivot"] is False
    assert "POCKET_PIVOT_VOLUME_FAIL" in result["reason_codes"]


def test_score_band_reversion_gates_pbb_and_pbs_with_band_quality() -> None:
    pbb = signal_patterns.score_band_reversion(
        pd.DataFrame(),
        {
            "close": 101.0,
            "open": 99.0,
            "high": 102.0,
            "low": 95.0,
            "bb_lower": 96.0,
            "bb_mid": 103.0,
            "bb_upper": 110.0,
            "bb_z_score": -0.8,
            "bb_percent_b": 0.20,
            "rsi14": 44.0,
            "daily_return_pct": 1.0,
            "close_position_pct": 0.86,
            "above_200ma": True,
            "alignment_state": "BULLISH",
            "support_trend_rising": True,
            "ema_turn_down": False,
            "pullback_profile_pass": True,
            "risk_heat": False,
        },
    )
    assert pbb["pbb_ready"] is True
    assert pbb["pbs_ready"] is False
    assert "PBB_BAND_SUPPORT" in pbb["reason_codes"]

    pbs = signal_patterns.score_band_reversion(
        pd.DataFrame(),
        {
            "close": 99.0,
            "open": 102.0,
            "high": 104.0,
            "low": 98.0,
            "bb_lower": 94.0,
            "bb_mid": 101.0,
            "bb_upper": 108.0,
            "bb_z_score": -0.2,
            "bb_percent_b": 0.42,
            "rsi14": 46.0,
            "daily_return_pct": -2.0,
            "close_position_pct": 0.17,
        },
    )
    assert pbs["pbs_ready"] is True
    assert pbs["pbb_ready"] is False
    assert "PBS_FAILED_RECLAIM" in pbs["reason_codes"]


def test_score_band_reversion_gates_mr_long_and_mr_short() -> None:
    mr_long = signal_patterns.score_band_reversion(
        pd.DataFrame(),
        {
            "close": 96.5,
            "open": 95.2,
            "high": 97.0,
            "low": 93.8,
            "bb_lower": 95.0,
            "bb_mid": 103.0,
            "bb_upper": 111.0,
            "bb_z_score": -1.3,
            "bb_percent_b": 0.08,
            "rsi14": 35.0,
            "daily_return_pct": 1.4,
            "close_position_pct": 0.84,
        },
    )
    assert mr_long["mr_long_ready"] is True
    assert "MR_LONG_OVERSOLD_RECLAIM" in mr_long["reason_codes"]

    mr_short = signal_patterns.score_band_reversion(
        pd.DataFrame(),
        {
            "close": 109.0,
            "open": 112.0,
            "high": 113.0,
            "low": 108.0,
            "bb_lower": 92.0,
            "bb_mid": 101.0,
            "bb_upper": 110.0,
            "bb_z_score": 1.4,
            "bb_percent_b": 0.94,
            "rsi14": 72.0,
            "daily_return_pct": -2.4,
            "close_position_pct": 0.20,
        },
    )
    assert mr_short["mr_short_ready"] is True
    assert "MR_SHORT_OVERHEAT_REJECT" in mr_short["reason_codes"]


def test_score_exit_pressure_returns_chandelier_candidate_and_never_down_effective_stop() -> None:
    frame = pd.DataFrame(
        [
            {
                "date": f"2026-03-{index + 1:02d}",
                "open": 90.0 + index * 0.2,
                "high": 92.0 + index * 0.4,
                "low": 88.0 + index * 0.2,
                "close": 91.0 + index * 0.3,
                "volume": 1_000_000.0,
            }
            for index in range(25)
        ]
    )
    frame["high"] = np.minimum(frame["high"].to_numpy(dtype=float), 100.0)
    frame.loc[frame.index[-1], "high"] = 100.0

    result = signal_patterns.score_exit_pressure(
        frame,
        {
            "atr14": 5.0,
            "close": 86.0,
            "high": 90.0,
            "low": 84.0,
            "in_channel8": False,
        },
        {
            "trailing_level": 80.0,
            "protected_stop_level": 82.0,
            "support_zone_low": 70.0,
        },
    )

    assert result["chandelier_long_stop"] == pytest.approx(85.0)
    assert result["effective_trailing_level"] == pytest.approx(85.0)
    assert result["trailing_break"] is False
    assert "ATR_CHANDELIER_22_3" in result["reason_codes"]

    never_down = signal_patterns.score_exit_pressure(
        frame,
        {
            "atr14": 5.0,
            "close": 86.0,
            "high": 90.0,
            "low": 84.0,
            "in_channel8": False,
        },
        {
            "trailing_level": 90.0,
            "protected_stop_level": 92.0,
            "support_zone_low": 70.0,
        },
    )
    assert never_down["effective_trailing_level"] == pytest.approx(92.0)
