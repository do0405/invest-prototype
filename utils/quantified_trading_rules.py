"""Quantified trading rules system.

This module implements a minimal rule-based framework
as described in ``quantified-trading-rules.md``.
It provides helper methods for leader stock and IPO
conditions which can be extended later.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any
import pandas as pd

from screeners.momentum_signals.indicators import (
    calculate_moving_averages,
    calculate_volume_indicators,
    detect_cup_and_handle,
)


def momentum_signal(df: pd.DataFrame, rs_score: float, sector_rs: float) -> Dict[str, bool]:
    """Check Stan Weinstein Stage 2A momentum signal.

    Parameters
    ----------
    df : pd.DataFrame
        Price dataframe containing ``close`` and ``volume`` columns.
    rs_score : float
        Relative strength score of the stock.
    sector_rs : float
        Relative strength percentile of the stock's sector.

    Returns
    -------
    Dict[str, bool]
        Dictionary with each condition and overall ``signal`` key.
    """

    df = df.copy()
    if "sma_30w" not in df.columns:
        df = calculate_moving_averages(df)
    if "volume_sma_20" not in df.columns:
        df = calculate_volume_indicators(df)

    cond_30w = len(df) >= 3 and all(
        df.iloc[-i]["close"] > df.iloc[-i]["sma_30w"] for i in range(1, 4)
    )
    cond_stage2a = (
        len(df) >= 50
        and df.iloc[-1]["sma_10"] > df.iloc[-10]["sma_10"]
        and df.iloc[-1]["sma_30w"] >= df.iloc[-10]["sma_30w"]
    )
    cond_volume = (
        df.iloc[-1]["volume"] >= df.iloc[-1]["volume_sma_20"] * 1.5
    )
    cond_rs = rs_score >= 70
    cond_sector = sector_rs >= 60
    cond_pattern = detect_cup_and_handle(df)

    signal = cond_30w and cond_stage2a and cond_volume and cond_rs and cond_sector

    return {
        "above_30w_3d": cond_30w,
        "stage_2a": cond_stage2a,
        "volume_surge": cond_volume,
        "rs_rank": cond_rs,
        "sector_strong": cond_sector,
        "cup_handle_breakout": cond_pattern,
        "signal": signal,
    }


@dataclass
class QuantifiedTradingSystem:
    """Rule based trading system"""

    fear_greed_index: float = 0.0
    market_stage: str = ""
    sector_rs_threshold: int = 70

    def get_sector_rs(self, sector: str) -> float:
        """Placeholder for sector relative strength."""
        # In real use this should fetch actual RS score
        return 0.0

    def get_vix(self) -> float:
        """Placeholder for market volatility index."""
        return 0.0

    def check_leader_stock_conditions(self, stock_data: Dict[str, Any]) -> bool:
        """Return True if leader stock conditions are met."""
        conditions = {
            "price_above_30w_ma": stock_data.get("close", 0) > stock_data.get("sma_30w", 0),
            "volume_confirmation": stock_data.get("volume", 0) >= stock_data.get("avg_volume_20d", 0) * 1.5,
            "rsi_oversold_exit": stock_data.get("rsi_14", 0) > 30,
            "sector_strength": self.get_sector_rs(stock_data.get("sector", "")) >= 70,
        }
        return all(conditions.values())

    def check_ipo_track1_conditions(self, ipo_data: Dict[str, Any]) -> Dict[str, bool]:
        """Conservative accumulation track conditions."""
        return {
            "price_below_ipo": ipo_data.get("current_price", 0) < ipo_data.get("ipo_price", 0) * 0.9,
            "rsi_oversold": ipo_data.get("rsi_14", 0) < 30,
            "low_volume": ipo_data.get("volume", 0) < ipo_data.get("avg_volume", 0) * 0.5,
            "market_vix": self.get_vix() < 25,
        }

    def check_ipo_track2_conditions(self, ipo_data: Dict[str, Any]) -> Dict[str, bool]:
        """Aggressive momentum track conditions."""
        return {
            "momentum_breakout": ipo_data.get("price_change_5d", 0) > 0.2,
            "macd_signal": ipo_data.get("macd", 0) > ipo_data.get("macd_signal", 0),
            "volume_surge": ipo_data.get("volume", 0) >= ipo_data.get("avg_volume", 0) * 3,
            "institutional_buying": ipo_data.get("institution_net_buy_3d", 0) > 0,
        }

