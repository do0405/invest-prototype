"""Quantified trading rules system.

This module implements a minimal rule-based framework
as described in ``quantified-trading-rules.md``.
It provides helper methods for leader stock and IPO
conditions which can be extended later.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any


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

