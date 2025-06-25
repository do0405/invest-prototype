from pathlib import Path
from typing import List, Set, Optional
import pandas as pd
import json
import logging

from config import (
    SCREENER_RESULTS_DIR,
    LEADER_STOCK_RESULTS_DIR,
    MOMENTUM_SIGNALS_RESULTS_DIR,
    PORTFOLIO_BUY_DIR,
    MARKET_REGIME_LATEST_PATH,
)
from .criteria_weights import InvestmentStrategy


def _collect_symbols_from_csv(directory: Path, patterns: List[str]) -> Set[str]:
    """Collect ticker symbols from CSV files within ``directory``.

    Rows that explicitly mark a short/sell position are ignored to ensure
    only buy candidates are returned.
    """
    collected: Set[str] = set()

    for pattern in patterns:
        for csv_path in directory.glob(pattern):
            try:
                df = pd.read_csv(csv_path)
            except Exception as e:
                logging.warning(f"{csv_path}: 읽기 실패 - {e}")
                continue

            # Filter out short/sell rows if a column indicates position direction
            if "롱여부" in df.columns:
                df = df[df["롱여부"].astype(str).str.lower().isin(["true", "1", "yes"])]
            if "long" in df.columns:
                df = df[df["long"].astype(str).str.lower().isin(["true", "1", "yes"])]
            if "signal" in df.columns:
                df = df[df["signal"].astype(str).str.lower().str.contains("buy")]

            col = None
            if "symbol" in df.columns:
                col = "symbol"
            elif "ticker" in df.columns:
                col = "ticker"
            elif "종목명" in df.columns:
                col = "종목명"

            if col:
                collected.update(df[col].dropna().astype(str).str.upper())

    return collected


def load_all_screener_symbols(limit: Optional[int] = None) -> List[str]:
    """Load unique symbols from screener and portfolio result files.

    ``strategy2``와 ``strategy6`` 결과는 제외하고, 그 외의 모든 스크리닝 및
    포트폴리오 매수 결과에 등장하는 종목을 모아 반환한다.
    """

    symbols: Set[str] = set()

    screener_dir = Path(SCREENER_RESULTS_DIR)
    symbols.update(_collect_symbols_from_csv(screener_dir, ["**/*.csv"]))

    leader_dir = Path(LEADER_STOCK_RESULTS_DIR)
    symbols.update(_collect_symbols_from_csv(leader_dir, ["*.csv"]))

    momentum_dir = Path(MOMENTUM_SIGNALS_RESULTS_DIR)
    symbols.update(_collect_symbols_from_csv(momentum_dir, ["*.csv"]))

    portfolio_dir = Path(PORTFOLIO_BUY_DIR)
    # 명시적으로 strategy1,3,4,5만 포함해 strategy2/6은 건너뛴다
    patterns = [
        "strategy1_results.csv",
        "strategy3_results.csv",
        "strategy4_results.csv",
        "strategy5_results.csv",
    ]
    symbols.update(_collect_symbols_from_csv(portfolio_dir, patterns))

    symbol_list = sorted(symbols)
    if limit is not None:
        return symbol_list[:limit]
    return symbol_list


def get_market_regime_strategy(
    default: InvestmentStrategy = InvestmentStrategy.BALANCED,
) -> InvestmentStrategy:
    "Return an investment strategy mapped from the latest market regime." 
    path = Path(MARKET_REGIME_LATEST_PATH)
    if not path.exists():
        return default

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        regime = str(data.get("regime", "")).lower()
    except Exception as e:
        logging.warning(f"Failed to read market regime: {e}")
        return default

    if regime in {"bull", "aggressive_bull"}:
        return InvestmentStrategy.AGGRESSIVE
    if regime in {"bear", "risk_management"}:
        return InvestmentStrategy.RISK_AVERSE
    return default
