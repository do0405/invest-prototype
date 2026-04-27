from __future__ import annotations

import warnings
import shutil
from pathlib import Path

import pandas as pd

from screeners.markminervini.ticker_tracker import track_new_tickers
from tests._paths import runtime_root
from utils.market_runtime import (
    get_markminervini_advanced_financial_results_path,
    get_markminervini_integrated_results_path,
    get_markminervini_new_tickers_path,
    get_markminervini_with_rs_path,
)


def test_track_new_tickers_appends_to_empty_tracker_without_future_warning(monkeypatch):
    root = runtime_root("_test_runtime_markminervini_ticker_tracker_warning")
    if root.exists():
        shutil.rmtree(root, ignore_errors=True)
    monkeypatch.setenv("INVEST_PROTO_RESULTS_DIR", str(root))
    market = "us"

    current_path = get_markminervini_with_rs_path(market)
    advanced_path = get_markminervini_advanced_financial_results_path(market)
    integrated_path = get_markminervini_integrated_results_path(market)
    tracked_path = get_markminervini_new_tickers_path(market)
    Path(current_path).parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "symbol": ["AAA"],
            "rs_score": [91.5],
            "met_count": [6],
        }
    ).to_csv(current_path, index=False)
    pd.DataFrame({"symbol": ["AAA"], "fin_met_count": [3]}).to_csv(advanced_path, index=False)
    pd.DataFrame({"symbol": ["AAA"], "met_count": [6], "total_met_count": [9]}).to_csv(
        integrated_path,
        index=False,
    )
    pd.DataFrame(
        columns=["symbol", "fin_met_count", "rs_score", "met_count", "total_met_count", "added_date"]
    ).to_csv(tracked_path, index=False)

    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        tracked = track_new_tickers(advanced_path, market=market)

    assert list(tracked["symbol"]) == ["AAA"]
    assert int(tracked.iloc[0]["fin_met_count"]) == 3
