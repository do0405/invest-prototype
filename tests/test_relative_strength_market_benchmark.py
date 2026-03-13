from __future__ import annotations

import pandas as pd

from utils.relative_strength import calculate_rs_score


def test_calculate_rs_score_supports_non_spy_benchmark_symbol():
    dates = pd.date_range("2024-01-01", periods=260, freq="D", tz="UTC")
    benchmark = pd.DataFrame(
        {
            "date": dates,
            "symbol": "KOSPI",
            "close": [100 + index * 0.2 for index in range(len(dates))],
        }
    )
    stock = pd.DataFrame(
        {
            "date": dates,
            "symbol": "AAA",
            "close": [100 + index * 0.35 for index in range(len(dates))],
        }
    )
    combined = pd.concat([benchmark, stock], ignore_index=True).set_index(["date", "symbol"])

    scores = calculate_rs_score(combined, price_col="close", use_enhanced=True, benchmark_symbol="KOSPI")

    assert "AAA" in scores.index
    assert float(scores["AAA"]) >= 0
