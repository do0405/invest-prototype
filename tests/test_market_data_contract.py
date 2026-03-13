from __future__ import annotations

import pandas as pd

from utils.market_data_contract import normalize_ohlcv_frame


def test_normalize_ohlcv_frame_handles_mixed_timezone_dates() -> None:
    frame = pd.DataFrame(
        {
            "date": [
                "2026-01-02 00:00:00",
                "2026-01-03T00:00:00+09:00",
                "2026-01-04T00:00:00Z",
            ],
            "open": [10.0, 11.0, 12.0],
            "high": [11.0, 12.0, 13.0],
            "low": [9.0, 10.0, 11.0],
            "close": [10.5, 11.5, 12.5],
            "volume": [100, 110, 120],
        }
    )

    normalized = normalize_ohlcv_frame(frame, symbol="AAA")

    assert len(normalized) == 3
    assert list(normalized["date"]) == ["2026-01-02", "2026-01-02", "2026-01-04"]
    assert set(normalized.columns) == {"date", "symbol", "open", "high", "low", "close", "volume"}
