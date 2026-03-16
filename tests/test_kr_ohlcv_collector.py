from __future__ import annotations

import pandas as pd

from data_collectors.kr_ohlcv_collector import _normalize_kr_ohlcv_frame


def test_normalize_kr_ohlcv_frame_with_korean_headers():
    raw = pd.DataFrame(
        {
            "시가": [10, 11],
            "고가": [11, 12],
            "저가": [9, 10],
            "종가": [10.5, 11.5],
            "거래량": [1000, 2000],
        },
        index=pd.to_datetime(["2026-01-01", "2026-01-02"]),
    )

    frame = _normalize_kr_ohlcv_frame(raw, ticker="005930")
    assert list(frame.columns) == [
        "date",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adj_close",
        "dividends",
        "stock_splits",
        "split_factor",
    ]
    assert frame.iloc[-1]["symbol"] == "005930"
    assert float(frame.iloc[-1]["close"]) == 11.5
    assert float(frame.iloc[-1]["adj_close"]) == 11.5
    assert float(frame.iloc[-1]["dividends"]) == 0.0
    assert float(frame.iloc[-1]["stock_splits"]) == 0.0
    assert float(frame.iloc[-1]["split_factor"]) == 1.0
