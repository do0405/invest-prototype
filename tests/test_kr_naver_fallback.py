from __future__ import annotations

from datetime import datetime

import pandas as pd

from data_collectors import kr_naver_fallback as fallback


MARKET_HTML = """
<html>
  <body>
    <td class="pgRR"><a href="/sise/sise_market_sum.naver?sosok=0&page=2">맨뒤</a></td>
    <a href="/item/main.naver?code=005930">삼성전자</a>
    <a href="/item/main.naver?code=000660">SK하이닉스</a>
  </body>
</html>
"""

MARKET_HTML_PAGE2 = """
<html>
  <body>
    <a href="/item/main.naver?code=051910">LG화학</a>
  </body>
</html>
"""

DAILY_HTML = """
<table class="type2">
  <tr><th>날짜</th><th>종가</th><th>전일비</th><th>시가</th><th>고가</th><th>저가</th><th>거래량</th></tr>
  <tr><td>2026.02.24</td><td>104</td><td>1</td><td>101</td><td>106</td><td>100</td><td>12000</td></tr>
  <tr><td>2026.02.21</td><td>103</td><td>1</td><td>100</td><td>105</td><td>99</td><td>10000</td></tr>
</table>
"""


def test_fetch_market_listing_codes(monkeypatch):
    def fake_request_text(url: str, *, params=None):  # noqa: ANN001
        _ = url
        return MARKET_HTML_PAGE2 if params["page"] == 2 else MARKET_HTML

    monkeypatch.setattr(fallback, "_request_text", fake_request_text)

    codes = fallback.fetch_market_listing_codes("KOSPI")

    assert codes == {"005930", "000660", "051910"}


def test_fetch_kr_symbol_universe_includes_etf_and_etn(monkeypatch):
    monkeypatch.setattr(fallback, "fetch_market_listing_codes", lambda market: {"005930"} if market == "KOSPI" else {"035720"})
    monkeypatch.setattr(fallback, "fetch_etf_codes", lambda: {"069500"})
    monkeypatch.setattr(fallback, "fetch_etn_codes", lambda: {"530001"})

    symbols = fallback.fetch_kr_symbol_universe(include_kosdaq=True, include_etf=True, include_etn=True)

    assert symbols == ["005930", "035720", "069500", "530001"]


def test_fetch_daily_ohlcv(monkeypatch):
    monkeypatch.setattr(fallback, "_request_text", lambda url, *, params=None: DAILY_HTML)

    frame = fallback.fetch_daily_ohlcv(
        "005930",
        start_dt=datetime(2026, 2, 20),
        end_dt=datetime(2026, 2, 24),
    )

    assert isinstance(frame, pd.DataFrame)
    assert list(frame["날짜"]) == ["2026-02-24", "2026-02-21"]
    assert list(frame["종가"]) == [104, 103]
