from __future__ import annotations

from utils.calc_utils import clean_tickers


def test_clean_tickers_emits_ascii_safe_warning(capsys):
    cleaned = clean_tickers(["BAD<TICK>"])

    captured = capsys.readouterr()
    assert cleaned == []
    assert "WARNING abnormal ticker excluded" in captured.out
    assert "⚠" not in captured.out
