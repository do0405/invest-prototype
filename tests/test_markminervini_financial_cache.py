from __future__ import annotations

from pathlib import Path

import pandas as pd

from tests._paths import runtime_root
from screeners.markminervini import data_fetching


def _write_cache_row(cache_dir: Path, symbol: str, row: dict) -> Path:
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"{symbol}.csv"
    pd.DataFrame([row]).to_csv(path, index=False)
    return path


def _reset_dir(path: Path) -> None:
    if path.exists():
        for child in sorted(path.rglob("*"), reverse=True):
            try:
                if child.is_file():
                    child.unlink()
                elif child.is_dir():
                    child.rmdir()
            except Exception:
                continue
    path.mkdir(parents=True, exist_ok=True)


def test_collect_financial_data_hybrid_uses_fresh_csv_cache(monkeypatch):
    cache_dir = runtime_root("_test_runtime_financial_cache_fresh")
    _reset_dir(cache_dir)
    monkeypatch.setattr(data_fetching, "_FINANCIAL_CACHE_DIR", str(cache_dir))

    _write_cache_row(
        cache_dir,
        "AAPL",
        {
            "symbol": "AAPL",
            "annual_eps_growth": 21.5,
            "quarterly_revenue_growth": 12.3,
            "has_error": False,
            "error_details": "[]",
        },
    )

    def _fail_external(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("external provider should not be called when fresh cache exists")

    monkeypatch.setattr(data_fetching.yf, "Ticker", _fail_external)
    monkeypatch.setattr(data_fetching, "Ticker", _fail_external)

    result = data_fetching.collect_financial_data_hybrid(
        ["AAPL"],
        max_retries=1,
        delay=0,
        use_cache=True,
        cache_max_age_hours=24,
    )

    assert not result.empty
    assert result.iloc[0]["symbol"] == "AAPL"
    assert float(result.iloc[0]["annual_eps_growth"]) == 21.5


def test_collect_financial_data_hybrid_updates_csv_cache(monkeypatch):
    cache_dir = runtime_root("_test_runtime_financial_cache_update")
    _reset_dir(cache_dir)
    monkeypatch.setattr(data_fetching, "_FINANCIAL_CACHE_DIR", str(cache_dir))

    fresh_payload = {
        "symbol": "MSFT",
        "annual_eps_growth": 32.0,
        "quarterly_revenue_growth": 15.0,
        "error_details": [],
        "has_error": False,
    }

    monkeypatch.setattr(
        data_fetching,
        "_collect_symbol_metrics",
        lambda symbol, mode, max_retries, delay: {**fresh_payload, "symbol": symbol},
    )

    result = data_fetching.collect_financial_data_hybrid(
        ["MSFT"],
        max_retries=1,
        delay=0,
        use_cache=True,
        cache_max_age_hours=0,
    )

    assert not result.empty
    cache_file = cache_dir / "MSFT.csv"
    assert cache_file.exists()
    saved = pd.read_csv(cache_file)
    assert saved.iloc[-1]["symbol"] == "MSFT"
    assert float(saved.iloc[-1]["annual_eps_growth"]) == 32.0
