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


def test_collect_financial_data_hybrid_reuses_fresh_cache_without_refresh(monkeypatch):
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
            "fetch_status": "complete",
            "has_error": False,
            "error_details": "[]",
        },
    )

    def _fail_refresh(*args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("fresh financial cache should be reused without network collection")

    monkeypatch.setattr(data_fetching, "_collect_symbol_metrics", _fail_refresh)

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
    saved = pd.read_csv(cache_dir / "AAPL.csv")
    assert float(saved.iloc[-1]["annual_eps_growth"]) == 21.5
    assert result.attrs["collector_diagnostics"]["counts"]["fresh_cache_hits"] == 1
    assert result.attrs["collector_diagnostics"]["counts"]["provider_fetch_symbols"] == 0
    assert result.attrs["timings"]["provider_fetch_seconds"] == 0.0


def test_collect_financial_data_hybrid_updates_csv_cache(monkeypatch):
    cache_dir = runtime_root("_test_runtime_financial_cache_update")
    _reset_dir(cache_dir)
    monkeypatch.setattr(data_fetching, "_FINANCIAL_CACHE_DIR", str(cache_dir))

    fresh_payload = {
        "symbol": "MSFT",
        "annual_eps_growth": 32.0,
        "quarterly_revenue_growth": 15.0,
        "fetch_status": "complete",
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


def test_collect_financial_data_hybrid_reuses_stale_cache_after_rate_limit(monkeypatch):
    cache_dir = runtime_root("_test_runtime_financial_cache_stale_rate_limit")
    _reset_dir(cache_dir)
    monkeypatch.setattr(data_fetching, "_FINANCIAL_CACHE_DIR", str(cache_dir))

    stale_payload = {
        "symbol": "AAPL",
        "annual_eps_growth": 21.5,
        "quarterly_revenue_growth": 12.3,
        "fetch_status": "complete",
        "error_details": [],
        "has_error": False,
    }

    monkeypatch.setattr(data_fetching, "_load_cached_hybrid_payload", lambda *args, **kwargs: None)
    monkeypatch.setattr(data_fetching, "_load_cached_hybrid_payload_any_age", lambda *args, **kwargs: dict(stale_payload))
    monkeypatch.setattr(
        data_fetching,
        "_collect_symbol_metrics",
        lambda symbol, mode, max_retries, delay, market="us": {
            "symbol": symbol,
            "provider_symbol": None,
            "fetch_status": "rate_limited",
            "unavailable_reason": "rate limited",
            "error_details": ["HTTP Error 429: Too Many Requests"],
            "has_error": True,
        },
    )

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
    assert bool(result.iloc[0]["has_error"]) is True
    assert result.iloc[0]["fetch_status"] == "complete"
    assert result.iloc[0]["cache_status"] == "stale_reused_after_rate_limit"


def test_collect_symbol_metrics_classifies_rate_limited(monkeypatch):
    monkeypatch.setattr(
        data_fetching,
        "iter_preferred_provider_symbols",
        lambda symbol, market, strict=False: [symbol],
    )
    monkeypatch.setattr(
        data_fetching,
        "_collect_yfinance_metrics",
        lambda provider_symbol, payload, delay: (_ for _ in ()).throw(RuntimeError("HTTP Error 429: Too Many Requests")),
    )

    payload = data_fetching._collect_symbol_metrics("AAPL", mode="yfinance", max_retries=1, delay=0, market="us")

    assert payload["fetch_status"] == "rate_limited"
    assert payload["unavailable_reason"] == "rate limited"
    assert bool(payload["has_error"]) is True


def test_collect_symbol_metrics_classifies_soft_unavailable_when_no_financial_data(monkeypatch):
    monkeypatch.setattr(
        data_fetching,
        "get_security_profile",
        lambda symbol, market: {
            "symbol": symbol,
            "provider_symbol": symbol,
            "preferred_provider_symbol": symbol,
            "earnings_expected": True,
            "fundamentals_expected": True,
            "earnings_skip_reason": "",
            "security_type": "EQUITY",
        },
    )
    monkeypatch.setattr(
        data_fetching,
        "iter_preferred_provider_symbols",
        lambda symbol, market, strict=False: [symbol],
    )
    monkeypatch.setattr(data_fetching, "_collect_yfinance_metrics", lambda provider_symbol, payload, delay: False)

    payload = data_fetching._collect_symbol_metrics("AAPL", mode="yfinance", max_retries=1, delay=0, market="us")

    assert payload["fetch_status"] == "soft_unavailable"
    assert payload["unavailable_reason"] == "financial data unavailable"
    assert bool(payload["has_error"]) is True


def test_collect_symbol_metrics_uses_strict_preferred_provider_for_kr(
    monkeypatch,
):
    calls: list[str] = []

    monkeypatch.setattr(
        data_fetching,
        "iter_preferred_provider_symbols",
        lambda symbol, market, strict=False: ["005930.KS"] if strict else [symbol],
    )
    monkeypatch.setattr(
        data_fetching,
        "_collect_yfinance_metrics",
        lambda provider_symbol, payload, delay: calls.append(provider_symbol) or False,
    )

    payload = data_fetching._collect_symbol_metrics(
        "005930",
        mode="yfinance",
        max_retries=1,
        delay=0,
        market="kr",
    )

    assert calls == ["005930.KS"]
    assert payload["fetch_status"] == "soft_unavailable"


def test_collect_symbol_metrics_skips_fundamental_ineligible_symbol(monkeypatch):
    monkeypatch.setattr(
        data_fetching,
        "get_security_profile",
        lambda symbol, market: {
            "symbol": symbol,
            "provider_symbol": symbol,
            "earnings_expected": False,
            "fundamentals_expected": False,
            "earnings_skip_reason": "etf",
            "security_type": "ETF",
        },
    )
    monkeypatch.setattr(
        data_fetching,
        "iter_preferred_provider_symbols",
        lambda symbol, market, strict=False: (_ for _ in ()).throw(AssertionError("provider iteration should be skipped")),
    )

    payload = data_fetching._collect_symbol_metrics("AAAU", mode="hybrid", max_retries=1, delay=0, market="us")

    assert payload["fetch_status"] == "soft_unavailable"
    assert payload["unavailable_reason"] == "ineligible:etf"
    assert payload["source"] == "metadata_skip"
