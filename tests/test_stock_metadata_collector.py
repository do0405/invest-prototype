from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd
import pytest

from tests._paths import runtime_root
from data_collectors import stock_metadata_collector as collector


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


class _FakeKrStockModule:
    def get_nearest_business_day_in_a_week(self, day_str: str) -> str:  # noqa: ANN001
        return day_str

    def get_market_ticker_list(self, as_of_yyyymmdd: str, market: str):  # noqa: ANN001
        _ = as_of_yyyymmdd
        if market == "KOSPI":
            return ["005930"]
        if market == "ETF":
            return ["069500"]
        if market == "ETN":
            return ["530001"]
        return []


def test_get_symbols_for_kr_excludes_index_csv(monkeypatch):
    root = runtime_root("_test_runtime_kr_metadata_symbols")
    _reset_dir(root)
    kr_dir = root / "kr"
    kr_dir.mkdir(parents=True, exist_ok=True)

    (kr_dir / "005930.csv").write_text("date,symbol,close\n", encoding="utf-8")
    (kr_dir / "114800.csv").write_text("date,symbol,close\n", encoding="utf-8")
    (kr_dir / "KOSPI.csv").write_text("date,symbol,close\n", encoding="utf-8")

    monkeypatch.setattr(collector, "get_market_data_dir", lambda market: str(kr_dir))

    symbols = collector.get_symbols("kr")

    assert "005930" in symbols
    assert "114800" in symbols
    assert "KOSPI" not in symbols


def test_get_symbols_for_us_uses_seed_universe_and_excludes_special_issue(monkeypatch):
    root = runtime_root("_test_runtime_us_metadata_symbols")
    _reset_dir(root)
    data_dir = root
    us_dir = root / "us"
    us_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = data_dir / "stock_metadata.csv"

    pd.DataFrame([{"symbol": "AAPL"}, {"symbol": "SQQQ"}, {"symbol": "AACIW"}]).to_csv(
        data_dir / "nasdaq_symbols.csv",
        index=False,
    )
    pd.DataFrame([{"symbol": "GLD"}]).to_csv(metadata_path, index=False)

    monkeypatch.setattr(collector, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(collector, "get_market_data_dir", lambda market: str(us_dir))
    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))

    symbols = collector.get_symbols("us")

    assert "AAPL" in symbols
    assert "SQQQ" in symbols
    assert "GLD" in symbols
    assert "AACIW" not in symbols
    assert "^VIX" in symbols


def test_get_symbols_for_us_recovers_symbol_from_safe_filename_alias(monkeypatch):
    root = runtime_root("_test_runtime_us_metadata_safe_filename")
    _reset_dir(root)
    data_dir = root
    us_dir = root / "us"
    us_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = data_dir / "stock_metadata.csv"

    (us_dir / "CON_file.csv").write_text(
        "date,symbol,close\n2026-03-11,CON,20.5\n",
        encoding="utf-8",
    )
    (us_dir / "N_A.csv").write_text("date,symbol,close\n", encoding="utf-8")
    pd.DataFrame([{"symbol": "GLD"}]).to_csv(metadata_path, index=False)

    monkeypatch.setattr(collector, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(collector, "get_market_data_dir", lambda market: str(us_dir))
    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))

    symbols = collector.get_symbols("us")

    assert "CON" in symbols
    assert "N_A" not in symbols


def test_get_symbols_for_kr_uses_collectable_universe_including_etf_and_etn(monkeypatch):
    root = runtime_root("_test_runtime_kr_metadata_collectable_symbols")
    _reset_dir(root)
    kr_dir = root / "kr"
    kr_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(collector, "get_market_data_dir", lambda market: str(kr_dir))

    symbols = collector.get_symbols("kr", stock_module=_FakeKrStockModule())

    assert "005930" in symbols
    assert "069500" in symbols
    assert "530001" in symbols


def test_metadata_main_prints_target_summary_and_saved_message(monkeypatch, capsys):
    root = runtime_root("_test_runtime_metadata_progress_output")
    _reset_dir(root)
    metadata_path = root / "stock_metadata.csv"

    monkeypatch.setattr(collector, "get_symbols", lambda market="us", stock_module=None: ["AAA", "BBB", "CCC"])
    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))
    monkeypatch.setattr(
        collector,
        "load_cached_metadata",
        lambda market="us", max_age_days=7, allow_stale=True: pd.DataFrame([{"symbol": "AAA"}]),
    )
    monkeypatch.setattr(
        collector,
        "collect_stock_metadata",
        lambda symbols, **kwargs: pd.DataFrame([{"symbol": symbol, "market": "us"} for symbol in symbols]),
    )
    monkeypatch.setattr(
        collector,
        "merge_metadata",
        lambda cached_df, new_df, *, market: pd.DataFrame(
            [{"symbol": "AAA", "market": "us"}, {"symbol": "BBB", "market": "us"}, {"symbol": "CCC", "market": "us"}]
        ),
    )

    collector.main(market="us")

    captured = capsys.readouterr()
    assert "[Metadata] Batch 1/1 (us) - size=2, processed=0/2" in captured.out
    assert "[Metadata] Checkpoint saved (us) - processed=2/2" in captured.out
    assert "[Metadata] Target summary (us) - total=3, cached=1, missing=2" in captured.out
    assert "[Metadata] Saved (us) - total=3" in captured.out


def test_collect_stock_metadata_prints_progress(monkeypatch, capsys):
    monkeypatch.setattr(
        collector,
        "fetch_metadata",
        lambda symbol, **kwargs: {"symbol": symbol, "market": "us", "sector": "Tech"},
    )

    collector.collect_stock_metadata(["AAA", "BBB", "CCC", "DDD", "EEE"], market="us", max_workers=2, delay=0)

    captured = capsys.readouterr()
    assert "[Metadata] Fetch started (us) - total=5, workers=2" in captured.out
    assert "[Metadata] Progress (us) - completed=5/5, success=5" in captured.out


def test_load_cached_metadata_reuses_stale_cache(monkeypatch):
    root = runtime_root("_test_runtime_metadata_stale_cache")
    _reset_dir(root)
    metadata_path = root / "stock_metadata.csv"
    pd.DataFrame([{"symbol": "AAA", "market": "us"}]).to_csv(metadata_path, index=False)
    stale_time = time.time() - (10 * 24 * 3600)
    os.utime(metadata_path, (stale_time, stale_time))

    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))

    cached = collector.load_cached_metadata("us", max_age_days=7, allow_stale=True)

    assert cached is not None
    assert list(cached["symbol"]) == ["AAA"]


def test_metadata_main_raises_for_empty_symbol_universe(monkeypatch):
    root = runtime_root("_test_runtime_metadata_empty_universe")
    _reset_dir(root)
    metadata_path = root / "stock_metadata.csv"

    monkeypatch.setattr(collector, "get_stock_metadata_path", lambda market: str(metadata_path))
    monkeypatch.setattr(collector, "get_symbols", lambda market="us", stock_module=None: [])

    with pytest.raises(RuntimeError, match="Metadata symbol universe is empty"):
        collector.main(market="kr")
