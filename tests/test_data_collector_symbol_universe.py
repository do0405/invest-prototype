from __future__ import annotations

import shutil
from pathlib import Path

from tests._paths import runtime_root as runtime_test_root
import pandas as pd

import data_collector as dc


def _write_minimal_csv(path: Path, rows: list[dict[str, object]]) -> None:
    frame = pd.DataFrame(rows)
    frame.to_csv(path, index=False)


def _reset_runtime_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def test_load_us_symbol_universe_includes_etf_inverse_and_vix(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_symbol_universe")
    _reset_runtime_dir(runtime_root)

    data_dir = runtime_root
    us_dir = data_dir / "us"
    us_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = data_dir / "stock_metadata.csv"

    # Existing CSV symbol.
    (us_dir / "QQQ.csv").write_text("date,symbol,close\n", encoding="utf-8")

    # External symbol seeds (includes ETF/inverse symbols).
    _write_minimal_csv(
        data_dir / "nasdaq_symbols.csv",
        [{"symbol": "SQQQ"}, {"symbol": "SOXL"}, {"symbol": "UVXY"}],
    )
    _write_minimal_csv(metadata_path, [{"symbol": "SPY"}])

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "DATA_US_DIR", str(us_dir))
    monkeypatch.setattr(dc, "STOCK_METADATA_PATH", str(metadata_path))

    universe = dc._load_us_symbol_universe()

    assert "QQQ" in universe
    assert "SQQQ" in universe
    assert "SOXL" in universe
    assert "UVXY" in universe
    assert "^VIX" in universe
    assert "^VVIX" in universe


def test_update_symbol_list_writes_new_csvs_to_existing_data_us_dir(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_symbol_update")
    _reset_runtime_dir(runtime_root)

    data_dir = runtime_root
    us_dir = data_dir / "us"
    us_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = data_dir / "stock_metadata.csv"

    # Existing symbol.
    (us_dir / "SPY.csv").write_text("date,symbol,open,high,low,close,volume\n", encoding="utf-8")

    # New symbol seed.
    _write_minimal_csv(data_dir / "nasdaq_symbols.csv", [{"symbol": "SQQQ"}])
    _write_minimal_csv(metadata_path, [{"symbol": "SPY"}])

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "DATA_US_DIR", str(us_dir))
    monkeypatch.setattr(dc, "STOCK_METADATA_PATH", str(metadata_path))

    symbols = dc.update_symbol_list()

    assert "SPY" in symbols
    assert "SQQQ" in symbols
    assert (us_dir / "SQQQ.csv").exists()


def test_collect_data_without_symbol_update_uses_existing_csv_universe(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(dc, "_list_symbols_from_existing_us_csv", lambda: {"AAA", "BBB"})
    monkeypatch.setattr(dc, "_load_us_symbol_universe", lambda: (_ for _ in ()).throw(AssertionError("seeded universe should not be used")))
    monkeypatch.setattr(dc, "ensure_dir", lambda directory: None)
    monkeypatch.setattr(
        dc,
        "fetch_and_save_us_ohlcv_chunked",
        lambda **kwargs: captured.update({"tickers": kwargs["tickers"]}),
    )

    dc.collect_data(update_symbols=False)

    assert captured["tickers"] == ["AAA", "BBB"]


def test_collect_data_update_symbol_failure_falls_back_to_existing_csv_universe(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(dc, "update_symbol_list", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(dc, "_list_symbols_from_existing_us_csv", lambda: {"SPY"})
    monkeypatch.setattr(dc, "_load_us_symbol_universe", lambda: (_ for _ in ()).throw(AssertionError("seeded universe should not be used")))
    monkeypatch.setattr(dc, "ensure_dir", lambda directory: None)
    monkeypatch.setattr(
        dc,
        "fetch_and_save_us_ohlcv_chunked",
        lambda **kwargs: captured.update({"tickers": kwargs["tickers"]}),
    )

    dc.collect_data(update_symbols=True)

    assert captured["tickers"] == ["SPY"]
