from __future__ import annotations

import inspect
import shutil
from pathlib import Path

import pandas as pd

import data_collector as dc
from data_collectors import symbol_universe as su
from tests._paths import runtime_root as runtime_test_root


def _write_minimal_csv(path: Path, rows: list[dict[str, object]]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def _reset_runtime_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def test_load_us_symbol_universe_includes_seed_symbols_and_indexes(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_symbol_universe")
    _reset_runtime_dir(runtime_root)

    data_dir = runtime_root
    us_dir = data_dir / "us"
    us_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = data_dir / "stock_metadata.csv"

    (us_dir / "QQQ.csv").write_text("date,symbol,close\n", encoding="utf-8")
    _write_minimal_csv(data_dir / "broad_us_seed.csv", [{"symbol": "SQQQ"}, {"symbol": "SOXL"}])
    _write_minimal_csv(metadata_path, [{"symbol": "SPY"}])

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "DATA_US_DIR", str(us_dir))
    monkeypatch.setattr(dc, "STOCK_METADATA_PATH", str(metadata_path))

    universe = dc._load_us_symbol_universe()

    assert "QQQ" in universe
    assert "SQQQ" in universe
    assert "SOXL" in universe
    assert "SPY" in universe
    assert "^VIX" in universe


def test_load_kr_symbol_universe_signature_removes_legacy_compat_params() -> None:
    parameters = inspect.signature(su.load_kr_symbol_universe).parameters

    assert "stock_module" not in parameters
    assert "as_of" not in parameters


def test_update_symbol_list_writes_new_csvs_to_existing_data_us_dir(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_symbol_update")
    _reset_runtime_dir(runtime_root)

    data_dir = runtime_root
    us_dir = data_dir / "us"
    us_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = data_dir / "stock_metadata.csv"

    (us_dir / "SPY.csv").write_text("date,symbol,open,high,low,close,volume\n", encoding="utf-8")
    _write_minimal_csv(data_dir / "broad_us_seed.csv", [{"symbol": "SQQQ"}])
    _write_minimal_csv(metadata_path, [{"symbol": "SPY"}])

    monkeypatch.setattr(dc, "DATA_DIR", str(data_dir))
    monkeypatch.setattr(dc, "DATA_US_DIR", str(us_dir))
    monkeypatch.setattr(dc, "STOCK_METADATA_PATH", str(metadata_path))
    monkeypatch.setattr(dc, "sync_official_us_symbol_directory", lambda **kwargs: {"broad_us_seed.csv": 1})

    symbols = dc.update_symbol_list()

    assert "SPY" in symbols
    assert "SQQQ" in symbols
    assert (us_dir / "SQQQ.csv").exists()


def test_collect_data_without_symbol_update_uses_existing_csv_universe(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(dc, "_list_symbols_from_existing_us_csv", lambda: {"AAA", "BBB"})
    monkeypatch.setattr(dc, "_load_us_symbol_universe", lambda progress=None: (_ for _ in ()).throw(AssertionError("seeded universe should not be used")))
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "ensure_dir", lambda directory: None)
    monkeypatch.setattr(dc, "fetch_and_save_us_ohlcv_chunked", lambda **kwargs: captured.update({"tickers": kwargs["tickers"]}))

    dc.collect_data(update_symbols=False)

    assert captured["tickers"] == ["AAA", "BBB"]


def test_collect_data_update_symbol_failure_falls_back_to_existing_csv_universe(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr(dc, "update_symbol_list", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(dc, "_list_symbols_from_existing_us_csv", lambda: {"SPY"})
    monkeypatch.setattr(dc, "bootstrap_windows_utf8", lambda: None)
    monkeypatch.setattr(dc, "ensure_dir", lambda directory: None)
    monkeypatch.setattr(dc, "fetch_and_save_us_ohlcv_chunked", lambda **kwargs: captured.update({"tickers": kwargs["tickers"]}))

    dc.collect_data(update_symbols=True)

    assert captured["tickers"] == ["SPY"]


def test_load_us_symbol_universe_uses_basename_fast_path_and_progress(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_symbol_universe_progress")
    _reset_runtime_dir(runtime_root)

    data_dir = runtime_root
    us_dir = data_dir / "us"
    us_dir.mkdir(parents=True, exist_ok=True)
    (us_dir / "AAPL.csv").write_text("date,symbol,close\n", encoding="utf-8")
    (us_dir / "CON_file.csv").write_text("date,symbol,close\n2026-03-11,CON,10\n", encoding="utf-8")

    calls: list[str] = []
    messages: list[str] = []
    original = su._read_symbol_from_existing_csv

    def _tracking_reader(path: str) -> str:
        calls.append(Path(path).name)
        return original(path)

    monkeypatch.setattr(su, "_read_symbol_from_existing_csv", _tracking_reader)

    symbols = su.load_us_symbol_universe(
        data_dir=str(data_dir),
        us_data_dir=str(us_dir),
        stock_metadata_path=None,
        progress=messages.append,
    )

    assert "AAPL" in symbols
    assert "CON" in symbols
    assert calls == ["CON_file.csv"]
    assert any("Local OHLCV scan started" in message for message in messages)
    assert any("US universe ready" in message for message in messages)


def test_sync_official_us_symbol_directory_writes_split_and_filtered_seeds(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_symbol_universe_official_sync")
    _reset_runtime_dir(runtime_root)

    nasdaq_raw = "\n".join(
        [
            "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares",
            "AAPL|Apple Inc.|Q|N|N|100|N|N",
            "AACIW|Legacy warrant|Q|N|N|100|N|N",
            "File Creation Time: 03142026",
        ]
    )
    other_raw = "\n".join(
        [
            "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol",
            "KSS|Kohl's Corp.|N|KSS|N|100|N|KSS",
            "SPY|SPDR S&P 500 ETF|P|SPY|Y|100|N|SPY",
            "KEY$I|KeyCorp Depositary|N|KEY-I|N|100|N|KEY$I",
            "File Creation Time: 03142026",
        ]
    )

    class _Response:
        def __init__(self, text: str) -> None:
            self.text = text

        def raise_for_status(self) -> None:
            return None

    class _Requests:
        def get(self, url: str, **kwargs):  # noqa: ANN001, ANN201
            _ = kwargs
            if url.endswith("nasdaqlisted.txt"):
                return _Response(nasdaq_raw)
            if url.endswith("otherlisted.txt"):
                return _Response(other_raw)
            raise AssertionError(f"unexpected url: {url}")

    messages: list[str] = []
    summary = su.sync_official_us_symbol_directory(
        data_dir=str(runtime_root),
        progress=messages.append,
        requests_module=_Requests(),
        timeout_seconds=1.0,
    )

    assert summary["nasdaq_symbols.csv"] == 1
    assert summary["nyse_symbols.csv"] == 1
    assert summary["nyse_arca_symbols.csv"] == 1
    assert summary["broad_us_seed.csv"] == 3

    nasdaq = pd.read_csv(runtime_root / "nasdaq_symbols.csv")
    nyse = pd.read_csv(runtime_root / "nyse_symbols.csv")
    arca = pd.read_csv(runtime_root / "nyse_arca_symbols.csv")
    broad = pd.read_csv(runtime_root / "broad_us_seed.csv")

    assert list(nasdaq["symbol"]) == ["AAPL"]
    assert list(nyse["symbol"]) == ["KSS"]
    assert list(arca["symbol"]) == ["SPY"]
    assert set(broad["symbol"]) == {"AAPL", "KSS", "SPY"}
    assert not any("AACIW" in value for value in broad["symbol"].tolist())
    assert not any("KEY$I" in value for value in broad["symbol"].tolist())
    assert any("Official seed manifest saved" in message for message in messages)


def test_list_symbols_from_existing_us_csv_filters_special_issues_and_recovers_safe_filenames(monkeypatch):
    runtime_root = runtime_test_root("_test_runtime_existing_us_csv_filtering")
    _reset_runtime_dir(runtime_root)

    us_dir = runtime_root / "us"
    us_dir.mkdir(parents=True, exist_ok=True)
    (us_dir / "SPY.csv").write_text("date,symbol,close\n", encoding="utf-8")
    (us_dir / "IVR$C.csv").write_text("date,symbol,close\n2026-03-11,IVR$C,10\n", encoding="utf-8")
    (us_dir / "CON_file.csv").write_text("date,symbol,close\n2026-03-11,CON,10\n", encoding="utf-8")

    monkeypatch.setattr(dc, "DATA_US_DIR", str(us_dir))

    symbols = dc._list_symbols_from_existing_us_csv()

    assert "SPY" in symbols
    assert "CON" in symbols
    assert "IVR$C" not in symbols


def test_load_kr_symbol_universe_prefers_fdr_listings_and_keeps_local_additions():
    runtime_root = runtime_test_root("_test_runtime_kr_symbol_universe_fdr")
    _reset_runtime_dir(runtime_root)

    kr_dir = runtime_root / "kr"
    kr_dir.mkdir(parents=True, exist_ok=True)
    (kr_dir / "000001.csv").write_text("date,symbol,close\n", encoding="utf-8")
    (kr_dir / "INVALID.csv").write_text("date,symbol,close\n2026-04-18,INVALID,10\n", encoding="utf-8")

    metadata_path = runtime_root / "stock_metadata_kr.csv"
    _write_minimal_csv(
        metadata_path,
        [
            {"symbol": "030200"},
            {"symbol": "ABC123"},
        ],
    )

    class _FDR:
        @staticmethod
        def StockListing(name: str) -> pd.DataFrame:
            mapping = {
                "KOSPI": pd.DataFrame(
                    [
                        {"Code": "005930", "Name": "Samsung Electronics", "Market": "KOSPI"},
                        {"Code": "INVALID", "Name": "Ignore", "Market": "KOSPI"},
                    ]
                ),
                "KOSDAQ": pd.DataFrame([{"Code": "035720", "Name": "Kakao", "Market": "KOSDAQ"}]),
                "ETF/KR": pd.DataFrame([{"Code": "069500", "Name": "KODEX 200", "Market": "ETF"}]),
                "ETN/KR": pd.DataFrame([{"Code": "530065", "Name": "Example ETN", "Market": "ETN"}]),
            }
            return mapping.get(name, pd.DataFrame())

    universe = su.load_kr_symbol_universe(
        data_dir=str(kr_dir),
        stock_metadata_path=str(metadata_path),
        include_kosdaq=True,
        include_etf=True,
        include_etn=True,
        fdr_module=_FDR(),
    )

    assert universe == ["000001", "005930", "030200", "035720", "069500", "530065"]
