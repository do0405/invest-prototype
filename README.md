# Invest Prototype

Collector and screener runtime for US/KR OHLCV workflows.

## Scope

- US OHLCV collection and refresh
- KR OHLCV collection and stock metadata refresh
- Mark Minervini, Weinstein Stage 2, Leader/Lagging, Qullamaggie, TradingView-style preset, and multi-screener signal runtime behavior
- Market-separated outputs under `results/us/**` and `results/kr/**`
- Consume-side compatibility with `market-intel-core` exported leader and market-context artifacts

## Docs

- Canonical docs guide: `docs/README.md`
- Runtime families: `docs/runtime/pipeline-and-collection/`, `docs/runtime/market-intel-core-compatibility/`
- Engine families: `docs/engines/markminervini/`, `docs/engines/weinstein-stage2/`, `docs/engines/leader-lagging/`, `docs/engines/qullamaggie/`, `docs/engines/tradingview/`, `docs/engines/signals/`
- Dated audits and matrices: `docs/audits/`
- Archived raw markdown sources absorbed by the stable docs: `docs/archive/raw-sources/`

## Key Paths

- `main.py`
- `orchestrator/tasks.py`
- `data_collector.py`
- `data_collectors/`
- `screeners/`
- `utils/market_runtime.py`
- `utils/market_data_contract.py`

## Install

```powershell
.\.venv\Scripts\python -m pip install -r requirements.txt
```

- KR runtime now requires `FinanceDataReader` and `financedatabase` in the project venv.
- KR symbol-universe, metadata prefill, OHLCV, and benchmark intake use `FinanceDataReader` primary with yfinance fallback where needed.
- KR metadata gap fill uses `FinanceDatabase`; `pykrx` is no longer part of the runtime dependency set.
- Windows runtime note:
  - If `.venv` is a Python `3.13` environment and crashes with an access violation, use system Python `3.12` instead of the venv.
  - Example install command:

```powershell
& "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe" -m pip install -r requirements.txt
```

## Run

```powershell
.\.venv\Scripts\python main.py
.\.venv\Scripts\python main.py --task screening --skip-data --market both
.\.venv\Scripts\python main.py --task signals --market both
.\.venv\Scripts\python main.py --task screening --skip-data --market us --enable-augment
.\.venv\Scripts\python main.py --task kr-collect --market kr
.\.venv\Scripts\python main.py --task tradingview --skip-data --market both
.\.venv\Scripts\python main.py --task weinstein --market both
.\.venv\Scripts\python main.py --task leader --market both
```

Windows `3.12` fallback example:

```powershell
& "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe" main.py --task all --market us
```

## Data Contract

- Input OHLCV: `data/{market}/*.csv`
- Canonical OHLCV columns: `date,symbol,open,high,low,close,volume`
- Metadata: `data/stock_metadata.csv` and `data/stock_metadata_kr.csv`
- Screener outputs: `results/{market}/screeners/**`
- Signal outputs: `results/{market}/signals/**`
- Compatibility inputs from `market-intel-core`: `results/compat/invest_prototype/{market}/**`

## Validation

```powershell
.\.venv\Scripts\python -m pytest -q
```

Windows `3.12` fallback example:

```powershell
& "$env:LOCALAPPDATA\Programs\Python\Python312\python.exe" -m pytest -q
```

## Notes

- `docs/archive/raw-sources/PRD/` and `docs/archive/raw-sources/Reference/` remain reference material, not canonical runtime docs.
- Stable documentation now lives under `docs/` and separates live implementation fidelity from higher-end reference intent.
