# Invest Prototype

Collector and screener runtime for US/KR OHLCV workflows.

## Scope
- US OHLCV collection and refresh
- KR OHLCV collection
- Mark Minervini, Weinstein Stage 2, Leader/Lagging, Qullamaggie, and TradingView-style preset screeners
- Market-separated outputs under `results/us/**` and `results/kr/**`

Market state logic such as `regime`, `hazard`, `theme`, and `alert` now lives in the separate `market-intel-core` repository:
`https://github.com/do0405/Initial-market-intel-core`

## Key Paths
- `data_collectors/kr_ohlcv_collector.py`
- `data_collector.py`
- `screeners/`
- `main.py`
- `orchestrator/tasks.py`

## Install
```powershell
.\.venv\Scripts\python -m pip install -r requirements.txt
```

## Run
```powershell
# Full data + screening flow
.\.venv\Scripts\python main.py

# Screening only (US)
.\.venv\Scripts\python main.py --task screening --skip-data --market us

# Screening only (US + KR)
.\.venv\Scripts\python main.py --task screening --skip-data --market both

# KR OHLCV collection
.\.venv\Scripts\python main.py --task kr-collect --market kr

# TradingView-style preset screeners only
.\.venv\Scripts\python main.py --task tradingview --skip-data --market both

# Weinstein Early Stage 2 only
.\.venv\Scripts\python main.py --task weinstein --market both

# Leader / lagging leader-follower screener only
.\.venv\Scripts\python main.py --task leader --market both
```

## Data Contract
- Input OHLCV: `data/{market}/*.csv`
- Common columns: `date,symbol,open,high,low,close,volume`
- Metadata: `data/stock_metadata.csv`
- KR benchmark CSVs may include `data/kr/KOSPI.csv` and `data/kr/KOSDAQ.csv`
- Screener outputs:
  - `results/us/**`
  - `results/kr/**`

## Validation
```powershell
.\.venv\Scripts\python -m pytest -q
```
