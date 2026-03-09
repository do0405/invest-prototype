# Invest Prototype

Collector and screener runtime for US/KR OHLCV workflows.

## Scope
- US OHLCV collection and refresh
- KR OHLCV collection
- Breadth external cache generation
- Mark Minervini, Leader Stock, Momentum Signals, and Qullamaggie screeners

Market state logic such as `regime`, `hazard`, `theme`, and `alert` now lives in the separate `market-intel-core` repository:
`https://github.com/do0405/Initial-market-intel-core`

## Key Paths
- `data_collectors/kr_ohlcv_collector.py`
- `data_collectors/market_breadth_external_collector.py`
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

# Screening only
.\.venv\Scripts\python main.py --task screening --skip-data

# KR OHLCV collection
.\.venv\Scripts\python main.py --task kr-collect --market kr

# Breadth external cache
.\.venv\Scripts\python main.py --task breadth-external-collect --market us
.\.venv\Scripts\python main.py --task breadth-external-collect --market kr
```

## Data Contract
- Input OHLCV: `data/{market}/*.csv`
- Common columns: `date,symbol,open,high,low,close,volume`
- Metadata: `data/stock_metadata.csv`
- Breadth cache: `data/external/**`
- Screener outputs:
  - `results/screeners/**`
  - `results/leader_stock/**`
  - `results/momentum_signals/**`

## Validation
```powershell
.\.venv\Scripts\python -m pytest -q
```
