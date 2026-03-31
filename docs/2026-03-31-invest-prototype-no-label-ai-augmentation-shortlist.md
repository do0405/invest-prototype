# Invest-Prototype Augmentation Reference

Updated: 2026-03-31

## Purpose

This document is the expanded reference version of the repo-specific augmentation note for `invest-prototype-main`.

It combines:

- the broader upstream-fit open-source augmentation catalog
- the no-label and no-local-label AI shortlist
- the financial engineering ideas that matter for collection, screening, reranking, and backtesting

The target is not generic AI adoption. The target is what helps this repository as the upstream collection-and-screening runtime.

## Repo Fit Summary

`invest-prototype-main` owns:

- US and KR OHLCV collection
- metadata and symbol-universe collection
- screening runtimes such as `markminervini`, `qullamaggie`, `weinstein_stage2`, `leader_lagging`, `tradingview`, and `signals`
- `results/{market}/screeners/**` outputs consumed downstream by `market-intel-core`

This repo does not own the broader downstream `regime`, `hazard`, `theme`, or `alert` responsibilities unless the user explicitly asks for a cross-repo move.

Primary existing integration surfaces include:

- `main.py`
- `orchestrator/tasks.py`
- `data_collector.py`
- `data_collectors/*`
- `screeners/*`
- `utils/indicator_helpers.py`
- `utils/relative_strength.py`
- `utils/market_data_contract.py`
- `utils/yfinance_runtime.py`

## Broader Augmentation Catalog

This section includes the broader repo-fit catalog, not only the no-label shortlist.

## KR And US Data Intake

Best-fit libraries and platforms:

- `FinanceDataReader`
- `PyKRX`
- `OpenBB`

Why they fit:

- they reduce over-dependence on `yfinance`
- they improve KR-specific metadata and market-structure coverage
- they can improve coverage for listed and delisted names, indexes, FX, and sector-style inputs

Best-fit repo areas:

- `data_collector.py`
- `data_collectors/kr_ohlcv_collector.py`
- `data_collectors/stock_metadata_collector.py`
- `data_collectors/symbol_universe.py`
- `utils/yfinance_runtime.py`
- `utils/market_data_contract.py`

Main use cases here:

- improve KR market coverage
- improve metadata completeness
- reduce single-provider fragility
- broaden benchmark, FX, and market-context inputs for downstream screeners

## Screener Validation And Backtesting

Best-fit libraries:

- `vectorbt`
- `backtesting.py`
- `Backtrader`

Why they fit:

- this repo already produces candidate lists and screening outputs; the next step is to evaluate those rules as research objects
- they turn screeners from static candidate dumps into testable research systems
- they help reveal where the rules work, fail, or depend on market regime

Best-fit repo areas:

- `screeners/*`
- `screeners/signals/*`
- `orchestrator/tasks.py`
- result export and validation flows

Main use cases here:

- event-study style validation of screener outputs
- follow-through and failure analysis by candidate type
- confidence scoring and ranking based on research results

## Indicator And Pattern Layer

Best-fit library:

- `TA-Lib`

Why it fits:

- the core screeners already depend on MA, ATR, breakout, contraction, and pattern logic
- `TA-Lib` standardizes technical feature computation and reduces custom drift in indicator semantics

Best-fit repo areas:

- `utils/indicator_helpers.py`
- `screeners/markminervini/*`
- `screeners/qullamaggie/*`
- `screeners/weinstein_stage2/*`
- `screeners/leader_lagging/*`

Main use cases here:

- standardize moving-average and volatility features
- support contraction and breakout diagnostics
- reduce ad hoc indicator divergence across screeners

## Research-Grade Quant Stack

Best-fit platform:

- `Qlib`

Why it fits:

- it is the strongest single reference for evolving a simple screener runtime into a research platform
- it includes factor data workflows, model components, PIT-aware data design, backtesting, and portfolio tooling
- even if not adopted wholesale, it is a strong architecture reference

Best-fit repo areas:

- research and backtest sidecars rather than day-one core runtime replacement
- future validation and factor-prototyping workflows

Main use cases here:

- factor research and ranking experiments
- PIT database and research workflow reference
- backtesting and portfolio simulation reference design

## Pipeline Hygiene And Reproducibility

Best-fit tooling:

- `DuckDB/Parquet`
- `DVC`
- `Prefect`
- `Kedro`

Why they fit:

- current screener outputs and intermediate artifacts are still mostly file-centric and CSV-heavy
- these tools help move the repo toward reproducible research DAGs rather than loose output dumps
- the goal is not just cleaner storage; it is stronger repeatability and validation

Best-fit repo areas:

- `orchestrator/tasks.py`
- `data_collector.py`
- `screeners/signals/*`
- result output and intermediate artifact handling

Main use cases here:

- reproducible intermediate features
- versioned screen outputs
- more explicit flow orchestration and rerun logic

## Financial Engineering Framing

These concepts matter more than any individual library.

## 1. Momentum Needs Industry Structure

A screener that only looks at individual-stock momentum is weaker than one that also understands:

- industry momentum
- market state conditioning
- cross-sectional crowding and rotation

This is why downstream `leader` and rotation logic get stronger when industry structure is not ignored.

## 2. Liquidity Is A Real Risk Axis

For breakout and leader-style screens, liquidity should include more than a simple minimum-volume filter.

Important dimensions include:

- turnover
- dollar volume
- gap risk
- volume regime
- illiquidity proxies

This matters because many attractive charts fail for liquidity and execution reasons rather than for pattern reasons alone.

## 3. Portfolio And Risk Overlay Matters Even For Screeners

Even though this repo is not the downstream market-intel runtime, screener research still benefits from:

- CVaR and drawdown-aware thinking
- portfolio construction overlays
- position sizing discipline
- volatility-aware follow-through evaluation

This is why `vectorbt`, `backtesting.py`, `Backtrader`, `PyPortfolioOpt`, and `Riskfolio-Lib` are useful reference neighbors even if not all belong in the first implementation pass here.

## 4. PIT Semantics Matter For Research Validity

If as-of semantics are wrong, then:

- screener backtests are polluted
- feature calibration is polluted
- ranking validation is polluted

So data engineering here is part of quant validity, not just operations hygiene.

## 5. Regime And Hazard Still Matter, But Downstream

This repo should be aware of regime and hazard concepts, but it should not absorb those responsibilities by default.

The clean responsibility split remains:

- `invest-prototype-main` generates and validates upstream collection and screening outputs
- `market-intel-core` handles broader market structure, risk, and downstream intelligence

## No-Label Taxonomy

This is the most useful classification for current repo decisions.

## A. Fully No-Label Methods

These do not need a hand-labeled local dataset and do not require a repo-local supervised training program for baseline use.

Key examples:

- `STUMPY`
- `ruptures`
- `Merlion`
- `aeon`
- `PyOD`

Best interpretation:

- motif discovery
- anomaly and discord detection
- change and structure break detection
- unsupervised ranking support

## B. No Local Labels Required, But Pretrained Externally

These do not need your own labeled dataset to get started, but the models themselves were pretrained or supervised elsewhere.

Key examples:

- `Chronos-2`
- `TimesFM 2.5`
- `MOMENT`
- `Moirai`
- `Granite TSFM`
- `FinBERT`
- `BGE-M3`
- `multilingual-e5-large`
- `FinGPT`

Best interpretation:

- zero-shot forecasting
- sequence embeddings and candidate similarity
- multilingual retrieval and clustering
- text enrichment and narrative generation

## Repo-Specific No-Label And Pretrained-AI Guidance

## Foundation Models Work Best As Post-Screener Rerankers

For this repo, foundation models are best used after the existing screeners have already created a candidate set.

That means:

- first run `markminervini`, `qullamaggie`, `weinstein`, or `leader_lagging`
- then apply a zero-shot forward-path or sequence-quality rerank

Strong candidates for this role:

- `Chronos-2`
- `TimesFM 2.5`
- `Moirai`

Best additive outputs:

- follow-through probability proxies
- quantile breach risk
- expected-versus-realized path deviation
- forecast dispersion as confidence or fragility signal

This is much better than asking a foundation model to replace the first-pass screener logic.

## STUMPY Is The Best First No-Label Upgrade

For this repo, `STUMPY` is especially strong because it directly helps with pattern-heavy screening.

It can support no-label detection of:

- VCP-style contractions
- base structures
- pullback shapes
- failed breakout patterns
- abnormal subsequence discord windows

This is usually a better first upgrade than trying to use a generic LLM for pattern reading.

## FinBERT Plus BGE-M3 For Candidate Enrichment

A strong immediate combination is:

- `FinBERT` for finance-aware sentiment and stress summaries
- `BGE-M3` or `multilingual-e5-large` for semantic similarity, clustering, and theme linkage

This supports:

- news-aware candidate notes
- theme attachment for screened names
- shortlisting with narrative context rather than chart-only context

## Chart-Image LLMs Are Not First Priority

Showing chart images to an LLM or VLM can be useful as a demo or analyst aide.

But for this repo, it is not the highest-signal first move.

More stable first choices are:

- numeric time-series models
- motif and discord detection
- explicit backtest or event-study validation

## Agent And LLM Guidance

The most realistic agent role here is research assistance, not signal creation.

Good roles:

- market or candidate summary generation
- evidence collection for shortlisted names
- screener-output explanation
- red-flag memo generation
- export-layer narrative support

Good frameworks:

- `FinRobot`
- `AutoGen`
- `LlamaIndex Workflows`
- `CrewAI`

More experimental direction:

- `MoiraiAgent` is interesting as a future design reference for combining LLM routing with time-series experts
- this should be treated as a future architecture reference, not the first thing to wire into this repo

Benchmarking support:

- `TimeSeriesGym` is useful as a reference for evaluating agent-style time-series workflows

## Priority Order

Current practical priority for this repo:

1. `STUMPY`
2. `Chronos-2` or `TimesFM 2.5` as post-screener rerank
3. `FinBERT` plus `BGE-M3`
4. agent layer for explanation and research support
5. richer sequence embedding work with `MOMENT`, `Granite TSFM`, or `Moirai` after the above is stable

## Cautions

Important caveats for this repo:

- `TimeGPT` is intentionally not in the main shortlist because it is not strict open source in the same sense as the main preferred references
- `FinGPT` and `FinBERT` do not require your own local labels, but they are not unsupervised models
- `STUMPY`, `ruptures`, `Merlion`, `aeon`, and much of `PyOD` are genuinely closer to the no-label side of the taxonomy
- handing raw OHLCV arrays directly to a general LLM and asking it to forecast is not the recommended path here
- numerical path ranking should stay with time-series foundation models
- explanation and analyst workflow should stay with LLMs and agents

The clean role split is:

- time-series foundation model for numbers and reranking
- LLM or agent for explanation and research support

## Source And Reference List

Broader upstream-fit catalog:

- `FinanceDataReader`: <https://github.com/FinanceData/FinanceDataReader>
- `PyKRX`: <https://github.com/sharebook-kr/pykrx>
- `OpenBB`: <https://docs.openbb.co/>
- `vectorbt`: <https://vectorbt.dev/api/portfolio/>
- `backtesting.py`: <https://kernc.github.io/backtesting.py/doc/examples/Quick%20Start%20User%20Guide.html>
- `Backtrader`: <https://www.backtrader.com/docu/concepts/>
- `TA-Lib`: <https://ta-lib.github.io/ta-lib-python/>
- `Qlib`: <https://github.com/microsoft/qlib>
- `DuckDB`: <https://duckdb.org/docs/>
- `DVC`: <https://dvc.org/doc>
- `Prefect`: <https://docs.prefect.io/>
- `Kedro`: <https://docs.kedro.org/>

No-label and pretrained-AI shortlist:

- `Chronos-2`: <https://github.com/amazon-science/chronos-forecasting>
- `TimesFM 2.5`: <https://github.com/google-research/timesfm>
- `MOMENT`: <https://github.com/moment-timeseries-foundation-model/moment>
- `Moirai / Uni2TS`: <https://github.com/SalesforceAIResearch/uni2ts>
- `Granite TSFM`: <https://github.com/ibm-granite/granite-tsfm>
- `STUMPY`: <https://stumpy.readthedocs.io/en/latest/>
- `Merlion`: <https://github.com/salesforce/Merlion>
- `aeon anomaly detection`: <https://www.aeon-toolkit.org/en/stable/api_reference/anomaly_detection.html>
- `PyOD`: <https://pyod.readthedocs.io/>
- `BGE-M3`: <https://huggingface.co/BAAI/bge-m3>
- `multilingual-e5-large`: <https://huggingface.co/intfloat/multilingual-e5-large>
- `FinBERT`: <https://huggingface.co/ProsusAI/finbert>
- `FinGPT`: <https://github.com/AI4Finance-Foundation/FinGPT>
- `FinRobot`: <https://github.com/AI4Finance-Foundation/FinRobot>
- `AutoGen`: <https://microsoft.github.io/autogen/0.2/docs/Getting-Started>
- `LlamaIndex Workflows`: <https://docs.llamaindex.ai/>
- `CrewAI`: <https://docs.crewai.com/>
- `MoiraiAgent` overview: <https://www.salesforce.com/blog/moiraiagent/>
- `TimeSeriesGym`: <https://github.com/moment-timeseries-foundation-model/TimeSeriesGym>

Financial engineering references emphasized in this note:

- `Jegadeesh-Titman 1993`
- `Moskowitz-Grinblatt 1999`
- `Amihud 2002`
- `Kelly 1956`