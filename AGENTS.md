# invest-prototype-main

## 1. Ownership
- Owns US and KR OHLCV collection plus screening runtime behavior.
- Does not own broader market-intel logic such as regime, hazard, theme, or alert unless the user explicitly requests cross-repo changes.

## 2. Runtime surfaces
- `main.py`: CLI entrypoint and task selection.
- `orchestrator/tasks.py`: task orchestration, scheduler behavior, and market contract enforcement.
- `data_collector.py` and `data_collectors/`: acquisition and refresh logic.
- `screeners/`: screening logic and market-specific filters.
- `utils/`: data-contract, runtime, and hygiene helpers.
- `tests/`: regression coverage.

## 3. Commands
- Install: `.\.venv\Scripts\python -m pip install -r requirements.txt`
- Main run: `.\.venv\Scripts\python main.py`
- Screening only: `.\.venv\Scripts\python main.py --task screening --skip-data --market both`
- KR collect: `.\.venv\Scripts\python main.py --task kr-collect --market kr`
- Tests: `.\.venv\Scripts\python -m pytest -q`

## 4. Repository contracts
- Preserve explicit market contracts. Invalid market values should fail fast; do not add silent fallbacks.
- Keep unit tests local and deterministic. Avoid adding live network dependency to tests unless the user explicitly asks for integration coverage.
- When changing collectors, orchestration, or screeners, update or add the closest regression test in `tests/`.
- Treat `data/`, `results/`, `backup/`, `Reference/`, and `PRD/` as operator data or reference material. Do not reorganize or delete them unless explicitly asked.
- Use `utils/repo_hygiene.py` for non-destructive cleanup scans; do not perform bulk deletes as part of normal changes.
- Keep market-intel or regime-engine responsibilities in the separate market-intel-core repository unless the user explicitly wants them moved here.

## 5. Review focus
- Data contract stability for `data/{market}` and `results/{market}`.
- Scheduler and task orchestration behavior.
- Market-aware argument validation and fail-fast behavior.
- Screener assumptions that could leak across `us` and `kr`.
- Output path safety and accidental destructive behavior.

## 6. Local skill routing
- Use `market-research` for source-attributed market, competitor, or investor research.
- Use `investment-thesis-review` for thesis maintenance, screener interpretation, position notes, and catalyst tracking.
- Use `market-regime-sanity-check` when a change or analysis depends on regime assumptions, breadth, volatility, or cross-market context.
- Use `investor-materials` when turning research into memos, decks, one-pagers, or internally consistent investment documents.
- Use `investor-outreach` for investor-facing emails, updates, intro blurbs, or process communications.
