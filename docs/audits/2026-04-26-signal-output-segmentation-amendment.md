# Signal Output Segmentation Amendment

Date: `2026-04-26`
Related audits:
- `docs/audits/2026-04-26-signal-algorithm-audit.md`
- `docs/audits/2026-04-26-signal-algorithm-audit-v2.md`

## Corrected Output Contract

- `buy_signals_all_symbols_v1` remains the today-only BUY projection for the full OHLCV-backed universe scan.
- `buy_signals_screened_symbols_v1` remains the today-only BUY subset for symbols that came through screener/source selection.
- An all-scope row with `is_screened=false`, `source_buy_eligible=false`, and `source_fit_label=NONE` is a universe discovery candidate, not automatically a recommendation false positive.
- Recommendation or priority review should use `buy_signals_screened_symbols_v1`, or filter `buy_signals_all_symbols_v1` by `is_screened`, `source_buy_eligible`, `source_fit_label`, and `source_disposition`.

## Implemented Classification Fields

Public buy/sell projection rows now carry additive `source_disposition`:

- `buy_eligible`: source/screener or PEG context supports BUY eligibility.
- `watch_only`: source context is present but not BUY eligible.
- `discovery_only`: all-scope universe scan row without source/screener eligibility.

`signal_summary.json` now includes `buy_signal_segments`:

- `all_total`
- `screened_total`
- `all_only_discovery_total`
- `all_source_disposition_counts`
- `all_source_fit_label_counts`
- `screened_source_fit_label_counts`
- `all_signal_code_counts`
- `screened_signal_code_counts`

## Audit Interpretation

Future false-positive, over-signal, and signal-scarcity checks should segment results before judging quality:

- Evaluate `screened` and source-eligible rows as the higher-priority recommendation surface.
- Evaluate `discovery_only` rows as broad universe discovery, with separate precision/recall expectations.
- Keep algorithm-level risks, such as excessive `TF_BUY_MOMENTUM`, broad `UG_W` state detection, Weinstein actionable scarcity, Leader/Lagging zero-output behavior, Qullamaggie stale outputs, and Mark Minervini pattern-label overlap, but report their counts separately for screened/source and discovery-only segments.

## Validation

Regression coverage:

- `tests/test_signal_engine_restoration.py::test_run_signal_scan_emits_today_only_buy_sell_scope_outputs`
- `tests/test_signals_package.py`

