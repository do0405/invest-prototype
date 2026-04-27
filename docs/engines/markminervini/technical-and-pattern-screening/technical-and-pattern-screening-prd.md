# Technical And Pattern Screening PRD

## Purpose

Trend-template screening, RS handling, and pattern-analysis surfaces for Minervini-style candidates.

## Canonical Code Surfaces

- `screeners/markminervini/screener.py`
- `screeners/markminervini/enhanced_pattern_analyzer.py`
## Output Contract

- `with_rs.csv` plus VCP/cup-handle evidence and state buckets.

## Closest Tests

- `tests/test_markminervini_screener.py`
- `tests/test_markminervini_pattern_analyzer.py`
## Current Fidelity And High-End Target

- Current fidelity: Medium-high fidelity for live code; strategy notes are higher-end reference only.
- Higher-end target: Clearer pattern grammar with stronger explainability and state lineage.

## Related Raw Sources

- `docs/archive/raw-sources/PRD/VCP & cup and handle/minervini_vcp_cup_handle_quant_screener_prd_v2.md`
- `docs/archive/raw-sources/screeners/markminervini/pattern.md`
