---
name: market-regime-sanity-check
description: Use when a code change or analysis depends on market regime assumptions, trend context, breadth, volatility, or cross-market screening behavior.
origin: local-thin-wrapper
---

# Market Regime Sanity Check

Use this skill when regime assumptions are shaping the decision.

## Workflow
1. State the regime assumption explicitly instead of implying it.
2. Require the relevant horizon, market, and universe to be explicit.
3. Name the observable inputs that support or weaken it.
4. Do not claim regime, momentum, or institutional behavior without observable support.
5. Check whether the assumption should differ across `us` and `kr`.
6. Check whether this repository should own the logic or defer to an external market-intel layer.
7. Separate market context from code-path ownership. A true statement about regime does not automatically justify embedding new logic here.

## Output expectations
- A clear statement of the current regime assumption
- The evidence or missing evidence behind it
- The horizon, market, and universe the assumption applies to
- Whether the change belongs in this repository or a separate intelligence layer
- The specific validation needed before trusting the assumption
