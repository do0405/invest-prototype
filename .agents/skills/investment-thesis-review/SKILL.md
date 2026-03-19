---
name: investment-thesis-review
description: Use when creating, updating, or pressure-testing an investment thesis, screener interpretation, catalyst note, or position review. Keeps the thesis falsifiable, tracks evidence over time, and separates fact, inference, and action.
origin: adapted-from-anthropics-financial-services-plugins
upstream_commit: b881b3c73e76a463731aacb7e886d7280f773348
---

# Investment Thesis Review

Use this skill to turn a loose market idea into a decision-grade thesis review.

## Review lens

- Separate facts from inference and inference from speculation.
- Require horizon, market, and universe to be explicit when the thesis depends on them.
- Call out missing evidence before confidence is increased.
- Distinguish signal from narrative convenience.
- Treat a screener hit as a lead, not as a complete thesis.

## Step 1: Define or load the thesis

Capture the minimum thesis record:
- company and ticker
- position direction: long, short, watchlist, or neutral
- time horizon
- thesis statement in one or two sentences
- key pillars
- key risks
- catalysts
- valuation or expected payoff framing if relevant
- invalidation or exit conditions

If the input is only a screener hit, treat it as a lead, not as a complete thesis.

## Step 2: Update the evidence log

For each new data point or event, record:
- date
- what changed
- which pillar or risk it affects
- whether it strengthens, weakens, or is neutral to the thesis
- what action, if any, follows

## Step 3: Maintain a thesis scorecard

Track the current state of each pillar:

| Pillar | Original expectation | Current status | Trend |
|---|---|---|---|
| | | | |

## Step 4: Track catalysts and invalidation

Keep a short catalyst calendar:

| Date | Event | Expected impact | What would invalidate the thesis |
|---|---|---|---|
| | | | |

## Step 5: Produce the review

Default output:
1. thesis status: intact, weakened, or broken
2. supporting evidence
3. counter-evidence, open risks, and missing evidence
4. catalyst watchlist and invalidation triggers
5. action or no-action conclusion
6. next data that would change confidence materially

## Important notes

- A thesis must be falsifiable.
- Track disconfirming evidence as rigorously as confirming evidence.
- Separate facts, inference, speculation, and recommendation clearly.
- Do not present conviction without an explicit horizon.
- Review theses regularly even when nothing dramatic has happened.
