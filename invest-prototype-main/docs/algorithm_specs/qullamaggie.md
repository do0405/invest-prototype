# Qullamaggie Trading Algorithm Specification

This document details the current implementation of the Qullamaggie screener as found in `screeners/qullamaggie/`.

## Overview
The screener implements three specific setups defined by Kristjan Kullamägi (Qullamaggie): Breakouts, Episodic Pivots (EP), and Parabolic Shorts.

## Common Filters (`apply_basic_filters`)
All setups (unless specified) share these baseline requirements:

*   **Minimum Price:** $5.00
*   **Minimum Liquidity:** 20-day Avg Volume >= 500,000 shares.
*   **ADR (Average Daily Range):** >= 3.5% (20-day average).
*   **Moving Average Trend:**
    *   Close > 10-day SMA.
    *   Close > 20-day SMA.
*   **Momentum (Price Performance):**
    *   1-Month Change >= 25%
    *   3-Month Change >= 50%
    *   6-Month Change >= 100%
*   **Market Cap:** >= $300M (if data available).

## 1. Breakout Setup (`screen_breakout_setup`)
Targets stocks consolidating in a strong uptrend that are breaking out.

### Criteria
*   **Primary Trend:**
    *   Stock has risen 30%+ in the last 1-3 months.
    *   Price is >= 70% of its 52-week High.
*   **Pattern: VCP (Volatility Contraction Pattern):**
    *   Checks for at least 3 contraction periods in the last 60 days.
    *   **Contraction:** Volatility (ADR) decreases by 20%+, Volume decreases by 30%+, Lows are rising.
    *   *Implementation:* `check_vcp_pattern` function.
*   **Breakout Signal:**
    *   Close > Consolidation High (Max High of last 20 days) * 1.02.
    *   Volume > 20-day Avg Volume * 1.5.
*   **Risk Management:**
    *   (Close - Low) / Close <= ADR * 0.67 (Daily range is within expected limits).
    *   Stop Loss set at Day's Low.
*   **MA Alignment:** Close > 10 SMA > 20 SMA.

## 2. Episodic Pivot (EP) Setup (`screen_episode_pivot_setup`)
Targets stocks gapping up on high volume, typically due to news or earnings.

### Criteria
*   **Context:**
    *   No "excessive" rise (>100%) in the last 3 months (to avoid exhaustion gaps).
    *   Close > 50-day SMA.
*   **The Signal:**
    *   **Gap Up:** Open >= Previous Close * 1.10 (10% Gap).
    *   **Volume:** Volume >= 20-day Avg Volume * 3.0 (Huge volume).
*   **Earnings Filter (Optional but Default=True):**
    *   Uses `EarningsDataCollector` to check for recent earnings surprises.
    *   **Conditions (Actual):** EPS Growth > 100% YoY, Revenue Growth > 20% YoY, EPS Surprise > 20%, Rev Surprise > 20%.
    *   **Conditions (Estimated):** Lower thresholds if actual data is missing (Growth > 50%, etc.).

## 3. Parabolic Short Setup (`screen_parabolic_short_setup`)
Targets extended stocks that are likely to reverse.

### Criteria
*   **Extension (Parabolic Move):**
    *   **Large Caps (>$10B):** 50-100% rise in 5-10 days.
    *   **Small/Mid Caps:** 200-500% rise in 5-10 days.
    *   3+ consecutive up days.
*   **Exhaustion Signal:**
    *   Volume Ratio >= 5.0 (Blow-off volume).
    *   RSI(14) >= 80 (Extreme overbought).
    *   Price is > 50% above 20-day SMA.
*   **Trigger:**
    *   First down candle after 3+ up days (Close < Open).

## Scoring System
Each setup calculates a `score` based on how many conditions are met.
*   **Breakout:** Requires all conditions OR Score >= 5.
*   **Episode Pivot:** Requires all conditions OR Score >= 3.
*   **Parabolic Short:** Requires all conditions OR Score >= 4.

## Current Limitations / Observations
*   **Data Dependency:** The Earnings Filter for EP relies heavily on external libraries (`yahoo_fin`) and might be a bottleneck if data is unavailable.
*   **VCP Logic:** The VCP detection is quite strict (requiring 3 specific contractions). It might miss looser but valid bases.
*   **ADR:** Fixed at 3.5% minimum. Some high-quality large caps might be excluded if they are slightly less volatile.
