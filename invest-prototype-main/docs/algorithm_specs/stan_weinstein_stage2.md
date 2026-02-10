# Stan Weinstein Stage 2 Breakout Algorithm Specification

This document details the current implementation of the Stan Weinstein Stage 2 Breakout screener as found in `screeners/momentum_signals/screener.py`.

## Overview
The screener aims to identify stocks entering a "Stage 2" uptrend based on Stan Weinstein's methodology. It analyzes weekly price data to find breakouts from a base with specific characteristics.

## Core Logic Components

### 1. Market Environment Check (`_check_market_environment`)
*   **Target:** S&P 500 (SPY)
*   **Conditions:**
    1.  SPY Price > 30-week SMA (150-day SMA).
    2.  30-week SMA is trending upwards (current SMA > 10-day prior SMA).
*   **Status:** Active.

### 2. Weekly Data Calculation (`_calculate_weekly_data`)
*   Converts daily OHLCV data to weekly data.
*   **Status:** Active.

### 3. Moving Averages (`_calculate_moving_averages`)
*   Calculates 10, 20, 30, and 40-week SMAs.
*   **Status:** Active.

### 4. Volume Indicators (`_calculate_volume_indicators`)
*   **Average Volume:** 20-week simple moving average.
*   **Volume Ratio:** Current Volume / 20-week Average Volume.
*   **OBV (On-Balance Volume):** Standard calculation.
*   **OBV Rising:** Checks if current OBV > previous week's OBV.
*   **Status:** Active.

### 5. Relative Strength (`_calculate_relative_strength`)
*   **Benchmark:** SPY (S&P 500).
*   **Calculation:** (Stock Price Change / SPY Price Change).
*   **Note:** This is a ratio calculation relative to SPY, not the 0-99 percentile ranking used in Minervini's screener.
*   **Status:** Active.

### 6. Momentum Signal Detection (`_detect_momentum_signal`)
Scans the last 6 weeks for a breakout week that meets the following criteria:

*   **Condition 1: Price vs MA:** Close > 30-week SMA.
*   **Condition 2: MA Trend:** 30-week SMA is rising (checked over last 5 weeks).
*   **Condition 3: Volume Surge:** Volume Ratio >= 2.0 (Volume is at least 200% of average).
*   **Condition 4: Relative Strength:** RS Ratio >= 1.0 (Outperforming SPY).
*   **Condition 5: OBV:** OBV is rising.
*   **Condition 6: Breakout Level:** Close > Resistance Level (calculated as 95% of 10-week High).

#### Disabled / Placeholder Logic
The following features are present in the code structure but are currently **disabled** (commented out or always returning `True`):

*   **Higher Highs/Higher Lows (`_check_higher_highs_lows`):**
    *   *Intended Logic:* Check for 2+ higher highs and 1+ higher low in the last 6 weeks.
    *   *Current Status:* **Disabled** (Always returns `True`).
*   **Entry Type Classification (`_identify_entry_type`):**
    *   *Intended Logic:* Distinguish between 'Type A' (direct breakout) and 'Type B' (pullback).
    *   *Current Status:* **Disabled** (Always returns 'A형').
*   **Sustained Breakout:**
    *   *Current Status:* **Disabled** (Always returns `True`).
*   **Sector Leadership (`_check_sector_leadership`):**
    *   *Current Status:* **Removed** (Always returns `True`).

### 7. Minimal Resistance (`_check_minimal_resistance`)
*   **Condition:** Current Price >= 52-week High * 0.95.
*   **Purpose:** Ensures the stock is near its yearly high, implying little overhead supply.
*   **Status:** Active (Applied in `screen_momentum_signals` loop, though not inside `_detect_momentum_signal`).

## Summary of Active Filters
To pass the screener, a stock must:
1.  Have valid data (min 1 year).
2.  Pass the Market Environment check (SPY in uptrend).
3.  Have a breakout week within the last 6 weeks where:
    *   Price > Rising 30w SMA.
    *   Volume > 2x Average.
    *   RS > 1.0.
    *   OBV Rising.
4.  Currently be within 5% of its 52-week High.

## Areas for Improvement (Based on Code Review)
1.  **Re-enable Pattern Checks:** The Higher Highs/Lows logic is crucial for confirming a trend but is currently bypassed.
2.  **Refine RS Calculation:** The current RS is a simple ratio. Integrating the percentile-based RS Rating (0-99) from `utils.relative_strength` might be more robust.
3.  **Entry Type Logic:** Reactivating the A/B type classification would help in trade planning.
