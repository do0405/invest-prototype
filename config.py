# -*- coding: utf-8 -*-

from __future__ import annotations

import os


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
SCREENER_RESULTS_DIR = os.path.join(RESULTS_DIR, "screeners")
OPTION_RESULTS_DIR = os.path.join(RESULTS_DIR, "option")

MARKMINERVINI_RESULTS_DIR = os.path.join(SCREENER_RESULTS_DIR, "markminervini")
QULLAMAGGIE_RESULTS_DIR = os.path.join(SCREENER_RESULTS_DIR, "qullamaggie")
RANKING_RESULTS_DIR = os.path.join(RESULTS_DIR, "ranking")

DATA_US_DIR = os.path.join(DATA_DIR, "us")
DATA_KR_DIR = os.path.join(DATA_DIR, "kr")
OPTION_DATA_DIR = os.path.join(DATA_DIR, "options")
EXTERNAL_DATA_DIR = os.path.join(DATA_DIR, "external")
BACKUP_DIR = os.path.join(BASE_DIR, "backup")

SCREENERS_DIR = os.path.join(BASE_DIR, "screeners")
MARKMINERVINI_DIR = os.path.join(SCREENERS_DIR, "markminervini")
QULLAMAGGIE_DIR = os.path.join(SCREENERS_DIR, "qullamaggie")

US_WITH_RS_PATH = os.path.join(MARKMINERVINI_RESULTS_DIR, "us_with_rs.csv")
ADVANCED_FINANCIAL_RESULTS_PATH = os.path.join(MARKMINERVINI_RESULTS_DIR, "advanced_financial_results.csv")
INTEGRATED_RESULTS_PATH = os.path.join(MARKMINERVINI_RESULTS_DIR, "integrated_results.csv")
STOCK_METADATA_PATH = os.path.join(DATA_DIR, "stock_metadata.csv")

TECHNICAL_CONDITION_COUNT = 8
FINANCIAL_CONDITION_COUNT = 9
ADVANCED_FINANCIAL_MIN_MET = 4
ADVANCED_FINANCIAL_CRITERIA = {
    "min_annual_eps_growth": 20,
    "min_annual_revenue_growth": 15,
    "max_debt_to_equity": 150,
}

YAHOO_FINANCE_MAX_RETRIES = 3
YAHOO_FINANCE_DELAY = 1
OPTION_DATA_SOURCES = [
    "yfinance",
    "exclusion",
]

FINANCIAL_CACHE_DIR = os.path.join(EXTERNAL_DATA_DIR, "financials", "us")
EARNINGS_CACHE_DIR = os.path.join(EXTERNAL_DATA_DIR, "earnings", "us")
RISK_INPUTS_CACHE_DIR = os.path.join(EXTERNAL_DATA_DIR, "risk_inputs")
