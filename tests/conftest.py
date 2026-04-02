from __future__ import annotations

import shutil
import sys
from collections.abc import Generator

import pytest

import config
import utils.market_runtime as market_runtime
from tests._paths import cache_root


def _apply_module_attrs(module_name: str, updates: dict[str, str]) -> dict[str, object]:
    module = sys.modules.get(module_name)
    if module is None:
        return {}

    original: dict[str, object] = {}
    for name, value in updates.items():
        if hasattr(module, name):
            original[name] = getattr(module, name)
            setattr(module, name, value)
    return original


@pytest.fixture(scope="session", autouse=True)
def _redirect_results_root_for_tests() -> Generator[None, None, None]:
    base = cache_root("results")
    if base.exists():
        shutil.rmtree(base, ignore_errors=True)
    (base / "screeners").mkdir(parents=True, exist_ok=True)

    updates = {
        "RESULTS_DIR": str(base),
        "SCREENER_RESULTS_DIR": str(base / "screeners"),
        "OPTION_RESULTS_DIR": str(base / "option"),
        "MARKMINERVINI_RESULTS_DIR": str(base / "screeners" / "markminervini"),
        "QULLAMAGGIE_RESULTS_DIR": str(base / "screeners" / "qullamaggie"),
        "RANKING_RESULTS_DIR": str(base / "ranking"),
        "EXTERNAL_DATA_DIR": str(cache_root("external_data")),
        "FINANCIAL_CACHE_DIR": str(cache_root("external_data") / "financials" / "us"),
        "EARNINGS_CACHE_DIR": str(cache_root("external_data") / "earnings" / "us"),
        "RISK_INPUTS_CACHE_DIR": str(cache_root("external_data") / "risk_inputs"),
    }

    original_config = {name: getattr(config, name) for name in updates}
    for name, value in updates.items():
        setattr(config, name, value)

    original_market_runtime = market_runtime.RESULTS_DIR
    original_market_runtime_external = market_runtime.EXTERNAL_DATA_DIR
    market_runtime.RESULTS_DIR = updates["RESULTS_DIR"]
    market_runtime.EXTERNAL_DATA_DIR = updates["EXTERNAL_DATA_DIR"]

    module_originals = {
        "data_collector": _apply_module_attrs("data_collector", {"RESULTS_DIR": updates["RESULTS_DIR"]}),
        "main": _apply_module_attrs("main", {"RESULTS_DIR": updates["RESULTS_DIR"]}),
        "utils.io_utils": _apply_module_attrs(
            "utils.io_utils",
            {
                "RESULTS_DIR": updates["RESULTS_DIR"],
                "QULLAMAGGIE_RESULTS_DIR": updates["QULLAMAGGIE_RESULTS_DIR"],
                "OPTION_RESULTS_DIR": updates["OPTION_RESULTS_DIR"],
            },
        ),
        "utils.yfinance_runtime": _apply_module_attrs(
            "utils.yfinance_runtime",
            {"EXTERNAL_DATA_DIR": updates["EXTERNAL_DATA_DIR"]},
        ),
    }

    try:
        yield
    finally:
        for name, value in original_config.items():
            setattr(config, name, value)
        market_runtime.RESULTS_DIR = original_market_runtime
        market_runtime.EXTERNAL_DATA_DIR = original_market_runtime_external
        for module_name, originals in module_originals.items():
            module = sys.modules.get(module_name)
            if module is None:
                continue
            for attr_name, attr_value in originals.items():
                setattr(module, attr_name, attr_value)
