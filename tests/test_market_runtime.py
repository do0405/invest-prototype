from __future__ import annotations

from utils.market_runtime import (
    get_leader_lagging_results_dir,
    get_markminervini_with_rs_path,
    get_primary_benchmark_symbol,
    get_stock_metadata_path,
    get_tradingview_results_dir,
    get_weinstein_stage2_results_dir,
    iter_provider_symbols,
)


def test_market_result_paths_are_separated():
    us_rs = get_markminervini_with_rs_path("us")
    kr_rs = get_markminervini_with_rs_path("kr")
    assert us_rs != kr_rs
    assert "results" in us_rs and "results" in kr_rs
    assert "/us/" in us_rs.replace("\\", "/")
    assert "/kr/" in kr_rs.replace("\\", "/")

    us_weinstein = get_weinstein_stage2_results_dir("us")
    kr_weinstein = get_weinstein_stage2_results_dir("kr")
    assert us_weinstein != kr_weinstein
    assert "/us/" in us_weinstein.replace("\\", "/")
    assert "/kr/" in kr_weinstein.replace("\\", "/")

    us_leader = get_leader_lagging_results_dir("us")
    kr_leader = get_leader_lagging_results_dir("kr")
    assert us_leader != kr_leader
    assert "/us/" in us_leader.replace("\\", "/")
    assert "/kr/" in kr_leader.replace("\\", "/")

    us_tradingview = get_tradingview_results_dir("us")
    kr_tradingview = get_tradingview_results_dir("kr")
    assert us_tradingview != kr_tradingview
    assert "/us/screeners/tradingview" in us_tradingview.replace("\\", "/")
    assert "/kr/screeners/tradingview" in kr_tradingview.replace("\\", "/")


def test_kr_market_provider_symbols_and_benchmark():
    assert iter_provider_symbols("005930", "kr") == ["005930.KS", "005930.KQ"]
    assert iter_provider_symbols("KOSPI", "kr") == ["^KS11"]
    assert get_primary_benchmark_symbol("kr") == "KOSPI"


def test_market_metadata_paths_are_separated():
    us_path = get_stock_metadata_path("us")
    kr_path = get_stock_metadata_path("kr")
    assert us_path != kr_path
    assert us_path.endswith("stock_metadata.csv")
    assert kr_path.endswith("stock_metadata_kr.csv")
