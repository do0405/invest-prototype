from __future__ import annotations

import pytest
import utils.market_runtime as market_runtime

from tests._paths import cache_root
from utils.market_runtime import (
    get_results_root,
    get_collector_run_state_path,
    get_leader_lagging_results_dir,
    market_key,
    get_markminervini_with_rs_path,
    get_market_signals_root,
    get_market_runtime_root,
    get_market_source_registry_snapshot_path,
    get_multi_screener_signals_results_dir,
    get_peg_imminent_results_dir,
    get_primary_benchmark_symbol,
    get_runtime_profile_path,
    get_runtime_symbol_limit,
    get_signal_engine_results_dir,
    get_stock_metadata_path,
    get_tradingview_results_dir,
    get_weinstein_stage2_results_dir,
    iter_preferred_provider_symbols,
    iter_provider_symbols,
    limit_runtime_symbols,
    preflight_market_output_dirs,
    results_root_override_active,
    require_market_key,
)
from utils.runtime_context import RuntimeContext


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


def test_results_root_env_override_scopes_market_runtime_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    override_root = cache_root("market_runtime", "runtime-results")
    monkeypatch.setenv("INVEST_PROTO_RESULTS_DIR", str(override_root))

    assert get_results_root() == str(override_root.resolve())
    assert get_market_runtime_root("us").replace("\\", "/").endswith("/runtime-results/us/runtime")
    assert get_leader_lagging_results_dir("kr").replace("\\", "/").endswith(
        "/runtime-results/kr/screeners/leader_lagging"
    )


def test_results_root_override_active_reflects_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("INVEST_PROTO_RESULTS_DIR", raising=False)
    assert results_root_override_active() is False

    monkeypatch.setenv("INVEST_PROTO_RESULTS_DIR", str(cache_root("market_runtime", "override-flag")))
    assert results_root_override_active() is True


def test_preflight_market_output_dirs_reports_unwritable_directory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INVEST_PROTO_RESULTS_DIR", str(cache_root("market_runtime", "preflight-fail")))

    def _raise_permission(path: str) -> None:
        raise PermissionError(f"locked: {path}")

    monkeypatch.setattr(market_runtime, "_probe_writable_directory", _raise_permission)

    with pytest.raises(PermissionError, match="Output preflight failed"):
        preflight_market_output_dirs("us")


def test_preflight_market_output_dirs_accepts_writable_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INVEST_PROTO_RESULTS_DIR", str(cache_root("market_runtime", "preflight-ok")))

    checked = preflight_market_output_dirs("us", include_signal_dirs=True)

    normalized = [path.replace("\\", "/") for path in checked]
    assert any(path.endswith("/us/runtime") for path in normalized)
    assert any(path.endswith("/us/screeners/leader_lagging") for path in normalized)
    assert any(path.endswith("/us/signals/multi_screener") for path in normalized)



def test_kr_market_provider_symbols_and_benchmark():
    assert iter_provider_symbols("005930", "kr") == ["005930.KS", "005930.KQ"]
    assert iter_provider_symbols("KOSPI", "kr") == ["^KS11"]
    assert get_primary_benchmark_symbol("kr") == "KOSPI"


def test_iter_preferred_provider_symbols_can_lock_to_kr_metadata_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        market_runtime,
        "get_preferred_provider_symbol",
        lambda symbol, market: "005930.KS",
    )

    assert iter_preferred_provider_symbols("005930", "kr") == [
        "005930.KS",
        "005930.KQ",
    ]
    assert iter_preferred_provider_symbols("005930", "kr", strict=True) == [
        "005930.KS"
    ]



def test_market_metadata_paths_are_separated():
    us_path = get_stock_metadata_path("us")
    kr_path = get_stock_metadata_path("kr")
    assert us_path != kr_path
    assert us_path.endswith("stock_metadata.csv")
    assert kr_path.endswith("stock_metadata_kr.csv")



def test_signal_runtime_paths_are_separated() -> None:
    us_root = get_market_signals_root("us")
    kr_root = get_market_signals_root("kr")
    assert "/us/signals" in us_root.replace("\\", "/")
    assert "/kr/signals" in kr_root.replace("\\", "/")

    us_signal_engine = get_signal_engine_results_dir("us")
    kr_signal_engine = get_signal_engine_results_dir("kr")
    assert "/us/signals/multi_screener" in us_signal_engine.replace("\\", "/")
    assert "/kr/signals/multi_screener" in kr_signal_engine.replace("\\", "/")
    assert us_signal_engine == get_multi_screener_signals_results_dir("us")
    assert kr_signal_engine == get_multi_screener_signals_results_dir("kr")

    us_peg = get_peg_imminent_results_dir("us")
    kr_peg = get_peg_imminent_results_dir("kr")
    assert "/us/screeners/peg_imminent" in us_peg.replace("\\", "/")
    assert "/kr/screeners/peg_imminent" in kr_peg.replace("\\", "/")

    us_snapshot = get_market_source_registry_snapshot_path("us")
    kr_snapshot = get_market_source_registry_snapshot_path("kr")
    assert "/us/screeners/source_registry_snapshot.json" in us_snapshot.replace("\\", "/")
    assert "/kr/screeners/source_registry_snapshot.json" in kr_snapshot.replace("\\", "/")

    us_profile = get_runtime_profile_path("us")
    kr_profile = get_runtime_profile_path("kr")
    assert "/us/runtime/runtime_profile.json" in us_profile.replace("\\", "/")
    assert "/kr/runtime/runtime_profile.json" in kr_profile.replace("\\", "/")

    collector_state = get_collector_run_state_path("us")
    assert "/us/runtime/collector_run_state.json" in collector_state.replace("\\", "/")


def test_collector_run_state_path_can_follow_test_data_root() -> None:
    test_root = "E:/tmp/runtime_fixture"

    path = get_collector_run_state_path("us", data_dir=test_root)

    assert path.replace("\\", "/").endswith(
        "/runtime_fixture/results/us/runtime/collector_run_state.json"
    )


def test_collector_run_state_path_uses_results_override_when_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    override_root = cache_root("market_runtime", "collector-state-override")
    monkeypatch.setenv("INVEST_PROTO_RESULTS_DIR", str(override_root))

    path = get_collector_run_state_path("us")

    assert path.replace("\\", "/").endswith(
        "/collector-state-override/us/runtime/collector_run_state.json"
    )



def test_require_market_key_accepts_supported_markets() -> None:
    assert require_market_key("US") == "us"
    assert require_market_key("kr") == "kr"



def test_require_market_key_rejects_unsupported_markets() -> None:
    with pytest.raises(ValueError, match="Unsupported market"):
        require_market_key("jp")


@pytest.mark.parametrize("market", ["../kr", "us?", "kr/us"])
def test_require_market_key_rejects_malformed_market_inputs(market: str) -> None:
    with pytest.raises(ValueError, match="Unsupported market"):
        require_market_key(market)


def test_market_key_rejects_unsupported_markets() -> None:
    with pytest.raises(ValueError, match="Unsupported market"):
        market_key("jp")


def test_runtime_context_rejects_unsupported_markets() -> None:
    with pytest.raises(ValueError, match="Unsupported market"):
        RuntimeContext(market="jp")


def test_runtime_symbol_limit_defaults_to_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("INVEST_PROTO_RUNTIME_SYMBOL_LIMIT", raising=False)

    assert get_runtime_symbol_limit() is None
    assert limit_runtime_symbols(["A", "B", "C"]) == ["A", "B", "C"]


def test_runtime_symbol_limit_trims_sequence_when_env_is_positive_int(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INVEST_PROTO_RUNTIME_SYMBOL_LIMIT", "2")

    assert get_runtime_symbol_limit() == 2
    assert limit_runtime_symbols(["A", "B", "C"]) == ["A", "B"]


def test_runtime_symbol_limit_ignores_invalid_values(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INVEST_PROTO_RUNTIME_SYMBOL_LIMIT", "0")
    assert get_runtime_symbol_limit() is None

    monkeypatch.setenv("INVEST_PROTO_RUNTIME_SYMBOL_LIMIT", "not-a-number")
    assert get_runtime_symbol_limit() is None
