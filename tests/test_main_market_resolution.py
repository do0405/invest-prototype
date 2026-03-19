from __future__ import annotations

import sys
import types

import main as main_module
import pytest
from main import _resolve_markets_arg


def test_resolve_markets_arg_both():
    assert _resolve_markets_arg("both") == ["us", "kr"]


def test_resolve_markets_arg_csv_list_filters_unknown_and_deduplicates():
    assert _resolve_markets_arg("us,kr,jp,us,kr") == ["us", "kr"]


def test_resolve_markets_arg_single():
    assert _resolve_markets_arg("kr") == ["kr"]


def test_resolve_markets_arg_none_tokens_default_to_us():
    assert _resolve_markets_arg("none") == ["us"]
    assert _resolve_markets_arg("null") == ["us"]


def test_resolve_markets_arg_csv_list_ignores_none_tokens():
    assert _resolve_markets_arg("us,none,kr") == ["us", "kr"]


def test_resolve_markets_arg_invalid_values_default_to_us():
    assert _resolve_markets_arg("jp") == ["us"]
    assert _resolve_markets_arg("jp,tw") == ["us"]


def test_main_removed_momentum_task_is_rejected_by_parser(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "momentum", "--market", "both"])
    with pytest.raises(SystemExit) as exc_info:
        main_module.main()
    assert exc_info.value.code == 2


def _install_main_runtime_stubs(monkeypatch, calls: list[object]) -> None:
    fake_console_runtime = types.ModuleType("utils.console_runtime")
    setattr(fake_console_runtime, "bootstrap_windows_utf8", lambda: calls.append("bootstrap_windows_utf8"))
    monkeypatch.setitem(sys.modules, "utils.console_runtime", fake_console_runtime)

    fake_yfinance_runtime = types.ModuleType("utils.yfinance_runtime")
    setattr(fake_yfinance_runtime, "bootstrap_yfinance_cache", lambda: calls.append("bootstrap_yfinance_cache"))
    monkeypatch.setitem(sys.modules, "utils.yfinance_runtime", fake_yfinance_runtime)

    fake_config = types.ModuleType("config")
    setattr(fake_config, "RESULTS_DIR", "results")
    monkeypatch.setitem(sys.modules, "config", fake_config)

    fake_tasks = types.ModuleType("orchestrator.tasks")
    setattr(fake_tasks, "collect_data_main", lambda **kwargs: calls.append(("collect_data_main", kwargs)))
    setattr(fake_tasks, "ensure_directories", lambda: calls.append("ensure_directories"))
    setattr(fake_tasks, "run_all_screening_processes", lambda **kwargs: calls.append(("run_all_screening_processes", kwargs)))
    setattr(fake_tasks, "run_kr_ohlcv_collection", lambda **kwargs: calls.append(("run_kr_ohlcv_collection", kwargs)) or {"saved": 1})
    setattr(fake_tasks, "run_leader_lagging_screening", lambda **kwargs: calls.append(("run_leader_lagging_screening", kwargs)))
    setattr(fake_tasks, "run_tradingview_preset_screeners", lambda **kwargs: calls.append(("run_tradingview_preset_screeners", kwargs)))
    setattr(fake_tasks, "run_qullamaggie_strategy_task", lambda **kwargs: calls.append(("run_qullamaggie_strategy_task", kwargs)))
    setattr(fake_tasks, "run_weinstein_stage2_screening", lambda **kwargs: calls.append(("run_weinstein_stage2_screening", kwargs)))
    setattr(fake_tasks, "run_signal_engine_processes", lambda **kwargs: calls.append(("run_signal_engine_processes", kwargs)))
    setattr(fake_tasks, "run_scheduler", lambda: calls.append("run_scheduler"))
    setattr(fake_tasks, "setup_scheduler", lambda: calls.append("setup_scheduler"))
    monkeypatch.setitem(sys.modules, "orchestrator.tasks", fake_tasks)

    fake_cleanup = types.ModuleType("utils.file_cleanup")
    setattr(
        fake_cleanup,
        "cleanup_old_timestamped_files",
        lambda **kwargs: calls.append(("cleanup_old_timestamped_files", kwargs)) or {"deleted_count": 0},
    )
    monkeypatch.setitem(sys.modules, "utils.file_cleanup", fake_cleanup)


def test_main_signals_task_dispatches_signal_engine_processes(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "signals", "--market", "both"])

    main_module.main()

    assert ("run_signal_engine_processes", {"markets": ["us", "kr"]}) in calls
    assert not any(call for call in calls if isinstance(call, tuple) and call[0] == "run_all_screening_processes")


def test_main_all_runs_signal_engine_after_screening(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "all", "--market", "kr", "--skip-data"])

    main_module.main()

    screening_index = calls.index(("run_all_screening_processes", {"skip_data": True, "markets": ["kr"]}))
    signals_index = calls.index(("run_signal_engine_processes", {"markets": ["kr"]}))

    assert screening_index < signals_index
