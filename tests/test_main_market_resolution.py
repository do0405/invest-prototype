from __future__ import annotations

import importlib
import sys
import types

import main as main_module
import pytest
from main import _resolve_markets_arg, _unstable_runtime_reason
from tests._paths import cache_root


def test_resolve_markets_arg_both():
    assert _resolve_markets_arg("both") == ["us", "kr"]



def test_resolve_markets_arg_csv_list_filters_unknown_and_deduplicates():
    assert _resolve_markets_arg("us,kr,us,kr") == ["us", "kr"]



def test_resolve_markets_arg_single():
    assert _resolve_markets_arg("kr") == ["kr"]



def test_resolve_markets_arg_none_tokens_default_to_us():
    assert _resolve_markets_arg("none") == ["us"]
    assert _resolve_markets_arg("null") == ["us"]



def test_resolve_markets_arg_csv_list_ignores_none_tokens():
    assert _resolve_markets_arg("us,none,kr") == ["us", "kr"]



def test_resolve_markets_arg_invalid_values_raise():
    with pytest.raises(ValueError, match="Unsupported market"):
        _resolve_markets_arg("jp")
    with pytest.raises(ValueError, match="Unsupported market"):
        _resolve_markets_arg("jp,tw")


def test_unstable_runtime_reason_flags_windows_python313_venv():
    reason = _unstable_runtime_reason(
        os_name="nt",
        version_info=(3, 13, 11),
        executable=r"E:\side project\invest-prototype-main\.venv\Scripts\python.exe",
        prefix=r"E:\side project\invest-prototype-main\.venv",
        base_prefix=r"C:\Users\15 gram\AppData\Local\Programs\Python\Python313",
        override="",
    )

    assert reason is not None
    assert "non-venv interpreter" in reason
    assert "Python 3.12" in reason


def test_unstable_runtime_reason_allows_system_python_on_windows_python313():
    reason = _unstable_runtime_reason(
        os_name="nt",
        version_info=(3, 13, 11),
        executable=r"C:\Users\15 gram\AppData\Local\Programs\Python\Python313\python.exe",
        prefix=r"C:\Users\15 gram\AppData\Local\Programs\Python\Python313",
        base_prefix=r"C:\Users\15 gram\AppData\Local\Programs\Python\Python313",
        override="",
    )

    assert reason is None



def test_main_removed_momentum_task_is_rejected_by_parser(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "momentum", "--market", "both"])
    with pytest.raises(SystemExit) as exc_info:
        main_module.main()
    assert exc_info.value.code == 2


def test_main_removed_force_screening_flag_is_rejected_by_parser(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["main.py", "--force-screening"])
    with pytest.raises(SystemExit) as exc_info:
        main_module.main()
    assert exc_info.value.code == 2


def test_utils_no_longer_exports_ensure_directory_exists():
    utils_module = importlib.import_module("utils")

    assert "ensure_directory_exists" not in getattr(utils_module, "__all__", [])
    assert not hasattr(utils_module, "ensure_directory_exists")
    assert "fetch_market_cap" not in getattr(utils_module, "__all__", [])
    assert "fetch_quarterly_eps_growth" not in getattr(utils_module, "__all__", [])
    assert not hasattr(utils_module, "fetch_market_cap")
    assert not hasattr(utils_module, "fetch_quarterly_eps_growth")



def _install_main_runtime_stubs(monkeypatch, calls: list[object]) -> None:
    monkeypatch.setenv("INVEST_PROTO_ALLOW_UNSTABLE_VENV_PY313", "1")

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
    setattr(
        fake_tasks,
        "collect_data_main",
        lambda **kwargs: calls.append(("collect_data_main", kwargs)) or {"ok": True, "failed_steps": 0},
    )
    setattr(
        fake_tasks,
        "ensure_directories",
        lambda **kwargs: calls.append(("ensure_directories", kwargs)),
    )
    setattr(
        fake_tasks,
        "run_all_screening_processes",
        lambda **kwargs: calls.append(("run_all_screening_processes", kwargs)) or {"ok": True, "failed_steps": 0},
    )
    setattr(fake_tasks, "run_kr_ohlcv_collection", lambda **kwargs: calls.append(("run_kr_ohlcv_collection", kwargs)) or {"saved": 1})
    setattr(fake_tasks, "run_leader_lagging_screening", lambda **kwargs: calls.append(("run_leader_lagging_screening", kwargs)))
    setattr(fake_tasks, "run_tradingview_preset_screeners", lambda **kwargs: calls.append(("run_tradingview_preset_screeners", kwargs)))
    setattr(fake_tasks, "run_qullamaggie_strategy_task", lambda **kwargs: calls.append(("run_qullamaggie_strategy_task", kwargs)))
    setattr(fake_tasks, "run_weinstein_stage2_screening", lambda **kwargs: calls.append(("run_weinstein_stage2_screening", kwargs)))
    setattr(
        fake_tasks,
        "run_signal_engine_processes",
        lambda **kwargs: calls.append(("run_signal_engine_processes", kwargs)) or {"ok": True, "failed_steps": 0},
    )
    setattr(
        fake_tasks,
        "run_screening_augment_processes",
        lambda **kwargs: calls.append(("run_screening_augment_processes", kwargs)) or {"ok": True, "failed_steps": 0},
    )

    def _fake_run_market_analysis_pipeline(  # noqa: ANN202
        *,
        skip_data=False,
        markets=None,
        include_signals=False,
        enable_augment=False,
        standalone=False,
        as_of_date=None,
    ):
        pipeline_payload = {
            "skip_data": skip_data,
            "markets": markets,
            "include_signals": include_signals,
            "enable_augment": enable_augment,
            "standalone": standalone,
        }
        screening_payload = {"skip_data": skip_data, "markets": markets, "standalone": standalone}
        signal_payload = {"markets": markets, "standalone": standalone}
        if skip_data:
            signal_payload["local_only"] = True
        if as_of_date is not None:
            pipeline_payload["as_of_date"] = as_of_date
            screening_payload["as_of_date"] = as_of_date
            signal_payload["as_of_date"] = as_of_date
        calls.append(("run_market_analysis_pipeline", pipeline_payload))
        calls.append(("run_all_screening_processes", screening_payload))
        if enable_augment:
            calls.append(("run_screening_augment_processes", {"markets": markets}))
        if include_signals:
            calls.append(("run_signal_engine_processes", signal_payload))
        return {"ok": True, "failed_steps": 0}

    setattr(fake_tasks, "run_market_analysis_pipeline", _fake_run_market_analysis_pipeline)
    setattr(fake_tasks, "run_scheduler", lambda: calls.append("run_scheduler"))
    setattr(fake_tasks, "setup_scheduler", lambda: calls.append("setup_scheduler"))
    setattr(
        fake_tasks,
        "write_full_run_summaries",
        lambda label, summary, *, markets: calls.append(
            ("write_full_run_summaries", {"label": label, "markets": markets, "ok": summary.get("ok")})
        ),
    )
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

    assert ("run_signal_engine_processes", {"markets": ["us", "kr"], "standalone": False}) in calls
    assert not any(call for call in calls if isinstance(call, tuple) and call[0] == "run_all_screening_processes")


def test_main_screening_scopes_directory_setup_to_selected_market_and_phase(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "screening", "--market", "us"])

    main_module.main()

    assert (
        "ensure_directories",
        {
            "markets": ["us"],
            "include_signal_dirs": False,
            "include_augment_dirs": False,
        },
    ) in calls


def test_main_all_scopes_directory_setup_to_selected_markets_and_phases(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(
        sys,
        "argv",
        ["main.py", "--task", "all", "--market", "kr", "--skip-data", "--enable-augment"],
    )

    main_module.main()

    assert (
        "ensure_directories",
        {
            "markets": ["kr"],
            "include_signal_dirs": True,
            "include_augment_dirs": True,
        },
    ) in calls


def test_main_does_not_cleanup_old_results_by_default(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "signals", "--market", "us"])

    main_module.main()

    assert not any(
        call
        for call in calls
        if isinstance(call, tuple) and call[0] == "cleanup_old_timestamped_files"
    )


def test_main_cleanup_old_results_is_explicit_opt_in(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(
        sys,
        "argv",
        ["main.py", "--task", "signals", "--market", "us", "--cleanup-old-results"],
    )

    main_module.main()

    assert (
        "cleanup_old_timestamped_files",
        {
            "directory": "results",
            "days_threshold": 30,
            "extensions": [".csv", ".json"],
            "dry_run": False,
        },
    ) in calls


def test_main_cleanup_old_results_uses_results_override_when_set(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)
    override_root = cache_root("main", "runtime-results")
    monkeypatch.setenv("INVEST_PROTO_RESULTS_DIR", str(override_root))

    monkeypatch.setattr(
        sys,
        "argv",
        ["main.py", "--task", "signals", "--market", "us", "--cleanup-old-results"],
    )

    main_module.main()

    assert (
        "cleanup_old_timestamped_files",
        {
            "directory": str(override_root.resolve()),
            "days_threshold": 30,
            "extensions": [".csv", ".json"],
            "dry_run": False,
        },
    ) in calls


def test_main_unexpected_exception_exits_nonzero(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)
    fake_tasks = sys.modules["orchestrator.tasks"]
    setattr(
        fake_tasks,
        "ensure_directories",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "signals", "--market", "us"])

    with pytest.raises(SystemExit) as exc_info:
        main_module.main()

    assert exc_info.value.code == 1


def test_main_signals_task_dispatches_standalone_signal_engine_processes(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "signals", "--market", "both", "--standalone"])

    main_module.main()

    assert ("run_signal_engine_processes", {"markets": ["us", "kr"], "standalone": True}) in calls


def test_main_signals_skip_data_dispatches_local_only_signal_engine_processes(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "signals", "--market", "us", "--skip-data"])

    main_module.main()

    assert ("run_signal_engine_processes", {"markets": ["us"], "standalone": False, "local_only": True}) in calls



def test_main_all_runs_signal_engine_after_screening(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "all", "--market", "kr", "--skip-data"])

    main_module.main()

    screening_index = calls.index(("run_all_screening_processes", {"skip_data": True, "markets": ["kr"], "standalone": False}))
    signals_index = calls.index(("run_signal_engine_processes", {"markets": ["kr"], "standalone": False, "local_only": True}))

    assert screening_index < signals_index


def test_main_all_skip_data_skips_data_phase(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "all", "--market", "us", "--skip-data"])

    main_module.main()

    assert not any(call for call in calls if isinstance(call, tuple) and call[0] == "collect_data_main")


def test_main_enable_augment_is_rejected_for_non_screening_tasks(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "signals", "--market", "us", "--enable-augment"])

    with pytest.raises(SystemExit) as exc_info:
        main_module.main()

    assert exc_info.value.code == 2
    assert not any(
        call
        for call in calls
        if isinstance(call, tuple) and call[0] == "run_screening_augment_processes"
    )


def test_main_screening_enable_augment_dispatches_market_pipeline(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(
        sys,
        "argv",
        ["main.py", "--task", "screening", "--market", "both", "--skip-data", "--enable-augment"],
    )

    main_module.main()

    assert (
        "run_market_analysis_pipeline",
        {
            "skip_data": True,
            "markets": ["us", "kr"],
            "include_signals": False,
            "enable_augment": True,
            "standalone": False,
        },
    ) in calls


def test_main_all_enable_augment_runs_between_screening_and_signals(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(
        sys,
        "argv",
        ["main.py", "--task", "all", "--market", "us", "--skip-data", "--enable-augment"],
    )

    main_module.main()

    screening_index = calls.index(("run_all_screening_processes", {"skip_data": True, "markets": ["us"], "standalone": False}))
    augment_index = calls.index(("run_screening_augment_processes", {"markets": ["us"]}))
    signals_index = calls.index(("run_signal_engine_processes", {"markets": ["us"], "standalone": False, "local_only": True}))

    assert screening_index < augment_index < signals_index


def test_main_screening_standalone_dispatches_market_pipeline(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(
        sys,
        "argv",
        ["main.py", "--task", "screening", "--market", "both", "--skip-data", "--standalone"],
    )

    main_module.main()

    assert (
        "run_market_analysis_pipeline",
        {
            "skip_data": True,
            "markets": ["us", "kr"],
            "include_signals": False,
            "enable_augment": False,
            "standalone": True,
        },
    ) in calls


def test_main_as_of_dispatches_to_standalone_pipeline(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py",
            "--task",
            "all",
            "--market",
            "both",
            "--skip-data",
            "--standalone",
            "--as-of",
            "2026-04-17",
        ],
    )

    main_module.main()

    assert (
        "run_market_analysis_pipeline",
        {
            "skip_data": True,
            "markets": ["us", "kr"],
            "include_signals": True,
            "enable_augment": False,
            "standalone": True,
            "as_of_date": "2026-04-17",
        },
    ) in calls


@pytest.mark.parametrize(
    ("task", "function_name", "expected_kwargs"),
    [
        (
            "leader",
            "run_leader_lagging_screening",
            {"market": "us", "standalone": True, "as_of_date": "2026-04-17"},
        ),
        (
            "qullamaggie",
            "run_qullamaggie_strategy_task",
            {"skip_data": False, "market": "us", "standalone": True, "as_of_date": "2026-04-17"},
        ),
        (
            "tradingview",
            "run_tradingview_preset_screeners",
            {"market": "us", "standalone": True, "as_of_date": "2026-04-17"},
        ),
        (
            "weinstein",
            "run_weinstein_stage2_screening",
            {"market": "us", "standalone": True, "as_of_date": "2026-04-17"},
        ),
    ],
)
def test_main_as_of_dispatches_to_direct_screening_tasks(
    monkeypatch,
    task,
    function_name,
    expected_kwargs,
):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "main.py",
            "--task",
            task,
            "--market",
            "us",
            "--standalone",
            "--as-of",
            "2026-04-17",
        ],
    )

    main_module.main()

    assert (function_name, expected_kwargs) in calls



def test_main_invalid_market_is_rejected(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "signals", "--market", "jp"])

    with pytest.raises(SystemExit) as exc_info:
        main_module.main()

    assert exc_info.value.code == 2
    assert not any(call for call in calls if isinstance(call, tuple) and call[0] == "run_signal_engine_processes")



def test_main_all_reports_pipeline_failures_honestly(monkeypatch, capsys):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    fake_tasks = sys.modules["orchestrator.tasks"]
    setattr(
        fake_tasks,
        "run_market_analysis_pipeline",
        lambda **kwargs: calls.append(("run_market_analysis_pipeline", kwargs)) or {"ok": False, "failed_steps": 1},
    )

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "all", "--market", "us", "--skip-data"])

    with pytest.raises(SystemExit) as exc_info:
        main_module.main()

    captured = capsys.readouterr()
    assert exc_info.value.code == 1
    assert "[Main] All tasks completed" not in captured.out
    assert "[Main] Pipeline completed with failures" in captured.out


def test_main_all_writes_nonstandalone_full_run_label_by_default(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    fake_tasks = sys.modules["orchestrator.tasks"]
    setattr(
        fake_tasks,
        "run_market_analysis_pipeline",
        lambda **kwargs: calls.append(("run_market_analysis_pipeline", kwargs))
        or {"ok": True, "failed_steps": 0, "market_truth_modes": {"us": "compat"}},
    )

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "all", "--market", "us", "--skip-data"])

    main_module.main()

    assert ("write_full_run_summaries", {"label": "Full live e2e", "markets": ["us"], "ok": True}) in calls


def test_main_all_writes_auto_fallback_full_run_label_when_analysis_reports_standalone_auto(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    fake_tasks = sys.modules["orchestrator.tasks"]
    setattr(
        fake_tasks,
        "run_market_analysis_pipeline",
        lambda **kwargs: calls.append(("run_market_analysis_pipeline", kwargs))
        or {"ok": True, "failed_steps": 0, "market_truth_modes": {"us": "standalone_auto"}},
    )

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "all", "--market", "us", "--skip-data"])

    main_module.main()

    assert (
        "write_full_run_summaries",
        {"label": "Full live auto-fallback e2e", "markets": ["us"], "ok": True},
    ) in calls


def test_main_all_writes_standalone_full_run_label_when_flag_set(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    fake_tasks = sys.modules["orchestrator.tasks"]
    setattr(
        fake_tasks,
        "run_market_analysis_pipeline",
        lambda **kwargs: calls.append(("run_market_analysis_pipeline", kwargs))
        or {"ok": True, "failed_steps": 0, "market_truth_modes": {"us": "compat"}},
    )

    monkeypatch.setattr(
        sys,
        "argv",
        ["main.py", "--task", "all", "--market", "us", "--skip-data", "--standalone"],
    )

    main_module.main()

    assert (
        "write_full_run_summaries",
        {"label": "Full live standalone e2e", "markets": ["us"], "ok": True},
    ) in calls


def test_main_direct_leader_task_exits_nonzero_on_failure(monkeypatch, capsys):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    fake_tasks = sys.modules["orchestrator.tasks"]
    setattr(
        fake_tasks,
        "run_leader_lagging_screening",
        lambda **kwargs: calls.append(("run_leader_lagging_screening", kwargs)) or {"error": "boom"},
    )

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "leader", "--market", "us"])

    with pytest.raises(SystemExit) as exc_info:
        main_module.main()

    output_lines = capsys.readouterr().out.splitlines()
    assert exc_info.value.code == 1
    assert "[Main] Leader task completed" not in output_lines
    assert "[Main] Leader task completed with failures" in output_lines


def test_main_all_kr_market_excludes_us_data_phase(monkeypatch):
    calls: list[object] = []
    _install_main_runtime_stubs(monkeypatch, calls)

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "all", "--market", "kr"])

    main_module.main()

    assert (
        "collect_data_main",
        {
            "update_symbols": True,
            "skip_ohlcv": False,
            "include_kr": True,
            "include_us": False,
            "skip_us_ohlcv": False,
        },
    ) in calls
