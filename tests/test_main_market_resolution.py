from __future__ import annotations

import sys

import main as main_module
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


def test_main_deprecated_momentum_task_exits_without_orchestrator_runtime(monkeypatch, capsys):
    import orchestrator.tasks as tasks_module
    import utils.file_cleanup as cleanup_module

    monkeypatch.setattr(sys, "argv", ["main.py", "--task", "momentum", "--market", "both"])
    monkeypatch.setattr(tasks_module, "ensure_directories", lambda: None)
    monkeypatch.setattr(tasks_module, "setup_scheduler", lambda: None)
    monkeypatch.setattr(tasks_module, "run_scheduler", lambda: None)
    monkeypatch.setattr(cleanup_module, "cleanup_old_timestamped_files", lambda **kwargs: {"deleted_count": 0})

    main_module.main()

    captured = capsys.readouterr()
    assert "Momentum task is deprecated" in captured.out
