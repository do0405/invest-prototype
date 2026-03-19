from __future__ import annotations

from pathlib import Path

from tests._paths import cache_root


def test_test_cache_root_builds_repo_local_pytest_cache_paths() -> None:
    assert cache_root("signal_engine_runtime") == Path(".pytest_cache") / "codex" / "signal_engine_runtime"
