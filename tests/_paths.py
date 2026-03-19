"""Shared test fixture path helpers after test-data directory refactor."""

from __future__ import annotations

from pathlib import Path


TEST_CACHE_ROOT = Path(".pytest_cache") / "codex"


def cache_root(*parts: str) -> Path:
    return TEST_CACHE_ROOT.joinpath(*parts)


def _select_test_data_root() -> Path:
    preferred = Path("data") / "_test"
    fallback = cache_root("test_data")

    try:
        preferred.mkdir(parents=True, exist_ok=True)
        probe = preferred / ".write_probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        return preferred
    except OSError:
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


TEST_DATA_ROOT = _select_test_data_root()
RUNTIME_TEST_ROOT = TEST_DATA_ROOT / "runtime"


def _split_test_name(raw_name: str) -> tuple[str, str]:
    """Split a test name into `<category>/<name>` style folders."""
    if "_" not in raw_name:
        return raw_name, raw_name

    category, rest = raw_name.split("_", 1)
    if not rest:
        return raw_name, raw_name
    return category, f"{category}_{rest}"


def _normalize_runtime_name(raw_name: str) -> str:
    return raw_name.removeprefix("_test_runtime_") or raw_name


def runtime_root(raw_name: str) -> Path:
    base = _normalize_runtime_name(raw_name)
    category, name = _split_test_name(base)
    return RUNTIME_TEST_ROOT / category / name


def ensure_root(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path
