from __future__ import annotations

import os
import sys
import types

import utils.yfinance_runtime as yfinance_runtime
from tests._paths import cache_root


def test_bootstrap_yfinance_cache_tolerates_cache_dir_permission_error(monkeypatch):
    monkeypatch.setattr(
        yfinance_runtime,
        "ensure_dir",
        lambda directory: (_ for _ in ()).throw(PermissionError("denied")),
    )
    monkeypatch.setattr(yfinance_runtime, "EXTERNAL_DATA_DIR", os.path.join("sandbox", "external"))

    cache_dir = yfinance_runtime.bootstrap_yfinance_cache()

    assert cache_dir == os.path.join("sandbox", "external", "yfinance_cache")


def test_bootstrap_yfinance_cache_sets_shared_cache_location(monkeypatch):
    calls: list[tuple[str, str]] = []
    fake_cache = types.SimpleNamespace(
        set_cache_location=lambda path: calls.append(("cache", path)),
        set_tz_cache_location=lambda path: calls.append(("cache_tz", path)),
    )
    fake_yfinance = types.SimpleNamespace(
        cache=fake_cache,
        set_tz_cache_location=lambda path: calls.append(("yf_tz", path)),
    )

    monkeypatch.setitem(sys.modules, "yfinance", fake_yfinance)
    monkeypatch.setitem(sys.modules, "yfinance.cache", fake_cache)
    monkeypatch.setattr(yfinance_runtime, "EXTERNAL_DATA_DIR", os.path.join("sandbox", "external"))
    monkeypatch.setattr(yfinance_runtime, "ensure_dir", lambda directory: None)

    cache_dir = yfinance_runtime.bootstrap_yfinance_cache()

    assert cache_dir == os.path.join("sandbox", "external", "yfinance_cache")
    assert ("yf_tz", cache_dir) in calls
    assert ("cache", cache_dir) in calls


def test_bootstrap_yfinance_cache_uses_results_override(monkeypatch):
    override_root = cache_root("yfinance_runtime", "results-root")
    monkeypatch.setenv("INVEST_PROTO_RESULTS_DIR", str(override_root))
    monkeypatch.setattr(yfinance_runtime, "ensure_dir", lambda directory: None)

    cache_dir = yfinance_runtime.bootstrap_yfinance_cache()

    assert cache_dir == os.path.abspath(os.path.join(str(override_root), "_cache", "yfinance"))
