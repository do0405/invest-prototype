from __future__ import annotations

import sys

from utils.console_runtime import bootstrap_windows_utf8
import utils.console_runtime as console_runtime


class _Stream:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def reconfigure(self, **kwargs) -> None:  # noqa: ANN003
        self.calls.append(kwargs)


class _Kernel32:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def SetConsoleCP(self, code: int) -> None:  # noqa: N802
        self.calls.append(("input", code))

    def SetConsoleOutputCP(self, code: int) -> None:  # noqa: N802
        self.calls.append(("output", code))


class _Ctypes:
    def __init__(self, kernel32: _Kernel32) -> None:
        self.windll = type("Windll", (), {"kernel32": kernel32})()


def test_bootstrap_windows_utf8_reconfigures_streams(monkeypatch):
    stdin = _Stream()
    stdout = _Stream()
    stderr = _Stream()
    kernel32 = _Kernel32()

    monkeypatch.setattr(console_runtime.os, "name", "nt", raising=False)
    monkeypatch.setattr(console_runtime.sys, "stdin", stdin)
    monkeypatch.setattr(console_runtime.sys, "stdout", stdout)
    monkeypatch.setattr(console_runtime.sys, "stderr", stderr)
    monkeypatch.setitem(sys.modules, "ctypes", _Ctypes(kernel32))

    bootstrap_windows_utf8()

    assert stdin.calls == [{"encoding": "utf-8", "errors": "strict"}]
    assert stdout.calls == [{"encoding": "utf-8", "errors": "replace"}]
    assert stderr.calls == [{"encoding": "utf-8", "errors": "replace"}]
    assert ("input", 65001) in kernel32.calls
    assert ("output", 65001) in kernel32.calls
