from __future__ import annotations

import os
import sys


def bootstrap_windows_utf8() -> None:
    """Best-effort UTF-8 console setup for Windows terminals."""
    if os.name != "nt":
        return

    os.environ.setdefault("PYTHONIOENCODING", "utf-8")

    for stream_name in ("stdin", "stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None:
            continue
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                errors = "strict" if stream_name == "stdin" else "replace"
                reconfigure(encoding="utf-8", errors=errors)
            except Exception:
                pass

    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleCP(65001)
        kernel32.SetConsoleOutputCP(65001)
    except Exception:
        pass
