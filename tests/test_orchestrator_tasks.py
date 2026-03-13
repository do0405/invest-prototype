from __future__ import annotations

import sys
import types

from orchestrator import tasks


def test_collect_data_main_runs_ohlcv_before_metadata(monkeypatch):
    calls: list[tuple[str, object]] = []

    fake_data_collector = types.ModuleType("data_collector")

    def _fake_collect_data(*, update_symbols: bool = True):  # noqa: ANN202
        calls.append(("us_ohlcv", update_symbols))

    fake_data_collector.collect_data = _fake_collect_data

    monkeypatch.setitem(sys.modules, "data_collector", fake_data_collector)
    monkeypatch.setattr(
        tasks,
        "run_stock_metadata_collection",
        lambda *, market="us": calls.append((f"{market}_metadata", None)),
    )
    monkeypatch.setattr(
        tasks,
        "run_kr_ohlcv_collection",
        lambda **kwargs: calls.append(("kr_ohlcv", kwargs)),
    )

    tasks.collect_data_main(update_symbols=True, skip_ohlcv=False, include_kr=True)

    assert calls == [
        ("us_ohlcv", True),
        ("kr_ohlcv", {"include_etn": True}),
        ("us_metadata", None),
        ("kr_metadata", None),
    ]


def test_run_kr_ohlcv_collection_defaults_to_include_etn(monkeypatch):
    observed: dict[str, object] = {}
    fake_module = types.ModuleType("data_collectors.kr_ohlcv_collector")

    def _fake_collect_kr_ohlcv_csv(**kwargs):  # noqa: ANN202
        observed.update(kwargs)
        return {"total": 1, "saved": 1, "failed": 0}

    fake_module.collect_kr_ohlcv_csv = _fake_collect_kr_ohlcv_csv
    monkeypatch.setitem(sys.modules, "data_collectors.kr_ohlcv_collector", fake_module)

    summary = tasks.run_kr_ohlcv_collection()

    assert summary["saved"] == 1
    assert observed["include_etn"] is True
