from __future__ import annotations

import pandas as pd
import pytest

from tests._paths import cache_root
import utils.io_utils as io_utils
from utils.runtime_context import RuntimeContext


def test_create_required_dirs_defaults_to_base_runtime_roots(monkeypatch):
    created: list[str] = []

    monkeypatch.setattr(io_utils, "ensure_dir", lambda directory: created.append(directory))

    io_utils.create_required_dirs()

    assert created == [
        io_utils.DATA_DIR,
        io_utils.DATA_US_DIR,
        io_utils.DATA_KR_DIR,
        io_utils.RESULTS_DIR,
        io_utils.EXTERNAL_DATA_DIR,
    ]


def test_write_dataframe_csv_with_fallback_reports_primary_and_fallback_failure(
    monkeypatch,
) -> None:
    output_path = cache_root("io_utils", "locked", "leaders.csv")

    def _raise_permission(self, path, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        raise PermissionError(f"locked {path}")

    monkeypatch.setattr(pd.DataFrame, "to_csv", _raise_permission)

    with pytest.raises(PermissionError) as exc_info:
        io_utils.write_dataframe_csv_with_fallback(pd.DataFrame([{"symbol": "A"}]), str(output_path))

    message = str(exc_info.value)
    assert str(output_path) in message
    assert "fallback" in message
    assert "INVEST_PROTO_RESULTS_DIR" in message


def test_write_helpers_record_output_metrics_and_compact_json(monkeypatch) -> None:
    output_dir = cache_root("io_utils", "metrics")
    csv_path = output_dir / "leaders.csv"
    json_path = output_dir / "leaders.json"
    payload_path = output_dir / "summary.json"
    runtime_context = RuntimeContext(market="us")

    monkeypatch.setenv("INVEST_PROTO_COMPACT_JSON", "1")

    frame = pd.DataFrame([{"symbol": "AAA", "score": 10.0}, {"symbol": "BBB", "score": 9.0}])
    io_utils.write_dataframe_csv_with_fallback(
        frame,
        str(csv_path),
        runtime_context=runtime_context,
        metric_label="unit.csv",
    )
    io_utils.write_dataframe_json_with_fallback(
        frame,
        str(json_path),
        orient="records",
        indent=2,
        force_ascii=False,
        runtime_context=runtime_context,
        metric_label="unit.frame_json",
    )
    io_utils.write_json_with_fallback(
        {"rows": frame.to_dict(orient="records")},
        str(payload_path),
        ensure_ascii=False,
        indent=2,
        runtime_context=runtime_context,
        metric_label="unit.payload_json",
    )

    metrics = runtime_context.runtime_metrics["output_persist"]
    assert metrics["files"] == 3
    assert metrics["rows"] == 6
    assert metrics["bytes"] > 0
    assert metrics["seconds"] >= 0.0
    assert "\n  " not in json_path.read_text(encoding="utf-8")
    assert "\n  " not in payload_path.read_text(encoding="utf-8")
