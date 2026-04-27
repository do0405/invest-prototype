from __future__ import annotations

import inspect
import json
from pathlib import Path

import pandas as pd

from tests._paths import runtime_root
from utils.screener_utils import save_screening_results


def _reset_dir(path: Path) -> None:
    if path.exists():
        for child in sorted(path.rglob("*"), reverse=True):
            try:
                if child.is_file():
                    child.unlink()
                elif child.is_dir():
                    child.rmdir()
            except Exception:
                continue
    path.mkdir(parents=True, exist_ok=True)


def test_save_screening_results_keeps_latest_and_snapshot_history():
    root = runtime_root("_test_runtime_screener_results_snapshots")
    _reset_dir(root)

    first = [{"symbol": "AAA", "score": 1}, {"symbol": "BBB", "score": 2}]
    second = [{"symbol": "BBB", "score": 3}]

    assert "incremental_update" not in inspect.signature(save_screening_results).parameters

    first_paths = save_screening_results(first, str(root), "sample_results", include_timestamp=True)
    second_paths = save_screening_results(second, str(root), "sample_results", include_timestamp=True)

    csv_path = root / "sample_results.csv"
    json_path = root / "sample_results.json"

    csv_frame = pd.read_csv(csv_path)
    with open(json_path, 'r', encoding='utf-8') as f:
        payload = json.load(f)

    snapshot_csvs = sorted(root.glob("sample_results_*.csv"))
    snapshot_jsons = sorted(root.glob("sample_results_*.json"))

    assert list(csv_frame["symbol"]) == ["BBB"]
    assert int(csv_frame.iloc[0]["score"]) == 3
    assert payload == [{"symbol": "BBB", "score": 3}]
    assert Path(first_paths["snapshot_csv_path"]).exists()
    assert Path(second_paths["snapshot_csv_path"]).exists()
    assert len(snapshot_csvs) == 2
    assert len(snapshot_jsons) == 2
