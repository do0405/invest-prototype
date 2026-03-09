from __future__ import annotations

import shutil
from pathlib import Path

from utils.repo_hygiene import collect_cleanup_candidates

from tests._paths import runtime_root



def _reset_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)


def test_collect_cleanup_candidates_reports_only_matching_paths():
    root = runtime_root("_test_runtime_hygiene")
    _reset_dir(root)
    try:
        (root / "artifacts_alpha").mkdir(parents=True, exist_ok=True)
        (root / "provider_beta").mkdir(parents=True, exist_ok=True)
        (root / "keep").mkdir(parents=True, exist_ok=True)

        (root / "artifacts_alpha" / "a.txt").write_text("x", encoding="utf-8")
        (root / "provider_beta" / "b.log").write_text("y", encoding="utf-8")
        (root / "keep" / "c.txt").write_text("z", encoding="utf-8")

        report = collect_cleanup_candidates(
            root=root,
            patterns=("artifacts_*", "provider_*"),
        )

        paths = {row["path"] for row in report["candidates"]}
        assert "artifacts_alpha" in paths
        assert "provider_beta" in paths
        assert "keep" not in paths
        assert report["total_candidates"] == 2
        assert report["total_size_bytes"] >= 2
    finally:
        shutil.rmtree(root, ignore_errors=True)


