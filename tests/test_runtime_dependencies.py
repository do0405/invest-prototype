from __future__ import annotations

from pathlib import Path


def _requirements() -> list[str]:
    return [
        line.strip().lower()
        for line in Path("requirements.txt").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def test_requirements_include_parquet_engine_for_ohlcv_sidecar_cache() -> None:
    requirements = _requirements()

    assert any(
        requirement.startswith("pyarrow") or requirement.startswith("fastparquet")
        for requirement in requirements
    )
