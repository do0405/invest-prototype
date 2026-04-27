from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from utils.io_utils import write_json_with_fallback


def _module_status_counts(module_summaries: Mapping[str, Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for summary in module_summaries.values():
        status = str(summary.get("status") or "").strip().upper()
        if not status:
            continue
        counts[status] = counts.get(status, 0) + 1
    return counts


def build_augment_run_summary(
    *,
    market: str,
    as_of_date: str,
    status: str,
    diagnostic_only: bool,
    input_universe_counts: Mapping[str, int],
    source_registry_provenance: Mapping[str, Any],
    module_summaries: Mapping[str, Mapping[str, Any]],
    timings: Mapping[str, float],
    cache_stats: Mapping[str, int],
    rows_read: int,
    rows_written: int,
    output_files: Sequence[str],
) -> dict[str, Any]:
    soft_skip_reasons = [
        {
            "module": name,
            "status": str(summary.get("status") or "").strip().upper(),
            "detail": str(summary.get("detail") or "").strip(),
        }
        for name, summary in module_summaries.items()
        if str(summary.get("status") or "").strip().upper() not in {"", "OK"}
    ]
    return {
        "market": str(market or "").strip().upper(),
        "as_of_date": str(as_of_date or "").strip(),
        "status": str(status or "").strip().lower(),
        "diagnostic_only": bool(diagnostic_only),
        "input_universe_counts": {
            str(key): int(value) for key, value in input_universe_counts.items()
        },
        "source_registry_provenance": dict(source_registry_provenance),
        "module_summaries": {
            str(name): dict(summary) for name, summary in module_summaries.items()
        },
        "status_counts": _module_status_counts(module_summaries),
        "timings": {str(key): round(float(value), 6) for key, value in timings.items()},
        "cache_stats": {str(key): int(value) for key, value in cache_stats.items()},
        "rows_read": int(rows_read),
        "rows_written": int(rows_written),
        "soft_skip_reasons": soft_skip_reasons,
        "output_files": [str(path) for path in output_files if str(path).strip()],
    }


def write_augment_run_summary(path: str, payload: Mapping[str, Any]) -> str:
    return write_json_with_fallback(dict(payload), path, ensure_ascii=False, indent=2)
