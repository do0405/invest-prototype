from __future__ import annotations

from collections import Counter


US_STYLE_STATUS_ORDER: tuple[str, ...] = (
    "saved",
    "latest",
    "kept_existing",
    "soft_unavailable",
    "delisted",
    "rate_limited",
    "failed",
)

US_STYLE_STATUS_LABELS: dict[str, str] = {
    "saved": "saved",
    "latest": "latest",
    "kept_existing": "kept",
    "soft_unavailable": "soft",
    "delisted": "delisted",
    "rate_limited": "rate_limited",
    "failed": "failed",
}


def format_us_style_chunk_start(chunk_num: int, total_chunks: int, chunk: list[str]) -> str:
    return f"\n[Chunk] Start {chunk_num}/{total_chunks} - size={len(chunk)} tickers={chunk}"


def format_us_style_chunk_summary(
    chunk_num: int,
    total_chunks: int,
    statuses: list[str],
    *,
    status_order: tuple[str, ...] = US_STYLE_STATUS_ORDER,
    status_labels: dict[str, str] = US_STYLE_STATUS_LABELS,
) -> str:
    counter = Counter(statuses)
    metrics = " | ".join(
        f"{status_labels[status]} {counter[status]}"
        for status in status_order
    )
    return f"[Chunk] Done {chunk_num}/{total_chunks} - processed={len(statuses)} | {metrics}"
