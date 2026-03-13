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
    "saved": "저장",
    "latest": "최신",
    "kept_existing": "유지",
    "soft_unavailable": "soft",
    "delisted": "상폐",
    "rate_limited": "제한",
    "failed": "실패",
}


def format_us_style_chunk_start(chunk_num: int, total_chunks: int, chunk: list[str]) -> str:
    return f"\n⏱️ Chunk {chunk_num}/{total_chunks} 시작 ({len(chunk)}개): {chunk}"


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
    return (
        f"✅ 청크 {chunk_num}/{total_chunks} 완료: "
        f"처리 {len(statuses)}개 | {metrics}"
    )
