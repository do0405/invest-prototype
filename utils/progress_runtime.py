from __future__ import annotations


def progress_interval(
    total: int,
    *,
    target_updates: int = 10,
    min_interval: int = 25,
    max_interval: int = 500,
) -> int:
    total_items = max(0, int(total or 0))
    if total_items <= 0:
        return 1
    if total_items <= min_interval:
        return max(1, total_items)

    desired_updates = max(1, int(target_updates or 1))
    interval = max(1, total_items // desired_updates)
    return max(1, min(max_interval, max(min_interval, interval)))


def is_progress_tick(processed: int, total: int, interval: int) -> bool:
    processed_items = max(0, int(processed or 0))
    total_items = max(0, int(total or 0))
    step = max(1, int(interval or 1))
    if total_items <= 0:
        return False
    return processed_items >= total_items or (processed_items % step) == 0
