# -*- coding: utf-8 -*-
"""File cleanup utilities for timestamped result files."""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from typing import Optional

__all__ = [
    "extract_timestamp_from_filename",
    "cleanup_old_timestamped_files",
    "get_timestamped_files",
]


def extract_timestamp_from_filename(filename: str) -> Optional[datetime]:
    """Extract `_YYYYMMDD` or `_YYYY-MM-DD` timestamps from a filename."""
    match = re.search(r"_(\d{8})(?:\.[^.]+)?$", filename)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y%m%d")
        except ValueError:
            pass

    match = re.search(r"_(\d{4}-\d{2}-\d{2})(?:\.[^.]+)?$", filename)
    if match:
        try:
            return datetime.strptime(match.group(1), "%Y-%m-%d")
        except ValueError:
            pass

    return None


def get_timestamped_files(
    directory: str,
    extensions: list[str] | None = None,
) -> list[tuple[str, datetime]]:
    """Return files under `directory` whose names contain supported timestamps."""
    selected_extensions = extensions or [".csv", ".json"]
    timestamped_files: list[tuple[str, datetime]] = []

    if not os.path.exists(directory):
        return timestamped_files

    for root, _dirs, files in os.walk(directory):
        for filename in files:
            if not any(filename.lower().endswith(ext) for ext in selected_extensions):
                continue
            timestamp = extract_timestamp_from_filename(filename)
            if timestamp:
                timestamped_files.append((os.path.join(root, filename), timestamp))

    return timestamped_files


def cleanup_old_timestamped_files(
    directory: str,
    days_threshold: int = 30,
    extensions: list[str] | None = None,
    dry_run: bool = False,
) -> dict:
    """Delete timestamped files older than `days_threshold`, unless `dry_run` is true."""
    selected_extensions = extensions or [".csv", ".json"]
    cutoff_date = datetime.now() - timedelta(days=days_threshold)
    deleted_files: list[dict[str, object]] = []
    errors: list[str] = []

    for file_path, timestamp in get_timestamped_files(directory, selected_extensions):
        if timestamp >= cutoff_date:
            continue
        relative_path = os.path.relpath(file_path)
        try:
            if dry_run:
                print(f"[Cleanup] Candidate: {relative_path} (date={timestamp:%Y-%m-%d})")
            else:
                os.remove(file_path)
                print(f"[Cleanup] Deleted: {relative_path} (date={timestamp:%Y-%m-%d})")
            deleted_files.append(
                {
                    "path": file_path,
                    "timestamp": timestamp,
                    "relative_path": relative_path,
                }
            )
        except Exception as exc:
            message = f"delete failed: {relative_path} - {exc}"
            errors.append(message)
            print(f"[Cleanup] {message}")

    if deleted_files:
        mode = "candidates" if dry_run else "deleted"
        print(f"[Cleanup] {len(deleted_files)} old timestamped files {mode} ({days_threshold} days)")
    else:
        print(f"[Cleanup] No old timestamped files found ({days_threshold} days)")

    if errors:
        print(f"[Cleanup] {len(errors)} errors while cleaning timestamped files")

    return {
        "deleted_count": len(deleted_files),
        "deleted_files": deleted_files,
        "errors": errors,
        "cutoff_date": cutoff_date,
        "dry_run": dry_run,
    }
