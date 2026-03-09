"""Utilities for persisting and reusing external data snapshots."""

from __future__ import annotations

import os
import tempfile
import time
from typing import Optional

import pandas as pd


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)


def is_file_fresh(path: str, max_age_seconds: int) -> bool:
    if max_age_seconds <= 0:
        return os.path.exists(path)
    if not os.path.exists(path):
        return False
    age = time.time() - os.path.getmtime(path)
    return age <= max_age_seconds


def load_csv(path: str) -> Optional[pd.DataFrame]:
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None


def load_csv_if_fresh(path: str, max_age_seconds: int) -> Optional[pd.DataFrame]:
    if not is_file_fresh(path, max_age_seconds=max_age_seconds):
        return None
    return load_csv(path)


def write_csv_atomic(frame: pd.DataFrame, path: str, index: bool = False) -> None:
    ensure_parent_dir(path)
    target_dir = os.path.dirname(path) or "."
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tmp", dir=target_dir, delete=False, encoding="utf-8") as tmp:
        tmp_path = tmp.name
    try:
        frame.to_csv(tmp_path, index=index)
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass

