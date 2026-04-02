"""Non-destructive repository hygiene candidate scanner.

This module only reports cleanup candidates and never deletes files.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence


DEFAULT_PATTERNS: tuple[str, ...] = (
    ".pytest_cache",
    ".pytest_tmp",
    "artifacts_*",
    "provider_*",
    "logs/*",
    "logs/**/*",
    "**/__pycache__",
    "**/*.pyc",
    "data/_test/*",
    "data/_test/provider/*",
    "data/_test/runtime/*",
    "data/_test_provider_*",
    "data/_test_runtime_*",
    "results/_test_runtime_*",
)

_DEFAULT_EXCLUDED_DIRS = {".git", ".venv", "venv", "env", "ENV", "node_modules"}


def _to_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _path_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size

    total = 0
    try:
        for root, _, files in os.walk(path):
            for name in files:
                file_path = Path(root) / name
                try:
                    total += file_path.stat().st_size
                except OSError:
                    continue
    except OSError:
        return 0
    return total


def _iter_matches(root: Path, patterns: Sequence[str]) -> Iterable[tuple[Path, str]]:
    for pattern in patterns:
        for path in root.glob(pattern):
            yield path, pattern


def _is_excluded_candidate(root: Path, candidate: Path) -> bool:
    try:
        relative = candidate.relative_to(root)
    except ValueError:
        return True
    return any(part in _DEFAULT_EXCLUDED_DIRS for part in relative.parts)


def collect_cleanup_candidates(
    root: str | Path = ".",
    patterns: Sequence[str] | None = None,
) -> dict:
    """Return cleanup candidates for manual approval."""
    root_path = Path(root).resolve()
    selected_patterns = tuple(patterns or DEFAULT_PATTERNS)

    candidates: list[dict] = []
    seen: set[str] = set()
    for path, matched_pattern in _iter_matches(root_path, selected_patterns):
        try:
            resolved = path.resolve()
        except OSError:
            continue
        if _is_excluded_candidate(root_path, resolved):
            continue
        key = str(resolved).lower()
        if key in seen:
            continue
        seen.add(key)

        try:
            stat = resolved.stat()
            mtime_iso = _to_iso(stat.st_mtime)
        except OSError:
            mtime_iso = None

        rel = resolved.relative_to(root_path) if resolved.is_relative_to(root_path) else resolved
        candidates.append(
            {
                "path": rel.as_posix() if isinstance(rel, Path) else str(rel),
                "kind": "dir" if resolved.is_dir() else "file",
                "size_bytes": _path_size(resolved),
                "matched_pattern": matched_pattern,
                "last_modified": mtime_iso,
            }
        )

    candidates.sort(key=lambda row: row["size_bytes"], reverse=True)
    total_size = sum(row["size_bytes"] for row in candidates)

    return {
        "schema_version": "1.0",
        "generated_at": _to_iso(datetime.now(timezone.utc).timestamp()),
        "root": root_path.as_posix(),
        "pattern_count": len(selected_patterns),
        "total_candidates": len(candidates),
        "total_size_bytes": total_size,
        "candidates": candidates,
    }


def _to_markdown(report: dict) -> str:
    lines = [
        "# Repository Hygiene Cleanup Candidates",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- total_candidates: `{report.get('total_candidates')}`",
        f"- total_size_bytes: `{report.get('total_size_bytes')}`",
        "",
        "## Candidates",
    ]
    candidates = report.get("candidates", [])
    if not candidates:
        lines.extend(["", "No candidates found."])
        return "\n".join(lines) + "\n"

    lines.extend(
        [
            "",
            "| path | kind | size_bytes | matched_pattern |",
            "|---|---:|---:|---|",
        ]
    )
    for row in candidates:
        lines.append(
            f"| `{row.get('path')}` | {row.get('kind')} | {row.get('size_bytes')} | `{row.get('matched_pattern')}` |"
        )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Report cleanup candidates without deleting files")
    parser.add_argument("--root", default=".", help="Repository root directory")
    parser.add_argument("--json-out", default="", help="Write JSON report path")
    parser.add_argument("--md-out", default="", help="Write Markdown report path")
    args = parser.parse_args()

    report = collect_cleanup_candidates(root=args.root)

    if args.json_out:
        out = Path(args.json_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.md_out:
        out = Path(args.md_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(_to_markdown(report), encoding="utf-8")

    print(
        f"[repo_hygiene] candidates={report['total_candidates']}, total_size_bytes={report['total_size_bytes']}"
    )


if __name__ == "__main__":
    main()
