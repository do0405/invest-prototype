from __future__ import annotations

from main import _resolve_markets_arg


def test_resolve_markets_arg_both():
    assert _resolve_markets_arg("both") == ["us", "kr"]


def test_resolve_markets_arg_csv_list():
    assert _resolve_markets_arg("us,kr,jp") == ["us", "kr", "jp"]


def test_resolve_markets_arg_single():
    assert _resolve_markets_arg("kr") == ["kr"]


def test_resolve_markets_arg_none_tokens_default_to_us():
    assert _resolve_markets_arg("none") == ["us"]
    assert _resolve_markets_arg("null") == ["us"]


def test_resolve_markets_arg_csv_list_ignores_none_tokens():
    assert _resolve_markets_arg("us,none,kr") == ["us", "kr"]
