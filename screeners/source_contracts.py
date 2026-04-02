from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class SourceSpec:
    relative_path: str
    source_tag: str
    screen_stage: str
    buy_eligible: bool


CANONICAL_SOURCE_SPECS: tuple[SourceSpec, ...] = (
    SourceSpec("leader_lagging/leaders.csv", "LL_LEADER", "LEADER", True),
    SourceSpec("leader_lagging/followers.csv", "LL_FOLLOWER", "FOLLOWER", True),
    SourceSpec(
        "markminervini/integrated_actionable_patterns.csv",
        "MM_ACTIONABLE",
        "ACTIONABLE",
        True,
    ),
    SourceSpec(
        "markminervini/integrated_results.csv",
        "MM_INTEGRATED",
        "INTEGRATED",
        True,
    ),
    SourceSpec(
        "tradingview/us_breakout_rvol.csv",
        "TV_BREAKOUT_RVOL",
        "TV_BREAKOUT_RVOL",
        True,
    ),
    SourceSpec(
        "tradingview/us_breakout_10m.csv",
        "TV_BREAKOUT_10M",
        "TV_BREAKOUT_10M",
        True,
    ),
    SourceSpec(
        "tradingview/us_breakout_strength.csv",
        "TV_BREAKOUT_STRENGTH",
        "TV_BREAKOUT_STRENGTH",
        True,
    ),
    SourceSpec(
        "tradingview/us_market_leader.csv",
        "TV_MARKET_LEADER",
        "TV_MARKET_LEADER",
        True,
    ),
    SourceSpec(
        "tradingview/us_trend_breakout.csv",
        "TV_TREND_BREAKOUT",
        "TV_TREND_BREAKOUT",
        True,
    ),
    SourceSpec(
        "tradingview/us_high_volatility.csv",
        "TV_HIGH_VOLATILITY",
        "TV_HIGH_VOLATILITY",
        True,
    ),
    SourceSpec(
        "tradingview/kr_breakout_rvol.csv",
        "TV_BREAKOUT_RVOL",
        "TV_BREAKOUT_RVOL",
        True,
    ),
    SourceSpec(
        "tradingview/kr_market_leader.csv",
        "TV_MARKET_LEADER",
        "TV_MARKET_LEADER",
        True,
    ),
    SourceSpec(
        "weinstein_stage2/primary_candidates.csv",
        "WS_PRIMARY",
        "WS_PRIMARY",
        True,
    ),
    SourceSpec(
        "weinstein_stage2/breakout_week_candidates.csv",
        "WS_BREAKOUT_WEEK",
        "WS_BREAKOUT_WEEK",
        True,
    ),
    SourceSpec(
        "weinstein_stage2/fresh_stage2_candidates.csv",
        "WS_FRESH_STAGE2",
        "WS_FRESH_STAGE2",
        True,
    ),
    SourceSpec(
        "weinstein_stage2/retest_candidates.csv",
        "WS_RETEST",
        "WS_RETEST",
        True,
    ),
    SourceSpec(
        "weinstein_stage2/secondary_candidates.csv",
        "WS_SECONDARY",
        "WS_SECONDARY",
        True,
    ),
    SourceSpec(
        "weinstein_stage2/pre_stage2_candidates.csv",
        "WS_PRE_STAGE2",
        "WS_PRE_STAGE2",
        False,
    ),
    SourceSpec(
        "weinstein_stage2/pattern_included_candidates.csv",
        "WS_PATTERN_INCLUDED",
        "WS_PATTERN_INCLUDED",
        False,
    ),
    SourceSpec(
        "qullamaggie/daily_focus_list.csv",
        "QMG_DAILY",
        "DAILY_FOCUS",
        True,
    ),
    SourceSpec(
        "qullamaggie/weekly_focus_list.csv",
        "QMG_WEEKLY",
        "WEEKLY_FOCUS",
        True,
    ),
    SourceSpec(
        "qullamaggie/universe_list.csv",
        "QMG_UNIVERSE",
        "UNIVERSE_WATCH",
        False,
    ),
)

MIC_LEADER_CORE_SOURCE_TAG = "MIC_LEADER_CORE"
MIC_LEADER_CORE_STAGE = "LEADER_CORE"


SOURCE_STAGE_PRIORITY = {
    "DAILY_FOCUS": 100,
    "WEEKLY_FOCUS": 90,
    MIC_LEADER_CORE_STAGE: 89,
    "WS_PRIMARY": 88,
    "WS_BREAKOUT_WEEK": 87,
    "WS_FRESH_STAGE2": 86,
    "WS_RETEST": 85,
    "WS_SECONDARY": 84,
    "ACTIONABLE": 80,
    "TV_BREAKOUT_RVOL": 79,
    "TV_BREAKOUT_10M": 78,
    "TV_BREAKOUT_STRENGTH": 77,
    "TV_TREND_BREAKOUT": 76,
    "TV_MARKET_LEADER": 75,
    "TV_HIGH_VOLATILITY": 74,
    "LEADER": 70,
    "FOLLOWER": 60,
    "INTEGRATED": 50,
    "WS_PRE_STAGE2": 25,
    "WS_PATTERN_INCLUDED": 20,
    "UNIVERSE_WATCH": 10,
}

SOURCE_TAG_PRIORITY = {
    "QMG_DAILY": 10.0,
    "QMG_WEEKLY": 8.5,
    MIC_LEADER_CORE_SOURCE_TAG: 8.8,
    "WS_PRIMARY": 8.0,
    "WS_BREAKOUT_WEEK": 7.8,
    "WS_FRESH_STAGE2": 7.5,
    "WS_RETEST": 7.0,
    "WS_SECONDARY": 6.2,
    "MM_ACTIONABLE": 6.8,
    "TV_BREAKOUT_RVOL": 6.5,
    "TV_BREAKOUT_10M": 6.2,
    "TV_BREAKOUT_STRENGTH": 5.8,
    "TV_TREND_BREAKOUT": 5.8,
    "TV_MARKET_LEADER": 5.5,
    "LL_LEADER": 5.0,
    "TV_HIGH_VOLATILITY": 4.0,
    "MM_INTEGRATED": 3.8,
    "LL_FOLLOWER": 3.5,
    "WS_PRE_STAGE2": 2.0,
    "WS_PATTERN_INCLUDED": 1.5,
    "QMG_UNIVERSE": 1.0,
    "PEG_READY": 4.5,
    "PEG_ONLY": 2.5,
}

SOURCE_TAG_STYLE = {
    "QMG_DAILY": "TREND",
    "QMG_WEEKLY": "TREND",
    MIC_LEADER_CORE_SOURCE_TAG: "LEADERSHIP",
    "WS_PRIMARY": "STRUCTURE",
    "WS_BREAKOUT_WEEK": "BREAKOUT",
    "WS_FRESH_STAGE2": "STRUCTURE",
    "WS_RETEST": "PULLBACK",
    "WS_SECONDARY": "STRUCTURE",
    "MM_ACTIONABLE": "BREAKOUT",
    "MM_INTEGRATED": "LEADERSHIP",
    "TV_BREAKOUT_RVOL": "BREAKOUT",
    "TV_BREAKOUT_10M": "BREAKOUT",
    "TV_BREAKOUT_STRENGTH": "BREAKOUT",
    "TV_TREND_BREAKOUT": "TREND",
    "TV_MARKET_LEADER": "LEADERSHIP",
    "TV_HIGH_VOLATILITY": "VOLATILITY",
    "LL_LEADER": "LEADERSHIP",
    "LL_FOLLOWER": "LEADERSHIP",
    "WS_PRE_STAGE2": "WATCH",
    "WS_PATTERN_INCLUDED": "WATCH",
    "QMG_UNIVERSE": "WATCH",
    "PEG_READY": "PEG",
    "PEG_ONLY": "PEG",
}

TREND_STYLE_BONUS = {
    "TREND": 4.0,
    "STRUCTURE": 3.5,
    "PULLBACK": 3.0,
    "BREAKOUT": 2.5,
    "LEADERSHIP": 1.5,
    "VOLATILITY": 0.5,
    "WATCH": 0.0,
    "PEG": 1.0,
}

UG_STYLE_BONUS = {
    "BREAKOUT": 4.0,
    "LEADERSHIP": 2.5,
    "TREND": 2.0,
    "STRUCTURE": 1.5,
    "PULLBACK": 1.5,
    "VOLATILITY": 1.0,
    "WATCH": 0.0,
    "PEG": 0.5,
}


def _safe_text(value: object) -> str:
    return str(value or "").strip().upper()


def _to_list(value: Iterable[str] | object | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, Iterable):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def stage_priority(stage: str) -> int:
    return int(SOURCE_STAGE_PRIORITY.get(str(stage or "").strip(), 0))


def source_tag_priority(tag: str) -> float:
    return float(SOURCE_TAG_PRIORITY.get(_safe_text(tag), 0.0))


def source_tag_style(tag: str) -> str:
    return _safe_text(SOURCE_TAG_STYLE.get(_safe_text(tag), "WATCH"))


def source_style_bonus(style: str, *, engine: str) -> float:
    normalized_engine = _safe_text(engine)
    normalized_style = _safe_text(style)
    if normalized_engine == "UG":
        return float(UG_STYLE_BONUS.get(normalized_style, 0.0))
    return float(TREND_STYLE_BONUS.get(normalized_style, 0.0))


def sorted_source_tags(tags: Iterable[str] | object | None) -> list[str]:
    unique_tags = list(dict.fromkeys(_to_list(tags)))
    return sorted(unique_tags, key=lambda tag: (-source_tag_priority(tag), tag))


def source_priority_score(tags: Iterable[str]) -> float:
    ordered = sorted_source_tags(tags)
    if not ordered:
        return 0.0
    top_two = ordered[:2]
    overlap_bonus = max(len(ordered) - 1, 0) * 1.5
    return round(
        min(sum(source_tag_priority(tag) for tag in top_two) + overlap_bonus, 20.0),
        2,
    )


def source_engine_bonus(tags: Iterable[str], *, engine: str) -> float:
    ordered = sorted_source_tags(tags)
    if not ordered:
        return 0.0
    top_two = ordered[:2]
    return round(
        min(
            sum(source_style_bonus(source_tag_style(tag), engine=engine) for tag in top_two),
            10.0,
        ),
        2,
    )


def source_style_tags(tags: Iterable[str]) -> list[str]:
    styles = [source_tag_style(tag) for tag in sorted_source_tags(tags)]
    return list(dict.fromkeys(style for style in styles if style))


def primary_source_style(tags: Iterable[str]) -> str:
    styles = source_style_tags(tags)
    return styles[0] if styles else ""
