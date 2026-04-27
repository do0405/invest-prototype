from __future__ import annotations

import utils.security_profile as security_profile
from utils.security_profile import CANONICAL_METADATA_COLUMNS, enrich_metadata_record


def test_enrich_metadata_record_disables_earnings_for_us_etf_hint() -> None:
    enriched = enrich_metadata_record(
        {
            "symbol": "AAAU",
            "market": "us",
            "provider_symbol": "AAAU",
            "quote_type": "",
            "security_type": "",
            "fetch_status": "partial_fast_info",
        },
        market="us",
        official_hint={"is_etf": True, "security_name": "Gold ETF"},
    )

    assert list(CANONICAL_METADATA_COLUMNS)
    assert enriched["name"] == "Gold ETF"
    assert enriched["security_type"] == "ETF"
    assert enriched["earnings_expected"] is False
    assert enriched["fundamentals_expected"] is False
    assert enriched["earnings_skip_reason"] == "etf"


def test_enrich_metadata_record_keeps_common_stock_eligible() -> None:
    enriched = enrich_metadata_record(
        {
            "symbol": "AAOI",
            "market": "us",
            "provider_symbol": "AAOI",
            "quote_type": "EQUITY",
            "security_type": "",
            "name": "Applied Optoelectronics",
            "sector": "Technology",
            "industry": "Communication Equipment",
            "fetch_status": "complete",
        },
        market="us",
    )

    assert enriched["security_type"] == "COMMON_STOCK"
    assert enriched["earnings_expected"] is True
    assert enriched["fundamentals_expected"] is True
    assert enriched["earnings_skip_reason"] == ""


def test_enrich_metadata_record_disables_not_found_symbols() -> None:
    enriched = enrich_metadata_record(
        {
            "symbol": "AACT",
            "market": "us",
            "provider_symbol": "AACT",
            "fetch_status": "not_found",
        },
        market="us",
    )

    assert enriched["earnings_expected"] is False
    assert enriched["fundamentals_expected"] is False
    assert enriched["earnings_skip_reason"] == "metadata_not_found"


def test_get_security_profile_derives_kr_anchor_for_preferred_share(monkeypatch) -> None:
    monkeypatch.setattr(
        security_profile,
        "_load_metadata_records",
        lambda market: {
            "000150": {
                "symbol": "000150",
                "market": "kr",
                "provider_symbol": "000150.KS",
                "name": "\uB450\uC0B0",
                "exchange": "KOSPI",
                "security_type": "COMMON_STOCK",
                "fetch_status": "complete",
            },
            "000155": {
                "symbol": "000155",
                "market": "kr",
                "provider_symbol": "000155.KS",
                "name": "\uB450\uC0B0\uC6B0",
                "exchange": "KOSPI",
                "security_type": "COMMON_STOCK",
                "fetch_status": "complete",
            },
        },
    )

    profile = security_profile.get_security_profile("000155", "kr")

    assert profile["share_class_type"] == "PREFERRED"
    assert profile["issuer_symbol"] == "000150"
    assert profile["earnings_anchor_symbol"] == "000150"
