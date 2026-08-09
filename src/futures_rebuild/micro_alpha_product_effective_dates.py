"""Fail-closed official product-effective-date evidence for Apex micros.

Databento parent and continuous symbology prove availability and continuity;
they do not prove an exchange product launch date.  This module accepts only
sealed CME Group primary-source verification reports and exposes no network,
credential, DBN, or historical-row surface.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Final
from urllib.parse import urlsplit

from .canonical import sha256_json
from .errors import IntegrityError


M6E_REPORT_PATH: Final = Path(
    "state/unpublished_evidence/"
    "apex_micro_m6e_product_effective_date_source_v1/report.json"
)
REMAINING_REPORT_PATH: Final = Path(
    "state/unpublished_evidence/"
    "apex_micro_remaining_product_effective_dates_source_v1/report.json"
)
M6E_SCHEMA: Final = (
    "apex_micro_m6e_product_effective_date_source_verification/1.0.0"
)
REMAINING_SCHEMA: Final = (
    "apex_micro_remaining_product_effective_dates_source_verification/1.0.0"
)
M6E_EXPECTED_REPORT_ID: Final = (
    "c061f4ff78fd6bc408ae237b69ab0e6898c0d3b5a2419955ab4f27278b32b54c"
)
EXPECTED_REMAINING_PRODUCTS: Final = {
    "MES": {"parent_product": "ES", "product_family": "MICRO_E_MINI_SP_500"},
    "MCL": {"parent_product": "CL", "product_family": "MICRO_WTI_CRUDE_OIL"},
    "MGC": {"parent_product": "GC", "product_family": "MICRO_GOLD"},
}


def _object(path: Path, name: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{name} is unavailable or invalid") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"{name} is not an object")
    return value


def _verify_report_id(report: Mapping[str, object], *, name: str) -> str:
    core = dict(report)
    report_id = core.pop("report_id", None)
    if type(report_id) is not str or report_id != sha256_json(core):
        raise IntegrityError(f"{name} identity drifted")
    return report_id


def _iso_date(value: object, *, name: str) -> str:
    if type(value) is not str:
        raise IntegrityError(f"{name} is not an exact ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise IntegrityError(f"{name} is not an exact ISO date") from exc
    if parsed.isoformat() != value:
        raise IntegrityError(f"{name} is not an exact ISO date")
    return value


def _validate_official_sources(value: object, *, name: str) -> None:
    if not isinstance(value, list) or len(value) < 1:
        raise IntegrityError(f"{name} lacks an official CME source")
    document_ids: set[str] = set()
    for source in value:
        if not isinstance(source, Mapping):
            raise IntegrityError(f"{name} source is malformed")
        document_id = source.get("document_id")
        url = source.get("url")
        claims = source.get("verified_claims")
        if (
            type(document_id) is not str
            or not document_id
            or document_id in document_ids
            or type(url) is not str
            or not isinstance(claims, list)
            or not claims
            or not all(type(claim) is str and claim for claim in claims)
        ):
            raise IntegrityError(f"{name} source identity or claims are invalid")
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "www.cmegroup.com"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port is not None
            or parsed.query
            or parsed.fragment
        ):
            raise IntegrityError(f"{name} source is not an exact official CME URL")
        document_ids.add(document_id)


def _validate_lookup(value: object, *, maximum_requests: int, name: str) -> None:
    if not isinstance(value, Mapping):
        raise IntegrityError(f"{name} lookup accounting is malformed")
    observed = value.get("observed_network_requests")
    approved = value.get("approved_maximum_network_requests")
    if (
        type(observed) is not int
        or type(approved) is not int
        or not (0 < observed <= approved <= maximum_requests)
        or value.get("maximum_external_cost_usd") != "0"
        or value.get("external_cost_incurred_usd") != "0"
        or value.get("automatic_retries") != 0
        or value.get("databento_calls") != 0
    ):
        raise IntegrityError(f"{name} lookup authority or effects drifted")


def _validate_effects(value: object, *, name: str) -> None:
    if not isinstance(value, Mapping) or not value:
        raise IntegrityError(f"{name} effects are malformed")
    if any(flag is not False for flag in value.values()):
        raise IntegrityError(f"{name} recorded a forbidden effect")


def load_m6e_product_effective_date(*, root: Path) -> str:
    report = _object(root / M6E_REPORT_PATH, "M6E official-source report")
    report_id = _verify_report_id(report, name="M6E official-source report")
    disposition = report.get("date_disposition")
    if not isinstance(disposition, Mapping):
        raise IntegrityError("M6E date disposition is malformed")
    effective = _iso_date(
        disposition.get("product_listing_effective_date"),
        name="M6E product listing effective date",
    )
    first_trade = _iso_date(
        disposition.get("first_trade_date"), name="M6E first trade date"
    )
    if (
        report_id != M6E_EXPECTED_REPORT_ID
        or report.get("schema_version") != M6E_SCHEMA
        or report.get("state") != "PASS_OFFICIAL_PRIMARY_SOURCE_METADATA_ONLY"
        or report.get("market") != "M6E"
        or report.get("parent_product") != "6E"
        or report.get("source_authority") != "CME Group"
        or report.get("source_domain") != "www.cmegroup.com"
        or effective != "2009-03-22"
        or first_trade != "2009-03-23"
        or disposition.get("semantic_basis")
        != "CME_EFFECTIVE_AND_LISTING_DATE_WITH_SEPARATE_TRADE_DATE"
        or disposition.get("effective_before_databento_dataset_start") is not True
        or disposition.get("effective_before_phase1a_acquisition_start") is not True
        or disposition.get("phase1a_prelaunch_intervals") != 0
    ):
        raise IntegrityError("M6E official product-date evidence drifted")
    _validate_lookup(report.get("source_lookup"), maximum_requests=12, name="M6E")
    _validate_official_sources(report.get("official_sources"), name="M6E")
    _validate_effects(report.get("effects"), name="M6E")
    return effective


def load_remaining_product_effective_dates(*, root: Path) -> dict[str, str]:
    report = _object(
        root / REMAINING_REPORT_PATH, "remaining micro official-source report"
    )
    _verify_report_id(report, name="remaining micro official-source report")
    markets = report.get("markets")
    if (
        report.get("schema_version") != REMAINING_SCHEMA
        or report.get("state") != "PASS_OFFICIAL_PRIMARY_SOURCE_METADATA_ONLY"
        or report.get("source_authority") != "CME Group"
        or report.get("source_domain") != "www.cmegroup.com"
        or not isinstance(markets, Mapping)
        or set(markets) != set(EXPECTED_REMAINING_PRODUCTS)
    ):
        raise IntegrityError("remaining micro official product-date evidence drifted")
    _validate_lookup(
        report.get("source_lookup"), maximum_requests=18, name="remaining micro"
    )
    _validate_effects(report.get("effects"), name="remaining micro")
    result: dict[str, str] = {}
    for market, expected in EXPECTED_REMAINING_PRODUCTS.items():
        record = markets.get(market)
        if not isinstance(record, Mapping):
            raise IntegrityError(f"{market} official product-date record is malformed")
        effective = _iso_date(
            record.get("product_listing_effective_date"),
            name=f"{market} product listing effective date",
        )
        first_trade = _iso_date(
            record.get("first_trade_date"), name=f"{market} first trade date"
        )
        if (
            record.get("parent_product") != expected["parent_product"]
            or record.get("product_family") != expected["product_family"]
            or record.get("semantic_basis")
            != "CME_EFFECTIVE_AND_LISTING_DATE_WITH_SEPARATE_TRADE_DATE"
            or record.get("databento_mapping_role")
            != "AVAILABILITY_AND_CONTINUITY_ONLY_NOT_PRODUCT_LAUNCH"
            or record.get("databento_mapping_date_used_as_product_effective_date")
            is not False
            or date.fromisoformat(effective) > date.fromisoformat(first_trade)
        ):
            raise IntegrityError(f"{market} official product-date semantics drifted")
        _validate_official_sources(record.get("official_sources"), name=market)
        result[market] = effective
    return result


def load_official_product_effective_dates(*, root: Path) -> dict[str, str]:
    dates = load_remaining_product_effective_dates(root=root)
    dates["M6E"] = load_m6e_product_effective_date(root=root)
    if set(dates) != {"MES", "MCL", "MGC", "M6E"}:
        raise IntegrityError("official micro product-date scope drifted")
    return dict(sorted(dates.items()))


__all__ = [
    "M6E_REPORT_PATH",
    "REMAINING_REPORT_PATH",
    "load_m6e_product_effective_date",
    "load_official_product_effective_dates",
    "load_remaining_product_effective_dates",
]
