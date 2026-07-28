"""Explicit quote-convention economics; provider sentinel fields never authorize P&L."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from ..canonical import sha256_json
from ..errors import ContractError, IntegrityError
from .records import ProviderDefinition


_RULEBOOK_VERSION = "1.3.0"
_AUTHORITATIVE_DBN_RELEASE_ID = (
    "086282eaef7b36a61626f88d93d06c93b87c1cb3407c936d065d0d1b9d98599e"
)
_APPROVED_MARKETS = frozenset(
    {
        "6A",
        "6B",
        "6C",
        "6E",
        "6J",
        "6M",
        "6N",
        "6S",
        "BTC",
        "CL",
        "ES",
        "ETH",
        "GC",
        "GF",
        "HE",
        "HG",
        "HO",
        "KE",
        "LE",
        "NG",
        "NQ",
        "PA",
        "PL",
        "RB",
        "RTY",
        "SI",
        "SR1",
        "SR3",
        "TN",
        "UB",
        "YM",
        "ZB",
        "ZC",
        "ZF",
        "ZL",
        "ZM",
        "ZN",
        "ZQ",
        "ZS",
        "ZT",
        "ZW",
    }
)


@dataclass(frozen=True)
class EconomicsRule:
    market: str
    point_value: Decimal
    expected_unit_qty: Decimal
    quote_convention: str
    source_ids: tuple[str, ...]


@dataclass(frozen=True)
class ResolvedEconomics:
    market: str
    point_value: Decimal
    tick_size: Decimal
    tick_value: Decimal
    currency: str
    quote_convention: str
    source_ids: tuple[str, ...]
    rulebook_hash: str
    provider_unit_qty_state: str

    @property
    def record_hash(self) -> str:
        return sha256_json(
            {
                "currency": self.currency,
                "market": self.market,
                "point_value": str(self.point_value),
                "provider_unit_qty_state": self.provider_unit_qty_state,
                "quote_convention": self.quote_convention,
                "rulebook_hash": self.rulebook_hash,
                "source_ids": list(self.source_ids),
                "tick_size": str(self.tick_size),
                "tick_value": str(self.tick_value),
            }
        )


@dataclass(frozen=True)
class EconomicsRuleBook:
    rules: Mapping[str, EconomicsRule]
    source_ids: frozenset[str]
    rulebook_hash: str

    @classmethod
    def from_file(cls, path: Path) -> "EconomicsRuleBook":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise IntegrityError("economics rulebook JSON is invalid") from exc
        return cls.from_payload(payload)

    @classmethod
    def from_payload(cls, payload: object) -> "EconomicsRuleBook":
        return cls._from_payload(
            payload,
            allowed_versions=frozenset({_RULEBOOK_VERSION}),
            authoritative_dbn_release_id=_AUTHORITATIVE_DBN_RELEASE_ID,
            exact_markets=_APPROVED_MARKETS,
            required_market=None,
        )

    @classmethod
    def from_embedded_payload(
        cls, payload: object, *, required_market: str
    ) -> "EconomicsRuleBook":
        if (
            not isinstance(required_market, str)
            or required_market not in _APPROVED_MARKETS
        ):
            raise IntegrityError(
                "embedded economics required market is invalid"
            )
        return cls._from_payload(
            payload,
            allowed_versions=frozenset({"1.2.0", _RULEBOOK_VERSION}),
            authoritative_dbn_release_id=None,
            exact_markets=None,
            required_market=required_market,
        )

    @classmethod
    def _from_payload(
        cls,
        payload: object,
        *,
        allowed_versions: frozenset[str],
        authoritative_dbn_release_id: str | None,
        exact_markets: frozenset[str] | None,
        required_market: str | None,
    ) -> "EconomicsRuleBook":
        expected = {
            "authority_policy",
            "currency",
            "dataset",
            "forbidden_authorities",
            "point_value_definition",
            "rules",
            "rules_version",
            "valid_from",
            "verification_sources",
        }
        if (
            not isinstance(payload, dict)
            or set(payload) != expected
            or payload.get("rules_version") not in allowed_versions
            or payload.get("dataset") != "GLBX.MDP3"
            or payload.get("currency") != "USD"
            or payload.get("forbidden_authorities")
            != [
                "min_price_increment_amount",
                "contract_multiplier",
                "legacy_phase1b_multiplier",
            ]
            or not isinstance(payload.get("verification_sources"), dict)
            or not isinstance(payload.get("rules"), list)
        ):
            raise IntegrityError("economics rulebook schema/policy is invalid")
        if payload.get("authority_policy") != {
            "eligible_contract_requires_provider_unit_qty_match": True,
            "mutable_public_urls_authorize_economics": False,
            "provider_sentinel_allowed": False,
            "rulebook_hash_bound_into_every_economics_record": True,
        }:
            raise IntegrityError("economics authority policy is not fail closed")
        sources = payload["verification_sources"]
        required_source_fields = {"authoritative", "binding", "locator", "role"}
        if any(
            not isinstance(key, str)
            or not key
            or not isinstance(value, dict)
            or set(value) != required_source_fields
            or type(value.get("authoritative")) is not bool
            or any(
                not isinstance(value.get(field), str) or not value.get(field)
                for field in ("binding", "locator", "role")
            )
            for key, value in sources.items()
        ):
            raise IntegrityError("economics source registry is invalid")
        authoritative = [
            key for key, value in sources.items() if value["authoritative"] is True
        ]
        databento_source = sources.get("DATABENTO_DEFINITION_GLBX_MDP3")
        databento_locator = (
            databento_source.get("locator")
            if isinstance(databento_source, dict)
            else None
        )
        if authoritative_dbn_release_id is None:
            locator_is_valid = (
                isinstance(databento_locator, str)
                and re.fullmatch(
                    (
                        r"manifests/data_releases/dbn/[0-9a-f]{64}\.json"
                        r"#data/dbn/definition/\{market\}/\{year\}/\{filename\}"
                    ),
                    databento_locator,
                )
                is not None
            )
        else:
            locator_is_valid = databento_locator == (
                "manifests/data_releases/dbn/"
                f"{authoritative_dbn_release_id}.json"
                "#data/dbn/definition/{market}/{year}/{filename}"
            )
        if (
            authoritative != ["DATABENTO_DEFINITION_GLBX_MDP3"]
            or not isinstance(databento_source, dict)
            or databento_source.get("binding")
            != "EXACT_LAYOUT_V2_DBN_RELEASE_LOGICAL_DEFINITION_PATH_AND_PROVIDER_EVENT_TIME"
            or not locator_is_valid
            or any(
            value["authoritative"] is False
            and value["binding"] != "MUTABLE_PUBLIC_REFERENCE_NOT_TRUST_EVIDENCE"
            for value in sources.values()
            )
        ):
            raise IntegrityError("economics authority registry is ambiguous")
        parsed: dict[str, EconomicsRule] = {}
        required_fields = {
            "expected_unit_qty",
            "market",
            "point_value",
            "quote_convention",
            "source_ids",
        }
        for raw in payload["rules"]:
            if not isinstance(raw, dict) or set(raw) != required_fields:
                raise IntegrityError("economics rule schema is not exact")
            try:
                market = raw["market"]
                point = Decimal(raw["point_value"])
                expected_qty = Decimal(raw["expected_unit_qty"])
                quote = raw["quote_convention"]
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise IntegrityError("economics rule numeric/source fields are invalid") from exc
            raw_source_ids = raw["source_ids"]
            if (
                not isinstance(raw_source_ids, list)
                or any(not isinstance(item, str) or not item for item in raw_source_ids)
            ):
                raise IntegrityError("economics source IDs must be an exact string list")
            source_ids = tuple(raw_source_ids)
            if (
                not isinstance(market, str)
                or re.fullmatch(r"[0-9A-Z]{2,3}", market) is None
                or not isinstance(quote, str)
                or not quote
                or not point.is_finite()
                or point <= 0
                or not expected_qty.is_finite()
                or expected_qty <= 0
                or source_ids != tuple(sorted(set(source_ids)))
                or len(source_ids) < 2
                or any(item not in sources for item in source_ids)
                or market in parsed
            ):
                raise IntegrityError("economics rule is ambiguous or incomplete")
            parsed[market] = EconomicsRule(
                market, point, expected_qty, quote, source_ids
            )
        parsed_markets = frozenset(parsed)
        if exact_markets is not None:
            if parsed_markets != exact_markets:
                raise IntegrityError(
                    "economics rulebook must cover exactly the approved 41 markets"
                )
        elif (
            not parsed_markets
            or not parsed_markets.issubset(_APPROVED_MARKETS)
            or required_market not in parsed_markets
        ):
            raise IntegrityError(
                "embedded predecessor economics does not cover the required market"
            )
        return cls(
            MappingProxyType(parsed),
            frozenset(sources),
            sha256_json(payload),
        )

    def resolve(self, market: str, definition: ProviderDefinition) -> ResolvedEconomics:
        if market != definition.market:
            raise ContractError("market and definition family disagree")
        try:
            rule = self.rules[market]
        except KeyError as exc:
            raise ContractError("market has no verified quote-convention rule") from exc
        return _resolve_economics(
            self.rulebook_hash,
            rule,
            market,
            definition,
        )


@lru_cache(maxsize=65_536)
def _resolve_economics(
    rulebook_hash: str,
    rule: EconomicsRule,
    market: str,
    definition: ProviderDefinition,
) -> ResolvedEconomics:
    """Resolve one complete immutable provider definition once per rulebook."""

    if rule.market != market:
        raise ContractError("economics rule and market disagree")
    if not rulebook_hash:
        raise ContractError("economics rulebook hash is required")
    observed = definition.observed_unit_qty
    if observed is None:
        raise ContractError(
            "provider unit quantity is unavailable; economics fail closed"
        )
    if observed != rule.expected_unit_qty:
        raise ContractError("provider unit quantity contradicts the pinned market rule")
    quantity_state = "PROVIDER_DEFINITION_CROSSCHECK_MATCH"
    tick_size = definition.min_tick
    tick_value = tick_size * rule.point_value
    if not tick_value.is_finite() or tick_value <= 0:
        raise ContractError("resolved tick value is invalid")
    return ResolvedEconomics(
        market=market,
        point_value=rule.point_value,
        tick_size=tick_size,
        tick_value=tick_value,
        currency="USD",
        quote_convention=rule.quote_convention,
        source_ids=rule.source_ids,
        rulebook_hash=rulebook_hash,
        provider_unit_qty_state=quantity_state,
    )
