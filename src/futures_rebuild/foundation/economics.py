"""Explicit quote-convention economics; provider sentinel fields never authorize P&L."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from ..canonical import sha256_json
from ..errors import ContractError, IntegrityError
from .records import ProviderDefinition


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
            or payload.get("rules_version") != "1.1.0"
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
        if authoritative != ["DATABENTO_DEFINITION_GLBX_MDP3"] or any(
            value["authoritative"] is False
            and value["binding"] != "MUTABLE_PUBLIC_REFERENCE_NOT_TRUST_EVIDENCE"
            for value in sources.values()
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
        if len(parsed) != 33:
            raise IntegrityError("economics rulebook must cover exactly 33 declared markets")
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
            rulebook_hash=self.rulebook_hash,
            provider_unit_qty_state=quantity_state,
        )
