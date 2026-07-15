"""Verified actual-contract economics required before P&L or active inference."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Mapping

from .boundary import RepoBoundary
from .canonical import sha256_json
from .errors import ContractError, IntegrityError
from .identity import ActualContractIdentity
from .release import VerifiedReleaseReceipt
from .time_contracts import require_utc


FORBIDDEN_TICK_VALUE_AUTHORITIES = {"min_price_increment_amount"}
_VERIFIED_ECONOMICS_FACTORY = object()


@dataclass(frozen=True)
class VerifiedContractEconomics:
    actual_identity_hash: str
    economics_release_receipt_id: str
    tick_size: Decimal
    tick_value: Decimal
    point_value: Decimal
    currency: str
    asset_class: str
    quote_convention_id: str
    verification_source_ids: tuple[str, ...]
    source_fields_used: tuple[str, ...]
    effective_at: datetime
    source_received_at: datetime
    available_at: datetime
    record_id: str
    _factory_token: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._factory_token is not _VERIFIED_ECONOMICS_FACTORY:
            raise ContractError(
                "verified economics can only be created from a verified release"
            )

    @property
    def economics_hash(self) -> str:
        return self.record_id

    def as_dict(self) -> dict[str, object]:
        return {
            "actual_identity_hash": self.actual_identity_hash,
            "asset_class": self.asset_class,
            "available_at": self.available_at.isoformat(),
            "currency": self.currency,
            "economics_release_receipt_id": self.economics_release_receipt_id,
            "effective_at": self.effective_at.isoformat(),
            "point_value": str(self.point_value),
            "quote_convention_id": self.quote_convention_id,
            "record_id": self.record_id,
            "source_fields_used": list(self.source_fields_used),
            "source_received_at": self.source_received_at.isoformat(),
            "tick_size": str(self.tick_size),
            "tick_value": str(self.tick_value),
            "verification_source_ids": list(self.verification_source_ids),
        }


@dataclass(frozen=True)
class VerifiedEconomicsRegistry:
    release_receipt: VerifiedReleaseReceipt
    records: Mapping[str, VerifiedContractEconomics]
    registry_hash: str
    boundary: RepoBoundary
    _factory_token: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self._factory_token is not _VERIFIED_ECONOMICS_FACTORY:
            raise ContractError(
                "verified economics registry requires the release factory"
            )

    @classmethod
    def from_release(
        cls, receipt: VerifiedReleaseReceipt, boundary: RepoBoundary
    ) -> "VerifiedEconomicsRegistry":
        manifest = receipt.verify(boundary)
        if manifest.release_kind != "actual_contract_economics":
            raise IntegrityError("economics receipt has the wrong release kind")
        if {entry.path for entry in manifest.files} != {"contract_economics.json"}:
            raise IntegrityError("economics release must contain exactly one registry file")
        path = boundary.active_root / receipt.relative_root / "contract_economics.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise IntegrityError("economics registry JSON is invalid") from exc
        if (
            not isinstance(payload, dict)
            or set(payload) != {"records", "schema_version"}
            or payload.get("schema_version") != "1.0.0"
            or not isinstance(payload.get("records"), list)
            or not payload["records"]
        ):
            raise IntegrityError("economics registry schema/version is invalid")
        expected_fields = {
            "actual_identity_hash",
            "ambiguity_reasons",
            "asset_class",
            "available_at",
            "currency",
            "effective_at",
            "point_value",
            "quote_convention_id",
            "source_fields_used",
            "source_received_at",
            "tick_size",
            "tick_value",
            "verification_source_ids",
        }
        records: dict[str, VerifiedContractEconomics] = {}
        for raw in payload["records"]:
            if not isinstance(raw, dict) or set(raw) != expected_fields:
                raise IntegrityError("economics row schema is invalid")
            try:
                if (
                    any(
                        type(raw[name]) is not str
                        for name in (
                            "actual_identity_hash",
                            "asset_class",
                            "available_at",
                            "currency",
                            "effective_at",
                            "point_value",
                            "quote_convention_id",
                            "source_received_at",
                            "tick_size",
                            "tick_value",
                        )
                    )
                    or any(
                        not isinstance(raw[name], list)
                        or any(type(item) is not str for item in raw[name])
                        for name in (
                            "ambiguity_reasons",
                            "source_fields_used",
                            "verification_source_ids",
                        )
                    )
                ):
                    raise ContractError("economics row field types are not exact")
                ambiguity = tuple(raw["ambiguity_reasons"])
                sources = tuple(raw["verification_source_ids"])
                source_fields = tuple(raw["source_fields_used"])
                if ambiguity:
                    raise ContractError("ambiguous contract economics cannot be verified")
                if (
                    not sources
                    or sources != tuple(sorted(set(sources)))
                    or not source_fields
                    or source_fields != tuple(sorted(set(source_fields)))
                ):
                    raise ContractError("economics provenance must be explicit and canonical")
                if FORBIDDEN_TICK_VALUE_AUTHORITIES.intersection(source_fields):
                    raise ContractError(
                        "min_price_increment_amount cannot authorize tick value"
                    )
                tick_size = Decimal(raw["tick_size"])
                tick_value = Decimal(raw["tick_value"])
                point_value = Decimal(raw["point_value"])
                if any(
                    not value.is_finite() or value <= 0
                    for value in (tick_size, tick_value, point_value)
                ) or tick_size * point_value != tick_value:
                    raise ContractError("economics values are nonpositive or inconsistent")
                effective = require_utc(
                    datetime.fromisoformat(raw["effective_at"]), "economics.effective_at"
                )
                received = require_utc(
                    datetime.fromisoformat(raw["source_received_at"]),
                    "economics.source_received_at",
                )
                available = require_utc(
                    datetime.fromisoformat(raw["available_at"]),
                    "economics.available_at",
                )
                if not (effective <= received <= available):
                    raise ContractError("economics bitemporal chronology is invalid")
                actual_hash = raw["actual_identity_hash"]
                if re.fullmatch(r"[0-9a-f]{64}", actual_hash) is None:
                    raise ContractError("economics actual-identity hash is invalid")
                currency = raw["currency"]
                asset_class = raw["asset_class"]
                quote = raw["quote_convention_id"]
                if re.fullmatch(r"[A-Z]{3}", currency) is None or not asset_class:
                    raise ContractError("economics currency/asset class is invalid")
                if asset_class == "RATES" and (not quote or len(sources) < 2):
                    raise ContractError(
                        "rates economics require a quote convention and two independent sources"
                    )
                core = {
                    **raw,
                    "economics_release_receipt_id": receipt.receipt_id,
                }
                record = VerifiedContractEconomics(
                    actual_identity_hash=actual_hash,
                    economics_release_receipt_id=receipt.receipt_id,
                    tick_size=tick_size,
                    tick_value=tick_value,
                    point_value=point_value,
                    currency=currency,
                    asset_class=asset_class,
                    quote_convention_id=quote,
                    verification_source_ids=sources,
                    source_fields_used=source_fields,
                    effective_at=effective,
                    source_received_at=received,
                    available_at=available,
                    record_id=sha256_json(core),
                    _factory_token=_VERIFIED_ECONOMICS_FACTORY,
                )
            except (TypeError, ValueError, InvalidOperation, ContractError) as exc:
                raise IntegrityError("economics row is not verified") from exc
            if actual_hash in records:
                raise IntegrityError("economics registry has ambiguous actual-contract rows")
            records[actual_hash] = record
        registry_core = {
            "records": {
                key: records[key].as_dict() for key in sorted(records)
            },
            "release_receipt": receipt.as_dict(),
        }
        return cls(
            receipt,
            MappingProxyType(records),
            sha256_json(registry_core),
            boundary,
            _VERIFIED_ECONOMICS_FACTORY,
        )

    def verify(self) -> None:
        rebuilt = type(self).from_release(self.release_receipt, self.boundary)
        if (
            rebuilt.registry_hash != self.registry_hash
            or dict(rebuilt.records) != dict(self.records)
        ):
            raise IntegrityError("economics registry changed after verification")

    def resolve(
        self, actual: ActualContractIdentity, decision_at: datetime
    ) -> VerifiedContractEconomics:
        self.verify()
        decision = require_utc(decision_at, "decision_at")
        try:
            record = self.records[actual.identity_hash]
        except KeyError as exc:
            raise ContractError("actual contract has no verified economics") from exc
        if (
            record.currency != actual.currency
            or record.tick_size != actual.min_tick
            or record.point_value != actual.multiplier
            or record.tick_value != actual.min_tick * actual.multiplier
            or record.available_at > decision
            or record.source_received_at > decision
            or record.effective_at > decision
        ):
            raise ContractError("actual-contract economics are mismatched or not causal")
        return record
