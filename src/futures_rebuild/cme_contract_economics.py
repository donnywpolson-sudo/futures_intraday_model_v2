"""Immutable CME contract-economics evidence; retrieval is deliberately external.

This module accepts already-captured source bytes.  It never opens a network
connection and has no command-line execution surface.  A Codex-approved
orchestrator is responsible for obtaining the bytes before publication.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Sequence

from .boundary import RepoBoundary
from .canonical import canonical_bytes, sha256_bytes, sha256_file, sha256_json
from .data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher as AtomicPublisher,
)
from .economics import VerifiedEconomicsRegistry
from .errors import ContractError, IntegrityError
from .foundation.economics import EconomicsRuleBook
from .high_risk import confirmation_required
from .time_contracts import require_utc


CME_EVIDENCE_RELEASE_KIND = "cme_contract_economics_evidence"
CME_EVIDENCE_SCHEMA_VERSION = "1.0.0"
CME_EVIDENCE_GAP_REPORT_RELEASE_KIND = "cme_contract_economics_gap_report"
CME_EVIDENCE_GAP_REPORT_SCHEMA_VERSION = "1.0.0"
MAX_CME_SOURCE_SNAPSHOTS = 48
_SOURCE_ID = re.compile(r"CME_[A-Z0-9_]+")
_IDENTITY_HASH = re.compile(r"[0-9a-f]{64}")
_MARKETS = frozenset({"ES", "CL", "ZN", "6E"})
_HISTORY_START = datetime(2018, 1, 1, tzinfo=timezone.utc)
_HISTORY_END = datetime(2022, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
_DOCUMENT_KINDS = frozenset({"baseline", "checkpoint", "amendment"})


def prepare_phase8_cme_evidence_capture() -> dict[str, object]:
    """Describe the bounded capture; execution remains a Codex-only boundary."""

    return confirmation_required(
        "Capture official CME contract-economics evidence for Phase 8",
        scope={
            "markets": "ES, CL, ZN, 6E",
            "maximum_source_snapshots": str(MAX_CME_SOURCE_SNAPSHOTS),
            "historical_coverage": "every observed actual-contract identity",
            "provider_calls": "0",
        },
        outputs=(
            "immutable cme_contract_economics_evidence release",
            "derived actual_contract_economics validation report",
            "immutable CME evidence gap report if historic proof is incomplete",
        ),
        preservation=(
            "Preserve accepted releases and existing rulebook bytes; create only "
            "a new immutable evidence successor and stop on missing historic coverage."
        ),
    )


@dataclass(frozen=True)
class VerifiedCmeContractEconomics:
    actual_identity_hash: str
    market: str
    product_family: str
    asset_class: str
    currency: str
    contract_unit_quantity: Decimal
    effective_at: datetime
    available_at: datetime
    point_value: Decimal
    tick_size: Decimal
    tick_value: Decimal
    quote_convention_id: str
    source_ids: tuple[str, ...]

    @property
    def record_id(self) -> str:
        return sha256_json(
            {
                "actual_identity_hash": self.actual_identity_hash,
                "asset_class": self.asset_class,
                "available_at": self.available_at.isoformat(),
                "currency": self.currency,
                "contract_unit_quantity": str(self.contract_unit_quantity),
                "effective_at": self.effective_at.isoformat(),
                "market": self.market,
                "point_value": str(self.point_value),
                "product_family": self.product_family,
                "quote_convention_id": self.quote_convention_id,
                "source_ids": list(self.source_ids),
                "tick_size": str(self.tick_size),
                "tick_value": str(self.tick_value),
            }
        )


@dataclass(frozen=True)
class VerifiedCmeEvidenceRegistry:
    receipt: VerifiedReleaseReceipt
    records: Mapping[str, VerifiedCmeContractEconomics]
    source_ids: frozenset[str]

    @classmethod
    def from_release(
        cls, receipt: VerifiedReleaseReceipt, boundary: RepoBoundary
    ) -> "VerifiedCmeEvidenceRegistry":
        manifest = receipt.verify(boundary)
        if (
            receipt.phase != "reference"
            or manifest.release_kind != CME_EVIDENCE_RELEASE_KIND
            or manifest.schema_version != CME_EVIDENCE_SCHEMA_VERSION
            or manifest.source_release_ids
        ):
            raise IntegrityError("CME economics evidence receipt has the wrong contract")
        path = receipt.resolve_file(
            "data/reference/economics/cme_contract_economics_evidence.json", boundary
        )
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise IntegrityError("CME economics evidence JSON is invalid") from exc
        if not isinstance(payload, dict) or set(payload) != {"records", "schema_version", "sources"}:
            raise IntegrityError("CME economics evidence schema is invalid")
        if payload["schema_version"] != CME_EVIDENCE_SCHEMA_VERSION:
            raise IntegrityError("CME economics evidence schema version is invalid")
        sources = _parse_sources(payload["sources"], receipt, boundary)
        records = _parse_records(payload["records"], sources)
        expected_files = {
            "data/reference/economics/cme_contract_economics_evidence.json",
            *(
                f"data/reference/economics/{source_id}.bin"
                for source_id in sources
            ),
        }
        if {entry.logical_path for entry in manifest.files} != expected_files:
            raise IntegrityError("CME economics evidence files differ from its registry")
        _validate_historical_coverage(sources)
        return cls(receipt, MappingProxyType(records), frozenset(sources))

    def resolve(self, actual_identity_hash: str) -> VerifiedCmeContractEconomics:
        try:
            return self.records[actual_identity_hash]
        except KeyError as exc:
            raise IntegrityError("CME economics evidence does not cover actual contract") from exc


@dataclass(frozen=True)
class VerifiedCmeEvidenceGapReport:
    """An immutable failure record; it is never economics evidence."""

    receipt: VerifiedReleaseReceipt
    uncovered_intervals: tuple[Mapping[str, str], ...]

    @classmethod
    def from_release(
        cls, receipt: VerifiedReleaseReceipt, boundary: RepoBoundary
    ) -> "VerifiedCmeEvidenceGapReport":
        manifest = receipt.verify(boundary)
        if (
            receipt.phase != "reference"
            or manifest.release_kind != CME_EVIDENCE_GAP_REPORT_RELEASE_KIND
            or manifest.schema_version != CME_EVIDENCE_GAP_REPORT_SCHEMA_VERSION
            or manifest.source_release_ids
        ):
            raise IntegrityError("CME economics gap report has the wrong contract")
        path = receipt.resolve_file(
            "data/reference/economics/cme_contract_economics_gap_report.json", boundary
        )
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise IntegrityError("CME economics gap report JSON is invalid") from exc
        if not isinstance(payload, dict) or set(payload) != {
            "inspected_sources", "schema_version", "uncovered_intervals"
        } or payload["schema_version"] != CME_EVIDENCE_GAP_REPORT_SCHEMA_VERSION:
            raise IntegrityError("CME economics gap report schema is invalid")
        intervals = _parse_gap_intervals(payload["uncovered_intervals"])
        _parse_gap_sources(payload["inspected_sources"])
        if {entry.logical_path for entry in manifest.files} != {
            "data/reference/economics/cme_contract_economics_gap_report.json"
        }:
            raise IntegrityError("CME economics gap report files are invalid")
        return cls(receipt, intervals)


@dataclass(frozen=True)
class DbnEconomicsCrosscheck:
    """The fields read from one verified DBN definition, supplied by orchestration."""

    actual_identity_hash: str
    market: str
    currency: str
    tick_size: Decimal
    contract_unit_quantity: Decimal


def crosscheck_cme_against_dbn(
    evidence: VerifiedCmeEvidenceRegistry, check: DbnEconomicsCrosscheck
) -> VerifiedCmeContractEconomics:
    """Require CME primary values and DBN actual-contract fields to agree."""

    record = evidence.resolve(check.actual_identity_hash)
    if (
        record.market != check.market
        or record.currency != check.currency
        or record.tick_size != check.tick_size
        or record.contract_unit_quantity != check.contract_unit_quantity
    ):
        raise IntegrityError("CME economics evidence contradicts the DBN definition")
    if record.market == "ZN" and len(record.source_ids) + 1 < 2:
        raise IntegrityError("ZN economics requires CME and DBN independent sources")
    return record


def validate_phase8_authoritative_economics(
    registry: VerifiedEconomicsRegistry,
    evidence: VerifiedCmeEvidenceRegistry,
    dbn_checks: Sequence[DbnEconomicsCrosscheck],
) -> None:
    """Bind a derived registry to CME primary evidence and DBN cross-checks.

    This is intentionally separate from the historic generic reader: old
    immutable releases remain readable, while every new Phase 8 registry must
    satisfy this stronger contract before evaluation can consume it.
    """

    manifest = registry.release_receipt.verify(registry.boundary)
    evidence.receipt.verify(registry.boundary)
    if evidence.receipt.release_id not in manifest.source_release_ids:
        raise IntegrityError("actual economics registry omits its CME evidence provenance")
    checks_by_identity = {item.actual_identity_hash: item for item in dbn_checks}
    if len(checks_by_identity) != len(dbn_checks) or set(checks_by_identity) != set(evidence.records):
        raise IntegrityError("Phase 8 CME evidence coverage is incomplete or ambiguous")
    for actual_identity_hash in sorted(checks_by_identity):
        cme = crosscheck_cme_against_dbn(evidence, checks_by_identity[actual_identity_hash])
        try:
            derived = registry.records[actual_identity_hash]
        except KeyError as exc:
            raise IntegrityError("actual economics registry omits CME-covered identity") from exc
        expected_sources = tuple(sorted((*cme.source_ids, "DATABENTO_DEFINITION_GLBX_MDP3")))
        if (
            derived.asset_class != cme.asset_class
            or derived.currency != cme.currency
            or derived.point_value != cme.point_value
            or derived.tick_size != cme.tick_size
            or derived.tick_value != cme.tick_value
            or derived.quote_convention_id != cme.quote_convention_id
            or derived.verification_source_ids != expected_sources
        ):
            raise IntegrityError("actual economics registry contradicts CME or DBN evidence")


def _publish_phase8_authoritative_actual_economics(
    *,
    evidence: VerifiedCmeEvidenceRegistry,
    dbn_checks: Sequence[DbnEconomicsCrosscheck],
    rulebook: EconomicsRuleBook,
    causal_receipt: VerifiedReleaseReceipt,
    definition_receipt: VerifiedReleaseReceipt,
    policy_receipt: VerifiedReleaseReceipt,
    session_receipt: VerifiedReleaseReceipt,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    """Publish a new Phase 8 registry from CME primary evidence only.

    Callers are responsible for the conversational high-risk approval before
    invoking this low-level publisher.  It performs no retrieval itself.
    """

    if publisher.boundary != boundary or evidence.receipt.repository_id != boundary.repository_id:
        raise IntegrityError("Phase 8 economics publisher or CME evidence has the wrong boundary")
    upstream = (
        causal_receipt,
        definition_receipt,
        policy_receipt,
        session_receipt,
        evidence.receipt,
    )
    for receipt in upstream:
        receipt.verify(boundary)
    checks_by_identity = {item.actual_identity_hash: item for item in dbn_checks}
    if len(checks_by_identity) != len(dbn_checks) or set(checks_by_identity) != set(evidence.records):
        raise IntegrityError("Phase 8 actual economics coverage is incomplete or ambiguous")
    records: list[dict[str, object]] = []
    for identity in sorted(checks_by_identity):
        cme = crosscheck_cme_against_dbn(evidence, checks_by_identity[identity])
        try:
            rule = rulebook.rules[cme.market]
        except KeyError as exc:
            raise IntegrityError("CME evidence market is absent from the compatibility rulebook") from exc
        if (
            rule.point_value != cme.point_value
            or rule.expected_unit_qty != cme.contract_unit_quantity
            or rule.quote_convention != cme.quote_convention_id
        ):
            raise IntegrityError("CME evidence contradicts the compatibility rulebook")
        records.append(
            {
                "actual_identity_hash": identity,
                "ambiguity_reasons": [],
                "asset_class": cme.asset_class,
                "available_at": cme.available_at.isoformat(),
                "currency": cme.currency,
                "effective_at": cme.effective_at.isoformat(),
                "point_value": str(cme.point_value),
                "quote_convention_id": cme.quote_convention_id,
                "source_fields_used": [
                    "cme_captured_contract_economics",
                    "provider_unit_of_measure_qty",
                ],
                "source_received_at": cme.available_at.isoformat(),
                "tick_size": str(cme.tick_size),
                "tick_value": str(cme.tick_value),
                "verification_source_ids": sorted(
                    [*cme.source_ids, "DATABENTO_DEFINITION_GLBX_MDP3"]
                ),
            }
        )
    stage = publisher.create_stage("phase8_authoritative_economics")
    payload = {"records": records, "schema_version": "1.1.0"}
    filename = "contract_economics.json"
    (stage / filename).write_bytes(canonical_bytes(payload) + b"\n")
    manifest = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind="actual_contract_economics",
        schema_version="1.1.0",
        logical_paths={filename: "data/reference/economics/contract_economics.json"},
        source_release_ids=tuple(sorted(item.release_id for item in upstream)),
        metadata={
            "cme_evidence_receipt_id": evidence.receipt.receipt_id,
            "market_scope": sorted({record.market for record in evidence.records.values()}),
            "phase8_authoritative": True,
        },
    )
    receipt = VerifiedReleaseReceipt.from_manifest(publisher.publish(stage, manifest), boundary)
    registry = VerifiedEconomicsRegistry.from_release(receipt, boundary)
    validate_phase8_authoritative_economics(registry, evidence, dbn_checks)
    return receipt


def _parse_time(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise IntegrityError(f"{name} must be an ISO timestamp")
    try:
        return require_utc(datetime.fromisoformat(value), name)
    except ValueError as exc:
        raise IntegrityError(f"{name} is invalid") from exc


def _parse_decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, str):
        raise IntegrityError(f"{name} must be a decimal string")
    try:
        result = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise IntegrityError(f"{name} is invalid") from exc
    if not result.is_finite() or result <= 0:
        raise IntegrityError(f"{name} must be positive")
    return result


@dataclass(frozen=True)
class _CmeSource:
    source_id: str
    market: str
    effective_from: datetime
    effective_until: datetime
    published_at: datetime
    retrieved_at: datetime


@dataclass(frozen=True)
class CapturedCmeSource:
    """A bounded CME-hosted historical document captured by orchestration."""

    source_id: str
    market: str
    locator: str
    document_kind: str
    published_at: datetime
    effective_from: datetime
    effective_until: datetime
    retrieved_at: datetime
    content: bytes


def _parse_sources(
    raw: object, receipt: VerifiedReleaseReceipt, boundary: RepoBoundary
) -> dict[str, _CmeSource]:
    if not isinstance(raw, list):
        raise IntegrityError("CME economics evidence sources are invalid")
    if not raw or len(raw) > MAX_CME_SOURCE_SNAPSHOTS:
        raise IntegrityError("CME economics evidence exceeds the source snapshot limit")
    parsed: dict[str, _CmeSource] = {}
    for item in raw:
        if not isinstance(item, dict) or set(item) != {
            "content_sha256", "document_kind", "effective_from", "effective_until",
            "locator", "market", "published_at", "retrieved_at", "source_id"
        }:
            raise IntegrityError("CME source schema is invalid")
        source_id = item["source_id"]
        locator = item["locator"]
        digest = item["content_sha256"]
        market = item["market"]
        if (
            not isinstance(source_id, str)
            or _SOURCE_ID.fullmatch(source_id) is None
            or source_id in parsed
            or not isinstance(locator, str)
            or not locator.startswith("https://www.cmegroup.com/")
            or "contractspecs.html" in locator.lower()
            or not isinstance(market, str)
            or market not in _MARKETS
            or item["document_kind"] not in _DOCUMENT_KINDS
            or not isinstance(digest, str)
            or _IDENTITY_HASH.fullmatch(digest) is None
        ):
            raise IntegrityError("CME source identity is invalid or mutable")
        retrieved = _parse_time(item["retrieved_at"], "CME source retrieved_at")
        published = _parse_time(item["published_at"], "CME source published_at")
        effective_from = _parse_time(item["effective_from"], "CME source effective_from")
        effective_until = _parse_time(item["effective_until"], "CME source effective_until")
        if effective_from > effective_until or published > _HISTORY_END:
            raise IntegrityError("CME source cannot establish historical coverage")
        source_path = receipt.resolve_file(
            f"data/reference/economics/{source_id}.bin", boundary
        )
        if sha256_file(source_path) != digest:
            raise IntegrityError("CME source bytes differ from their declared hash")
        parsed[source_id] = _CmeSource(
            source_id, market, effective_from, effective_until, published, retrieved
        )
    if not parsed or list(parsed) != sorted(parsed):
        raise IntegrityError("CME sources must be nonempty and canonically ordered")
    return parsed


def _parse_records(
    raw: object, sources: Mapping[str, _CmeSource]
) -> dict[str, VerifiedCmeContractEconomics]:
    if not isinstance(raw, list):
        raise IntegrityError("CME economics evidence records are invalid")
    expected = {
        "actual_identity_hash", "asset_class", "available_at", "contract_unit_quantity", "currency",
        "effective_at", "market", "point_value", "product_family",
        "quote_convention_id", "source_ids", "tick_size", "tick_value",
    }
    parsed: dict[str, VerifiedCmeContractEconomics] = {}
    previous = ""
    for raw_record in raw:
        if not isinstance(raw_record, dict) or set(raw_record) != expected:
            raise IntegrityError("CME economics record schema is invalid")
        actual_hash = raw_record["actual_identity_hash"]
        market = raw_record["market"]
        source_ids_raw = raw_record["source_ids"]
        if (
            not isinstance(actual_hash, str)
            or _IDENTITY_HASH.fullmatch(actual_hash) is None
            or actual_hash <= previous
            or not isinstance(market, str)
            or market not in _MARKETS
            or not isinstance(source_ids_raw, list)
            or any(not isinstance(item, str) for item in source_ids_raw)
        ):
            raise IntegrityError("CME economics record identity is invalid")
        source_ids = tuple(source_ids_raw)
        if not source_ids or source_ids != tuple(sorted(set(source_ids))) or any(item not in sources for item in source_ids):
            raise IntegrityError("CME economics record provenance is invalid")
        effective = _parse_time(raw_record["effective_at"], "CME effective_at")
        available = _parse_time(raw_record["available_at"], "CME available_at")
        if (
            effective > available
            or any(available > sources[item].retrieved_at for item in source_ids)
            or not any(
                source.market == market
                and source.effective_from <= effective <= source.effective_until
                and source.published_at <= available
                for source in (sources[item] for item in source_ids)
            )
        ):
            raise IntegrityError("CME economics history is not temporally supported")
        point = _parse_decimal(raw_record["point_value"], "CME point_value")
        contract_unit_quantity = _parse_decimal(
            raw_record["contract_unit_quantity"], "CME contract_unit_quantity"
        )
        tick_size = _parse_decimal(raw_record["tick_size"], "CME tick_size")
        tick_value = _parse_decimal(raw_record["tick_value"], "CME tick_value")
        if tick_size * point != tick_value:
            raise IntegrityError("CME tick value is inconsistent with point value")
        if (
            not isinstance(raw_record["product_family"], str) or not raw_record["product_family"]
            or not isinstance(raw_record["asset_class"], str) or not raw_record["asset_class"]
            or not isinstance(raw_record["currency"], str) or re.fullmatch(r"[A-Z]{3}", raw_record["currency"]) is None
            or not isinstance(raw_record["quote_convention_id"], str) or not raw_record["quote_convention_id"]
        ):
            raise IntegrityError("CME economics descriptive fields are invalid")
        parsed[actual_hash] = VerifiedCmeContractEconomics(
            actual_hash, market, raw_record["product_family"], raw_record["asset_class"],
            raw_record["currency"], contract_unit_quantity, effective, available, point, tick_size,
            tick_value, raw_record["quote_convention_id"], source_ids,
        )
        previous = actual_hash
    if not parsed:
        raise IntegrityError("CME economics evidence cannot be empty")
    return parsed


def _validate_historical_coverage(sources: Mapping[str, _CmeSource]) -> None:
    """Require an explicit, uninterrupted CME-hosted chain for every market."""

    for market in sorted(_MARKETS):
        chain = sorted(
            (source for source in sources.values() if source.market == market),
            key=lambda source: (source.effective_from, source.effective_until, source.source_id),
        )
        if not chain or chain[0].effective_from > _HISTORY_START:
            raise IntegrityError(f"CME historical coverage lacks a {market} baseline")
        if chain[0].published_at > _HISTORY_START or chain[0].effective_from > _HISTORY_START:
            raise IntegrityError(f"CME historical coverage lacks a dated {market} baseline")
        coverage_end = chain[0].effective_until
        for source in chain[1:]:
            if source.effective_from > coverage_end:
                raise IntegrityError(f"CME historical coverage has a {market} interval gap")
            coverage_end = max(coverage_end, source.effective_until)
        if coverage_end < _HISTORY_END:
            raise IntegrityError(f"CME historical coverage ends before 2022 for {market}")


def _parse_gap_intervals(raw: object) -> tuple[Mapping[str, str], ...]:
    if not isinstance(raw, list) or not raw:
        raise IntegrityError("CME economics gap report requires uncovered intervals")
    parsed: list[Mapping[str, str]] = []
    previous = ""
    for item in raw:
        if not isinstance(item, dict) or set(item) != {"market", "reason", "until", "from"}:
            raise IntegrityError("CME economics gap interval schema is invalid")
        if (
            not isinstance(item["market"], str)
            or item["market"] not in _MARKETS
            or not isinstance(item["reason"], str)
            or not item["reason"]
        ):
            raise IntegrityError("CME economics gap interval is invalid")
        start = _parse_time(item["from"], "CME gap from")
        end = _parse_time(item["until"], "CME gap until")
        canonical = f"{item['market']}:{start.isoformat()}:{end.isoformat()}"
        if start > end or canonical <= previous:
            raise IntegrityError("CME economics gap intervals are not canonically ordered")
        previous = canonical
        parsed.append(MappingProxyType(dict(item)))
    return tuple(parsed)


def _parse_gap_sources(raw: object) -> None:
    if not isinstance(raw, list) or len(raw) > MAX_CME_SOURCE_SNAPSHOTS:
        raise IntegrityError("CME economics gap report sources are invalid")
    previous = ""
    for item in raw:
        if not isinstance(item, dict) or set(item) != {
            "content_sha256", "locator", "market", "retrieved_at"
        }:
            raise IntegrityError("CME economics gap report source schema is invalid")
        if (
            not isinstance(item["locator"], str)
            or not item["locator"].startswith("https://www.cmegroup.com/")
            or not isinstance(item["market"], str)
            or item["market"] not in _MARKETS
            or not isinstance(item["content_sha256"], str)
            or _IDENTITY_HASH.fullmatch(item["content_sha256"]) is None
        ):
            raise IntegrityError("CME economics gap report source is invalid")
        _parse_time(item["retrieved_at"], "CME gap source retrieved_at")
        canonical = f"{item['market']}:{item['locator']}"
        if canonical <= previous:
            raise IntegrityError("CME economics gap sources are not canonically ordered")
        previous = canonical


def _publish_cme_contract_economics_evidence(
    *,
    sources: Sequence[CapturedCmeSource],
    records: Sequence[Mapping[str, object]],
    expected_actual_identity_hashes: Sequence[str],
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    """Publish already captured CME bytes; this function never retrieves them."""

    if publisher.boundary != boundary:
        raise ContractError("CME evidence publisher belongs to another boundary")
    if not sources or len(sources) > MAX_CME_SOURCE_SNAPSHOTS:
        raise ContractError("CME evidence source snapshot limit is invalid")
    source_ids = [source.source_id for source in sources]
    expected_identities = tuple(sorted(expected_actual_identity_hashes))
    if (
        len(set(source_ids)) != len(source_ids)
        or tuple(sorted(source_ids)) != tuple(source_ids)
        or not expected_identities
        or len(set(expected_identities)) != len(expected_identities)
        or any(_IDENTITY_HASH.fullmatch(identity) is None for identity in expected_identities)
    ):
        raise ContractError("CME evidence identity input is invalid")
    source_contracts = {
        source.source_id: _CmeSource(
            source.source_id,
            source.market,
            require_utc(source.effective_from, "CME effective_from"),
            require_utc(source.effective_until, "CME effective_until"),
            require_utc(source.published_at, "CME published_at"),
            require_utc(source.retrieved_at, "CME retrieved_at"),
        )
        for source in sources
    }
    _validate_historical_coverage(source_contracts)
    stage = publisher.create_stage("cme_contract_economics")
    source_payload: list[dict[str, str]] = []
    logical_paths: dict[str, str] = {}
    for source in sources:
        if not isinstance(source.content, bytes):
            raise ContractError("CME source content must be bytes")
        filename = f"{source.source_id}.bin"
        (stage / filename).write_bytes(source.content)
        logical_paths[filename] = f"data/reference/economics/{filename}"
        source_payload.append(
            {
                "content_sha256": sha256_bytes(source.content),
                "document_kind": source.document_kind,
                "effective_from": source_contracts[source.source_id].effective_from.isoformat(),
                "effective_until": source_contracts[source.source_id].effective_until.isoformat(),
                "locator": source.locator,
                "market": source.market,
                "published_at": source_contracts[source.source_id].published_at.isoformat(),
                "retrieved_at": source_contracts[source.source_id].retrieved_at.isoformat(),
                "source_id": source.source_id,
            }
        )
    payload = {
        "records": [dict(record) for record in records],
        "schema_version": CME_EVIDENCE_SCHEMA_VERSION,
        "sources": source_payload,
    }
    registry_name = "cme_contract_economics_evidence.json"
    (stage / registry_name).write_bytes(canonical_bytes(payload) + b"\n")
    logical_paths[registry_name] = "data/reference/economics/cme_contract_economics_evidence.json"
    manifest = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=CME_EVIDENCE_RELEASE_KIND,
        schema_version=CME_EVIDENCE_SCHEMA_VERSION,
        logical_paths=logical_paths,
        metadata={"actual_identity_count": len(records), "market_scope": sorted(_MARKETS.intersection({str(item.get("market")) for item in records}))},
    )
    receipt = VerifiedReleaseReceipt.from_manifest(publisher.publish(stage, manifest), boundary)
    verified = VerifiedCmeEvidenceRegistry.from_release(receipt, boundary)
    if tuple(verified.records) != expected_identities:
        raise IntegrityError("CME evidence does not cover exactly the expected actual identities")
    return receipt


def _publish_cme_contract_economics_gap_report(
    *,
    uncovered_intervals: Sequence[Mapping[str, str]],
    inspected_sources: Sequence[Mapping[str, str]],
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    """Publish a failure report without retaining any downloaded source bytes."""

    if publisher.boundary != boundary:
        raise ContractError("CME gap report publisher belongs to another boundary")
    payload = {
        "inspected_sources": [dict(source) for source in inspected_sources],
        "schema_version": CME_EVIDENCE_GAP_REPORT_SCHEMA_VERSION,
        "uncovered_intervals": [dict(interval) for interval in uncovered_intervals],
    }
    _parse_gap_intervals(payload["uncovered_intervals"])
    _parse_gap_sources(payload["inspected_sources"])
    stage = publisher.create_stage("cme_contract_economics_gap_report")
    filename = "cme_contract_economics_gap_report.json"
    (stage / filename).write_bytes(canonical_bytes(payload) + b"\n")
    manifest = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=CME_EVIDENCE_GAP_REPORT_RELEASE_KIND,
        schema_version=CME_EVIDENCE_GAP_REPORT_SCHEMA_VERSION,
        logical_paths={filename: "data/reference/economics/cme_contract_economics_gap_report.json"},
        metadata={"authoritative_economics": False, "publication_status": "GAP_ONLY"},
    )
    receipt = VerifiedReleaseReceipt.from_manifest(publisher.publish(stage, manifest), boundary)
    VerifiedCmeEvidenceGapReport.from_release(receipt, boundary)
    return receipt
