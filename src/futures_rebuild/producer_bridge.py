"""Non-alpha producers bridging verified foundation releases into trust contracts.

Every producer is deterministic, release-bound, and intentionally contains no
provider access, fitting, target construction, hypothesis logic, or candidate
authority.  The only writable objects are AtomicPublisher stages/releases.
"""

from __future__ import annotations

import json
import math
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from types import MappingProxyType
from typing import Iterator, Mapping, Sequence

import pyarrow.parquet as pq

from .boundary import RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .economics import VerifiedEconomicsRegistry
from .errors import ContractError, IntegrityError
from .foundation.economics import ResolvedEconomics
from .foundation.materialize import (
    CAUSAL_RELEASE_KIND,
    load_causal_interval,
    load_raw_interval,
)
from .foundation.parquet import CAUSAL_BAR_SCHEMA, read_definitions
from .foundation.records import ProviderDefinition, datetime_to_ns, ns_to_datetime
from .foundation.support import VerifiedFoundationPolicies
from .identity import ActualContractIdentity, ContractDefinition
from .inference import VerifiedIdentityRegistry
from .ledger import PredictionCensusReceipt, PredictionLedger
from .data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher as AtomicPublisher,
)
from .schemas import (
    FeatureLineage,
    FeatureRow,
    OutcomeCoverageReport,
    OutcomeRow,
    OutcomeStatus,
)
from .session_policy import VerifiedSessionPolicy
from .time_contracts import AvailabilityBasis


DEFINITION_RELEASE_KIND = "actual_contract_definitions"
DEFINITION_SCHEMA_VERSION = "2.1.0"
DEFINITION_INELIGIBILITY_DOCUMENT = "definition_ineligibility.json"
DEFINITION_INELIGIBILITY_SCHEMA_VERSION = "1.0.0"
ECONOMICS_RELEASE_KIND = "actual_contract_economics"
ECONOMICS_SCHEMA_VERSION = "1.1.0"
SESSION_RELEASE_KIND = "versioned_session_policy"
SESSION_SCHEMA_VERSION = "1.0.0"
FEATURE_RELEASE_KIND = "feature_release"
FEATURE_SCHEMA_VERSION = "1.0.0"
OUTCOME_RELEASE_KIND = "outcome_release"
OUTCOME_SCHEMA_VERSION = "1.1.0"
CAUSAL_OUTCOME_LABEL_METHOD_ID = "ACTUAL_CONTRACT_EVENT_OPEN_TO_EVENT_OPEN_1M_V1"

_ONE_MINUTE_NS = 60_000_000_000
_PRICE_NANO = Decimal(1_000_000_000)

_ENVIRONMENT_CONFIG = "environment.lock.json"
_SESSION_CONFIG = "session_policy.json"

_ASSET_CLASSES: Mapping[str, str] = MappingProxyType(
    {
        **{
            market: "FX"
            for market in ("6A", "6B", "6C", "6E", "6J", "6M", "6N", "6S")
        },
        **{market: "CRYPTO" for market in ("BTC", "ETH")},
        **{market: "ENERGY" for market in ("CL", "HO", "NG", "RB")},
        **{market: "EQUITY_INDEX" for market in ("ES", "NQ", "RTY", "YM")},
        **{market: "METALS" for market in ("GC", "HG", "PA", "PL", "SI")},
        **{
            market: "AGRICULTURE"
            for market in ("GF", "HE", "KE", "LE", "ZC", "ZL", "ZM", "ZS", "ZW")
        },
        **{
            market: "RATES"
            for market in (
                "SR1",
                "SR3",
                "TN",
                "UB",
                "ZB",
                "ZF",
                "ZN",
                "ZQ",
                "ZT",
            )
        },
    }
)

_FEATURE_FORMULAS: Mapping[str, str] = MappingProxyType(
    {
        "bar_body_fraction": "(close_nano-open_nano)/open_nano",
        "bar_return": "close_nano/open_nano-1",
        "close_price": "close_nano/1e9",
        "intrabar_range_fraction": "(high_nano-low_nano)/open_nano",
        "volume": "exact_nonnegative_volume",
    }
)

_ACTUAL_CAUSAL_FIELDS = frozenset(
    {
        "actual_identity_hash",
        "currency",
        "definition_manifest_sha256",
        "definition_release_id",
        "definition_row_sha256",
        "definition_ts_event_ns",
        "definition_ts_recv_ns",
        "definition_index_date_utc",
        "definition_activation_ns",
        "definition_expiration_ns",
        "definition_security_update_action",
        "definition_instrument_class",
        "definition_security_type",
        "definition_source_row_ordinal",
        "economics_rulebook_hash",
        "exchange",
        "exchange_session_date",
        "point_value",
        "provider_unit_qty_state",
        "quote_convention",
        "raw_symbol",
        "tick_size",
        "tick_value",
    }
)
_TRUST_CACHE_FIELDS = tuple(
    sorted(
        _ACTUAL_CAUSAL_FIELDS
        | {
            "dataset",
            "instrument_id",
            "market",
            "publisher_id",
            "source_manifest_sha256",
            "source_release_id",
        }
    )
)
_CAUSAL_CENSUS_FIELDS = frozenset(
    _ACTUAL_CAUSAL_FIELDS
    | {
        "disposition",
        "failure_code",
        "failure_detail_sha256",
        "prediction_in_coverage_denominator",
    }
)
_ECONOMICS_CAUSAL_FIELDS = tuple(
    sorted(
        set(_TRUST_CACHE_FIELDS)
        | {
            "actual_identity_hash",
            "availability_basis",
            "availability_policy_hash",
            "available_at_ns",
            "event_at_ns",
            "foundation_policy_set_id",
            "instrument_id_date_utc",
            "provider_timestamp_epoch_id",
            "resolution_as_of_ns",
            "source_raw_release_id",
        }
    )
)
_RESOLVED_CAUSAL_DISPOSITIONS = frozenset({"ELIGIBLE", "ANOMALY_QUARANTINED"})
_UNRESOLVED_CAUSAL_DISPOSITION = "UNRESOLVED_FAIL_CLOSED"


class _ColumnarBatchRow(Mapping[str, object]):
    """A read-only row view that does not materialize one dict per Parquet row."""

    __slots__ = ("_columns", "_index")

    def __init__(
        self, columns: Mapping[str, Sequence[object]], index: int
    ) -> None:
        self._columns = columns
        self._index = index

    def __getitem__(self, name: str) -> object:
        return self._columns[name][self._index]

    def __iter__(self) -> Iterator[str]:
        return iter(self._columns)

    def __len__(self) -> int:
        return len(self._columns)


def _module_hash() -> str:
    return sha256_file(Path(__file__).resolve())


def _config_path(boundary: RepoBoundary, name: str) -> Path:
    path = boundary.active_root / "configs" / name
    return boundary.assert_active_path(
        path, purpose=f"producer bridge config {name}", subtree="configs"
    )


def _environment_hash(boundary: RepoBoundary) -> str:
    return sha256_file(_config_path(boundary, _ENVIRONMENT_CONFIG))


def _assert_publisher(boundary: RepoBoundary, publisher: AtomicPublisher) -> None:
    if publisher.boundary.repository_id != boundary.repository_id:
        raise IntegrityError("producer publisher belongs to another repository")


def _write_canonical(path: Path, payload: object) -> None:
    path.write_bytes(canonical_bytes(payload) + b"\n")
    with path.open("r+b") as handle:
        os.fsync(handle.fileno())


def _read_canonical(path: Path) -> object:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"bridge payload is invalid: {path.name}") from exc
    if raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"bridge payload is not canonical JSON: {path.name}")
    return payload


def _source_receipt_hash(receipts: Sequence[VerifiedReleaseReceipt]) -> str:
    return sha256_json(
        [receipt.as_dict() for receipt in sorted(receipts, key=lambda item: item.release_id)]
    )


def _base_metadata(
    boundary: RepoBoundary, receipts: Sequence[VerifiedReleaseReceipt]
) -> dict[str, object]:
    return {
        "bridge_code_sha256": _module_hash(),
        "environment_lock_sha256": _environment_hash(boundary),
        "source_receipts_sha256": _source_receipt_hash(receipts),
    }


def _definition_record(
    provider: ProviderDefinition, policies: VerifiedFoundationPolicies
) -> dict[str, object]:
    economics = policies.economics.resolve(provider.market, provider)
    available = provider.ts_recv
    activation = provider.activation
    expiration = provider.expiration
    return {
        "available_at": available.isoformat(),
        "currency": economics.currency,
        "dataset": provider.dataset,
        "definition_index_date_utc": provider.instrument_id_date_utc,
        "effective_at": None if activation is None else activation.isoformat(),
        "exchange": provider.exchange,
        "expires_at": None if expiration is None else expiration.isoformat(),
        "instrument_id": provider.instrument_id,
        "instrument_class": provider.instrument_class,
        "market": provider.market,
        "min_tick": str(economics.tick_size),
        "multiplier": str(economics.point_value),
        "provider_definition_manifest_sha256": provider.source_manifest_sha256,
        "provider_definition_activation_ns": provider.activation_ns,
        "provider_definition_expiration_ns": provider.expiration_ns,
        "provider_definition_release_id": provider.source_release_id,
        "provider_definition_row_sha256": provider.row_sha256,
        "provider_definition_ts_event_ns": provider.ts_event_ns,
        "provider_definition_ts_recv_ns": provider.ts_recv_ns,
        "provider_event_at": provider.ts_event.isoformat(),
        "provider_min_price_increment_nano": provider.min_price_increment_nano,
        "provider_source_file_path": provider.source_file_path,
        "provider_source_file_sha256": provider.source_file_sha256,
        "provider_unit_of_measure": provider.unit_of_measure,
        "provider_unit_of_measure_qty_nano": provider.unit_of_measure_qty_nano,
        "publisher_id": provider.publisher_id,
        "raw_symbol": provider.raw_symbol,
        "security_type": provider.security_type,
        "security_update_action": provider.security_update_action,
        "source_received_at": provider.ts_recv.isoformat(),
        "source_row_ordinal": provider.row_ordinal,
    }


def _definition_ineligibility_record(
    provider: ProviderDefinition,
    policies: VerifiedFoundationPolicies,
    error: ContractError,
) -> dict[str, object]:
    message = str(error)
    if message == "provider unit quantity is unavailable; economics fail closed":
        reason_code = "PROVIDER_UNIT_QTY_UNAVAILABLE"
    elif message == "provider unit quantity contradicts the pinned market rule":
        reason_code = "PROVIDER_UNIT_QTY_CONTRADICTION"
    else:
        reason_code = "ECONOMICS_CONTRACT_UNRESOLVED"
    core: dict[str, object] = {
        "dataset": provider.dataset,
        "definition_index_date_utc": provider.instrument_id_date_utc,
        "disposition": "ABSTAIN_ECONOMICS_UNRESOLVED",
        "economics_rulebook_hash": policies.economics.rulebook_hash,
        "failure_detail_sha256": sha256_json(
            {"error_type": type(error).__name__, "message": message}
        ),
        "instrument_class": provider.instrument_class,
        "instrument_id": provider.instrument_id,
        "market": provider.market,
        "prediction_in_coverage_denominator": True,
        "provider_definition_manifest_sha256": provider.source_manifest_sha256,
        "provider_definition_release_id": provider.source_release_id,
        "provider_definition_row_sha256": provider.row_sha256,
        "provider_definition_ts_event_ns": provider.ts_event_ns,
        "provider_definition_ts_recv_ns": provider.ts_recv_ns,
        "provider_min_price_increment_nano": provider.min_price_increment_nano,
        "provider_source_file_path": provider.source_file_path,
        "provider_source_file_sha256": provider.source_file_sha256,
        "provider_unit_of_measure": provider.unit_of_measure,
        "provider_unit_of_measure_qty_nano": provider.unit_of_measure_qty_nano,
        "publisher_id": provider.publisher_id,
        "raw_symbol": provider.raw_symbol,
        "reason_code": reason_code,
        "research_eligible": False,
        "security_type": provider.security_type,
        "security_update_action": provider.security_update_action,
        "source_row_ordinal": provider.row_ordinal,
    }
    return {
        **core,
        "economics_ineligibility_id": sha256_json(core),
    }


def _definition_projection(
    raw_receipt: VerifiedReleaseReceipt,
    policies: VerifiedFoundationPolicies,
    boundary: RepoBoundary,
) -> tuple[tuple[dict[str, object], ...], tuple[dict[str, object], ...]]:
    loaded = load_raw_interval(raw_receipt, boundary=boundary)
    policies.verify()
    providers = read_definitions(loaded.definitions_path)
    eligible: list[dict[str, object]] = []
    ineligible: list[dict[str, object]] = []
    for provider in providers:
        try:
            eligible.append(_definition_record(provider, policies))
        except ContractError as exc:
            ineligible.append(
                _definition_ineligibility_record(provider, policies, exc)
            )
    ordering = lambda item: (
        str(item["dataset"]),
        str(item["market"]),
        int(item["publisher_id"]),
        int(item["instrument_id"]),
        str(item["definition_index_date_utc"]),
        int(item["provider_definition_ts_recv_ns"]),
        str(item["provider_source_file_path"]),
        int(item["source_row_ordinal"]),
        str(item["provider_definition_row_sha256"]),
    )
    records = tuple(sorted(eligible, key=ordering))
    exclusions = tuple(sorted(ineligible, key=ordering))
    source_rows = {
        str(item["provider_definition_row_sha256"])
        for item in (*records, *exclusions)
    }
    if (
        not providers
        or len(source_rows) != len(providers)
        or len(records) + len(exclusions) != len(providers)
    ):
        raise IntegrityError(
            "definition bridge eligibility partition is empty, duplicate, or incomplete"
        )
    return records, exclusions


def _definition_records(
    raw_receipt: VerifiedReleaseReceipt,
    policies: VerifiedFoundationPolicies,
    boundary: RepoBoundary,
) -> tuple[dict[str, object], ...]:
    records, _ = _definition_projection(raw_receipt, policies, boundary)
    return records


def _definition_ineligibility_document(
    *,
    records: Sequence[Mapping[str, object]],
    ineligible: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    core: dict[str, object] = {
        "eligible_definition_row_count": len(records),
        "ineligible_definition_row_count": len(ineligible),
        "records": list(ineligible),
        "schema_version": DEFINITION_INELIGIBILITY_SCHEMA_VERSION,
        "source_definition_row_count": len(records) + len(ineligible),
    }
    return {
        **core,
        "definition_ineligibility_ledger_id": sha256_json(core),
    }


@dataclass(frozen=True)
class BridgeDefinitionRecord:
    provider: ProviderDefinition
    registry_row_id: str


@dataclass(frozen=True)
class LoadedActualContractDefinitions:
    receipt: VerifiedReleaseReceipt
    raw_receipt: VerifiedReleaseReceipt
    policy_receipt: VerifiedReleaseReceipt
    registry: VerifiedIdentityRegistry
    by_provider_row: Mapping[str, BridgeDefinitionRecord]
    ineligible_by_provider_row: Mapping[str, Mapping[str, object]]

    def provider_record(self, provider_row_sha256: str) -> BridgeDefinitionRecord:
        try:
            return self.by_provider_row[provider_row_sha256]
        except KeyError as exc:
            raise ContractError("causal row has no verified bridged definition") from exc


def publish_actual_contract_definitions(
    *,
    raw_receipt: VerifiedReleaseReceipt,
    policies: VerifiedFoundationPolicies,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    """Publish eligible identities plus every fail-closed provider-row abstention."""

    _assert_publisher(boundary, publisher)
    if policies.boundary.repository_id != boundary.repository_id:
        raise IntegrityError("foundation policies belong to another repository")
    records, ineligible = _definition_projection(raw_receipt, policies, boundary)
    ineligibility = _definition_ineligibility_document(
        records=records,
        ineligible=ineligible,
    )
    sources = (raw_receipt, policies.receipt)
    metadata = {
        **_base_metadata(boundary, sources),
        "definition_row_count": len(records),
        "eligible_definition_row_count": len(records),
        "economics_rulebook_hash": policies.economics.rulebook_hash,
        "foundation_policy_receipt_id": policies.receipt.receipt_id,
        "foundation_policy_set_id": policies.policy_set_id,
        "ineligible_definition_row_count": len(ineligible),
        "ineligibility_ledger_id": ineligibility[
            "definition_ineligibility_ledger_id"
        ],
        "source_definition_row_count": len(records) + len(ineligible),
        "source_raw_release_receipt_id": raw_receipt.receipt_id,
    }
    stage = publisher.create_stage("actual_contract_definitions")
    _write_canonical(
        stage / "identities.json",
        {"records": list(records), "schema_version": DEFINITION_SCHEMA_VERSION},
    )
    manifest = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=DEFINITION_RELEASE_KIND,
        schema_version=DEFINITION_SCHEMA_VERSION,
        logical_paths={
            "identities.json": "data/reference/definitions/identities.json"
        },
        source_release_ids=tuple(receipt.release_id for receipt in sources),
        embedded_documents={
            DEFINITION_INELIGIBILITY_DOCUMENT: ineligibility,
        },
        metadata=metadata,
    )
    manifest_path = publisher.publish(stage, manifest)
    receipt = VerifiedReleaseReceipt.from_manifest(manifest_path, boundary)
    load_actual_contract_definitions(
        receipt,
        raw_receipt=raw_receipt,
        policies=policies,
        boundary=boundary,
    )
    return receipt


def load_actual_contract_definitions(
    receipt: VerifiedReleaseReceipt,
    *,
    raw_receipt: VerifiedReleaseReceipt,
    policies: VerifiedFoundationPolicies,
    boundary: RepoBoundary,
) -> LoadedActualContractDefinitions:
    manifest = receipt.verify(boundary)
    records, ineligible = _definition_projection(raw_receipt, policies, boundary)
    ineligibility = _definition_ineligibility_document(
        records=records,
        ineligible=ineligible,
    )
    sources = (raw_receipt, policies.receipt)
    expected_metadata = {
        **_base_metadata(boundary, sources),
        "definition_row_count": len(records),
        "eligible_definition_row_count": len(records),
        "economics_rulebook_hash": policies.economics.rulebook_hash,
        "foundation_policy_receipt_id": policies.receipt.receipt_id,
        "foundation_policy_set_id": policies.policy_set_id,
        "ineligible_definition_row_count": len(ineligible),
        "ineligibility_ledger_id": ineligibility[
            "definition_ineligibility_ledger_id"
        ],
        "source_definition_row_count": len(records) + len(ineligible),
        "source_raw_release_receipt_id": raw_receipt.receipt_id,
    }
    if (
        receipt.phase != "reference"
        or manifest.release_kind != DEFINITION_RELEASE_KIND
        or manifest.schema_version != DEFINITION_SCHEMA_VERSION
        or {Path(entry.path).name for entry in manifest.files} != {"identities.json"}
        or manifest.source_release_ids
        != tuple(sorted(receipt.release_id for receipt in sources))
        or dict(manifest.embedded_documents)
        != {DEFINITION_INELIGIBILITY_DOCUMENT: ineligibility}
        or dict(manifest.metadata) != expected_metadata
    ):
        raise IntegrityError("definition bridge manifest or provenance is invalid")
    payload = _read_canonical(
        receipt.resolve_unique_filename("identities.json", boundary)
    )
    if payload != {"records": list(records), "schema_version": DEFINITION_SCHEMA_VERSION}:
        raise IntegrityError("definition bridge changed, dropped, or reordered source rows")
    registry = VerifiedIdentityRegistry.from_release(receipt, boundary)
    by_provider: dict[str, BridgeDefinitionRecord] = {}
    for raw in records:
        row_id = sha256_json(raw)
        provider = ProviderDefinition(
            dataset=str(raw["dataset"]),
            market=str(raw["market"]),
            publisher_id=int(raw["publisher_id"]),
            instrument_id=int(raw["instrument_id"]),
            instrument_id_date_utc=str(raw["definition_index_date_utc"]),
            ts_event_ns=int(raw["provider_definition_ts_event_ns"]),
            ts_recv_ns=int(raw["provider_definition_ts_recv_ns"]),
            activation_ns=int(raw["provider_definition_activation_ns"]),
            expiration_ns=int(raw["provider_definition_expiration_ns"]),
            security_update_action=str(raw["security_update_action"]),
            instrument_class=str(raw["instrument_class"]),
            security_type=str(raw["security_type"]),
            raw_symbol=str(raw["raw_symbol"]),
            exchange=str(raw["exchange"]),
            currency=str(raw["currency"]),
            min_price_increment_nano=int(raw["provider_min_price_increment_nano"]),
            unit_of_measure_qty_nano=int(raw["provider_unit_of_measure_qty_nano"]),
            unit_of_measure=str(raw["provider_unit_of_measure"]),
            source_release_id=str(raw["provider_definition_release_id"]),
            source_manifest_sha256=str(raw["provider_definition_manifest_sha256"]),
            source_file_path=str(raw["provider_source_file_path"]),
            source_file_sha256=str(raw["provider_source_file_sha256"]),
            row_ordinal=int(raw["source_row_ordinal"]),
            row_sha256=str(raw["provider_definition_row_sha256"]),
        )
        if row_id not in registry.definitions or provider.row_sha256 in by_provider:
            raise IntegrityError("definition bridge registry/provider mapping is ambiguous")
        by_provider[provider.row_sha256] = BridgeDefinitionRecord(provider, row_id)
    ineligible_by_provider: dict[str, Mapping[str, object]] = {}
    for raw in ineligible:
        provider_hash = str(raw["provider_definition_row_sha256"])
        if provider_hash in by_provider or provider_hash in ineligible_by_provider:
            raise IntegrityError("definition eligibility partition is ambiguous")
        ineligible_by_provider[provider_hash] = MappingProxyType(dict(raw))
    if (
        len(by_provider) != len(records)
        or len(ineligible_by_provider) != len(ineligible)
        or len(by_provider) + len(ineligible_by_provider)
        != expected_metadata["source_definition_row_count"]
    ):
        raise IntegrityError("definition eligibility partition census is invalid")
    return LoadedActualContractDefinitions(
        receipt,
        raw_receipt,
        policies.receipt,
        registry,
        MappingProxyType(by_provider),
        MappingProxyType(ineligible_by_provider),
    )


def publish_versioned_session_policy(
    *,
    policies: VerifiedFoundationPolicies,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    """Promote the exact active/policy-set session bytes to the trust role."""

    _assert_publisher(boundary, publisher)
    policies.verify()
    active = _config_path(boundary, _SESSION_CONFIG)
    active_payload = _read_canonical(active)
    if policies.receipt.embedded_document(_SESSION_CONFIG, boundary) != active_payload:
        raise IntegrityError("active session policy differs from the verified foundation policy")
    sources = (policies.receipt,)
    metadata = {
        **_base_metadata(boundary, sources),
        "foundation_policy_receipt_id": policies.receipt.receipt_id,
        "foundation_policy_set_id": policies.policy_set_id,
        "session_policy_sha256": sha256_file(active),
    }
    stage = publisher.create_stage("versioned_session_policy")
    manifest = ReleaseManifest.build(
        stage,
        phase="controls",
        release_kind=SESSION_RELEASE_KIND,
        schema_version=SESSION_SCHEMA_VERSION,
        logical_paths={},
        source_release_ids=(policies.receipt.release_id,),
        embedded_documents={"session_policy.json": active_payload},
        metadata=metadata,
    )
    manifest_path = publisher.publish(stage, manifest)
    receipt = VerifiedReleaseReceipt.from_manifest(manifest_path, boundary)
    load_versioned_session_policy(
        receipt, policies=policies, boundary=boundary
    )
    return receipt


def load_versioned_session_policy(
    receipt: VerifiedReleaseReceipt,
    *,
    policies: VerifiedFoundationPolicies,
    boundary: RepoBoundary,
) -> VerifiedSessionPolicy:
    manifest = receipt.verify(boundary)
    active = _config_path(boundary, _SESSION_CONFIG)
    active_payload = _read_canonical(active)
    sources = (policies.receipt,)
    expected_metadata = {
        **_base_metadata(boundary, sources),
        "foundation_policy_receipt_id": policies.receipt.receipt_id,
        "foundation_policy_set_id": policies.policy_set_id,
        "session_policy_sha256": sha256_file(active),
    }
    if (
        policies.receipt.embedded_document(_SESSION_CONFIG, boundary) != active_payload
        or manifest.release_kind != SESSION_RELEASE_KIND
        or manifest.schema_version != SESSION_SCHEMA_VERSION
        or manifest.files
        or set(manifest.embedded_documents) != {"session_policy.json"}
        or manifest.source_release_ids != (policies.receipt.release_id,)
        or dict(manifest.metadata) != expected_metadata
        or manifest.embedded_documents["session_policy.json"] != active_payload
    ):
        raise IntegrityError("session-policy bridge provenance is invalid")
    return VerifiedSessionPolicy.from_release(receipt, boundary)


def _iter_causal_rows(
    receipt: VerifiedReleaseReceipt,
    boundary: RepoBoundary,
    *,
    columns: Sequence[str] | None = None,
) -> Iterator[Mapping[str, object]]:
    bars, report = load_causal_interval(receipt, boundary=boundary)
    try:
        parquet = pq.ParquetFile(bars)
    except Exception as exc:
        raise IntegrityError("causal bridge input is invalid Parquet") from exc
    if not parquet.schema_arrow.equals(CAUSAL_BAR_SCHEMA, check_metadata=True):
        raise IntegrityError("causal bridge input schema differs from Phase 2")
    schema_names = tuple(CAUSAL_BAR_SCHEMA.names)
    if columns is None:
        selected_names = schema_names
    else:
        requested = set(columns) | _CAUSAL_CENSUS_FIELDS
        unknown = requested.difference(schema_names)
        if unknown:
            raise IntegrityError(
                "causal bridge requested unknown columns: "
                + ",".join(sorted(unknown))
            )
        selected_names = tuple(name for name in schema_names if name in requested)
    count = denominator_count = 0
    dispositions: dict[str, int] = {}
    for batch in parquet.iter_batches(
        batch_size=100_000, columns=list(selected_names)
    ):
        column_values: Mapping[str, Sequence[object]] = MappingProxyType(
            {
                name: batch.column(index).to_pylist()
                for index, name in enumerate(selected_names)
            }
        )
        if any(len(values) != batch.num_rows for values in column_values.values()):
            raise IntegrityError("causal bridge column length differs from its batch")
        for index in range(batch.num_rows):
            row = _ColumnarBatchRow(column_values, index)
            disposition = row.get("disposition")
            failure_code = row.get("failure_code")
            failure_hash = row.get("failure_detail_sha256")
            if disposition in _RESOLVED_CAUSAL_DISPOSITIONS:
                if (
                    any(row.get(name) is None for name in _ACTUAL_CAUSAL_FIELDS)
                    or failure_code is not None
                    or failure_hash is not None
                ):
                    raise IntegrityError("resolved causal row has incomplete identity/economics")
            elif disposition == _UNRESOLVED_CAUSAL_DISPOSITION:
                if (
                    any(row.get(name) is not None for name in _ACTUAL_CAUSAL_FIELDS)
                    or type(failure_code) is not str
                    or not failure_code
                    or type(failure_hash) is not str
                    or re.fullmatch(r"[0-9a-f]{64}", failure_hash) is None
                ):
                    raise IntegrityError("unresolved causal row is not fail-closed")
            else:
                raise IntegrityError("causal row uses an undeclared disposition")
            if row.get("prediction_in_coverage_denominator") is not True:
                raise IntegrityError("causal row was removed from the prediction denominator")
            count += 1
            denominator_count += 1
            dispositions[disposition] = dispositions.get(disposition, 0) + 1
            yield row
    if (
        count != report.get("row_count")
        or denominator_count
        != report.get("prediction_in_coverage_denominator_rows")
        or dict(sorted(dispositions.items())) != report.get("disposition_counts")
        or report.get("learned_or_outcome_informed_transform_count") != 0
    ):
        raise IntegrityError("causal Parquet census differs from its interval receipt")


def causal_feature_ready_join_keys(
    receipt: VerifiedReleaseReceipt, *, boundary: RepoBoundary
) -> frozenset[tuple[str, int, int]]:
    """Return exact keys that can support deterministic bar-local features."""

    keys: set[tuple[str, int, int]] = set()
    for row in _iter_causal_rows(
        receipt,
        boundary,
        columns=(
            "actual_identity_hash",
            "available_at_ns",
            "disposition",
            "event_at_ns",
        ),
    ):
        if row.get("disposition") != "ELIGIBLE":
            continue
        identity_hash = row.get("actual_identity_hash")
        event_ns = row.get("event_at_ns")
        available_ns = row.get("available_at_ns")
        if (
            type(identity_hash) is not str
            or re.fullmatch(r"[0-9a-f]{64}", identity_hash) is None
            or type(event_ns) is not int
            or type(available_ns) is not int
        ):
            raise IntegrityError("feature-ready causal key is invalid")
        key = (identity_hash, event_ns, available_ns)
        if key in keys:
            raise IntegrityError("feature-ready causal key is duplicated")
        keys.add(key)
    return frozenset(keys)


def _verify_bridge_context(
    *,
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    boundary: RepoBoundary,
) -> None:
    if policies.boundary.repository_id != boundary.repository_id:
        raise IntegrityError("foundation policies belong to another repository")
    policies.verify()
    rebuilt_definitions = load_actual_contract_definitions(
        definitions.receipt,
        raw_receipt=definitions.raw_receipt,
        policies=policies,
        boundary=boundary,
    )
    if (
        definitions.policy_receipt != policies.receipt
        or definitions.registry.registry_hash
        != rebuilt_definitions.registry.registry_hash
        or dict(definitions.by_provider_row)
        != dict(rebuilt_definitions.by_provider_row)
        or dict(definitions.ineligible_by_provider_row)
        != dict(rebuilt_definitions.ineligible_by_provider_row)
    ):
        raise IntegrityError("definition bridge is not bound to the supplied policy set")
    rebuilt_session = load_versioned_session_policy(
        session_policy.receipt, policies=policies, boundary=boundary
    )
    if rebuilt_session.policy_hash != session_policy.policy_hash:
        raise IntegrityError("session bridge is not bound to the supplied policy set")
    causal_manifest = causal_receipt.verify(boundary)
    _, causal_report = load_causal_interval(causal_receipt, boundary=boundary)
    raw_manifest = definitions.raw_receipt.verify(boundary)
    expected_sources = tuple(
        sorted(
            (
                definitions.raw_receipt.release_id,
                policies.receipt.release_id,
            )
        )
    )
    if (
        causal_manifest.source_release_ids != expected_sources
        or causal_report.get("source_raw_release_id")
        != definitions.raw_receipt.release_id
        or causal_report.get("foundation_policy_release_id")
        != policies.receipt.release_id
        or causal_report.get("foundation_policy_set_id") != policies.policy_set_id
        or causal_manifest.metadata.get("market") != raw_manifest.metadata.get("market")
        or causal_manifest.metadata.get("year") != raw_manifest.metadata.get("year")
    ):
        raise IntegrityError("causal release dependency closure is invalid")


def _verify_causal_policy_row(
    row: Mapping[str, object],
    *,
    definitions: LoadedActualContractDefinitions,
    policies: VerifiedFoundationPolicies,
) -> tuple[int, int]:
    event_ns = _exact_int(row.get("event_at_ns"), "causal.event_at_ns")
    available_ns = _exact_int(row.get("available_at_ns"), "causal.available_at_ns")
    event = ns_to_datetime(event_ns, "causal.event_at_ns")
    if (
        row.get("source_raw_release_id") != definitions.raw_receipt.release_id
        or row.get("foundation_policy_set_id") != policies.policy_set_id
        or row.get("availability_basis") != policies.foundation.availability_basis
        or row.get("availability_policy_hash") != policies.foundation.policy_hash
        or row.get("resolution_as_of_ns") != available_ns
        or policies.foundation.bar_available_at_ns(event_ns) != available_ns
        or row.get("provider_timestamp_epoch_id")
        != policies.foundation.provider_timestamp_epoch_id(event_ns)
        or row.get("instrument_id_date_utc") != event.date().isoformat()
    ):
        raise IntegrityError("causal row differs from its raw/policy dependency chain")
    return event_ns, available_ns


def _trust_actual_from_causal(
    row: Mapping[str, object],
    definitions: LoadedActualContractDefinitions,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    *,
    verified_times: tuple[int, int] | None = None,
) -> tuple[ActualContractIdentity, BridgeDefinitionRecord, ResolvedEconomics]:
    provider_hash = row.get("definition_row_sha256")
    if type(provider_hash) is not str:
        raise IntegrityError("resolved causal row lacks provider definition provenance")
    bridged = definitions.provider_record(provider_hash)
    provider = bridged.provider
    observation = definitions.registry.definitions[bridged.registry_row_id]
    economics = policies.economics.resolve(provider.market, provider)
    if verified_times is None:
        event_ns, available_ns = _verify_causal_policy_row(
            row, definitions=definitions, policies=policies
        )
    else:
        event_ns, available_ns = verified_times
    event = ns_to_datetime(event_ns, "event")
    instrument_date = _exact_date(row.get("instrument_id_date_utc"), "instrument date")
    definition_index_date = _exact_date(
        row.get("definition_index_date_utc"), "definition index date"
    )
    activation = provider.activation
    expiration = provider.expiration
    if (
        provider.dataset != row.get("dataset")
        or provider.market != row.get("market")
        or provider.publisher_id != row.get("publisher_id")
        or provider.instrument_id != row.get("instrument_id")
        or provider.raw_symbol != row.get("raw_symbol")
        or provider.exchange != row.get("exchange")
        or provider.currency != row.get("currency")
        or provider.source_release_id != row.get("source_release_id")
        or provider.source_manifest_sha256 != row.get("source_manifest_sha256")
        or provider.source_release_id != row.get("definition_release_id")
        or provider.source_manifest_sha256 != row.get("definition_manifest_sha256")
        or provider.row_sha256 != row.get("definition_row_sha256")
        or provider.ts_event_ns != row.get("definition_ts_event_ns")
        or provider.ts_recv_ns != row.get("definition_ts_recv_ns")
        or provider.instrument_id_date_utc != definition_index_date.isoformat()
        or definition_index_date != instrument_date
        or provider.activation_ns != row.get("definition_activation_ns")
        or provider.expiration_ns != row.get("definition_expiration_ns")
        or provider.security_update_action
        != row.get("definition_security_update_action")
        or provider.instrument_class != row.get("definition_instrument_class")
        or provider.security_type != row.get("definition_security_type")
        or provider.row_ordinal != row.get("definition_source_row_ordinal")
        or provider.ts_recv_ns > event_ns
        or provider.ts_recv_ns > available_ns
        or provider.security_update_action not in {"ADD", "MODIFY"}
        or provider.instrument_class != "FUTURE"
        or provider.security_type != "FUT"
        or activation is None
        or expiration is None
        or not provider.activation_ns <= event_ns < provider.expiration_ns
        or str(economics.tick_size) != row.get("tick_size")
        or str(economics.point_value) != row.get("point_value")
        or str(economics.tick_value) != row.get("tick_value")
        or economics.quote_convention != row.get("quote_convention")
        or economics.rulebook_hash != row.get("economics_rulebook_hash")
        or economics.provider_unit_qty_state != row.get("provider_unit_qty_state")
        or observation.effective_at != activation
        or observation.expires_at != expiration
        or observation.provider_event_at != provider.ts_event
        or observation.source_received_at != provider.ts_recv
        or observation.available_at != provider.ts_recv
        or observation.definition_index_date_utc != definition_index_date
        or observation.security_update_action != provider.security_update_action
        or observation.instrument_class != provider.instrument_class
        or observation.security_type != provider.security_type
        or observation.source_file_path != provider.source_file_path
        or observation.source_row_ordinal != provider.row_ordinal
    ):
        raise IntegrityError("causal row contradicts its verified definition/economics chain")
    policies.foundation.assert_definition_lifecycle_trusted(event_ns)
    session_date = _exact_date(row.get("exchange_session_date"), "session date")
    if (
        instrument_date != event.date()
        or session_policy.exchange_session_date(provider.exchange, event) != session_date
    ):
        raise IntegrityError("causal session date differs from the verified session policy")
    actual = ActualContractIdentity.from_definition(
        observation.definition,
        instrument_id_date_utc=instrument_date,
        exchange_session_date=session_date,
    )
    foundation_definition = ContractDefinition(
        dataset=provider.dataset,
        publisher_id=provider.publisher_id,
        instrument_id=provider.instrument_id,
        raw_symbol=provider.raw_symbol,
        exchange=provider.exchange,
        definition_release_id=provider.source_release_id,
        definition_manifest_sha256=provider.source_manifest_sha256,
        definition_row_id=provider.row_sha256,
        currency=economics.currency,
        multiplier=economics.point_value,
        min_tick=economics.tick_size,
    )
    foundation_actual = ActualContractIdentity.from_definition(
        foundation_definition,
        instrument_id_date_utc=instrument_date,
        exchange_session_date=session_date,
    )
    if foundation_actual.identity_hash != row.get("actual_identity_hash"):
        raise IntegrityError("foundation actual-identity hash is not reproducible")
    return actual, bridged, economics


def _trust_actual_from_causal_cached(
    row: Mapping[str, object],
    definitions: LoadedActualContractDefinitions,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    cache: dict[
        tuple[object, ...],
        tuple[ActualContractIdentity, BridgeDefinitionRecord, ResolvedEconomics],
    ],
    *,
    verified_times: tuple[int, int] | None = None,
    verified_session_policy_hash: str | None = None,
) -> tuple[ActualContractIdentity, BridgeDefinitionRecord, ResolvedEconomics]:
    """Reuse only a fully validated static identity while rechecking row time."""

    times = verified_times or _verify_causal_policy_row(
        row, definitions=definitions, policies=policies
    )
    key = tuple(row.get(name) for name in _TRUST_CACHE_FIELDS)
    cached = cache.get(key)
    if cached is None:
        cached = _trust_actual_from_causal(
            row,
            definitions,
            policies,
            session_policy,
            verified_times=times,
        )
        cache[key] = cached
        return cached

    actual, bridged, economics = cached
    provider = bridged.provider
    event_ns, available_ns = times
    event = ns_to_datetime(event_ns, "event")
    instrument_date = _exact_date(
        row.get("instrument_id_date_utc"), "instrument date"
    )
    session_date = _exact_date(row.get("exchange_session_date"), "session date")
    activation = provider.activation
    expiration = provider.expiration
    if (
        activation is None
        or expiration is None
        or provider.ts_recv_ns > event_ns
        or provider.ts_recv_ns > available_ns
        or not provider.activation_ns <= event_ns < provider.expiration_ns
            or instrument_date != event.date()
            or (
                session_policy.exchange_session_date(provider.exchange, event)
                if verified_session_policy_hash is None
                else session_policy._exchange_session_date_preverified(
                    provider.exchange,
                    event,
                    expected_policy_hash=verified_session_policy_hash,
                )
            )
            != session_date
            or economics.rulebook_hash != row.get("economics_rulebook_hash")
        ):
        raise IntegrityError("cached causal identity differs at this event time")
    policies.foundation.assert_definition_lifecycle_trusted(event_ns)
    return cached


def _exact_int(value: object, name: str) -> int:
    if type(value) is not int:
        raise IntegrityError(f"{name} must be an exact integer")
    return value


def _exact_date(value: object, name: str) -> date:
    if type(value) is not str:
        raise IntegrityError(f"{name} must be an exact ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise IntegrityError(f"{name} is invalid") from exc
    if parsed.isoformat() != value:
        raise IntegrityError(f"{name} is not canonical")
    return parsed


def _economics_records(
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    boundary: RepoBoundary,
) -> tuple[dict[str, object], ...]:
    _verify_bridge_context(
        causal_receipt=causal_receipt,
        definitions=definitions,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
    )
    records: dict[str, dict[str, object]] = {}
    session_policy.verify()
    identity_signatures: dict[str, tuple[object, ...]] = {}
    for row in _iter_causal_rows(
        causal_receipt, boundary, columns=_ECONOMICS_CAUSAL_FIELDS
    ):
        identity_hash = row.get("actual_identity_hash")
        if identity_hash is None:
            continue
        if type(identity_hash) is not str:
            raise IntegrityError("resolved causal identity hash is invalid")
        signature = tuple(row.get(name) for name in _TRUST_CACHE_FIELDS)
        prior_signature = identity_signatures.get(identity_hash)
        if prior_signature is not None:
            if prior_signature != signature:
                raise IntegrityError(
                    "one actual identity has conflicting causal provenance"
                )
            continue
        actual, bridged, economics = _trust_actual_from_causal(
            row,
            definitions,
            policies,
            session_policy,
        )
        identity_signatures[identity_hash] = signature
        observation = definitions.registry.definitions[bridged.registry_row_id]
        try:
            asset_class = _ASSET_CLASSES[economics.market]
        except KeyError as exc:
            raise IntegrityError("economics asset class is not pinned") from exc
        record = {
            "actual_identity_hash": actual.identity_hash,
            "ambiguity_reasons": [],
            "asset_class": asset_class,
            "available_at": observation.available_at.isoformat(),
            "currency": economics.currency,
            "effective_at": observation.effective_at.isoformat(),
            "point_value": str(economics.point_value),
            "quote_convention_id": economics.quote_convention,
            "source_fields_used": [
                "min_price_increment",
                "pinned_point_value_rule",
                "provider_unit_of_measure_qty",
            ],
            "source_received_at": observation.source_received_at.isoformat(),
            "tick_size": str(economics.tick_size),
            "tick_value": str(economics.tick_value),
            "verification_source_ids": list(economics.source_ids),
        }
        prior = records.get(actual.identity_hash)
        if prior is not None and prior != record:
            raise IntegrityError("one actual identity has conflicting economics")
        records[actual.identity_hash] = record
    return tuple(records[key] for key in sorted(records))


def _economics_sources(
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
) -> tuple[VerifiedReleaseReceipt, ...]:
    return (
        causal_receipt,
        definitions.receipt,
        policies.receipt,
        session_policy.receipt,
    )


def _economics_metadata(
    *,
    actual_identity_count: int,
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    boundary: RepoBoundary,
) -> dict[str, object]:
    sources = _economics_sources(
        causal_receipt, definitions, policies, session_policy
    )
    return {
        **_base_metadata(boundary, sources),
        "actual_identity_count": actual_identity_count,
        "definition_release_receipt_id": definitions.receipt.receipt_id,
        "economics_rulebook_hash": policies.economics.rulebook_hash,
        "foundation_policy_set_id": policies.policy_set_id,
        "session_policy_receipt_id": session_policy.receipt.receipt_id,
        "source_causal_release_receipt_id": causal_receipt.receipt_id,
    }


def _validate_actual_contract_economics_release(
    receipt: VerifiedReleaseReceipt,
    *,
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    boundary: RepoBoundary,
    expected_records: tuple[dict[str, object], ...] | None,
) -> VerifiedEconomicsRegistry:
    manifest = receipt.verify(boundary)
    registry = VerifiedEconomicsRegistry.from_release(receipt, boundary)
    sources = _economics_sources(
        causal_receipt, definitions, policies, session_policy
    )
    expected_count = (
        len(registry.records)
        if expected_records is None
        else len(expected_records)
    )
    expected_metadata = _economics_metadata(
        actual_identity_count=expected_count,
        causal_receipt=causal_receipt,
        definitions=definitions,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
    )
    if (
        receipt.phase != "reference"
        or manifest.release_kind != ECONOMICS_RELEASE_KIND
        or manifest.schema_version != ECONOMICS_SCHEMA_VERSION
        or {Path(entry.path).name for entry in manifest.files}
        != {"contract_economics.json"}
        or manifest.source_release_ids
        != tuple(sorted(source.release_id for source in sources))
        or dict(manifest.metadata) != expected_metadata
    ):
        raise IntegrityError("economics bridge manifest or provenance is invalid")
    payload = _read_canonical(
        receipt.resolve_unique_filename("contract_economics.json", boundary)
    )
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != ECONOMICS_SCHEMA_VERSION
        or not isinstance(payload.get("records"), list)
        or len(payload["records"]) != len(registry.records)
        or (
            expected_records is not None
            and payload
            != {
                "records": list(expected_records),
                "schema_version": ECONOMICS_SCHEMA_VERSION,
            }
        )
    ):
        raise IntegrityError("economics bridge differs from its verified inputs")
    return registry


def publish_actual_contract_economics(
    *,
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    _assert_publisher(boundary, publisher)
    records = _economics_records(
        causal_receipt, definitions, policies, session_policy, boundary
    )
    sources = _economics_sources(
        causal_receipt, definitions, policies, session_policy
    )
    metadata = _economics_metadata(
        actual_identity_count=len(records),
        causal_receipt=causal_receipt,
        definitions=definitions,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
    )
    stage = publisher.create_stage("actual_contract_economics")
    _write_canonical(
        stage / "contract_economics.json",
        {"records": list(records), "schema_version": ECONOMICS_SCHEMA_VERSION},
    )
    manifest = ReleaseManifest.build(
        stage,
        phase="reference",
        release_kind=ECONOMICS_RELEASE_KIND,
        schema_version=ECONOMICS_SCHEMA_VERSION,
        logical_paths={
            "contract_economics.json": (
                "data/reference/economics/contract_economics.json"
            )
        },
        source_release_ids=tuple(receipt.release_id for receipt in sources),
        metadata=metadata,
    )
    manifest_path = publisher.publish(stage, manifest)
    receipt = VerifiedReleaseReceipt.from_manifest(manifest_path, boundary)
    _validate_actual_contract_economics_release(
        receipt,
        causal_receipt=causal_receipt,
        definitions=definitions,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
        expected_records=records,
    )
    return receipt


def load_actual_contract_economics(
    receipt: VerifiedReleaseReceipt,
    *,
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    boundary: RepoBoundary,
) -> VerifiedEconomicsRegistry:
    records = _economics_records(
        causal_receipt, definitions, policies, session_policy, boundary
    )
    return _validate_actual_contract_economics_release(
        receipt,
        causal_receipt=causal_receipt,
        definitions=definitions,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
        expected_records=records,
    )


def verify_actual_contract_economics_context(
    receipt: VerifiedReleaseReceipt,
    *,
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    boundary: RepoBoundary,
) -> VerifiedEconomicsRegistry:
    """Rebind one already derived registry without rescanning causal rows."""

    return _validate_actual_contract_economics_release(
        receipt,
        causal_receipt=causal_receipt,
        definitions=definitions,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
        expected_records=None,
    )


@dataclass(frozen=True)
class CausalFeatureSpec:
    """Finite, ordered, bar-local transforms; never a fitted preprocessing object."""

    feature_names: tuple[str, ...]
    entry_delay_seconds: int
    label_horizon_seconds: int

    def __post_init__(self) -> None:
        if (
            type(self.feature_names) is not tuple
            or not self.feature_names
            or self.feature_names != tuple(dict.fromkeys(self.feature_names))
            or any(
                type(name) is not str or name not in _FEATURE_FORMULAS
                for name in self.feature_names
            )
            or type(self.entry_delay_seconds) is not int
            or self.entry_delay_seconds <= 0
            or type(self.label_horizon_seconds) is not int
            or self.label_horizon_seconds <= self.entry_delay_seconds
        ):
            raise ContractError("causal feature specification is invalid")

    def as_dict(self) -> dict[str, object]:
        return {
            "entry_delay_seconds": self.entry_delay_seconds,
            "feature_names": list(self.feature_names),
            "formulas": {
                name: _FEATURE_FORMULAS[name] for name in self.feature_names
            },
            "label_horizon_seconds": self.label_horizon_seconds,
            "spec_version": "1.0.0",
        }

    @property
    def spec_hash(self) -> str:
        return sha256_json(self.as_dict())

    @classmethod
    def from_dict(cls, payload: object) -> "CausalFeatureSpec":
        if not isinstance(payload, dict) or set(payload) != {
            "entry_delay_seconds",
            "feature_names",
            "formulas",
            "label_horizon_seconds",
            "spec_version",
        }:
            raise IntegrityError("feature specification schema is invalid")
        if (
            payload["spec_version"] != "1.0.0"
            or not isinstance(payload["feature_names"], list)
            or any(type(name) is not str for name in payload["feature_names"])
            or type(payload["entry_delay_seconds"]) is not int
            or type(payload["label_horizon_seconds"]) is not int
            or not isinstance(payload["formulas"], dict)
        ):
            raise IntegrityError("feature specification fields are invalid")
        result = cls(
            tuple(payload["feature_names"]),
            payload["entry_delay_seconds"],
            payload["label_horizon_seconds"],
        )
        if result.as_dict() != payload:
            raise IntegrityError("feature specification is not canonical")
        return result


def _feature_values(
    row: Mapping[str, object], spec: CausalFeatureSpec
) -> dict[str, float | int]:
    opening = Decimal(_exact_int(row.get("open_nano"), "feature.open_nano"))
    high = Decimal(_exact_int(row.get("high_nano"), "feature.high_nano"))
    low = Decimal(_exact_int(row.get("low_nano"), "feature.low_nano"))
    close = Decimal(_exact_int(row.get("close_nano"), "feature.close_nano"))
    volume = _exact_int(row.get("volume"), "feature.volume")
    if opening <= 0 or high < max(opening, close) or low > min(opening, close) or volume < 0:
        raise IntegrityError("feature source OHLCV is invalid")
    candidates: Mapping[str, float | int] = {
        "bar_body_fraction": float((close - opening) / opening),
        "bar_return": float(close / opening - Decimal(1)),
        "close_price": float(close / Decimal(1_000_000_000)),
        "intrabar_range_fraction": float((high - low) / opening),
        "volume": volume,
    }
    values = {name: candidates[name] for name in spec.feature_names}
    if any(
        type(value) is float and not math.isfinite(value) for value in values.values()
    ):
        raise IntegrityError("bar-local feature transform produced a non-finite value")
    return values


def _feature_transform_hash(name: str, spec: CausalFeatureSpec) -> str:
    return sha256_json(
        {
            "feature_name": name,
            "feature_spec_hash": spec.spec_hash,
            "formula": _FEATURE_FORMULAS[name],
            "fit_or_global_state": False,
        }
    )


def _feature_record(
    row: Mapping[str, object],
    *,
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    economics_registry: VerifiedEconomicsRegistry,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    spec: CausalFeatureSpec,
    trust_cache: dict[
        tuple[object, ...],
        tuple[ActualContractIdentity, BridgeDefinitionRecord, ResolvedEconomics],
    ],
    verified_economics_registry_hash: str,
    verified_session_policy_hash: str,
) -> dict[str, object]:
    event_ns, available_ns = _verify_causal_policy_row(
        row, definitions=definitions, policies=policies
    )
    event = ns_to_datetime(event_ns, "event")
    available = ns_to_datetime(available_ns, "available")
    if row.get("prediction_in_coverage_denominator") is not True or available < event:
        raise IntegrityError("causal feature source violates timing/denominator policy")
    source_row = row.get("source_row_sha256")
    disposition = row.get("disposition")
    failure_code = row.get("failure_code")
    failure_hash = row.get("failure_detail_sha256")
    if (
        type(source_row) is not str
        or re.fullmatch(r"[0-9a-f]{64}", source_row) is None
        or type(disposition) is not str
        or (failure_code is not None and type(failure_code) is not str)
        or (failure_hash is not None and type(failure_hash) is not str)
    ):
        raise IntegrityError("causal feature source identity/disposition is invalid")
    planned_entry = available + timedelta(seconds=spec.entry_delay_seconds)
    label_unlock = available + timedelta(seconds=spec.label_horizon_seconds)
    base: dict[str, object] = {
        "actual_contract": None,
        "available_at": available.isoformat(),
        "bar_event_at": event.isoformat(),
        "decision_at": available.isoformat(),
        "failure_code": failure_code,
        "failure_detail_sha256": failure_hash,
        "inputs_complete": False,
        "label_unlock_at": label_unlock.isoformat(),
        "lineage": None,
        "planned_entry_at": planned_entry.isoformat(),
        "prediction_in_coverage_denominator": True,
        "status": f"UPSTREAM_{disposition}",
        "upstream_disposition": disposition,
        "upstream_foundation_actual_identity_hash": row.get("actual_identity_hash"),
        "upstream_release_id": causal_receipt.release_id,
        "upstream_release_receipt_id": causal_receipt.receipt_id,
        "upstream_source_row_sha256": source_row,
        "values": None,
    }
    if disposition == "ANOMALY_QUARANTINED":
        _trust_actual_from_causal_cached(
            row,
            definitions,
            policies,
            session_policy,
            trust_cache,
            verified_times=(event_ns, available_ns),
            verified_session_policy_hash=verified_session_policy_hash,
        )
    if disposition != "ELIGIBLE":
        return {**base, "record_id": sha256_json(base)}
    actual, _, _ = _trust_actual_from_causal_cached(
        row,
        definitions,
        policies,
        session_policy,
        trust_cache,
        verified_times=(event_ns, available_ns),
        verified_session_policy_hash=verified_session_policy_hash,
    )
    verified_economics = economics_registry._resolve_preverified(
        actual,
        available,
        expected_registry_hash=verified_economics_registry_hash,
    )
    if (
        str(verified_economics.tick_size) != row.get("tick_size")
        or str(verified_economics.point_value) != row.get("point_value")
        or verified_economics.currency != row.get("currency")
    ):
        raise IntegrityError("feature row economics differs from its verified registry")
    values = _feature_values(row, spec)
    availability_policy_hash = sha256_json(
        {
            "feature_spec_hash": spec.spec_hash,
            "upstream_availability_basis": row.get("availability_basis"),
            "upstream_availability_policy_hash": row.get("availability_policy_hash"),
        }
    )
    lineage = {
        name: {
            "availability_basis": AvailabilityBasis.DERIVED_FROM_VERIFIED_UPSTREAM.value,
            "availability_policy_hash": availability_policy_hash,
            "available_at": available.isoformat(),
            "contract_segment_hash": actual.contract_segment_hash,
            "source_release_id": causal_receipt.release_id,
            "source_release_retrieved_at": available.isoformat(),
            "transform_hash": _feature_transform_hash(name, spec),
            "upstream_source_row_sha256": source_row,
            "uses_future_outcome": False,
            "uses_retrospective_roll_mapping": False,
        }
        for name in spec.feature_names
    }
    ready = {
        **base,
        "actual_contract": actual.as_dict(),
        "failure_code": None,
        "failure_detail_sha256": None,
        "inputs_complete": True,
        "lineage": lineage,
        "status": "FEATURE_READY",
        "values": values,
    }
    return {**ready, "record_id": sha256_json(ready)}


def _iter_feature_records(
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    economics_registry: VerifiedEconomicsRegistry,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    spec: CausalFeatureSpec,
    boundary: RepoBoundary,
) -> Iterator[dict[str, object]]:
    economics_registry.verify()
    verified_economics_registry_hash = economics_registry.registry_hash
    session_policy.verify()
    verified_session_policy_hash = session_policy.policy_hash
    trust_cache: dict[
        tuple[object, ...],
        tuple[ActualContractIdentity, BridgeDefinitionRecord, ResolvedEconomics],
    ] = {}
    for row in _iter_causal_rows(causal_receipt, boundary):
        yield _feature_record(
            row,
            causal_receipt=causal_receipt,
            definitions=definitions,
            economics_registry=economics_registry,
            policies=policies,
            session_policy=session_policy,
            spec=spec,
            trust_cache=trust_cache,
            verified_economics_registry_hash=verified_economics_registry_hash,
            verified_session_policy_hash=verified_session_policy_hash,
        )


def _actual_from_dict(payload: object) -> ActualContractIdentity:
    expected = {
        "currency",
        "dataset",
        "definition_manifest_sha256",
        "definition_release_id",
        "definition_row_id",
        "exchange",
        "exchange_session_date",
        "instrument_id",
        "instrument_id_date_utc",
        "min_tick",
        "multiplier",
        "publisher_id",
        "raw_symbol",
    }
    string_fields = expected.difference({"instrument_id", "publisher_id"})
    if (
        not isinstance(payload, dict)
        or set(payload) != expected
        or any(type(payload[name]) is not str for name in string_fields)
        or type(payload["instrument_id"]) is not int
        or type(payload["publisher_id"]) is not int
    ):
        raise IntegrityError("actual-contract JSON schema is invalid")
    try:
        actual = ActualContractIdentity(
            dataset=payload["dataset"],
            publisher_id=payload["publisher_id"],
            instrument_id=payload["instrument_id"],
            instrument_id_date_utc=date.fromisoformat(payload["instrument_id_date_utc"]),
            exchange_session_date=date.fromisoformat(payload["exchange_session_date"]),
            raw_symbol=payload["raw_symbol"],
            exchange=payload["exchange"],
            definition_release_id=payload["definition_release_id"],
            definition_manifest_sha256=payload["definition_manifest_sha256"],
            definition_row_id=payload["definition_row_id"],
            currency=payload["currency"],
            multiplier=Decimal(payload["multiplier"]),
            min_tick=Decimal(payload["min_tick"]),
        )
    except (ValueError, InvalidOperation, ContractError) as exc:
        raise IntegrityError("actual-contract JSON is invalid") from exc
    if actual.as_dict() != payload:
        raise IntegrityError("actual-contract JSON is not canonical")
    return actual


@dataclass(frozen=True)
class LoadedFeatureRelease:
    receipt: VerifiedReleaseReceipt
    source_causal_receipt: VerifiedReleaseReceipt
    feature_spec: CausalFeatureSpec
    rows: tuple[FeatureRow, ...]
    total_upstream_rows: int
    unresolved_upstream_rows: int


@dataclass(frozen=True)
class CausalOutcomeContext:
    """Exact verified dependency closure for one causal outcome interval."""

    causal_receipt: VerifiedReleaseReceipt
    definitions: LoadedActualContractDefinitions
    economics_registry: VerifiedEconomicsRegistry
    policies: VerifiedFoundationPolicies
    session_policy: VerifiedSessionPolicy

    def verify(self, boundary: RepoBoundary) -> None:
        _verify_bridge_context(
            causal_receipt=self.causal_receipt,
            definitions=self.definitions,
            policies=self.policies,
            session_policy=self.session_policy,
            boundary=boundary,
        )
        _verify_economics_context(
            self.economics_registry,
            causal_receipt=self.causal_receipt,
            definitions=self.definitions,
            policies=self.policies,
            session_policy=self.session_policy,
            boundary=boundary,
        )


def _feature_sources(
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    economics_registry: VerifiedEconomicsRegistry,
    session_policy: VerifiedSessionPolicy,
    policies: VerifiedFoundationPolicies,
) -> tuple[VerifiedReleaseReceipt, ...]:
    return (
        causal_receipt,
        definitions.receipt,
        economics_registry.release_receipt,
        policies.receipt,
        session_policy.receipt,
    )


def _verify_economics_context(
    economics_registry: VerifiedEconomicsRegistry,
    *,
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    boundary: RepoBoundary,
) -> None:
    rebuilt = verify_actual_contract_economics_context(
        economics_registry.release_receipt,
        causal_receipt=causal_receipt,
        definitions=definitions,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
    )
    if rebuilt.registry_hash != economics_registry.registry_hash:
        raise IntegrityError("economics bridge is not bound to the supplied causal chain")


def _publish_causal_feature_release(
    *,
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    economics_registry: VerifiedEconomicsRegistry,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    feature_spec: CausalFeatureSpec,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
    verify_readback: bool,
) -> VerifiedReleaseReceipt:
    """Publish bar-local features while retaining every upstream disposition row."""

    _assert_publisher(boundary, publisher)
    _verify_bridge_context(
        causal_receipt=causal_receipt,
        definitions=definitions,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
    )
    _verify_economics_context(
        economics_registry,
        causal_receipt=causal_receipt,
        definitions=definitions,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
    )
    sources = _feature_sources(
        causal_receipt, definitions, economics_registry, session_policy, policies
    )
    stage = publisher.create_stage("causal_features")
    rows_path = stage / "feature_rows.jsonl"
    total = ready = unresolved = 0
    with rows_path.open("xb") as handle:
        for record in _iter_feature_records(
            causal_receipt,
            definitions,
            economics_registry,
            policies,
            session_policy,
            feature_spec,
            boundary,
        ):
            handle.write(canonical_bytes(record) + b"\n")
            total += 1
            if record["status"] == "FEATURE_READY":
                ready += 1
            else:
                unresolved += 1
        handle.flush()
        os.fsync(handle.fileno())
    if total == 0 or ready + unresolved != total:
        raise IntegrityError("feature release census is invalid")
    contract = {
        **_base_metadata(boundary, sources),
        "feature_ready_rows": ready,
        "feature_spec": feature_spec.as_dict(),
        "feature_spec_hash": feature_spec.spec_hash,
        "schema_version": FEATURE_SCHEMA_VERSION,
        "source_causal_release_receipt_id": causal_receipt.receipt_id,
        "total_upstream_rows": total,
        "unresolved_upstream_rows": unresolved,
    }
    _write_canonical(stage / "feature_contract.json", contract)
    metadata = {
        key: value for key, value in contract.items() if key != "feature_spec"
    }
    causal_manifest = causal_receipt.verify(boundary)
    causal_root = str(causal_manifest.metadata.get("logical_root", ""))
    causal_prefix = "data/causally_gated_normalized/"
    if not causal_root.startswith(causal_prefix):
        raise IntegrityError("feature release lacks a layout-v2 causal selector")
    feature_root = (
        f"data/features/{feature_spec.spec_hash}/"
        f"{causal_root.removeprefix(causal_prefix)}"
    )
    manifest = ReleaseManifest.build(
        stage,
        phase="features",
        release_kind=FEATURE_RELEASE_KIND,
        schema_version=FEATURE_SCHEMA_VERSION,
        logical_paths={
            "feature_contract.json": f"{feature_root}/feature_contract.json",
            "feature_rows.jsonl": f"{feature_root}/feature_rows.jsonl",
        },
        source_release_ids=tuple(source.release_id for source in sources),
        metadata=metadata,
    )
    manifest_path = publisher.publish(stage, manifest)
    receipt = VerifiedReleaseReceipt.from_manifest(manifest_path, boundary)
    if verify_readback:
        load_causal_feature_release(
            receipt,
            causal_receipt=causal_receipt,
            definitions=definitions,
            economics_registry=economics_registry,
            policies=policies,
            session_policy=session_policy,
            boundary=boundary,
        )
    return receipt


def publish_causal_feature_release(
    *,
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    economics_registry: VerifiedEconomicsRegistry,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    feature_spec: CausalFeatureSpec,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    """Publish and independently reproduce a feature release."""

    return _publish_causal_feature_release(
        causal_receipt=causal_receipt,
        definitions=definitions,
        economics_registry=economics_registry,
        policies=policies,
        session_policy=session_policy,
        feature_spec=feature_spec,
        boundary=boundary,
        publisher=publisher,
        verify_readback=True,
    )


def publish_causal_feature_release_checkpointed(
    *,
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    economics_registry: VerifiedEconomicsRegistry,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    feature_spec: CausalFeatureSpec,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    """Publish once for an orchestrator that persists the exact receipt next."""

    return _publish_causal_feature_release(
        causal_receipt=causal_receipt,
        definitions=definitions,
        economics_registry=economics_registry,
        policies=policies,
        session_policy=session_policy,
        feature_spec=feature_spec,
        boundary=boundary,
        publisher=publisher,
        verify_readback=False,
    )


def _parse_feature_row(
    record: Mapping[str, object],
    *,
    feature_receipt: VerifiedReleaseReceipt,
    causal_receipt: VerifiedReleaseReceipt,
    spec: CausalFeatureSpec,
    boundary: RepoBoundary,
) -> FeatureRow:
    if record.get("status") != "FEATURE_READY":
        raise IntegrityError("only feature-ready records can become FeatureRow objects")
    actual = _actual_from_dict(record.get("actual_contract"))
    values = record.get("values")
    raw_lineage = record.get("lineage")
    if (
        not isinstance(values, dict)
        or set(values) != set(spec.feature_names)
        or not isinstance(raw_lineage, dict)
        or set(raw_lineage) != set(spec.feature_names)
    ):
        raise IntegrityError("feature row schema differs from its contract")
    ordered_values = {name: values[name] for name in spec.feature_names}
    lineage: dict[str, FeatureLineage] = {}
    for name in spec.feature_names:
        raw = raw_lineage[name]
        if not isinstance(raw, dict) or set(raw) != {
            "availability_basis",
            "availability_policy_hash",
            "available_at",
            "contract_segment_hash",
            "source_release_id",
            "source_release_retrieved_at",
            "transform_hash",
            "upstream_source_row_sha256",
            "uses_future_outcome",
            "uses_retrospective_roll_mapping",
        }:
            raise IntegrityError("feature lineage payload is invalid")
        try:
            lineage[name] = FeatureLineage(
                source_release_id=raw["source_release_id"],  # type: ignore[arg-type]
                available_at=datetime.fromisoformat(raw["available_at"]),  # type: ignore[arg-type]
                transform_hash=raw["transform_hash"],  # type: ignore[arg-type]
                availability_basis=AvailabilityBasis(raw["availability_basis"]),
                availability_policy_hash=raw["availability_policy_hash"],  # type: ignore[arg-type]
                source_release_retrieved_at=datetime.fromisoformat(
                    raw["source_release_retrieved_at"]  # type: ignore[arg-type]
                ),
                contract_segment_hash=raw["contract_segment_hash"],  # type: ignore[arg-type]
                uses_retrospective_roll_mapping=raw[
                    "uses_retrospective_roll_mapping"
                ],  # type: ignore[arg-type]
                uses_future_outcome=raw["uses_future_outcome"],  # type: ignore[arg-type]
            )
        except (TypeError, ValueError, ContractError) as exc:
            raise IntegrityError("feature lineage payload violates its contract") from exc
    try:
        return FeatureRow(
            actual=actual,
            bar_event_at=datetime.fromisoformat(record["bar_event_at"]),  # type: ignore[arg-type]
            decision_at=datetime.fromisoformat(record["decision_at"]),  # type: ignore[arg-type]
            available_at_max=datetime.fromisoformat(record["available_at"]),  # type: ignore[arg-type]
            source_release_id=feature_receipt.release_id,
            allowed_upstream_release_ids=(causal_receipt.release_id,),
            verified_release_receipts=tuple(
                sorted((feature_receipt, causal_receipt), key=lambda item: item.release_id)
            ),
            boundary=boundary,
            values=ordered_values,  # type: ignore[arg-type]
            lineage=lineage,
            inputs_complete=record["inputs_complete"],  # type: ignore[arg-type]
            planned_entry_at=datetime.fromisoformat(record["planned_entry_at"]),  # type: ignore[arg-type]
            label_unlock_at=datetime.fromisoformat(record["label_unlock_at"]),  # type: ignore[arg-type]
        )
    except (KeyError, TypeError, ValueError, ContractError) as exc:
        raise IntegrityError("feature-ready row violates FeatureRow") from exc


_FEATURE_RECORD_KEYS = frozenset(
    {
        "actual_contract",
        "available_at",
        "bar_event_at",
        "decision_at",
        "failure_code",
        "failure_detail_sha256",
        "inputs_complete",
        "label_unlock_at",
        "lineage",
        "planned_entry_at",
        "prediction_in_coverage_denominator",
        "record_id",
        "status",
        "upstream_disposition",
        "upstream_foundation_actual_identity_hash",
        "upstream_release_id",
        "upstream_release_receipt_id",
        "upstream_source_row_sha256",
        "values",
    }
)


def _verified_feature_release_header(
    receipt: VerifiedReleaseReceipt,
    *,
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    economics_registry: VerifiedEconomicsRegistry,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    boundary: RepoBoundary,
) -> tuple[ReleaseManifest, dict[str, object], CausalFeatureSpec, Path]:
    manifest = receipt.verify(boundary)
    if (
        receipt.phase != "features"
        or manifest.release_kind != FEATURE_RELEASE_KIND
        or manifest.schema_version != FEATURE_SCHEMA_VERSION
    ):
        raise IntegrityError("receipt is not an exact feature release")
    _verify_bridge_context(
        causal_receipt=causal_receipt,
        definitions=definitions,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
    )
    _verify_economics_context(
        economics_registry,
        causal_receipt=causal_receipt,
        definitions=definitions,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
    )
    sources = _feature_sources(
        causal_receipt, definitions, economics_registry, session_policy, policies
    )
    paths = {
        Path(entry.logical_path).name: boundary.assert_active_path(
            boundary.active_root / manifest.physical_relative_path(entry),
            purpose="verified feature release file",
            subtree="data/features",
        )
        for entry in manifest.files
    }
    if set(paths) != {"feature_contract.json", "feature_rows.jsonl"}:
        raise IntegrityError("feature release file set is invalid")
    contract = _read_canonical(paths["feature_contract.json"])
    if not isinstance(contract, dict) or set(contract) != {
        "bridge_code_sha256",
        "environment_lock_sha256",
        "feature_ready_rows",
        "feature_spec",
        "feature_spec_hash",
        "schema_version",
        "source_causal_release_receipt_id",
        "source_receipts_sha256",
        "total_upstream_rows",
        "unresolved_upstream_rows",
    }:
        raise IntegrityError("feature release contract schema is invalid")
    spec = CausalFeatureSpec.from_dict(contract["feature_spec"])
    expected_metadata = {
        key: value for key, value in contract.items() if key != "feature_spec"
    }
    if (
        contract["schema_version"] != FEATURE_SCHEMA_VERSION
        or contract["feature_spec_hash"] != spec.spec_hash
        or manifest.source_release_ids
        != tuple(sorted(source.release_id for source in sources))
        or dict(manifest.metadata) != expected_metadata
        or contract["source_receipts_sha256"] != _source_receipt_hash(sources)
        or contract["bridge_code_sha256"] != _module_hash()
        or contract["environment_lock_sha256"] != _environment_hash(boundary)
    ):
        raise IntegrityError("feature release provenance is invalid")
    return manifest, contract, spec, paths["feature_rows.jsonl"]


def _load_causal_feature_release(
    receipt: VerifiedReleaseReceipt,
    *,
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    economics_registry: VerifiedEconomicsRegistry,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    boundary: RepoBoundary,
    reproduce_from_source: bool,
) -> LoadedFeatureRelease:
    _, contract, spec, rows_path = _verified_feature_release_header(
        receipt,
        causal_receipt=causal_receipt,
        definitions=definitions,
        economics_registry=economics_registry,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
    )
    expected_records = (
        _iter_feature_records(
            causal_receipt,
            definitions,
            economics_registry,
            policies,
            session_policy,
            spec,
            boundary,
        )
        if reproduce_from_source
        else None
    )
    rows: list[FeatureRow] = []
    total = ready = unresolved = 0
    try:
        with rows_path.open("rb") as handle:
            paired = (
                zip(handle, expected_records, strict=True)
                if expected_records is not None
                else ((line, None) for line in handle)
            )
            for line, expected in paired:
                observed = json.loads(line.decode("utf-8"))
                if line != canonical_bytes(observed) + b"\n" or (
                    expected is not None and observed != expected
                ):
                    raise IntegrityError("feature row differs from verified causal input")
                total += 1
                if observed["status"] == "FEATURE_READY":
                    rows.append(
                        _parse_feature_row(
                            observed,
                            feature_receipt=receipt,
                            causal_receipt=causal_receipt,
                            spec=spec,
                            boundary=boundary,
                        )
                    )
                    ready += 1
                else:
                    unresolved += 1
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("feature rows JSONL is invalid") from exc
    if (
        total != contract["total_upstream_rows"]
        or ready != contract["feature_ready_rows"]
        or unresolved != contract["unresolved_upstream_rows"]
        or ready + unresolved != total
    ):
        raise IntegrityError("feature release census is invalid")
    return LoadedFeatureRelease(
        receipt, causal_receipt, spec, tuple(rows), total, unresolved
    )


def load_causal_feature_release(
    receipt: VerifiedReleaseReceipt,
    *,
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    economics_registry: VerifiedEconomicsRegistry,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    boundary: RepoBoundary,
) -> LoadedFeatureRelease:
    """Reproduce a persisted release from its full causal dependency chain."""

    return _load_causal_feature_release(
        receipt,
        causal_receipt=causal_receipt,
        definitions=definitions,
        economics_registry=economics_registry,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
        reproduce_from_source=True,
    )


def verify_newly_published_causal_feature_release(
    receipt: VerifiedReleaseReceipt,
    *,
    causal_receipt: VerifiedReleaseReceipt,
    definitions: LoadedActualContractDefinitions,
    economics_registry: VerifiedEconomicsRegistry,
    policies: VerifiedFoundationPolicies,
    session_policy: VerifiedSessionPolicy,
    boundary: RepoBoundary,
) -> None:
    """Stream-verify a just-published release without rebuilding FeatureRows."""

    manifest, contract, spec, rows_path = _verified_feature_release_header(
        receipt,
        causal_receipt=causal_receipt,
        definitions=definitions,
        economics_registry=economics_registry,
        policies=policies,
        session_policy=session_policy,
        boundary=boundary,
    )
    total = ready = unresolved = 0
    try:
        with rows_path.open("rb") as handle:
            for line in handle:
                observed = json.loads(line.decode("utf-8"))
                if not isinstance(observed, dict) or set(observed) != _FEATURE_RECORD_KEYS:
                    raise IntegrityError("feature row schema is invalid")
                core = dict(observed)
                record_id = core.pop("record_id")
                disposition = observed["upstream_disposition"]
                status = observed["status"]
                if (
                    type(record_id) is not str
                    or record_id != sha256_json(core)
                    or type(disposition) is not str
                    or disposition
                    not in _RESOLVED_CAUSAL_DISPOSITIONS
                    | {_UNRESOLVED_CAUSAL_DISPOSITION}
                    or observed["prediction_in_coverage_denominator"] is not True
                    or observed["upstream_release_id"] != causal_receipt.release_id
                    or observed["upstream_release_receipt_id"]
                    != causal_receipt.receipt_id
                ):
                    raise IntegrityError("feature row identity or dependency is invalid")
                total += 1
                if status == "FEATURE_READY":
                    if (
                        disposition != "ELIGIBLE"
                        or observed["inputs_complete"] is not True
                        or not isinstance(observed["actual_contract"], dict)
                        or not isinstance(observed["values"], dict)
                        or set(observed["values"]) != set(spec.feature_names)
                        or not isinstance(observed["lineage"], dict)
                        or set(observed["lineage"]) != set(spec.feature_names)
                        or observed["failure_code"] is not None
                        or observed["failure_detail_sha256"] is not None
                    ):
                        raise IntegrityError("feature-ready row is not fail-closed")
                    ready += 1
                else:
                    if (
                        disposition == "ELIGIBLE"
                        or status != f"UPSTREAM_{disposition}"
                        or observed["inputs_complete"] is not False
                        or observed["actual_contract"] is not None
                        or observed["values"] is not None
                        or observed["lineage"] is not None
                    ):
                        raise IntegrityError("unresolved feature row is invalid")
                    unresolved += 1
    except (OSError, UnicodeDecodeError, ValueError, KeyError, TypeError) as exc:
        raise IntegrityError("feature rows JSONL is invalid") from exc
    if (
        total != contract["total_upstream_rows"]
        or ready != contract["feature_ready_rows"]
        or unresolved != contract["unresolved_upstream_rows"]
        or ready + unresolved != total
    ):
        raise IntegrityError("feature release census is invalid")
    if receipt.verify(boundary).as_dict() != manifest.as_dict():
        raise IntegrityError("feature release changed during streaming verification")


def _prediction_market(
    prediction: object,
    definitions: LoadedActualContractDefinitions,
) -> str:
    from .schemas import PredictionRow

    if type(prediction) is not PredictionRow:
        raise ContractError("causal outcome generation requires exact prediction rows")
    matches: list[str] = []
    for bridged in definitions.by_provider_row.values():
        observation = definitions.registry.definitions[bridged.registry_row_id]
        candidate = ActualContractIdentity.from_definition(
            observation.definition,
            instrument_id_date_utc=prediction.actual.instrument_id_date_utc,
            exchange_session_date=prediction.actual.exchange_session_date,
        )
        if candidate == prediction.actual:
            matches.append(bridged.provider.market)
    if len(matches) != 1:
        raise ContractError(
            "prediction actual contract is absent or ambiguous in its causal interval"
        )
    return matches[0]


def _same_definition_basis(
    left: ActualContractIdentity, right: ActualContractIdentity
) -> bool:
    left_payload = left.as_dict()
    right_payload = right.as_dict()
    for name in ("instrument_id_date_utc", "exchange_session_date"):
        left_payload.pop(name)
        right_payload.pop(name)
    return left_payload == right_payload


def _tick_valid_price(price_nano: int, tick_size: Decimal) -> bool:
    tick_nano = tick_size * _PRICE_NANO
    if tick_nano != tick_nano.to_integral_value() or tick_nano <= 0:
        raise IntegrityError("verified tick size cannot be represented in nanounits")
    return price_nano > 0 and price_nano % int(tick_nano) == 0


def _causal_outcome_for_prediction(
    prediction: object,
    *,
    market: str,
    rows_by_market_event: Mapping[tuple[str, int], tuple[Mapping[str, object], ...]],
    context: CausalOutcomeContext,
) -> OutcomeRow:
    from .schemas import PredictionRow

    if type(prediction) is not PredictionRow:
        raise ContractError("causal outcome generation requires exact prediction rows")
    start_ns = datetime_to_ns(prediction.planned_entry_at, "planned_entry_at")
    end_ns = datetime_to_ns(prediction.label_unlock_at, "label_unlock_at")
    prediction_segment = prediction.actual.contract_segment_hash
    segments: list[str] = [prediction_segment]
    available_times: list[datetime] = []
    missing = False
    prices: dict[int, int] = {}

    prediction_economics = context.economics_registry.resolve(
        prediction.actual, prediction.decision_at
    )
    if prediction_economics.record_id != prediction.economics_record_id:
        declared_missing_economics = (
            prediction.abstained
            and prediction.economics_record_id == "0" * 64
            and "MISSING_OR_AMBIGUOUS_ECONOMICS" in prediction.abstention_reasons
        )
        if not declared_missing_economics:
            raise ContractError(
                "prediction economics ID differs from its verified actual contract"
            )
        missing = True
    if start_ns % _ONE_MINUTE_NS or end_ns % _ONE_MINUTE_NS or end_ns < start_ns:
        missing = True
        expected_events: tuple[int, ...] = ()
    else:
        expected_events = tuple(range(start_ns, end_ns + 1, _ONE_MINUTE_NS))

    for event_ns in expected_events:
        candidates = rows_by_market_event.get((market, event_ns), ())
        if len(candidates) != 1:
            missing = True
        if not candidates:
            continue
        for row in candidates:
            available_ns = _exact_int(
                row.get("available_at_ns"), "outcome.available_at_ns"
            )
            available = ns_to_datetime(available_ns, "outcome.available_at_ns")
            available_times.append(available)
            disposition = row.get("disposition")
            if disposition not in _RESOLVED_CAUSAL_DISPOSITIONS:
                missing = True
                continue
            actual, _, _ = _trust_actual_from_causal(
                row,
                context.definitions,
                context.policies,
                context.session_policy,
            )
            segments.append(actual.contract_segment_hash)
            if disposition != "ELIGIBLE":
                missing = True
                continue
            row_economics = context.economics_registry.resolve(actual, available)
            price_nano = _exact_int(row.get("open_nano"), "outcome.open_nano")
            if (
                actual.contract_segment_hash != prediction_segment
                or not _same_definition_basis(actual, prediction.actual)
                or actual.exchange_session_date
                != prediction.actual.exchange_session_date
                or row_economics.tick_size != prediction_economics.tick_size
                or row_economics.point_value != prediction_economics.point_value
                or row_economics.tick_value != prediction_economics.tick_value
                or row_economics.currency != prediction_economics.currency
                or not _tick_valid_price(price_nano, row_economics.tick_size)
            ):
                missing = True
                continue
            prices[event_ns] = price_nano

    crossed_contract = len(set(segments)) != 1
    matured_at = max((prediction.label_unlock_at, *available_times))
    if crossed_contract:
        status = OutcomeStatus.ROLL_UNRESOLVED
        price_return: float | None = None
    elif (
        missing
        or not expected_events
        or set(prices) != set(expected_events)
    ):
        status = OutcomeStatus.MISSING_SOURCE
        price_return = None
    else:
        opening = Decimal(prices[start_ns])
        closing = Decimal(prices[end_ns])
        price_return = float(closing / opening - Decimal(1))
        if not math.isfinite(price_return):
            raise IntegrityError("causal outcome return is non-finite")
        status = OutcomeStatus.MATURED
    return OutcomeRow(
        prediction_id=prediction.prediction_id,
        actual=prediction.actual,
        decision_at=prediction.decision_at,
        label_end_at=prediction.label_unlock_at,
        matured_at=matured_at,
        source_release_id=context.causal_receipt.release_id,
        interval_contract_segment_hashes=tuple(segments),
        included_in_coverage_denominator=True,
        status=status,
        price_return=price_return,
    )


def generate_causal_outcomes(
    *,
    prediction_census: PredictionCensusReceipt,
    prediction_ledger: PredictionLedger,
    context: CausalOutcomeContext,
    boundary: RepoBoundary,
) -> tuple[OutcomeRow, ...]:
    """Generate exact one-minute open-to-open labels without crossing contracts.

    Missing bars, ambiguous rows, non-grid timestamps, quarantined inputs, session
    changes, economics changes, and tick-invalid prices remain explicit denominator
    rows.  An observed actual-contract change is always ``ROLL_UNRESOLVED``.
    """

    prediction_census.verify(prediction_ledger)
    context.verify(boundary)
    rows = tuple(_iter_causal_rows(context.causal_receipt, boundary))
    grouped: dict[tuple[str, int], list[Mapping[str, object]]] = {}
    for row in rows:
        market = row.get("market")
        event_ns = row.get("event_at_ns")
        if type(market) is not str or type(event_ns) is not int:
            raise IntegrityError("causal outcome row market/event identity is invalid")
        grouped.setdefault((market, event_ns), []).append(row)
    frozen_grouped = {
        key: tuple(value)
        for key, value in sorted(grouped.items(), key=lambda item: item[0])
    }
    predictions = prediction_ledger.prediction_rows()
    if tuple(row.prediction_id for row in predictions) != prediction_census.prediction_ids:
        raise IntegrityError("prediction census order differs from its verified ledger")
    outcomes = tuple(
        _causal_outcome_for_prediction(
            prediction,
            market=_prediction_market(prediction, context.definitions),
            rows_by_market_event=frozen_grouped,
            context=context,
        )
        for prediction in predictions
    )
    OutcomeCoverageReport(prediction_census, outcomes, prediction_ledger)
    return outcomes


def _outcome_payload(row: OutcomeRow) -> dict[str, object]:
    return {
        "actual_contract": row.actual.as_dict(),
        "decision_at": row.decision_at.isoformat(),
        "included_in_coverage_denominator": row.included_in_coverage_denominator,
        "interval_contract_segment_hashes": list(
            row.interval_contract_segment_hashes
        ),
        "label_end_at": row.label_end_at.isoformat(),
        "matured_at": row.matured_at.isoformat(),
        "prediction_id": row.prediction_id,
        "price_return": row.price_return,
        "source_release_id": row.source_release_id,
        "status": row.status.value,
    }


def _outcome_from_payload(payload: object) -> OutcomeRow:
    expected = {
        "actual_contract",
        "decision_at",
        "included_in_coverage_denominator",
        "interval_contract_segment_hashes",
        "label_end_at",
        "matured_at",
        "prediction_id",
        "price_return",
        "source_release_id",
        "status",
    }
    if (
        not isinstance(payload, dict)
        or set(payload) != expected
        or any(
            type(payload[name]) is not str
            for name in (
                "decision_at",
                "label_end_at",
                "matured_at",
                "prediction_id",
                "source_release_id",
                "status",
            )
        )
        or type(payload["included_in_coverage_denominator"]) is not bool
        or not isinstance(payload["interval_contract_segment_hashes"], list)
        or any(
            type(item) is not str
            for item in payload["interval_contract_segment_hashes"]
        )
        or (
            payload["price_return"] is not None
            and type(payload["price_return"]) not in {int, float}
        )
    ):
        raise IntegrityError("outcome payload schema/types are invalid")
    try:
        row = OutcomeRow(
            prediction_id=payload["prediction_id"],
            actual=_actual_from_dict(payload["actual_contract"]),
            decision_at=datetime.fromisoformat(payload["decision_at"]),
            label_end_at=datetime.fromisoformat(payload["label_end_at"]),
            matured_at=datetime.fromisoformat(payload["matured_at"]),
            source_release_id=payload["source_release_id"],
            interval_contract_segment_hashes=tuple(
                payload["interval_contract_segment_hashes"]
            ),
            included_in_coverage_denominator=payload[
                "included_in_coverage_denominator"
            ],
            status=OutcomeStatus(payload["status"]),
            price_return=payload["price_return"],
        )
    except (ValueError, TypeError, ContractError) as exc:
        raise IntegrityError("outcome payload violates OutcomeRow") from exc
    if _outcome_payload(row) != payload:
        raise IntegrityError("outcome payload is not canonical")
    return row


def _census_contract(
    census: PredictionCensusReceipt, ledger: PredictionLedger
) -> dict[str, object]:
    census.verify(ledger)
    return {
        "ledger_head": {
            "anchor_hash": census.head.anchor_hash,
            "record_hash": census.head.record_hash,
            "sequence": census.head.sequence,
        },
        "ledger_id": census.ledger_id,
        "prediction_census_receipt_id": census.receipt_id,
        "prediction_ids_sha256": sha256_json(list(census.prediction_ids)),
        "prediction_issuer_mac_sha256": sha256_json(census.issuer_mac),
        "repository_id": census.repository_id,
    }


@dataclass(frozen=True)
class LoadedOutcomeRelease:
    receipt: VerifiedReleaseReceipt
    outcomes: tuple[OutcomeRow, ...]
    coverage: OutcomeCoverageReport
    label_method_id: str | None


def _verified_outcome_sources(
    source_receipts: tuple[VerifiedReleaseReceipt, ...],
    boundary: RepoBoundary,
) -> frozenset[str]:
    if (
        type(source_receipts) is not tuple
        or not source_receipts
        or any(type(item) is not VerifiedReleaseReceipt for item in source_receipts)
        or tuple(item.release_id for item in source_receipts)
        != tuple(sorted({item.release_id for item in source_receipts}))
    ):
        raise ContractError("outcome sources must be explicit, unique, and sorted")
    source_ids: set[str] = set()
    for source in source_receipts:
        manifest = source.verify(boundary)
        if manifest.release_kind != CAUSAL_RELEASE_KIND:
            raise ContractError("outcomes accept only verified Phase 2 sources")
        source_ids.add(source.release_id)
    return frozenset(source_ids)


def _verify_outcome_prediction_join(
    outcomes: tuple[OutcomeRow, ...],
    prediction_census: PredictionCensusReceipt,
    prediction_ledger: PredictionLedger,
) -> None:
    prediction_census.verify(prediction_ledger)
    predictions = prediction_ledger.prediction_rows()
    if tuple(row.prediction_id for row in predictions) != prediction_census.prediction_ids:
        raise IntegrityError("prediction census order differs from its verified ledger")
    by_id = {row.prediction_id: row for row in predictions}
    for outcome in outcomes:
        prediction = by_id.get(outcome.prediction_id)
        if (
            prediction is None
            or outcome.actual != prediction.actual
            or outcome.decision_at != prediction.decision_at
            or outcome.label_end_at != prediction.label_unlock_at
        ):
            raise ContractError(
                "outcome identity/timing does not match its exact prediction"
            )


def publish_outcome_release(
    *,
    outcomes: tuple[OutcomeRow, ...],
    prediction_census: PredictionCensusReceipt,
    prediction_ledger: PredictionLedger,
    source_receipts: tuple[VerifiedReleaseReceipt, ...],
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
    label_method_id: str | None = None,
) -> VerifiedReleaseReceipt:
    """Publish an exact post-prediction join; unresolved rows remain denominator rows."""

    _assert_publisher(boundary, publisher)
    if label_method_id != CAUSAL_OUTCOME_LABEL_METHOD_ID:
        raise ContractError("outcome label method is not an allowlisted contract")
    source_ids = _verified_outcome_sources(source_receipts, boundary)
    _verify_outcome_prediction_join(
        outcomes, prediction_census, prediction_ledger
    )
    coverage = OutcomeCoverageReport(
        prediction_census, outcomes, prediction_ledger
    )
    if any(row.source_release_id not in source_ids for row in outcomes):
        raise ContractError("outcome row uses an unverified source release")
    census_contract = _census_contract(prediction_census, prediction_ledger)
    metadata = {
        **_base_metadata(boundary, source_receipts),
        **census_contract,
        "denominator_count": coverage.denominator_count,
        "label_method_id": label_method_id,
        "resolved_count": coverage.resolved_count,
        "unresolved_count": coverage.unresolved_count,
    }
    stage = publisher.create_stage("outcome_release")
    _write_canonical(
        stage / "outcomes.json",
        {
            "outcomes": [_outcome_payload(row) for row in outcomes],
            "schema_version": OUTCOME_SCHEMA_VERSION,
        },
    )
    _write_canonical(stage / "outcome_contract.json", metadata)
    causal_sources = [
        item.verify(boundary)
        for item in source_receipts
        if item.phase == "causally_gated_normalized"
    ]
    if len(causal_sources) != 1:
        raise ContractError("outcome publication requires one exact causal data release")
    causal_root = str(causal_sources[0].metadata.get("logical_root", ""))
    causal_prefix = "data/causally_gated_normalized/"
    if not causal_root.startswith(causal_prefix):
        raise IntegrityError("outcome release lacks a layout-v2 causal selector")
    outcome_root = (
        f"data/outcomes/{label_method_id}/"
        f"{causal_root.removeprefix(causal_prefix)}"
    )
    manifest = ReleaseManifest.build(
        stage,
        phase="outcomes",
        release_kind=OUTCOME_RELEASE_KIND,
        schema_version=OUTCOME_SCHEMA_VERSION,
        logical_paths={
            "outcome_contract.json": f"{outcome_root}/outcome_contract.json",
            "outcomes.json": f"{outcome_root}/outcomes.json",
        },
        source_release_ids=tuple(source.release_id for source in source_receipts),
        metadata=metadata,
    )
    manifest_path = publisher.publish(stage, manifest)
    receipt = VerifiedReleaseReceipt.from_manifest(manifest_path, boundary)
    load_outcome_release(
        receipt,
        prediction_census=prediction_census,
        prediction_ledger=prediction_ledger,
        source_receipts=source_receipts,
        boundary=boundary,
        expected_label_method_id=label_method_id,
    )
    return receipt


def load_outcome_release(
    receipt: VerifiedReleaseReceipt,
    *,
    prediction_census: PredictionCensusReceipt,
    prediction_ledger: PredictionLedger,
    source_receipts: tuple[VerifiedReleaseReceipt, ...],
    boundary: RepoBoundary,
    expected_label_method_id: str | None = None,
) -> LoadedOutcomeRelease:
    """Load only while the exact live prediction census still verifies."""

    manifest = receipt.verify(boundary)
    source_ids = _verified_outcome_sources(source_receipts, boundary)
    census_contract = _census_contract(prediction_census, prediction_ledger)
    contract = _read_canonical(
        receipt.resolve_unique_filename("outcome_contract.json", boundary)
    )
    payload = _read_canonical(
        receipt.resolve_unique_filename("outcomes.json", boundary)
    )
    if (
        not isinstance(contract, dict)
        or not isinstance(payload, dict)
        or set(payload) != {"outcomes", "schema_version"}
        or payload["schema_version"] != OUTCOME_SCHEMA_VERSION
        or not isinstance(payload["outcomes"], list)
    ):
        raise IntegrityError("outcome release payload schema is invalid")
    outcomes = tuple(_outcome_from_payload(item) for item in payload["outcomes"])
    _verify_outcome_prediction_join(
        outcomes, prediction_census, prediction_ledger
    )
    if any(row.source_release_id not in source_ids for row in outcomes):
        raise IntegrityError("outcome row uses an unverified source release")
    coverage = OutcomeCoverageReport(
        prediction_census, outcomes, prediction_ledger
    )
    expected_contract = {
        **_base_metadata(boundary, source_receipts),
        **census_contract,
        "denominator_count": coverage.denominator_count,
        "label_method_id": expected_label_method_id,
        "resolved_count": coverage.resolved_count,
        "unresolved_count": coverage.unresolved_count,
    }
    if (
        contract != expected_contract
        or receipt.phase != "outcomes"
        or manifest.release_kind != OUTCOME_RELEASE_KIND
        or manifest.schema_version != OUTCOME_SCHEMA_VERSION
        or {Path(entry.path).name for entry in manifest.files}
        != {"outcome_contract.json", "outcomes.json"}
        or manifest.source_release_ids
        != tuple(sorted(source.release_id for source in source_receipts))
        or dict(manifest.metadata) != expected_contract
    ):
        raise IntegrityError("outcome release provenance/census is invalid")
    return LoadedOutcomeRelease(
        receipt, outcomes, coverage, expected_label_method_id
    )


def publish_causal_outcome_release(
    *,
    prediction_census: PredictionCensusReceipt,
    prediction_ledger: PredictionLedger,
    context: CausalOutcomeContext,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    """Publish only outcomes reproducible from the exact verified causal context."""

    outcomes = generate_causal_outcomes(
        prediction_census=prediction_census,
        prediction_ledger=prediction_ledger,
        context=context,
        boundary=boundary,
    )
    return publish_outcome_release(
        outcomes=outcomes,
        prediction_census=prediction_census,
        prediction_ledger=prediction_ledger,
        source_receipts=(context.causal_receipt,),
        boundary=boundary,
        publisher=publisher,
        label_method_id=CAUSAL_OUTCOME_LABEL_METHOD_ID,
    )


def load_causal_outcome_release(
    receipt: VerifiedReleaseReceipt,
    *,
    prediction_census: PredictionCensusReceipt,
    prediction_ledger: PredictionLedger,
    context: CausalOutcomeContext,
    boundary: RepoBoundary,
) -> LoadedOutcomeRelease:
    """Reload and reproduce the deterministic label path before trusting outcomes."""

    loaded = load_outcome_release(
        receipt,
        prediction_census=prediction_census,
        prediction_ledger=prediction_ledger,
        source_receipts=(context.causal_receipt,),
        boundary=boundary,
        expected_label_method_id=CAUSAL_OUTCOME_LABEL_METHOD_ID,
    )
    expected = generate_causal_outcomes(
        prediction_census=prediction_census,
        prediction_ledger=prediction_ledger,
        context=context,
        boundary=boundary,
    )
    if loaded.outcomes != expected:
        raise IntegrityError(
            "outcome release differs from deterministic causal label generation"
        )
    return loaded
