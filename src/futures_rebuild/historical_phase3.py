"""Prediction-independent Phase 3 outcome contracts for registered research samples.

This module does not discover samples, fit models, access providers, or grant
real-history authority.  It labels one exact predeclared sample contract from
verified Phase 2 causal rows.  Real-history use requires both a registered
experiment charter and an already-consumed exact historical-run receipt.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Mapping

from .boundary import RepoBoundary
from .canonical import canonical_bytes, sha256_json
from .data_layout import (
    DataReleaseManifest as ReleaseManifest,
    DataReleaseReceipt as VerifiedReleaseReceipt,
    PhasePublisher as AtomicPublisher,
)
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .foundation.materialize import CAUSAL_RELEASE_KIND
from .foundation.records import datetime_to_ns, ns_to_datetime
from .historical_capability import AuthorizedHistoricalRun
from .identity import ActualContractIdentity
from .producer_bridge import (
    CAUSAL_OUTCOME_LABEL_METHOD_ID,
    CausalOutcomeContext,
    _assert_publisher,
    _iter_causal_rows,
    _RESOLVED_CAUSAL_DISPOSITIONS,
    _same_definition_basis,
    _tick_valid_price,
    _trust_actual_from_causal,
)
from .schemas import OutcomeStatus
from .time_contracts import require_utc
from .trial import ExperimentCharter


PHASE3_RELEASE_KIND = "historical_phase3_outcome_release"
PHASE3_SCHEMA_VERSION = "1.0.0"
SYNTHETIC_SOURCE_KIND = "SYNTHETIC_MECHANICS_ONLY"
REAL_SOURCE_KIND = "EXTERNALLY_AUTHORIZED_REAL_HISTORY"
_ONE_MINUTE_NS = 60_000_000_000
_SHA256 = re.compile(r"[0-9a-f]{64}")


def _hash(value: str, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ContractError(f"{name} must be an exact SHA-256 value")
    return value


@dataclass(frozen=True)
class Phase3Sample:
    """One pre-model decision row with explicit entry and label timestamps."""

    market: str
    actual: ActualContractIdentity
    decision_at: datetime
    planned_entry_at: datetime
    label_unlock_at: datetime
    source_feature_input_release_id: str

    def __post_init__(self) -> None:
        decision = require_utc(self.decision_at, "decision_at")
        entry = require_utc(self.planned_entry_at, "planned_entry_at")
        unlock = require_utc(self.label_unlock_at, "label_unlock_at")
        if (
            type(self.market) is not str
            or not self.market
            or not self.market.isascii()
            or type(self.actual) is not ActualContractIdentity
            or not decision < entry < unlock
        ):
            raise ContractError("Phase 3 sample identity or timing is invalid")
        _hash(self.source_feature_input_release_id, "feature input release ID")

    def core(self) -> dict[str, object]:
        return {
            "actual_contract": self.actual.as_dict(),
            "decision_at": self.decision_at.isoformat(),
            "label_unlock_at": self.label_unlock_at.isoformat(),
            "market": self.market,
            "planned_entry_at": self.planned_entry_at.isoformat(),
            "source_feature_input_release_id": self.source_feature_input_release_id,
        }

    @property
    def sample_id(self) -> str:
        return sha256_json(self.core())


@dataclass(frozen=True)
class Phase3SampleContract:
    """Exact, ordered denominator fixed before any Phase 3 outcome is read."""

    samples: tuple[Phase3Sample, ...]
    causal_release_id: str
    entry_delay_seconds: int
    label_horizon_seconds: int

    def __post_init__(self) -> None:
        _hash(self.causal_release_id, "causal release ID")
        if (
            type(self.samples) is not tuple
            or not self.samples
            or any(type(item) is not Phase3Sample for item in self.samples)
            or tuple(item.sample_id for item in self.samples)
            != tuple(sorted({item.sample_id for item in self.samples}))
            or type(self.entry_delay_seconds) is not int
            or self.entry_delay_seconds <= 0
            or type(self.label_horizon_seconds) is not int
            or self.label_horizon_seconds <= self.entry_delay_seconds
        ):
            raise ContractError("Phase 3 sample contract is invalid")
        for sample in self.samples:
            decision_ns = datetime_to_ns(sample.decision_at, "decision_at")
            entry_ns = datetime_to_ns(sample.planned_entry_at, "planned_entry_at")
            unlock_ns = datetime_to_ns(sample.label_unlock_at, "label_unlock_at")
            if (
                entry_ns - decision_ns != self.entry_delay_seconds * 1_000_000_000
                or unlock_ns - decision_ns
                != self.label_horizon_seconds * 1_000_000_000
            ):
                raise ContractError("Phase 3 sample timing differs from its frozen policy")

    def core(self) -> dict[str, object]:
        return {
            "causal_release_id": self.causal_release_id,
            "entry_delay_seconds": self.entry_delay_seconds,
            "label_horizon_seconds": self.label_horizon_seconds,
            "label_method_id": CAUSAL_OUTCOME_LABEL_METHOD_ID,
            "sample_ids": [item.sample_id for item in self.samples],
            "samples": [item.core() for item in self.samples],
        }

    @property
    def contract_id(self) -> str:
        return sha256_json(self.core())


@dataclass(frozen=True)
class Phase3Outcome:
    sample_id: str
    actual: ActualContractIdentity
    decision_at: datetime
    label_end_at: datetime
    matured_at: datetime
    source_release_id: str
    interval_contract_segment_hashes: tuple[str, ...]
    included_in_coverage_denominator: bool
    status: OutcomeStatus
    price_return: float | None

    def __post_init__(self) -> None:
        _hash(self.sample_id, "sample ID")
        _hash(self.source_release_id, "source release ID")
        decision = require_utc(self.decision_at, "decision_at")
        label_end = require_utc(self.label_end_at, "label_end_at")
        matured = require_utc(self.matured_at, "matured_at")
        crossed = len(set(self.interval_contract_segment_hashes)) != 1
        if (
            type(self.actual) is not ActualContractIdentity
            or not decision < label_end <= matured
            or not self.interval_contract_segment_hashes
            or any(_SHA256.fullmatch(item) is None for item in self.interval_contract_segment_hashes)
            or self.interval_contract_segment_hashes[0]
            != self.actual.contract_segment_hash
            or self.included_in_coverage_denominator is not True
            or type(self.status) is not OutcomeStatus
        ):
            raise ContractError("Phase 3 outcome identity, timing, or denominator is invalid")
        if crossed and self.status is not OutcomeStatus.ROLL_UNRESOLVED:
            raise ContractError("cross-contract Phase 3 outcome must remain unresolved")
        if self.status is OutcomeStatus.MATURED:
            if (
                self.price_return is None
                or isinstance(self.price_return, bool)
                or not isinstance(self.price_return, (int, float))
                or not math.isfinite(self.price_return)
            ):
                raise ContractError("matured Phase 3 outcomes require a finite return")
        elif self.price_return is not None:
            raise ContractError("unresolved Phase 3 outcomes cannot contain a return")

    def as_dict(self) -> dict[str, object]:
        return {
            "actual_contract": self.actual.as_dict(),
            "decision_at": self.decision_at.isoformat(),
            "included_in_coverage_denominator": self.included_in_coverage_denominator,
            "interval_contract_segment_hashes": list(
                self.interval_contract_segment_hashes
            ),
            "label_end_at": self.label_end_at.isoformat(),
            "matured_at": self.matured_at.isoformat(),
            "price_return": self.price_return,
            "sample_id": self.sample_id,
            "source_release_id": self.source_release_id,
            "status": self.status.value,
        }


@dataclass(frozen=True)
class Phase3OutcomeBatch:
    sample_contract_id: str
    source_kind: str
    trial_charter_id: str | None
    outcomes: tuple[Phase3Outcome, ...]
    source_causal_release_id: str

    def __post_init__(self) -> None:
        _hash(self.sample_contract_id, "sample contract ID")
        _hash(self.source_causal_release_id, "source causal release ID")
        if (
            self.source_kind not in {SYNTHETIC_SOURCE_KIND, REAL_SOURCE_KIND}
            or type(self.outcomes) is not tuple
            or not self.outcomes
            or any(type(item) is not Phase3Outcome for item in self.outcomes)
            or tuple(item.sample_id for item in self.outcomes)
            != tuple(sorted({item.sample_id for item in self.outcomes}))
            or any(
                item.source_release_id != self.source_causal_release_id
                for item in self.outcomes
            )
        ):
            raise ContractError("Phase 3 outcome batch is invalid")
        if self.source_kind == REAL_SOURCE_KIND:
            if self.trial_charter_id is None:
                raise ContractError("real Phase 3 outcomes require a trial charter")
            _hash(self.trial_charter_id, "trial charter ID")
        elif self.trial_charter_id is not None:
            raise ContractError("synthetic Phase 3 outcomes cannot claim a trial charter")

    @property
    def resolved_count(self) -> int:
        return sum(item.status is OutcomeStatus.MATURED for item in self.outcomes)

    def core(self) -> dict[str, object]:
        return {
            "denominator_count": len(self.outcomes),
            "label_method_id": CAUSAL_OUTCOME_LABEL_METHOD_ID,
            "outcomes": [item.as_dict() for item in self.outcomes],
            "resolved_count": self.resolved_count,
            "sample_contract_id": self.sample_contract_id,
            "source_causal_release_id": self.source_causal_release_id,
            "source_kind": self.source_kind,
            "trial_charter_id": self.trial_charter_id,
            "unresolved_count": len(self.outcomes) - self.resolved_count,
        }

    @property
    def batch_id(self) -> str:
        return sha256_json(self.core())


def _authorize(
    *,
    contract: Phase3SampleContract,
    source_kind: str,
    charter: ExperimentCharter | None,
    authorized_run: AuthorizedHistoricalRun | None,
    boundary: RepoBoundary,
) -> str | None:
    if source_kind == SYNTHETIC_SOURCE_KIND:
        if charter is not None or authorized_run is not None:
            raise UnauthorizedOperation("synthetic Phase 3 mechanics cannot claim real authority")
        return None
    if (
        source_kind != REAL_SOURCE_KIND
        or type(charter) is not ExperimentCharter
        or type(authorized_run) is not AuthorizedHistoricalRun
    ):
        raise UnauthorizedOperation("real Phase 3 outcomes lack exact historical authority")
    authorized_run.verify(boundary)
    if (
        charter.charter_id != authorized_run.trial_charter_id
        or charter.target_policy_hash != contract.contract_id
    ):
        raise UnauthorizedOperation("Phase 3 sample contract is not bound by the trial charter")
    return charter.charter_id


def _market_for_actual(actual: ActualContractIdentity, context: CausalOutcomeContext) -> str:
    matches: list[str] = []
    for bridged in context.definitions.by_provider_row.values():
        observation = context.definitions.registry.definitions[bridged.registry_row_id]
        candidate = ActualContractIdentity.from_definition(
            observation.definition,
            instrument_id_date_utc=actual.instrument_id_date_utc,
            exchange_session_date=actual.exchange_session_date,
        )
        if candidate == actual:
            matches.append(bridged.provider.market)
    if len(matches) != 1:
        raise ContractError("Phase 3 actual contract is absent or ambiguous")
    return matches[0]


def _label_sample(
    sample: Phase3Sample,
    *,
    grouped: Mapping[tuple[str, int], tuple[Mapping[str, object], ...]],
    context: CausalOutcomeContext,
) -> Phase3Outcome:
    if _market_for_actual(sample.actual, context) != sample.market:
        raise ContractError("Phase 3 sample market differs from verified definition")
    start_ns = datetime_to_ns(sample.planned_entry_at, "planned_entry_at")
    end_ns = datetime_to_ns(sample.label_unlock_at, "label_unlock_at")
    expected = (
        tuple(range(start_ns, end_ns + 1, _ONE_MINUTE_NS))
        if not start_ns % _ONE_MINUTE_NS and not end_ns % _ONE_MINUTE_NS and end_ns >= start_ns
        else ()
    )
    missing = not expected
    segments = [sample.actual.contract_segment_hash]
    available_times: list[datetime] = []
    prices: dict[int, int] = {}
    sample_economics = context.economics_registry.resolve(
        sample.actual, sample.decision_at
    )
    for event_ns in expected:
        candidates = grouped.get((sample.market, event_ns), ())
        if len(candidates) != 1:
            missing = True
        for row in candidates:
            available = ns_to_datetime(
                int(row.get("available_at_ns")), "outcome.available_at_ns"
            )
            available_times.append(available)
            if row.get("disposition") not in _RESOLVED_CAUSAL_DISPOSITIONS:
                missing = True
                continue
            actual, _, _ = _trust_actual_from_causal(
                row,
                context.definitions,
                context.policies,
                context.session_policy,
            )
            segments.append(actual.contract_segment_hash)
            if row.get("disposition") != "ELIGIBLE":
                missing = True
                continue
            economics = context.economics_registry.resolve(actual, available)
            price_nano = int(row.get("open_nano"))
            if (
                actual.contract_segment_hash != sample.actual.contract_segment_hash
                or not _same_definition_basis(actual, sample.actual)
                or actual.exchange_session_date != sample.actual.exchange_session_date
                or economics.tick_size != sample_economics.tick_size
                or economics.point_value != sample_economics.point_value
                or economics.tick_value != sample_economics.tick_value
                or economics.currency != sample_economics.currency
                or not _tick_valid_price(price_nano, economics.tick_size)
            ):
                missing = True
                continue
            prices[event_ns] = price_nano
    crossed = len(set(segments)) != 1
    matured_at = max((sample.label_unlock_at, *available_times))
    if crossed:
        status = OutcomeStatus.ROLL_UNRESOLVED
        price_return: float | None = None
    elif missing or set(prices) != set(expected):
        status = OutcomeStatus.MISSING_SOURCE
        price_return = None
    else:
        price_return = float(Decimal(prices[end_ns]) / Decimal(prices[start_ns]) - 1)
        if not math.isfinite(price_return):
            raise IntegrityError("Phase 3 outcome return is non-finite")
        status = OutcomeStatus.MATURED
    return Phase3Outcome(
        sample_id=sample.sample_id,
        actual=sample.actual,
        decision_at=sample.decision_at,
        label_end_at=sample.label_unlock_at,
        matured_at=matured_at,
        source_release_id=context.causal_receipt.release_id,
        interval_contract_segment_hashes=tuple(segments),
        included_in_coverage_denominator=True,
        status=status,
        price_return=price_return,
    )


def build_phase3_outcomes(
    *,
    contract: Phase3SampleContract,
    context: CausalOutcomeContext,
    boundary: RepoBoundary,
    source_kind: str,
    charter: ExperimentCharter | None = None,
    authorized_run: AuthorizedHistoricalRun | None = None,
) -> Phase3OutcomeBatch:
    """Label one exact sample denominator without fitting or reading predictions."""

    if type(contract) is not Phase3SampleContract or type(context) is not CausalOutcomeContext:
        raise ContractError("Phase 3 builder requires exact contracts")
    context.verify(boundary)
    if contract.causal_release_id != context.causal_receipt.release_id:
        raise ContractError("Phase 3 sample contract uses a different causal release")
    trial_charter_id = _authorize(
        contract=contract,
        source_kind=source_kind,
        charter=charter,
        authorized_run=authorized_run,
        boundary=boundary,
    )
    grouped: dict[tuple[str, int], list[Mapping[str, object]]] = {}
    for row in _iter_causal_rows(context.causal_receipt, boundary):
        market = row.get("market")
        event_ns = row.get("event_at_ns")
        if type(market) is not str or type(event_ns) is not int:
            raise IntegrityError("Phase 3 causal row market/event identity is invalid")
        grouped.setdefault((market, event_ns), []).append(row)
    frozen = {
        key: tuple(value) for key, value in sorted(grouped.items(), key=lambda item: item[0])
    }
    outcomes = tuple(
        _label_sample(sample, grouped=frozen, context=context)
        for sample in contract.samples
    )
    return Phase3OutcomeBatch(
        sample_contract_id=contract.contract_id,
        source_kind=source_kind,
        trial_charter_id=trial_charter_id,
        outcomes=outcomes,
        source_causal_release_id=context.causal_receipt.release_id,
    )


def publish_phase3_outcome_release(
    *,
    batch: Phase3OutcomeBatch,
    contract: Phase3SampleContract,
    context: CausalOutcomeContext,
    boundary: RepoBoundary,
    publisher: AtomicPublisher,
) -> VerifiedReleaseReceipt:
    """Publish one immutable batch after its exact authority-bound build."""

    _assert_publisher(boundary, publisher)
    context.verify(boundary)
    if (
        type(batch) is not Phase3OutcomeBatch
        or batch.sample_contract_id != contract.contract_id
        or batch.source_causal_release_id != context.causal_receipt.release_id
        or tuple(item.sample_id for item in batch.outcomes)
        != tuple(item.sample_id for item in contract.samples)
    ):
        raise ContractError("Phase 3 publication inputs are not the exact built batch")
    causal_manifest = context.causal_receipt.verify(boundary)
    causal_root = str(causal_manifest.metadata.get("logical_root", ""))
    prefix = "data/causally_gated_normalized/"
    if causal_manifest.release_kind != CAUSAL_RELEASE_KIND or not causal_root.startswith(prefix):
        raise IntegrityError("Phase 3 publication lacks an exact causal layout selector")
    payload = {
        "batch": batch.core(),
        "batch_id": batch.batch_id,
        "sample_contract": contract.core(),
        "schema_version": PHASE3_SCHEMA_VERSION,
    }
    metadata = {
        "batch_id": batch.batch_id,
        "denominator_count": len(batch.outcomes),
        "label_method_id": CAUSAL_OUTCOME_LABEL_METHOD_ID,
        "resolved_count": batch.resolved_count,
        "sample_contract_id": contract.contract_id,
        "source_kind": batch.source_kind,
        "trial_charter_id": batch.trial_charter_id,
        "unresolved_count": len(batch.outcomes) - batch.resolved_count,
    }
    root = f"data/outcomes/{CAUSAL_OUTCOME_LABEL_METHOD_ID}/{causal_root.removeprefix(prefix)}"
    stage = publisher.create_stage("historical_phase3_outcomes")
    name = "phase3_outcomes.json"
    (stage / name).write_bytes(canonical_bytes(payload) + b"\n")
    manifest = ReleaseManifest.build(
        stage,
        phase="outcomes",
        release_kind=PHASE3_RELEASE_KIND,
        schema_version=PHASE3_SCHEMA_VERSION,
        logical_paths={name: f"{root}/{name}"},
        source_release_ids=(context.causal_receipt.release_id,),
        metadata=metadata,
    )
    return VerifiedReleaseReceipt.from_manifest(
        publisher.publish(stage, manifest), boundary
    )


def load_phase3_outcome_release(
    receipt: VerifiedReleaseReceipt,
    *,
    expected_batch: Phase3OutcomeBatch,
    expected_contract: Phase3SampleContract,
    context: CausalOutcomeContext,
    boundary: RepoBoundary,
) -> dict[str, object]:
    """Verify one published Phase 3 batch against its in-memory postimage."""

    manifest = receipt.verify(boundary)
    context.verify(boundary)
    path = receipt.resolve_unique_filename("phase3_outcomes.json", boundary)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("Phase 3 outcome payload is invalid") from exc
    expected_payload = {
        "batch": expected_batch.core(),
        "batch_id": expected_batch.batch_id,
        "sample_contract": expected_contract.core(),
        "schema_version": PHASE3_SCHEMA_VERSION,
    }
    expected_metadata = {
        "batch_id": expected_batch.batch_id,
        "denominator_count": len(expected_batch.outcomes),
        "label_method_id": CAUSAL_OUTCOME_LABEL_METHOD_ID,
        "resolved_count": expected_batch.resolved_count,
        "sample_contract_id": expected_contract.contract_id,
        "source_kind": expected_batch.source_kind,
        "trial_charter_id": expected_batch.trial_charter_id,
        "unresolved_count": len(expected_batch.outcomes) - expected_batch.resolved_count,
    }
    if (
        canonical_bytes(payload) + b"\n" != path.read_bytes()
        or payload != expected_payload
        or receipt.phase != "outcomes"
        or manifest.release_kind != PHASE3_RELEASE_KIND
        or manifest.schema_version != PHASE3_SCHEMA_VERSION
        or manifest.source_release_ids != (context.causal_receipt.release_id,)
        or dict(manifest.metadata) != expected_metadata
    ):
        raise IntegrityError("Phase 3 outcome release provenance or content is invalid")
    return payload
