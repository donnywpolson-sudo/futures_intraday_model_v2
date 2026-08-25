"""Observation-only causal foundation producer and evidence contracts.

This module has no DBN decoder.  A separately authorized caller must obtain a
sealed one-use context before opening any exact source selected by
``causal_source_closure``.  Synthetic contexts are restricted to synthetic
lineage and cannot authorize real source rows.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import sha256_file, sha256_json
from .causal_full_build_durable_host import expected_durable_host_plan
from .causal_observation_parquet import FORMAT_VERSION, FILENAMES, write_bundle
from .data_layout import DataReleaseManifest, PhasePublisher
from .errors import ContractError, IntegrityError, UnauthorizedOperation
from .foundation.records import ProviderBar, exact_int, validate_timestamp_ns
from .research_gateway_policy import (
    CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION,
    CAUSAL_OBSERVATION_CANARY_OPERATION,
    CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
)


CAUSAL_OBSERVATION_CONTRACT_ID = (
    "a11f587644168555d23042b945799b16947723203e5a592af6451027d301bdc7"
)
CANARY_SOURCE_CONTRACT_ID = (
    "47ad7a1c100bec86494f3c1eb1e78ba56a4d35c6be993da6ded8e2e7f925823f"
)
CANARY_CANONICAL_RELEASE_ID = (
    "9867aedac9cfe732d015489fc4093ffc4aaab5ad698b75a5fa00ca7e1f457995"
)
ACTIVE_SOURCE_CONTRACT_ID = (
    "8af92cf4e250e025b61ffb51bc2677f340a4dd4f4f8d6f9825ed28b916dd43d1"
)
ACTIVE_CANONICAL_RELEASE_ID = (
    "4ca353d7814941782bb4c6640afe89b04371492868f57174bb10d632b6e7c9be"
)
ECONOMICS_RULEBOOK_PATH = "configs/contract_economics_rules.json"
ECONOMICS_RULEBOOK_SHA256 = (
    "6a43960f252dc9103ea39f5ef4d082a71aa3aeefe89370c528dc29ac319e0f33"
)
ECONOMICS_RULEBOOK_ID = (
    "83008522be3b959f3c08cc3a9f5ff4d55878210c0e23cff5ceb7bf650ba2ef68"
)
DEVELOPMENT_END_EXCLUSIVE = "2025-07-13T22:00:00Z"
V9_CHECKPOINT_MARKETS = (
    "ES", "GC", "6E", "CL", "NQ", "6A", "6B", "6C", "6J", "6M", "6N",
    "6S", "BTC", "ETH", "GF", "HE", "HG", "HO", "KE", "LE", "NG", "PA",
    "PL", "RB", "RTY", "SI", "SR1", "SR3", "TN", "UB", "YM", "ZB", "ZC",
    "ZF", "ZL", "ZM", "ZN", "ZQ", "ZS", "ZT", "ZW",
)
SYNTHETIC_RELEASE_ID = "0" * 64
RELEASE_KIND = "development_only_causal_observation_partition"
SCHEMA_VERSION = "causal_observation_partition/1.1.0"
EVIDENCE_SCHEMA_VERSION = "causal_observation_evidence/1.1.0"
_SEAL = object()
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MARKET = re.compile(r"^[0-9A-Z]{1,16}$")
_CADENCE = frozenset({"1s", "1m", "1h", "1d", "project_session_daily"})
MISSINGNESS_STATES = frozenset(
    {
        "OBSERVED_VALID",
        "NO_TRADE_EXPECTED",
        "MARKET_CLOSED",
        "HALTED_OR_PAUSED",
        "NOT_YET_LISTED",
        "ROLL_EXCLUDED",
        "SOURCE_UNAVAILABLE",
        "UNEXPECTED_GAP",
        "CORRUPT_OR_CONFLICTING",
        "UNKNOWN_FAIL_CLOSED",
    }
)
CADENCE_RESULTS = frozenset({"MATCH", "DISAGREEMENT", "NOT_COMPARABLE", "SOURCE_MISSING"})
FORBIDDEN_OUTPUT_FIELDS = frozenset(
    {
        "outcome",
        "target",
        "label",
        "feature",
        "fold",
        "prediction",
        "evaluation",
        "pnl",
        "return",
        "model",
        "promotion",
        "mechanism",
    }
)

OBSERVATION_FIELDS = frozenset(
    {
        "row_id",
        "market",
        "source_contract_id",
        "source_release_id",
        "source_file_path",
        "source_file_sha256",
        "source_row_sha256",
        "source_cadence",
        "bar_start_ns",
        "bar_end_ns",
        "source_timestamp_ns",
        "available_at_ns",
        "decision_eligible_at_ns",
        "publisher_id",
        "instrument_id",
        "raw_symbol",
        "actual_contract",
        "definition_source_file_path",
        "definition_source_file_sha256",
        "definition_row_sha256",
        "definition_event_at_ns",
        "definition_received_at_ns",
        "listing_activation_ns",
        "expiration_ns",
        "open_nano",
        "high_nano",
        "low_nano",
        "close_nano",
        "volume",
        "currency",
        "min_price_increment_nano",
        "multiplier_nano",
        "project_session_id",
        "project_trade_date",
        "project_grouping_start_ns",
        "project_grouping_end_ns",
        "project_timezone",
        "official_schedule_state",
    }
)


@dataclass(frozen=True, slots=True)
class CausalObservationOperationContext:
    operation: str
    classification: OperationClassification
    source_contract_id: str
    causal_contract_id: str
    source_release_id: str
    plan_id: str
    plan_sha256: str
    exact_source_entries_sha256: str
    economics_rulebook_sha256: str
    output_staging_path: str
    receipt_id: str
    synthetic: bool
    _seal: object


@dataclass(frozen=True, slots=True)
class PreparedObservationPartition:
    stage: Path
    manifest: DataReleaseManifest
    staged_paths: Mapping[str, str]


def _digest(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ContractError(f"{name} must be a SHA-256 identity")
    return value


def _canonical_path(value: object, name: str) -> str:
    if type(value) is not str or not value:
        raise ContractError(f"{name} must be a nonempty relative path")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise ContractError(f"{name} is not a canonical relative path")
    return value


def required_canary_scope(
    *,
    plan: Mapping[str, object],
    plan_sha256: str,
) -> dict[str, str]:
    """Return the exact future one-use receipt scope; this does not issue it."""

    source = plan.get("source")
    limits = plan.get("limits")
    authority = plan.get("authority")
    if (
        plan.get("schema_version") != "causal_observation_canary_operation/1.0.0"
        or plan.get("operation") != CAUSAL_OBSERVATION_CANARY_OPERATION
        or plan.get("causal_contract_id") != CAUSAL_OBSERVATION_CONTRACT_ID
        or not isinstance(source, Mapping)
        or not isinstance(limits, Mapping)
        or not isinstance(authority, Mapping)
        or source.get("source_contract_id") != CANARY_SOURCE_CONTRACT_ID
        or source.get("canonical_release_id") != CANARY_CANONICAL_RELEASE_ID
        or plan.get("development_end_exclusive") != DEVELOPMENT_END_EXCLUSIVE
        or plan.get("holdout_allowed") is not False
        or plan.get("forward_allowed") is not False
        or plan.get("provider_calls") != 0
        or plan.get("execution_authorized") is not False
        or any(bool(value) for value in authority.values())
    ):
        raise UnauthorizedOperation("causal-observation canary plan authority is invalid")
    plan_id = _digest(plan.get("plan_id"), "plan_id")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    if sha256_json(core) != plan_id:
        raise IntegrityError("causal-observation canary plan identity differs")
    return {
        "operation_kind": "DEVELOPMENT_CAUSAL_OBSERVATION_ONLY",
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "source_contract_id": CANARY_SOURCE_CONTRACT_ID,
        "canonical_release_id": CANARY_CANONICAL_RELEASE_ID,
        "exact_source_entries_sha256": _digest(
            source.get("exact_source_entries_sha256"), "exact_source_entries_sha256"
        ),
        "output_staging_path": _canonical_path(
            plan.get("output_staging_path"), "output_staging_path"
        ),
        "development_end_exclusive": DEVELOPMENT_END_EXCLUSIVE,
        "maximum_payload_bytes": str(exact_int(limits.get("maximum_payload_bytes"), "maximum_payload_bytes", nonnegative=True)),
        "maximum_decoded_records": str(exact_int(limits.get("maximum_decoded_records"), "maximum_decoded_records", nonnegative=True)),
        "maximum_output_bytes": str(exact_int(limits.get("maximum_output_bytes"), "maximum_output_bytes", nonnegative=True)),
        "provider_calls": "0",
        "holdout": "false",
        "forward": "false",
        "outcomes": "false",
        "features": "false",
        "wfa": "false",
        "fitting": "false",
        "prediction": "false",
        "evaluation": "false",
        "mechanism": "false",
        "publication": "false",
        "activation": "false",
        "approval_command": CAUSAL_OBSERVATION_CANARY_OPERATION,
        "approval_plan_id": plan_id,
        "approval_plan_sha256": _digest(plan_sha256, "plan_sha256"),
    }


def authorize_canary_row_read(
    *,
    boundary: RepoBoundary,
    receipt: OperationReceipt,
    plan: Mapping[str, object],
    plan_sha256: str,
) -> CausalObservationOperationContext:
    """Consume one exact user-approved claim before any source payload open."""

    scope = required_canary_scope(plan=plan, plan_sha256=plan_sha256)
    receipt.consume(
        boundary,
        operation=CAUSAL_OBSERVATION_CANARY_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    return CausalObservationOperationContext(
        operation=CAUSAL_OBSERVATION_CANARY_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        source_contract_id=CANARY_SOURCE_CONTRACT_ID,
        causal_contract_id=CAUSAL_OBSERVATION_CONTRACT_ID,
        source_release_id=CANARY_CANONICAL_RELEASE_ID,
        plan_id=str(plan["plan_id"]),
        plan_sha256=plan_sha256,
        exact_source_entries_sha256=str(plan["source"]["exact_source_entries_sha256"]),
        economics_rulebook_sha256=ECONOMICS_RULEBOOK_SHA256,
        output_staging_path=str(plan["output_staging_path"]),
        receipt_id=receipt.receipt_id,
        synthetic=False,
        _seal=_SEAL,
    )


def required_bounded_2025_smoke_scope(
    *,
    plan: Mapping[str, object],
    plan_sha256: str,
    source_contract_id: str,
    canonical_release_id: str,
) -> dict[str, str]:
    """Return the exact one-use scope for the bounded-2025 runtime smoke."""

    source = plan.get("source")
    limits = plan.get("limits")
    authority = plan.get("authority")
    execution = plan.get("execution")
    storage = plan.get("storage")
    if (
        plan.get("schema_version")
        != "bounded_2025_causal_observation_smoke_plan/1.0.0"
        or plan.get("operation")
        != CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION
        or plan.get("causal_contract_id") != CAUSAL_OBSERVATION_CONTRACT_ID
        or not isinstance(source, Mapping)
        or not isinstance(limits, Mapping)
        or not isinstance(authority, Mapping)
        or not isinstance(execution, Mapping)
        or not isinstance(storage, Mapping)
        or source.get("source_contract_id") != source_contract_id
        or source.get("canonical_release_id") != canonical_release_id
        or plan.get("development_end_exclusive") != DEVELOPMENT_END_EXCLUSIVE
        or plan.get("roots") != ["6A"]
        or plan.get("holdout_allowed") is not False
        or plan.get("forward_allowed") is not False
        or plan.get("provider_calls") != 0
        or plan.get("execution_authorized") is not False
        or any(bool(value) for value in authority.values())
        or execution
        != {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_runtime_seconds": 7_200,
            "maximum_workers": 1,
        }
        or storage.get("publication_authorized") is not False
        or storage.get("activation_authorized") is not False
    ):
        raise UnauthorizedOperation("bounded-2025 smoke plan authority is invalid")
    plan_id = _digest(plan.get("plan_id"), "plan_id")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    if sha256_json(core) != plan_id:
        raise IntegrityError("bounded-2025 smoke plan identity differs")
    return {
        "operation_kind": "BOUNDED_2025_CAUSAL_OBSERVATION_SMOKE_ONLY",
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "source_contract_id": _digest(source_contract_id, "source_contract_id"),
        "canonical_release_id": _digest(
            canonical_release_id, "canonical_release_id"
        ),
        "exact_source_entries_sha256": _digest(
            source.get("exact_source_entries_sha256"),
            "exact_source_entries_sha256",
        ),
        "economics_rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
        "output_staging_path": _canonical_path(
            plan.get("output_staging_path"), "output_staging_path"
        ),
        "development_end_exclusive": DEVELOPMENT_END_EXCLUSIVE,
        "maximum_payload_bytes": str(
            exact_int(
                limits.get("maximum_payload_bytes"),
                "maximum_payload_bytes",
                nonnegative=True,
            )
        ),
        "maximum_decoded_records": str(
            exact_int(
                limits.get("maximum_decoded_records"),
                "maximum_decoded_records",
                nonnegative=True,
            )
        ),
        "maximum_output_bytes": str(
            exact_int(
                limits.get("maximum_output_bytes"),
                "maximum_output_bytes",
                nonnegative=True,
            )
        ),
        "maximum_partition_count": str(
            exact_int(
                limits.get("maximum_partition_count"),
                "maximum_partition_count",
                nonnegative=True,
            )
        ),
        "maximum_runtime_seconds": "7200",
        "maximum_workers": "1",
        "maximum_attempts": "1",
        "maximum_retries": "0",
        "provider_calls": "0",
        "holdout": "false",
        "forward": "false",
        "outcomes": "false",
        "features": "false",
        "wfa": "false",
        "fitting": "false",
        "prediction": "false",
        "evaluation": "false",
        "mechanism": "false",
        "publication": "false",
        "activation": "false",
        "approval_command": CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION,
        "approval_plan_id": plan_id,
        "approval_plan_sha256": _digest(plan_sha256, "plan_sha256"),
    }


def _active_source_identity(boundary: RepoBoundary) -> tuple[str, str]:
    contract_path = boundary.active_root / "configs/source_contract.json"
    try:
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("active causal-observation source contract is invalid") from exc
    if not isinstance(contract, dict):
        raise IntegrityError("active causal-observation source contract is not an object")
    source_contract_id = contract.get("contract_id")
    source = contract.get("active_canonical_source")
    core = {key: value for key, value in contract.items() if key != "contract_id"}
    if (
        type(source_contract_id) is not str
        or sha256_json(core) != source_contract_id
        or not isinstance(source, Mapping)
        or type(source.get("release_id")) is not str
    ):
        raise IntegrityError("active causal-observation source identity differs")
    return source_contract_id, str(source["release_id"])


def authorize_bounded_2025_smoke_row_read(
    *,
    boundary: RepoBoundary,
    receipt: OperationReceipt,
    plan: Mapping[str, object],
    plan_sha256: str,
) -> CausalObservationOperationContext:
    """Consume one smoke approval before the first bounded-2025 DBN open."""

    source_contract_id, canonical_release_id = _active_source_identity(boundary)
    scope = required_bounded_2025_smoke_scope(
        plan=plan,
        plan_sha256=plan_sha256,
        source_contract_id=source_contract_id,
        canonical_release_id=canonical_release_id,
    )
    receipt.consume(
        boundary,
        operation=CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    return CausalObservationOperationContext(
        operation=CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        source_contract_id=source_contract_id,
        causal_contract_id=CAUSAL_OBSERVATION_CONTRACT_ID,
        source_release_id=canonical_release_id,
        plan_id=str(plan["plan_id"]),
        plan_sha256=plan_sha256,
        exact_source_entries_sha256=str(
            plan["source"]["exact_source_entries_sha256"]
        ),
        economics_rulebook_sha256=ECONOMICS_RULEBOOK_SHA256,
        output_staging_path=str(plan["output_staging_path"]),
        receipt_id=receipt.receipt_id,
        synthetic=False,
        _seal=_SEAL,
    )


def required_full_build_scope(
    *,
    plan: Mapping[str, object],
    plan_sha256: str,
    source_contract_id: str,
    canonical_release_id: str,
) -> dict[str, str]:
    """Return the sealed scope for a future full development-only build."""

    source = plan.get("source")
    limits = plan.get("limits")
    authority = plan.get("authority")
    execution = plan.get("execution")
    runtime_projection = plan.get("runtime_projection")
    storage = plan.get("storage")
    economics = plan.get("economics")
    if (
        plan.get("schema_version")
        != "development_causal_observation_full_build_plan/1.5.0"
        or plan.get("operation") != CAUSAL_OBSERVATION_FULL_BUILD_OPERATION
        or plan.get("causal_contract_id") != CAUSAL_OBSERVATION_CONTRACT_ID
        or not isinstance(source, Mapping)
        or not isinstance(limits, Mapping)
        or not isinstance(authority, Mapping)
        or not isinstance(execution, Mapping)
        or not isinstance(runtime_projection, Mapping)
        or not isinstance(storage, Mapping)
        or not isinstance(economics, Mapping)
        or source.get("source_contract_id") != source_contract_id
        or source.get("canonical_release_id") != canonical_release_id
        or plan.get("development_end_exclusive") != DEVELOPMENT_END_EXCLUSIVE
        or plan.get("holdout_allowed") is not False
        or plan.get("forward_allowed") is not False
        or plan.get("provider_calls") != 0
        or plan.get("execution_authorized") is not False
        or any(bool(value) for value in authority.values())
        or execution.get("maximum_workers") != 1
        or execution.get("maximum_attempts") != 1
        or execution.get("maximum_retries") != 0
        or execution.get("maximum_runtime_seconds") != 216_000
        or execution.get("priority_markets") != ["ES", "GC", "6E", "CL", "NQ"]
        or execution.get("remaining_order")
        != "MARKET_LEXICOGRAPHIC_THEN_YEAR_ASCENDING"
        or execution.get("python_executable") != ".venv/Scripts/python.exe"
        or execution.get("databento_version") != "0.78.0"
        or storage.get("publication_authorized") is not False
        or storage.get("activation_authorized") is not False
        or economics
        != {
            "rulebook_path": ECONOMICS_RULEBOOK_PATH,
            "rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
            "provider_null_fallback_only": True,
            "negative_or_contradictory_provider_value": "FAIL_CLOSED",
        }
    ):
        raise UnauthorizedOperation("full causal-observation build plan authority is invalid")
    plan_id = _digest(plan.get("plan_id"), "plan_id")
    core = {key: value for key, value in plan.items() if key != "plan_id"}
    if sha256_json(core) != plan_id:
        raise IntegrityError("full causal-observation build plan identity differs")
    return {
        "operation_kind": "FULL_DEVELOPMENT_CAUSAL_OBSERVATION_ONLY",
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "source_contract_id": _digest(source_contract_id, "source_contract_id"),
        "canonical_release_id": _digest(canonical_release_id, "canonical_release_id"),
        "exact_source_entries_sha256": _digest(
            source.get("exact_source_entries_sha256"),
            "exact_source_entries_sha256",
        ),
        "economics_rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
        "output_staging_path": _canonical_path(
            plan.get("output_staging_path"), "output_staging_path"
        ),
        "development_end_exclusive": DEVELOPMENT_END_EXCLUSIVE,
        "maximum_payload_bytes": str(
            exact_int(
                limits.get("maximum_payload_bytes"),
                "maximum_payload_bytes",
                nonnegative=True,
            )
        ),
        "maximum_decoded_records": str(
            exact_int(
                limits.get("maximum_decoded_records"),
                "maximum_decoded_records",
                nonnegative=True,
            )
        ),
        "maximum_output_bytes": str(
            exact_int(
                limits.get("maximum_output_bytes"),
                "maximum_output_bytes",
                nonnegative=True,
            )
        ),
        "maximum_partition_count": str(
            exact_int(
                limits.get("maximum_partition_count"),
                "maximum_partition_count",
                nonnegative=True,
            )
        ),
        "runtime_projection_id": _digest(
            runtime_projection.get("projection_id"), "runtime_projection_id"
        ),
        "runtime_projection_sha256": _digest(
            runtime_projection.get("sha256"), "runtime_projection_sha256"
        ),
        "maximum_runtime_seconds": "216000",
        "work_unit_priority_markets": "ES,GC,6E,CL,NQ",
        "remaining_work_unit_order": "MARKET_LEXICOGRAPHIC_THEN_YEAR_ASCENDING",
        "python_executable": ".venv/Scripts/python.exe",
        "databento_version": "0.78.0",
        "maximum_workers": "1",
        "maximum_attempts": "1",
        "maximum_retries": "0",
        "provider_calls": "0",
        "holdout": "false",
        "forward": "false",
        "outcomes": "false",
        "features": "false",
        "wfa": "false",
        "fitting": "false",
        "prediction": "false",
        "evaluation": "false",
        "mechanism": "false",
        "publication": "false",
        "activation": "false",
        "approval_command": CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        "approval_plan_id": plan_id,
        "approval_plan_sha256": _digest(plan_sha256, "plan_sha256"),
    }


def authorize_full_build_row_read(
    *,
    boundary: RepoBoundary,
    receipt: OperationReceipt,
    plan: Mapping[str, object],
    plan_sha256: str,
) -> CausalObservationOperationContext:
    """Consume one exact full-build approval before the first DBN open."""

    source_contract_id, canonical_release_id = _active_source_identity(boundary)
    scope = required_full_build_scope(
        plan=plan,
        plan_sha256=plan_sha256,
        source_contract_id=source_contract_id,
        canonical_release_id=canonical_release_id,
    )
    receipt.consume(
        boundary,
        operation=CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    return CausalObservationOperationContext(
        operation=CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        source_contract_id=source_contract_id,
        causal_contract_id=CAUSAL_OBSERVATION_CONTRACT_ID,
        source_release_id=canonical_release_id,
        plan_id=str(plan["plan_id"]),
        plan_sha256=plan_sha256,
        exact_source_entries_sha256=str(plan["source"]["exact_source_entries_sha256"]),
        economics_rulebook_sha256=ECONOMICS_RULEBOOK_SHA256,
        output_staging_path=str(plan["output_staging_path"]),
        receipt_id=receipt.receipt_id,
        synthetic=False,
        _seal=_SEAL,
    )


def required_market_checkpoint_scope(
    *,
    plan: Mapping[str, object],
    plan_sha256: str,
    source_contract_id: str,
    canonical_release_id: str,
) -> dict[str, str]:
    """Seal one complete-market V9 checkpoint independently of other markets."""

    market = plan.get("target_market")
    attempt_id = plan.get("attempt_id")
    source = plan.get("source")
    limits = plan.get("limits")
    execution = plan.get("execution")
    authority = plan.get("authority")
    if (
        plan.get("schema_version")
        != "development_causal_observation_market_checkpoint_plan/1.0.0"
        or plan.get("operation") != CAUSAL_OBSERVATION_FULL_BUILD_OPERATION
        or plan.get("causal_contract_id") != CAUSAL_OBSERVATION_CONTRACT_ID
        or type(market) is not str
        or market not in V9_CHECKPOINT_MARKETS
        or type(attempt_id) is not str
        or not isinstance(source, Mapping)
        or not isinstance(limits, Mapping)
        or not isinstance(execution, Mapping)
        or not isinstance(authority, Mapping)
        or any(bool(value) for value in authority.values())
        or plan.get("durable_host") != expected_durable_host_plan(market, attempt_id)
        or source.get("source_contract_id") != source_contract_id
        or source.get("canonical_release_id") != canonical_release_id
        or plan.get("development_end_exclusive") != DEVELOPMENT_END_EXCLUSIVE
        or plan.get("holdout_allowed") is not False
        or plan.get("forward_allowed") is not False
        or plan.get("provider_calls") != 0
        or plan.get("execution_authorized") is not False
        or execution
        != {
            "maximum_attempts": 1,
            "maximum_retries": 0,
            "maximum_runtime_seconds": 216_000,
            "maximum_workers": 1,
            "python_executable": ".venv/Scripts/python.exe",
            "databento_version": "0.78.0",
        }
        or plan.get("output_staging_path")
        != (
            "state/data_publication_staging/"
            f"causal_observation_full_development_bounded_2025_v9/{market}/{attempt_id}"
        )
        or any(
            type(source.get(name)) is not int or int(source[name]) <= 0
            for name in (
                "exact_source_entry_count",
                "exact_dbn_file_count",
                "exact_sidecar_file_count",
                "total_source_bytes",
                "maximum_payload_bytes",
                "work_unit_count",
            )
        )
        or source.get("exact_source_entry_count")
        != source.get("exact_dbn_file_count") + source.get("exact_sidecar_file_count")
    ):
        raise UnauthorizedOperation("market-checkpoint plan authority is invalid")
    plan_id = _digest(plan.get("plan_id"), "plan_id")
    attempt_id = _digest(attempt_id, "attempt_id")
    checkpoint_set_id = _digest(plan.get("checkpoint_set_id"), "checkpoint_set_id")
    if sha256_json({key: value for key, value in plan.items() if key != "plan_id"}) != plan_id:
        raise IntegrityError("market-checkpoint plan identity differs")
    return {
        "operation_kind": "COMPLETE_MARKET_CAUSAL_OBSERVATION_CHECKPOINT_ONLY",
        "target_market": market,
        "attempt_id": attempt_id,
        "checkpoint_set_id": checkpoint_set_id,
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "source_contract_id": _digest(source_contract_id, "source_contract_id"),
        "canonical_release_id": _digest(canonical_release_id, "canonical_release_id"),
        "exact_source_entries_sha256": _digest(
            source.get("exact_source_entries_sha256"), "exact_source_entries_sha256"
        ),
        "exact_dbn_entries_sha256": _digest(
            source.get("exact_dbn_entries_sha256"), "exact_dbn_entries_sha256"
        ),
        "exact_source_entry_count": str(
            exact_int(source.get("exact_source_entry_count"), "exact_source_entry_count")
        ),
        "exact_dbn_file_count": str(
            exact_int(source.get("exact_dbn_file_count"), "exact_dbn_file_count")
        ),
        "exact_sidecar_file_count": str(
            exact_int(source.get("exact_sidecar_file_count"), "exact_sidecar_file_count")
        ),
        "total_source_bytes": str(
            exact_int(source.get("total_source_bytes"), "total_source_bytes")
        ),
        "work_unit_count": str(
            exact_int(source.get("work_unit_count"), "work_unit_count")
        ),
        "maximum_payload_bytes": str(
            exact_int(limits.get("maximum_payload_bytes"), "maximum_payload_bytes")
        ),
        "maximum_decoded_records": str(
            exact_int(limits.get("maximum_decoded_records"), "maximum_decoded_records")
        ),
        "maximum_output_bytes": str(
            exact_int(limits.get("maximum_output_bytes"), "maximum_output_bytes")
        ),
        "maximum_partition_count": str(
            exact_int(limits.get("maximum_partition_count"), "maximum_partition_count")
        ),
        "output_staging_path": _canonical_path(
            plan.get("output_staging_path"), "output_staging_path"
        ),
        "development_end_exclusive": DEVELOPMENT_END_EXCLUSIVE,
        "economics_rulebook_sha256": ECONOMICS_RULEBOOK_SHA256,
        "durable_host_kind": str(plan["durable_host"]["kind"]),
        "durable_host_task_name": str(plan["durable_host"]["task_name"]),
        "durable_host_evidence_path": str(plan["durable_host"]["evidence_path"]),
        "maximum_runtime_seconds": "216000",
        "maximum_workers": "1",
        "maximum_attempts": "1",
        "maximum_retries": "0",
        "python_executable": ".venv/Scripts/python.exe",
        "databento_version": "0.78.0",
        "provider_calls": "0",
        "holdout": "false",
        "forward": "false",
        "outcomes": "false",
        "features": "false",
        "wfa": "false",
        "fitting": "false",
        "prediction": "false",
        "evaluation": "false",
        "mechanism": "false",
        "publication": "false",
        "activation": "false",
        "approval_command": CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        "approval_plan_id": plan_id,
        "approval_plan_sha256": _digest(plan_sha256, "plan_sha256"),
    }


def authorize_market_checkpoint_row_read(
    *,
    boundary: RepoBoundary,
    receipt: OperationReceipt,
    plan: Mapping[str, object],
    plan_sha256: str,
) -> CausalObservationOperationContext:
    """Consume one receipt for one complete market, never for another market."""

    source_contract_id, canonical_release_id = _active_source_identity(boundary)
    scope = required_market_checkpoint_scope(
        plan=plan,
        plan_sha256=plan_sha256,
        source_contract_id=source_contract_id,
        canonical_release_id=canonical_release_id,
    )
    receipt.consume(
        boundary,
        operation=CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    return CausalObservationOperationContext(
        operation=CAUSAL_OBSERVATION_FULL_BUILD_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        source_contract_id=source_contract_id,
        causal_contract_id=CAUSAL_OBSERVATION_CONTRACT_ID,
        source_release_id=canonical_release_id,
        plan_id=str(plan["plan_id"]),
        plan_sha256=plan_sha256,
        exact_source_entries_sha256=str(plan["source"]["exact_source_entries_sha256"]),
        economics_rulebook_sha256=ECONOMICS_RULEBOOK_SHA256,
        output_staging_path=str(plan["output_staging_path"]),
        receipt_id=receipt.receipt_id,
        synthetic=False,
        _seal=_SEAL,
    )


def issue_synthetic_observation_context(
    *, boundary: RepoBoundary, fixture_id: str
) -> CausalObservationOperationContext:
    """Issue an isolated synthetic-only context with no real-source authority."""

    fixture = _digest(fixture_id, "fixture_id")
    scope = {
        "causal_contract_id": CAUSAL_OBSERVATION_CONTRACT_ID,
        "fixture_id": fixture,
        "source_release_id": SYNTHETIC_RELEASE_ID,
        "source_scope": "SYNTHETIC_ONLY",
    }
    receipt = OperationReceipt.issue_local(
        boundary,
        operation="BUILD_SYNTHETIC_CAUSAL_OBSERVATION_FIXTURE",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
        scope=scope,
    )
    receipt.verify(
        boundary,
        operation="BUILD_SYNTHETIC_CAUSAL_OBSERVATION_FIXTURE",
        classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
        required_scope=scope,
    )
    return CausalObservationOperationContext(
        operation=receipt.operation,
        classification=receipt.classification,
        source_contract_id=ACTIVE_SOURCE_CONTRACT_ID,
        causal_contract_id=CAUSAL_OBSERVATION_CONTRACT_ID,
        source_release_id=SYNTHETIC_RELEASE_ID,
        plan_id=fixture,
        plan_sha256=fixture,
        exact_source_entries_sha256=fixture,
        economics_rulebook_sha256=ECONOMICS_RULEBOOK_SHA256,
        output_staging_path="synthetic/fixture",
        receipt_id=receipt.receipt_id,
        synthetic=True,
        _seal=_SEAL,
    )


def _require_context(context: CausalObservationOperationContext) -> None:
    identity_valid = False
    if type(context) is CausalObservationOperationContext:
        if context.synthetic:
            identity_valid = (
                context.classification is OperationClassification.SYNTHETIC_MECHANICS_ONLY
                and context.source_contract_id == ACTIVE_SOURCE_CONTRACT_ID
                and context.source_release_id == SYNTHETIC_RELEASE_ID
            )
        elif context.operation == CAUSAL_OBSERVATION_CANARY_OPERATION:
            identity_valid = (
                context.classification
                is OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION
                and context.source_contract_id == CANARY_SOURCE_CONTRACT_ID
                and context.source_release_id == CANARY_CANONICAL_RELEASE_ID
            )
        elif context.operation == CAUSAL_OBSERVATION_BOUNDED_2025_SMOKE_OPERATION:
            identity_valid = (
                context.classification
                is OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION
                and _SHA256.fullmatch(context.source_contract_id) is not None
                and _SHA256.fullmatch(context.source_release_id) is not None
                and context.source_release_id != SYNTHETIC_RELEASE_ID
            )
        elif context.operation == CAUSAL_OBSERVATION_FULL_BUILD_OPERATION:
            identity_valid = (
                context.classification
                is OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION
                and _SHA256.fullmatch(context.source_contract_id) is not None
                and _SHA256.fullmatch(context.source_release_id) is not None
                and context.source_release_id != SYNTHETIC_RELEASE_ID
            )
    if (
        type(context) is not CausalObservationOperationContext
        or context._seal is not _SEAL
        or context.causal_contract_id != CAUSAL_OBSERVATION_CONTRACT_ID
        or context.economics_rulebook_sha256 != ECONOMICS_RULEBOOK_SHA256
        or not identity_valid
    ):
        raise UnauthorizedOperation("causal-observation operation context is absent or invalid")


def _validate_observation(row: Mapping[str, object], context: CausalObservationOperationContext) -> dict[str, object]:
    if set(row) != OBSERVATION_FIELDS or FORBIDDEN_OUTPUT_FIELDS & set(row):
        raise ContractError("causal observation schema is not exact")
    market = str(row["market"])
    cadence = str(row["source_cadence"])
    if _MARKET.fullmatch(market) is None or cadence not in _CADENCE:
        raise ContractError("causal observation market or cadence is invalid")
    if row["source_contract_id"] != context.source_contract_id or row["source_release_id"] != context.source_release_id:
        raise UnauthorizedOperation("causal observation source authority differs")
    path = _canonical_path(row["source_file_path"], "source_file_path")
    definition_path = _canonical_path(
        row["definition_source_file_path"], "definition_source_file_path"
    )
    if context.synthetic and not path.startswith("synthetic/"):
        raise UnauthorizedOperation("synthetic context cannot admit a real source path")
    if context.synthetic and not definition_path.startswith("synthetic/"):
        raise UnauthorizedOperation("synthetic context cannot admit a real definition path")
    for name in (
        "row_id", "source_file_sha256", "source_row_sha256",
        "definition_source_file_sha256", "definition_row_sha256",
    ):
        _digest(row[name], name)
    start = validate_timestamp_ns(row["bar_start_ns"], "bar_start_ns")
    end = validate_timestamp_ns(row["bar_end_ns"], "bar_end_ns")
    source_at = validate_timestamp_ns(row["source_timestamp_ns"], "source_timestamp_ns")
    available = validate_timestamp_ns(row["available_at_ns"], "available_at_ns")
    eligible = validate_timestamp_ns(row["decision_eligible_at_ns"], "decision_eligible_at_ns")
    definition_event = validate_timestamp_ns(
        row["definition_event_at_ns"], "definition_event_at_ns"
    )
    definition_received = validate_timestamp_ns(
        row["definition_received_at_ns"], "definition_received_at_ns"
    )
    activation = exact_int(
        row["listing_activation_ns"], "listing_activation_ns", nonnegative=True
    )
    expiration = exact_int(row["expiration_ns"], "expiration_ns", nonnegative=True)
    if not start < end <= available <= eligible or not start <= source_at <= end:
        raise ContractError("causal observation timing order is invalid")
    if (
        definition_event > available
        or definition_received > available
        or (activation not in {0, 2**64 - 1} and activation > source_at)
        or (expiration not in {0, 2**64 - 1} and expiration <= source_at)
    ):
        raise ContractError("point-in-time definition is unavailable or inactive")
    provider = ProviderBar(
        dataset="GLBX.MDP3",
        market=market,
        publisher_id=row["publisher_id"],
        instrument_id=row["instrument_id"],
        event_at_ns=source_at,
        open_nano=row["open_nano"],
        high_nano=row["high_nano"],
        low_nano=row["low_nano"],
        close_nano=row["close_nano"],
        volume=row["volume"],
        source_release_id=str(row["source_release_id"]),
        source_manifest_sha256=str(row["source_contract_id"]),
        source_file_path=path,
        source_file_sha256=str(row["source_file_sha256"]),
        row_sha256=str(row["source_row_sha256"]),
    )
    del provider
    if (
        type(row["raw_symbol"]) is not str
        or not row["raw_symbol"]
        or type(row["actual_contract"]) is not str
        or not row["actual_contract"]
        or row["currency"] != "USD"
    ):
        raise ContractError("causal observation contract identity is invalid")
    exact_int(row["min_price_increment_nano"], "min_price_increment_nano", nonnegative=True)
    exact_int(row["multiplier_nano"], "multiplier_nano", nonnegative=True)
    if row["min_price_increment_nano"] <= 0 or row["multiplier_nano"] <= 0:
        raise ContractError("causal observation scaling is invalid")
    grouping_start = validate_timestamp_ns(
        row["project_grouping_start_ns"], "project_grouping_start_ns"
    )
    grouping_end = validate_timestamp_ns(
        row["project_grouping_end_ns"], "project_grouping_end_ns"
    )
    if (
        not grouping_start <= start < end <= grouping_end
        or row["project_timezone"] != "America/Chicago"
        or type(row["project_session_id"]) is not str
        or not row["project_session_id"]
        or type(row["project_trade_date"]) is not str
        or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", row["project_trade_date"]) is None
        or row["official_schedule_state"]
        not in {"AUTHORITATIVE_APPLICABLE", "AUTHORITATIVE_CLOSED", "UNKNOWN_FAIL_CLOSED"}
    ):
        raise ContractError("project grouping or official schedule state is invalid")
    return dict(row)


def _validate_missingness(row: Mapping[str, object]) -> dict[str, object]:
    expected = {
        "evidence_id", "observation_row_id", "market", "interval_start_ns",
        "interval_end_ns", "state", "authority", "evidence_sha256",
    }
    if set(row) != expected or row.get("state") not in MISSINGNESS_STATES:
        raise ContractError("missingness evidence is invalid")
    _digest(row["evidence_id"], "missingness.evidence_id")
    _digest(row["evidence_sha256"], "missingness.evidence_sha256")
    if row["observation_row_id"] is not None:
        _digest(row["observation_row_id"], "missingness.observation_row_id")
    start = validate_timestamp_ns(row["interval_start_ns"], "missingness.interval_start_ns")
    end = validate_timestamp_ns(row["interval_end_ns"], "missingness.interval_end_ns")
    if (
        not start < end
        or _MARKET.fullmatch(str(row["market"])) is None
        or (row["state"] == "OBSERVED_VALID") != (row["observation_row_id"] is not None)
    ):
        raise ContractError("missingness interval or observation binding is invalid")
    if type(row["authority"]) is not str or not row["authority"]:
        raise ContractError("missingness authority is absent")
    if row["state"] in {"NO_TRADE_EXPECTED", "MARKET_CLOSED"} and row["authority"] in {"NONE", "UNKNOWN", "OBSERVED_ABSENCE"}:
        raise UnauthorizedOperation("missing rows cannot imply an expected closure")
    return dict(row)


def _validate_roll(row: Mapping[str, object]) -> dict[str, object]:
    expected = {
        "row_id", "actual_contract_before", "actual_contract_after", "effective_time_ns",
        "causal_selection_evidence_sha256", "roll_flag", "price_discontinuity_flag",
        "crossing_status",
    }
    if set(row) != expected:
        raise ContractError("roll evidence is invalid")
    _digest(row["row_id"], "roll.row_id")
    _digest(row["causal_selection_evidence_sha256"], "roll.causal_selection_evidence_sha256")
    if not all(type(row[name]) is str and row[name] for name in ("actual_contract_before", "actual_contract_after", "crossing_status")):
        raise ContractError("roll contract identity is absent")
    if type(row["roll_flag"]) is not bool or type(row["price_discontinuity_flag"]) is not bool:
        raise ContractError("roll flags are invalid")
    if row["effective_time_ns"] is not None:
        validate_timestamp_ns(row["effective_time_ns"], "roll.effective_time_ns")
    if not row["roll_flag"] and (
        row["actual_contract_before"] != row["actual_contract_after"]
        or row["price_discontinuity_flag"]
    ):
        raise ContractError("non-roll evidence contains a contract discontinuity")
    return dict(row)


def _validate_quality(row: Mapping[str, object], context: CausalObservationOperationContext) -> dict[str, object]:
    expected = {
        "row_id", "row_identity_sha256", "ohlc_valid", "volume_valid",
        "timestamp_order_valid", "duplicate_state", "source_contract_id",
        "source_release_id", "source_file_sha256", "quality_flags",
    }
    if set(row) != expected:
        raise ContractError("quality evidence is invalid")
    for name in ("row_id", "row_identity_sha256", "source_file_sha256"):
        _digest(row[name], f"quality.{name}")
    if row["source_contract_id"] != context.source_contract_id or row["source_release_id"] != context.source_release_id:
        raise UnauthorizedOperation("quality evidence source binding differs")
    if any(type(row[name]) is not bool for name in ("ohlc_valid", "volume_valid", "timestamp_order_valid")):
        raise ContractError("quality invariant flags are invalid")
    if row["duplicate_state"] not in {"UNIQUE", "DUPLICATE_IDENTICAL", "DUPLICATE_CONFLICT"}:
        raise ContractError("quality duplicate state is invalid")
    if not isinstance(row["quality_flags"], list) or any(type(value) is not str or not value for value in row["quality_flags"]):
        raise ContractError("quality flags are invalid")
    return dict(row)


def _validate_cadence(row: Mapping[str, object]) -> dict[str, object]:
    expected = {
        "comparison_id", "row_id", "source_cadence", "comparison_cadence",
        "interval_boundary_compatible", "result", "exception_state",
    }
    if set(row) != expected:
        raise ContractError("cadence evidence is invalid")
    _digest(row["comparison_id"], "cadence.comparison_id")
    _digest(row["row_id"], "cadence.row_id")
    if row["source_cadence"] not in _CADENCE or row["comparison_cadence"] not in _CADENCE:
        raise ContractError("cadence identity is invalid")
    if type(row["interval_boundary_compatible"]) is not bool or row["result"] not in CADENCE_RESULTS:
        raise ContractError("cadence comparison result is invalid")
    if type(row["exception_state"]) is not str or not row["exception_state"]:
        raise ContractError("cadence exception state is absent")
    if row["result"] != "MATCH" and row["exception_state"] == "NONE":
        raise ContractError("cadence disagreement lacks an explicit exception")
    return dict(row)


def prepare_observation_partition(
    *,
    publisher: PhasePublisher,
    context: CausalObservationOperationContext,
    market: str,
    year: int,
    interval: str,
    observations: Iterable[Mapping[str, object]],
    missingness: Iterable[Mapping[str, object]],
    rolls: Iterable[Mapping[str, object]],
    quality: Iterable[Mapping[str, object]],
    cadence: Iterable[Mapping[str, object]],
) -> PreparedObservationPartition:
    """Prepare one create-only candidate stage; never publish or activate it."""

    _require_context(context)
    if _MARKET.fullmatch(market) is None or isinstance(year, bool) or not isinstance(year, int) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{4}-[0-9]{2}-[0-9]{2}", interval):
        raise ContractError("causal observation partition selector is invalid")
    observation_rows = tuple(_validate_observation(row, context) for row in observations)
    missingness_rows = tuple(_validate_missingness(row) for row in missingness)
    roll_rows = tuple(_validate_roll(row) for row in rolls)
    quality_rows = tuple(_validate_quality(row, context) for row in quality)
    cadence_rows = tuple(_validate_cadence(row) for row in cadence)
    order = tuple((str(row["market"]), int(row["bar_start_ns"]), str(row["row_id"])) for row in observation_rows)
    if not observation_rows or order != tuple(sorted(order)) or len({row["row_id"] for row in observation_rows}) != len(observation_rows):
        raise IntegrityError("causal observation rows are empty, unordered, or duplicate")
    row_ids = {str(row["row_id"]) for row in observation_rows}
    observed_missingness_ids = [
        str(row["observation_row_id"])
        for row in missingness_rows
        if row["observation_row_id"] is not None
    ]
    evidence_ids = [str(row["evidence_id"]) for row in missingness_rows]
    if (
        set(observed_missingness_ids) != row_ids
        or len(observed_missingness_ids) != len(row_ids)
        or len(evidence_ids) != len(set(evidence_ids))
    ):
        raise IntegrityError("missingness ledger does not cover observations or has duplicate evidence")
    for name, rows in (("roll", roll_rows), ("quality", quality_rows)):
        ids = [str(row["row_id"]) for row in rows]
        if set(ids) != row_ids or len(ids) != len(row_ids):
            raise IntegrityError(f"{name} ledger does not cover every causal observation exactly once")
    if any(str(row["row_id"]) not in row_ids for row in cadence_rows):
        raise IntegrityError("cadence ledger references an unknown causal observation")

    stage = publisher.create_stage("causal_observation")
    tables = {
        "observations": observation_rows,
        "missingness": missingness_rows,
        "roll": roll_rows,
        "quality": quality_rows,
        "cadence": cadence_rows,
    }
    write_bundle(stage / "candidate", tables=tables)
    logical_paths: dict[str, str] = {}
    staged_paths: dict[str, str] = {}
    for filename in FILENAMES.values():
        relative = f"candidate/{filename}"
        logical = f"data/causally_gated_normalized/{market}/{year}/{interval}/{filename}"
        logical_paths[relative] = logical
        staged_paths[logical] = relative
    metadata = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "storage_format": FORMAT_VERSION,
        "compression": "zstd-9",
        "deterministic_identity_columns_reconstructed": True,
        "causal_contract_id": context.causal_contract_id,
        "source_contract_id": context.source_contract_id,
        "source_release_id": context.source_release_id,
        "plan_id": context.plan_id,
        "plan_sha256": context.plan_sha256,
        "exact_source_entries_sha256": context.exact_source_entries_sha256,
        "economics_rulebook_sha256": context.economics_rulebook_sha256,
        "economics_rulebook_id": ECONOMICS_RULEBOOK_ID,
        "observation_count": len(observation_rows),
        "missingness_count": len(missingness_rows),
        "roll_count": len(roll_rows),
        "quality_count": len(quality_rows),
        "cadence_comparison_count": len(cadence_rows),
        "outcome_count": 0,
        "feature_count": 0,
        "prediction_count": 0,
        "evaluation_count": 0,
        "publication_authorized": False,
        "activation_authorized": False,
    }
    manifest = DataReleaseManifest.build(
        stage,
        phase="causally_gated_normalized",
        release_kind=RELEASE_KIND,
        schema_version=SCHEMA_VERSION,
        logical_paths=logical_paths,
        source_release_ids=(context.source_release_id,),
        metadata=metadata,
    )
    return PreparedObservationPartition(stage=stage, manifest=manifest, staged_paths=staged_paths)


def publish_prepared_observation_partition(
    prepared: PreparedObservationPartition,
    *,
    publisher: PhasePublisher,
    context: CausalObservationOperationContext,
) -> Path:
    """Publish only when a separately supplied publisher receipt authorizes it."""

    _require_context(context)
    if prepared.manifest.metadata.get("publication_authorized") is not True:
        raise UnauthorizedOperation("prepared causal observation candidate is not publication-authorized")
    return publisher.publish(
        prepared.stage,
        prepared.manifest,
        staged_paths=prepared.staged_paths,
    )


def prepared_inventory(prepared: PreparedObservationPartition) -> dict[str, object]:
    """Return deterministic candidate inventory without treating it as accepted."""

    files = [entry.as_dict() for entry in prepared.manifest.files]
    return {
        "release_id": prepared.manifest.release_id,
        "manifest": prepared.manifest.as_dict(),
        "files": files,
        "files_sha256": sha256_json(files),
        "stage_file_sha256": {
            relative: sha256_file(prepared.stage / relative)
            for relative in sorted(prepared.staged_paths.values())
        },
        "producer_success_is_not_certification": True,
    }
