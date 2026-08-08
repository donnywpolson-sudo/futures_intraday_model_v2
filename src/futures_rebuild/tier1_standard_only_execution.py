"""Fail-closed execution boundary for the registered Standard-only Tier 1 trial."""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path

from scripts.run_v12_local_source_alternative_census import _candidate_path, _catalog

from . import tier1_bracket_v5 as v5
from . import tier1_bracket_v10 as v10
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .runtime_environment import require_locked_repository_environment
from .tier1_frozen_source_adequacy_census import _load_selected_sources
from .tier1_frozen_trial_pipeline import run_frozen_trial_pipeline
from .tier1_standard_only_lifecycle import (
    ACTIVE_POINTER_PATH, CERTIFICATE_ROOT, TRIAL_REGISTRY_ROOT,
)
from .tier1_standard_only_protocol import load_standard_only_protocol


PLAN_PATH = Path("configs/tier1_standard_only_historical_execution_plan.json")
OPERATION = "EXECUTE_TIER1_STANDARD_ONLY_HISTORICAL_SCREEN"
OUTPUT_ROOT = Path("state/tier1_standard_only_unpublished")
MARKETS = ("6E", "CL", "ES", "ZN")
YEARS = tuple(range(2018, 2023))


@dataclass(frozen=True)
class StandardOnlyExecutionResult:
    result: v5.V5PipelineResult
    source_integrity_audit: Mapping[str, Mapping[str, object]]
    authorization_claim_path: Path


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid Standard-only execution artifact: {path}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("Standard-only execution artifact is not an object")
    return value


def load_execution_plan(*, root: Path) -> dict[str, object]:
    """Validate the pre-registered execution envelope without opening source rows."""

    plan = _object(root / PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    forbidden = plan.get("forbidden_actions")
    source_scope = plan.get("source_scope")
    protocol = load_standard_only_protocol(root=root)
    source = protocol.get("source")
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version")
        != "tier1_standard_only_historical_execution_plan/1.0.0"
        or plan.get("state") != "PREPARED_REQUIRES_SEPARATE_EXECUTION_APPROVAL"
        or plan.get("operation") != OPERATION
        or plan.get("execution_mode") != "IN_MEMORY_UNPUBLISHED_RESULT"
        or plan.get("maximum_host_runtime_seconds") != 900
        or plan.get("estimated_external_cost_usd") != "0"
        or plan.get("output_root") != OUTPUT_ROOT.as_posix()
        or not isinstance(forbidden, Mapping) or not forbidden
        or not all(value is True for value in forbidden.values())
        or not isinstance(source_scope, Mapping)
        or source_scope.get("markets") != list(MARKETS)
        or source_scope.get("years") != list(YEARS)
        or source_scope.get("selected_release_count") != 20
        or not isinstance(source, Mapping)
        or plan.get("protocol_id") != protocol.get("protocol_id")
        or plan.get("selected_sources_id") != source.get("selected_sources_id")
        or plan.get("calendar_release_id") != source.get("calendar_release_id")
        or plan.get("selected_missing_execution_path_result")
        != "INCONCLUSIVE_DATA_OR_COVERAGE"
        or plan.get("runner_up_substitution") is not False
        or plan.get("zero_return_imputation") is not False
    ):
        raise UnauthorizedOperation("Standard-only historical execution plan drifted")
    return plan


def load_registered_execution_context(
    *, root: Path, plan: Mapping[str, object],
) -> tuple[str, dict[str, object], dict[str, object], dict[str, object]]:
    """Require one active hash-bound declaration and PASS certificate."""

    pointer = _object(root / ACTIVE_POINTER_PATH)
    trial_id = pointer.get("trial_id")
    if not isinstance(trial_id, str) or len(trial_id) != 64:
        raise UnauthorizedOperation("registered Standard-only pointer is unavailable")
    registry_path = root / TRIAL_REGISTRY_ROOT / f"{trial_id}.json"
    certificate_path = root / CERTIFICATE_ROOT / f"{trial_id}.json"
    registry = _object(registry_path)
    certificate = _object(certificate_path)
    bindings = registry.get("bindings")
    if (
        pointer.get("state") != "ACTIVE_REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or pointer.get("trial_registry_path")
        != registry_path.relative_to(root).as_posix()
        or pointer.get("preexecution_certificate_path")
        != certificate_path.relative_to(root).as_posix()
        or registry.get("trial_id") != trial_id
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or registry.get("source_row_access") is not False
        or registry.get("historical_evaluation") is not False
        or registry.get("holdout_or_forward_access") is not False
        or certificate.get("trial_id") != trial_id
        or certificate.get("state") != "PUBLISHED_PREEXECUTION_PASS"
        or certificate.get("overall_decision") != "PASS"
        or certificate.get("active_pointer_id") != pointer.get("pointer_id")
        or registry.get("protocol_id") != plan.get("protocol_id")
        or registry.get("selected_sources_id") != plan.get("selected_sources_id")
        or registry.get("calendar_release_id") != plan.get("calendar_release_id")
        or not isinstance(bindings, Mapping)
        or bindings.get(PLAN_PATH.as_posix()) != sha256_file(root / PLAN_PATH)
        or any(sha256_file(root / str(path)) != digest for path, digest in bindings.items())
    ):
        raise UnauthorizedOperation("registered Standard-only execution context drifted")
    return trial_id, pointer, registry, certificate


def _required_scope(
    *, trial_id: str, plan: Mapping[str, object], output_root: Path,
) -> dict[str, str]:
    return {
        "trial_id": trial_id,
        "selected_sources_id": str(plan["selected_sources_id"]),
        "output_root": output_root.as_posix(),
        "holdout_or_forward_access": "false",
        "provider_access": "false",
        "publication": "false",
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": str(plan["plan_sha256"]),
    }


def claim_execution_authorization(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, plan: Mapping[str, object], output_root: Path,
) -> Path:
    boundary.assert_active_path(output_root.absolute(), purpose="Standard-only output root")
    receipt.verify(
        boundary, operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
    )
    required = _required_scope(
        trial_id=trial_id, plan=plan, output_root=output_root,
    )
    if dict(receipt.scope) != required or not receipt.single_use or not receipt.externally_authorized:
        raise UnauthorizedOperation("Standard-only execution requires an exact single-use receipt")
    claim = root / "state/authorization_uses" / f"{receipt.receipt_id}.json"
    boundary.assert_active_path(
        claim.absolute(), purpose="Standard-only authorization use",
        subtree="state/authorization_uses",
    )
    claim.parent.mkdir(parents=True, exist_ok=True)
    try:
        with claim.open("xb") as stream:
            stream.write(canonical_bytes({
                "schema_version": "tier1_standard_only_authorization_use/1.0.0",
                "receipt_id": receipt.receipt_id,
                "trial_id": trial_id,
                "selected_sources_id": plan["selected_sources_id"],
                "approval_plan_id": plan["plan_id"],
                "approval_plan_sha256": plan["plan_sha256"],
                "holdout_or_forward_access": False,
                "provider_access": False,
                "publication": False,
            }) + b"\n")
    except FileExistsError as exc:
        raise UnauthorizedOperation("Standard-only execution receipt was already consumed") from exc
    return claim


def resolve_authorized_source_streams(
    *, root: Path, boundary: RepoBoundary, selected_sources_id: str,
) -> tuple[
    Mapping[tuple[str, int], Iterator[v5.V5SourceRecord]],
    Mapping[tuple[str, int], v10.SourceIntegrityAuditV10],
]:
    """Resolve and hash the exact 20 sources only after authorization is claimed."""

    selected = _load_selected_sources(root=root)
    if sha256_json(selected) != selected_sources_id:
        raise IntegrityError("registered selected-source identity drifted")
    parsed_keys = []
    for key in selected:
        try:
            market, year_text = key.split("/")
            year = int(year_text)
        except (AttributeError, TypeError, ValueError) as exc:
            raise IntegrityError("selected causal source key is malformed") from exc
        if year == 2025 or market not in MARKETS or year not in YEARS:
            raise UnauthorizedOperation("holdout or forward source rejected before open")
        parsed_keys.append((key, market, year))
    catalog = {str(item["release_id"]): item for item in _catalog()}
    paths: dict[tuple[str, int], Path] = {}
    for key, market, year in parsed_keys:
        source = selected[key]
        item = catalog.get(str(source["release_id"]))
        if (
            item is None or item.get("market") != market or item.get("year") != year
            or item.get("payload_sha256") != source.get("payload_sha256")
        ):
            raise IntegrityError("selected causal source manifest drifted")
        path = _candidate_path(boundary=boundary, item=item)
        if sha256_file(path) != source["payload_sha256"]:
            raise IntegrityError("selected causal source bytes drifted")
        paths[(market, year)] = path
    expected = {(market, year) for market in MARKETS for year in YEARS}
    if set(paths) != expected:
        raise IntegrityError("selected causal source map is incomplete")
    audits = {key: v10.SourceIntegrityAuditV10(key[0]) for key in sorted(paths)}
    streams = {
        key: v10.iter_source_records_from_parquet_v10(
            market=key[0], path=paths[key], audit=audits[key],
        )
        for key in sorted(paths)
    }
    return streams, audits


def execute_authorized_standard_only(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
) -> StandardOnlyExecutionResult:
    plan = load_execution_plan(root=root)
    plan = {**plan, "plan_sha256": sha256_file(root / PLAN_PATH)}
    trial_id, _, registry, _ = load_registered_execution_context(root=root, plan=plan)
    require_locked_repository_environment(root)
    output_root = root / OUTPUT_ROOT
    claim = claim_execution_authorization(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        plan=plan, output_root=output_root,
    )
    streams, audits = resolve_authorized_source_streams(
        root=root, boundary=boundary,
        selected_sources_id=str(registry["selected_sources_id"]),
    )
    sessions = v5.load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(registry["calendar_release_id"]),
    )
    runtime = dict(v5.prepare_runtime_receipt_v5(root=root, trial_id=trial_id))
    runtime.pop("runtime_receipt_id", None)
    runtime.update({
        "schema_version": "tier1_standard_only_runtime_receipt/1.0.0",
        "authorization_receipt_id": receipt.receipt_id,
        "authorization_claim_path": claim.relative_to(root).as_posix(),
        "authorization_claim_sha256": sha256_file(claim),
        "execution_plan_id": plan["plan_id"],
        "execution_plan_sha256": plan["plan_sha256"],
    })
    runtime["runtime_receipt_id"] = sha256_json(runtime)
    result = run_frozen_trial_pipeline(
        streams=streams,
        census=v5.build_expected_census_from_calendar(sessions=sessions),
        contract=load_standard_only_protocol(root=root),
        trial_id=trial_id,
        runtime_receipt=runtime,
    )
    return StandardOnlyExecutionResult(
        result=result,
        source_integrity_audit={
            f"{market}/{year}": audit.as_dict()
            for (market, year), audit in sorted(audits.items())
        },
        authorization_claim_path=claim,
    )
