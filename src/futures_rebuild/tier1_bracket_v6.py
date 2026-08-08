"""Source-integrity successor for the pre-model-fit V5 execution failure.

V5 remains immutable audit evidence.  V6 retains V5's registered strategy,
cost, risk, model, inference, and promotion rules, but makes source defects
representable before the modeling pipeline sees a row.  A non-tradable row,
an unresolved row without a session label, or a missing minute inside a
session makes that entire market-session ambiguous.  V5's existing
materializer then emits explicit pre-prediction abstentions for all declared
checkpoints in that session.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .runtime_environment import require_locked_repository_environment
from . import tier1_bracket_v5 as v5
from .tier1_bracket_v5 import (
    NS_PER_MINUTE,
    REQUIRED_PARQUET_COLUMNS,
    TRADABLE_DISPOSITIONS,
    V5SourceRecord,
    _hex64,
    source_record_from_mapping,
)


V6_AUDIT_COLUMNS = frozenset(
    set(REQUIRED_PARQUET_COLUMNS)
    | {
        "failure_code",
        "failure_detail_sha256",
        "prediction_in_coverage_denominator",
    }
)
V5_TRIAL_ID = "8f6fed0171979ffe76256117c29937bc1f469f674d722525414b16ca5bfd4e03"
V5_REGISTRY = Path("state/trial_registry/tier1_bracket_successor_v5") / f"{V5_TRIAL_ID}.json"
V5_EVENT = Path("state/trial_events/tier1_bracket_successor_v5") / f"{V5_TRIAL_ID}.json"
V5_EXECUTION_PLAN = Path("configs/tier1_bracket_successor_v5_historical_execution_plan.json")
V5_RETIREMENT_PREPARATION = Path("configs/tier1_bracket_v5_retirement_preparation.json")
V6_CONTRACT = Path("configs/tier1_bracket_successor_v6.json")
V5_RETIREMENT_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_v5_retirement")
V5_RETIREMENT_EVENT_ROOT = Path("state/trial_events/tier1_bracket_v5_retirement")
V6_REGISTRY_ROOT = Path("state/trial_registry/tier1_bracket_successor_v6")
V6_EVENT_ROOT = Path("state/trial_events/tier1_bracket_successor_v6")


@dataclass
class SourceIntegrityAuditV6:
    market: str
    total_rows: int = 0
    tradable_rows: int = 0
    nontradable_rows: int = 0
    sessionless_nontradable_rows: int = 0
    same_session_gap_count: int = 0
    ambiguous_sessions: set[str] = field(default_factory=set)
    failure_codes: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {
            "market": self.market,
            "total_rows": self.total_rows,
            "tradable_rows": self.tradable_rows,
            "nontradable_rows": self.nontradable_rows,
            "sessionless_nontradable_rows": self.sessionless_nontradable_rows,
            "same_session_gap_count": self.same_session_gap_count,
            "ambiguous_sessions": sorted(self.ambiguous_sessions),
            "failure_codes": dict(sorted(self.failure_codes.items())),
        }


def _event(row: Mapping[str, object]) -> int:
    value = row.get("event_at_ns")
    if type(value) is not int:
        raise IntegrityError("V6 source row lacks an event identity")
    return value


def _record_failure(audit: SourceIntegrityAuditV6, row: Mapping[str, object]) -> None:
    code = row.get("failure_code")
    if not isinstance(code, str) or not code:
        code = "MISSING_FAILURE_CODE"
    audit.failure_codes[code] = audit.failure_codes.get(code, 0) + 1


def _normalize_orphan(
    *, market: str, row: Mapping[str, object], session: str,
) -> V5SourceRecord:
    if row.get("disposition") in TRADABLE_DISPOSITIONS:
        raise IntegrityError("V6 cannot assign a session to a tradable orphan")
    if row.get("prediction_in_coverage_denominator") is not True:
        raise IntegrityError("V6 orphan is absent from the declared coverage universe")
    failure = row.get("failure_code")
    detail = row.get("failure_detail_sha256")
    if not isinstance(failure, str) or not failure or not _hex64(detail):
        raise IntegrityError("V6 orphan lacks fail-closed provenance")
    normalized = dict(row)
    normalized["exchange_session_date"] = session
    record = source_record_from_mapping(market=market, row=normalized)
    if record.executable:
        raise IntegrityError("V6 orphan normalization manufactured eligibility")
    return record


def _emit_ambiguous(
    *, record: V5SourceRecord, audit: SourceIntegrityAuditV6,
) -> tuple[V5SourceRecord, V5SourceRecord]:
    if record.bar is None:
        raise IntegrityError("V6 cannot mark a source defect without an event-bearing row")
    audit.ambiguous_sessions.add(record.exchange_session_date)
    # The duplicate event is deliberate. V5 already treats duplicate event
    # identities as MISSING_OR_AMBIGUOUS_MARKET_IDENTITY for the whole session.
    return record, record


def normalize_source_mappings_v6(
    *, market: str, rows: Iterator[Mapping[str, object]],
    audit: SourceIntegrityAuditV6,
) -> Iterator[V5SourceRecord]:
    """Convert source mappings without hiding non-tradable rows or minute gaps."""

    previous: V5SourceRecord | None = None
    pending_orphans: list[Mapping[str, object]] = []
    for row in rows:
        audit.total_rows += 1
        disposition = row.get("disposition")
        tradable = disposition in TRADABLE_DISPOSITIONS
        if tradable:
            audit.tradable_rows += 1
        else:
            audit.nontradable_rows += 1
            _record_failure(audit, row)
        session = row.get("exchange_session_date")
        if not isinstance(session, str):
            if tradable:
                raise IntegrityError("tradable V6 source row lacks a session identity")
            if not _hex64(row.get("source_row_sha256")):
                raise IntegrityError("sessionless V6 source row lacks a source identity")
            audit.sessionless_nontradable_rows += 1
            pending_orphans.append(dict(row))
            continue

        current = source_record_from_mapping(market=market, row=row)
        bridged_orphans = False
        if pending_orphans:
            if previous is None or previous.exchange_session_date != current.exchange_session_date:
                raise IntegrityError("V6 cannot unambiguously locate a sessionless source defect")
            expected_event = previous.bar.event_at_ns + NS_PER_MINUTE if previous.bar is not None else None
            for orphan in pending_orphans:
                if expected_event is None or _event(orphan) != expected_event:
                    raise IntegrityError("V6 sessionless source defect is not minute-contiguous")
                normalized = _normalize_orphan(
                    market=market, row=orphan,
                    session=current.exchange_session_date,
                )
                for emitted in _emit_ambiguous(record=normalized, audit=audit):
                    yield emitted
                expected_event += NS_PER_MINUTE
            if current.bar is None or current.bar.event_at_ns != expected_event:
                raise IntegrityError("V6 source defect lacks matching causal neighbors")
            pending_orphans.clear()
            bridged_orphans = True

        if (
            not bridged_orphans
            and previous is not None
            and previous.exchange_session_date == current.exchange_session_date
            and previous.bar is not None
            and current.bar is not None
            and current.bar.event_at_ns - previous.bar.event_at_ns != NS_PER_MINUTE
        ):
            audit.same_session_gap_count += 1
            for emitted in _emit_ambiguous(record=current, audit=audit):
                yield emitted
        elif current.executable:
            yield current
        else:
            for emitted in _emit_ambiguous(record=current, audit=audit):
                yield emitted
        previous = current
    if pending_orphans:
        raise IntegrityError("V6 source stream ends with an unresolved sessionless defect")


def iter_source_records_from_parquet_v6(
    *, market: str, path: Path, audit: SourceIntegrityAuditV6,
    batch_size: int = 65_536,
) -> Iterator[V5SourceRecord]:
    """Batch-stream one source while retaining source-integrity failures."""

    if audit.market != market or batch_size < 1 or batch_size > 65_536:
        raise IntegrityError("V6 parquet stream request is outside its bounds")
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(path)
    if not V6_AUDIT_COLUMNS.issubset(parquet.schema_arrow.names):
        raise IntegrityError("V6 source schema lacks audit columns")

    def mappings() -> Iterator[Mapping[str, object]]:
        for batch in parquet.iter_batches(
            batch_size=batch_size, columns=sorted(V6_AUDIT_COLUMNS),
        ):
            columns = batch.to_pydict()
            for index in range(batch.num_rows):
                yield {name: values[index] for name, values in columns.items()}

    yield from normalize_source_mappings_v6(
        market=market, rows=mappings(), audit=audit,
    )


def _load_object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid V6 JSON artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError(f"V6 artifact is not an object: {path.as_posix()}")
    return value


def load_v6_contract(*, root: Path) -> tuple[dict[str, object], dict[str, object]]:
    delta = _load_object(root / V6_CONTRACT)
    inherited_path = delta.get("inherited_v5_contract_path")
    inherited_hash = delta.get("inherited_v5_contract_sha256")
    if (
        delta.get("schema_version") != "tier1_bracket_successor_v6_contract/1.0.0"
        or delta.get("state") != "PREPARED_NOT_REGISTERED"
        or delta.get("supersedes_v5_trial_id") != V5_TRIAL_ID
        or inherited_path != "configs/tier1_bracket_successor_v5.json"
        or not _hex64(inherited_hash)
        or sha256_file(root / str(inherited_path)) != inherited_hash
    ):
        raise IntegrityError("V6 successor contract or frozen V5 binding drifted")
    source = delta.get("source_integrity_successor")
    authority = delta.get("authority")
    if (
        not isinstance(source, dict)
        or source.get("silent_drop_or_shortened_feature_window") != "FORBIDDEN"
        or source.get("source_integrity_audit_artifact") != "REQUIRED_HASH_BOUND_CREATE_ONLY"
        or not isinstance(authority, dict)
        or authority.get("holdout_or_forward_access") is not False
        or authority.get("publication_requires_separate_approval") is not True
    ):
        raise IntegrityError("V6 source-integrity or authority contract is incomplete")
    return v5.load_v5_contract(root=root), delta


@dataclass(frozen=True)
class PreparedV5RetirementV6:
    record_id: str
    canonical_payload: Mapping[str, object]


@dataclass(frozen=True)
class PreparedV6Registration:
    trial_id: str
    canonical_payload: Mapping[str, object]


def prepare_v5_retirement_v6(*, root: Path) -> PreparedV5RetirementV6:
    preparation = _load_object(root / V5_RETIREMENT_PREPARATION)
    registry = _load_object(root / V5_REGISTRY)
    event = _load_object(root / V5_EVENT)
    use_path = Path(str(preparation.get("authorization_use_path")))
    use = _load_object(root / use_path)
    registered_bindings = registry.get("bindings")
    if (
        preparation.get("trial_id") != V5_TRIAL_ID
        or preparation.get("disposition")
        != "INVALID_PRE_MODEL_FIT_SOURCE_CENSUS_IDENTITY_DEFECT"
        or preparation.get("strategy_failure_inference_allowed") is not False
        or preparation.get("model_fit_performed") is not False
        or registry.get("trial_id") != V5_TRIAL_ID
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or event.get("trial_id") != V5_TRIAL_ID
        or use.get("trial_id") != V5_TRIAL_ID
        or use.get("receipt_id") != preparation.get("authorization_receipt_id")
        or not isinstance(registered_bindings, dict)
        or any(sha256_file(root / path) != digest for path, digest in registered_bindings.items())
    ):
        raise IntegrityError("V5 retirement preparation or preserved execution evidence is invalid")
    preserved = (
        Path("configs/tier1_bracket_successor_v5.json"),
        Path("src/futures_rebuild/tier1_bracket_v5.py"),
        Path("tests/test_tier1_bracket_v5.py"),
        V5_REGISTRY,
        V5_EVENT,
        V5_EXECUTION_PLAN,
        use_path,
    )
    preserved_hashes = dict(registered_bindings)
    preserved_hashes.update({
        path.as_posix(): sha256_file(root / path) for path in preserved
    })
    core = {
        **preparation,
        "preserved_v5_sha256": dict(sorted(preserved_hashes.items())),
    }
    return PreparedV5RetirementV6(sha256_json(core), core)


def prepare_v6_registration(*, root: Path) -> PreparedV6Registration:
    _, delta = load_v6_contract(root=root)
    retirement = prepare_v5_retirement_v6(root=root)
    v5_registry = _load_object(root / V5_REGISTRY)
    registered_bindings = v5_registry.get("bindings")
    raw_sources = v5_registry.get("source_bindings")
    if (
        not isinstance(registered_bindings, dict)
        or any(sha256_file(root / path) != digest for path, digest in registered_bindings.items())
        or not isinstance(raw_sources, list)
        or not all(isinstance(item, dict) for item in raw_sources)
    ):
        raise IntegrityError("registered V5 lineage changed before V6 preparation")
    source_binding_id = v5.source_binding_id_from_metadata_v5(raw_sources)
    use_path = Path(str(retirement.canonical_payload["authorization_use_path"]))
    new_paths = (
        V5_RETIREMENT_PREPARATION,
        V6_CONTRACT,
        Path("src/futures_rebuild/tier1_bracket_v6.py"),
        Path("tests/test_tier1_bracket_v6.py"),
        V5_REGISTRY,
        V5_EVENT,
        V5_EXECUTION_PLAN,
        use_path,
    )
    bindings = dict(registered_bindings)
    bindings.update({path.as_posix(): sha256_file(root / path) for path in new_paths})
    core = {
        "schema_version": "tier1_bracket_successor_v6_registration/1.0.0",
        "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        "classification": delta["classification"],
        "supersedes_v5_trial_id": V5_TRIAL_ID,
        "v5_retirement_record_id": retirement.record_id,
        "change_scope": "SOURCE_INTEGRITY_REPRESENTATION_ONLY",
        "inherited_strategy_contract_sha256": delta["inherited_v5_contract_sha256"],
        "bindings": bindings,
        "calendar_release_id": v5_registry["calendar_release_id"],
        "dependency_lock_receipt_id": v5_registry["dependency_lock_receipt_id"],
        "source_bindings": sorted(
            (dict(item) for item in raw_sources),
            key=lambda item: (str(item["market"]), int(item["year"])),
        ),
        "source_binding_id": source_binding_id,
        "source_row_access": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "publication": False,
        "holdout_or_forward_access": False,
        "provider_access": False,
        "trading": False,
    }
    return PreparedV6Registration(sha256_json(core), core)


def persist_v5_retirement_v6(
    *, root: Path, prepared: PreparedV5RetirementV6,
) -> dict[str, str]:
    """Create-only surface; publication requires explicit external approval."""

    if prepared.record_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V5 retirement identity is invalid")
    preserved = prepared.canonical_payload.get("preserved_v5_sha256")
    if not isinstance(preserved, dict) or any(
        sha256_file(root / path) != digest for path, digest in preserved.items()
    ):
        raise IntegrityError("preserved V5 bytes changed after retirement preparation")
    registry = V5_RETIREMENT_REGISTRY_ROOT / f"{prepared.record_id}.json"
    event = V5_RETIREMENT_EVENT_ROOT / f"{prepared.record_id}.json"
    registry_path, event_path = root / registry, root / event
    if registry_path.exists() or event_path.exists():
        raise IntegrityError("V5 retirement publication is create-only")
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    event_path.parent.mkdir(parents=True, exist_ok=True)
    with registry_path.open("xb") as stream:
        stream.write(canonical_bytes({
            **prepared.canonical_payload,
            "state": "RETIRED_INVALID_BEFORE_MODEL_FIT",
        }) + b"\n")
    with event_path.open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_bracket_v5_retirement_event/1.0.0",
            "event_type": "RETIRED",
            "trial_id": V5_TRIAL_ID,
            "record_id": prepared.record_id,
        }) + b"\n")
    return {
        "record_id": prepared.record_id,
        "registry_path": registry.as_posix(),
        "event_path": event.as_posix(),
    }


def persist_v6_registration(
    *, root: Path, prepared: PreparedV6Registration,
) -> dict[str, str]:
    """Create-only surface; publication requires explicit external approval."""

    if prepared.trial_id != sha256_json(prepared.canonical_payload):
        raise IntegrityError("V6 trial identity is invalid")
    bindings = prepared.canonical_payload.get("bindings")
    if not isinstance(bindings, dict) or any(
        sha256_file(root / path) != digest for path, digest in bindings.items()
    ):
        raise IntegrityError("V6 registration binding changed after preparation")
    retirement_id = prepared.canonical_payload.get("v5_retirement_record_id")
    if not _hex64(retirement_id):
        raise IntegrityError("V6 registration lacks a V5 retirement identity")
    retirement_path = root / V5_RETIREMENT_REGISTRY_ROOT / f"{retirement_id}.json"
    retirement = _load_object(retirement_path)
    if (
        retirement.get("state") != "RETIRED_INVALID_BEFORE_MODEL_FIT"
        or sha256_json({
            **retirement,
            "state": "PREPARED_REQUIRES_PUBLICATION_APPROVAL",
        }) != retirement_id
    ):
        raise IntegrityError("published V5 retirement is absent or inconsistent")
    registry = V6_REGISTRY_ROOT / f"{prepared.trial_id}.json"
    event = V6_EVENT_ROOT / f"{prepared.trial_id}.json"
    registry_path, event_path = root / registry, root / event
    if registry_path.exists() or event_path.exists():
        raise IntegrityError("V6 registration publication is create-only")
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    event_path.parent.mkdir(parents=True, exist_ok=True)
    with registry_path.open("xb") as stream:
        stream.write(canonical_bytes({
            **prepared.canonical_payload,
            "state": "REGISTERED_BEFORE_SOURCE_ROW_ACCESS",
            "trial_id": prepared.trial_id,
        }) + b"\n")
    with event_path.open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_bracket_successor_v6_event/1.0.0",
            "event_type": "DECLARED",
            "trial_id": prepared.trial_id,
            "source_row_access": False,
            "model_fit": False,
            "prediction_generation": False,
            "historical_evaluation": False,
            "holdout_or_forward_access": False,
        }) + b"\n")
    return {
        "trial_id": prepared.trial_id,
        "registry_path": registry.as_posix(),
        "event_path": event.as_posix(),
    }


def verify_historical_operation_receipt_v6(
    *, boundary: RepoBoundary, receipt: OperationReceipt, trial_id: str,
    source_binding_id: str, output_root: Path,
) -> str:
    if not _hex64(trial_id) or not _hex64(source_binding_id):
        raise UnauthorizedOperation("V6 historical receipt scope is invalid")
    boundary.assert_active_path(output_root.absolute(), purpose="V6 historical output root")
    required = {
        "trial_id": trial_id,
        "source_binding_id": source_binding_id,
        "output_root": output_root.as_posix(),
        "holdout_or_forward_access": "false",
        "provider_access": "false",
        "publication": "false",
    }
    receipt.verify(
        boundary,
        operation="EXECUTE_TIER1_BRACKET_SUCCESSOR_V6_HISTORICAL_SCREEN",
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
    )
    observed = dict(receipt.scope)
    approval = {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    if set(observed) != set(required) | approval or any(
        observed.get(key) != value for key, value in required.items()
    ):
        raise UnauthorizedOperation("V6 receipt does not grant the exact historical scope")
    if not receipt.single_use or not receipt.externally_authorized:
        raise UnauthorizedOperation("V6 execution requires single-use external authority")
    return receipt.receipt_id


def claim_historical_operation_receipt_v6(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_binding_id: str, output_root: Path,
) -> Path:
    receipt_id = verify_historical_operation_receipt_v6(
        boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_binding_id=source_binding_id, output_root=output_root,
    )
    claim = root / "state/authorization_uses" / f"{receipt_id}.json"
    boundary.assert_active_path(
        claim.absolute(), purpose="V6 authorization use",
        subtree="state/authorization_uses",
    )
    claim.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "tier1_bracket_v6_authorization_use/1.0.0",
        "receipt_id": receipt_id,
        "trial_id": trial_id,
        "source_binding_id": source_binding_id,
        "output_root": output_root.as_posix(),
        "holdout_or_forward_access": False,
    }
    try:
        with claim.open("xb") as stream:
            stream.write(canonical_bytes(payload) + b"\n")
    except FileExistsError as exc:
        raise UnauthorizedOperation("V6 historical receipt was already consumed") from exc
    return claim


@dataclass(frozen=True)
class V6PipelineResult:
    base: v5.V5PipelineResult
    source_integrity_audit: Mapping[str, Mapping[str, object]]


def authorized_source_streams_v6(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_paths: Mapping[tuple[str, int], Path],
    output_root: Path,
) -> tuple[
    Mapping[tuple[str, int], Iterator[V5SourceRecord]],
    Mapping[tuple[str, int], SourceIntegrityAuditV6],
]:
    if any(year == 2025 for _, year in source_paths):
        raise UnauthorizedOperation("2025 holdout path is rejected before open")
    registry = _load_object(root / V6_REGISTRY_ROOT / f"{trial_id}.json")
    bindings = registry.get("bindings")
    if (
        registry.get("trial_id") != trial_id
        or registry.get("state") != "REGISTERED_BEFORE_SOURCE_ROW_ACCESS"
        or registry.get("holdout_or_forward_access") is not False
        or not isinstance(bindings, dict)
        or any(sha256_file(root / path) != digest for path, digest in bindings.items())
    ):
        raise UnauthorizedOperation("registered V6 declaration is unavailable or drifted")
    require_locked_repository_environment(root)
    raw = registry.get("source_bindings")
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        raise IntegrityError("registered V6 source bindings are absent")
    binding_id = v5.source_binding_id_from_metadata_v5(raw)
    if registry.get("source_binding_id") != binding_id:
        raise IntegrityError("V6 registered source binding is inconsistent")
    expected = {
        (str(item["market"]), int(item["year"])): str(item["source_parquet_sha256"])
        for item in raw
    }
    if set(source_paths) != set(expected):
        raise IntegrityError("V6 source path map differs from registration")
    claim_historical_operation_receipt_v6(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_binding_id=binding_id, output_root=output_root,
    )
    for key, path in source_paths.items():
        if sha256_file(path) != expected[key]:
            raise IntegrityError("V6 source bytes differ from registration")
    audits = {key: SourceIntegrityAuditV6(key[0]) for key in sorted(source_paths)}
    streams = {
        key: iter_source_records_from_parquet_v6(
            market=key[0], path=source_paths[key], audit=audits[key],
        )
        for key in sorted(source_paths)
    }
    return streams, audits


def execute_authorized_v6(
    *, root: Path, boundary: RepoBoundary, receipt: OperationReceipt,
    trial_id: str, source_paths: Mapping[tuple[str, int], Path],
    output_root: Path,
) -> V6PipelineResult:
    """Execute V6 in memory; never publish evidence automatically."""

    streams, audits = authorized_source_streams_v6(
        root=root, boundary=boundary, receipt=receipt, trial_id=trial_id,
        source_paths=source_paths, output_root=output_root,
    )
    registry = _load_object(root / V6_REGISTRY_ROOT / f"{trial_id}.json")
    sessions = v5.load_registered_calendar_sessions_v5(
        boundary=boundary,
        registered_calendar_index_release_id=str(registry["calendar_release_id"]),
    )
    census = v5.build_expected_census_from_calendar(sessions=sessions)
    inherited, _ = load_v6_contract(root=root)
    result = v5.run_v5_pipeline(
        streams=streams,
        census=census,
        contract=inherited,
        trial_id=trial_id,
        runtime_receipt=v5.prepare_runtime_receipt_v5(root=root, trial_id=trial_id),
    )
    payload = {
        f"{market}/{year}": audit.as_dict()
        for (market, year), audit in sorted(audits.items())
    }
    return V6PipelineResult(result, payload)


def _evidence_payloads_v6(result: V6PipelineResult) -> dict[str, object]:
    base = result.base.evidence
    raw = {
        "model": base.model,
        "predictions": base.predictions,
        "opportunity_ledger": base.opportunity_ledger,
        "fills": base.fills,
        "continuous_equity_marks": base.continuous_equity_marks,
        "segmented_metrics": base.segmented_metrics,
        "inference": base.inference,
        "decision": base.decision,
        "runtime_receipt": base.runtime_receipt,
        "source_integrity_audit": result.source_integrity_audit,
    }
    safe = v5._json_safe(raw)
    if not isinstance(safe, dict):
        raise IntegrityError("V6 evidence cannot be canonicalized")
    return safe


def build_evidence_manifest_v6(
    *, trial_id: str, result: V6PipelineResult,
) -> dict[str, object]:
    base = result.base.evidence
    payloads = _evidence_payloads_v6(result)
    if not base.predictions or not base.opportunity_ledger:
        raise IntegrityError("V6 evidence lacks frozen predictions or opportunity rows")
    files = {
        f"{name}.json": sha256_bytes(canonical_bytes({"payload": payload}) + b"\n")
        for name, payload in sorted(payloads.items())
    }
    core = {
        "schema_version": "tier1_bracket_successor_v6_evidence_manifest/1.0.0",
        "trial_id": trial_id,
        "files": files,
    }
    return {**core, "manifest_id": sha256_json(core)}


def persist_evidence_bundle_v6(
    *, boundary: RepoBoundary, output_root: Path, trial_id: str,
    result: V6PipelineResult,
) -> dict[str, str]:
    """Publish the complete V6 evidence create-only after separate approval."""

    manifest = build_evidence_manifest_v6(trial_id=trial_id, result=result)
    boundary.assert_active_path(
        output_root.absolute(), purpose="V6 evidence output root"
    )
    destination = output_root / trial_id / str(manifest["manifest_id"])
    if destination.exists():
        raise IntegrityError("V6 evidence publication is create-only")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(
        prefix=f".staging-{manifest['manifest_id']}-",
        dir=destination.parent,
    ))
    payloads = _evidence_payloads_v6(result)
    for filename, expected_hash in manifest["files"].items():
        name = filename.removesuffix(".json")
        path = staging / filename
        with path.open("xb") as stream:
            stream.write(canonical_bytes({"payload": payloads[name]}) + b"\n")
        if sha256_file(path) != expected_hash:
            raise IntegrityError("persisted V6 evidence hash mismatch")
    manifest_path = staging / "manifest.json"
    with manifest_path.open("xb") as stream:
        stream.write(canonical_bytes(manifest) + b"\n")
    if destination.exists():
        raise IntegrityError("V6 evidence destination appeared during publication")
    staging.replace(destination)
    final_manifest = destination / "manifest.json"
    return {
        "manifest_id": str(manifest["manifest_id"]),
        "manifest_path": final_manifest.as_posix(),
        "manifest_sha256": sha256_file(final_manifest),
    }
