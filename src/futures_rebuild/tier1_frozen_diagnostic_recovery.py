"""Receipt-gated diagnostic feasibility census for frozen execution gaps.

This module is deliberately diagnostic-only.  It derives the exact set of
feature-complete checkpoints whose execution path failed the frozen source
gate, then asks whether already-accepted one-second bars or trades contain
price-free timing and instrument-identity evidence in the required windows.
It cannot create research data, alter the protocol, register a trial, or
evaluate performance.
"""

from __future__ import annotations

import json
import time
from bisect import bisect_right
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from . import tier1_bracket_v5 as v5
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .foundation.decoder import ProviderObservationHeader, iter_observation_headers
from .foundation.snapshot import DbnReleaseFile, PublishedDbnRelease
from .runtime_environment import require_locked_repository_environment
from .source_symbology import build_query_contract
from .tier1_frozen_successor_source_semantics import (
    ENTRY_DELAY_MINUTES,
    MAXIMUM_HOLD_MINUTES,
    MAXIMUM_LIQUIDATION_DELAY_MINUTES,
)
from .tier1_preexecution_recovery_feasibility import (
    CALENDAR_RELEASE_ID,
    DBN_MANIFEST_PATH,
    DBN_MANIFEST_SHA256,
    DBN_RELEASE_ID,
    _validate_source_contract,
)


PLAN_PATH = Path("configs/tier1_frozen_diagnostic_recovery_plan.json")
SOURCE_ADEQUACY_RECORD_PATH = Path(
    "state/source_quality/tier1_frozen_source_adequacy/"
    "b3d8efbb010631922a944f13aff2de77e20d6775a2d98e5333994eca33cb5fbf.json"
)
SOURCE_ADEQUACY_RECORD_ID = SOURCE_ADEQUACY_RECORD_PATH.stem
SOURCE_ADEQUACY_RECORD_SHA256 = (
    "81057522b9038f3580f32ce51807b07435ebdd80f390ca812fad2c0cce010c9f"
)
PROTOCOL_PATH = Path("configs/tier1_frozen_trial_protocol.json")
PROTOCOL_ID = "d647438200d54b60f9c7ddb69117adcd0abc23050b971dae542cda3fbdc21867"
PROTOCOL_SHA256 = "7b6dcc144f52ef9feac7298dc87bbfbb6cb51f9f4628bfaaad773923d70a9662"
OPERATION = "CENSUS_FROZEN_TIER1_DIAGNOSTIC_EXECUTION_RECOVERY_AND_PUBLISH"
RECORD_ROOT = Path("state/source_quality/tier1_frozen_diagnostic_recovery")
EVENT_ROOT = Path("state/source_quality_events/tier1_frozen_diagnostic_recovery")
DIAGNOSTIC_SCHEMAS = ("ohlcv-1s", "trades")
EXPECTED_GAP_COUNTS = {"ENTRY": 27, "LIQUIDATION": 6, "IDENTITY": 1}
MAXIMUM_HOST_RUNTIME_SECONDS = 900


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid diagnostic recovery artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("diagnostic recovery artifact is not an object")
    return value


@dataclass(frozen=True)
class DiagnosticGapTarget:
    opportunity_id: str
    market: str
    year: int
    exchange_session_date: str
    checkpoint: str
    decision_at_ns: int
    category: str
    source_reason: str
    window_start_ns: int
    window_end_exclusive_ns: int

    def as_dict(self) -> dict[str, object]:
        return {
            "opportunity_id": self.opportunity_id,
            "market": self.market,
            "year": self.year,
            "exchange_session_date": self.exchange_session_date,
            "checkpoint": self.checkpoint,
            "decision_at_ns": self.decision_at_ns,
            "category": self.category,
            "source_reason": self.source_reason,
            "window_start_ns": self.window_start_ns,
            "window_end_exclusive_ns": self.window_end_exclusive_ns,
        }


def _gap_category(reason: object) -> str:
    mapping = {
        "exact reported entry bar is absent or non-executable": "ENTRY",
        "no observed executable liquidation exists within the delay limit": "LIQUIDATION",
        "reported execution span contains a non-qualified or foreign identity row": "IDENTITY",
    }
    try:
        return mapping[reason]
    except (KeyError, TypeError) as exc:
        raise IntegrityError("source adequacy contains an unknown execution gap") from exc


def derive_gap_targets(
    *, source_record: Mapping[str, object],
    expected_checkpoints: Sequence[v5.CensusCheckpoint],
) -> tuple[DiagnosticGapTarget, ...]:
    """Derive exactly the failed feature-complete execution checkpoints."""

    coverage = source_record.get("checkpoint_coverage")
    if (
        source_record.get("record_id") != SOURCE_ADEQUACY_RECORD_ID
        or source_record.get("adjudication", {}).get("decision") != "FAIL"  # type: ignore[union-attr]
        or not isinstance(coverage, list)
    ):
        raise IntegrityError("source adequacy record is not the frozen failed census")
    expected_by_id = {
        item.expected.opportunity_id: item.expected
        for item in expected_checkpoints if item.calendar_open
    }
    selected = [
        item for item in coverage
        if isinstance(item, Mapping)
        and item.get("feature_status") == "COMPLETE"
        and item.get("execution_status") == "EXPLICIT_UNAVAILABLE"
    ]
    if len(selected) != 34:
        raise IntegrityError("diagnostic recovery target count changed")
    minute = v5.NS_PER_MINUTE
    output: list[DiagnosticGapTarget] = []
    for item in selected:
        opportunity_id = str(item.get("opportunity_id"))
        expected = expected_by_id.get(opportunity_id)
        if expected is None:
            raise IntegrityError("diagnostic target is absent from the registered calendar")
        category = _gap_category(item.get("execution_reason"))
        entry_at = expected.decision_at_ns + ENTRY_DELAY_MINUTES * minute
        timeout_at = entry_at + MAXIMUM_HOLD_MINUTES * minute
        deadline = timeout_at + MAXIMUM_LIQUIDATION_DELAY_MINUTES * minute
        if category == "ENTRY":
            start, end = entry_at, entry_at + minute
        elif category == "LIQUIDATION":
            start, end = timeout_at, deadline + minute
        else:
            start, end = entry_at, deadline + minute
        if (
            expected.market != item.get("market")
            or expected.year != item.get("year")
            or expected.exchange_session_date != item.get("exchange_session_date")
            or expected.checkpoint != item.get("checkpoint")
            or expected.year not in range(2018, 2023)
        ):
            raise IntegrityError("diagnostic target differs from its calendar checkpoint")
        output.append(DiagnosticGapTarget(
            opportunity_id=opportunity_id,
            market=expected.market,
            year=expected.year,
            exchange_session_date=expected.exchange_session_date,
            checkpoint=expected.checkpoint,
            decision_at_ns=expected.decision_at_ns,
            category=category,
            source_reason=str(item["execution_reason"]),
            window_start_ns=start,
            window_end_exclusive_ns=end,
        ))
    output.sort(key=lambda item: (item.window_start_ns, item.market, item.opportunity_id))
    counts = {category: sum(item.category == category for item in output) for category in EXPECTED_GAP_COUNTS}
    if counts != EXPECTED_GAP_COUNTS or len({item.opportunity_id for item in output}) != 34:
        raise IntegrityError("diagnostic recovery target composition changed")
    return tuple(output)


def _query_contract_from_sidecar(
    *, sidecar: Mapping[str, object], schema: str, market: str,
) -> dict[str, object]:
    if (
        sidecar.get("vendor") != "databento"
        or sidecar.get("dataset") != "GLBX.MDP3"
        or sidecar.get("schema") != schema
        or sidecar.get("market") != market
        or sidecar.get("stype_out") != "instrument_id"
        or sidecar.get("request_status") != "ok"
        or not isinstance(sidecar.get("symbols_requested"), list)
    ):
        raise IntegrityError("diagnostic DBN sidecar is invalid")
    return build_query_contract(
        schema=schema,
        market=market,
        start=str(sidecar["start"]),
        end=str(sidecar["end"]),
        stype_in=str(sidecar["stype_in"]),
        symbols=sidecar["symbols_requested"],
    )


def diagnostic_catalog(
    *, root: Path, boundary: RepoBoundary,
    targets: Sequence[DiagnosticGapTarget],
) -> tuple[PublishedDbnRelease, tuple[dict[str, object], ...]]:
    """Bind only diagnostic files for target market-years; never open DBN rows."""

    manifest = root / DBN_MANIFEST_PATH
    if sha256_file(manifest) != DBN_MANIFEST_SHA256:
        raise IntegrityError("accepted DBN manifest changed")
    release = PublishedDbnRelease.open(manifest, boundary=boundary, verify_files=False)
    if release.source_release_id != DBN_RELEASE_ID:
        raise IntegrityError("accepted DBN release identity changed")
    cells = sorted({(item.market, item.year) for item in targets})
    items: list[dict[str, object]] = []
    for schema in DIAGNOSTIC_SCHEMAS:
        directory = schema.replace("-", "_")
        for market, year in cells:
            prefix = f"dbn/{directory}/{market}/{year}/"
            keys = sorted(
                key for key in release.files
                if key.startswith(prefix) and key.endswith(".dbn.zst")
            )
            if len(keys) > 1:
                raise IntegrityError("diagnostic DBN cell is ambiguous")
            if not keys:
                items.append({
                    "schema": schema, "market": market, "year": year,
                    "status": "SOURCE_FILE_ABSENT",
                })
                continue
            dbn_file = release.file(keys[0])
            sidecar_file = release.file(f"{keys[0]}.manifest.json")
            sidecar = _object(sidecar_file.verify())
            query = _query_contract_from_sidecar(
                sidecar=sidecar, schema=schema, market=market,
            )
            logical = PurePosixPath(keys[0])
            if logical.name != f"{year}-01-01_{year + 1}-01-01.dbn.zst":
                raise IntegrityError("diagnostic DBN file does not cover the exact year")
            items.append({
                "schema": schema, "market": market, "year": year,
                "status": "BOUND_IMMUTABLE_FILE",
                "relative_path": keys[0],
                "file_sha256": dbn_file.sha256,
                "file_size": dbn_file.size,
                "sidecar_sha256": sidecar_file.sha256,
                "query_contract": query,
            })
    return release, tuple(items)


def classify_target_observations(
    *, target: DiagnosticGapTarget,
    observations_by_schema: Mapping[str, Sequence[ProviderObservationHeader]],
) -> dict[str, object]:
    """Classify presence and identity while omitting every price field."""

    families: dict[str, dict[str, object]] = {}
    any_ambiguous = False
    any_single = False
    for schema in DIAGNOSTIC_SCHEMAS:
        scoped = [
            item for item in observations_by_schema.get(schema, ())
            if item.market == target.market
            and target.window_start_ns <= item.event_at_ns < target.window_end_exclusive_ns
        ]
        identities = sorted({item.instrument_id for item in scoped})
        publishers = sorted({item.publisher_id for item in scoped})
        if not scoped:
            disposition = "NOT_OBSERVED"
        elif len(identities) == 1:
            disposition = "OBSERVED_SINGLE_IDENTITY"
            any_single = True
        else:
            disposition = "OBSERVED_AMBIGUOUS_IDENTITY"
            any_ambiguous = True
        families[schema] = {
            "disposition": disposition,
            "observation_count": len(scoped),
            "first_event_offset_ns": min(
                (item.event_at_ns - target.window_start_ns for item in scoped),
                default=None,
            ),
            "last_event_offset_ns": max(
                (item.event_at_ns - target.window_start_ns for item in scoped),
                default=None,
            ),
            "instrument_identity_count": len(identities),
            "instrument_identity_set_id": sha256_json(identities),
            "publisher_count": len(publishers),
        }
    if any_ambiguous:
        disposition = "DIAGNOSTIC_IDENTITY_AMBIGUOUS_FAIL_CLOSED"
    elif any_single:
        disposition = "DIAGNOSTIC_RECOVERY_CANDIDATE"
    else:
        disposition = "NOT_OBSERVED_IN_BOUND_DIAGNOSTIC_SOURCES"
    return {
        **target.as_dict(),
        "disposition": disposition,
        "families": families,
        "prices_reported": False,
    }


def matching_target_for_event(
    *, targets: Sequence[DiagnosticGapTarget], event_at_ns: int,
) -> DiagnosticGapTarget | None:
    """Select one non-overlapping target interval in logarithmic time."""

    ordered = tuple(sorted(targets, key=lambda item: item.window_start_ns))
    if any(
        ordered[index - 1].window_end_exclusive_ns > ordered[index].window_start_ns
        for index in range(1, len(ordered))
    ):
        raise IntegrityError("diagnostic target windows overlap within one source cell")
    index = bisect_right(
        [item.window_start_ns for item in ordered], event_at_ns,
    ) - 1
    if index < 0 or event_at_ns >= ordered[index].window_end_exclusive_ns:
        return None
    return ordered[index]


def _load_frozen_inputs(
    *, root: Path,
) -> tuple[dict[str, object], tuple[v5.CensusCheckpoint, ...]]:
    source_path = root / SOURCE_ADEQUACY_RECORD_PATH
    if sha256_file(source_path) != SOURCE_ADEQUACY_RECORD_SHA256:
        raise IntegrityError("source adequacy record changed")
    if sha256_file(root / PROTOCOL_PATH) != PROTOCOL_SHA256:
        raise IntegrityError("frozen trial protocol changed")
    protocol = _object(root / PROTOCOL_PATH)
    if protocol.get("protocol_id") != PROTOCOL_ID:
        raise IntegrityError("frozen trial protocol identity changed")
    sessions = v5.load_registered_calendar_sessions_v5(
        boundary=RepoBoundary(root),
        registered_calendar_index_release_id=CALENDAR_RELEASE_ID,
    )
    return _object(source_path), v5.build_expected_census_from_calendar(sessions=sessions)


def load_diagnostic_recovery_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    source, checkpoints = _load_frozen_inputs(root=root)
    targets = derive_gap_targets(source_record=source, expected_checkpoints=checkpoints)
    _, catalog = diagnostic_catalog(
        root=root, boundary=RepoBoundary(root), targets=targets,
    )
    forbidden = plan.get("forbidden_actions")
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version") != "tier1_frozen_diagnostic_recovery_plan/1.0.0"
        or plan.get("state") != "PREPARED_REQUIRES_SEPARATE_APPROVAL"
        or plan.get("operation") != OPERATION
        or plan.get("source_adequacy_record_id") != SOURCE_ADEQUACY_RECORD_ID
        or plan.get("source_adequacy_record_sha256") != SOURCE_ADEQUACY_RECORD_SHA256
        or plan.get("protocol_id") != PROTOCOL_ID
        or plan.get("protocol_sha256") != PROTOCOL_SHA256
        or plan.get("dbn_release_id") != DBN_RELEASE_ID
        or plan.get("dbn_manifest_sha256") != DBN_MANIFEST_SHA256
        or plan.get("calendar_release_id") != CALENDAR_RELEASE_ID
        or plan.get("target_count") != 34
        or plan.get("target_set_id") != sha256_json([item.as_dict() for item in targets])
        or plan.get("diagnostic_catalog_id") != sha256_json(catalog)
        or plan.get("diagnostic_decoder_sha256") != sha256_file(
            root / "src/futures_rebuild/foundation/decoder.py"
        )
        or plan.get("maximum_host_runtime_seconds") != MAXIMUM_HOST_RUNTIME_SECONDS
        or plan.get("estimated_external_cost_usd") != "0"
        or plan.get("implementation_sha256") != sha256_file(Path(__file__))
        or plan.get("source_contract_sha256") != _validate_source_contract(root=root)
        or not isinstance(forbidden, dict) or not forbidden
        or not all(value is True for value in forbidden.values())
    ):
        raise UnauthorizedOperation("diagnostic recovery plan is absent or drifted")
    return plan


def _required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "source_adequacy_record_id": SOURCE_ADEQUACY_RECORD_ID,
        "source_adequacy_record_sha256": SOURCE_ADEQUACY_RECORD_SHA256,
        "target_count": "34",
        "target_set_id": str(plan["target_set_id"]),
        "diagnostic_catalog_id": str(plan["diagnostic_catalog_id"]),
        "source_scope": "6E,CL,ES,ZN|2018,2019,2020,2021,2022|34-execution-gaps-only",
        "historical_row_read": "true", "publication": "true",
        "provider_access": "false", "successor_data_creation": "false",
        "active_data_mutation": "false", "protocol_change": "false",
        "model_fit": "false", "prediction_generation": "false",
        "historical_evaluation": "false", "trial_registration_or_retirement": "false",
        "holdout_or_forward_access": "false", "prices_reported": "false",
        "staging": "false", "commit": "false", "push": "false", "trading": "false",
        "publication_root": RECORD_ROOT.as_posix(),
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def execute_authorized_diagnostic_recovery(
    *, root: Path, authorization: OperationReceipt,
) -> dict[str, object]:
    boundary = RepoBoundary(root)
    plan = load_diagnostic_recovery_plan(root=root)
    require_locked_repository_environment(root)
    claim = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=_required_scope(root=root, plan=plan),
    )
    started = time.monotonic()
    source, checkpoints = _load_frozen_inputs(root=root)
    targets = derive_gap_targets(source_record=source, expected_checkpoints=checkpoints)
    release, catalog = diagnostic_catalog(root=root, boundary=boundary, targets=targets)
    by_cell: dict[tuple[str, int], list[DiagnosticGapTarget]] = {}
    for target in targets:
        by_cell.setdefault((target.market, target.year), []).append(target)
    cell_starts: dict[tuple[str, int], list[int]] = {}
    for cell, cell_targets in by_cell.items():
        cell_targets.sort(key=lambda item: item.window_start_ns)
        # Validate the indexing invariant before opening any diagnostic file.
        for index in range(1, len(cell_targets)):
            if (
                cell_targets[index - 1].window_end_exclusive_ns
                > cell_targets[index].window_start_ns
            ):
                raise IntegrityError("diagnostic target windows overlap within one source cell")
        cell_starts[cell] = [item.window_start_ns for item in cell_targets]
    observations: dict[str, dict[str, list[ProviderObservationHeader]]] = {
        item.opportunity_id: {schema: [] for schema in DIAGNOSTIC_SCHEMAS}
        for item in targets
    }
    audits: list[dict[str, object]] = []
    for item in catalog:
        if item["status"] == "SOURCE_FILE_ABSENT":
            audits.append({**item, "rows_scanned": 0, "matching_rows": 0})
            continue
        if time.monotonic() - started >= MAXIMUM_HOST_RUNTIME_SECONDS:
            raise IntegrityError("diagnostic recovery runtime limit reached before completion")
        market, year, schema = str(item["market"]), int(item["year"]), str(item["schema"])
        logical = PurePosixPath(str(item["relative_path"]))
        binding: DbnReleaseFile = release.dbn_file(
            schema=schema, market=market, year=year, filename=logical.name,
        )
        scanned = matched = 0
        scoped_targets = by_cell[(market, year)]
        scoped_starts = cell_starts[(market, year)]
        for observation in iter_observation_headers(
            binding, market=market,
            expected_query_contract=item["query_contract"], schema=schema,
        ):
            scanned += 1
            target_index = bisect_right(scoped_starts, observation.event_at_ns) - 1
            if (
                target_index >= 0
                and observation.event_at_ns
                < scoped_targets[target_index].window_end_exclusive_ns
            ):
                target = scoped_targets[target_index]
                observations[target.opportunity_id][schema].append(observation)
                matched += 1
        audits.append({
            "schema": schema, "market": market, "year": year,
            "status": "SCANNED_BOUND_IMMUTABLE_FILE",
            "source_file_sha256": item["file_sha256"],
            "rows_scanned": scanned, "matching_rows": matched,
            "target_count": len(scoped_targets),
        })
    recovery = [
        classify_target_observations(
            target=target, observations_by_schema=observations[target.opportunity_id],
        )
        for target in targets
    ]
    counts: dict[str, int] = {}
    for item in recovery:
        disposition = str(item["disposition"])
        counts[disposition] = counts.get(disposition, 0) + 1
    core = {
        "schema_version": "tier1_frozen_diagnostic_recovery/1.0.0",
        "state": "PREPARED_CREATE_ONLY",
        "plan_id": plan["plan_id"], "plan_sha256": sha256_file(root / PLAN_PATH),
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_claim_sha256": sha256_file(claim),
        "source_adequacy_record_id": SOURCE_ADEQUACY_RECORD_ID,
        "source_adequacy_record_sha256": SOURCE_ADEQUACY_RECORD_SHA256,
        "protocol_id": PROTOCOL_ID, "protocol_sha256": PROTOCOL_SHA256,
        "dbn_release_id": DBN_RELEASE_ID, "dbn_manifest_sha256": DBN_MANIFEST_SHA256,
        "calendar_release_id": CALENDAR_RELEASE_ID,
        "target_count": len(targets),
        "target_set_id": sha256_json([item.as_dict() for item in targets]),
        "diagnostic_catalog_id": sha256_json(catalog),
        "disposition_counts": dict(sorted(counts.items())),
        "recovery_map": recovery, "source_audit": audits,
        "interpretation": {
            "diagnostic_presence_is_not_research_authority": True,
            "recovery_requires_a_separately_authorized_immutable_successor_source": True,
            "ambiguous_identity_fails_closed": True,
            "absence_cannot_be_filled_or_synthesized": True,
        },
        "prices_reported": False, "provider_access": False,
        "successor_data_created": False, "active_data_mutation": False,
        "protocol_changed": False, "model_fit": False,
        "prediction_generation": False, "historical_evaluation": False,
        "trial_registration_or_retirement": False,
        "holdout_or_forward_access": False, "trading": False,
    }
    record_id = sha256_json(core)
    record = root / RECORD_ROOT / f"{record_id}.json"
    event = root / EVENT_ROOT / f"{record_id}.json"
    boundary.assert_active_path(
        record.absolute(), purpose="diagnostic recovery record",
        subtree=RECORD_ROOT.as_posix(),
    )
    boundary.assert_active_path(
        event.absolute(), purpose="diagnostic recovery event",
        subtree=EVENT_ROOT.as_posix(),
    )
    if record.exists() or event.exists():
        raise IntegrityError("diagnostic recovery publication is create-only")
    record.parent.mkdir(parents=True, exist_ok=True)
    event.parent.mkdir(parents=True, exist_ok=True)
    with record.open("xb") as stream:
        stream.write(canonical_bytes({
            **core, "state": "PUBLISHED_SOURCE_QUALITY_ONLY", "record_id": record_id,
        }) + b"\n")
    with event.open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_frozen_diagnostic_recovery_event/1.0.0",
            "event_type": "PUBLISHED", "record_id": record_id,
            "source_adequacy_record_id": SOURCE_ADEQUACY_RECORD_ID,
            "authorization_receipt_id": authorization.receipt_id,
        }) + b"\n")
    return {
        "record_id": record_id,
        "record_path": record.relative_to(root).as_posix(),
        "event_path": event.relative_to(root).as_posix(),
        "authorization_claim_path": claim.relative_to(root).as_posix(),
        "target_count": len(targets),
        "disposition_counts": dict(sorted(counts.items())),
        "runtime_seconds": time.monotonic() - started,
    }
