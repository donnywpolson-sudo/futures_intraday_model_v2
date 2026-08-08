"""Canonical-local recovery feasibility for the frozen Tier 1 source gaps.

The prepared operation checks only the accepted immutable Databento
``ohlcv-1m`` files for the exact dependency timestamps in the published gap
inventory.  It records identities and presence, never prices or performance.
Diagnostic one-second and trade families are deliberately excluded because
the current source contract does not authorize them as canonical research
inputs.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation
from .foundation.decoder import iter_bars
from .foundation.records import ProviderBar
from .foundation.snapshot import DbnReleaseFile, PublishedDbnRelease
from .runtime_environment import require_locked_repository_environment
from .source_symbology import build_query_contract
from .tier1_bracket_v5 import NS_PER_MINUTE, load_registered_calendar_sessions_v5


PLAN_PATH = Path("configs/tier1_preexecution_recovery_feasibility_plan.json")
GAP_RECORD_PATH = Path(
    "state/source_quality/tier1_preexecution_gap_inventory/"
    "874c2f97b76e8bc19077bc209bae58a4b07e09a629c823af10948e6021772d61.json"
)
GAP_RECORD_ID = GAP_RECORD_PATH.stem
GAP_RECORD_SHA256 = "74bdfa599e7b2f7e84f201dba3a6905cf595449f7007304ac394914140a1a167"
DBN_RELEASE_ID = "086282eaef7b36a61626f88d93d06c93b87c1cb3407c936d065d0d1b9d98599e"
DBN_MANIFEST_PATH = Path(f"manifests/data_releases/dbn/{DBN_RELEASE_ID}.json")
DBN_MANIFEST_SHA256 = "c2584d5e1a65103f8651a871de6f704ac31ec2c2f7ec5c2e1a941aae6a4dc8fd"
CALENDAR_RELEASE_ID = "038940d82031f31e2c66ed37186e98a6ee6cff3e7248f634f2c7a8e94ea6ecf3"
SOURCE_CONTRACT_PATH = Path("configs/source_contract.json")
OPERATION = "CENSUS_FROZEN_TIER1_LOCAL_CANONICAL_RECOVERY_AND_PUBLISH"
RECORD_ROOT = Path("state/source_quality/tier1_preexecution_recovery_feasibility")
EVENT_ROOT = Path("state/source_quality_events/tier1_preexecution_recovery_feasibility")
MARKETS = ("6E", "CL", "ES", "ZN")
YEARS = tuple(range(2018, 2023))


def _object(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"invalid recovery-feasibility artifact: {path.as_posix()}") from exc
    if not isinstance(value, dict):
        raise IntegrityError("recovery-feasibility artifact is not an object")
    return value


def _load_gap_record(*, root: Path) -> dict[str, object]:
    path = root / GAP_RECORD_PATH
    if sha256_file(path) != GAP_RECORD_SHA256:
        raise IntegrityError("published dependency-gap inventory changed")
    record = _object(path)
    if (
        record.get("record_id") != GAP_RECORD_ID
        or record.get("state") != "PUBLISHED_SOURCE_QUALITY_ONLY"
        or record.get("checkpoint_count") != 15_343
        or record.get("holdout_or_forward_access") is not False
        or record.get("historical_evaluation") is not False
    ):
        raise IntegrityError("dependency-gap inventory is not the certified record")
    return record


@dataclass(frozen=True)
class RecoveryTarget:
    market: str
    event_at_ns: int
    dependency_roles: tuple[str, ...]
    checkpoint_ids: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "market": self.market,
            "event_at_ns": self.event_at_ns,
            "dependency_roles": list(self.dependency_roles),
            "checkpoint_ids": list(self.checkpoint_ids),
        }


@dataclass(frozen=True)
class SessionRecoveryTarget:
    market: str
    exchange_session_date: str
    checkpoint_id: str
    checkpoint: str
    decision_at_ns: int

    def as_dict(self) -> dict[str, object]:
        return {
            "market": self.market,
            "exchange_session_date": self.exchange_session_date,
            "checkpoint_id": self.checkpoint_id,
            "checkpoint": self.checkpoint,
            "decision_at_ns": self.decision_at_ns,
        }


def build_recovery_targets(
    gap_record: Mapping[str, object],
) -> tuple[tuple[RecoveryTarget, ...], tuple[SessionRecoveryTarget, ...]]:
    """Build the exact timestamp union without consulting any provider rows."""

    inventory = gap_record.get("checkpoint_inventory")
    if not isinstance(inventory, list) or len(inventory) != gap_record.get("checkpoint_count"):
        raise IntegrityError("gap checkpoint inventory is incomplete")
    roles: dict[tuple[str, int], set[str]] = {}
    checkpoints: dict[tuple[str, int], set[str]] = {}
    session_targets: dict[str, SessionRecoveryTarget] = {}

    def add(market: str, event: object, role: str, checkpoint_id: str) -> None:
        if (
            market not in MARKETS or type(event) is not int or event <= 0
            or datetime.fromtimestamp(event / 1_000_000_000, tz=timezone.utc).year
            not in YEARS
        ):
            raise IntegrityError("gap recovery target identity is invalid")
        key = (market, event)
        roles.setdefault(key, set()).add(role)
        checkpoints.setdefault(key, set()).add(checkpoint_id)

    for item in inventory:
        if not isinstance(item, Mapping):
            raise IntegrityError("gap checkpoint entry is malformed")
        market = item.get("market")
        checkpoint_id = item.get("checkpoint_id")
        session_date = item.get("exchange_session_date")
        checkpoint = item.get("checkpoint")
        decision = item.get("decision_at_ns")
        reasons = item.get("reason_codes")
        if (
            not isinstance(market, str)
            or not isinstance(checkpoint_id, str)
            or not isinstance(session_date, str)
            or not isinstance(checkpoint, str)
            or type(decision) is not int
            or not isinstance(reasons, list)
        ):
            raise IntegrityError("gap checkpoint recovery identity is incomplete")
        if "MISSING_SOURCE_SESSION" in reasons:
            if checkpoint_id in session_targets:
                raise IntegrityError("missing-session checkpoint is duplicated")
            session_targets[checkpoint_id] = SessionRecoveryTarget(
                market, session_date, checkpoint_id, checkpoint, decision,
            )
        for field, role in (
            ("missing_feature_timestamps_ns", "MISSING_FEATURE_DEPENDENCY"),
            ("nonexecutable_feature_timestamps_ns", "NONEXECUTABLE_FEATURE_DEPENDENCY"),
            ("causally_late_feature_timestamps_ns", "CAUSALLY_LATE_FEATURE_DEPENDENCY"),
            ("identity_mismatch_feature_timestamps_ns", "IDENTITY_MISMATCH_FEATURE_DEPENDENCY"),
            ("missing_execution_timestamps_ns", "MISSING_EXECUTION_DEPENDENCY"),
            ("nonexecutable_execution_timestamps_ns", "NONEXECUTABLE_EXECUTION_DEPENDENCY"),
        ):
            values = item.get(field)
            if not isinstance(values, list):
                raise IntegrityError("gap checkpoint timestamp list is malformed")
            for event in values:
                add(market, event, role, checkpoint_id)
    timestamp_result = tuple(
        RecoveryTarget(
            market, event, tuple(sorted(roles[(market, event)])),
            tuple(sorted(checkpoints[(market, event)])),
        )
        for market, event in sorted(roles)
    )
    session_result = tuple(session_targets[key] for key in sorted(session_targets))
    if not timestamp_result and not session_result:
        raise IntegrityError("published gaps produce no recovery targets")
    return timestamp_result, session_result


def _validate_source_contract(*, root: Path) -> str:
    path = root / SOURCE_CONTRACT_PATH
    payload = _object(path)
    provider = payload.get("provider")
    families = payload.get("source_families")
    canonical = payload.get("canonical_dbn_release")
    ohlcv = [
        item for item in families if isinstance(item, Mapping)
        and item.get("id") == "dbn_ohlcv_1m"
    ] if isinstance(families, list) else []
    diagnostics = {
        str(item.get("id")): str(item.get("role"))
        for item in families if isinstance(item, Mapping)
        and item.get("id") in {"dbn_ohlcv_1s", "dbn_trades"}
    } if isinstance(families, list) else {}
    if (
        not isinstance(provider, Mapping)
        or provider.get("name") != "Databento"
        or provider.get("dataset") != "GLBX.MDP3"
        or provider.get("downloads_authorized") is not False
        or provider.get("paid_calls_authorized") is not False
        or len(ohlcv) != 1
        or ohlcv[0].get("schema") != "ohlcv-1m"
        or ohlcv[0].get("role") != "immutable_provider_source_and_canonical_research_input"
        or set(diagnostics) != {"dbn_ohlcv_1s", "dbn_trades"}
        or set(diagnostics.values()) != {"immutable_provider_source_diagnostic"}
        or not isinstance(canonical, Mapping)
        or canonical.get("release_id") != DBN_RELEASE_ID
        or canonical.get("manifest_sha256") != DBN_MANIFEST_SHA256
    ):
        raise IntegrityError("source contract does not preserve the canonical-only recovery boundary")
    return sha256_file(path)


def _query_contract_from_sidecar(
    *, sidecar: Mapping[str, object], market: str,
) -> dict[str, object]:
    if (
        sidecar.get("vendor") != "databento"
        or sidecar.get("dataset") != "GLBX.MDP3"
        or sidecar.get("schema") != "ohlcv-1m"
        or sidecar.get("market") != market
        or sidecar.get("stype_out") != "instrument_id"
        or sidecar.get("request_status") != "ok"
        or not isinstance(sidecar.get("symbols_requested"), list)
    ):
        raise IntegrityError("canonical one-minute sidecar is invalid")
    return build_query_contract(
        schema="ohlcv-1m", market=market,
        start=str(sidecar["start"]), end=str(sidecar["end"]),
        stype_in=str(sidecar["stype_in"]), symbols=sidecar["symbols_requested"],
    )


def canonical_ohlcv_catalog(
    *, root: Path, boundary: RepoBoundary,
) -> tuple[PublishedDbnRelease, tuple[dict[str, object], ...]]:
    manifest = root / DBN_MANIFEST_PATH
    if sha256_file(manifest) != DBN_MANIFEST_SHA256:
        raise IntegrityError("canonical DBN manifest changed")
    release = PublishedDbnRelease.open(manifest, boundary=boundary, verify_files=False)
    if release.source_release_id != DBN_RELEASE_ID:
        raise IntegrityError("canonical DBN release identity changed")
    items: list[dict[str, object]] = []
    for market in MARKETS:
        for year in YEARS:
            prefix = f"dbn/ohlcv_1m/{market}/{year}/"
            keys = sorted(
                key for key in release.files
                if key.startswith(prefix) and key.endswith(".dbn.zst")
            )
            if len(keys) != 1:
                raise IntegrityError("canonical DBN cell does not contain exactly one one-minute file")
            dbn_file = release.file(keys[0])
            sidecar_file = release.file(f"{keys[0]}.manifest.json")
            sidecar = _object(sidecar_file.verify())
            query = _query_contract_from_sidecar(sidecar=sidecar, market=market)
            logical = PurePosixPath(keys[0])
            expected_name = f"{year}-01-01_{year + 1}-01-01.dbn.zst"
            if logical.name != expected_name:
                raise IntegrityError("canonical DBN file does not cover its exact calendar year")
            items.append({
                "market": market,
                "year": year,
                "relative_path": keys[0],
                "file_sha256": dbn_file.sha256,
                "file_size": dbn_file.size,
                "sidecar_sha256": sidecar_file.sha256,
                "query_contract": query,
            })
    return release, tuple(items)


def classify_target_presence(
    *, targets: Sequence[RecoveryTarget], bars: Iterable[ProviderBar],
) -> tuple[dict[str, object], ...]:
    """Classify direct canonical presence while never serializing prices."""

    target_map = {(item.market, item.event_at_ns): item for item in targets}
    if len(target_map) != len(targets):
        raise IntegrityError("recovery target list is duplicated")
    matches: dict[tuple[str, int], list[ProviderBar]] = {key: [] for key in target_map}
    for bar in bars:
        key = (bar.market, bar.event_at_ns)
        if key in matches:
            matches[key].append(bar)
    output: list[dict[str, object]] = []
    for key in sorted(target_map):
        target = target_map[key]
        found = matches[key]
        disposition = (
            "CANONICAL_OHLCV_1M_ABSENT" if not found
            else "CANONICAL_OHLCV_1M_PRESENT" if len(found) == 1
            else "CANONICAL_OHLCV_1M_AMBIGUOUS"
        )
        output.append({
            **target.as_dict(),
            "disposition": disposition,
            "canonical_row_identities": sorted({
                sha256_json({
                    "publisher_id": bar.publisher_id,
                    "instrument_id": bar.instrument_id,
                    "row_sha256": bar.row_sha256,
                    "source_file_sha256": bar.source_file_sha256,
                }) for bar in found
            }),
        })
    return tuple(output)


def classify_session_checkpoint_presence(
    *, target: SessionRecoveryTarget, bars: Sequence[ProviderBar],
) -> dict[str, object]:
    """Resolve dependencies using the canonical rows, never a guessed anchor."""

    by_event: dict[int, list[ProviderBar]] = {}
    for bar in bars:
        if bar.market != target.market:
            raise IntegrityError("session recovery received a foreign market bar")
        by_event.setdefault(bar.event_at_ns, []).append(bar)
    duplicates = sorted(event for event, values in by_event.items() if len(values) != 1)
    if duplicates:
        return {
            **target.as_dict(),
            "disposition": "CANONICAL_SESSION_CHECKPOINT_AMBIGUOUS",
            "reason_codes": ["DUPLICATE_CANONICAL_EVENT_TIMESTAMP"],
            "feature_anchor_at_ns": None,
            "missing_feature_timestamps_ns": [],
            "identity_mismatch_feature_timestamps_ns": [],
            "missing_execution_timestamps_ns": [],
            "duplicate_timestamps_ns": duplicates,
        }
    causal_events = sorted(
        event for event in by_event
        if event + NS_PER_MINUTE + 5_000_000_000 <= target.decision_at_ns
    )
    anchor = causal_events[-1] if causal_events else None
    missing_feature: list[int] = []
    identity_mismatch: list[int] = []
    if anchor is not None:
        required_feature = {
            anchor - offset * NS_PER_MINUTE for offset in range(61)
        }
        anchor_bar = by_event[anchor][0]
        anchor_identity = (anchor_bar.publisher_id, anchor_bar.instrument_id)
        for event in sorted(required_feature):
            values = by_event.get(event)
            if values is None:
                missing_feature.append(event)
            elif (values[0].publisher_id, values[0].instrument_id) != anchor_identity:
                identity_mismatch.append(event)
    required_execution = {
        target.decision_at_ns + offset * NS_PER_MINUTE for offset in range(1, 62)
    }
    missing_execution = sorted(required_execution - set(by_event))
    reasons: list[str] = []
    if anchor is None:
        reasons.append("NO_CANONICAL_CAUSAL_FEATURE_ANCHOR")
    if missing_feature:
        reasons.append("CANONICAL_FEATURE_TIMESTAMPS_ABSENT")
    if identity_mismatch:
        reasons.append("CANONICAL_FEATURE_IDENTITY_CHANGE")
    if missing_execution:
        reasons.append("CANONICAL_EXECUTION_TIMESTAMPS_ABSENT")
    return {
        **target.as_dict(),
        "disposition": (
            "CANONICAL_SESSION_CHECKPOINT_DEPENDENCIES_PRESENT"
            if not reasons else "CANONICAL_SESSION_CHECKPOINT_DEPENDENCIES_INCOMPLETE"
        ),
        "reason_codes": reasons,
        "feature_anchor_at_ns": anchor,
        "missing_feature_timestamps_ns": missing_feature,
        "identity_mismatch_feature_timestamps_ns": identity_mismatch,
        "missing_execution_timestamps_ns": missing_execution,
        "duplicate_timestamps_ns": [],
    }


def load_recovery_feasibility_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH)
    core = dict(plan)
    plan_id = core.pop("plan_id", None)
    forbidden = plan.get("forbidden_actions")
    gap = _load_gap_record(root=root)
    timestamp_targets, session_targets = build_recovery_targets(gap)
    target_set = {
        "timestamp_targets": [item.as_dict() for item in timestamp_targets],
        "session_checkpoint_targets": [item.as_dict() for item in session_targets],
    }
    boundary = RepoBoundary(root)
    _, catalog = canonical_ohlcv_catalog(root=root, boundary=boundary)
    if (
        plan_id != sha256_json(core)
        or plan.get("schema_version") != "tier1_preexecution_recovery_feasibility_plan/1.0.0"
        or plan.get("state") != "PREPARED_REQUIRES_SEPARATE_APPROVAL"
        or plan.get("operation") != OPERATION
        or plan.get("gap_record_id") != GAP_RECORD_ID
        or plan.get("gap_record_sha256") != GAP_RECORD_SHA256
        or plan.get("timestamp_target_count") != len(timestamp_targets)
        or plan.get("session_checkpoint_target_count") != len(session_targets)
        or plan.get("recovery_target_set_id") != sha256_json(target_set)
        or plan.get("dbn_release_id") != DBN_RELEASE_ID
        or plan.get("dbn_manifest_sha256") != DBN_MANIFEST_SHA256
        or plan.get("calendar_release_id") != CALENDAR_RELEASE_ID
        or plan.get("canonical_file_count") != 20
        or plan.get("canonical_file_catalog_id") != sha256_json(catalog)
        or plan.get("source_contract_sha256") != _validate_source_contract(root=root)
        or plan.get("maximum_host_runtime_seconds") != 900
        or plan.get("estimated_external_cost_usd") != "0"
        or plan.get("implementation_sha256") != sha256_file(Path(__file__))
        or not isinstance(forbidden, dict)
        or not forbidden
        or not all(value is True for value in forbidden.values())
    ):
        raise UnauthorizedOperation("recovery-feasibility plan is absent or drifted")
    return plan


def _required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    return {
        "gap_record_id": GAP_RECORD_ID,
        "gap_record_sha256": GAP_RECORD_SHA256,
        "timestamp_target_count": str(plan["timestamp_target_count"]),
        "session_checkpoint_target_count": str(plan["session_checkpoint_target_count"]),
        "recovery_target_set_id": str(plan["recovery_target_set_id"]),
        "dbn_release_id": DBN_RELEASE_ID,
        "calendar_release_id": CALENDAR_RELEASE_ID,
        "canonical_file_count": "20",
        "source_scope": "6E,CL,ES,ZN|2018,2019,2020,2021,2022|ohlcv-1m",
        "publication_root": RECORD_ROOT.as_posix(),
        "historical_row_read": "true",
        "publication": "true",
        "provider_access": "false",
        "diagnostic_source_families": "false",
        "successor_data_creation": "false",
        "active_data_mutation": "false",
        "model_fit": "false",
        "prediction_generation": "false",
        "historical_evaluation": "false",
        "trial_registration_or_retirement": "false",
        "holdout_or_forward_access": "false",
        "staging": "false",
        "commit": "false",
        "push": "false",
        "trading": "false",
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": sha256_file(root / PLAN_PATH),
    }


def execute_authorized_recovery_feasibility(
    *, root: Path, authorization: OperationReceipt,
) -> dict[str, object]:
    """Consume one approval, read 20 canonical files, and publish one map."""

    boundary = RepoBoundary(root)
    plan = load_recovery_feasibility_plan(root=root)
    require_locked_repository_environment(root)
    claim = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=_required_scope(root=root, plan=plan),
    )
    gap = _load_gap_record(root=root)
    timestamp_targets, session_targets = build_recovery_targets(gap)
    target_set = {
        "timestamp_targets": [item.as_dict() for item in timestamp_targets],
        "session_checkpoint_targets": [item.as_dict() for item in session_targets],
    }
    target_keys = {(item.market, item.event_at_ns) for item in timestamp_targets}
    calendar_sessions = load_registered_calendar_sessions_v5(
        boundary=boundary, registered_calendar_index_release_id=CALENDAR_RELEASE_ID,
    )
    calendar_map = {
        (item.market, item.exchange_session_date): item for item in calendar_sessions
    }
    session_keys = {
        (item.market, item.exchange_session_date) for item in session_targets
    }
    if not session_keys <= set(calendar_map):
        raise IntegrityError("missing-session recovery target is absent from the calendar")
    session_bars: dict[tuple[str, str], list[ProviderBar]] = {
        key: [] for key in session_keys
    }
    release, catalog = canonical_ohlcv_catalog(root=root, boundary=boundary)
    all_bars: list[ProviderBar] = []
    source_audit: list[dict[str, object]] = []
    for item in catalog:
        market, year = str(item["market"]), int(item["year"])
        scoped = {
            event for target_market, event in target_keys
            if target_market == market
            and datetime.fromtimestamp(event / 1_000_000_000, tz=timezone.utc).year == year
        }
        scoped_sessions = [
            (key, calendar_map[key]) for key in sorted(session_keys)
            if key[0] == market
        ]
        binding: DbnReleaseFile = release.dbn_file(
            schema="ohlcv-1m", market=market, year=year,
            filename=PurePosixPath(str(item["relative_path"])).name,
        )
        scanned = matched = 0
        for bar in iter_bars(
            binding, market=market,
            expected_query_contract=item["query_contract"], schema="ohlcv-1m",
        ):
            scanned += 1
            if bar.event_at_ns in scoped:
                all_bars.append(bar)
                matched += 1
            for key, session in scoped_sessions:
                if session.open_at_ns <= bar.event_at_ns < session.close_at_ns:
                    session_bars[key].append(bar)
        source_audit.append({
            "market": market, "year": year,
            "source_file_sha256": item["file_sha256"],
            "rows_scanned": scanned, "target_timestamps": len(scoped),
            "matching_rows": matched,
            "target_sessions": sum(
                1 for key, _ in scoped_sessions
                if datetime.fromisoformat(key[1]).year == year
            ),
        })
    timestamp_recovery = classify_target_presence(
        targets=timestamp_targets, bars=all_bars,
    )
    session_recovery = tuple(
        classify_session_checkpoint_presence(
            target=target,
            bars=tuple(session_bars[(target.market, target.exchange_session_date)]),
        )
        for target in session_targets
    )
    counts: dict[str, int] = {}
    for item in (*timestamp_recovery, *session_recovery):
        value = str(item["disposition"])
        counts[value] = counts.get(value, 0) + 1
    core = {
        "schema_version": "tier1_preexecution_recovery_feasibility/1.0.0",
        "state": "PREPARED_CREATE_ONLY",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_claim_sha256": sha256_file(claim),
        "gap_record_id": GAP_RECORD_ID,
        "gap_record_sha256": GAP_RECORD_SHA256,
        "timestamp_target_count": len(timestamp_targets),
        "session_checkpoint_target_count": len(session_targets),
        "recovery_target_set_id": sha256_json(target_set),
        "dbn_release_id": DBN_RELEASE_ID,
        "dbn_manifest_sha256": DBN_MANIFEST_SHA256,
        "calendar_release_id": CALENDAR_RELEASE_ID,
        "canonical_file_catalog_id": sha256_json(catalog),
        "disposition_counts": dict(sorted(counts.items())),
        "timestamp_recovery_map": list(timestamp_recovery),
        "session_checkpoint_recovery_map": list(session_recovery),
        "source_audit": source_audit,
        "interpretation": {
            "canonical_present": "DOWNSTREAM_MATERIALIZATION_OR_CAUSAL_QUALIFICATION_REMEDIATION_CANDIDATE",
            "canonical_absent": "NOT_RECOVERABLE_FROM_CURRENT_CANONICAL_OHLCV_1M_RELEASE",
            "canonical_ambiguous": "FAIL_CLOSED",
            "diagnostic_families_not_used_as_research_authority": True,
        },
        "prices_reported": False,
        "provider_access": False,
        "diagnostic_source_families_read": False,
        "successor_data_created": False,
        "active_data_mutation": False,
        "model_fit": False,
        "prediction_generation": False,
        "historical_evaluation": False,
        "trial_registration_or_retirement": False,
        "holdout_or_forward_access": False,
        "trading": False,
    }
    record_id = sha256_json(core)
    record = root / RECORD_ROOT / f"{record_id}.json"
    event = root / EVENT_ROOT / f"{record_id}.json"
    boundary.assert_active_path(
        record.absolute(), purpose="canonical recovery feasibility map",
        subtree=RECORD_ROOT.as_posix(),
    )
    boundary.assert_active_path(
        event.absolute(), purpose="canonical recovery feasibility event",
        subtree=EVENT_ROOT.as_posix(),
    )
    if record.exists() or event.exists():
        raise IntegrityError("recovery-feasibility publication is create-only")
    record.parent.mkdir(parents=True, exist_ok=True)
    event.parent.mkdir(parents=True, exist_ok=True)
    with record.open("xb") as stream:
        stream.write(canonical_bytes({
            **core, "state": "PUBLISHED_SOURCE_QUALITY_ONLY", "record_id": record_id,
        }) + b"\n")
    with event.open("xb") as stream:
        stream.write(canonical_bytes({
            "schema_version": "tier1_preexecution_recovery_feasibility_event/1.0.0",
            "event_type": "PUBLISHED", "record_id": record_id,
            "gap_record_id": GAP_RECORD_ID,
            "authorization_receipt_id": authorization.receipt_id,
        }) + b"\n")
    return {
        "record_id": record_id,
        "record_path": record.relative_to(root).as_posix(),
        "event_path": event.relative_to(root).as_posix(),
        "authorization_claim_path": claim.relative_to(root).as_posix(),
        "timestamp_target_count": len(timestamp_targets),
        "session_checkpoint_target_count": len(session_targets),
        "disposition_counts": dict(sorted(counts.items())),
    }
