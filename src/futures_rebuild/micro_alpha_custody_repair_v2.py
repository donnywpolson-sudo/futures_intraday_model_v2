"""Binding-complete one-use repair for the v24 Windows hard-link defect.

The only permitted mutation is removal of the exact 320 staging aliases.  DBN
bytes are hash-verified before and after each alias removal but are never
decoded.  Final custody paths are never deleted, overwritten, or replaced.
"""

from __future__ import annotations

import json
import os
import stat
import time
from collections.abc import Callable, Mapping
from pathlib import Path

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import (
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    contained_path,
    sha256_file,
    sha256_json,
)
from .errors import IntegrityError, UnauthorizedOperation


OPERATION = "REPAIR_APEX_MICRO_V24_HARDLINK_CUSTODY_V2_ONCE"
V1_PLAN_PATH = Path("configs/apex_micro_tier01_v24_custody_repair_plan_v1.json")
PLAN_PATH = Path("configs/apex_micro_tier01_v24_custody_repair_plan_v2.json")
V24_PLAN_PATH = Path("configs/apex_micro_tier01_phase1a_acquisition_plan_v24.json")
V24_STAGING_ROOT = Path("state/provider_acquisition_staging/apex_micro_tier01_v24")
FAILURE_REPORT_PATH = Path(
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_v24_verification_failure/report.json"
)
V1_SUPERSESSION_PATH = Path(
    "state/unpublished_evidence/apex_micro_v24_custody_repair_v1_supersession/report.json"
)
AUDIT_PATH = Path(
    "state/unpublished_evidence/apex_micro_v24_custody_repair_plan_v2/audit.json"
)
REPAIR_TERMINAL_PATH = Path(
    "state/unpublished_evidence/apex_micro_v24_custody_repair_v2/terminal.json"
)
PLAN_SCHEMA_VERSION = "apex_micro_v24_custody_repair_plan/2.0.0"
AUDIT_SCHEMA_VERSION = "apex_micro_v24_custody_repair_audit/2.0.0"
TERMINAL_SCHEMA_VERSION = "apex_micro_v24_custody_repair_terminal/2.0.0"
MAXIMUM_RUNTIME_SECONDS = 7_200
EXPECTED_REQUESTS = 160
EXPECTED_ALIAS_REMOVALS = 320


def _object(path: Path, description: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise IntegrityError(f"{description} is unreadable") from exc
    if type(value) is not dict:
        raise IntegrityError(f"{description} must be a JSON object")
    return value


def _self_hashed(value: Mapping[str, object], key: str) -> bool:
    core = dict(value)
    identifier = core.pop(key, None)
    return type(identifier) is str and identifier == sha256_json(core)


def _git_head(root: Path) -> str:
    import subprocess

    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _implementation_sha256() -> str:
    return sha256_file(Path(__file__))


def _same_file(first: Path, second: Path) -> bool:
    left = first.stat()
    right = second.stat()
    return left.st_dev == right.st_dev and left.st_ino == right.st_ino


def _mark_read_only(path: Path) -> None:
    path.chmod(stat.S_IREAD)


def _make_writable(path: Path) -> None:
    path.chmod(stat.S_IREAD | stat.S_IWRITE)


def _is_read_only(path: Path) -> bool:
    info = path.stat()
    attributes = getattr(info, "st_file_attributes", None)
    if attributes is not None:
        return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_READONLY", 0x1))
    return not bool(info.st_mode & stat.S_IWRITE)


def _write_create_only(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(value) + b"\n")
    _mark_read_only(path)


def build_v1_supersession_report(*, root: Path) -> dict[str, object]:
    """Classify v1 without changing or authorizing its immutable plan."""

    root = root.resolve(strict=True)
    v1_path = root / V1_PLAN_PATH
    v1 = _object(v1_path, "v1 custody repair plan")
    if (
        not _self_hashed(v1, "plan_id")
        or v1.get("state") != "PREPARED_NOT_EXECUTED_EXACT_CUSTODY_REPAIR"
        or v1.get("operation") != "REPAIR_APEX_MICRO_V24_HARDLINK_CUSTODY_ONCE"
    ):
        raise IntegrityError("v1 custody repair plan identity or state is invalid")
    repairs = v1.get("repairs")
    if type(repairs) is not list or len(repairs) != EXPECTED_ALIAS_REMOVALS:
        raise IntegrityError("v1 custody repair plan does not bind the exact alias count")
    core = {
        "schema_version": "apex_micro_v24_custody_repair_v1_supersession/1.0.0",
        "state": "SUPERSEDED_PREPARATION_INCOMPLETE_EXECUTION_BINDINGS",
        "predecessor_plan_id": v1["plan_id"],
        "predecessor_plan_sha256": sha256_file(v1_path),
        "predecessor_committed_head": v1["committed_head"],
        "predecessor_operation": v1["operation"],
        "exact_alias_records_preserved": len(repairs),
        "missing_mandatory_controls": [
            "EXECUTION_TIME_IMPLEMENTATION_HASH_RECHECK",
            "EXECUTION_TIME_SEALED_V24_PLAN_HASH_RECHECK",
            "EXECUTION_TIME_SEALED_V24_TERMINAL_HASH_RECHECK",
            "EXECUTION_TIME_FAILURE_REPORT_HASH_RECHECK",
            "PRE_MUTATION_DBN_SHA256_VERIFICATION",
            "FROZEN_SIDECAR_MANIFEST_ID_PER_ALIAS",
            "FAILURE_PATH_READ_ONLY_RESTORATION_PROOF",
        ],
        "authority_and_effects": {
            "authorization_issued": False,
            "authorization_consumed": False,
            "provider_calls": 0,
            "staging_aliases_removed": 0,
            "final_files_deleted_overwritten_or_replaced": 0,
            "dbn_payload_bytes_read": 0,
            "dbn_rows_decoded": 0,
            "catalog_or_pointer_activated": False,
            "published_registered_evaluated_or_traded": False,
        },
        "successor_requirement": "BINDING_COMPLETE_V2_REPAIR_AND_SEPARATE_APPROVAL",
    }
    return {**core, "report_id": sha256_json(core)}


def write_v1_supersession_report_create_only(*, root: Path) -> dict[str, object]:
    report = build_v1_supersession_report(root=root)
    _write_create_only(root / V1_SUPERSESSION_PATH, report)
    return report


def _load_bound_evidence(root: Path) -> dict[str, dict[str, object]]:
    supersession = _object(root / V1_SUPERSESSION_PATH, "v1 supersession report")
    failure = _object(root / FAILURE_REPORT_PATH, "v24 verification failure report")
    v24_plan = _object(root / V24_PLAN_PATH, "v24 acquisition plan")
    terminal_path = contained_path(root, str(failure.get("v24_terminal_path", "")))
    v24_terminal = _object(terminal_path, "v24 acquisition terminal")
    if (
        not _self_hashed(supersession, "report_id")
        or supersession.get("state")
        != "SUPERSEDED_PREPARATION_INCOMPLETE_EXECUTION_BINDINGS"
        or not _self_hashed(failure, "report_id")
        or failure.get("state")
        != "FAIL_CLOSED_FINAL_CUSTODY_HARDLINK_REPAIR_REQUIRED"
        or not _self_hashed(v24_plan, "plan_id")
        or not _self_hashed(v24_terminal, "terminal_id")
        or v24_terminal.get("state") != "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY"
    ):
        raise IntegrityError("sealed custody evidence identity or state is invalid")
    if (
        supersession.get("predecessor_plan_id")
        != _object(root / V1_PLAN_PATH, "v1 plan").get("plan_id")
        or supersession.get("predecessor_plan_sha256")
        != sha256_file(root / V1_PLAN_PATH)
        or failure.get("v24_plan_id") != v24_plan.get("plan_id")
        or failure.get("v24_plan_sha256") != sha256_file(root / V24_PLAN_PATH)
        or failure.get("v24_terminal_id") != v24_terminal.get("terminal_id")
        or failure.get("v24_terminal_sha256") != sha256_file(terminal_path)
    ):
        raise IntegrityError("sealed custody evidence cross-binding is invalid")
    return {
        "supersession": supersession,
        "failure": failure,
        "v24_plan": v24_plan,
        "v24_terminal": v24_terminal,
    }


def _topology_record(
    *, root: Path, item: Mapping[str, object], require_two_links: bool
) -> tuple[Path, Path, os.stat_result, os.stat_result]:
    staging = contained_path(root, str(item.get("staging_path", "")))
    final = contained_path(root, str(item.get("final_path", "")))
    assert_no_linklike_ancestors(staging)
    assert_no_linklike_ancestors(final)
    left = assert_plain_file(staging, reject_hardlinks=False)
    right = assert_plain_file(final, reject_hardlinks=False)
    if (
        (require_two_links and (left.st_nlink != 2 or right.st_nlink != 2))
        or not _same_file(staging, final)
        or right.st_size != item.get("byte_count")
    ):
        raise IntegrityError("custody repair topology or byte count drifted")
    return staging, final, left, right


def build_repair_plan(*, root: Path, implementation_head: str) -> dict[str, object]:
    """Freeze exact hashes and identities without reading any DBN payload."""

    root = root.resolve(strict=True)
    if implementation_head != _git_head(root):
        raise IntegrityError("repair plan must bind the live implementation HEAD")
    evidence = _load_bound_evidence(root)
    failure = evidence["failure"]
    v24_terminal = evidence["v24_terminal"]
    topology = failure.get("topology")
    accepted_files = v24_terminal.get("accepted_files")
    if (
        type(topology) is not list
        or len(topology) != EXPECTED_ALIAS_REMOVALS
        or type(accepted_files) is not list
        or len(accepted_files) != EXPECTED_REQUESTS
    ):
        raise IntegrityError("sealed v24 evidence does not contain the exact records")
    accepted_by_id = {
        str(item.get("request_id")): item
        for item in accepted_files
        if type(item) is dict
    }
    repairs: list[dict[str, object]] = []
    dbn_count = 0
    sidecar_count = 0
    for raw in topology:
        if type(raw) is not dict:
            raise IntegrityError("v24 failure topology record is malformed")
        request_id = str(raw.get("request_id", ""))
        accepted = accepted_by_id.get(request_id)
        if accepted is None:
            raise IntegrityError("repair record is absent from sealed accepted files")
        kind = raw.get("kind")
        record = {
            "request_id": request_id,
            "kind": kind,
            "staging_path": raw.get("staging_path"),
            "final_path": raw.get("final_path"),
            "byte_count": raw.get("byte_count"),
            "expected_link_count_before_repair": 2,
            "expected_link_count_after_repair": 1,
        }
        if kind == "dbn":
            if (
                raw.get("byte_count") != accepted.get("byte_count")
                or type(accepted.get("sha256")) is not str
            ):
                raise IntegrityError("sealed DBN size or hash binding is invalid")
            record["expected_sha256"] = accepted["sha256"]
            dbn_count += 1
        elif kind == "sidecar":
            final = contained_path(root, str(raw.get("final_path", "")))
            sidecar = _object(final, "inactive sidecar")
            if (
                not _self_hashed(sidecar, "manifest_id")
                or sidecar.get("request_id") != request_id
                or sidecar.get("plan_id") != evidence["v24_plan"].get("plan_id")
                or sidecar.get("sha256") != accepted.get("sha256")
                or final.stat().st_size != raw.get("byte_count")
            ):
                raise IntegrityError("sealed sidecar identity or binding is invalid")
            record["expected_manifest_id"] = sidecar["manifest_id"]
            record["expected_sidecar_sha256"] = sha256_file(
                final, reject_hardlinks=False
            )
            sidecar_count += 1
        else:
            raise IntegrityError("repair kind is not dbn or sidecar")
        repairs.append(record)
    if dbn_count != EXPECTED_REQUESTS or sidecar_count != EXPECTED_REQUESTS:
        raise IntegrityError("repair plan does not have exact DBN and sidecar counts")
    supersession = evidence["supersession"]
    terminal_path = contained_path(root, str(failure["v24_terminal_path"]))
    core = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "state": "PREPARED_NOT_EXECUTED_BINDING_COMPLETE_CUSTODY_REPAIR",
        "operation": OPERATION,
        "implementation_head": implementation_head,
        "implementation_sha256": _implementation_sha256(),
        "v1_supersession_report_id": supersession["report_id"],
        "v1_supersession_report_sha256": sha256_file(root / V1_SUPERSESSION_PATH),
        "failure_report_id": failure["report_id"],
        "failure_report_sha256": sha256_file(root / FAILURE_REPORT_PATH),
        "v24_plan_id": evidence["v24_plan"]["plan_id"],
        "v24_plan_sha256": sha256_file(root / V24_PLAN_PATH),
        "v24_terminal_id": evidence["v24_terminal"]["terminal_id"],
        "v24_terminal_sha256": sha256_file(terminal_path),
        "v24_terminal_path": failure["v24_terminal_path"],
        "repairs": repairs,
        "limits": {
            "exact_request_count": EXPECTED_REQUESTS,
            "exact_dbn_alias_removal_count": EXPECTED_REQUESTS,
            "exact_sidecar_alias_removal_count": EXPECTED_REQUESTS,
            "exact_total_alias_removal_count": EXPECTED_ALIAS_REMOVALS,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_provider_calls": 0,
            "maximum_external_cost_usd": "0",
            "maximum_attempts": 1,
            "maximum_retries": 0,
        },
        "effects": {
            "remove_only_exact_bound_staging_aliases": True,
            "delete_overwrite_replace_or_relabel_final_files": False,
            "provider_or_network_access": False,
            "decode_dbn_rows": False,
            "open_2025_or_2026_payloads_for_row_access": False,
            "publish_activate_register_evaluate_trade_or_cleanup_other_data": False,
            "pre_and_post_mutation_dbn_sha256_verification_without_decoding": True,
            "pre_and_post_mutation_sidecar_identity_verification": True,
            "restore_final_read_only_state_on_success_or_failure": True,
            "stop_after_first_failure": True,
        },
        "audit_path": AUDIT_PATH.as_posix(),
        "terminal_path": REPAIR_TERMINAL_PATH.as_posix(),
    }
    return {**core, "plan_id": sha256_json(core)}


def write_repair_plan_create_only(
    *, root: Path, implementation_head: str
) -> dict[str, object]:
    plan = build_repair_plan(root=root, implementation_head=implementation_head)
    _write_create_only(root / PLAN_PATH, plan)
    return plan


def load_repair_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH, "v2 custody repair plan")
    if (
        not _self_hashed(plan, "plan_id")
        or plan.get("schema_version") != PLAN_SCHEMA_VERSION
        or plan.get("operation") != OPERATION
        or plan.get("state")
        != "PREPARED_NOT_EXECUTED_BINDING_COMPLETE_CUSTODY_REPAIR"
    ):
        raise IntegrityError("v2 custody repair plan identity or state is invalid")
    return plan


def _validate_plan_bindings(*, root: Path, plan: Mapping[str, object]) -> None:
    if _git_head(root) != plan.get("implementation_head"):
        raise UnauthorizedOperation("custody repair implementation HEAD drifted")
    if _implementation_sha256() != plan.get("implementation_sha256"):
        raise IntegrityError("custody repair implementation hash drifted")
    evidence = _load_bound_evidence(root)
    failure = evidence["failure"]
    terminal_path = contained_path(root, str(plan.get("v24_terminal_path", "")))
    expected = {
        "v1_supersession_report_id": evidence["supersession"].get("report_id"),
        "v1_supersession_report_sha256": sha256_file(root / V1_SUPERSESSION_PATH),
        "failure_report_id": failure.get("report_id"),
        "failure_report_sha256": sha256_file(root / FAILURE_REPORT_PATH),
        "v24_plan_id": evidence["v24_plan"].get("plan_id"),
        "v24_plan_sha256": sha256_file(root / V24_PLAN_PATH),
        "v24_terminal_id": evidence["v24_terminal"].get("terminal_id"),
        "v24_terminal_sha256": sha256_file(terminal_path),
    }
    if any(plan.get(key) != value for key, value in expected.items()):
        raise IntegrityError("custody repair sealed evidence binding drifted")


def _validate_pre_repair_topology(*, root: Path, plan: Mapping[str, object]) -> None:
    repairs = plan.get("repairs")
    if type(repairs) is not list or len(repairs) != EXPECTED_ALIAS_REMOVALS:
        raise IntegrityError("custody repair alias count drifted")
    seen: set[tuple[str, str]] = set()
    dbn_count = 0
    sidecar_count = 0
    for item in repairs:
        if type(item) is not dict:
            raise IntegrityError("custody repair record is malformed")
        key = (str(item.get("request_id")), str(item.get("kind")))
        if key in seen:
            raise IntegrityError("custody repair record is duplicated")
        seen.add(key)
        _topology_record(root=root, item=item, require_two_links=True)
        if item.get("kind") == "dbn" and type(item.get("expected_sha256")) is str:
            dbn_count += 1
        elif (
            item.get("kind") == "sidecar"
            and type(item.get("expected_manifest_id")) is str
            and type(item.get("expected_sidecar_sha256")) is str
        ):
            sidecar_count += 1
        else:
            raise IntegrityError("custody repair expected identity is incomplete")
    if dbn_count != EXPECTED_REQUESTS or sidecar_count != EXPECTED_REQUESTS:
        raise IntegrityError("custody repair kind counts drifted")


def build_plan_audit(*, root: Path) -> dict[str, object]:
    """Audit bindings and topology without reading DBN payload bytes."""

    root = root.resolve(strict=True)
    plan = load_repair_plan(root=root)
    if build_repair_plan(
        root=root, implementation_head=str(plan["implementation_head"])
    ) != plan:
        raise IntegrityError("v2 custody repair plan reconstruction differs")
    _validate_plan_bindings(root=root, plan=plan)
    _validate_pre_repair_topology(root=root, plan=plan)
    core = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "state": "PASS_SOURCE_SAFE_EXACT_CUSTODY_REPAIR_PLAN",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "implementation_head": plan["implementation_head"],
        "implementation_sha256": plan["implementation_sha256"],
        "exact_alias_pair_count": EXPECTED_ALIAS_REMOVALS,
        "exact_dbn_alias_pair_count": EXPECTED_REQUESTS,
        "exact_sidecar_alias_pair_count": EXPECTED_REQUESTS,
        "all_pairs_same_file_identity_with_two_links": True,
        "all_sealed_evidence_bindings_match": True,
        "dbn_payload_bytes_read_by_audit": 0,
        "dbn_rows_decoded": 0,
        "provider_calls": 0,
        "mutation_performed": False,
        "next_boundary": "SEPARATE_EXACT_V2_CUSTODY_REPAIR_APPROVAL",
    }
    return {**core, "audit_id": sha256_json(core)}


def write_plan_audit_create_only(*, root: Path) -> dict[str, object]:
    audit = build_plan_audit(root=root)
    _write_create_only(root / AUDIT_PATH, audit)
    return audit


def _load_and_validate_audit(root: Path, plan: Mapping[str, object]) -> dict[str, object]:
    audit = _object(contained_path(root, str(plan.get("audit_path", ""))), "v2 audit")
    if (
        not _self_hashed(audit, "audit_id")
        or audit.get("schema_version") != AUDIT_SCHEMA_VERSION
        or audit.get("state") != "PASS_SOURCE_SAFE_EXACT_CUSTODY_REPAIR_PLAN"
        or audit.get("plan_id") != plan.get("plan_id")
        or audit.get("plan_sha256") != sha256_file(root / PLAN_PATH)
        or audit.get("implementation_head") != plan.get("implementation_head")
        or audit.get("implementation_sha256") != plan.get("implementation_sha256")
        or audit.get("dbn_payload_bytes_read_by_audit") != 0
        or audit.get("mutation_performed") is not False
    ):
        raise IntegrityError("v2 custody repair audit binding is invalid")
    return audit


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    audit = _load_and_validate_audit(root, plan)
    plan_sha256 = sha256_file(root / PLAN_PATH)
    return {
        "operation": OPERATION,
        "plan_id": str(plan["plan_id"]),
        "plan_sha256": plan_sha256,
        "audit_id": str(audit["audit_id"]),
        "audit_sha256": sha256_file(root / AUDIT_PATH),
        "implementation_head": str(plan["implementation_head"]),
        "implementation_sha256": str(plan["implementation_sha256"]),
        "failure_report_id": str(plan["failure_report_id"]),
        "v24_terminal_id": str(plan["v24_terminal_id"]),
        "exact_alias_removals": str(EXPECTED_ALIAS_REMOVALS),
        "maximum_provider_calls": "0",
        "maximum_external_cost_usd": "0",
        "maximum_attempts": "1",
        "maximum_retries": "0",
        "dbn_rows_decoded": "0",
        "activation_permitted": "false",
        "terminal_path": str(plan["terminal_path"]),
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": plan_sha256,
    }


def _write_terminal(path: Path, core: Mapping[str, object]) -> dict[str, object]:
    terminal = {**core, "terminal_id": sha256_json(core)}
    _write_create_only(path, terminal)
    return terminal


def _verify_record_before_mutation(root: Path, item: Mapping[str, object]) -> tuple[Path, Path]:
    staging, final, _, _ = _topology_record(
        root=root, item=item, require_two_links=True
    )
    if item["kind"] == "dbn":
        if sha256_file(final, reject_hardlinks=False) != item["expected_sha256"]:
            raise IntegrityError("DBN hash differs before custody mutation")
    else:
        sidecar = _object(final, "inactive sidecar")
        if (
            not _self_hashed(sidecar, "manifest_id")
            or sidecar.get("manifest_id") != item["expected_manifest_id"]
            or sha256_file(final, reject_hardlinks=False)
            != item["expected_sidecar_sha256"]
        ):
            raise IntegrityError("sidecar identity differs before custody mutation")
    return staging, final


def _verify_record_after_mutation(final: Path, item: Mapping[str, object]) -> None:
    info = assert_plain_file(final)
    if info.st_nlink != 1 or info.st_size != item["byte_count"]:
        raise IntegrityError("repaired final is not exact single-link custody")
    if item["kind"] == "dbn":
        if sha256_file(final) != item["expected_sha256"]:
            raise IntegrityError("DBN hash differs after custody mutation")
    else:
        sidecar = _object(final, "repaired sidecar")
        if (
            not _self_hashed(sidecar, "manifest_id")
            or sidecar.get("manifest_id") != item["expected_manifest_id"]
            or sha256_file(final) != item["expected_sidecar_sha256"]
        ):
            raise IntegrityError("sidecar identity differs after custody mutation")


def execute_authorized_repair(
    *,
    root: Path,
    authorization: OperationReceipt,
    clock: Callable[[], float] = time.monotonic,
    unlink_file: Callable[[Path], None] = Path.unlink,
    mark_immutable: Callable[[Path], None] = _mark_read_only,
) -> dict[str, object]:
    """Remove exact staging aliases after complete pre-authority validation."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_repair_plan(root=root)
    terminal_path = contained_path(root, str(plan["terminal_path"]))
    if terminal_path.exists():
        raise IntegrityError("v2 custody repair terminal already exists")
    _validate_plan_bindings(root=root, plan=plan)
    _validate_pre_repair_topology(root=root, plan=plan)
    audit = _load_and_validate_audit(root, plan)
    if build_plan_audit(root=root) != audit:
        raise IntegrityError("v2 custody repair audit reconstruction differs")
    scope = required_scope(root=root, plan=plan)
    authorization.verify(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    use_path = authorization.consume(
        boundary,
        operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        required_scope=scope,
    )
    started = clock()
    completed: list[dict[str, object]] = []
    failure: dict[str, object] | None = None
    current_item: Mapping[str, object] | None = None
    current_final: Path | None = None
    for item in plan["repairs"]:
        current_item = item
        current_final = contained_path(root, str(item.get("final_path", "")))
        try:
            if clock() - started > MAXIMUM_RUNTIME_SECONDS:
                raise TimeoutError("custody repair runtime ceiling exceeded")
            staging, final = _verify_record_before_mutation(root, item)
            current_final = final
            _make_writable(staging)
            unlink_file(staging)
            if staging.exists():
                raise IntegrityError("custody repair staging alias survived unlink")
            _verify_record_after_mutation(final, item)
            mark_immutable(final)
            if not _is_read_only(final):
                raise IntegrityError("repaired final is not read-only")
            completed.append(
                {
                    "request_id": item["request_id"],
                    "kind": item["kind"],
                    "staging_path": item["staging_path"],
                    "final_path": item["final_path"],
                    "byte_count": item["byte_count"],
                }
            )
        except Exception as exc:
            preservation: dict[str, object] = {
                "final_exists": bool(current_final and current_final.exists()),
                "read_only_restored": False,
                "restoration_exception_type": None,
            }
            if current_final is not None and current_final.exists():
                try:
                    mark_immutable(current_final)
                    preservation["read_only_restored"] = _is_read_only(current_final)
                    if not preservation["read_only_restored"]:
                        preservation["restoration_exception_type"] = "ReadOnlyVerificationFailure"
                except Exception as restore_exc:
                    preservation["restoration_exception_type"] = type(restore_exc).__name__
            failure = {
                "exception_type": type(exc).__name__,
                "failed_request_id": item.get("request_id"),
                "failed_kind": item.get("kind"),
                "failed_staging_path": item.get("staging_path"),
                "failed_final_path": item.get("final_path"),
                "preservation": preservation,
            }
            break
    state = (
        "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY_REPAIRED"
        if failure is None and len(completed) == EXPECTED_ALIAS_REMOVALS
        else "FAILURE_INACTIVE_CUSTODY_REPAIR_EVIDENCE_PRESERVED"
    )
    core = {
        "schema_version": TERMINAL_SCHEMA_VERSION,
        "state": state,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "audit_id": audit["audit_id"],
        "audit_sha256": sha256_file(root / AUDIT_PATH),
        "implementation_head": plan["implementation_head"],
        "implementation_sha256": plan["implementation_sha256"],
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "completed_alias_removal_count": len(completed),
        "completed_repairs": completed,
        "failure": failure,
        "provider_calls": 0,
        "external_cost_incurred_usd": "0",
        "attempts": 1,
        "automatic_retries": 0,
        "dbn_rows_decoded": 0,
        "payloads_opened_for_row_access": 0,
        "year_2025_or_2026_payloads_opened_for_row_access": 0,
        "catalog_or_pointer_activated": False,
        "published": False,
        "registered": False,
        "evaluated": False,
        "trading": False,
        "terminal_written_last": True,
    }
    return _write_terminal(terminal_path, core)


def verify_completed_repair(*, root: Path) -> dict[str, object]:
    root = root.resolve(strict=True)
    plan = load_repair_plan(root=root)
    terminal = _object(root / REPAIR_TERMINAL_PATH, "v2 custody repair terminal")
    if (
        not _self_hashed(terminal, "terminal_id")
        or terminal.get("schema_version") != TERMINAL_SCHEMA_VERSION
        or terminal.get("state") != "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY_REPAIRED"
        or terminal.get("plan_id") != plan.get("plan_id")
        or terminal.get("plan_sha256") != sha256_file(root / PLAN_PATH)
        or terminal.get("completed_alias_removal_count") != EXPECTED_ALIAS_REMOVALS
        or terminal.get("failure") is not None
    ):
        raise IntegrityError("v2 custody repair terminal is not complete success evidence")
    for item in plan["repairs"]:
        staging = contained_path(root, str(item["staging_path"]))
        final = contained_path(root, str(item["final_path"]))
        if staging.exists():
            raise IntegrityError("repaired staging alias still exists")
        _verify_record_after_mutation(final, item)
        if not _is_read_only(final):
            raise IntegrityError("repaired final is not immutable read-only custody")
    return {
        "status": "PASS_SINGLE_LINK_INACTIVE_CUSTODY_NO_ROW_DECODE",
        "terminal_id": terminal["terminal_id"],
        "verified_dbn_count": EXPECTED_REQUESTS,
        "verified_sidecar_count": EXPECTED_REQUESTS,
        "dbn_rows_decoded": 0,
        "catalog_or_pointer_activated": False,
    }


__all__ = [
    "AUDIT_PATH",
    "FAILURE_REPORT_PATH",
    "OPERATION",
    "PLAN_PATH",
    "REPAIR_TERMINAL_PATH",
    "V1_PLAN_PATH",
    "V1_SUPERSESSION_PATH",
    "build_plan_audit",
    "build_repair_plan",
    "build_v1_supersession_report",
    "execute_authorized_repair",
    "load_repair_plan",
    "required_scope",
    "verify_completed_repair",
    "write_plan_audit_create_only",
    "write_repair_plan_create_only",
    "write_v1_supersession_report_create_only",
]
