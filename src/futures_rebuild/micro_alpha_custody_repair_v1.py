"""One-use source-safe repair for the executed v24 Windows hard-link defect."""

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
from .errors import ContractError, IntegrityError, UnauthorizedOperation


OPERATION = "REPAIR_APEX_MICRO_V24_HARDLINK_CUSTODY_ONCE"
PLAN_PATH = Path("configs/apex_micro_tier01_v24_custody_repair_plan_v1.json")
V24_PLAN_PATH = Path("configs/apex_micro_tier01_phase1a_acquisition_plan_v24.json")
V24_STAGING_ROOT = Path("state/provider_acquisition_staging/apex_micro_tier01_v24")
FAILURE_REPORT_PATH = Path(
    "state/unpublished_evidence/apex_micro_phase1a_acquisition_v24_verification_failure/report.json"
)
REPAIR_TERMINAL_PATH = Path(
    "state/unpublished_evidence/apex_micro_v24_custody_repair_v1/terminal.json"
)
SCHEMA_VERSION = "apex_micro_v24_custody_repair_plan/1.0.0"
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


def _terminal_path(root: Path) -> Path:
    candidates = sorted((root / V24_STAGING_ROOT).glob("*/terminal.json"))
    if len(candidates) != 1:
        raise IntegrityError("exactly one v24 terminal is required")
    return candidates[0]


def _same_file(first: Path, second: Path) -> bool:
    left = first.stat()
    right = second.stat()
    return left.st_dev == right.st_dev and left.st_ino == right.st_ino


def _mark_read_only(path: Path) -> None:
    path.chmod(stat.S_IREAD)


def _make_writable(path: Path) -> None:
    path.chmod(stat.S_IREAD | stat.S_IWRITE)


def build_failure_report(*, root: Path) -> dict[str, object]:
    """Describe the live link topology without reading any DBN payload bytes."""

    root = root.resolve(strict=True)
    plan_path = root / V24_PLAN_PATH
    terminal_path = _terminal_path(root)
    plan = _object(plan_path, "v24 acquisition plan")
    terminal = _object(terminal_path, "v24 acquisition terminal")
    if not _self_hashed(plan, "plan_id") or not _self_hashed(terminal, "terminal_id"):
        raise IntegrityError("v24 plan or terminal identity is invalid")
    accepted = terminal.get("accepted_files")
    failures = terminal.get("staging_cleanup_failures")
    if (
        terminal.get("state") != "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY"
        or terminal.get("accepted_dbn_count") != EXPECTED_REQUESTS
        or terminal.get("accepted_sidecar_count") != EXPECTED_REQUESTS
        or type(accepted) is not list
        or len(accepted) != EXPECTED_REQUESTS
        or type(failures) is not list
        or len(failures) != EXPECTED_ALIAS_REMOVALS
    ):
        raise IntegrityError("v24 terminal does not match the hard-link defect")
    plan_by_id = {
        str(item["request_id"]): item for item in plan.get("requests", [])
    }
    topology: list[dict[str, object]] = []
    for record in accepted:
        if type(record) is not dict:
            raise IntegrityError("v24 accepted-file record is malformed")
        request_id = str(record.get("request_id"))
        request = plan_by_id.get(request_id)
        if request is None:
            raise IntegrityError("v24 accepted request is absent from the plan")
        staging_prefix = terminal_path.parent / "downloads"
        staging_id = request_id[:16]
        for kind, final_key, suffix in (
            ("dbn", "dbn_destination", ".dbn.zst.partial"),
            ("sidecar", "sidecar_destination", ".manifest.json.partial"),
        ):
            staging = staging_prefix / f"{staging_id}{suffix}"
            final = contained_path(root, str(request[final_key]))
            assert_no_linklike_ancestors(staging)
            assert_no_linklike_ancestors(final)
            left = assert_plain_file(staging, reject_hardlinks=False)
            right = assert_plain_file(final, reject_hardlinks=False)
            if left.st_nlink != 2 or right.st_nlink != 2 or not _same_file(staging, final):
                raise IntegrityError("v24 staging/final pair is not the exact hard-link defect")
            topology.append(
                {
                    "request_id": request_id,
                    "kind": kind,
                    "staging_path": staging.relative_to(root).as_posix(),
                    "final_path": final.relative_to(root).as_posix(),
                    "byte_count": right.st_size,
                    "observed_link_count": right.st_nlink,
                }
            )
    core = {
        "schema_version": "apex_micro_v24_verification_failure/1.0.0",
        "state": "FAIL_CLOSED_FINAL_CUSTODY_HARDLINK_REPAIR_REQUIRED",
        "observed_head": _git_head(root),
        "v24_plan_id": plan["plan_id"],
        "v24_plan_sha256": sha256_file(plan_path),
        "v24_terminal_id": terminal["terminal_id"],
        "v24_terminal_sha256": sha256_file(terminal_path),
        "v24_terminal_path": terminal_path.relative_to(root).as_posix(),
        "provider_execution": {
            "accepted_dbn_count": terminal["accepted_dbn_count"],
            "accepted_sidecar_count": terminal["accepted_sidecar_count"],
            "total_bytes": terminal["total_bytes"],
            "external_cost_incurred_usd": terminal["external_cost_incurred_usd"],
            "automatic_retries": terminal["automatic_retries"],
            "provider_call_counts": terminal["provider_call_counts"],
            "provider_client_count": terminal["provider_client_count"],
            "download_worker_count": terminal["download_worker_count"],
        },
        "defect": {
            "classification": "WINDOWS_READ_ONLY_HARDLINK_STAGING_CLEANUP_FAILURE",
            "staging_cleanup_failure_count": len(failures),
            "staging_dbn_alias_count": EXPECTED_REQUESTS,
            "staging_sidecar_alias_count": EXPECTED_REQUESTS,
            "final_hardlinked_dbn_count": EXPECTED_REQUESTS,
            "final_hardlinked_sidecar_count": EXPECTED_REQUESTS,
            "canonical_verifier_result": "FAIL_CLOSED_HARDLINK_FORBIDDEN",
        },
        "topology": topology,
        "safety": {
            "dbn_payload_bytes_read_by_report_builder": 0,
            "dbn_rows_decoded": 0,
            "year_2025_or_2026_payloads_opened_for_row_access": 0,
            "provider_calls_after_v24": 0,
            "catalog_or_pointer_activated": False,
            "publication_registration_evaluation_or_trading": False,
            "cleanup_mutation_performed": False,
        },
        "next_boundary": "SEPARATE_EXACT_CUSTODY_REPAIR_APPROVAL_AFTER_COMMITTED_IMPLEMENTATION",
    }
    return {**core, "report_id": sha256_json(core)}


def write_failure_report_create_only(*, root: Path) -> dict[str, object]:
    report = build_failure_report(root=root)
    path = root / FAILURE_REPORT_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(report) + b"\n")
    _mark_read_only(path)
    return report


def build_repair_plan(*, root: Path, committed_head: str) -> dict[str, object]:
    root = root.resolve(strict=True)
    if committed_head != _git_head(root):
        raise IntegrityError("repair plan must bind the live committed HEAD")
    failure = _object(root / FAILURE_REPORT_PATH, "v24 verification failure report")
    if not _self_hashed(failure, "report_id"):
        raise IntegrityError("v24 verification failure report is invalid")
    if failure.get("state") != "FAIL_CLOSED_FINAL_CUSTODY_HARDLINK_REPAIR_REQUIRED":
        raise IntegrityError("v24 verification failure report state is invalid")
    repairs = [
        {
            "request_id": item["request_id"],
            "kind": item["kind"],
            "staging_path": item["staging_path"],
            "final_path": item["final_path"],
            "byte_count": item["byte_count"],
        }
        for item in failure["topology"]
    ]
    core = {
        "schema_version": SCHEMA_VERSION,
        "state": "PREPARED_NOT_EXECUTED_EXACT_CUSTODY_REPAIR",
        "operation": OPERATION,
        "committed_head": committed_head,
        "failure_report_id": failure["report_id"],
        "failure_report_sha256": sha256_file(root / FAILURE_REPORT_PATH),
        "v24_plan_id": failure["v24_plan_id"],
        "v24_plan_sha256": failure["v24_plan_sha256"],
        "v24_terminal_id": failure["v24_terminal_id"],
        "v24_terminal_sha256": failure["v24_terminal_sha256"],
        "v24_terminal_path": failure["v24_terminal_path"],
        "implementation_sha256": sha256_file(Path(__file__)),
        "repairs": repairs,
        "limits": {
            "exact_request_count": EXPECTED_REQUESTS,
            "exact_staging_alias_removal_count": EXPECTED_ALIAS_REMOVALS,
            "maximum_runtime_seconds": MAXIMUM_RUNTIME_SECONDS,
            "maximum_provider_calls": 0,
            "maximum_external_cost_usd": "0",
            "maximum_attempts": 1,
            "maximum_retries": 0,
        },
        "effects": {
            "remove_only_bound_staging_hardlink_aliases": True,
            "delete_or_overwrite_final_dbns": False,
            "redownload_or_provider_access": False,
            "decode_dbn_rows": False,
            "open_2025_or_2026_payloads_for_row_access": False,
            "publish_activate_register_evaluate_or_trade": False,
            "verify_final_bytes_by_sha256_without_decoding": True,
            "mark_final_files_read_only_after_single_link_verification": True,
        },
        "terminal_path": REPAIR_TERMINAL_PATH.as_posix(),
    }
    return {**core, "plan_id": sha256_json(core)}


def write_repair_plan_create_only(*, root: Path, committed_head: str) -> dict[str, object]:
    plan = build_repair_plan(root=root, committed_head=committed_head)
    path = root / PLAN_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(plan) + b"\n")
    _mark_read_only(path)
    return plan


def load_repair_plan(*, root: Path) -> dict[str, object]:
    plan = _object(root / PLAN_PATH, "v24 custody repair plan")
    if not _self_hashed(plan, "plan_id") or plan.get("schema_version") != SCHEMA_VERSION:
        raise IntegrityError("v24 custody repair plan identity is invalid")
    return plan


def required_scope(*, root: Path, plan: Mapping[str, object]) -> dict[str, str]:
    plan_sha256 = sha256_file(root / PLAN_PATH)
    return {
        "operation": OPERATION,
        "plan_id": str(plan["plan_id"]),
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "committed_head": str(plan["committed_head"]),
        "failure_report_id": str(plan["failure_report_id"]),
        "v24_terminal_id": str(plan["v24_terminal_id"]),
        "exact_alias_removals": str(EXPECTED_ALIAS_REMOVALS),
        "maximum_provider_calls": "0",
        "maximum_external_cost_usd": "0",
        "maximum_attempts": "1",
        "maximum_retries": "0",
        "terminal_path": str(plan["terminal_path"]),
        "dbn_rows_decoded": "0",
        "activation_permitted": "false",
        "approval_command": OPERATION,
        "approval_plan_id": str(plan["plan_id"]),
        "approval_plan_sha256": plan_sha256,
    }


def _write_terminal(path: Path, core: Mapping[str, object]) -> dict[str, object]:
    terminal = {**core, "terminal_id": sha256_json(core)}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(terminal) + b"\n")
    _mark_read_only(path)
    return terminal


def execute_authorized_repair(
    *,
    root: Path,
    authorization: OperationReceipt,
    clock: Callable[[], float] = time.monotonic,
    unlink_file: Callable[[Path], None] = Path.unlink,
    mark_immutable: Callable[[Path], None] = _mark_read_only,
) -> dict[str, object]:
    """Remove only exact staging aliases; never delete or decode final DBNs."""

    root = root.resolve(strict=True)
    boundary = RepoBoundary(root)
    plan = load_repair_plan(root=root)
    if _git_head(root) != plan["committed_head"]:
        raise UnauthorizedOperation("custody repair committed HEAD drifted")
    terminal_path = contained_path(root, str(plan["terminal_path"]))
    if terminal_path.exists():
        raise IntegrityError("custody repair terminal already exists")
    repairs = plan.get("repairs")
    if type(repairs) is not list or len(repairs) != EXPECTED_ALIAS_REMOVALS:
        raise IntegrityError("custody repair alias count drifted")
    for item in repairs:
        staging = contained_path(root, str(item["staging_path"]))
        final = contained_path(root, str(item["final_path"]))
        left = assert_plain_file(staging, reject_hardlinks=False)
        right = assert_plain_file(final, reject_hardlinks=False)
        if (
            left.st_nlink != 2
            or right.st_nlink != 2
            or not _same_file(staging, final)
            or right.st_size != item["byte_count"]
        ):
            raise IntegrityError("custody repair precondition drifted")
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
    v24_terminal = _object(
        contained_path(root, str(plan["v24_terminal_path"])), "v24 terminal"
    )
    accepted_by_id = {
        entry["request_id"]: entry for entry in v24_terminal["accepted_files"]
    }
    try:
        for item in repairs:
            if clock() - started > MAXIMUM_RUNTIME_SECONDS:
                raise TimeoutError("custody repair runtime ceiling exceeded")
            staging = contained_path(root, str(item["staging_path"]))
            final = contained_path(root, str(item["final_path"]))
            _make_writable(staging)
            unlink_file(staging)
            if staging.exists():
                raise IntegrityError("custody repair staging alias survived unlink")
            info = assert_plain_file(final)
            if info.st_nlink != 1 or info.st_size != item["byte_count"]:
                raise IntegrityError("custody repair final file is not single-link exact-size")
            if item["kind"] == "dbn":
                accepted = accepted_by_id[item["request_id"]]
                if sha256_file(final) != accepted["sha256"]:
                    raise IntegrityError("custody repair DBN hash differs")
            else:
                sidecar = _object(final, "repaired sidecar")
                if not _self_hashed(sidecar, "manifest_id"):
                    raise IntegrityError("custody repair sidecar identity differs")
            mark_immutable(final)
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
        failure = {
            "exception_type": type(exc).__name__,
            "failed_request_id": item.get("request_id") if type(item) is dict else None,
            "failed_kind": item.get("kind") if type(item) is dict else None,
        }
    state = (
        "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY_REPAIRED"
        if failure is None and len(completed) == EXPECTED_ALIAS_REMOVALS
        else "FAILURE_INACTIVE_CUSTODY_REPAIR_EVIDENCE_PRESERVED"
    )
    core = {
        "schema_version": "apex_micro_v24_custody_repair_terminal/1.0.0",
        "state": state,
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(root / PLAN_PATH),
        "committed_head": plan["committed_head"],
        "authorization_receipt_id": authorization.receipt_id,
        "authorization_use_path": use_path.relative_to(root).as_posix(),
        "completed_alias_removal_count": len(completed),
        "completed_repairs": completed,
        "failure": failure,
        "provider_calls": 0,
        "external_cost_incurred_usd": "0",
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
    plan = load_repair_plan(root=root)
    terminal = _object(root / REPAIR_TERMINAL_PATH, "custody repair terminal")
    if (
        not _self_hashed(terminal, "terminal_id")
        or terminal.get("state") != "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY_REPAIRED"
        or terminal.get("completed_alias_removal_count") != EXPECTED_ALIAS_REMOVALS
    ):
        raise IntegrityError("custody repair terminal is not a complete success")
    v24_terminal = _object(
        contained_path(root, str(plan["v24_terminal_path"])), "v24 terminal"
    )
    accepted = {item["request_id"]: item for item in v24_terminal["accepted_files"]}
    for item in plan["repairs"]:
        staging = contained_path(root, str(item["staging_path"]))
        final = contained_path(root, str(item["final_path"]))
        if staging.exists():
            raise IntegrityError("repaired staging alias still exists")
        info = assert_plain_file(final)
        if info.st_nlink != 1 or info.st_size != item["byte_count"]:
            raise IntegrityError("repaired final custody metadata differs")
        if item["kind"] == "dbn" and sha256_file(final) != accepted[item["request_id"]]["sha256"]:
            raise IntegrityError("repaired final DBN hash differs")
        if item["kind"] == "sidecar" and not _self_hashed(_object(final, "sidecar"), "manifest_id"):
            raise IntegrityError("repaired sidecar identity differs")
    return {
        "status": "PASS_SINGLE_LINK_INACTIVE_CUSTODY_NO_ROW_DECODE",
        "terminal_id": terminal["terminal_id"],
        "verified_dbn_count": EXPECTED_REQUESTS,
        "verified_sidecar_count": EXPECTED_REQUESTS,
        "dbn_rows_decoded": 0,
    }


__all__ = [
    "FAILURE_REPORT_PATH",
    "OPERATION",
    "PLAN_PATH",
    "REPAIR_TERMINAL_PATH",
    "build_failure_report",
    "build_repair_plan",
    "execute_authorized_repair",
    "load_repair_plan",
    "required_scope",
    "verify_completed_repair",
    "write_failure_report_create_only",
    "write_repair_plan_create_only",
]
