"""Create a fresh full-certification attempt after a fail-closed interruption."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

from .active_data_full_plan import _load_canonical, _write_new_or_exact
from .active_data_full_plan import (
    FULL_CERTIFICATION_EXECUTOR,
    FULL_PLAN_GENERATOR,
)
from .active_data_full_supervisor import (
    SUPERVISOR_LAUNCHER_PATH,
    SUPERVISOR_PATH,
    build_supervision_binding,
)
from .active_data_plan import (
    IMPLEMENTATION_PATHS,
    build_supersession_record,
)
from .active_data_view import (
    ACTIVE_ROOT,
    build_pending_approval,
    verify_approval,
    verify_contract,
    verify_plan_bindings,
)
from .canonical import (
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    sha256_file,
    sha256_json,
)
from .errors import IntegrityError
from .runtime_environment import require_locked_repository_environment


INTERRUPTION_SCHEMA = "causal_active_full_interruption/1.0.0"
SUCCESSOR_GENERATOR = "src/futures_rebuild/active_data_full_successor.py"
ACTIVE_DATA_PLAN_IMPLEMENTATION = "src/futures_rebuild/active_data_plan.py"
ACTIVE_DATA_VIEW_IMPLEMENTATION = "src/futures_rebuild/active_data_view.py"
ALLOWED_REASONS = frozenset(
    {
        "PINNED_ENVIRONMENT_MISMATCH",
        "UNEXPECTED_CONCURRENT_PYTEST",
        "UNEXPLAINED_PROCESS_TERMINATION",
    }
)
RECONCILIATION_REFRESH_PATHS = frozenset(
    {
        ACTIVE_DATA_PLAN_IMPLEMENTATION,
        ACTIVE_DATA_VIEW_IMPLEMENTATION,
        FULL_CERTIFICATION_EXECUTOR,
        FULL_PLAN_GENERATOR,
        SUPERVISOR_LAUNCHER_PATH,
        SUPERVISOR_PATH,
        SUCCESSOR_GENERATOR,
    }
)


def _copy_json(value: Mapping[str, object]) -> dict[str, object]:
    copied = json.loads(canonical_bytes(value).decode("utf-8"))
    if not isinstance(copied, dict):
        raise IntegrityError("canonical JSON copy is not an object")
    return copied


def _verify_reconciliation_plan_bindings(
    repository_root: Path,
    plan: Mapping[str, object],
) -> None:
    root = repository_root.resolve(strict=True)
    try:
        verify_plan_bindings(root, plan)
        return
    except IntegrityError as exc:
        changed = str(exc).removeprefix("active-view binding changed: ")
        if changed not in RECONCILIATION_REFRESH_PATHS:
            raise
    verification_plan = _copy_json(plan)
    implementation = verification_plan.get("implementation_bindings")
    if not isinstance(implementation, dict):
        raise IntegrityError("predecessor implementation bindings are absent")
    for relative in RECONCILIATION_REFRESH_PATHS:
        if relative in implementation:
            implementation[relative] = sha256_file(root / relative)
    verification_plan["plan_id"] = sha256_json(
        {key: value for key, value in verification_plan.items() if key != "plan_id"}
    )
    verify_plan_bindings(root, verification_plan)


def _relative_file(root: Path, path: Path, description: str) -> str:
    resolved = path.resolve(strict=True)
    try:
        relative = resolved.relative_to(root)
    except ValueError as exc:
        raise IntegrityError(f"{description} is outside the repository") from exc
    assert_plain_file(resolved)
    return relative.as_posix()


def _interrupted_state_inventory(
    repository_root: Path,
    state_root: Path,
) -> list[dict[str, object]]:
    root = repository_root.resolve(strict=True)
    state = state_root.resolve(strict=True)
    try:
        state.relative_to(root)
    except ValueError as exc:
        raise IntegrityError("interrupted state root is outside the repository") from exc
    assert_no_linklike_ancestors(state)
    inventory: list[dict[str, object]] = []
    for directory, dirnames, filenames in os.walk(state, followlinks=False):
        current = Path(directory)
        dirnames.sort()
        filenames.sort()
        for dirname in dirnames:
            assert_no_linklike_ancestors(current / dirname)
        for filename in filenames:
            path = current / filename
            info = assert_plain_file(path)
            inventory.append(
                {
                    "path": path.relative_to(root).as_posix(),
                    "sha256": sha256_file(path),
                    "size": info.st_size,
                }
            )
    if not inventory:
        raise IntegrityError("interrupted certification state has no preserved evidence")
    return inventory


def build_interruption_record(
    *,
    repository_root: Path,
    predecessor_plan_path: Path,
    predecessor_approval_path: Path,
    reason: str,
) -> tuple[dict[str, object], dict[str, object]]:
    root = repository_root.resolve(strict=True)
    if reason not in ALLOWED_REASONS:
        raise IntegrityError("full-certification interruption reason is not allowed")
    if (root / ACTIVE_ROOT).exists():
        raise IntegrityError("interruption recovery requires data/active to remain absent")
    plan_relative = _relative_file(
        root, predecessor_plan_path, "predecessor full-certification plan"
    )
    approval_relative = _relative_file(
        root,
        predecessor_approval_path,
        "predecessor full-certification approval",
    )
    plan = _load_canonical(root / PurePosixPath(plan_relative), "predecessor plan")
    approval = _load_canonical(
        root / PurePosixPath(approval_relative), "predecessor approval"
    )
    verify_contract(root)
    _verify_reconciliation_plan_bindings(root, plan)
    approval_receipt_id = verify_approval(
        approval,
        plan,
        expected_operation="CERTIFY_CAUSAL_ACTIVE_VIEW",
    )
    scope_id = plan.get("certification_scope_id")
    if not isinstance(scope_id, str):
        raise IntegrityError("predecessor certification scope is absent")
    state_relative = (
        f"state/active_data_view_certification/full/{scope_id}"
    )
    expected_outputs = {
        state_relative,
        f"reports/active_data_view/full/{scope_id}",
    }
    if set(plan.get("outputs", ())) != expected_outputs:
        raise IntegrityError("predecessor output scope is invalid")
    state_root = root / PurePosixPath(state_relative)
    inventory = _interrupted_state_inventory(root, state_root)
    report_parents = {
        str(PurePosixPath(str(item["path"])).parent)
        for item in inventory
        if str(item["path"]).endswith("/certification_report.json")
    }
    receipt_parents = {
        str(PurePosixPath(str(item["path"])).parent)
        for item in inventory
        if str(item["path"]).endswith("/content_validation_receipt.json")
    }
    core: dict[str, object] = {
        "active_root_absent": True,
        "completed_candidate_count": len(report_parents & receipt_parents),
        "interrupted_state_inventory": inventory,
        "interrupted_state_inventory_id": sha256_json(inventory),
        "interrupted_state_root": state_relative,
        "predecessor_approval_path": approval_relative,
        "predecessor_approval_receipt_id": approval_receipt_id,
        "predecessor_approval_sha256": sha256_file(
            root / PurePosixPath(approval_relative)
        ),
        "predecessor_plan_id": plan["plan_id"],
        "predecessor_plan_path": plan_relative,
        "predecessor_plan_sha256": sha256_file(
            root / PurePosixPath(plan_relative)
        ),
        "predecessor_scope_id": scope_id,
        "preservation_rule": "LEAVE_INTERRUPTED_WORKSPACE_UNCHANGED",
        "reason": reason,
        "schema_version": INTERRUPTION_SCHEMA,
        "status": "INTERRUPTED_FAIL_CLOSED",
    }
    return (
        {**core, "interruption_id": sha256_json(core)},
        plan,
    )


def build_successor_plan(
    *,
    repository_root: Path,
    predecessor_plan: Mapping[str, object],
    interruption_record_path: Path,
    interruption_record: Mapping[str, object],
) -> tuple[dict[str, object], dict[str, object]]:
    root = repository_root.resolve(strict=True)
    record_relative = _relative_file(
        root, interruption_record_path, "interruption record"
    )
    record_id = interruption_record.get("interruption_id")
    if (
        not isinstance(record_id, str)
        or record_id
        != sha256_json(
            {
                key: value
                for key, value in interruption_record.items()
                if key != "interruption_id"
            }
        )
        or interruption_record.get("predecessor_plan_id")
        != predecessor_plan.get("plan_id")
    ):
        raise IntegrityError("interruption record does not bind the predecessor")
    plan = _copy_json(predecessor_plan)
    implementation = plan.get("implementation_bindings")
    semantic = plan.get("semantic_bindings")
    if not isinstance(implementation, dict) or not isinstance(semantic, dict):
        raise IntegrityError("predecessor implementation bindings are absent")
    for relative in (
        *IMPLEMENTATION_PATHS,
        FULL_PLAN_GENERATOR,
        FULL_CERTIFICATION_EXECUTOR,
        SUPERVISOR_PATH,
        SUPERVISOR_LAUNCHER_PATH,
        SUCCESSOR_GENERATOR,
    ):
        implementation[relative] = sha256_file(root / relative)
    semantic[record_relative] = sha256_file(root / PurePosixPath(record_relative))
    previous_attempt = predecessor_plan.get("execution_attempt")
    attempt_number = (
        int(previous_attempt.get("attempt_number", 1)) + 1
        if isinstance(previous_attempt, dict)
        else 2
    )
    execution_attempt = {
        "attempt_number": attempt_number,
        "interruption_id": record_id,
        "interruption_record_path": record_relative,
        "predecessor_plan_id": predecessor_plan["plan_id"],
        "predecessor_scope_id": predecessor_plan["certification_scope_id"],
        "preservation_rule": "LEAVE_INTERRUPTED_WORKSPACE_UNCHANGED",
    }
    limits = plan.get("limits")
    if (
        not isinstance(limits, dict)
        or type(limits.get("maximum_duration_seconds")) is not int
    ):
        raise IntegrityError("successor execution duration bound is absent")
    supervision = build_supervision_binding(
        repository_root=root,
        interruption_id=str(record_id),
        attempt_number=attempt_number,
        maximum_duration_seconds=int(limits["maximum_duration_seconds"]),
    )
    scope_id = sha256_json(
        {
            "entries": plan["entries"],
            "environment_bindings": plan["environment_bindings"],
            "execution_attempt": execution_attempt,
            "foundation_release_id": plan["foundation_release_id"],
            "implementation_bindings": implementation,
            "measured_projection": plan["measured_projection"],
            "semantic_bindings": semantic,
            "source_objects": plan["source_objects"],
            "supervision": supervision,
        }
    )
    if scope_id == predecessor_plan.get("certification_scope_id"):
        raise IntegrityError("successor certification scope was not refreshed")
    plan["certification_scope_id"] = scope_id
    plan["execution_attempt"] = execution_attempt
    plan["outputs"] = [
        f"reports/active_data_view/full/{scope_id}",
        f"state/active_data_view_certification/full/{scope_id}",
    ]
    plan["supervision"] = supervision
    plan["plan_id"] = sha256_json(
        {key: value for key, value in plan.items() if key != "plan_id"}
    )
    return plan, build_pending_approval(plan)


def build_successor_dry_run(
    *,
    predecessor_dry_run: Mapping[str, object],
    successor_plan: Mapping[str, object],
    interruption_record: Mapping[str, object],
) -> dict[str, object]:
    report = _copy_json(predecessor_dry_run)
    report.pop("dry_run_report_id", None)
    report["certification_scope_id"] = successor_plan["certification_scope_id"]
    report["plan_id"] = successor_plan["plan_id"]
    report["plan_sha256"] = sha256_json(successor_plan)
    report["predecessor_interruption_id"] = interruption_record["interruption_id"]
    report["status"] = "PENDING_EXACT_APPROVAL"
    report["dry_run_report_id"] = sha256_json(report)
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    parser.add_argument("--predecessor-plan", type=Path, required=True)
    parser.add_argument("--predecessor-approval", type=Path, required=True)
    parser.add_argument("--predecessor-dry-run", type=Path, required=True)
    parser.add_argument("--interruption-output", type=Path, required=True)
    parser.add_argument("--plan-output", type=Path, required=True)
    parser.add_argument("--approval-output", type=Path, required=True)
    parser.add_argument("--supersession-output", type=Path, required=True)
    parser.add_argument("--reason", choices=sorted(ALLOWED_REASONS), required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.repository_root.resolve(strict=True)
    require_locked_repository_environment(root)
    resolve = lambda path: path if path.is_absolute() else root / path
    interruption, predecessor_plan = build_interruption_record(
        repository_root=root,
        predecessor_plan_path=resolve(args.predecessor_plan),
        predecessor_approval_path=resolve(args.predecessor_approval),
        reason=args.reason,
    )
    interruption_path = resolve(args.interruption_output)
    _write_new_or_exact(interruption_path, interruption)
    plan, approval = build_successor_plan(
        repository_root=root,
        predecessor_plan=predecessor_plan,
        interruption_record_path=interruption_path,
        interruption_record=interruption,
    )
    predecessor_dry_run = _load_canonical(
        resolve(args.predecessor_dry_run), "predecessor dry-run report"
    )
    dry_run = build_successor_dry_run(
        predecessor_dry_run=predecessor_dry_run,
        successor_plan=plan,
        interruption_record=interruption,
    )
    plan_path = resolve(args.plan_output)
    approval_path = resolve(args.approval_output)
    supersession_path = resolve(args.supersession_output)
    _write_new_or_exact(plan_path, plan)
    _write_new_or_exact(approval_path, approval)
    supersession = build_supersession_record(
        repository_root=root,
        predecessor_plan_path=resolve(args.predecessor_plan)
        .relative_to(root)
        .as_posix(),
        successor_plan=plan,
    )
    _write_new_or_exact(supersession_path, supersession)
    report_root = (
        root
        / "reports"
        / "active_data_view"
        / "full"
        / str(plan["certification_scope_id"])
    )
    _write_new_or_exact(report_root / "dry_run_report.json", dry_run)
    print(
        canonical_bytes(
            {
                "approval_status": "PENDING",
                "interruption_id": interruption["interruption_id"],
                "plan_id": plan["plan_id"],
                "scope_id": plan["certification_scope_id"],
            }
        ).decode()
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
