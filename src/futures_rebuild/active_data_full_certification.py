"""Approval-gated, sequential full certification for one exact active-view plan."""

from __future__ import annotations

import argparse
import json
import shutil
import time
from collections.abc import Mapping
from pathlib import Path, PurePosixPath

from .active_data_certification import certify_market_year
from .active_data_view import (
    ACTIVE_ROOT,
    CERTIFICATION_STATE,
    validate_content_validation_receipt,
    verify_approval,
    verify_contract,
    verify_plan_bindings,
)
from .boundary import RepoBoundary
from .canonical import (
    assert_no_linklike_ancestors,
    assert_plain_file,
    canonical_bytes,
    fsync_directory,
    sha256_file,
    sha256_json,
)
from .errors import IntegrityError, UnauthorizedOperation


FULL_REPORT_SCHEMA = "causal_active_full_certification_report/1.0.0"


def _load_canonical(path: Path, description: str) -> dict[str, object]:
    assert_plain_file(path)
    raw = path.read_bytes()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"{description} is invalid JSON") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"{description} is not canonical JSON")
    return payload


def _write_new_or_exact(path: Path, payload: Mapping[str, object]) -> None:
    assert_no_linklike_ancestors(path)
    encoded = canonical_bytes(payload) + b"\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        assert_plain_file(path)
        if path.read_bytes() != encoded:
            raise IntegrityError(f"refusing to overwrite different evidence: {path}")
        return
    with path.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
    fsync_directory(path.parent)


def verify_source_inventory_once(
    repository_root: Path,
    plan: Mapping[str, object],
) -> dict[str, object]:
    """Hash each plan-declared source object exactly once in this inventory pass."""

    root = repository_root.resolve(strict=True)
    source_objects = plan.get("source_objects")
    limits = plan.get("limits")
    if not isinstance(source_objects, list) or not isinstance(limits, dict):
        raise IntegrityError("full certification source inventory is absent")
    seen: set[str] = set()
    verified: list[dict[str, object]] = []
    total_bytes = 0
    for item in source_objects:
        if (
            not isinstance(item, dict)
            or type(item.get("path")) is not str
            or type(item.get("sha256")) is not str
            or type(item.get("size")) is not int
        ):
            raise IntegrityError("full certification source object is invalid")
        relative = str(item["path"])
        if relative in seen:
            raise IntegrityError("full certification source inventory is duplicated")
        seen.add(relative)
        path = root / PurePosixPath(relative)
        info = assert_plain_file(path)
        observed_hash = sha256_file(path)
        if info.st_size != item["size"] or observed_hash != item["sha256"]:
            raise IntegrityError(f"full certification source changed: {relative}")
        total_bytes += info.st_size
        verified.append(
            {
                "path": relative,
                "sha256": observed_hash,
                "size": info.st_size,
            }
        )
    if (
        len(verified) != limits.get("maximum_source_files")
        or total_bytes != limits.get("maximum_source_bytes")
    ):
        raise IntegrityError("full certification source ceilings differ from plan")
    return {
        "source_bytes": total_bytes,
        "source_files": len(verified),
        "source_inventory_id": sha256_json(verified),
    }


def _report_identity(report: Mapping[str, object]) -> str:
    report_id = report.get("certification_report_id")
    if (
        not isinstance(report_id, str)
        or report_id
        != sha256_json(
            {
                key: value
                for key, value in report.items()
                if key != "certification_report_id"
            }
        )
    ):
        raise IntegrityError("full certification report self-hash is invalid")
    return report_id


def _verify_completed_candidate(
    *,
    report: Mapping[str, object],
    receipt: Mapping[str, object],
    entry: Mapping[str, object],
    plan: Mapping[str, object],
) -> tuple[str, str]:
    report_id = _report_identity(report)
    receipt_id = validate_content_validation_receipt(receipt)
    if (
        report.get("status") != "PASS"
        or report.get("state") != CERTIFICATION_STATE
        or report.get("market") != entry.get("market")
        or report.get("year") != entry.get("year")
        or report.get("foundation_release_id") != plan.get("foundation_release_id")
        or report.get("foundation_manifest_sha256")
        != plan.get("foundation_manifest_sha256")
        or report.get("content_validation_receipt_id") != receipt_id
        or receipt.get("implementation_bindings")
        != plan.get("implementation_bindings")
        or receipt.get("environment_bindings") != plan.get("environment_bindings")
    ):
        raise IntegrityError("completed candidate differs from the full plan")
    return report_id, receipt_id


def execute_full_certification(
    *,
    repository_root: Path,
    plan: Mapping[str, object],
    approval: Mapping[str, object],
    batch_rows: int = 100_000,
) -> dict[str, object]:
    root = repository_root.resolve(strict=True)
    verify_contract(root)
    verify_plan_bindings(root, plan)
    approval_receipt_id = verify_approval(
        approval,
        plan,
        expected_operation="CERTIFY_CAUSAL_ACTIVE_VIEW",
    )
    if (root / ACTIVE_ROOT).exists():
        raise IntegrityError("full certification requires data/active to remain absent")
    limits = plan.get("limits")
    scope_id = plan.get("certification_scope_id")
    if (
        not isinstance(limits, dict)
        or limits.get("maximum_workers") != 1
        or not isinstance(scope_id, str)
    ):
        raise IntegrityError("full certification execution bounds are invalid")
    expected_outputs = {
        f"reports/active_data_view/full/{scope_id}",
        f"state/active_data_view_certification/full/{scope_id}",
    }
    if set(plan.get("outputs", ())) != expected_outputs:
        raise IntegrityError("full certification outputs differ from plan")
    started = time.perf_counter()
    state_root = root / "state" / "active_data_view_certification" / "full" / scope_id
    report_root = root / "reports" / "active_data_view" / "full" / scope_id
    assert_no_linklike_ancestors(state_root)
    assert_no_linklike_ancestors(report_root)
    state_root.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(state_root).free < int(limits["maximum_temporary_bytes"]):
        raise IntegrityError("insufficient disk for approved full certification")
    inventory = verify_source_inventory_once(root, plan)
    if time.perf_counter() - started > int(limits["maximum_duration_seconds"]):
        raise IntegrityError("full certification duration ceiling reached")
    entries = [
        entry
        for entry in plan.get("entries", ())
        if isinstance(entry, dict)
        and entry.get("disposition") == CERTIFICATION_STATE
    ]
    if len(entries) != limits.get("maximum_candidates"):
        raise IntegrityError("full certification candidate count differs from plan")
    completed: list[dict[str, object]] = []
    for entry in sorted(
        entries, key=lambda item: (str(item["market"]), int(item["year"]))
    ):
        if time.perf_counter() - started > int(limits["maximum_duration_seconds"]):
            raise IntegrityError("full certification duration ceiling reached")
        market = str(entry["market"])
        year = int(entry["year"])
        workspace = state_root / market / str(year)
        report_path = workspace / "certification_report.json"
        receipt_path = workspace / "content_validation_receipt.json"
        if workspace.exists():
            if not report_path.is_file() or not receipt_path.is_file():
                raise IntegrityError(
                    f"incomplete candidate requires reviewed recovery: {market}/{year}"
                )
            report = _load_canonical(report_path, "completed certification report")
            receipt = _load_canonical(receipt_path, "completed content receipt")
        else:
            report, receipt = certify_market_year(
                boundary=RepoBoundary(active_root=root),
                foundation_release_id=str(plan["foundation_release_id"]),
                foundation_manifest_sha256=str(plan["foundation_manifest_sha256"]),
                foundation_intervals=entry["intervals"],  # type: ignore[arg-type]
                workspace=workspace,
                semantic_bindings=plan["semantic_bindings"],  # type: ignore[arg-type]
                implementation_bindings=plan["implementation_bindings"],  # type: ignore[arg-type]
                environment_bindings=plan["environment_bindings"],  # type: ignore[arg-type]
                batch_rows=batch_rows,
            )
            _write_new_or_exact(report_path, report)
            _write_new_or_exact(receipt_path, receipt)
        report_id, receipt_id = _verify_completed_candidate(
            report=report,
            receipt=receipt,
            entry=entry,
            plan=plan,
        )
        temporary_bytes = sum(
            path.stat().st_size for path in state_root.rglob("*") if path.is_file()
        )
        if temporary_bytes > int(limits["maximum_temporary_bytes"]):
            raise IntegrityError("full certification temporary-storage ceiling reached")
        measurements = report.get("measurements")
        if (
            not isinstance(measurements, dict)
            or int(measurements.get("peak_working_set_bytes", -1))
            > int(limits["maximum_memory_bytes"])
        ):
            raise IntegrityError("full certification memory ceiling reached")
        candidate_report_root = report_root / market / str(year)
        _write_new_or_exact(
            candidate_report_root / "certification_report.json", report
        )
        _write_new_or_exact(
            candidate_report_root / "content_validation_receipt.json", receipt
        )
        completed.append(
            {
                "certification_report_id": report_id,
                "content_validation_receipt_id": receipt_id,
                "market": market,
                "year": year,
            }
        )
        if time.perf_counter() - started > int(limits["maximum_duration_seconds"]):
            raise IntegrityError("full certification duration ceiling reached")
    if len(completed) != len(entries):
        raise IntegrityError("full certification did not complete every candidate")
    core: dict[str, object] = {
        "approval_receipt_id": approval_receipt_id,
        "authority": "NON_AUTHORIZING_FULL_CERTIFICATION_EVIDENCE_ONLY",
        "candidate_count": len(completed),
        "candidates": completed,
        "does_not_authorize": [
            "ACTIVE_ROOT_MUTATION",
            "ARCHIVE_OR_DELETE",
            "HOLDOUT_OR_FORWARD_PAYLOAD_ACCESS",
            "MATERIALIZATION",
            "MODEL_FIT_OR_EVALUATION",
            "OUTCOME_LABEL_PREDICTION_ACCESS",
            "PROVIDER_CALL_OR_DOWNLOAD",
            "PUBLICATION",
            "TRADING",
        ],
        "elapsed_seconds": format(time.perf_counter() - started, ".9f"),
        "plan_id": plan["plan_id"],
        "schema_version": FULL_REPORT_SCHEMA,
        "source_inventory": inventory,
        "status": "PASS",
    }
    result = {**core, "full_certification_report_id": sha256_json(core)}
    _write_new_or_exact(report_root / "full_certification_report.json", result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root", type=Path, default=Path.cwd())
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--approval", type=Path, required=True)
    parser.add_argument("--batch-rows", type=int, default=100_000)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = args.repository_root.resolve(strict=True)
    plan_path = args.plan if args.plan.is_absolute() else root / args.plan
    approval_path = (
        args.approval if args.approval.is_absolute() else root / args.approval
    )
    plan = _load_canonical(plan_path, "full certification plan")
    approval = _load_canonical(approval_path, "full certification approval")
    if approval.get("status") != "APPROVED":
        raise UnauthorizedOperation(
            "full certification requires exact approved receipt"
        )
    result = execute_full_certification(
        repository_root=root,
        plan=plan,
        approval=approval,
        batch_rows=args.batch_rows,
    )
    print(
        canonical_bytes(
            {
                "candidate_count": result["candidate_count"],
                "full_certification_report_id": result[
                    "full_certification_report_id"
                ],
                "status": result["status"],
            }
        ).decode()
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
