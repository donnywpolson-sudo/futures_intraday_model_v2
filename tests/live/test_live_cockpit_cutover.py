from __future__ import annotations

import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.live_cockpit.approval import (
    LiveSmokeApprovalError,
    build_live_smoke_plan,
)
from futures_rebuild.live_cockpit.cutover_guard import verify_cutover
from futures_rebuild.live_cockpit.smoke import RESULT_SCHEMA


ROOT = Path(__file__).resolve().parents[2]


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    executable = tmp_path / "FuturesLiveCockpit.exe"
    executable.write_bytes(b"synthetic packaged executable")
    executable_hash = sha256_file(executable)

    plan = build_live_smoke_plan(
        executable_hash,
        source_revision="b" * 40,
        package_inputs=[
            {"path": "src/example.py", "bytes": 7, "sha256": "c" * 64},
        ],
    )
    plan_path = tmp_path / "plan.json"
    plan_path.write_bytes(canonical_bytes(plan) + b"\n")

    result_core = {
        "schema_version": RESULT_SCHEMA,
        "status": "PASS",
        "plan_id": plan["plan_id"],
        "plan_sha256": sha256_file(plan_path),
        "approval_receipt_id": "a" * 64,
        "completed_at": "2026-07-25T00:00:00Z",
        "result_output_relative": plan["scope"]["result_output_relative"],
        "summary": {
            "status": "PASS",
            "reasons": [],
            "runtime": {
                "frozen": True,
                "executable_sha256": executable_hash,
            },
        },
    }
    result = {**result_core, "result_id": sha256_json(result_core)}
    result_path = tmp_path / "result.json"
    result_path.write_bytes(canonical_bytes(result) + b"\n")
    return plan_path, result_path, executable


def test_cutover_requires_exact_passing_package_bound_result(
    tmp_path: Path,
) -> None:
    plan_path, result_path, executable = _fixture(tmp_path)
    evidence = verify_cutover(
        plan_path=plan_path,
        result_path=result_path,
        executable_path=executable,
    )
    assert evidence["executable_sha256"] == sha256_file(executable)
    assert evidence["approval_receipt_id"] == "a" * 64


@pytest.mark.parametrize("mutation", ["status", "runtime", "result_id"])
def test_cutover_rejects_false_pass_mutations(
    tmp_path: Path, mutation: str
) -> None:
    plan_path, result_path, executable = _fixture(tmp_path)
    result = json.loads(result_path.read_text(encoding="utf-8"))
    if mutation == "status":
        result["status"] = "FAIL"
    elif mutation == "runtime":
        result["summary"]["runtime"]["executable_sha256"] = "b" * 64
    else:
        result["result_id"] = "c" * 64
    result_path.write_bytes(canonical_bytes(result) + b"\n")

    with pytest.raises(LiveSmokeApprovalError, match="package-bound"):
        verify_cutover(
            plan_path=plan_path,
            result_path=result_path,
            executable_path=executable,
        )


def test_cutover_rejects_noncanonical_result(tmp_path: Path) -> None:
    plan_path, result_path, executable = _fixture(tmp_path)
    result = json.loads(result_path.read_text(encoding="utf-8"))
    result_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    with pytest.raises(LiveSmokeApprovalError, match="canonical"):
        verify_cutover(
            plan_path=plan_path,
            result_path=result_path,
            executable_path=executable,
        )
