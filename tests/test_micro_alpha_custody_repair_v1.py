from __future__ import annotations

import json
import os
import shutil
import stat
from pathlib import Path

import pytest

from futures_rebuild import micro_alpha_custody_repair_v1 as repair
from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.research_gateway_policy import PREPARATORY_REAL_HISTORY_OPERATIONS


pytestmark = [pytest.mark.current, pytest.mark.high_risk]
HEAD = "d" * 40


def _write_self_hashed(path: Path, core: dict[str, object], key: str) -> dict[str, object]:
    value = {**core, key: sha256_json(core)}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(value) + b"\n")
    return value


def _fixture_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "active"
    request_id = "a" * 16
    final_dbn = Path("data/dbn/definition/MES/2019/2019-05-05_2020-01-01.dbn.zst")
    final_sidecar = Path(str(final_dbn) + ".manifest.json")
    v24_plan_core = {
        "schema_version": "test",
        "requests": [
            {
                "request_id": request_id,
                "dbn_destination": final_dbn.as_posix(),
                "sidecar_destination": final_sidecar.as_posix(),
            }
        ],
    }
    plan = _write_self_hashed(root / repair.V24_PLAN_PATH, v24_plan_core, "plan_id")
    staging_dir = root / repair.V24_STAGING_ROOT / ("b" * 16) / "downloads"
    staging_dir.mkdir(parents=True)
    staging_dbn = staging_dir / f"{request_id}.dbn.zst.partial"
    staging_sidecar = staging_dir / f"{request_id}.manifest.json.partial"
    staging_dbn.write_bytes(b"synthetic-dbn-not-decoded")
    sidecar_core = {"request_id": request_id, "synthetic": True}
    sidecar = {**sidecar_core, "manifest_id": sha256_json(sidecar_core)}
    staging_sidecar.write_bytes(canonical_bytes(sidecar) + b"\n")
    (root / final_dbn).parent.mkdir(parents=True)
    os.link(staging_dbn, root / final_dbn)
    os.link(staging_sidecar, root / final_sidecar)
    staging_dbn.chmod(stat.S_IREAD)
    staging_sidecar.chmod(stat.S_IREAD)
    accepted = [
        {
            "request_id": request_id,
            "dbn_destination": final_dbn.as_posix(),
            "sidecar_destination": final_sidecar.as_posix(),
            "byte_count": (root / final_dbn).stat().st_size,
            "sha256": sha256_file(root / final_dbn, reject_hardlinks=False),
            "provider_warning_count": 0,
            "provider_warning_categories": [],
        }
    ]
    terminal_core = {
        "schema_version": "test",
        "state": "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY",
        "accepted_dbn_count": 160,
        "accepted_sidecar_count": 160,
        "accepted_files": accepted,
        "staging_cleanup_failures": [
            {"request_id": request_id, "kind": "dbn", "exception_type": "PermissionError"},
            {"request_id": request_id, "kind": "sidecar", "exception_type": "PermissionError"},
        ],
        "total_bytes": (root / final_dbn).stat().st_size,
        "external_cost_incurred_usd": 0,
        "automatic_retries": 0,
        "provider_call_counts": {"get_cost": 160, "get_range": 160},
        "provider_client_count": 3,
        "download_worker_count": 2,
    }
    terminal = _write_self_hashed(
        staging_dir.parent / "terminal.json", terminal_core, "terminal_id"
    )
    topology = [
        {
            "request_id": request_id,
            "kind": "dbn",
            "staging_path": staging_dbn.relative_to(root).as_posix(),
            "final_path": final_dbn.as_posix(),
            "byte_count": staging_dbn.stat().st_size,
            "observed_link_count": 2,
        },
        {
            "request_id": request_id,
            "kind": "sidecar",
            "staging_path": staging_sidecar.relative_to(root).as_posix(),
            "final_path": final_sidecar.as_posix(),
            "byte_count": staging_sidecar.stat().st_size,
            "observed_link_count": 2,
        },
    ]
    failure_core = {
        "schema_version": "test",
        "state": "FAIL_CLOSED_FINAL_CUSTODY_HARDLINK_REPAIR_REQUIRED",
        "v24_plan_id": plan["plan_id"],
        "v24_plan_sha256": sha256_file(root / repair.V24_PLAN_PATH),
        "v24_terminal_id": terminal["terminal_id"],
        "v24_terminal_sha256": sha256_file(staging_dir.parent / "terminal.json"),
        "v24_terminal_path": (staging_dir.parent / "terminal.json").relative_to(root).as_posix(),
        "topology": topology,
    }
    failure = _write_self_hashed(
        root / repair.FAILURE_REPORT_PATH, failure_core, "report_id"
    )
    monkeypatch.setattr(repair, "EXPECTED_REQUESTS", 1)
    monkeypatch.setattr(repair, "EXPECTED_ALIAS_REMOVALS", 2)
    monkeypatch.setattr(repair, "_git_head", lambda _root: HEAD)
    monkeypatch.setattr(repair, "sha256_file", lambda path, **kwargs: (
        sha256_file(path, **kwargs)
        if Path(path).resolve() != Path(repair.__file__).resolve()
        else "f" * 64
    ))
    plan_value = repair.write_repair_plan_create_only(root=root, committed_head=HEAD)
    assert plan_value["failure_report_id"] == failure["report_id"]
    return root


def _receipt(root: Path) -> OperationReceipt:
    plan = repair.load_repair_plan(root=root)
    full = repair.required_scope(root=root, plan=plan)
    scope = {k: v for k, v in full.items() if k not in {
        "approval_command", "approval_plan_id", "approval_plan_sha256"
    }}
    plan_sha = sha256_file(root / repair.PLAN_PATH)
    return OperationReceipt.issue_user_approved(
        RepoBoundary(root),
        operation=repair.OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope,
        approval_command=repair.OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=plan_sha,
        approval_line=f"APPROVE {repair.OPERATION} PLAN {plan['plan_id']} SHA256 {plan_sha}",
    )


def test_repair_is_retired_and_has_no_provider_or_decode_surface() -> None:
    assert repair.OPERATION not in PREPARATORY_REAL_HISTORY_OPERATIONS
    source = Path(repair.__file__).read_text(encoding="utf-8")
    assert "databento" not in source.casefold()
    assert "get_range" not in source
    assert "read_dbn" not in source
    assert "to_df" not in source


def test_retired_repair_cannot_consume_authority_or_remove_aliases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _fixture_root(tmp_path, monkeypatch)
    with pytest.raises(UnauthorizedOperation, match="retired"):
        repair.execute_authorized_repair(root=root, authorization=_receipt(root))
    assert not (root / "state/authorization_uses").exists()
    for item in repair.load_repair_plan(root=root)["repairs"]:
        assert (root / item["staging_path"]).exists()
        assert (root / item["final_path"]).stat().st_nlink == 2


def test_repair_refuses_topology_drift_before_consuming_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _fixture_root(tmp_path, monkeypatch)
    plan = repair.load_repair_plan(root=root)
    staging = root / plan["repairs"][0]["staging_path"]
    staging.chmod(stat.S_IREAD | stat.S_IWRITE)
    staging.unlink()
    with pytest.raises(IntegrityError, match="precondition drifted|required file"):
        repair.execute_authorized_repair(root=root, authorization=_receipt(root))
    assert not (root / "state/authorization_uses").exists()


def test_retired_repair_does_not_reach_injected_unlink_surface(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _fixture_root(tmp_path, monkeypatch)
    calls = 0

    def fail_second(path: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("synthetic-unlink-failure")
        path.unlink()

    with pytest.raises(UnauthorizedOperation, match="retired"):
        repair.execute_authorized_repair(
            root=root,
            authorization=_receipt(root),
            unlink_file=fail_second,
        )
    assert calls == 0


def test_plan_requires_live_committed_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _fixture_root(tmp_path, monkeypatch)
    (root / repair.PLAN_PATH).chmod(stat.S_IREAD | stat.S_IWRITE)
    (root / repair.PLAN_PATH).unlink()
    with pytest.raises(IntegrityError, match="live committed HEAD"):
        repair.build_repair_plan(root=root, committed_head="e" * 40)
