from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest

from futures_rebuild import micro_alpha_custody_repair_v2 as repair
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
    v24_plan = _write_self_hashed(
        root / repair.V24_PLAN_PATH,
        {
            "schema_version": "test",
            "requests": [
                {
                    "request_id": request_id,
                    "dbn_destination": final_dbn.as_posix(),
                    "sidecar_destination": final_sidecar.as_posix(),
                }
            ],
        },
        "plan_id",
    )
    staging_dir = root / repair.V24_STAGING_ROOT / ("b" * 16) / "downloads"
    staging_dir.mkdir(parents=True)
    staging_dbn = staging_dir / f"{request_id}.dbn.zst.partial"
    staging_sidecar = staging_dir / f"{request_id}.manifest.json.partial"
    staging_dbn.write_bytes(b"synthetic-dbn-not-decoded")
    dbn_hash = sha256_file(staging_dbn)
    sidecar_core = {
        "request_id": request_id,
        "plan_id": v24_plan["plan_id"],
        "sha256": dbn_hash,
        "state": "INACTIVE_CUSTODY_NOT_A_RESEARCH_SOURCE",
    }
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
            "sha256": dbn_hash,
        }
    ]
    terminal = _write_self_hashed(
        staging_dir.parent / "terminal.json",
        {
            "schema_version": "test",
            "state": "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY",
            "accepted_dbn_count": 1,
            "accepted_sidecar_count": 1,
            "accepted_files": accepted,
        },
        "terminal_id",
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
    failure = _write_self_hashed(
        root / repair.FAILURE_REPORT_PATH,
        {
            "schema_version": "test",
            "state": "FAIL_CLOSED_FINAL_CUSTODY_HARDLINK_REPAIR_REQUIRED",
            "v24_plan_id": v24_plan["plan_id"],
            "v24_plan_sha256": sha256_file(root / repair.V24_PLAN_PATH),
            "v24_terminal_id": terminal["terminal_id"],
            "v24_terminal_sha256": sha256_file(staging_dir.parent / "terminal.json"),
            "v24_terminal_path": (staging_dir.parent / "terminal.json")
            .relative_to(root)
            .as_posix(),
            "topology": topology,
        },
        "report_id",
    )
    v1_core = {
        "schema_version": "apex_micro_v24_custody_repair_plan/1.0.0",
        "state": "PREPARED_NOT_EXECUTED_EXACT_CUSTODY_REPAIR",
        "operation": "REPAIR_APEX_MICRO_V24_HARDLINK_CUSTODY_ONCE",
        "committed_head": "c" * 40,
        "failure_report_id": failure["report_id"],
        "repairs": topology,
    }
    _write_self_hashed(root / repair.V1_PLAN_PATH, v1_core, "plan_id")
    monkeypatch.setattr(repair, "EXPECTED_REQUESTS", 1)
    monkeypatch.setattr(repair, "EXPECTED_ALIAS_REMOVALS", 2)
    monkeypatch.setattr(repair, "_git_head", lambda _root: HEAD)
    monkeypatch.setattr(repair, "_implementation_sha256", lambda: "f" * 64)
    repair.write_v1_supersession_report_create_only(root=root)
    repair.write_repair_plan_create_only(root=root, implementation_head=HEAD)
    repair.write_plan_audit_create_only(root=root)
    return root


def _receipt(root: Path) -> OperationReceipt:
    plan = repair.load_repair_plan(root=root)
    full = repair.required_scope(root=root, plan=plan)
    scope = {
        key: value
        for key, value in full.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    }
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


def _make_writable(path: Path) -> None:
    path.chmod(stat.S_IREAD | stat.S_IWRITE)


def test_v2_is_only_current_repair_and_has_no_provider_or_decode_surface() -> None:
    assert repair.OPERATION in PREPARATORY_REAL_HISTORY_OPERATIONS
    assert "REPAIR_APEX_MICRO_V24_HARDLINK_CUSTODY_ONCE" not in PREPARATORY_REAL_HISTORY_OPERATIONS
    source = Path(repair.__file__).read_text(encoding="utf-8").casefold()
    assert "databento" not in source
    assert "get_range" not in source
    assert "read_dbn" not in source
    assert "to_df" not in source


def test_v1_supersession_is_deterministic_and_non_authorizing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _fixture_root(tmp_path, monkeypatch)
    report = json.loads((root / repair.V1_SUPERSESSION_PATH).read_text(encoding="utf-8"))
    assert report == repair.build_v1_supersession_report(root=root)
    assert report["state"] == "SUPERSEDED_PREPARATION_INCOMPLETE_EXECUTION_BINDINGS"
    assert report["authority_and_effects"]["staging_aliases_removed"] == 0
    assert report["authority_and_effects"]["dbn_payload_bytes_read"] == 0


def test_exact_v2_repair_pre_and_post_verifies_single_link_custody(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _fixture_root(tmp_path, monkeypatch)
    terminal = repair.execute_authorized_repair(root=root, authorization=_receipt(root))
    assert terminal["state"] == "SUCCESS_INACTIVE_IMMUTABLE_CUSTODY_REPAIRED"
    assert terminal["completed_alias_removal_count"] == 2
    assert terminal["provider_calls"] == 0
    assert terminal["dbn_rows_decoded"] == 0
    verified = repair.verify_completed_repair(root=root)
    assert verified["status"] == "PASS_SINGLE_LINK_INACTIVE_CUSTODY_NO_ROW_DECODE"
    for item in repair.load_repair_plan(root=root)["repairs"]:
        assert not (root / item["staging_path"]).exists()
        assert (root / item["final_path"]).stat().st_nlink == 1


@pytest.mark.parametrize(
    ("target", "message"),
    [
        (repair.FAILURE_REPORT_PATH, "sealed evidence binding"),
        (repair.V24_PLAN_PATH, "sealed custody evidence"),
    ],
)
def test_evidence_drift_is_rejected_before_authority_consumption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: Path,
    message: str,
) -> None:
    root = _fixture_root(tmp_path, monkeypatch)
    receipt = _receipt(root)
    path = root / target
    _make_writable(path)
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(IntegrityError, match=message):
        repair.execute_authorized_repair(root=root, authorization=receipt)
    assert not (root / "state/authorization_uses").exists()


def test_implementation_hash_drift_is_rejected_before_authority_consumption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _fixture_root(tmp_path, monkeypatch)
    receipt = _receipt(root)
    monkeypatch.setattr(repair, "_implementation_sha256", lambda: "e" * 64)
    with pytest.raises(IntegrityError, match="implementation hash drifted"):
        repair.execute_authorized_repair(root=root, authorization=receipt)
    assert not (root / "state/authorization_uses").exists()


def test_audit_drift_is_rejected_before_authority_consumption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _fixture_root(tmp_path, monkeypatch)
    receipt = _receipt(root)
    path = root / repair.AUDIT_PATH
    _make_writable(path)
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(
        (IntegrityError, UnauthorizedOperation), match="exact required scope|audit binding"
    ):
        repair.execute_authorized_repair(root=root, authorization=receipt)
    assert not (root / "state/authorization_uses").exists()


def test_topology_drift_is_rejected_before_authority_consumption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _fixture_root(tmp_path, monkeypatch)
    receipt = _receipt(root)
    plan = repair.load_repair_plan(root=root)
    staging = root / plan["repairs"][0]["staging_path"]
    _make_writable(staging)
    staging.unlink()
    with pytest.raises(IntegrityError, match="topology|required file"):
        repair.execute_authorized_repair(root=root, authorization=receipt)
    assert not (root / "state/authorization_uses").exists()


def test_dbn_hash_mismatch_fails_before_unlink_and_restores_read_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _fixture_root(tmp_path, monkeypatch)
    plan = repair.load_repair_plan(root=root)
    item = plan["repairs"][0]
    final = root / item["final_path"]
    staging = root / item["staging_path"]
    _make_writable(final)
    final.write_bytes(b"same-length-corruption-data"[: item["byte_count"]])
    assert final.stat().st_size == item["byte_count"]
    terminal = repair.execute_authorized_repair(root=root, authorization=_receipt(root))
    assert terminal["state"] == "FAILURE_INACTIVE_CUSTODY_REPAIR_EVIDENCE_PRESERVED"
    assert terminal["completed_alias_removal_count"] == 0
    assert staging.exists()
    assert os.path.samefile(staging, final)
    assert terminal["failure"]["preservation"]["read_only_restored"] is True


def test_unlink_failure_restores_shared_inode_read_only_and_stops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _fixture_root(tmp_path, monkeypatch)
    calls = 0

    def fail_first(_path: Path) -> None:
        nonlocal calls
        calls += 1
        raise OSError("synthetic-unlink-failure")

    terminal = repair.execute_authorized_repair(
        root=root,
        authorization=_receipt(root),
        unlink_file=fail_first,
    )
    assert calls == 1
    assert terminal["completed_alias_removal_count"] == 0
    assert terminal["automatic_retries"] == 0
    assert terminal["failure"]["preservation"]["read_only_restored"] is True
    item = repair.load_repair_plan(root=root)["repairs"][0]
    assert not ((root / item["final_path"]).stat().st_mode & stat.S_IWRITE)


def test_partial_failure_stops_without_touching_later_alias(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _fixture_root(tmp_path, monkeypatch)
    calls = 0

    def fail_second(path: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("synthetic-second-unlink-failure")
        path.unlink()

    terminal = repair.execute_authorized_repair(
        root=root,
        authorization=_receipt(root),
        unlink_file=fail_second,
    )
    assert calls == 2
    assert terminal["completed_alias_removal_count"] == 1
    assert terminal["state"] == "FAILURE_INACTIVE_CUSTODY_REPAIR_EVIDENCE_PRESERVED"
    second = repair.load_repair_plan(root=root)["repairs"][1]
    assert (root / second["staging_path"]).exists()


def test_plan_and_audit_reconstruct_exactly_without_dbn_hashing_in_audit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _fixture_root(tmp_path, monkeypatch)
    plan = repair.load_repair_plan(root=root)
    assert repair.build_repair_plan(root=root, implementation_head=HEAD) == plan
    audit = json.loads((root / repair.AUDIT_PATH).read_text(encoding="utf-8"))
    assert repair.build_plan_audit(root=root) == audit
    assert audit["dbn_payload_bytes_read_by_audit"] == 0
