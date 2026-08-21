from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from futures_rebuild import ohlcv_msf_1d_publication_successor as successor
from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
    _personal_approval_line,
)
from futures_rebuild.errors import UnauthorizedOperation


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(successor.serialized_json(value))


def _transaction_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[str, OperationReceipt, bytes]:
    run_id = "msf1dpub_fixture"
    report = tmp_path / successor.REPORT_PARENT_REL / run_id
    shadow = tmp_path / successor.SHADOW_PARENT_REL / run_id / "candidate_additions/data/dbn"
    canonical_file = tmp_path / successor.CANONICAL_ROOT_REL / "definition/ES/2026/current.dbn.zst"
    canonical_file.parent.mkdir(parents=True)
    canonical_file.write_bytes(b"prior")
    (tmp_path / successor.CANONICAL_ROOT_REL / "ohlcv_1d").mkdir()
    successor_file = shadow / "ohlcv_1d/MSF/2018/canary.dbn.zst"
    successor_file.parent.mkdir(parents=True)
    successor_file.write_bytes(b"successor")
    prior_inventory = successor._inventory(tmp_path / successor.CANONICAL_ROOT_REL)
    prior_summary = successor._inventory_summary(prior_inventory)
    shadow_inventory = successor._inventory(shadow)
    shadow_summary = successor._inventory_summary(shadow_inventory)
    successor_inventory = sorted(prior_inventory + shadow_inventory, key=lambda item: item["relative_path"])
    successor_summary = successor._inventory_summary(successor_inventory)

    monkeypatch.setattr(successor, "EXPECTED_PRIOR_FILES", 1)
    monkeypatch.setattr(successor, "EXPECTED_PRIOR_BYTES", len(b"prior"))
    monkeypatch.setattr(
        successor,
        "validate_active",
        lambda root: {"inventory_summary": prior_summary},
    )

    prior_pointer = {
        "schema_version": "canonical_data_dbn_active_pointer/1.0.0",
        "status": "ACTIVE",
        "release_id": successor.EXPECTED_PRIOR_RELEASE_ID,
    }
    pointer_path = tmp_path / successor.ACTIVE_POINTER_REL
    _write_json(pointer_path, prior_pointer)
    prior_pointer_bytes = pointer_path.read_bytes()
    prior_pointer_sha = successor.sha256_file(pointer_path)

    release_core = {"fixture": True, "scope": "EXACT_NINE_MSF_OHLCV_1D_PARTITIONS_ONLY"}
    release_id = successor.sha256_json(release_core)
    release = {
        "schema_version": "ohlcv_msf_1d_publication_successor_release/1.0.0",
        "release_id": release_id,
        "release_core": release_core,
        "complete_shadow_tree": successor_summary,
        "canonical_artifact_index": [
            {
                "future_project_relative_path": "data/dbn/ohlcv_1d/MSF/2018/canary.dbn.zst",
                "size_bytes": len(b"successor"),
                "sha256": successor.sha256_bytes(b"successor"),
            }
        ],
    }
    release_path = report / "successor_release_manifest.json"
    _write_json(release_path, release)
    _write_json(
        report / "candidate_tree_validation.json",
        {
            "status": "PASS_CERTIFIED_ISOLATED_ADDITION_AND_COMPLETE_VIRTUAL_SUCCESSOR",
            "candidate_summary": shadow_summary,
        },
    )
    wrapper_path = report / "successor_wrapper.json"
    _write_json(wrapper_path, {"status": "IMMUTABLE_SUCCESSOR_WRAPPER", "release_id": release_id})
    template = {
        "schema_version": "canonical_data_dbn_active_pointer/2.0.0",
        "status": "ACTIVE",
        "scope": "EXACT_NINE_MSF_OHLCV_1D_PARTITIONS_ONLY",
        "release_id": release_id,
        "release_manifest_path": release_path.relative_to(tmp_path).as_posix(),
        "release_manifest_sha256": successor.sha256_file(release_path),
        "wrapper_path": "placeholder",
        "wrapper_sha256": successor.sha256_file(wrapper_path),
        "canonical_artifact_root": "data/dbn",
        "activated_at_utc_rule": "SET_ONCE_AT_AUTHORIZED_EXECUTION",
        "pointer_template_id": "fixture",
    }
    _write_json(report / "active_pointer_template.json", template)
    packet_core = {
        "schema_version": "ohlcv_msf_1d_publication_approval_packet/1.0.0",
        "status": "CERTIFIED_NON_ACTIVE_REQUIRES_SEPARATE_PUBLICATION_APPROVAL",
        "scope": "EXACT_NINE_MSF_OHLCV_1D_PARTITIONS_ONLY",
        "successor_release_id": release_id,
        "current_active_pointer": {
            "path": successor.ACTIVE_POINTER_REL.as_posix(),
            "sha256": prior_pointer_sha,
        },
    }
    packet = {**packet_core, "packet_id": successor.sha256_json(packet_core)}
    packet_path = report / "activation_authorization_packet.json"
    _write_json(packet_path, packet)
    packet_sha = successor.sha256_file(packet_path)
    full_scope = successor._required_authorization_scope(packet, packet_sha)
    scope = {
        key: value
        for key, value in full_scope.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    }
    line = _personal_approval_line(
        successor.PUBLICATION_APPROVAL_COMMAND,
        packet["packet_id"],
        packet_sha,
    )
    authorization = OperationReceipt.issue_user_approved(
        RepoBoundary(tmp_path),
        operation=successor.PUBLICATION_OPERATION,
        classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
        scope=scope,
        approval_command=successor.PUBLICATION_APPROVAL_COMMAND,
        approval_plan_id=packet["packet_id"],
        approval_plan_sha256=packet_sha,
        approval_line=line,
    )
    return run_id, authorization, prior_pointer_bytes


def test_candidate_basis_is_deterministic(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(successor, "_source_hash", lambda root: "a" * 64)
    canary = {
        "partitions": [
            {
                "year": 2018,
                "future_dbn_path": "data/dbn/ohlcv_1d/MSF/2018/a.dbn.zst",
                "future_sidecar_path": "data/dbn/ohlcv_1d/MSF/2018/a.dbn.zst.manifest.json",
                "dbn_sha256": "b" * 64,
                "dbn_size_bytes": 1,
                "sidecar_sha256": "c" * 64,
                "sidecar_size_bytes": 2,
                "record_count": 3,
            }
        ]
    }
    active = {"pointer_sha256": "d" * 64}
    first = successor._candidate_basis(tmp_path, canary, active)
    second = successor._candidate_basis(tmp_path, canary, active)
    assert first == second
    assert successor.sha256_json(first) == successor.sha256_json(second)


def test_candidate_copy_is_plain_and_source_independent(tmp_path: Path) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    path = source / "nested/file.bin"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"immutable")
    successor._copy_tree_create_only(source, destination)
    linked = destination / "nested/file.bin"
    assert linked.read_bytes() == b"immutable"
    assert not os.path.samefile(path, linked)
    assert linked.stat().st_nlink == 1
    assert path.stat().st_nlink == 1


def test_authorization_must_bind_exact_packet(tmp_path: Path) -> None:
    run_id = "run"
    report = tmp_path / successor.REPORT_PARENT_REL / run_id
    packet = {
        "packet_id": "d" * 64,
        "successor_release_id": "e" * 64,
        "current_active_pointer": {"sha256": "f" * 64},
    }
    _write_json(report / "activation_authorization_packet.json", packet)
    invalid = OperationReceipt.issue_local(
        RepoBoundary(tmp_path),
        operation=successor.PUBLICATION_OPERATION,
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={},
    )
    with pytest.raises(UnauthorizedOperation, match="classification"):
        successor._verify_authorization(tmp_path, run_id, invalid)
    with pytest.raises(successor.SuccessorError, match="OperationReceipt"):
        successor._verify_authorization(tmp_path, run_id, {})  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "failpoint",
    [
        "before_install",
        "after_install",
        "before_pointer_replace",
        "after_pointer_replace",
        "after_readback",
    ],
)
def test_transaction_failures_restore_exact_prior_root_and_pointer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failpoint: str,
) -> None:
    run_id, authorization, prior_pointer_bytes = _transaction_fixture(tmp_path, monkeypatch)
    with pytest.raises(successor.SuccessorError, match="injected transaction failure"):
        successor.activate_authorized(tmp_path, run_id, authorization, failpoint=failpoint)
    pointer_path = tmp_path / successor.ACTIVE_POINTER_REL
    assert pointer_path.read_bytes() == prior_pointer_bytes
    assert (tmp_path / successor.CANONICAL_ROOT_REL / "definition/ES/2026/current.dbn.zst").read_bytes() == b"prior"
    assert (
        tmp_path
        / successor.SHADOW_PARENT_REL
        / run_id
        / "candidate_additions/data/dbn/ohlcv_1d/MSF/2018/canary.dbn.zst"
    ).read_bytes() == b"successor"
    assert not (tmp_path / successor.LOCK_REL).exists()


def test_activation_readback_and_repeat_are_idempotent_noop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_id, authorization, _ = _transaction_fixture(tmp_path, monkeypatch)
    receipt = successor.activate_authorized(tmp_path, run_id, authorization)
    assert receipt["status"] == "ACTIVATED_AND_VERIFIED"
    assert receipt["readback"]["canonical_artifacts_verified"] == 1
    repeated = successor.activate_authorized(tmp_path, run_id, authorization)
    assert repeated["status"] == "NO_ACTION_ALREADY_ACTIVE_SAME_RELEASE"
    assert repeated["readback"]["status"] == "PASS_ACTIVE_SUCCESSOR_CANONICAL_READBACK"


def test_scope_constants_prohibit_hourly_and_full_58() -> None:
    assert successor.CANARY_REL.as_posix().endswith("data/dbn/ohlcv_1d/MSF")
    assert successor.YEARS == tuple(range(2018, 2027))
    assert "ohlcv_1h" not in successor.CANARY_REL.as_posix()
