from __future__ import annotations

from pathlib import Path

import pytest

from futures_rebuild import ohlcv_completion_publication as publication
from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
    _personal_approval_line,
)


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(publication.serialized_json(value))


def _fixture(tmp_path: Path) -> tuple[str, OperationReceipt, bytes]:
    run_id = "ohlcvpub_fixture"
    report = tmp_path / publication.REPORT_PARENT_REL / run_id
    candidate = tmp_path / publication.STAGING_PARENT_REL / run_id / "candidate_additions/data/dbn"
    canonical = tmp_path / publication.CANONICAL_ROOT_REL
    prior_file = canonical / "definition/ES/2026/prior.dbn.zst"
    prior_file.parent.mkdir(parents=True)
    prior_file.write_bytes(b"prior")
    (canonical / "ohlcv_1d").mkdir()
    (canonical / "ohlcv_1h").mkdir()
    additions = [
        ("data/dbn/ohlcv_1h/MSF", b"hourly"),
        ("data/dbn/ohlcv_1d/ZQ", b"daily"),
    ]
    for relative, value in additions:
        path = candidate / Path(relative).relative_to(publication.CANONICAL_ROOT_REL) / "2026/file.dbn.zst"
        path.parent.mkdir(parents=True)
        path.write_bytes(value)
    prior_release_id = "a" * 64
    pointer = {
        "release_id": prior_release_id,
        "release_manifest_path": "reports/prior.json",
        "release_manifest_sha256": "b" * 64,
        "status": "ACTIVE",
    }
    pointer_path = tmp_path / publication.ACTIVE_POINTER_REL
    _write(pointer_path, pointer)
    prior_pointer = pointer_path.read_bytes()
    virtual = publication._inventory(canonical) + publication._inventory(candidate)
    virtual.sort(key=lambda item: item["relative_path"])
    artifacts = []
    for item in virtual:
        artifacts.append(
            {
                "future_project_relative_path": f"data/dbn/{item['relative_path']}",
                "kind": "DBN",
                "sha256": item["sha256"],
                "size_bytes": item["size_bytes"],
                "unit_id": item["relative_path"],
            }
        )
    release_core = {"fixture": True}
    release_id = publication.sha256_json(release_core)
    release = {
        "canonical_artifact_index": artifacts,
        "complete_shadow_tree": publication._inventory_summary(virtual),
        "release_core": release_core,
        "release_id": release_id,
    }
    release_path = report / "successor_release_manifest.json"
    _write(release_path, release)
    wrapper_path = report / "successor_wrapper.json"
    _write(wrapper_path, {"release_id": release_id})
    rollback_path = report / "rollback_plan.json"
    _write(
        rollback_path,
        {
            "added_directories": [value[0] for value in additions],
            "prior_pointer_sha256": publication.sha256_file(pointer_path),
            "prior_release_id": prior_release_id,
        },
    )
    pointer_template_path = report / "active_pointer_template.json"
    _write(
        pointer_template_path,
        {
            "activated_at_utc_rule": "SET_ONCE_AT_AUTHORIZED_EXECUTION",
            "release_id": release_id,
            "release_manifest_path": release_path.relative_to(tmp_path).as_posix(),
            "release_manifest_sha256": publication.sha256_file(release_path),
            "status": "ACTIVE",
            "wrapper_sha256": publication.sha256_file(wrapper_path),
        },
    )
    candidate_summary = publication._inventory_summary(publication._inventory(candidate, include_links=True))
    certificate_path = report / publication.INDEPENDENT_CERTIFICATE_NAME
    _write(
        certificate_path,
        {
            "release_id": release_id,
            "run_id": run_id,
            "status": "PASS_CERTIFIED_NON_ACTIVE_REQUIRES_PUBLICATION_APPROVAL",
        },
    )
    packet_core = {
        "added_directories": [value[0] for value in additions],
        "added_files": candidate_summary["file_count"],
        "added_logical_bytes": candidate_summary["total_bytes"],
        "candidate_inventory_sha256": candidate_summary["inventory_sha256"],
        "current_active_pointer_sha256": publication.sha256_file(pointer_path),
        "independent_certificate_sha256": publication.sha256_file(certificate_path),
        "pointer_template_sha256": publication.sha256_file(pointer_template_path),
        "provider_access": False,
        "release_manifest_sha256": publication.sha256_file(release_path),
        "rollback_plan_sha256": publication.sha256_file(rollback_path),
        "run_id": run_id,
        "schema_version": "ohlcv_58_completion_publication_approval_packet/2.0.0",
        "successor_release_id": release_id,
        "successor_wrapper_sha256": publication.sha256_file(wrapper_path),
    }
    packet = {**packet_core, "packet_id": publication.sha256_json(packet_core)}
    packet_path = report / publication.APPROVAL_PACKET_NAME
    _write(packet_path, packet)
    packet_sha = publication.sha256_file(packet_path)
    full_scope = publication.required_publication_scope(packet, packet_sha)
    scope = {
        key: value
        for key, value in full_scope.items()
        if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
    }
    line = _personal_approval_line(publication.PUBLICATION_OPERATION, packet["packet_id"], packet_sha)
    receipt = OperationReceipt.issue_user_approved(
        RepoBoundary(tmp_path),
        operation=publication.PUBLICATION_OPERATION,
        classification=OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
        scope=scope,
        approval_command=publication.PUBLICATION_OPERATION,
        approval_plan_id=packet["packet_id"],
        approval_plan_sha256=packet_sha,
        approval_line=line,
    )
    return run_id, receipt, prior_pointer


@pytest.mark.parametrize(
    "failpoint",
    [
        "before_install",
        "after_install_1",
        "after_install_2",
        "before_pointer_replace",
        "after_pointer_replace",
        "after_readback",
    ],
)
def test_publication_failures_restore_pointer_and_every_addition(
    tmp_path: Path, failpoint: str
) -> None:
    run_id, receipt, prior_pointer = _fixture(tmp_path)

    with pytest.raises(publication.IntegrityError, match="injected"):
        publication.activate_authorized(tmp_path, run_id, receipt, failpoint=failpoint)

    assert (tmp_path / publication.ACTIVE_POINTER_REL).read_bytes() == prior_pointer
    assert not (tmp_path / "data/dbn/ohlcv_1h/MSF").exists()
    assert not (tmp_path / "data/dbn/ohlcv_1d/ZQ").exists()
    candidate = tmp_path / publication.STAGING_PARENT_REL / run_id / "candidate_additions/data/dbn"
    assert (candidate / "ohlcv_1h/MSF/2026/file.dbn.zst").read_bytes() == b"hourly"
    assert (candidate / "ohlcv_1d/ZQ/2026/file.dbn.zst").read_bytes() == b"daily"
    assert not (tmp_path / publication.LOCK_REL).exists()


def test_publication_activation_and_repeat_are_idempotent(tmp_path: Path) -> None:
    run_id, receipt, _ = _fixture(tmp_path)

    result = publication.activate_authorized(tmp_path, run_id, receipt)
    repeated = publication.activate_authorized(tmp_path, run_id, receipt)

    assert result["status"] == "ACTIVATED_AND_VERIFIED"
    assert result["readback"]["canonical_artifacts_verified"] == 3
    assert repeated["status"] == "NO_ACTION_ALREADY_ACTIVE_SAME_RELEASE"
    assert repeated["readback"]["status"] == "PASS_ACTIVE_SUCCESSOR_CANONICAL_READBACK"


def test_evidence_only_unit_creates_no_placeholder_artifacts(tmp_path: Path) -> None:
    evidence = {
        "evidence_path": "reports/evidence.json",
        "evidence_sha256": "a" * 64,
        "job_id": "GLBX-20260821-AAAAAAAAAA",
        "provider_error_code": "symbology_invalid_request",
        "provider_error_message": "None of the symbols could be resolved",
        "provider_error_status": 422,
        "provider_manifest_hash": "b" * 64,
        "request_fingerprint": "c" * 64,
        "schema_version": "ohlcv_provider_no_data_evidence/1.0.0",
    }
    row = {
        "current_state": "NO_DATA_CONFIRMED",
        "execution_action": "NO_FILE_CREATE",
        "final_path": "data/dbn/ohlcv_1d/MJY/2023/file.dbn.zst",
        "intended_end_exclusive": "2024-01-01T00:00:00Z",
        "intended_start_inclusive": "2023-01-01T00:00:00Z",
        "market": "MJY",
        "no_data_evidence": evidence,
        "provider_record_count": 0,
        "schema": "ohlcv-1d",
        "sidecar_path": "data/dbn/ohlcv_1d/MJY/2023/file.dbn.zst.manifest.json",
        "year": 2023,
    }

    unit, artifacts = publication._addition_unit(
        tmp_path,
        tmp_path / "state",
        row,
        manifest_sha256="d" * 64,
        candidate_root=tmp_path / "candidate",
    )

    assert unit["canonical_dbn"] is None
    assert unit["publication_action"] == "NO_FILE_CREATE"
    assert artifacts == []


def test_packet_drift_fails_before_authorization_consumption(tmp_path: Path) -> None:
    run_id, receipt, _ = _fixture(tmp_path)
    packet_path = tmp_path / publication.REPORT_PARENT_REL / run_id / publication.APPROVAL_PACKET_NAME
    packet = publication.load_json(packet_path, "packet")
    packet["added_logical_bytes"] += 1
    _write(packet_path, packet)

    with pytest.raises(publication.IntegrityError, match="packet"):
        publication.activate_authorized(tmp_path, run_id, receipt)

    use_path = tmp_path / "state/authorization_uses" / f"{receipt.receipt_id}.json"
    assert not use_path.exists()
