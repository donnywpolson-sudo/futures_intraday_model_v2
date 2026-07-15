import os
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from futures_rebuild.boundary import (
    EXTERNAL_SIGNATURE_ALGORITHM,
    EXTERNAL_AUTHORITY_REGISTRY_HASH,
    OperationClassification,
    OperationReceipt,
)
from futures_rebuild.canonical import sha256_json
from futures_rebuild.errors import ContractError, UnauthorizedOperation
from futures_rebuild.release import AtomicPublisher, VerifiedReleaseReceipt, verify_release


@pytest.mark.parametrize("component", ["staging", "publication", "lock"])
@pytest.mark.parametrize("root_kind", ["legacy", "stock", "outside"])
def test_release_writer_rejects_every_nonactive_root_before_creating_paths(
    boundary, operation_factory, tmp_path, component, root_kind
) -> None:
    roots = {
        "legacy": boundary.legacy_roots[0],
        "stock": boundary.foreign_roots[0],
        "outside": tmp_path / "unscoped",
    }
    forbidden = roots[root_kind] / "must-not-exist" / component
    arguments = {
        "staging": boundary.active_root / "data" / "vault" / ".staging" / "releases" / "test",
        "publication": boundary.active_root / "data" / "vault" / "releases",
        "lock": boundary.active_root / "state" / "locks" / "publish.lock",
    }
    arguments[component] = forbidden
    with pytest.raises(UnauthorizedOperation):
        AtomicPublisher(
            arguments["staging"],
            arguments["publication"],
            arguments["lock"],
            boundary=boundary,
            operation_receipt=operation_factory("PUBLISH_RELEASE"),
        )
    assert not forbidden.exists()


def test_boundary_requires_exact_content_addressed_snapshot_location(boundary) -> None:
    valid = boundary.active_root / "data" / "vault" / "source_snapshots" / ("a" * 64)
    assert boundary.assert_snapshot_path(valid) == valid.resolve(strict=False)
    for invalid in (
        boundary.active_root / "data" / "vault" / "source_snapshots" / "not-a-hash",
        valid / "nested",
        boundary.active_root / "data" / "vault" / "other" / ("a" * 64),
    ):
        with pytest.raises(UnauthorizedOperation):
            boundary.assert_snapshot_path(invalid)


def test_local_code_cannot_issue_candidate_or_real_history_authority(boundary) -> None:
    for classification in (
        OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION,
        OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
    ):
        with pytest.raises(UnauthorizedOperation):
            OperationReceipt.issue_local(
                boundary,
                operation="FORBIDDEN",
                classification=classification,
            )


def test_recomputed_external_receipt_hash_without_authority_signature_is_rejected(
    boundary,
) -> None:
    issued = datetime.now(timezone.utc)
    core = {
        "authority_registry_hash": EXTERNAL_AUTHORITY_REGISTRY_HASH,
        "authority_key_id": "USER_GATED_REBUILD_AUTHORITY_V1",
        "classification": OperationClassification.EXTERNAL_CANDIDATE_AUTHORIZATION.value,
        "externally_authorized": True,
        "expires_at": (issued + timedelta(hours=1)).isoformat(),
        "issued_at": issued.isoformat(),
        "nonce": "a" * 64,
        "not_before": issued.isoformat(),
        "operation": "SEAL_CANDIDATE_BUNDLE",
        "repository_id": boundary.repository_id,
        "scope": [],
        "signature_algorithm": EXTERNAL_SIGNATURE_ALGORITHM,
        "single_use": True,
    }
    payload = {
        **core,
        "receipt_id": sha256_json(core),
        "signature_hex": "00" * 256,
    }
    path = boundary.active_root / "state" / "authorizations" / "forged.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(UnauthorizedOperation, match="pinned-authority signature"):
        OperationReceipt.load_external(path, boundary)


def test_verified_release_receipt_requires_exact_active_release_tree(
    boundary, operation_factory
) -> None:
    with pytest.raises(UnauthorizedOperation):
        AtomicPublisher(
            boundary.active_root / "data" / "vault" / ".staging" / "releases" / "other",
            boundary.active_root / "other-releases",
            boundary.active_root / "state" / "locks" / "other.lock",
            boundary=boundary,
            operation_receipt=operation_factory("PUBLISH_RELEASE"),
        )


def test_release_verification_rejects_hardlinked_payload(
    boundary, release_factory
) -> None:
    release, _ = release_factory(
        release_kind="synthetic_hardlink_test",
        filename="rows.bin",
        content=b"synthetic",
    )
    payload = release / "rows.bin"
    alias = boundary.active_root / "state" / "hardlink-alias.bin"
    alias.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(payload, alias)
    except OSError as exc:
        pytest.skip(f"hard links unavailable on this filesystem: {exc}")
    with pytest.raises(ContractError, match="Hard-linked|hard-linked"):
        verify_release(release)
