from __future__ import annotations

import json
from pathlib import Path
from shutil import copyfile

from futures_rebuild.canonical import sha256_file
from futures_rebuild.tier1_authoritative_valid_rejection import (
    ACTIVE_POINTER_PATH,
    BUNDLE_PATH,
    load_valid_rejection_preparation,
    load_valid_rejection_preparation_after_publication,
    publish_valid_rejection_closure,
    prepare_valid_rejection_closure,
    verify_published_valid_rejection,
)
from futures_rebuild.tier1_final_unpublished_evidence import verify_unpublished_evidence


ROOT = Path(__file__).resolve().parents[1]


def _is_published(root: Path) -> bool:
    pointer = json.loads((root / ACTIVE_POINTER_PATH).read_text(encoding="utf-8"))
    return pointer.get("state") == "NO_ACTIVE_TRIAL_VALID_REJECTION"


def _preparation(root: Path) -> dict[str, object]:
    if _is_published(root):
        return load_valid_rejection_preparation_after_publication(root=root)
    return load_valid_rejection_preparation(root=root)


def test_preparation_proves_valid_rejection_from_complete_sealed_evidence() -> None:
    preparation = _preparation(ROOT)
    decision = preparation["decision"]
    assert preparation["disposition"] == "VALID_REJECTION_AFTER_SEALED_HISTORICAL_EXECUTION"
    assert decision["candidate_selected_path_complete"] is True
    assert decision["inference_executed"] is True
    assert decision["missing_data_helped_decision"] is False
    assert decision["failed_mandatory_gates"] == [
        "STRESS_NET_PNL_NOT_POSITIVE",
        "CONTINUOUS_DRAWDOWN_EXCEEDS_1500_USD",
    ]
    assert verify_unpublished_evidence(root=ROOT, bundle_path=BUNDLE_PATH)["state"] == "SEALED_UNPUBLISHED"


def test_prepared_closure_is_not_invalid_retirement_and_pointer_is_retired() -> None:
    if _is_published(ROOT):
        verified = verify_published_valid_rejection(root=ROOT)
        preparation = _preparation(ROOT)
        pointer = json.loads((ROOT / ACTIVE_POINTER_PATH).read_text(encoding="utf-8"))
        assert verified["closure_id"] == preparation["record_id"]
        assert pointer["former_pointer_sha256"] == preparation["sealed_bindings"][ACTIVE_POINTER_PATH.as_posix()]
        assert pointer["active_execution_authority"] is False
    else:
        closure, tombstone = prepare_valid_rejection_closure(root=ROOT)
        assert closure["state"] == "CLOSED_VALID_REJECTION"
        assert closure["invalid_retirement"] is False
        assert tombstone["state"] == "NO_ACTIVE_TRIAL_VALID_REJECTION"
        assert tombstone["active_execution_authority"] is False
        assert tombstone["former_pointer_sha256"] == sha256_file(ROOT / ACTIVE_POINTER_PATH)


def test_shadow_publication_binds_closure_and_retires_pointer_last(tmp_path: Path) -> None:
    preparation = _preparation(ROOT)
    paths = set(preparation["sealed_bindings"])
    if _is_published(ROOT):
        paths.remove(ACTIVE_POINTER_PATH.as_posix())
        closure_id = preparation["record_id"]
        paths.update(
            {
                ACTIVE_POINTER_PATH.as_posix(),
                f"state/trial_registry/tier1_authoritative_valid_rejection_closure/{closure_id}.json",
                f"state/trial_events/tier1_authoritative_valid_rejection_closure/{closure_id}.json",
            }
        )
    manifest = verify_unpublished_evidence(root=ROOT, bundle_path=BUNDLE_PATH)
    paths.update((BUNDLE_PATH / name).as_posix() for name in manifest["files"])
    paths.add((BUNDLE_PATH / "manifest.json").as_posix())
    paths.add("configs/tier1_authoritative_valid_rejection_closure_preparation.json")
    for relative in paths:
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        copyfile(ROOT / relative, destination)

    result = publish_valid_rejection_closure(root=tmp_path)
    verified = verify_published_valid_rejection(root=tmp_path)

    assert result["closure_id"] == preparation["record_id"]
    assert verified == {
        "closure_id": preparation["record_id"],
        "pointer_state": "NO_ACTIVE_TRIAL_VALID_REJECTION",
    }
    assert publish_valid_rejection_closure(root=tmp_path) == result
