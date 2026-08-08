"""Resumable, create-only Phase 8 current-rulebook rebuild.

This module intentionally has no command-line entry point.  A Codex task may
call it only after conversational approval.  It publishes one independently
verified definition, causal, and economics successor at a time, then records
only the resulting receipt IDs in a small local checkpoint.
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Mapping

from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import canonical_bytes
from .data_layout import DataReleaseReceipt, PhasePublisher
from .errors import IntegrityError
from .foundation.materialize import materialize_causal_interval
from .foundation.support import VerifiedFoundationPolicies, publish_foundation_policies
from .contract_economics_audit import require_phase8_passing_contract_economics_audit
from .phase8_economics_index import (
    Phase8EconomicsContext,
    load_phase8_interval_selection,
    publish_phase8_actual_contract_economics_index,
    publish_phase8_interval_selection,
)
from .producer_bridge import (
    load_actual_contract_definitions,
    load_versioned_session_policy,
    publish_actual_contract_definitions,
    publish_actual_contract_economics,
    publish_versioned_session_policy,
)


CHECKPOINT_SCHEMA_VERSION = "phase8_current_rulebook_rebuild/1.0.0"


def _receipt(root: Path, phase: str, release_id: str, boundary: RepoBoundary) -> DataReleaseReceipt:
    return DataReleaseReceipt.from_manifest(
        root / "manifests" / "data_releases" / phase / f"{release_id}.json",
        boundary,
    )


def _write_checkpoint(path: Path, payload: Mapping[str, object], boundary: RepoBoundary) -> None:
    path = boundary.assert_active_path(
        path, purpose="Phase 8 rebuild checkpoint", subtree="state"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = canonical_bytes(dict(payload)) + b"\n"
    temporary = path.parent / f".{path.name}-{uuid.uuid4().hex}.tmp"
    descriptor = os.open(
        temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0)
    )
    try:
        os.write(descriptor, encoded)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)


def _load_checkpoint(
    path: Path, *, selection_receipt_id: str, boundary: RepoBoundary
) -> dict[str, object]:
    if not path.exists():
        return {
            "completed": {},
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "selection_receipt_id": selection_receipt_id,
        }
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError("Phase 8 rebuild checkpoint is unreadable") from exc
    if (
        not isinstance(payload, dict)
        or raw != canonical_bytes(payload) + b"\n"
        or set(payload) != {"completed", "schema_version", "selection_receipt_id"}
        or payload["schema_version"] != CHECKPOINT_SCHEMA_VERSION
        or payload["selection_receipt_id"] != selection_receipt_id
        or not isinstance(payload["completed"], dict)
    ):
        raise IntegrityError("Phase 8 rebuild checkpoint differs from this selection")
    return payload


def _verify_completed(
    completed: Mapping[str, object], *, repository_root: Path, boundary: RepoBoundary
) -> None:
    """Reject a checkpoint that tries to skip an unverified successor chain."""

    for prior_causal_id, raw_row in completed.items():
        if type(prior_causal_id) is not str or not isinstance(raw_row, dict) or set(raw_row) != {
            "causal_release_id", "definition_release_id", "economics_release_id",
            "prior_causal_release_id", "raw_release_id",
        } or raw_row["prior_causal_release_id"] != prior_causal_id:
            raise IntegrityError("Phase 8 rebuild checkpoint entry is invalid")
        raw = _receipt(repository_root, "raw", str(raw_row["raw_release_id"]), boundary)
        definitions = _receipt(repository_root, "reference", str(raw_row["definition_release_id"]), boundary)
        causal = _receipt(repository_root, "causally_gated_normalized", str(raw_row["causal_release_id"]), boundary)
        economics = _receipt(repository_root, "reference", str(raw_row["economics_release_id"]), boundary)
        if (
            raw.release_id not in definitions.verify(boundary).source_release_ids
            or raw.release_id not in causal.verify(boundary).source_release_ids
            or causal.release_id not in economics.verify(boundary).source_release_ids
            or definitions.release_id not in economics.verify(boundary).source_release_ids
        ):
            raise IntegrityError("Phase 8 rebuild checkpoint successor provenance is invalid")


def rebuild_selected_interval_chains(
    *,
    repository_root: Path,
    selection_receipt: DataReleaseReceipt,
    checkpoint_path: Path,
    maximum_intervals: int,
) -> dict[str, object]:
    """Rebuild at most ``maximum_intervals`` chains and durably checkpoint them.

    The caller is responsible for the conversational high-risk boundary.  The
    function deliberately does not publish an aggregate index; that requires
    a complete, separately revalidated 677-entry result.
    """

    if type(maximum_intervals) is not int or maximum_intervals <= 0:
        raise IntegrityError("Phase 8 rebuild maximum interval count is invalid")
    boundary = RepoBoundary(repository_root)
    selection = load_phase8_interval_selection(selection_receipt, boundary=boundary)
    checkpoint = _load_checkpoint(
        checkpoint_path, selection_receipt_id=selection_receipt.release_id, boundary=boundary
    )
    completed = dict(checkpoint["completed"])
    _verify_completed(completed, repository_root=repository_root, boundary=boundary)
    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={"operation": "phase8_current_rulebook_rebuild"},
    )
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation,
        lock_path=boundary.active_root / "state" / "locks" / "phase8-current-rulebook-rebuild.lock",
    )
    policy_receipt = publish_foundation_policies(
        boundary=boundary, publisher=publisher, config_root=boundary.active_root / "configs"
    )
    policies = VerifiedFoundationPolicies.from_release(policy_receipt, boundary=boundary)
    session_receipt = publish_versioned_session_policy(
        policies=policies, boundary=boundary, publisher=publisher
    )
    session_policy = load_versioned_session_policy(session_receipt, policies=policies, boundary=boundary)
    rebuilt: list[dict[str, str]] = []
    for interval in selection["intervals"]:  # type: ignore[index]
        causal_id = str(interval["release_id"])
        if causal_id in completed:
            continue
        old_causal = _receipt(repository_root, "causally_gated_normalized", causal_id, boundary)
        old_manifest = old_causal.verify(boundary)
        raw_ids = [
            release_id for release_id in old_manifest.source_release_ids
            if (repository_root / "manifests" / "data_releases" / "raw" / f"{release_id}.json").exists()
        ]
        if len(raw_ids) != 1:
            raise IntegrityError("selected causal interval has no unique pinned raw release")
        raw_receipt = _receipt(repository_root, "raw", raw_ids[0], boundary)
        definitions = publish_actual_contract_definitions(
            raw_receipt=raw_receipt, policies=policies, boundary=boundary, publisher=publisher
        )
        causal = materialize_causal_interval(
            raw_receipt=raw_receipt, policies=policies, publisher=publisher
        )
        economics = publish_actual_contract_economics(
            causal_receipt=causal,
            definitions=load_actual_contract_definitions(
                definitions, raw_receipt=raw_receipt, policies=policies, boundary=boundary
            ),
            policies=policies,
            session_policy=session_policy,
            boundary=boundary,
            publisher=publisher,
        )
        row = {
            "causal_release_id": causal.release_id,
            "definition_release_id": definitions.release_id,
            "economics_release_id": economics.release_id,
            "prior_causal_release_id": causal_id,
            "raw_release_id": raw_receipt.release_id,
        }
        completed[causal_id] = row
        checkpoint = {
            "completed": dict(sorted(completed.items())),
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "selection_receipt_id": selection_receipt.release_id,
        }
        _write_checkpoint(checkpoint_path, checkpoint, boundary)
        rebuilt.append(row)
        if len(rebuilt) >= maximum_intervals:
            break
    return {
        "completed_interval_count": len(completed),
        "rebuilt": rebuilt,
        "remaining_interval_count": 677 - len(completed),
        "policy_receipt_id": policy_receipt.release_id,
        "session_receipt_id": session_receipt.release_id,
    }


def publish_completed_rebuild_index(
    *,
    repository_root: Path,
    selection_receipt: DataReleaseReceipt,
    audit_receipt: DataReleaseReceipt,
    checkpoint_path: Path,
) -> dict[str, str]:
    """Publish the successor selection and index only after all chains verify.

    This is deliberately a separate finalization step.  It refuses a partial
    checkpoint, derives every new causal ID from that checkpoint rather than
    filesystem discovery, and revalidates each registry through the aggregate
    index publisher before returning its receipts.
    """

    boundary = RepoBoundary(repository_root)
    selection = load_phase8_interval_selection(selection_receipt, boundary=boundary)
    checkpoint = _load_checkpoint(
        checkpoint_path, selection_receipt_id=selection_receipt.release_id, boundary=boundary
    )
    completed = dict(checkpoint["completed"])
    if len(completed) != 677 or set(completed) != {
        str(item["release_id"]) for item in selection["intervals"]  # type: ignore[index]
    }:
        raise IntegrityError("Phase 8 successor index requires all 677 validated chains")
    _verify_completed(completed, repository_root=repository_root, boundary=boundary)

    operation = OperationReceipt.issue_local(
        boundary,
        operation="PUBLISH_RELEASE",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
        scope={"operation": "phase8_current_rulebook_index_publication"},
    )
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation,
        lock_path=boundary.active_root / "state" / "locks" / "phase8-current-rulebook-rebuild.lock",
    )
    policy_receipt = publish_foundation_policies(
        boundary=boundary, publisher=publisher, config_root=boundary.active_root / "configs"
    )
    policies = VerifiedFoundationPolicies.from_release(policy_receipt, boundary=boundary)
    require_phase8_passing_contract_economics_audit(
        audit_receipt, boundary=boundary, rulebook=policies.economics
    )
    session_receipt = publish_versioned_session_policy(
        policies=policies, boundary=boundary, publisher=publisher
    )
    session_policy = load_versioned_session_policy(
        session_receipt, policies=policies, boundary=boundary
    )

    successor_by_prior = {
        prior_id: str(row["causal_release_id"])
        for prior_id, row in completed.items()
    }
    successor_intervals = []
    for item in selection["intervals"]:  # type: ignore[index]
        successor = dict(item)
        successor["release_id"] = successor_by_prior[str(item["release_id"])]
        successor_intervals.append(successor)
    successor_selection_payload = {
        "foundation_manifest_sha256": selection["foundation_manifest_sha256"],
        "foundation_release_id": selection["foundation_release_id"],
        "intervals": successor_intervals,
        "market_years": [
            {
                **dict(item),
                "source_release_ids": [
                    successor_by_prior[str(release_id)]
                    for release_id in item["source_release_ids"]
                ],
            }
            for item in selection["market_years"]  # type: ignore[index]
        ],
        "schema_version": selection["schema_version"],
    }
    successor_selection = publish_phase8_interval_selection(
        payload=successor_selection_payload, boundary=boundary, publisher=publisher
    )

    contexts: dict[str, Phase8EconomicsContext] = {}
    economics_by_causal: dict[str, DataReleaseReceipt] = {}
    for prior_id, row in completed.items():
        raw = _receipt(repository_root, "raw", str(row["raw_release_id"]), boundary)
        causal = _receipt(repository_root, "causally_gated_normalized", str(row["causal_release_id"]), boundary)
        definitions_receipt = _receipt(repository_root, "reference", str(row["definition_release_id"]), boundary)
        economics = _receipt(repository_root, "reference", str(row["economics_release_id"]), boundary)
        definitions = load_actual_contract_definitions(
            definitions_receipt, raw_receipt=raw, policies=policies, boundary=boundary
        )
        contexts[causal.release_id] = Phase8EconomicsContext(
            causal_receipt=causal,
            definitions=definitions,
            policies=policies,
            session_policy=session_policy,
        )
        economics_by_causal[causal.release_id] = economics

    index_receipt = publish_phase8_actual_contract_economics_index(
        selection_receipt=successor_selection,
        audit_receipt=audit_receipt,
        economics_by_causal_release=economics_by_causal,
        rulebook=policies.economics,
        contexts_by_causal_release=contexts,
        boundary=boundary,
        publisher=publisher,
    )
    return {
        "index_receipt_id": index_receipt.release_id,
        "policy_receipt_id": policy_receipt.release_id,
        "selection_receipt_id": successor_selection.release_id,
        "session_receipt_id": session_receipt.release_id,
    }
