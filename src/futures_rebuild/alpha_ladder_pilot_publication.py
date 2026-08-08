"""Create-only publication and ES-pilot registration for the sealed V2 census.

This module performs no row decoding, fitting, prediction, evaluation, execution
claim, pointer mutation, or holdout access.  It publishes byte-identical copies
of the sealed readiness bundle and writes the pilot registration last through
``CertifiedResearchGateway``.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from hashlib import sha256
from pathlib import Path
from typing import Any

from .alpha_ladder_full_regular_readiness_v2 import load_plan
from .alpha_research_ladder import load_active_ladder, validate_stage_registration
from .boundary import RepoBoundary
from .canonical import canonical_bytes, sha256_file, sha256_json
from .certified_research_gateway import CertifiedResearchGateway
from .errors import IntegrityError, UnauthorizedOperation
from .preexecution_fold_certification import load_registration_ready_certificate


REPORT_ID = "cf727f9a2955a9909f74201050f2dfd8ccd11d4b78878feb40ad718c22a98f44"
MECHANISM_ID = "cfefe8ce78e46d1e6a68184cbebdf4f4fe6d46169dc7bbfcfcd501c595563dc3"
MECHANISM_SHA256 = "b63305f7d12e393e5fa7289913c23b47087eee4f3f52ca99e70621b70e3111a1"
PILOT_CERTIFICATE_ID = "ebec0f6cb9db0ab8765975276320a006924504664b24505ff0ce1fa94e1f929b"
TIER1_CERTIFICATE_ID = "15bee0b40e9e6b734b3becfa9512f8ef7ce2263a830cb540546a538ca6839609"
PLAN_ID = "a083052d01c3ff8a276a59d9d39606ca0843b314388f8fb3304b7f5d7516fb3a"
PLAN_SHA256 = "3362639cffe0ee17dddde88fa21374e9769fde4e58fe7c63bb46e979f4a86112"

SOURCE_ROOT = Path("state/unpublished_evidence/alpha_ladder_full_regular_readiness_v2")
PUBLISHED_ROOT = (
    Path("state/preexecution_certificates")
    / "alpha_ladder_full_regular_source_observable"
    / REPORT_ID
)
REGISTRATION_ROOT = Path("state/trial_registry/alpha_ladder_es_pilot")
MECHANISM_PATH = (
    Path("state/unpublished_evidence/alpha_ladder_full_regular_source_observable_successor")
    / MECHANISM_ID
    / "mechanism.json"
)
TIER0_DECISION_PATH = MECHANISM_PATH.with_name("tier0_decision.json")
ACTIVE_TRIAL_POINTER = Path("configs/active_tier1_trial.json")

ARTIFACT_SHA256 = {
    "checkpoint_accounting.json": "ed73bcbab367c043bf01b3600730d68b74c17d294118627dfcb1269f7dfb0d7b",
    "pilot_fold_selection.json": "4f704f98dca994c8020e96b1a7ba3c2e95f6c9b2a16e74d82d55fb6dc2eb622f",
    "pilot_readiness_certificate.json": "3e8fd6169bd2c1b5bfa51795a4a76ae7957e0640fd651e5b632618a5ca0ba497",
    "pilot_session_manifest.json": "291c0b59d10857e44f41f67c07dcade93a93d900ae6eef0b6ac112c724c120fc",
    "readiness_report.json": "d1637d709a2efbc1fee115630eff1bfb4744cd54035981e1b3868c6dad3432e4",
    "source_audit.json": "f7d811875287c1f29a58bb9b3bfd19fbbeba6d5f0c7e94f42ccb55824d10c1d2",
    "tier1_readiness_certificate.json": "e6e7aab27bd2104c7f15dd23934ecbf85c549ffeb6345d551f6b3162f011c48c",
    "tier1_session_manifest.json": "ac4f303ee778718775a88d299b623db9aab4397a7908e25c3edefd497981b875",
}


def _canonical_object(path: Path) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, ValueError) as exc:
        raise IntegrityError(f"invalid sealed Alpha readiness artifact: {path}") from exc
    if not isinstance(payload, dict) or raw != canonical_bytes(payload) + b"\n":
        raise IntegrityError(f"noncanonical sealed Alpha readiness artifact: {path}")
    return payload


def _active_pointer_is_non_authorizing(root: Path) -> str:
    pointer = _canonical_object(root / ACTIVE_TRIAL_POINTER)
    if (
        pointer.get("state") != "NO_ACTIVE_TRIAL_VALID_REJECTION"
        or pointer.get("active_execution_authority") is not False
        or pointer.get("historical_execution_authority") is not False
        or pointer.get("holdout_or_forward_access") is not False
        or pointer.get("trading") is not False
    ):
        raise UnauthorizedOperation("an active historical trial authority already exists")
    return sha256_file(root / ACTIVE_TRIAL_POINTER)


def _publication_manifest(*, root: Path) -> dict[str, object]:
    bindings: dict[str, dict[str, object]] = {}
    for name, expected_sha in sorted(ARTIFACT_SHA256.items()):
        source = SOURCE_ROOT / name
        target = PUBLISHED_ROOT / name
        if sha256_file(root / source) != expected_sha:
            raise IntegrityError(f"sealed Alpha V2 artifact changed: {name}")
        _canonical_object(root / source)
        bindings[name] = {
            "source_path": source.as_posix(),
            "source_sha256": expected_sha,
            "published_path": target.as_posix(),
            "published_sha256": expected_sha,
            "preserved_byte_for_byte": True,
        }
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_readiness_publication/1.0.0",
        "state": "PUBLISHED_REGISTRATION_READY_NOT_EXECUTED",
        "report_id": REPORT_ID,
        "mechanism_id": MECHANISM_ID,
        "plan_id": PLAN_ID,
        "pilot_certificate_id": PILOT_CERTIFICATE_ID,
        "tier1_certificate_id": TIER1_CERTIFICATE_ID,
        "artifact_bindings": bindings,
        "authority": {
            "pilot_registration_required_last": True,
            "historical_execution_claimed": False,
            "economic_evaluation": False,
            "holdout_2025_access": False,
            "provider_network_credentials": False,
            "active_pointer_mutation": False,
            "trading": False,
        },
    }
    return {**core, "publication_id": sha256_json(core)}


def _registration(
    *, root: Path, publication: Mapping[str, object],
    pilot_certificate: Mapping[str, object],
) -> dict[str, object]:
    contract, profile = load_active_ladder(root)
    pilot_manifest_source = SOURCE_ROOT / "pilot_session_manifest.json"
    tier1_manifest_source = SOURCE_ROOT / "tier1_session_manifest.json"
    tier1_certificate_source = SOURCE_ROOT / "tier1_readiness_certificate.json"
    pilot_certificate_target = PUBLISHED_ROOT / "pilot_readiness_certificate.json"
    pilot_manifest = _canonical_object(root / pilot_manifest_source)
    evaluation_ids = pilot_manifest.get("evaluation_session_ids")
    if not isinstance(evaluation_ids, list):
        raise IntegrityError("pilot session manifest lacks evaluation sessions")
    core: dict[str, object] = {
        "schema_version": "alpha_ladder_es_pilot_trial_registration/1.0.0",
        "state": "REGISTERED_NOT_CLAIMED_NOT_EXECUTED",
        "trial_family": pilot_certificate["trial_family"],
        "protocol_id": MECHANISM_ID,
        "stage_scope": {"stage": "pilot", "markets": ["ES"]},
        "fold_readiness_binding": {
            "evidence_path": pilot_certificate_target.as_posix(),
            "evidence_sha256": ARTIFACT_SHA256["pilot_readiness_certificate.json"],
            "certificate_id": PILOT_CERTIFICATE_ID,
        },
        "readiness_publication_binding": {
            "publication_path": (PUBLISHED_ROOT / "publication_manifest.json").as_posix(),
            "publication_id": publication["publication_id"],
            "publication_sha256": sha256(
                canonical_bytes(dict(publication)) + b"\n"
            ).hexdigest(),
            "report_id": REPORT_ID,
        },
        "alpha_ladder_binding": {
            "contract_id": contract["contract_id"],
            "profile_id": profile["profile_id"],
            "stage": "pilot",
            "mechanism_path": MECHANISM_PATH.as_posix(),
            "mechanism_sha256": MECHANISM_SHA256,
            "mechanism_id": MECHANISM_ID,
            "predecessor_decision_path": TIER0_DECISION_PATH.as_posix(),
            "predecessor_decision_sha256": sha256_file(root / TIER0_DECISION_PATH),
            "session_manifest_path": pilot_manifest_source.as_posix(),
            "session_manifest_sha256": ARTIFACT_SHA256["pilot_session_manifest.json"],
            "pilot_evaluation_session_ids_sha256": sha256_json(evaluation_ids),
            "tier_1_readiness_evidence_path": tier1_certificate_source.as_posix(),
            "tier_1_readiness_evidence_sha256": ARTIFACT_SHA256[
                "tier1_readiness_certificate.json"
            ],
            "tier_1_session_manifest_path": tier1_manifest_source.as_posix(),
            "tier_1_session_manifest_sha256": ARTIFACT_SHA256[
                "tier1_session_manifest.json"
            ],
        },
        "authority": {
            "historical_execution_claimed": False,
            "economic_evaluation": False,
            "holdout_2025_access": False,
            "provider_network_credentials": False,
            "active_pointer_mutation": False,
            "trading": False,
        },
    }
    return {**core, "trial_id": sha256_json(core)}


def preflight(*, root: Path) -> dict[str, Any]:
    root = root.resolve(strict=False)
    RepoBoundary(root).assert_active_root(root)
    if sha256_file(root / "configs/alpha_ladder_full_regular_readiness_census_plan_v2.json") != PLAN_SHA256:
        raise IntegrityError("sealed Alpha V2 plan changed")
    plan = load_plan(root=root, verify_protected=True)
    if plan.get("plan_id") != PLAN_ID or plan.get("mechanism_id") != MECHANISM_ID:
        raise IntegrityError("sealed Alpha V2 plan identity changed")
    if sha256_file(root / MECHANISM_PATH) != MECHANISM_SHA256:
        raise IntegrityError("frozen mechanism changed")
    pointer_sha = _active_pointer_is_non_authorizing(root)

    report = _canonical_object(root / SOURCE_ROOT / "readiness_report.json")
    report_core = dict(report)
    report_id = report_core.pop("report_id", None)
    if (
        report_id != REPORT_ID
        or report_id != sha256_json(report_core)
        or report.get("mechanism_id") != MECHANISM_ID
        or report.get("plan_id") != PLAN_ID
        or report.get("pilot_decision") != "PASS"
        or report.get("tier1_decision") != "PASS"
        or report.get("combined_registration_ready") is not True
        or report.get("pilot_certificate_id") != PILOT_CERTIFICATE_ID
        or report.get("tier1_certificate_id") != TIER1_CERTIFICATE_ID
    ):
        raise IntegrityError("sealed Alpha V2 readiness report is not registration-ready")

    pilot_certificate, _pilot_relative, _pilot_sha = load_registration_ready_certificate(
        root=root,
        certificate_evidence_path=root / SOURCE_ROOT / "pilot_readiness_certificate.json",
    )
    tier1_certificate, _tier1_relative, _tier1_sha = load_registration_ready_certificate(
        root=root,
        certificate_evidence_path=root / SOURCE_ROOT / "tier1_readiness_certificate.json",
    )
    if (
        pilot_certificate.get("certificate_id") != PILOT_CERTIFICATE_ID
        or tier1_certificate.get("certificate_id") != TIER1_CERTIFICATE_ID
        or pilot_certificate.get("protocol_id") != MECHANISM_ID
        or tier1_certificate.get("protocol_id") != MECHANISM_ID
    ):
        raise IntegrityError("sealed Alpha V2 certificate identity changed")

    publication = _publication_manifest(root=root)
    registration = _registration(
        root=root, publication=publication, pilot_certificate=pilot_certificate,
    )

    # Preflight the complete ladder semantics before any publication target exists.
    validate_stage_registration(registration, certificate=pilot_certificate, root=root)
    targets = [
        *(PUBLISHED_ROOT / name for name in ARTIFACT_SHA256),
        PUBLISHED_ROOT / "publication_manifest.json",
        REGISTRATION_ROOT / f"{registration['trial_id']}.json",
    ]
    existing = [path.as_posix() for path in targets if (root / path).exists()]
    if existing:
        raise UnauthorizedOperation(
            "create-only Alpha publication target already exists: " + ", ".join(existing)
        )
    return {
        "publication": publication,
        "registration": registration,
        "pilot_certificate": pilot_certificate,
        "tier1_certificate": tier1_certificate,
        "active_trial_pointer_sha256": pointer_sha,
    }


def _write_exclusive(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0),
    )
    try:
        os.write(descriptor, raw)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_and_register(*, root: Path) -> dict[str, str]:
    prepared = preflight(root=root)
    root = root.resolve(strict=False)
    publication = prepared["publication"]
    registration = prepared["registration"]
    assert isinstance(publication, Mapping)
    assert isinstance(registration, Mapping)

    for name in sorted(ARTIFACT_SHA256):
        _write_exclusive(root / PUBLISHED_ROOT / name, (root / SOURCE_ROOT / name).read_bytes())
    manifest_path = root / PUBLISHED_ROOT / "publication_manifest.json"
    _write_exclusive(manifest_path, canonical_bytes(dict(publication)) + b"\n")

    # Registration is deliberately the final write.
    gateway = CertifiedResearchGateway(root=root, boundary=RepoBoundary(root))
    registration_path = root / REGISTRATION_ROOT / f"{registration['trial_id']}.json"
    registered = gateway.register_trial(
        registration_path=registration_path,
        registration=registration,
        readiness_evidence_path=root / PUBLISHED_ROOT / "pilot_readiness_certificate.json",
    )
    if sha256_file(root / ACTIVE_TRIAL_POINTER) != prepared["active_trial_pointer_sha256"]:
        raise IntegrityError("active trial pointer changed during pilot registration")
    return verify_published_registration(root=root, expected=registered)


def verify_published_registration(
    *, root: Path, expected: Mapping[str, str] | None = None,
) -> dict[str, str]:
    root = root.resolve(strict=False)
    publication = _canonical_object(root / PUBLISHED_ROOT / "publication_manifest.json")
    publication_core = dict(publication)
    publication_id = publication_core.pop("publication_id", None)
    if publication_id != sha256_json(publication_core):
        raise IntegrityError("Alpha readiness publication identity changed")
    for name, expected_sha in ARTIFACT_SHA256.items():
        source = root / SOURCE_ROOT / name
        target = root / PUBLISHED_ROOT / name
        if target.read_bytes() != source.read_bytes() or sha256_file(target) != expected_sha:
            raise IntegrityError(f"published Alpha readiness bytes differ: {name}")

    pilot_certificate, _relative, _sha = load_registration_ready_certificate(
        root=root,
        certificate_evidence_path=root / PUBLISHED_ROOT / "pilot_readiness_certificate.json",
    )
    registration_candidates = list((root / REGISTRATION_ROOT).glob("*.json"))
    if len(registration_candidates) != 1:
        raise IntegrityError("ES pilot registry does not contain exactly one registration")
    registration_path = registration_candidates[0]
    registration = _canonical_object(registration_path)
    if registration_path.stem != registration.get("trial_id"):
        raise IntegrityError("ES pilot registration path identity changed")
    validate_stage_registration(registration, certificate=pilot_certificate, root=root)
    _active_pointer_is_non_authorizing(root)
    result = {
        "publication_id": str(publication_id),
        "publication_path": (PUBLISHED_ROOT / "publication_manifest.json").as_posix(),
        "trial_id": str(registration["trial_id"]),
        "registration_path": registration_path.relative_to(root).as_posix(),
        "registration_sha256": sha256_file(registration_path),
        "readiness_certificate_id": str(pilot_certificate["certificate_id"]),
        "state": str(registration["state"]),
    }
    if expected is not None and any(result.get(key) != value for key, value in expected.items()):
        raise IntegrityError("gateway registration result differs after publication")
    return result
