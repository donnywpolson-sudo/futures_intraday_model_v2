"""Synthetic Tier 0 evidence for the counted reported-trade-exit mechanism."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from .alpha_ladder_frozen_mechanism import (
    build_tier0_certificate,
    build_tier0_decision,
    validate_tier0_certificate,
)
from .alpha_ladder_limit_exit_closure_publication import (
    CLOSURE_ID,
    registry_path as closure_registry_path,
    verify_published_closure,
)
from .alpha_ladder_reported_trade_exit_successor import (
    PREDECESSOR_PATH,
    build_closure,
    validate_closure,
    validate_successor,
)
from .alpha_research_ladder import load_registered_ladder, validate_stage_decision
from .canonical import canonical_bytes, sha256_file, sha256_json
from .errors import IntegrityError, UnauthorizedOperation


MECHANISM_ID = "50dfc52cb5b4145dcbd6a761b3c626dae28c0aa974f6db35a1b60099297034e5"
MECHANISM_SHA256 = "ddd1a3549ebad192fec3e00059170d3404a4cbb3d61e44cd347db56d4146941d"
MECHANISM_PATH = Path(
    "state/unpublished_evidence/alpha_ladder_reported_trade_exit_successor"
) / MECHANISM_ID / "mechanism.json"
TIER0_CERTIFICATE_PATH = MECHANISM_PATH.with_name("tier0_certificate.json")
TIER0_DECISION_PATH = MECHANISM_PATH.with_name("tier0_decision.json")
CERTIFIER_MODULE_PATH = Path(
    "src/futures_rebuild/alpha_ladder_reported_trade_exit_tier0.py"
)
CERTIFIER_SCRIPT_PATH = Path(
    "scripts/certify_alpha_ladder_reported_trade_exit_tier0.py"
)
ACTIVE_LADDER_POINTER_PATH = Path("configs/active_alpha_research_ladder.json")
SUITE_MARKER = "high_risk"
COLLECTION_MODE = "ENTIRE_HIGH_RISK_LANE"
REQUIRED_TEST_FILES = frozenset({
    "tests/test_alpha_ladder_reported_trade_exit.py",
    "tests/test_alpha_ladder_limit_exit_closure_publication.py",
    "tests/test_alpha_ladder_reported_trade_exit_tier0.py",
    "tests/test_alpha_ladder_counted_mechanism.py",
    "tests/test_alpha_ladder_limit_readiness.py",
    "tests/test_alpha_research_ladder.py",
    "tests/test_certified_research_gateway.py",
    "tests/test_preexecution_fold_certification.py",
    "tests/test_tier1_trade_triggered_trial_design.py",
    "tests/test_tier1_bracket_evaluation.py",
    "tests/test_tier1_phase8_evaluator.py",
    "tests/test_research_hac_bootstrap_rw.py",
})


def _canonical_object(path: Path) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise IntegrityError(f"Tier 0 artifact is unreadable: {path}") from exc
    if not isinstance(value, dict) or raw != canonical_bytes(value) + b"\n":
        raise IntegrityError(f"Tier 0 artifact is not canonical: {path}")
    return value


def _test_file(node_id: str) -> str:
    path, separator, _rest = node_id.partition("::")
    normalized = path.replace("\\", "/")
    if not separator or not normalized.startswith("tests/") or not normalized.endswith(".py"):
        raise IntegrityError(f"invalid Tier 0 test node: {node_id}")
    return normalized


def validate_mechanism_context(*, root: Path) -> tuple[dict[str, object], dict[str, object]]:
    if sha256_file(root / MECHANISM_PATH) != MECHANISM_SHA256:
        raise IntegrityError("reported-trade-exit mechanism changed")
    mechanism = _canonical_object(root / MECHANISM_PATH)
    predecessor = _canonical_object(root / PREDECESSOR_PATH)
    closure = build_closure(root=root)
    validate_closure(closure, root=root)
    validate_successor(
        mechanism, predecessor=predecessor, closure=closure, root=root,
    )
    published = verify_published_closure(root=root)
    ladder_binding = mechanism.get("ladder_binding")
    if not isinstance(ladder_binding, Mapping):
        raise IntegrityError("reported-trade-exit mechanism lost its ladder binding")
    contract, profile = load_registered_ladder(
        root,
        contract_id=str(ladder_binding.get("contract_id", "")),
        profile_id=str(ladder_binding.get("profile_id", "")),
    )
    authority = mechanism.get("authority")
    outcomes = mechanism.get("outcome_access")
    if (
        mechanism.get("mechanism_id") != MECHANISM_ID
        or mechanism.get("restart_stage") != "tier_0"
        or mechanism.get("state")
        != "PREPARED_UNPUBLISHED_UNREGISTERED_TIER0_RESTART_REQUIRED"
        or published.get("closure_id") != CLOSURE_ID
        or not isinstance(authority, Mapping)
        or any(value is not False for value in authority.values())
        or not isinstance(outcomes, Mapping)
        or any(value is not False for value in outcomes.values())
    ):
        raise IntegrityError("reported-trade-exit Tier 0 context is not fail closed")
    return contract, profile


def _suite_bindings(*, root: Path, nodes: Sequence[str]) -> dict[str, str]:
    test_files = {_test_file(node) for node in nodes}
    if not REQUIRED_TEST_FILES <= test_files:
        missing = sorted(REQUIRED_TEST_FILES - test_files)
        raise UnauthorizedOperation(f"Tier 0 suite omitted required tests: {missing}")
    paths = {
        *test_files,
        MECHANISM_PATH.as_posix(),
        PREDECESSOR_PATH.as_posix(),
        closure_registry_path().as_posix(),
        ACTIVE_LADDER_POINTER_PATH.as_posix(),
        CERTIFIER_MODULE_PATH.as_posix(),
        CERTIFIER_SCRIPT_PATH.as_posix(),
    }
    return dict(sorted(
        (relative, sha256_file(root / relative)) for relative in paths
    ))


def build_certificate(
    *, root: Path, collected_test_nodes: Sequence[str],
) -> dict[str, object]:
    contract, profile = validate_mechanism_context(root=root)
    nodes = tuple(sorted(collected_test_nodes))
    if len(nodes) < 100 or len(nodes) != len(set(nodes)):
        raise UnauthorizedOperation("Tier 0 requires the complete unique high-risk lane")
    bindings = _suite_bindings(root=root, nodes=nodes)
    base = build_tier0_certificate(
        contract_id=str(contract["contract_id"]),
        profile_id=str(profile["profile_id"]),
        mechanism_id=MECHANISM_ID,
        mechanism_sha256=MECHANISM_SHA256,
        test_node_ids=nodes,
        passed_test_count=len(nodes),
    )
    core = {key: value for key, value in base.items() if key != "certificate_id"}
    core.update({
        "collection_mode": COLLECTION_MODE,
        "pytest_marker": SUITE_MARKER,
        "collected_test_count": len(nodes),
        "required_test_files": sorted(REQUIRED_TEST_FILES),
        "suite_bindings": bindings,
        "published_predecessor_closure_id": CLOSURE_ID,
        "source_compatibility_claim": False,
        "registration_authority": False,
        "historical_execution_authority": False,
        "holdout_2025_access": False,
    })
    certificate = {**core, "certificate_id": sha256_json(core)}
    validate_certificate(certificate, root=root)
    return certificate


def validate_certificate(
    certificate: Mapping[str, object], *, root: Path,
) -> dict[str, object]:
    contract, profile = validate_mechanism_context(root=root)
    validate_tier0_certificate(
        certificate,
        contract_id=str(contract["contract_id"]),
        mechanism_sha256=MECHANISM_SHA256,
    )
    nodes = certificate.get("test_node_ids")
    bindings = certificate.get("suite_bindings")
    if not isinstance(nodes, Sequence) or isinstance(nodes, (str, bytes)):
        raise IntegrityError("Tier 0 test nodes are malformed")
    normalized_nodes = tuple(str(node) for node in nodes)
    expected_bindings = _suite_bindings(root=root, nodes=normalized_nodes)
    core = {key: value for key, value in certificate.items() if key != "certificate_id"}
    if (
        certificate.get("certificate_id") != sha256_json(core)
        or certificate.get("profile_id") != profile.get("profile_id")
        or certificate.get("mechanism_id") != MECHANISM_ID
        or certificate.get("collection_mode") != COLLECTION_MODE
        or certificate.get("pytest_marker") != SUITE_MARKER
        or certificate.get("collected_test_count") != len(normalized_nodes)
        or len(normalized_nodes) < 100
        or tuple(sorted(normalized_nodes)) != normalized_nodes
        or certificate.get("required_test_files") != sorted(REQUIRED_TEST_FILES)
        or not isinstance(bindings, Mapping)
        or dict(bindings) != expected_bindings
        or certificate.get("published_predecessor_closure_id") != CLOSURE_ID
        or certificate.get("source_compatibility_claim") is not False
        or certificate.get("registration_authority") is not False
        or certificate.get("historical_execution_authority") is not False
        or certificate.get("holdout_2025_access") is not False
    ):
        raise IntegrityError("reported-trade-exit Tier 0 certificate is invalid")
    return dict(certificate)


def build_decision(
    *, root: Path, certificate: Mapping[str, object],
) -> dict[str, object]:
    contract, _profile = validate_mechanism_context(root=root)
    validate_certificate(certificate, root=root)
    certificate_sha = hashlib.sha256(
        canonical_bytes(dict(certificate)) + b"\n"
    ).hexdigest()
    return build_tier0_decision(
        contract_id=str(contract["contract_id"]),
        mechanism_sha256=MECHANISM_SHA256,
        synthetic_certificate_path=TIER0_CERTIFICATE_PATH.as_posix(),
        synthetic_certificate_sha256=certificate_sha,
    )


def validate_live_evidence(*, root: Path) -> dict[str, str]:
    certificate_exists = (root / TIER0_CERTIFICATE_PATH).exists()
    decision_exists = (root / TIER0_DECISION_PATH).exists()
    if certificate_exists != decision_exists or not certificate_exists:
        raise IntegrityError("Tier 0 certificate and decision are not a complete pair")
    certificate = _canonical_object(root / TIER0_CERTIFICATE_PATH)
    decision = _canonical_object(root / TIER0_DECISION_PATH)
    validate_certificate(certificate, root=root)
    contract, _profile = validate_mechanism_context(root=root)
    validate_stage_decision(
        decision,
        contract_id=str(contract["contract_id"]),
        mechanism_sha256=MECHANISM_SHA256,
        expected_stage="tier_0",
        root=root,
    )
    if decision != build_decision(root=root, certificate=certificate):
        raise IntegrityError("Tier 0 decision differs from its exact certificate")
    return {
        "mechanism_id": MECHANISM_ID,
        "certificate_id": str(certificate["certificate_id"]),
        "certificate_path": TIER0_CERTIFICATE_PATH.as_posix(),
        "certificate_sha256": sha256_file(root / TIER0_CERTIFICATE_PATH),
        "decision_id": str(decision["decision_id"]),
        "decision_path": TIER0_DECISION_PATH.as_posix(),
        "decision_sha256": sha256_file(root / TIER0_DECISION_PATH),
        "passed_test_count": str(certificate["passed_test_count"]),
    }
