"""Synthetically certify the sealed counted Alpha mechanism as Tier 0."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from futures_rebuild.alpha_ladder_frozen_mechanism import (
    build_tier0_certificate,
    build_tier0_decision,
    validate_tier0_certificate,
)
from futures_rebuild.alpha_ladder_limit_readiness import (
    MECHANISM_ID,
    MECHANISM_PATH,
    MECHANISM_SHA256,
    TIER0_CERTIFICATE_PATH,
    TIER0_DECISION_PATH,
)
from futures_rebuild.alpha_ladder_source_compatible_successor import validate_successor
from futures_rebuild.alpha_research_ladder import load_active_ladder, validate_stage_decision
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.cash_open_source_compatibility_census import _read_canonical
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
PREDECESSOR = Path(
    "state/unpublished_evidence/alpha_ladder_frozen_mechanism/"
    "186d8a103a581ae8c27fc531e0a556070991c9d2f87bbe5d62c1478867b5de3f/mechanism.json"
)
REJECTION = Path(
    "state/unpublished_evidence/alpha_ladder_v3_source_incompatibility_rejection/"
    "45011788be8b275a3aa874834f7382a960c8371aafbdc118b645d3b165d5ffbf/rejection.json"
)
TIER0_NODES = (
    "tests/test_alpha_ladder_counted_mechanism.py",
    "tests/test_alpha_ladder_limit_readiness.py",
    "tests/test_alpha_research_ladder.py",
    "tests/test_certified_research_gateway.py",
    "tests/test_preexecution_fold_certification.py",
    "tests/test_tier1_trade_triggered_trial_design.py",
    "tests/test_tier1_bracket_evaluation.py",
    "tests/test_tier1_phase8_evaluator.py",
    "tests/test_research_hac_bootstrap_rw.py",
)


def _write_once(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(canonical_bytes(payload) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def main() -> int:
    if (ROOT / TIER0_CERTIFICATE_PATH).exists() or (ROOT / TIER0_DECISION_PATH).exists():
        raise FileExistsError("counted mechanism Tier 0 evidence already exists")
    contract, profile = load_active_ladder(ROOT)
    mechanism_path = ROOT / MECHANISM_PATH
    if sha256_file(mechanism_path) != MECHANISM_SHA256:
        raise IntegrityError("counted mechanism changed")
    mechanism = _read_canonical(mechanism_path, name="counted mechanism")
    predecessor = _read_canonical(ROOT / PREDECESSOR, name="predecessor mechanism")
    rejection = _read_canonical(ROOT / REJECTION, name="V3 rejection")
    validate_successor(mechanism, predecessor=predecessor, rejection=rejection)
    if mechanism.get("mechanism_id") != MECHANISM_ID:
        raise IntegrityError("counted mechanism identity changed")
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-m", "high_risk", *TIER0_NODES],
        cwd=ROOT, check=False,
    )
    if completed.returncode != 0:
        raise IntegrityError("Tier 0 synthetic suite failed; nothing was certified")
    certificate = build_tier0_certificate(
        contract_id=str(contract["contract_id"]), profile_id=str(profile["profile_id"]),
        mechanism_id=MECHANISM_ID, mechanism_sha256=MECHANISM_SHA256,
        test_node_ids=TIER0_NODES, passed_test_count=len(TIER0_NODES),
    )
    validate_tier0_certificate(
        certificate, contract_id=str(contract["contract_id"]),
        mechanism_sha256=MECHANISM_SHA256,
    )
    _write_once(ROOT / TIER0_CERTIFICATE_PATH, certificate)
    certificate_sha = sha256_file(ROOT / TIER0_CERTIFICATE_PATH)
    decision = build_tier0_decision(
        contract_id=str(contract["contract_id"]), mechanism_sha256=MECHANISM_SHA256,
        synthetic_certificate_path=TIER0_CERTIFICATE_PATH.as_posix(),
        synthetic_certificate_sha256=certificate_sha,
    )
    _write_once(ROOT / TIER0_DECISION_PATH, decision)
    validate_stage_decision(
        decision, contract_id=str(contract["contract_id"]),
        mechanism_sha256=MECHANISM_SHA256, expected_stage="tier_0", root=ROOT,
    )
    print(json.dumps({
        "mechanism_id": MECHANISM_ID,
        "tier0_certificate_id": certificate["certificate_id"],
        "tier0_certificate_path": TIER0_CERTIFICATE_PATH.as_posix(),
        "tier0_certificate_sha256": certificate_sha,
        "tier0_decision_id": decision["decision_id"],
        "tier0_decision_path": TIER0_DECISION_PATH.as_posix(),
        "tier0_decision_sha256": sha256_file(ROOT / TIER0_DECISION_PATH),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
