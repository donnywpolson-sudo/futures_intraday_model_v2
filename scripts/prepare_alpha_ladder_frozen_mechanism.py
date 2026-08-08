"""Prepare the frozen Alpha mechanism and Tier 0 certificate after activation."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from hashlib import sha256
from pathlib import Path

from futures_rebuild.alpha_ladder_frozen_mechanism import (
    build_frozen_mechanism,
    build_tier0_certificate,
    build_tier0_decision,
)
from futures_rebuild.alpha_research_ladder import ALL_APPROVED, load_active_ladder
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = Path("configs/tier1_trade_triggered_trial_protocol.json")
SOURCE_ID = "101d51491b0303ce03fa7f1c2ba19d9b917f16cd86c8e54f895d500936ef3af2"
SOURCE_SHA256 = "9dcbb4f183124fc13321cd1fb84d75f81dcac57fa58d00335b317b204035b090"
TIER0_NODES = (
    "tests/test_alpha_ladder_frozen_mechanism.py",
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
    contract, profile = load_active_ladder(ROOT)
    source_path = ROOT / SOURCE_PATH
    if sha256_file(source_path) != SOURCE_SHA256:
        raise IntegrityError("reusable six-market source protocol changed")
    source = json.loads(source_path.read_text(encoding="utf-8"))
    if source.get("protocol_id") != SOURCE_ID:
        raise IntegrityError("reusable six-market source protocol identity changed")

    mechanism = build_frozen_mechanism(
        contract_id=str(contract["contract_id"]),
        profile_id=str(profile["profile_id"]),
        source_protocol_id=SOURCE_ID,
        source_protocol_sha256=SOURCE_SHA256,
        all_markets=ALL_APPROVED,
    )
    mechanism_id = str(mechanism["mechanism_id"])
    root = ROOT / "state" / "unpublished_evidence" / "alpha_ladder_frozen_mechanism" / mechanism_id
    mechanism_path = root / "mechanism.json"
    mechanism_raw = canonical_bytes(mechanism) + b"\n"
    mechanism_sha = sha256(mechanism_raw).hexdigest()

    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-m", "high_risk", *TIER0_NODES],
        cwd=ROOT, check=False,
    )
    if completed.returncode != 0:
        raise IntegrityError("Tier 0 synthetic suite failed; mechanism remains uncertified")
    certificate = build_tier0_certificate(
        contract_id=str(contract["contract_id"]),
        profile_id=str(profile["profile_id"]),
        mechanism_id=mechanism_id,
        mechanism_sha256=mechanism_sha,
        test_node_ids=TIER0_NODES,
        passed_test_count=len(TIER0_NODES),
    )
    certificate_path = root / "tier0_certificate.json"
    certificate_relative = certificate_path.relative_to(ROOT).as_posix()
    certificate_raw = canonical_bytes(certificate) + b"\n"
    certificate_sha = sha256(certificate_raw).hexdigest()
    decision = build_tier0_decision(
        contract_id=str(contract["contract_id"]), mechanism_sha256=mechanism_sha,
        synthetic_certificate_path=certificate_relative,
        synthetic_certificate_sha256=certificate_sha,
    )
    decision_path = root / "tier0_decision.json"
    _write_once(mechanism_path, mechanism)
    _write_once(certificate_path, certificate)
    _write_once(decision_path, decision)
    print(json.dumps({
        "mechanism_id": mechanism_id,
        "mechanism_path": mechanism_path.relative_to(ROOT).as_posix(),
        "mechanism_sha256": mechanism_sha,
        "tier0_certificate_id": certificate["certificate_id"],
        "tier0_certificate_path": certificate_relative,
        "tier0_certificate_sha256": certificate_sha,
        "tier0_decision_id": decision["decision_id"],
        "tier0_decision_path": decision_path.relative_to(ROOT).as_posix(),
        "tier0_decision_sha256": sha256_file(decision_path),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
