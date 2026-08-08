"""Complete the Tier 0 decision for an already sealed mechanism certificate."""

from __future__ import annotations

import json
import os
from pathlib import Path

from futures_rebuild.alpha_ladder_frozen_mechanism import (
    build_tier0_decision,
    validate_frozen_mechanism,
    validate_tier0_certificate,
)
from futures_rebuild.alpha_research_ladder import load_active_ladder, validate_stage_decision
from futures_rebuild.canonical import canonical_bytes, sha256_file
from futures_rebuild.cash_open_source_compatibility_census import _read_canonical


ROOT = Path(__file__).resolve().parents[1]
MECHANISM_ID = "186d8a103a581ae8c27fc531e0a556070991c9d2f87bbe5d62c1478867b5de3f"
BASE = Path("state/unpublished_evidence/alpha_ladder_frozen_mechanism") / MECHANISM_ID
MECHANISM_SHA = "1b0fa1d2beb1b463ec5c37f1341cca348a7ce1fee6d9dbae6074603b5ec37798"
CERTIFICATE_SHA = "cd2faa366c7fbf200bba9f1a7a809ee341a9faf3991caf9c446809a0e80af66f"


def main() -> int:
    contract, _profile = load_active_ladder(ROOT)
    mechanism_path = ROOT / BASE / "mechanism.json"
    certificate_path = ROOT / BASE / "tier0_certificate.json"
    if sha256_file(mechanism_path) != MECHANISM_SHA:
        raise RuntimeError("frozen mechanism changed")
    if sha256_file(certificate_path) != CERTIFICATE_SHA:
        raise RuntimeError("Tier 0 certificate changed")
    mechanism = _read_canonical(mechanism_path, name="frozen mechanism")
    certificate = _read_canonical(certificate_path, name="Tier 0 certificate")
    validate_frozen_mechanism(mechanism)
    validate_tier0_certificate(
        certificate, contract_id=str(contract["contract_id"]),
        mechanism_sha256=MECHANISM_SHA,
    )
    decision = build_tier0_decision(
        contract_id=str(contract["contract_id"]), mechanism_sha256=MECHANISM_SHA,
        synthetic_certificate_path=(BASE / "tier0_certificate.json").as_posix(),
        synthetic_certificate_sha256=CERTIFICATE_SHA,
    )
    path = ROOT / BASE / "tier0_decision.json"
    with path.open("xb") as stream:
        stream.write(canonical_bytes(decision) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    validate_stage_decision(
        decision, contract_id=str(contract["contract_id"]),
        mechanism_sha256=MECHANISM_SHA, expected_stage="tier_0", root=ROOT,
    )
    print(json.dumps({
        "decision_id": decision["decision_id"],
        "path": path.relative_to(ROOT).as_posix(),
        "sha256": sha256_file(path),
        "validation": "PASS",
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
