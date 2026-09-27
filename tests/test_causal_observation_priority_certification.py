from __future__ import annotations

from pathlib import Path

import pytest

from futures_rebuild.canonical import sha256_json
from futures_rebuild.causal_observation_priority_certification import (
    extend_priority_certification_plan,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


ROOT = Path(__file__).resolve().parents[1]


def test_priority_certification_extension_binds_predecessor_and_implementation() -> None:
    base_plan = {"target_market": "ZN", "plan_id": "base-plan"}
    build_plan = {
        "target_market": "ZN",
        "priority_sequence_gate": {
            "transition_id": "transition",
            "predecessor_market": "CL",
            "predecessor_certificate_id": "certificate",
            "predecessor_certificate_sha256": "certificate-sha",
        },
    }

    plan = extend_priority_certification_plan(
        repository_root=ROOT,
        base_plan=base_plan,
        build_plan=build_plan,
    )

    gate = plan["priority_certification_gate"]
    assert gate["base_certification_plan_id"] == "base-plan"
    assert gate["predecessor_market"] == "CL"
    assert gate["predecessor_certificate_id"] == "certificate"
    assert gate["implementation_path"] == (
        "src/futures_rebuild/causal_observation_priority_certification.py"
    )
    assert plan["plan_id"] == sha256_json(
        {key: value for key, value in plan.items() if key != "plan_id"}
    )


def test_priority_certification_extension_rejects_dual_gate() -> None:
    with pytest.raises(IntegrityError, match="cannot claim two sequence gates"):
        extend_priority_certification_plan(
            repository_root=ROOT,
            base_plan={"target_market": "ZN", "plan_id": "base-plan"},
            build_plan={
                "target_market": "ZN",
                "certified_sequence_gate": {},
                "priority_sequence_gate": {
                    "transition_id": "transition",
                    "predecessor_market": "CL",
                    "predecessor_certificate_id": "certificate",
                    "predecessor_certificate_sha256": "certificate-sha",
                },
            },
        )


def test_priority_certification_extension_rejects_unscoped_market() -> None:
    with pytest.raises(UnauthorizedOperation, match="absent or out of scope"):
        extend_priority_certification_plan(
            repository_root=ROOT,
            base_plan={"target_market": "GC", "plan_id": "base-plan"},
            build_plan={"target_market": "GC"},
        )
