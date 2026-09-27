from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import canonical_authority_file_bytes, sha256_json
from futures_rebuild.repository_surface import validate_active_alpha_authority_candidate
from scripts.prepare_canonical_bias_activation_packet import build_plan, verify_prospective


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.high_risk


def test_plan_binds_exact_predecessor_and_successor_pointer_bytes() -> None:
    first, first_path = build_plan()
    second, second_path = build_plan()
    assert first == second and first_path == second_path
    assert first["write_order"][-1] == "configs/active_alpha_research_ladder.json"
    assert first["rollback_order"] == list(reversed(first["write_order"]))
    for relative, binding in first["pointer_successors"].items():
        predecessor = base64.b64decode(binding["predecessor_bytes_base64"], validate=True)
        successor = base64.b64decode(binding["successor_bytes_base64"], validate=True)
        assert predecessor == (ROOT / relative).read_bytes()
        assert hashlib.sha256(predecessor).hexdigest() == binding["predecessor_sha256"]
        assert hashlib.sha256(successor).hexdigest() == binding["successor_sha256"]
        payload = json.loads(successor)
        assert successor == canonical_authority_file_bytes(payload)
        assert payload["pointer_id"] == sha256_json({key: value for key, value in payload.items() if key != "pointer_id"})


def test_prospective_state_validates_without_live_pointer_mutation() -> None:
    before = {path: (ROOT / path).read_bytes() for path in (
        "configs/active_alpha_research_ladder.json",
        "configs/active_alpha_historical_research_design.json",
        "configs/active_alpha_ladder_execution_design.json",
    )}
    plan, _path = build_plan()
    verify_prospective(plan)
    pointer = json.loads(base64.b64decode(
        plan["pointer_successors"]["configs/active_alpha_research_ladder.json"]["successor_bytes_base64"],
        validate=True,
    ))
    required = validate_active_alpha_authority_candidate(ROOT, pointer)["required_paths"]
    assert "configs/bounded_alpha_discovery_contract_v1.json" in required
    assert not any(path.startswith("data/") for path in required)
    assert before == {path: (ROOT / path).read_bytes() for path in before}


def test_pointer_mutation_cannot_retain_bound_hash() -> None:
    plan, _path = build_plan()
    item = next(iter(plan["pointer_successors"].values()))
    raw = bytearray(base64.b64decode(item["successor_bytes_base64"], validate=True))
    raw[-2] ^= 1
    assert hashlib.sha256(raw).hexdigest() != item["successor_sha256"]
