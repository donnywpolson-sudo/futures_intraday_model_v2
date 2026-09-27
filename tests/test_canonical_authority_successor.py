from __future__ import annotations

import json
from pathlib import Path

import pytest

from futures_rebuild.bias_robustness_successor import CONTRACT_SCHEMA, validate_contract
from futures_rebuild.canonical import (
    canonical_authority_file_bytes,
    read_canonical_authority,
    sha256_bytes,
    sha256_json,
    write_new_canonical_authority,
)
from futures_rebuild.errors import IntegrityError


ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.high_risk
BUNDLE = ROOT / "state/bias_robustness_canonical_successor_registry/c5d6f0ca4bd6c046ce9151927d77957be232a75538f533365767871eff482758/bundle.json"
FAILED = ROOT / "state/causal_search_access_contract_registry/5bff4a0914247a49db2f552caa199232490bab5d277051d468db01c539d11741/contract.json"


def test_canonical_authority_bytes_and_create_only_writer(tmp_path: Path) -> None:
    payload = {"schema_version": "test/1.0.0", "value": "é"}
    payload["record_id"] = sha256_json(payload)
    expected = canonical_authority_file_bytes(payload)
    assert expected.endswith(b"\n") and not expected.endswith(b"\n\n") and b"\r" not in expected
    path = tmp_path / payload["record_id"] / "record.json"
    assert write_new_canonical_authority(path, payload) == expected
    assert write_new_canonical_authority(path, payload) == expected
    assert path.read_bytes() == expected
    assert read_canonical_authority(path, expected_sha256=sha256_bytes(expected), identity_field="record_id", expected_identity=payload["record_id"], expected_schema="test/1.0.0") == payload
    changed = dict(payload, value="changed")
    changed["record_id"] = sha256_json({key: value for key, value in changed.items() if key != "record_id"})
    with pytest.raises(IntegrityError, match="different bytes"):
        write_new_canonical_authority(path, changed)


@pytest.mark.parametrize("raw", [
    b'{"a":1}',
    b'{"a":1}\n\n',
    b'{"a":1}\n\n\n',
    b'{"a":1}\r\n',
    b'\xef\xbb\xbf{"a":1}\n',
    b'{"a":1} \n',
    b'{"a":1}\n ',
    b'{ "a": 1 }\n',
    b'{"b":2,"a":1}\n',
    b'{"a":1,"a":1}\n',
])
def test_noncanonical_authority_variants_are_rejected(tmp_path: Path, raw: bytes) -> None:
    path = tmp_path / "authority.json"
    path.write_bytes(raw)
    with pytest.raises(IntegrityError):
        read_canonical_authority(path)


def test_identity_hash_and_schema_bindings_are_rejected(tmp_path: Path) -> None:
    payload = {"schema_version": "test/1.0.0", "value": 1}
    payload["record_id"] = sha256_json(payload)
    path = tmp_path / "authority.json"
    path.write_bytes(canonical_authority_file_bytes(payload))
    with pytest.raises(IntegrityError, match="SHA-256"):
        read_canonical_authority(path, expected_sha256="0" * 64)
    with pytest.raises(IntegrityError, match="identity"):
        read_canonical_authority(path, identity_field="record_id", expected_identity="0" * 64)
    with pytest.raises(IntegrityError, match="schema"):
        read_canonical_authority(path, expected_schema="wrong/1.0.0")


def test_corrected_successor_is_canonical_and_failed_predecessor_stays_ineligible() -> None:
    bundle = read_canonical_authority(BUNDLE, identity_field="bundle_id")
    stage_item = bundle["artifacts"]["stage_2_5"]
    stage_path = ROOT / stage_item["path"]
    stage = read_canonical_authority(stage_path, expected_sha256=stage_item["sha256"], identity_field="contract_id", expected_identity=stage_item["id"], expected_schema=CONTRACT_SCHEMA)
    assert validate_contract(stage, root=ROOT)["contract_id"] == stage_item["id"]
    assert FAILED.read_bytes().endswith(b"\n\n")
    with pytest.raises(IntegrityError, match="not canonical"):
        read_canonical_authority(FAILED, identity_field="contract_id")
