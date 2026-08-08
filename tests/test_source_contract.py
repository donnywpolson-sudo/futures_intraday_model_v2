from pathlib import Path
import json

import pytest

from futures_rebuild.errors import ContractError
from futures_rebuild.source_contract import legacy_roots_from_contract


def test_retired_external_repository_has_no_runtime_root() -> None:
    assert (
        legacy_roots_from_contract(
            {
                "legacy_repository": None,
                "external_repository_access": "FORBIDDEN",
            }
        )
        == ()
    )


def test_migration_era_contract_preserves_read_only_boundary() -> None:
    assert legacy_roots_from_contract(
        {"legacy_repository": "C:/migration-evidence"}
    ) == (Path("C:/migration-evidence"),)


def test_null_legacy_root_requires_explicit_forbidden_state() -> None:
    with pytest.raises(ContractError, match="FORBIDDEN"):
        legacy_roots_from_contract({"legacy_repository": None})


def test_cme_contract_economics_is_a_declared_legacy_exception_source_family() -> None:
    root = Path(__file__).parents[1]
    payload = json.loads((root / "configs" / "source_contract.json").read_text(encoding="utf-8"))
    source = next(item for item in payload["source_families"] if item["id"] == "cme_contract_economics")
    assert source == {
        "authority_url": "https://www.cmegroup.com/",
        "id": "cme_contract_economics",
        "network_calls_authorized": False,
        "path": "data/reference/economics",
        "role": "legacy_immutable_official_contract_economics_exception_evidence",
    }


def test_phase8_economics_gate_requires_the_aggregate_index() -> None:
    root = Path(__file__).parents[1]
    payload = json.loads((root / "configs" / "source_contract.json").read_text(encoding="utf-8"))
    gate = payload["pnl_economics_gate"]
    assert gate["verified_actual_contract_economics_release_required"] is True
    assert gate["verified_phase8_actual_contract_economics_index_required"] is True
