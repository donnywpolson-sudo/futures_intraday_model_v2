from pathlib import Path

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
