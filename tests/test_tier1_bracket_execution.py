from pathlib import Path

import pytest

from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.tier1_bracket_execution import (
    execute_tier1_bracket_materialization,
    prepare_tier1_bracket_materialization,
)


ROOT = Path(__file__).parents[1]


def test_prepare_binds_exact_registered_scope_and_returns_plain_confirmation_summary() -> None:
    preparation = prepare_tier1_bracket_materialization(root=ROOT)

    assert preparation.source_pairs == tuple((market, year) for market in ("ES", "CL", "ZN", "6E") for year in range(2018, 2023))
    assert preparation.confirmation_required["scope"].endswith("2025 excluded")
    assert len(preparation.signal_contract_id) == 64


def test_public_execution_fails_closed_without_codex_confirmation() -> None:
    preparation = prepare_tier1_bracket_materialization(root=ROOT)
    with pytest.raises(UnauthorizedOperation, match="Codex confirmation required"):
        execute_tier1_bracket_materialization(preparation=preparation)
