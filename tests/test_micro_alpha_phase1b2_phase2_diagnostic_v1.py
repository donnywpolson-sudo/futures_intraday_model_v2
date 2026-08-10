from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from futures_rebuild import micro_alpha_phase1b2_phase2_diagnostic as diagnostic
from futures_rebuild.errors import UnauthorizedOperation
from futures_rebuild.research_gateway_policy import (
    PREPARATORY_REAL_HISTORY_OPERATIONS,
    require_current_real_history_operation,
)
from scripts import prepare_apex_micro_phase1b2_phase2_diagnostic_v1 as prepare


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_live_diagnostic_preview_is_one_exact_price_free_source() -> None:
    plan = diagnostic.build_plan(
        root=ROOT, implementation_head=diagnostic._git_head(ROOT)
    )
    assert plan["state"] == (
        "PREPARED_REQUIRES_SEPARATE_DERIVED_ROW_DIAGNOSTIC_APPROVAL"
    )
    assert plan["source"]["market"] == "M6E"
    assert plan["source"]["schema"] == "ohlcv-1m"
    assert plan["source"]["year"] == 2018
    assert plan["source_footer"]["row_batches_opened"] == 0
    assert plan["source_footer"]["footer_only_plan_read"] is True
    assert plan["source_footer"]["schema_id"] == (
        "APEX_MICRO_PHASE1B_REPORTED_BAR_V1"
    )
    assert plan["limits"] == {
        "maximum_source_count": 1,
        "maximum_workers": 1,
        "maximum_runtime_seconds": 300,
        "maximum_output_bytes": 1024**3,
        "required_free_disk_bytes": 2 * 1024**3,
        "maximum_attempts": 1,
        "maximum_retries": 0,
        "provider_calls": 0,
        "external_cost_usd": "0",
    }
    assert not (ROOT / plan["staging_root"]).exists()
    assert not (ROOT / plan["evidence_root"]).exists()


def test_diagnostic_operation_is_exactly_allowlisted() -> None:
    assert diagnostic.OPERATION in PREPARATORY_REAL_HISTORY_OPERATIONS
    require_current_real_history_operation(diagnostic.OPERATION, {})
    with pytest.raises(UnauthorizedOperation, match="certified gateway"):
        require_current_real_history_operation(f"{diagnostic.OPERATION}_ALIAS", {})


def test_prepare_cli_has_no_execution_surface_and_requires_commit() -> None:
    source = inspect.getsource(prepare)
    assert "execute_once" not in source
    plan_existed = (ROOT / diagnostic.PLAN_PATH).exists()
    assert plan_existed is False
    with pytest.raises(SystemExit, match="committed"):
        prepare._require_committed_implementation()
    assert not (ROOT / diagnostic.PLAN_PATH).exists()


def test_execution_verifies_authorization_before_row_materialization() -> None:
    source = inspect.getsource(diagnostic.execute_once)
    assert source.index("authorization.verify(") < source.index(
        "materialize_causal_1m_inactive("
    )
    assert source.index("authorization.consume(") < source.index(
        "materialize_causal_1m_inactive("
    )


def test_diagnostic_has_no_dbn_provider_credential_or_publication_surface() -> None:
    source = inspect.getsource(diagnostic)
    assert "DBNStore" not in source
    assert "databento" not in source
    assert "api.env" not in source
    assert "requests." not in source
    assert "data/active" not in source
    assert "PhasePublisher" not in source
    assert "TrialRegistry" not in source
    assert "broker" not in source.lower()
