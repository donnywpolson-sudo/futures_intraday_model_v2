from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild import micro_alpha_phase1b2_preparation as prep


ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_prepared_contract_matches_deterministic_builder() -> None:
    persisted = json.loads((ROOT / prep.OUTPUT_PATH).read_text(encoding="utf-8"))
    rebuilt = prep.build_prepare_only_contract(root=ROOT)
    assert persisted == rebuilt
    prep.validate_prepare_only_contract(persisted, root=ROOT)
    assert persisted["state"] == (
        "PREPARED_NOT_EXECUTED_HISTORICAL_ROW_APPROVAL_REQUIRED"
    )
    assert persisted["markets"] == ["MES", "MCL", "MGC", "M6E"]
    assert persisted["authority"] == {
        "dbn_row_read": False,
        "year_2025_or_2026_payload_read": False,
        "phase1b_decode": False,
        "phase2_construction": False,
        "catalog_write": False,
        "catalog_activation": False,
        "registration": False,
        "evaluation": False,
        "publication": False,
        "trading": False,
    }


def test_all_five_decoder_contracts_have_separate_roles() -> None:
    contracts = {schema: prep.decoder_contract(schema) for schema in prep.SCHEMA_DECODER_CONTRACTS}
    assert contracts["definition"]["output_family"] == "definitions.parquet"
    assert contracts["status"]["diagnostic_only"] is True
    assert contracts["statistics"]["feature_eligible"] is False
    assert contracts["ohlcv-1m"]["feature_eligible"] is True
    assert contracts["ohlcv-1s"]["feature_eligible"] is False
    assert contracts["ohlcv-1s"]["output_family"] == "reported_trade_bars.parquet"
    with pytest.raises(IntegrityError):
        prep.decoder_contract("trades")


def test_holdout_and_forward_dispositions_are_not_decode_authority() -> None:
    assert prep.year_decode_disposition(year=2024) == "HISTORICAL_ROW_APPROVAL_REQUIRED"
    assert prep.year_decode_disposition(year=2025) == "SEALED_HOLDOUT_CUSTODY_ONLY"
    assert prep.year_decode_disposition(year=2026) == "FORWARD_PRE_FREEZE_CUSTODY_ONLY"
    with pytest.raises(IntegrityError):
        prep.year_decode_disposition(year=2027)


def _catalog_candidate() -> dict[str, object]:
    return {
        "lane_id": "apex_integer_micro_11",
        "contract_scale": "MICRO_INTEGER_ONLY",
        "state": "CERTIFIED_INACTIVE_NOT_PUBLISHED",
        "source_certification_id": "a" * 64,
        "source_certification_sha256": "b" * 64,
        "phase2_release_id": "c" * 64,
        "phase2_release_sha256": "d" * 64,
        "markets": ["MES", "MCL", "MGC", "M6E"],
        "years": list(range(2018, 2025)),
        "disposition_census_complete": True,
        "actual_identity_and_roll_continuity_certified": True,
        "holdout_2025_materialized": False,
        "forward_2026_materialized": False,
    }


def test_future_catalog_candidate_is_inactive_lane_bound_and_complete() -> None:
    prep.require_row_certified_catalog_candidate(_catalog_candidate())
    broken = _catalog_candidate()
    broken["lane_id"] = "standard_full_contract_41"
    with pytest.raises(UnauthorizedOperation):
        prep.require_row_certified_catalog_candidate(broken)
    broken = _catalog_candidate()
    broken["holdout_2025_materialized"] = True
    with pytest.raises(UnauthorizedOperation):
        prep.require_row_certified_catalog_candidate(broken)


def test_one_second_and_micro_risk_gates_are_fail_closed() -> None:
    contract = prep.build_prepare_only_contract(root=ROOT)
    execution = contract["one_second_execution_semantics"]
    assert execution["evidence"] == "REPORTED_TRADE_BARS_ONLY"
    assert set(execution["cannot_prove"]) == {
        "BBO_AVAILABILITY",
        "QUEUE_PRIORITY",
        "GUARANTEED_MARKET_ORDER_EXECUTION",
        "PRECISE_WITHIN_SECOND_TICK_ORDERING",
    }
    risk = contract["apex_cost_and_risk_gates"]
    assert risk["standard_full_contract_policy_reuse_for_micro_forbidden"] is True
    assert set(risk["official_micro_commission_verification"]) == {
        "MES", "MCL", "MGC", "M6E",
    }
    assert set(risk["official_micro_commission_verification"].values()) == {
        "UNRESOLVED_FAIL_CLOSED_BEFORE_MECHANISM_FREEZE"
    }


def test_preparation_source_has_no_dbn_or_provider_execution_surface() -> None:
    source = inspect.getsource(prep)
    forbidden = (
        "import databento",
        "from databento",
        "DBNStore",
        "read_dbn",
        "to_df(",
        "get_range(",
        "timeseries.get_range",
        "urlopen(",
        "requests.get",
    )
    assert not any(token.lower() in source.lower() for token in forbidden)
    assert "open(\"xb\")" not in source
