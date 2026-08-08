from __future__ import annotations

import pytest

from futures_rebuild.alpha_research_architecture import (
    MICRO_LANE,
    STANDARD_LANE,
    build_micro_contract,
    build_micro_profile,
    build_prepared_micro_pointer,
    validate_lane_binding,
    validate_micro_contract,
)
from futures_rebuild.errors import ContractError, UnauthorizedOperation
from futures_rebuild.micro_alpha_pipeline import (
    SCHEMAS,
    TIER_1_MARKETS,
    TIER_2_ADDITIONS,
    TIER_2_MARKETS,
    TIER_3_MARKETS,
    build_product_reference_requirements,
    build_phase2_contract,
    classify_product_session,
    phase1a_paths,
    phase1b_destination,
    phase1b_role,
    require_decode_authority,
    require_lane_catalog_entry,
    validate_economics_reference,
)


pytestmark = [pytest.mark.current, pytest.mark.high_risk]


def test_micro_ladder_is_exact_nested_and_inactive() -> None:
    contract = build_micro_contract()
    validate_micro_contract(contract)
    profile = build_micro_profile(contract_id=str(contract["contract_id"]))
    assert profile["tier_0"] == ["MES"]
    assert profile["tier_1"] == ["MES", "MCL", "MGC", "M6E"]
    assert profile["tier_2"] == ["MES", "MCL", "MGC", "M6E", "MNQ", "MYM", "M2K", "M6A", "SIL"]
    assert profile["tier_3"][-2:] == ["MBT", "MET"]
    assert tuple(profile["tier_2"]) == TIER_2_MARKETS
    assert TIER_2_ADDITIONS == ("MNQ", "MYM", "M2K", "M6A", "SIL")
    assert TIER_3_MARKETS == (*TIER_2_MARKETS, "MBT", "MET")
    assert contract["tiers"]["tier_1"]["represented_families"] == [
        "EQUITY", "ENERGY", "METALS", "FX",
    ]
    assert contract["tiers"]["tier_2"]["report_cohorts_separately"] is True
    assert contract["tiers"]["tier_3"]["traditional_must_pass_independently"] is True
    assert contract["tiers"]["tier_3"]["satellites_cannot_rescue_traditional"] is True
    assert contract["state"] == "PREPARED_NOT_PUBLISHED_NOT_ACTIVE"
    assert contract["sources"]["catalog_must_not_exist_until_phase2_certification"] is True
    pointer = build_prepared_micro_pointer(
        contract_path="state/unpublished_evidence/micro/contract.json",
        contract_sha256="a" * 64,
        contract_id=str(contract["contract_id"]),
        profile_path="state/unpublished_evidence/micro/profile.json",
        profile_sha256="b" * 64,
        profile_id=str(profile["profile_id"]),
    )
    assert pointer["state"] == "PREPARED_NOT_ACTIVE"
    assert pointer["future_active_path"] == "configs/active_micro_alpha_research_ladder.json"


@pytest.mark.parametrize("market", TIER_1_MARKETS)
@pytest.mark.parametrize("schema", SCHEMAS)
def test_phase1a_destinations_match_standard_folder_shape(market: str, schema: str) -> None:
    paths = phase1a_paths(
        market=market, schema=schema, year=2018, interval="2018-01-01_2019-01-01",
    )
    schema_folder = schema.replace("-", "_")
    assert paths["dbn"] == (
        f"data/dbn/{schema_folder}/{market}/2018/2018-01-01_2019-01-01.dbn.zst"
    )
    assert paths["sidecar"] == paths["dbn"] + ".manifest.json"


def test_phase1b_routes_features_execution_and_diagnostics_separately() -> None:
    release = "a" * 64
    assert phase1b_role("ohlcv-1m") == "CAUSAL_FEATURE_FOUNDATION_INPUT"
    assert phase1b_role("ohlcv-1s") == "CAUSAL_EXECUTION_EVIDENCE_INPUT"
    assert phase1b_destination(
        market="MES", schema="ohlcv-1m", year=2024, interval="x", release_id=release,
    ).startswith("data/raw/MES/2024/")
    assert phase1b_destination(
        market="MES", schema="ohlcv-1s", year=2024, interval="x", release_id=release,
    ).startswith("data/outcome_sources/MES/2024/")
    assert phase1b_destination(
        market="MES", schema="statistics", year=2024, interval="x", release_id=release,
    ).startswith("data/market_state/statistics/MES/2024/")


def test_forbidden_l0_schemas_and_holdout_forward_rows_fail_closed() -> None:
    with pytest.raises(ContractError):
        phase1a_paths(market="MES", schema="trades", year=2024, interval="x")
    with pytest.raises(UnauthorizedOperation, match="sealed holdout"):
        require_decode_authority(year=2025, mechanism_frozen_at="2024-12-01")
    with pytest.raises(UnauthorizedOperation, match="prior frozen"):
        require_decode_authority(year=2026, mechanism_frozen_at=None)
    with pytest.raises(UnauthorizedOperation, match="pre-freeze"):
        require_decode_authority(
            year=2026, mechanism_frozen_at="2026-01-03T00:00:00Z",
            source_interval_start="2026-01-02T00:00:00Z",
        )
    require_decode_authority(
        year=2026, mechanism_frozen_at="2026-01-03T00:00:00Z",
        source_interval_start="2026-01-03T00:00:00Z",
    )


def test_product_effective_dates_are_explicit() -> None:
    assert classify_product_session(
        session_id="2018-01-02", product_effective_date=None,
    ) == "PRODUCT_EFFECTIVE_DATE_UNVERIFIED"
    assert classify_product_session(
        session_id="2018-01-02", product_effective_date="2019-05-06",
    ) == "PRODUCT_NOT_YET_EFFECTIVE"


def test_phase2_separates_feature_and_execution_sources() -> None:
    contract = build_phase2_contract(
        market="MES",
        year=2024,
        product_effective_date="2019-05-06",
        source_bindings={schema: "a" * 64 for schema in SCHEMAS},
    )
    assert contract["feature_foundation"]["schema"] == "ohlcv-1m"
    assert contract["execution_foundation"]["schema"] == "ohlcv-1s"
    assert contract["execution_foundation"]["feature_eligibility"] is False
    assert contract["execution_foundation"]["evidence_semantics"] == "REPORTED_TRADE_BARS_ONLY"
    assert "BBO_AVAILABILITY" in contract["execution_foundation"]["cannot_prove"]
    assert contract["execution_foundation"]["explicit_states"] == ["UNFILLED", "NO_TRIGGER"]
    assert contract["coverage_policy"]["missing_or_sparse_checkpoints_never_silently_removed"] is True
    assert contract["diagnostics"]["statistics"] == "DIAGNOSTIC_ONLY_NEVER_FEATURE_ELIGIBLE"


def test_catalog_and_registration_cannot_cross_lanes() -> None:
    micro = build_micro_contract()
    require_lane_catalog_entry({
        "lane_id": MICRO_LANE, "market": "MES", "contract_scale": "MICRO_INTEGER_ONLY",
    })
    with pytest.raises(UnauthorizedOperation):
        require_lane_catalog_entry({
            "lane_id": STANDARD_LANE, "market": "ES", "contract_scale": "STANDARD_FULL_CONTRACT",
        })
    with pytest.raises(UnauthorizedOperation):
        validate_lane_binding(
            {"lane_id": MICRO_LANE, "contract_id": micro["contract_id"],
             "catalog_path": "data/active/catalog.json"},
            expected_lane=MICRO_LANE,
            expected_contract_id=str(micro["contract_id"]),
            expected_catalog_path="data/active/catalogs/apex_micro.json",
        )


def test_product_references_are_explicit_and_zn_proxy_is_forbidden() -> None:
    requirements = build_product_reference_requirements()
    assert set(requirements["markets"]) == {"MES", "MCL", "MGC", "M6E"}
    assert "ZN" not in requirements["markets"]
    assert requirements["selection_policy"]["invented_zn_micro_proxy_forbidden"] is True
    for market, reference in requirements["markets"].items():
        assert reference["product_effective_date"] == "PROVIDER_METADATA_PREFLIGHT_REQUIRED"
        assert reference["actual_instrument_identity"] == "DEFINITION_INSTRUMENT_ID_REQUIRED_PER_INTERVAL"
        assert reference["integer_contract_size"] == 1
        validate_economics_reference(market, reference)
    broken = dict(requirements["markets"]["MCL"])
    broken["tick_value_usd"] = "10"
    with pytest.raises(Exception, match="economics"):
        validate_economics_reference("MCL", broken)
