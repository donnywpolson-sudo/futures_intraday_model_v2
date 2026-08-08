from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_bracket_pipeline import (
    Tier1BracketPipelineDeclaration,
    build_tier1_bracket_model_contract,
    build_tier1_bracket_pipeline_declaration,
    persist_tier1_bracket_pipeline_registration,
    prepare_tier1_bracket_pipeline_registration,
    persist_tier1_bracket_model_contract,
    build_tier1_bracket_signal_contract,
    persist_tier1_bracket_signal_contract,
)


ROOT = Path(__file__).parents[1]
HASH = "a" * 64


def _contract() -> dict[str, object]:
    return {"trial_status": "LOCAL_IMPLEMENTATION_ONLY_NOT_REGISTERED"}


def _pairs() -> list[dict[str, object]]:
    return [
        {
            "market": market,
            "year": year,
            "prior_feature_release_id": f"feature-{market}-{year}",
            "prior_outcome_release_id": f"outcome-{market}-{year}",
            "source_parquet_sha256": f"source-{market}-{year}",
        }
        for market in ("ES", "CL", "ZN", "6E")
        for year in range(2018, 2023)
    ]


def test_declaration_locks_scope_and_leaves_every_row_based_output_uncreated() -> None:
    declaration = build_tier1_bracket_pipeline_declaration(
        bracket_contract=_contract(),
        evaluation_config_hash=HASH,
        risk_profile_hash=HASH,
        rulebook_hash=HASH,
        index_release_id=HASH,
        audit_release_id=HASH,
        source_pairs=_pairs(),
    )

    assert declaration.payload["locked_untouched_holdout"] == "2025"
    assert declaration.payload["old_five_minute_feature_outcome_reuse"] == "FORBIDDEN"
    assert all(value.startswith("NOT_CREATED") for value in declaration.payload["pipeline_outputs"].values())


def test_declaration_rejects_incomplete_source_pairs() -> None:
    with pytest.raises(IntegrityError, match="canonical 20"):
        build_tier1_bracket_pipeline_declaration(
            bracket_contract=_contract(),
            evaluation_config_hash=HASH,
            risk_profile_hash=HASH,
            rulebook_hash=HASH,
            index_release_id=HASH,
            audit_release_id=HASH,
            source_pairs=_pairs()[:-1],
        )


def test_registration_is_create_only(tmp_path: Path) -> None:
    declaration = Tier1BracketPipelineDeclaration(trial_id="b" * 64, payload={"schema_version": "test"})

    result = persist_tier1_bracket_pipeline_registration(root=tmp_path, declaration=declaration)

    assert result["trial_id"] == "b" * 64
    with pytest.raises(IntegrityError, match="already exists"):
        persist_tier1_bracket_pipeline_registration(root=tmp_path, declaration=declaration)


def test_model_contract_locks_new_directional_model_before_rows(tmp_path: Path) -> None:
    contract = build_tier1_bracket_model_contract(parent_trial_id="c" * 64)

    result = persist_tier1_bracket_model_contract(root=tmp_path, contract=contract)

    assert result["model_contract_id"] == contract.trial_id
    assert contract.payload["targets"]["long"].startswith("realized_net_r")
    assert contract.payload["model"]["ridge_penalty"] == 1.0


def test_signal_contract_locks_training_only_threshold_and_diagnostic_before_rows(tmp_path: Path) -> None:
    contract = build_tier1_bracket_signal_contract(parent_trial_id="c" * 64, model_contract_id="d" * 64)
    result = persist_tier1_bracket_signal_contract(root=tmp_path, contract=contract)

    assert result["signal_contract_id"] == contract.trial_id
    assert contract.payload["diagnostic_label"]["not_a_training_target"] is True
    assert contract.payload["signal"]["neutral_threshold"]["fit_scope"] == "outer_fold_training_rows_only"
    with pytest.raises(IntegrityError, match="already exists"):
        persist_tier1_bracket_signal_contract(root=tmp_path, contract=contract)


def test_live_preparation_refuses_to_register_the_accepted_trial_twice(
    local_evidence_root: Path,
) -> None:
    with pytest.raises(IntegrityError, match="not locally ready"):
        prepare_tier1_bracket_pipeline_registration(root=local_evidence_root)
