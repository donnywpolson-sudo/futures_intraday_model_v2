from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
import yaml

import futures_rebuild.certified_research_gateway as gateway_module
from futures_rebuild.alpha_research_ladder import (
    BIAS_GATE_CONTRACT_SCHEMA,
    BIAS_GATE_PROFILE_SCHEMA,
    validate_contract as validate_ladder_contract,
    validate_profile,
    validate_stage_decision,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.causal_search_access_contract import (
    EVIDENCE_CLASSES,
    REGISTRATION_BINDING,
    governance_ledgers,
    registration_contract_scope,
    validate_contract,
)
from futures_rebuild.certified_research_gateway import CertifiedResearchGateway
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.pipeline import main
from futures_rebuild.bias_robustness_successor import (
    CHALLENGE,
    CHALLENGE_CLAIM_SCHEMA,
    CONTRACT_SCHEMA as SUCCESSOR_SCHEMA,
    FAILED_CONTRACT_SCHEMA,
    DECLARATION_SCHEMA,
    FORWARD_PROTOCOL_SCHEMA,
    PREDICTIVE_CLAIM,
    REFIT,
    REFIT_CONTRACT_SCHEMA,
    REFIT_OUTPUT_SCHEMA,
    ROW_REQUEST_SCHEMA,
    claim_challenge_exposure,
    claim_refit_output,
    governance_ledgers as successor_ledgers,
    require_claim_label,
    require_forward_protocol,
    validate_contract as validate_successor_contract,
    validate_declaration,
    validate_forward_protocol,
    validate_refit_contract,
    validate_refit_output,
    validate_row_request,
)


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / (
    "state/causal_search_access_contract_registry/"
    "1ddbbc62f08bf4905a4cfb0efea5f8f39bbbbdf9003869eb87e780b59bc58d5e/contract.json"
)
LADDER_PATH = ROOT / "state/alpha_ladder_registry/69ba41a7fc2fae72bb771d043c7f78ceeb9e36165808c8988983a7ec0093fa16"


def _contract() -> dict[str, object]:
    return json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))


def _decision(stage: str, **extra: object) -> dict[str, object]:
    core = {
        "schema_version": "alpha_ladder_stage_decision/1.0.0",
        "contract_id": "1" * 64,
        "mechanism_sha256": "2" * 64,
        "stage": stage,
        "decision": "PASS",
        **extra,
    }
    return {**core, "decision_id": sha256_json(core)}


def test_inactive_stage_2_5_contract_and_v7_ladder_are_exact() -> None:
    contract = validate_contract(_contract(), root=ROOT)
    assert contract["state"] == "PREPARED_NOT_ACTIVE"
    assert contract["evidence_classes"] == list(EVIDENCE_CLASSES)
    assert contract["recent_regime_challenge"]["maximum_attempts"] == 1
    assert contract["deterministic_refit"]["performance_evaluation_allowed"] is False

    ladder = json.loads((LADDER_PATH / "universe_contract.json").read_text())
    profile = yaml.safe_load((LADDER_PATH / "alpha_tiered.yaml").read_text())
    assert validate_ladder_contract(ladder)["schema_version"] == BIAS_GATE_CONTRACT_SCHEMA
    assert validate_profile(profile, root=ROOT)["schema_version"] == BIAS_GATE_PROFILE_SCHEMA
    assert ladder["stages"]["final_historical_evaluation"][
        "predecessor_requirement"
    ] == "DETERMINISTIC_REFIT_COMPLETE_NO_EVALUATION"


def test_contract_identity_scope_and_result_responsive_macro_evidence_fail_closed(
    tmp_path: Path,
) -> None:
    contract = _contract()
    changed = copy.deepcopy(contract)
    changed["scope"]["predicted_effect_markets"].append("ZN")
    with pytest.raises(IntegrityError, match="identity"):
        validate_contract(changed, root=ROOT)

    changed = copy.deepcopy(contract)
    core = {key: value for key, value in changed.items() if key != "contract_id"}
    changed["zn_6e_selection"]["forbidden_evidence_fields"].remove("pnl")
    core = {key: value for key, value in changed.items() if key != "contract_id"}
    changed["contract_id"] = sha256_json(core)
    with pytest.raises(IntegrityError, match="result-responsive"):
        validate_contract(changed, root=ROOT)

    registration = {
        REGISTRATION_BINDING: {
            "contract_id": contract["contract_id"],
            "path": CONTRACT_PATH.relative_to(ROOT).as_posix(),
            "sha256": "0" * 64,
        }
    }
    with pytest.raises(IntegrityError, match="binding changed"):
        registration_contract_scope(ROOT, registration, required=True)
    with pytest.raises(IntegrityError, match="must be an object"):
        registration_contract_scope(tmp_path, {}, required=True)


def test_schema_v7_gateway_is_retired_before_stage_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Boundary:
        @staticmethod
        def assert_active_root(_root: Path) -> None:
            return None

    monkeypatch.setattr(
        gateway_module,
        "load_registration_ready_certificate",
        lambda **_kwargs: ({}, "", ""),
    )
    monkeypatch.setattr(
        gateway_module,
        "load_active_ladder",
        lambda _root: ({"schema_version": BIAS_GATE_CONTRACT_SCHEMA}, {}),
    )
    gateway = CertifiedResearchGateway(tmp_path, Boundary())  # type: ignore[arg-type]
    with pytest.raises(UnauthorizedOperation, match="active Alpha research ladder"):
        gateway.register_trial(
            registration_path=tmp_path / "trial.json",
            registration={},
            readiness_evidence_path=tmp_path / "readiness.json",
        )


def test_challenge_and_refit_decisions_are_one_shot_and_non_evaluative() -> None:
    challenge = _decision(
        "challenge",
        attempt_count=1,
        same_frozen_candidate=True,
        tuning_design_change_or_retry=False,
    )
    validate_stage_decision(
        challenge,
        contract_id="1" * 64,
        mechanism_sha256="2" * 64,
        expected_stage="challenge",
    )
    changed = copy.deepcopy(challenge)
    changed["attempt_count"] = 2
    changed["decision_id"] = sha256_json(
        {key: value for key, value in changed.items() if key != "decision_id"}
    )
    with pytest.raises(UnauthorizedOperation, match="one-shot"):
        validate_stage_decision(
            changed,
            contract_id="1" * 64,
            mechanism_sha256="2" * 64,
            expected_stage="challenge",
        )

    refit = _decision(
        "refit",
        fit_parameters_only=True,
        algorithm_changed=False,
        performance_evaluation=False,
        status="REFIT_COMPLETE_NO_EVALUATION",
    )
    validate_stage_decision(
        refit,
        contract_id="1" * 64,
        mechanism_sha256="2" * 64,
        expected_stage="refit",
    )
    changed = copy.deepcopy(refit)
    changed["performance_evaluation"] = True
    changed["decision_id"] = sha256_json(
        {key: value for key, value in changed.items() if key != "decision_id"}
    )
    with pytest.raises(UnauthorizedOperation, match="performed evaluation"):
        validate_stage_decision(
            changed,
            contract_id="1" * 64,
            mechanism_sha256="2" * 64,
            expected_stage="refit",
        )


def test_derived_ledgers_are_deterministic_and_non_authorizing() -> None:
    first = governance_ledgers(ROOT)
    second = governance_ledgers(ROOT)
    assert canonical_bytes(first) == canonical_bytes(second)
    assert first["authority"] == "DERIVED_READ_ONLY_NOT_AUTHORITY"
    assert first["unresolved_count"] == 0
    assert len(first["research_search"]) == 1
    assert first["research_search"][0]["candidate_budget"] == 27
    assert first["claim_evidence_status"][0]["decision"] == "NOT_EVALUATED"
    assert {item["status"] for item in first["protected_period_exposure"]} == {
        "DECLARED_NO_ACCESS",
        "FORBIDDEN_UNTIL_CHALLENGE_GATE",
        "SEALED_UNREAD",
        "NOT_STARTED",
    }


def test_pipeline_lists_the_read_only_ledger_interface(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["list"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["read_only_interfaces"] == ["research-governance-ledgers"]


SUCCESSOR_PATH = ROOT / "state/causal_search_access_contract_registry/5bff4a0914247a49db2f552caa199232490bab5d277051d468db01c539d11741/contract.json"


def _identity(payload: dict[str, object], field: str) -> dict[str, object]:
    payload[field] = sha256_json({key: value for key, value in payload.items() if key != field})
    return payload


def test_successor_exact_schema_rejects_reproduced_mutations() -> None:
    payload = json.loads(SUCCESSOR_PATH.read_text())
    assert validate_successor_contract(payload, root=ROOT)["schema_version"] == FAILED_CONTRACT_SCHEMA

    changed = copy.deepcopy(payload)
    changed["unknown"] = True
    _identity(changed, "contract_id")
    with pytest.raises(IntegrityError, match="keys are not exact"):
        validate_successor_contract(changed, root=ROOT)

    changed = copy.deepcopy(payload)
    changed["protocol_binding"]["contract_id"] = "0" * 64
    _identity(changed, "contract_id")
    with pytest.raises(IntegrityError, match="internal identity"):
        validate_successor_contract(changed, root=ROOT)

    changed = copy.deepcopy(payload)
    changed["scope"]["broad_generality_claim"] = True
    _identity(changed, "contract_id")
    with pytest.raises(IntegrityError, match="narrow claim"):
        validate_successor_contract(changed, root=ROOT)


@pytest.mark.parametrize("mutation", [
    lambda item: item["stage_order"].__setitem__(1, "UNKNOWN"),
    lambda item: item["question"].__setitem__("requested_claim_label", "CAUSALLY_SUPPORTED_MECHANISM"),
    lambda item: item["scope"].__setitem__("predicted_effect_markets", ["ES"]),
    lambda item: item["macro_disposition"].__setitem__("state", "SELECTED_ZN"),
    lambda item: item["timing"].__setitem__("challenge_start_inclusive", "2022-12-31T00:00:00Z"),
    lambda item: item["reset"].__setitem__("post_stage_6_material_change", "RESTART_SYNTHETIC_ONLY"),
])
def test_successor_semantic_conflicts_fail(mutation: object) -> None:
    payload = json.loads(SUCCESSOR_PATH.read_text())
    mutation(payload)  # type: ignore[operator]
    _identity(payload, "contract_id")
    with pytest.raises((IntegrityError, UnauthorizedOperation)):
        validate_successor_contract(payload, root=ROOT)


def test_successor_missing_nested_key_fails() -> None:
    payload = json.loads(SUCCESSOR_PATH.read_text())
    del payload["scope"]["last_applicable_replication_stage"]
    _identity(payload, "contract_id")
    with pytest.raises(IntegrityError, match="keys are not exact"):
        validate_successor_contract(payload, root=ROOT)


def test_active_authority_blocks_before_any_stage3_row_capable_helper(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    def forbidden(**_kwargs: object) -> object:
        nonlocal called
        called = True
        raise AssertionError("row-capable readiness helper was invoked")

    monkeypatch.setattr(gateway_module, "load_registration_ready_certificate", forbidden)

    class Boundary:
        @staticmethod
        def assert_active_root(_root: Path) -> None:
            return None

    gateway = CertifiedResearchGateway(ROOT, Boundary())  # type: ignore[arg-type]
    with pytest.raises((IntegrityError, UnauthorizedOperation)) as exc:
        gateway.register_trial(
            registration_path=ROOT / "never.json",
            registration={},
            readiness_evidence_path=ROOT / "never-readiness.json",
        )
    assert "BLOCKED_STAGE_2_5" in str(exc.value) or "Stage 3+ access request" in str(exc.value)
    assert called is False

    request = _identity({
        "schema_version": ROW_REQUEST_SCHEMA, "stage": "3",
        "purpose": "OUTCOME_OR_TARGET_CONSTRUCTION", "operation": "synthetic",
        "contract_binding": {}, "declaration_binding": {}, "markets": ["ES"],
        "start_inclusive": REFIT[0], "end_exclusive": "2023-01-01T00:00:00Z",
        "catalog_binding": {},
    }, "request_id")
    with pytest.raises((IntegrityError, UnauthorizedOperation)):
        validate_row_request(ROOT, request)


def test_gateway_validates_complete_access_request_before_readiness(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    events: list[str] = []
    monkeypatch.setattr(gateway_module, "require_stage3_active_authority", lambda _root: ({}, {}, {}))

    def reject(_root: Path, _request: object) -> object:
        events.append("access")
        raise UnauthorizedOperation("synthetic access rejection")

    def forbidden(**_kwargs: object) -> object:
        events.append("readiness")
        raise AssertionError("readiness was reached before the access guard")

    monkeypatch.setattr(gateway_module, "validate_row_request", reject)
    monkeypatch.setattr(gateway_module, "load_registration_ready_certificate", forbidden)

    class Boundary:
        @staticmethod
        def assert_active_root(_root: Path) -> None:
            return None

    gateway = CertifiedResearchGateway(tmp_path, Boundary())  # type: ignore[arg-type]
    with pytest.raises(UnauthorizedOperation, match="synthetic access rejection"):
        gateway.register_trial(
            registration_path=tmp_path / "trial.json",
            registration={"stage3_access_request": {}},
            readiness_evidence_path=tmp_path / "readiness.json",
        )
    assert events == ["access"]


def _declaration(*, state: str = "DECLARED_UNEVALUATED", predecessor: str | None = None) -> dict[str, object]:
    return _identity({
        "schema_version": DECLARATION_SCHEMA,
        "stage_2_5_contract_id": "1" * 64, "protocol_id": "2" * 64,
        "trial_family": "family", "candidate_id": "3" * 64,
        "candidate_budget_slot": 0, "predecessor_declaration_id": predecessor,
        "pipeline_stage": "5", "operation": "synthetic-test",
        "target_family": "directional", "target": "return", "horizon": "30m",
        "feature_pack": "trend", "model_family": "ridge", "hyperparameter": "1.0",
        "construction_variant": "canonical", "markets": ["ES"],
        "start_inclusive": "2018-01-01T00:00:00Z", "end_exclusive": "2023-01-01T00:00:00Z",
        "purpose": "MODEL_COMPARISON", "metric_family": "stress_net",
        "benchmark": "zero", "execution_version": "v1", "code_config_sha256": "4" * 64,
        "catalog_sha256": "5" * 64, "creation_evidence": "deterministic-test", "state": state,
    }, "declaration_id")


def test_declarations_and_ledgers_count_access_and_fail_missing_joins(tmp_path: Path) -> None:
    declaration = _declaration()
    validate_declaration(declaration)
    path = tmp_path / "state/pre_evaluation_declaration_registry/family/declaration.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(canonical_bytes(declaration) + b"\n")
    result = successor_ledgers(tmp_path)
    assert result["research_search"]["current_family_evaluated_count"] == 0
    assert result["research_search"]["legacy_aggregate_burden"] == 105

    evidence_path = tmp_path / "state/trial_registry/family/evidence.json"
    evidence_path.parent.mkdir(parents=True)
    evidence_path.write_bytes(canonical_bytes({
        "pre_evaluation_declaration_id": declaration["declaration_id"],
        "evaluation_state": "ABORTED_AFTER_ACCESS_COUNTED",
    }) + b"\n")
    first = successor_ledgers(tmp_path)
    assert first["research_search"]["current_family_evaluated_count"] == 1
    assert canonical_bytes(first) == canonical_bytes(successor_ledgers(tmp_path))

    evidence_path.write_bytes(canonical_bytes({
        "pre_evaluation_declaration_id": "f" * 64,
        "evaluation_state": "EVALUATED_FAIL",
    }) + b"\n")
    assert successor_ledgers(tmp_path)["unresolved_count"] == 1


def _challenge(tmp_path: Path) -> dict[str, object]:
    manifest = {"start_inclusive": CHALLENGE[0], "end_exclusive": CHALLENGE[1], "ordered_session_sha256": "9" * 64}
    manifest_path = tmp_path / "challenge_manifest.json"
    manifest_path.write_bytes(canonical_bytes(manifest) + b"\n")
    return _identity({
        "schema_version": CHALLENGE_CLAIM_SCHEMA, "program_id": "1" * 64,
        "period_id": "2" * 64, "trial_family": "family",
        "stage_2_5_contract_id": "3" * 64, "candidate_lineage": ["4" * 64],
        "claim_scope_sha256": "5" * 64, "replication_decision_sha256": "6" * 64,
        "session_manifest_path": manifest_path.relative_to(tmp_path).as_posix(),
        "session_manifest_sha256": __import__("hashlib").sha256(manifest_path.read_bytes()).hexdigest(),
        "ordered_session_sha256": "9" * 64, "markets": ["ES", "NQ", "CL", "GC"],
        "start_inclusive": CHALLENGE[0], "end_exclusive": CHALLENGE[1],
        "metrics_sha256": "a" * 64, "acceptance_rules_sha256": "b" * 64,
        "report_schema_sha256": "c" * 64, "failure_action": "CLOSE_MECHANISM_NO_RETRY",
    }, "claim_id")


def test_challenge_claim_is_scope_wide_atomic_and_one_shot(tmp_path: Path) -> None:
    claim = _challenge(tmp_path)
    assert claim_challenge_exposure(tmp_path, claim).is_file()
    with pytest.raises(UnauthorizedOperation, match="ALREADY_EXPOSED"):
        claim_challenge_exposure(tmp_path, claim)
    changed = copy.deepcopy(claim)
    changed["markets"] = ["ES"]
    _identity(changed, "claim_id")
    with pytest.raises(UnauthorizedOperation, match="complete declared scope"):
        claim_challenge_exposure(tmp_path, changed)


def test_challenge_rejects_wrong_period_manifest_and_ancestor_reuse(tmp_path: Path) -> None:
    claim = _challenge(tmp_path)
    changed = copy.deepcopy(claim)
    changed["start_inclusive"] = "2022-12-31T00:00:00Z"
    _identity(changed, "claim_id")
    with pytest.raises(UnauthorizedOperation, match="period"):
        claim_challenge_exposure(tmp_path, changed)
    changed = copy.deepcopy(claim)
    changed["ordered_session_sha256"] = "8" * 64
    _identity(changed, "claim_id")
    with pytest.raises(UnauthorizedOperation, match="cross-period"):
        claim_challenge_exposure(tmp_path, changed)
    assert claim_challenge_exposure(tmp_path, claim).is_file()
    successor = copy.deepcopy(claim)
    successor["trial_family"] = "renamed-family"
    successor["candidate_lineage"].append("7" * 64)
    _identity(successor, "claim_id")
    with pytest.raises(UnauthorizedOperation, match="ALREADY_EXPOSED"):
        claim_challenge_exposure(tmp_path, successor)


def _refit_contract() -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": REFIT_CONTRACT_SCHEMA,
        **{key: "d" * 64 for key in (
            "stage_2_5_contract_id", "candidate_sha256", "challenge_decision_sha256",
            "challenge_claim_sha256", "algorithm_sha256", "code_sha256", "config_sha256",
            "target_sha256", "features_sha256", "transformations_sha256",
            "hyperparameters_sha256", "session_manifest_sha256", "execution_design_sha256",
        )},
        "model_family": "ridge", "objective": "least_squares",
        "fitting_procedure": "deterministic", "convergence_policy": "fixed",
        "deterministic_setting": "seed_7", "permitted_parameter_names": ["beta"],
        "markets": ["ES", "NQ", "CL", "GC"], "session_manifest_path": "manifest.json",
        "start_inclusive": REFIT[0], "end_exclusive": REFIT[1],
        "output_schema": REFIT_OUTPUT_SCHEMA,
    }
    return _identity(payload, "contract_id")


def test_refit_contract_allows_only_exact_parameters_and_no_results() -> None:
    contract = _refit_contract()
    validate_refit_contract(contract)
    output = _identity({
        "schema_version": REFIT_OUTPUT_SCHEMA, "refit_contract_id": contract["contract_id"],
        "algorithm_sha256": contract["algorithm_sha256"],
        "session_manifest_sha256": contract["session_manifest_sha256"],
        "environment_sha256": "e" * 64, "parameters": {"beta": 1.25},
        "status": "REFIT_COMPLETE_NO_EVALUATION",
    }, "output_id")
    validate_refit_output(output, contract)
    changed = copy.deepcopy(output)
    changed["hidden"] = {"predictions": [1.0]}
    _identity(changed, "output_id")
    with pytest.raises(IntegrityError, match="keys are not exact"):
        validate_refit_output(changed, contract)


def test_refit_output_is_atomic_and_nested_results_fail(tmp_path: Path) -> None:
    contract = _refit_contract()
    output = _identity({
        "schema_version": REFIT_OUTPUT_SCHEMA, "refit_contract_id": contract["contract_id"],
        "algorithm_sha256": contract["algorithm_sha256"],
        "session_manifest_sha256": contract["session_manifest_sha256"],
        "environment_sha256": "e" * 64, "parameters": {"beta": 1.25},
        "status": "REFIT_COMPLETE_NO_EVALUATION",
    }, "output_id")
    assert claim_refit_output(tmp_path, output, contract).is_file()
    with pytest.raises(UnauthorizedOperation, match="REFIT_ALREADY_CLAIMED"):
        claim_refit_output(tmp_path, output, contract)
    changed = copy.deepcopy(output)
    changed["parameters"]["beta"] = {"nested_prediction": 1.0}
    _identity(changed, "output_id")
    with pytest.raises((IntegrityError, UnauthorizedOperation)):
        validate_refit_output(changed, contract)


def test_predictive_claim_and_forward_protocol_are_fail_closed() -> None:
    require_claim_label(PREDICTIVE_CLAIM)
    with pytest.raises(UnauthorizedOperation, match="CAUSAL_IDENTIFICATION"):
        require_claim_label("CAUSALLY_SUPPORTED_MECHANISM")
    with pytest.raises(UnauthorizedOperation, match="FORWARD_MONITORING"):
        require_forward_protocol(None)

    protocol = _identity({
        "schema_version": FORWARD_PROTOCOL_SCHEMA, "forward_start": "2026-07-14T00:00:00Z",
        "candidate_sha256": "1" * 64, "refit_output_sha256": "2" * 64,
        "code_config_sha256": "3" * 64, "markets": ["ES"], "execution_version": "v1",
        "cost_version": "v1", "review_cadence": "monthly", "minimum_calendar_duration": "12m",
        "minimum_independent_decisions": 20, "reporting_dates": ["month_end"],
        "allowed_interim_diagnostics": ["operations"],
        "prohibited_interim_success_declarations": True, "alert_rules": ["frozen"],
        "abstention_kill_rules": ["frozen"], "drift_definitions": ["frozen"],
        "execution_discrepancy_treatment": "record", "operational_failure_treatment": "record",
        "change_taxonomy": "material_or_immaterial", "material_change_reset": "new_version",
        "immaterial_fix_rules": "no_performance_direction", "version_succession": "immutable",
        "evidence_label_consequences": "no_transfer", "backfill_is_forward_evidence": False,
        "optional_stopping_can_create_success": False, "final_252_history_rewrite_allowed": False,
    }, "protocol_id")
    assert validate_forward_protocol(protocol)["protocol_id"]

    for key in ("review_cadence", "reporting_dates", "change_taxonomy", "material_change_reset"):
        changed = copy.deepcopy(protocol)
        changed[key] = [] if key == "reporting_dates" else ""
        _identity(changed, "protocol_id")
        with pytest.raises((IntegrityError, UnauthorizedOperation)):
            validate_forward_protocol(changed)
    for key in ("backfill_is_forward_evidence", "optional_stopping_can_create_success"):
        changed = copy.deepcopy(protocol)
        changed[key] = True
        _identity(changed, "protocol_id")
        with pytest.raises(UnauthorizedOperation):
            validate_forward_protocol(changed)
    changed = copy.deepcopy(protocol)
    changed["unknown"] = True
    _identity(changed, "protocol_id")
    with pytest.raises(IntegrityError, match="keys are not exact"):
        validate_forward_protocol(changed)


def test_inactive_v8_bundle_is_exact_and_old_bytes_are_preserved() -> None:
    from futures_rebuild.alpha_research_ladder import load_registered_ladder
    contract, profile = load_registered_ladder(
        ROOT,
        contract_id="2bc9e04a5f53b57c1c70517ee3b098b05784fd4b918b00596e7a724e83e11c87",
        profile_id="b9b4d88198f372656ec6685dc64753503c703e5349a1102c844ff193ef534474",
    )
    assert contract["macro_disposition"] == profile["macro_disposition"] == "NO_ELIGIBLE_MACRO_DIVERSIFIER"
    assert __import__("hashlib").sha256(CONTRACT_PATH.read_bytes()).hexdigest() == "abd4300b8de1986a9a6fa6fdbe9c0a90a0514335b215102300e9f5b523b19515"
    assert __import__("hashlib").sha256((LADDER_PATH / "universe_contract.json").read_bytes()).hexdigest() == "a70589d5e16756db94a14cd18d188233daf3e73f86237a325457025512244e2d"
