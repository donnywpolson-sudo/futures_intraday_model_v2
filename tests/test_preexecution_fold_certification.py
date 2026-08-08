from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
from pathlib import Path

import pytest
import yaml

from futures_rebuild.alpha_research_ladder import (
    ACTIVE_POINTER_PATH,
    ALL_APPROVED,
    DECISION_SCHEMA,
    SESSION_MANIFEST_SCHEMA,
    build_active_pointer,
    build_contract,
    build_profile,
)
from futures_rebuild.alpha_ladder_frozen_mechanism import (
    MANDATORY_BASELINES,
    build_frozen_mechanism,
)
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
    RepoBoundary,
)
from futures_rebuild.certified_research_gateway import CertifiedResearchGateway
from futures_rebuild.preexecution_fold_certification import (
    ROW_CERTIFIED,
    SYNTHETIC_ONLY,
    build_fold_readiness_certificate,
    create_registration_after_gate,
    require_execution_ready_before_claim,
    require_registration_ready,
)
from futures_rebuild.research_gateway_policy import (
    CERTIFIED_TRIAL_EXECUTION_OPERATION,
)


MARKETS = ("ES", "CL", "ZN", "6E")
BASELINES = ("flat", "continuation", "unconditional", "prior_momentum")
SCENARIOS = ("base", "stress", "extreme")
SOURCES = {
    f"source/{market}.parquet": digit * 64
    for market, digit in zip(MARKETS, "abcd", strict=True)
}


def _fold(*, fold: int, market: str, role: str = "OUTER") -> dict[str, object]:
    return {
        "fold_id": f"fold-{fold}",
        "market": market,
        "role": role,
        "counts": {
            "expected_training_sessions": 300,
            "complete_training_sessions": 280,
            "feature_complete_training_sessions": 280,
            "transformation_ready_training_sessions": 280,
            "expected_evaluation_sessions": 60,
            "feature_complete_evaluation_sessions": 58,
            "terminal_evaluation_sessions": 60,
            "execution_path_complete_evaluation_sessions": 58,
            "candidate_selected_sessions": 12,
            "candidate_selected_path_complete_sessions": 12,
            "scenario_risk_dispositions": {
                name: {
                    "feasible_sessions": 12,
                    "risk_abstention_sessions": 0,
                    "unresolved_sessions": 0,
                }
                for name in SCENARIOS
            },
            "purge_minutes": 60,
            "embargo_sessions": 1,
        },
        "checks": {
            "chronological_order": True,
            "purge_applied": True,
            "embargo_applied": True,
            "training_only_transformation": True,
            "contract_identity_discontinuities_terminalized": True,
            "roll_discontinuities_terminalized": True,
            "all_incomplete_sessions_terminalized": True,
            "complete_required_metrics": True,
            "promotion_path_computable": True,
        },
        "baseline_universe_readiness": {
            name: {
                "expected_sessions": 60,
                "terminal_sessions": 60,
                "selected_sessions": 0 if name == "flat" else 12,
                "selected_path_complete_sessions": 0 if name == "flat" else 12,
                "scenario_risk_dispositions": {
                    scenario: {
                        "feasible_sessions": 0 if name == "flat" else 12,
                        "risk_abstention_sessions": 0,
                        "unresolved_sessions": 0,
                    }
                    for scenario in SCENARIOS
                },
                "schedule_independently_derived": True,
                "flat_no_trade": name == "flat",
            }
            for name in BASELINES
        },
        "exclusion_reasons": {
            "TRAINING__MISSING_DEPENDENCY": 20,
            "EVALUATION__MISSING_DEPENDENCY": 2,
        },
        "market_year_breakdown": {
            "2018": {
                "expected_training_sessions": 300,
                "complete_training_sessions": 280,
                "expected_evaluation_sessions": 60,
                "feature_complete_evaluation_sessions": 58,
                "terminal_evaluation_sessions": 60,
                "execution_path_complete_evaluation_sessions": 58,
                "exclusion_reasons": {
                    "TRAINING__MISSING_DEPENDENCY": 20,
                    "EVALUATION__MISSING_DEPENDENCY": 2,
                },
            },
        },
    }


def _evidence(*, outer: int = 2, nested: int = 1) -> list[dict[str, object]]:
    return [
        _fold(fold=fold, market=market, role=role)
        for role, count in (("OUTER", outer), ("NESTED", nested))
        for fold in range(count)
        for market in MARKETS
    ]


def _certificate(
    evidence: list[dict[str, object]] | None = None, *,
    evidence_class: str = ROW_CERTIFIED, outer: int = 2, nested: int = 1,
    source_bindings: dict[str, str] = SOURCES,
    protocol_id: str = "protocol",
) -> dict[str, object]:
    return build_fold_readiness_certificate(
        trial_family="future_mechanism",
        protocol_id=protocol_id,
        source_bindings=source_bindings,
        fold_evidence=_evidence(outer=outer, nested=nested) if evidence is None else evidence,
        required_markets=MARKETS,
        required_baselines=BASELINES,
        required_cost_scenarios=SCENARIOS,
        required_outer_fold_ids=tuple(f"fold-{index}" for index in range(outer)),
        required_nested_fold_ids=tuple(f"fold-{index}" for index in range(nested)),
        expected_outer_folds=outer,
        expected_nested_folds=nested,
        minimum_training_sessions=252,
        minimum_evaluation_sessions=50,
        minimum_purge_minutes=60,
        minimum_embargo_sessions=1,
        evidence_class=evidence_class,
        historical_rows_opened=evidence_class == ROW_CERTIFIED,
    )


def _materialize_sources(root: Path) -> dict[str, str]:
    bindings: dict[str, str] = {}
    for market in MARKETS:
        relative = f"source/{market}.parquet"
        payload = f"immutable-{market}".encode()
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        bindings[relative] = sha256(payload).hexdigest()
    return bindings


def _certificate_evidence(
    root: Path, certificate: dict[str, object], *, name: str = "certificate",
) -> Path:
    path = root / "state" / "fold_readiness" / f"{name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes({"fold_readiness_certificate": certificate}) + b"\n")
    return path


def _registration(
    root: Path, certificate: dict[str, object], evidence_path: Path,
    *, alpha_ladder_binding: dict[str, object] | None = None,
) -> dict[str, object]:
    core = {
        "schema_version": "future_trial_registration/1.0.0",
        "trial_family": certificate["trial_family"],
        "protocol_id": certificate["protocol_id"],
        "fold_readiness_binding": {
            "evidence_path": evidence_path.relative_to(root).as_posix(),
            "evidence_sha256": sha256(evidence_path.read_bytes()).hexdigest(),
            "certificate_id": certificate["certificate_id"],
        },
    }
    if alpha_ladder_binding is not None:
        core["alpha_ladder_binding"] = alpha_ladder_binding
    return {**core, "trial_id": sha256_json(core)}


def _gateway_ladder_context(
    root: Path, *, mechanism: str = "a" * 64,
) -> tuple[dict[str, object], dict[str, str], str]:
    predecessor_relative = "configs/research_universe_contract.json"
    predecessor = root / predecessor_relative
    predecessor.parent.mkdir(parents=True, exist_ok=True)
    predecessor.write_bytes(canonical_bytes({"preserved": True}) + b"\n")
    predecessor_sha = sha256(predecessor.read_bytes()).hexdigest()
    contract = build_contract(
        predecessor_path=predecessor_relative,
        predecessor_sha256=predecessor_sha,
        predecessor_contract_id="d" * 64,
    )
    contract_relative = "state/alpha_ladder_registry/contract.json"
    contract_path = root / contract_relative
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_bytes(canonical_bytes(contract) + b"\n")
    contract_sha = sha256(contract_path.read_bytes()).hexdigest()
    profile = build_profile(
        contract_path=contract_relative,
        contract_sha256=contract_sha,
        contract_id=str(contract["contract_id"]),
    )
    profile_relative = "state/alpha_ladder_registry/profile.yaml"
    profile_path = root / profile_relative
    profile_path.write_text(yaml.safe_dump(profile, sort_keys=False), encoding="utf-8")
    profile_sha = sha256(profile_path.read_bytes()).hexdigest()
    pointer = build_active_pointer(
        contract_path=contract_relative,
        contract_sha256=contract_sha,
        contract_id=str(contract["contract_id"]),
        profile_path=profile_relative,
        profile_sha256=profile_sha,
        profile_id=str(profile["profile_id"]),
    )
    pointer_path = root / ACTIVE_POINTER_PATH
    pointer_path.parent.mkdir(parents=True, exist_ok=True)
    pointer_path.write_bytes(canonical_bytes(pointer) + b"\n")

    frozen = build_frozen_mechanism(
        contract_id=str(contract["contract_id"]), profile_id=str(profile["profile_id"]),
        source_protocol_id="b" * 64, source_protocol_sha256="c" * 64,
        all_markets=ALL_APPROVED,
    )
    mechanism_relative = "state/alpha_ladder_mechanisms/frozen.json"
    mechanism_path = root / mechanism_relative
    mechanism_path.parent.mkdir(parents=True, exist_ok=True)
    mechanism_path.write_bytes(canonical_bytes(frozen) + b"\n")
    mechanism = sha256(mechanism_path.read_bytes()).hexdigest()
    mechanism_id = str(frozen["mechanism_id"])

    predecessor_core = {
        "schema_version": DECISION_SCHEMA,
        "contract_id": contract["contract_id"],
        "mechanism_sha256": mechanism,
        "stage": "pilot",
        "decision": "PASS",
        "promotion_evidence": {
            "stress_net_pnl_usd": "100",
            "baseline_stress_net_pnl_usd": {
                name: "0" if name == "flat_no_trade" else "50"
                for name in MANDATORY_BASELINES
            },
            "trade_count": 8, "maximum_continuous_drawdown_usd": "1000",
            "complete_coverage": True, "complete_metrics": True,
            "risk_rules_compliant": True, "live_readiness_claim": False,
            "formal_significance_claim": False,
        },
    }
    predecessor_decision = {
        **predecessor_core, "decision_id": sha256_json(predecessor_core),
    }
    decision_relative = "state/alpha_ladder_decisions/pilot.json"
    decision_path = root / decision_relative
    decision_path.parent.mkdir(parents=True, exist_ok=True)
    decision_path.write_bytes(canonical_bytes(predecessor_decision) + b"\n")
    decision_sha = sha256(decision_path.read_bytes()).hexdigest()

    pilot_sessions = [f"pilot-{index:04d}" for index in range(63)]
    stage_sessions = [f"confirmation-{index:04d}" for index in range(504)]
    manifest_core = {
        "schema_version": SESSION_MANIFEST_SCHEMA,
        "contract_id": contract["contract_id"],
        "mechanism_sha256": mechanism,
        "stage": "tier_1",
        "excluded_pilot_evaluation_session_ids": pilot_sessions,
        "evaluation_session_ids_by_market": {
            market: stage_sessions for market in MARKETS
        },
    }
    manifest = {**manifest_core, "manifest_id": sha256_json(manifest_core)}
    manifest_relative = "state/alpha_ladder_sessions/tier_1.json"
    manifest_path = root / manifest_relative
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_bytes(canonical_bytes(manifest) + b"\n")
    manifest_sha = sha256(manifest_path.read_bytes()).hexdigest()
    binding = {
        "contract_id": contract["contract_id"],
        "profile_id": profile["profile_id"],
        "stage": "tier_1",
        "mechanism_sha256": mechanism,
        "mechanism_path": mechanism_relative,
        "mechanism_id": mechanism_id,
        "predecessor_decision_path": decision_relative,
        "predecessor_decision_sha256": decision_sha,
        "session_manifest_path": manifest_relative,
        "session_manifest_sha256": manifest_sha,
        "pilot_evaluation_session_ids_sha256": sha256_json(pilot_sessions),
    }
    return binding, {manifest_relative: manifest_sha}, mechanism_id


def _write_registration(root: Path, registration: dict[str, object]) -> Path:
    path = (
        root / "state" / "trial_registry" / "future"
        / f"{registration['trial_id']}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(registration) + b"\n")
    return path


def _mutate(evidence: list[dict[str, object]], **counts: int) -> list[dict[str, object]]:
    changed = deepcopy(evidence)
    changed[0]["counts"].update(counts)  # type: ignore[union-attr]
    for name, value in counts.items():
        if name in {
            "expected_training_sessions",
            "complete_training_sessions",
            "expected_evaluation_sessions",
            "feature_complete_evaluation_sessions",
            "terminal_evaluation_sessions",
            "execution_path_complete_evaluation_sessions",
        }:
            changed[0]["market_year_breakdown"]["2018"][name] = value  # type: ignore[index]
    current = changed[0]["counts"]  # type: ignore[assignment]
    training_missing = (
        current["expected_training_sessions"] - current["complete_training_sessions"]
    )
    evaluation_missing = (
        current["expected_evaluation_sessions"]
        - current["execution_path_complete_evaluation_sessions"]
    )
    changed[0]["exclusion_reasons"] = {
        "TRAINING__MISSING_DEPENDENCY": training_missing,
        "EVALUATION__MISSING_DEPENDENCY": evaluation_missing,
    }
    changed[0]["market_year_breakdown"]["2018"]["exclusion_reasons"] = {  # type: ignore[index]
        "TRAINING__MISSING_DEPENDENCY": training_missing,
        "EVALUATION__MISSING_DEPENDENCY": evaluation_missing,
    }
    return changed


def test_passing_row_certification_allows_registration_and_claim(tmp_path) -> None:
    sources = _materialize_sources(tmp_path)
    certificate = _certificate(source_bindings=sources)
    assert certificate == _certificate(source_bindings=sources)
    assert certificate["overall_decision"] == "PASS"
    assert require_registration_ready(certificate, root=tmp_path)
    evidence_path = _certificate_evidence(tmp_path, certificate)
    registration = _registration(tmp_path, certificate, evidence_path)
    claimed: list[bool] = []
    path = (
        tmp_path / "state" / "trial_registry" / "future"
        / f"{registration['trial_id']}.json"
    )
    create_registration_after_gate(
        root=tmp_path, path=path, payload=registration,
        certificate_evidence_path=evidence_path,
    )
    result = require_execution_ready_before_claim(
        root=tmp_path,
        registration_path=path,
        expected_registration_sha256=sha256(path.read_bytes()).hexdigest(),
        claim_authorization=lambda: claimed.append(True) or "claimed",
    )
    assert result == "claimed" and claimed == [True]


def test_current_gateway_derives_exact_scope_and_consumes_once(tmp_path) -> None:
    sources = _materialize_sources(tmp_path)
    ladder_binding, ladder_sources, mechanism_id = _gateway_ladder_context(tmp_path)
    sources.update(ladder_sources)
    certificate = _certificate(source_bindings=sources, protocol_id=mechanism_id)
    evidence_path = _certificate_evidence(tmp_path, certificate)
    registration = _registration(
        tmp_path, certificate, evidence_path,
        alpha_ladder_binding=ladder_binding,
    )
    registration_path = (
        tmp_path / "state" / "trial_registry" / "future"
        / f"{registration['trial_id']}.json"
    )
    gateway = CertifiedResearchGateway(tmp_path, RepoBoundary(tmp_path))
    registered = gateway.register_trial(
        registration_path=registration_path,
        registration=registration,
        readiness_evidence_path=evidence_path,
    )
    registration_sha256 = registered["registration_sha256"]
    extra = {"execution_plan_id": "e" * 64}
    scope = gateway.execution_scope(
        registration_path=registration_path,
        expected_registration_sha256=registration_sha256,
        additional_scope=extra,
    )
    plan_id = "a" * 64
    plan_sha256 = "b" * 64
    approval = (
        f"APPROVE {CERTIFIED_TRIAL_EXECUTION_OPERATION} "
        f"PLAN {plan_id} SHA256 {plan_sha256}"
    )
    receipt = OperationReceipt.issue_user_approved(
        gateway.boundary,
        operation=CERTIFIED_TRIAL_EXECUTION_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope,
        approval_command=CERTIFIED_TRIAL_EXECUTION_OPERATION,
        approval_plan_id=plan_id,
        approval_plan_sha256=plan_sha256,
        approval_line=approval,
    )
    claim = gateway.claim_historical_execution(
        registration_path=registration_path,
        expected_registration_sha256=registration_sha256,
        receipt=receipt,
        additional_scope=extra,
    )
    assert claim.exists()
    with pytest.raises(UnauthorizedOperation, match="already used"):
        gateway.claim_historical_execution(
            registration_path=registration_path,
            expected_registration_sha256=registration_sha256,
            receipt=receipt,
            additional_scope=extra,
        )


def test_current_gateway_rejects_scope_substitution_before_claim(tmp_path) -> None:
    sources = _materialize_sources(tmp_path)
    ladder_binding, ladder_sources, mechanism_id = _gateway_ladder_context(tmp_path)
    sources.update(ladder_sources)
    certificate = _certificate(source_bindings=sources, protocol_id=mechanism_id)
    evidence_path = _certificate_evidence(tmp_path, certificate)
    registration = _registration(
        tmp_path, certificate, evidence_path,
        alpha_ladder_binding=ladder_binding,
    )
    registration_path = (
        tmp_path / "state" / "trial_registry" / "future"
        / f"{registration['trial_id']}.json"
    )
    gateway = CertifiedResearchGateway(tmp_path, RepoBoundary(tmp_path))
    registered = gateway.register_trial(
        registration_path=registration_path,
        registration=registration,
        readiness_evidence_path=evidence_path,
    )
    scope = gateway.execution_scope(
        registration_path=registration_path,
        expected_registration_sha256=registered["registration_sha256"],
    )
    scope["trial_id"] = "f" * 64
    plan_id = "a" * 64
    plan_sha256 = "b" * 64
    receipt = OperationReceipt.issue_user_approved(
        gateway.boundary,
        operation=CERTIFIED_TRIAL_EXECUTION_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope,
        approval_command=CERTIFIED_TRIAL_EXECUTION_OPERATION,
        approval_plan_id=plan_id,
        approval_plan_sha256=plan_sha256,
        approval_line=(
            f"APPROVE {CERTIFIED_TRIAL_EXECUTION_OPERATION} "
            f"PLAN {plan_id} SHA256 {plan_sha256}"
        ),
    )
    with pytest.raises(UnauthorizedOperation, match="not bound"):
        gateway.claim_historical_execution(
            registration_path=registration_path,
            expected_registration_sha256=registered["registration_sha256"],
            receipt=receipt,
        )
    assert not (
        tmp_path / "state" / "authorization_uses" / f"{receipt.receipt_id}.json"
    ).exists()


def test_earliest_fold_below_252_fails_before_claim(tmp_path) -> None:
    sources = _materialize_sources(tmp_path)
    certificate = _certificate(_mutate(
        _evidence(), complete_training_sessions=251,
        feature_complete_training_sessions=251,
        transformation_ready_training_sessions=251,
    ), source_bindings=sources)
    evidence_path = _certificate_evidence(tmp_path, certificate)
    registration_path = _write_registration(
        tmp_path, _registration(tmp_path, certificate, evidence_path),
    )
    claimed: list[bool] = []
    with pytest.raises(UnauthorizedOperation, match="passing fold gate"):
        require_execution_ready_before_claim(
            root=tmp_path,
            registration_path=registration_path,
            expected_registration_sha256=sha256(
                registration_path.read_bytes()
            ).hexdigest(),
            claim_authorization=lambda: claimed.append(True),
        )
    assert claimed == []


def test_certificate_evidence_change_fails_before_claim(tmp_path) -> None:
    sources = _materialize_sources(tmp_path)
    certificate = _certificate(source_bindings=sources)
    evidence_path = _certificate_evidence(tmp_path, certificate)
    registration = _registration(tmp_path, certificate, evidence_path)
    registration_path = _write_registration(tmp_path, registration)
    evidence_path.write_bytes(b"changed\n")
    claimed: list[bool] = []
    with pytest.raises(IntegrityError, match="evidence binding changed"):
        require_execution_ready_before_claim(
            root=tmp_path,
            registration_path=registration_path,
            expected_registration_sha256=sha256(
                registration_path.read_bytes()
            ).hexdigest(),
            claim_authorization=lambda: claimed.append(True),
        )
    assert claimed == []


def test_registration_change_fails_before_claim(tmp_path) -> None:
    sources = _materialize_sources(tmp_path)
    certificate = _certificate(source_bindings=sources)
    evidence_path = _certificate_evidence(tmp_path, certificate)
    registration = _registration(tmp_path, certificate, evidence_path)
    registration_path = _write_registration(tmp_path, registration)
    expected = sha256(registration_path.read_bytes()).hexdigest()
    changed = dict(registration)
    changed["unregistered_change"] = True
    registration_path.write_bytes(canonical_bytes(changed) + b"\n")
    claimed: list[bool] = []
    with pytest.raises(IntegrityError, match="registration binding changed"):
        require_execution_ready_before_claim(
            root=tmp_path,
            registration_path=registration_path,
            expected_registration_sha256=expected,
            claim_authorization=lambda: claimed.append(True),
        )
    assert claimed == []


def test_registration_cannot_bind_a_different_passing_certificate(tmp_path) -> None:
    sources = _materialize_sources(tmp_path)
    first = _certificate(source_bindings=sources, protocol_id="protocol-a")
    second = _certificate(source_bindings=sources, protocol_id="protocol-b")
    first_path = _certificate_evidence(tmp_path, first, name="first")
    second_path = _certificate_evidence(tmp_path, second, name="second")
    registration = _registration(tmp_path, first, first_path)
    target = (
        tmp_path / "state" / "trial_registry" / "future"
        / f"{registration['trial_id']}.json"
    )
    with pytest.raises(IntegrityError, match="not bound to this fold certificate"):
        create_registration_after_gate(
            root=tmp_path,
            path=target,
            payload=registration,
            certificate_evidence_path=second_path,
        )
    assert not target.exists()


def test_aggregate_can_pass_while_one_fold_market_fails() -> None:
    evidence = _evidence()
    evidence[0]["counts"]["complete_training_sessions"] = 251  # type: ignore[index]
    evidence[0]["market_year_breakdown"]["2018"]["complete_training_sessions"] = 251  # type: ignore[index]
    evidence[0]["exclusion_reasons"]["TRAINING__MISSING_DEPENDENCY"] = 49  # type: ignore[index]
    evidence[0]["market_year_breakdown"]["2018"]["exclusion_reasons"][  # type: ignore[index]
        "TRAINING__MISSING_DEPENDENCY"
    ] = 49
    certificate = _certificate(evidence)
    assert sum(
        item["counts"]["complete_training_sessions"]  # type: ignore[index]
        for item in certificate["fold_market_results"]  # type: ignore[union-attr]
    ) > 252
    assert certificate["overall_decision"] == "FAIL"


def test_one_market_failure_cannot_be_hidden_by_other_markets() -> None:
    evidence = _evidence()
    failing = next(item for item in evidence if item["market"] == "CL")
    failing["counts"].update({  # type: ignore[union-attr]
        "complete_training_sessions": 100,
        "feature_complete_training_sessions": 100,
        "transformation_ready_training_sessions": 100,
    })
    failing["market_year_breakdown"]["2018"]["complete_training_sessions"] = 100  # type: ignore[index]
    failing["exclusion_reasons"]["TRAINING__MISSING_DEPENDENCY"] = 200  # type: ignore[index]
    failing["market_year_breakdown"]["2018"]["exclusion_reasons"][  # type: ignore[index]
        "TRAINING__MISSING_DEPENDENCY"
    ] = 200
    certificate = _certificate(evidence)
    result = next(
        item for item in certificate["fold_market_results"]  # type: ignore[union-attr]
        if item["market"] == "CL"
        and item["fold_id"] == "fold-0"
        and item["role"] == "OUTER"
    )
    assert result["status"] == "FAIL"


@pytest.mark.parametrize(
    ("field", "value", "gate"),
    (("purge_minutes", 59, "PURGE_REQUIREMENT"),
     ("embargo_sessions", 0, "EMBARGO_REQUIREMENT")),
)
def test_purge_or_embargo_reduction_fails(field: str, value: int, gate: str) -> None:
    certificate = _certificate(_mutate(_evidence(), **{field: value}))
    result = next(
        item for item in certificate["fold_market_results"]  # type: ignore[union-attr]
        if item["role"] == "OUTER"
        and item["fold_id"] == "fold-0"
        and item["market"] == "ES"
    )
    assert gate in result["failed_gates"]  # type: ignore[operator]


def test_missing_feature_history_fails() -> None:
    certificate = _certificate(_mutate(
        _evidence(), complete_training_sessions=200,
        feature_complete_training_sessions=200,
        transformation_ready_training_sessions=200,
    ))
    assert certificate["overall_decision"] == "FAIL"


def test_too_few_feature_complete_evaluation_sessions_fails() -> None:
    certificate = _certificate(_mutate(
        _evidence(), feature_complete_evaluation_sessions=49,
    ))
    assert certificate["overall_decision"] == "FAIL"


def test_incomplete_exact_execution_path_fails() -> None:
    certificate = _certificate(_mutate(
        _evidence(), candidate_selected_path_complete_sessions=11,
    ))
    assert certificate["overall_decision"] == "FAIL"


def test_unterminalized_evaluation_session_fails() -> None:
    certificate = _certificate(_mutate(
        _evidence(), terminal_evaluation_sessions=59,
    ))
    assert certificate["overall_decision"] == "FAIL"


def test_scenario_risk_infeasibility_fails() -> None:
    evidence = _evidence()
    evidence[0]["counts"]["scenario_risk_dispositions"]["stress"][  # type: ignore[index]
        "feasible_sessions"
    ] = 11
    certificate = _certificate(evidence)
    assert certificate["overall_decision"] == "FAIL"


def test_scenario_risk_abstention_is_a_complete_terminal_disposition() -> None:
    evidence = _evidence()
    disposition = evidence[0]["counts"]["scenario_risk_dispositions"]["stress"]  # type: ignore[index]
    disposition["feasible_sessions"] = 11
    disposition["risk_abstention_sessions"] = 1
    assert _certificate(evidence)["overall_decision"] == "PASS"


def test_missing_locked_cost_scenario_fails_closed() -> None:
    evidence = _evidence()
    del evidence[0]["counts"]["scenario_risk_dispositions"]["extreme"]  # type: ignore[index]
    with pytest.raises(IntegrityError, match="locked scenarios"):
        _certificate(evidence)


def test_market_year_breakdown_must_reconcile_to_fold_totals() -> None:
    evidence = _evidence()
    evidence[0]["market_year_breakdown"]["2018"]["complete_training_sessions"] = 279  # type: ignore[index]
    with pytest.raises(IntegrityError, match="do not reconcile"):
        _certificate(evidence)


@pytest.mark.parametrize(
    "check", ("complete_required_metrics", "promotion_path_computable"),
)
def test_metrics_or_promotion_path_unavailable_fails(check: str) -> None:
    evidence = _evidence()
    evidence[0]["checks"][check] = False  # type: ignore[index]
    assert _certificate(evidence)["overall_decision"] == "FAIL"


@pytest.mark.parametrize(
    "check",
    (
        "contract_identity_discontinuities_terminalized",
        "roll_discontinuities_terminalized",
    ),
)
def test_identity_or_roll_discontinuity_fails(check: str) -> None:
    evidence = _evidence()
    evidence[0]["checks"][check] = False  # type: ignore[index]
    assert _certificate(evidence)["overall_decision"] == "FAIL"


def test_candidate_pass_cannot_hide_mandatory_baseline_failure() -> None:
    evidence = _evidence()
    evidence[0]["baseline_universe_readiness"]["continuation"][  # type: ignore[index]
        "selected_path_complete_sessions"
    ] = 11
    certificate = _certificate(evidence)
    result = next(
        item for item in certificate["fold_market_results"]  # type: ignore[union-attr]
        if item["role"] == "OUTER"
        and item["fold_id"] == "fold-0"
        and item["market"] == "ES"
    )
    assert "MANDATORY_BASELINE_SELECTED_PATH_COVERAGE" in (
        result["failed_gates"]  # type: ignore[operator]
    )


def test_baseline_schedule_must_be_independently_derived() -> None:
    evidence = _evidence()
    evidence[0]["baseline_universe_readiness"]["continuation"][  # type: ignore[index]
        "schedule_independently_derived"
    ] = False
    result = next(
        item for item in _certificate(evidence)["fold_market_results"]
        if item["role"] == "OUTER" and item["fold_id"] == "fold-0"
        and item["market"] == "ES"
    )
    assert "MANDATORY_BASELINE_INDEPENDENT_SCHEDULING" in result["failed_gates"]


def test_flat_baseline_must_make_zero_trades() -> None:
    evidence = _evidence()
    flat = evidence[0]["baseline_universe_readiness"]["flat"]  # type: ignore[index]
    flat["selected_sessions"] = 1
    flat["selected_path_complete_sessions"] = 1
    flat["scenario_risk_dispositions"] = {
        name: {
            "feasible_sessions": 1,
            "risk_abstention_sessions": 0,
            "unresolved_sessions": 0,
        }
        for name in SCENARIOS
    }
    result = next(
        item for item in _certificate(evidence)["fold_market_results"]
        if item["role"] == "OUTER" and item["fold_id"] == "fold-0"
        and item["market"] == "ES"
    )
    assert "FLAT_BASELINE_MUST_MAKE_ZERO_TRADES" in result["failed_gates"]


def test_bound_source_change_invalidates_certificate(tmp_path) -> None:
    sources = _materialize_sources(tmp_path)
    certificate = _certificate(source_bindings=sources)
    (tmp_path / "source" / "ES.parquet").write_bytes(b"changed")
    with pytest.raises(IntegrityError, match="source binding changed"):
        require_registration_ready(certificate, root=tmp_path)


def test_registration_path_cannot_leave_registry(tmp_path) -> None:
    sources = _materialize_sources(tmp_path)
    certificate = _certificate(source_bindings=sources)
    evidence_path = _certificate_evidence(tmp_path, certificate)
    registration = _registration(tmp_path, certificate, evidence_path)
    with pytest.raises(UnauthorizedOperation, match="leaves the registry"):
        create_registration_after_gate(
            root=tmp_path, path=tmp_path / "outside.json",
            payload=registration, certificate_evidence_path=evidence_path,
        )
    assert not (tmp_path / "outside.json").exists()


def test_rehashed_forged_pass_cannot_bypass_semantic_rebuild(tmp_path) -> None:
    sources = _materialize_sources(tmp_path)
    certificate = _certificate(
        _mutate(
            _evidence(), complete_training_sessions=251,
            feature_complete_training_sessions=251,
            transformation_ready_training_sessions=251,
        ),
        source_bindings=sources,
    )
    forged = deepcopy(certificate)
    forged["fold_market_results"][0]["failed_gates"] = []  # type: ignore[index]
    forged["fold_market_results"][0]["status"] = "PASS"  # type: ignore[index]
    forged["failed_global_gates"] = []
    forged["registration_allowed"] = True
    forged["historical_execution_authorization_allowed"] = True
    forged["overall_decision"] = "PASS"
    core = dict(forged)
    core.pop("certificate_id")
    forged["certificate_id"] = sha256_json(core)
    with pytest.raises(IntegrityError, match="semantics do not reproduce"):
        require_registration_ready(forged, root=tmp_path)


def test_synthetic_pass_is_never_registration_evidence(tmp_path) -> None:
    sources = _materialize_sources(tmp_path)
    certificate = _certificate(
        evidence_class=SYNTHETIC_ONLY, source_bindings=sources,
    )
    assert certificate["overall_decision"] == "FAIL"
    evidence_path = _certificate_evidence(tmp_path, certificate)
    registration = _registration(tmp_path, certificate, evidence_path)
    with pytest.raises(UnauthorizedOperation, match="row-certified"):
        create_registration_after_gate(
            root=tmp_path,
            path=(tmp_path / "state" / "trial_registry"
                  / f"{registration['trial_id']}.json"),
            payload=registration, certificate_evidence_path=evidence_path,
        )
    assert not (tmp_path / "state" / "trial_registry" / "trial.json").exists()


def test_missing_fold_market_evidence_fails_exact_coverage() -> None:
    evidence = _evidence()[:-1]
    certificate = _certificate(evidence)
    assert "EXACT_FOLD_MARKET_COVERAGE" in certificate["failed_global_gates"]


def test_equal_row_count_with_wrong_market_topology_fails() -> None:
    evidence = _evidence()
    evidence[0]["market"] = "GC"
    certificate = _certificate(evidence)
    assert "EXACT_FOLD_MARKET_COVERAGE" in certificate["failed_global_gates"]


def test_equal_row_count_with_wrong_fold_identity_fails() -> None:
    evidence = _evidence()
    for item in evidence:
        if item["role"] == "OUTER" and item["fold_id"] == "fold-0":
            item["fold_id"] = "fold-wrong"
    certificate = _certificate(evidence)
    assert "EXACT_FOLD_MARKET_COVERAGE" in certificate["failed_global_gates"]


def test_malformed_counts_fail_closed() -> None:
    evidence = _mutate(_evidence(), candidate_selected_path_complete_sessions=13)
    with pytest.raises(IntegrityError, match="exceeds selected sessions"):
        _certificate(evidence)
