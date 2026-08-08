from __future__ import annotations

import copy
import json
from hashlib import sha256
from pathlib import Path

import pytest
import yaml

from futures_rebuild.alpha_research_ladder import (
    ACTIVE_POINTER_PATH,
    ALL_APPROVED,
    BALANCED,
    CORE,
    DECISION_SCHEMA,
    SATELLITE,
    SESSION_MANIFEST_SCHEMA,
    TRADITIONAL,
    build_active_pointer,
    build_contract,
    build_profile,
    load_active_ladder,
    validate_contract,
    validate_session_manifest,
    validate_stage_decision,
    validate_stage_registration,
)
from futures_rebuild.alpha_ladder_frozen_mechanism import (
    MANDATORY_BASELINES,
    build_frozen_mechanism,
    build_tier0_certificate,
)
from futures_rebuild.preexecution_fold_certification import (
    ROW_CERTIFIED,
    build_fold_readiness_certificate,
)
from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.certified_research_gateway import CertifiedResearchGateway
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation


def _write_json(root: Path, relative: str, payload: dict[str, object]) -> str:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = canonical_bytes(payload) + b"\n"
    path.write_bytes(raw)
    return sha256(raw).hexdigest()


def _identified(core: dict[str, object], key: str) -> dict[str, object]:
    return {**core, key: sha256_json(core)}


def _active_ladder(root: Path) -> tuple[dict[str, object], dict[str, object]]:
    predecessor_path = "configs/research_universe_contract.json"
    predecessor_sha = _write_json(root, predecessor_path, {"preserved": True})
    contract = build_contract(
        predecessor_path=predecessor_path, predecessor_sha256=predecessor_sha,
    )
    contract_path = "state/alpha_ladder_registry/contract.json"
    contract_sha = _write_json(root, contract_path, contract)
    profile = build_profile(
        contract_path=contract_path,
        contract_sha256=contract_sha,
        contract_id=str(contract["contract_id"]),
    )
    profile_path = root / "state/alpha_ladder_registry/profile.yaml"
    profile_path.write_text(yaml.safe_dump(profile, sort_keys=False), encoding="utf-8")
    profile_sha = sha256(profile_path.read_bytes()).hexdigest()
    pointer = build_active_pointer(
        contract_path=contract_path,
        contract_sha256=contract_sha,
        contract_id=str(contract["contract_id"]),
        profile_path="state/alpha_ladder_registry/profile.yaml",
        profile_sha256=profile_sha,
        profile_id=str(profile["profile_id"]),
    )
    _write_json(root, ACTIVE_POINTER_PATH.as_posix(), pointer)
    return contract, profile


def _promotion_evidence(stage: str, markets: tuple[str, ...]) -> dict[str, object]:
    result: dict[str, object] = {
        "stress_net_pnl_usd": "100",
        "baseline_stress_net_pnl_usd": {
            name: "0" if name == "flat_no_trade" else "50"
            for name in MANDATORY_BASELINES
        },
        "trade_count": 8 if stage == "pilot" else 40,
        "maximum_continuous_drawdown_usd": "1000",
        "complete_coverage": True, "complete_metrics": True,
        "risk_rules_compliant": True, "live_readiness_claim": False,
    }
    if stage == "pilot":
        result["formal_significance_claim"] = False
        return result
    result.update({
        "primary_bootstrap_lower_bound_above_zero": True,
        "all_paired_baseline_lower_bounds_above_zero": True,
        "positive_portfolio_years": [True, True, True, False, False],
        "positive_folds": [True, True, True, True, True, False, False, False],
    })
    if stage == "tier_1":
        result.update({"positive_markets": list(markets[:3]),
                       "positive_market_year_cells": [True] * 8 + [False] * 12})
    elif stage == "tier_2":
        additions = [market for market in markets if market not in CORE]
        result.update({
            "positive_markets": [*CORE[:3], *additions[:8]],
            "positive_market_year_cells": [True] * 32 + [False] * 48,
            "subgroup_decisions": {"core": "PASS", "additions": "PASS", "combined": "PASS"},
            "subgroup_stress_net_pnl_usd": {
                "core": "25", "additions": "75", "combined": "100",
            },
        })
    else:
        result.update({
            "positive_markets": list(TRADITIONAL[:26]),
            "positive_market_year_cells": [True] * 76 + [False] * 114,
            "subgroup_decisions": {
                "traditional": "PASS", "satellite": "REPORTED", "combined": "PASS",
                "satellite_can_rescue_traditional_failure": False,
            },
            "traditional_gate_results": {
                "stress_net_pnl_positive": True,
                "beat_zero_and_all_baselines": True,
                "formal_tests_passed": True,
                "complete_coverage_and_metrics": True,
                "drawdown_within_1500": True,
            },
        })
    return result


def _decision(
    *, contract_id: str, mechanism: str, stage: str, decision: str = "PASS",
    subgroup: dict[str, object] | None = None, extras: dict[str, object] | None = None,
) -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": DECISION_SCHEMA,
        "contract_id": contract_id,
        "mechanism_sha256": mechanism,
        "stage": stage,
        "decision": decision,
    }
    if subgroup is not None:
        core["subgroup_decisions"] = subgroup
    core.update(extras or {})
    return _identified(core, "decision_id")


def _frozen_mechanism(
    root: Path, contract: dict[str, object], profile: dict[str, object],
) -> tuple[str, str, str]:
    payload = build_frozen_mechanism(
        contract_id=str(contract["contract_id"]), profile_id=str(profile["profile_id"]),
        source_protocol_id="b" * 64, source_protocol_sha256="c" * 64,
        all_markets=ALL_APPROVED,
    )
    relative = "state/mechanisms/frozen.json"
    digest = _write_json(root, relative, payload)
    return relative, digest, str(payload["mechanism_id"])


def _row_certificate(
    root: Path, *, markets: tuple[str, ...], protocol_id: str, name: str,
    additional_sources: dict[str, str] | None = None,
) -> tuple[str, str]:
    scenarios = ("base", "stress", "extreme")
    sources: dict[str, str] = {}
    for market in markets:
        relative = f"source/{name}/{market}.bin"
        sources[relative] = _write_json(root, relative, {"market": market})
    sources.update(additional_sources or {})
    evidence = []
    for market in markets:
        evidence.append({
            "fold_id": "fold-0", "market": market, "role": "OUTER",
            "counts": {
                "expected_training_sessions": 504, "complete_training_sessions": 504,
                "feature_complete_training_sessions": 504,
                "transformation_ready_training_sessions": 504,
                "expected_evaluation_sessions": 63,
                "feature_complete_evaluation_sessions": 63,
                "terminal_evaluation_sessions": 63,
                "execution_path_complete_evaluation_sessions": 63,
                "candidate_selected_sessions": 10,
                "candidate_selected_path_complete_sessions": 10,
                "scenario_risk_dispositions": {
                    scenario: {"feasible_sessions": 10, "risk_abstention_sessions": 0,
                               "unresolved_sessions": 0} for scenario in scenarios
                },
                "purge_minutes": 40, "embargo_sessions": 1,
            },
            "checks": {
                "chronological_order": True, "purge_applied": True,
                "embargo_applied": True, "training_only_transformation": True,
                "contract_identity_discontinuities_terminalized": True,
                "roll_discontinuities_terminalized": True,
                "all_incomplete_sessions_terminalized": True,
                "complete_required_metrics": True, "promotion_path_computable": True,
            },
            "baseline_universe_readiness": {
                baseline: {
                    "expected_sessions": 63, "terminal_sessions": 63,
                    "selected_sessions": 0 if baseline == "flat_no_trade" else 10,
                    "selected_path_complete_sessions": 0 if baseline == "flat_no_trade" else 10,
                    "scenario_risk_dispositions": {
                        scenario: {
                            "feasible_sessions": 0 if baseline == "flat_no_trade" else 10,
                            "risk_abstention_sessions": 0, "unresolved_sessions": 0,
                        } for scenario in scenarios
                    },
                    "schedule_independently_derived": True,
                    "flat_no_trade": baseline == "flat_no_trade",
                } for baseline in MANDATORY_BASELINES
            },
            "exclusion_reasons": {},
            "market_year_breakdown": {
                "2018": {
                    "expected_training_sessions": 504, "complete_training_sessions": 504,
                    "expected_evaluation_sessions": 63,
                    "feature_complete_evaluation_sessions": 63,
                    "terminal_evaluation_sessions": 63,
                    "execution_path_complete_evaluation_sessions": 63,
                    "exclusion_reasons": {},
                },
            },
        })
    certificate = build_fold_readiness_certificate(
        trial_family="alpha_frozen", protocol_id=protocol_id,
        source_bindings=sources, fold_evidence=evidence,
        required_markets=markets, required_baselines=MANDATORY_BASELINES,
        required_cost_scenarios=scenarios, required_outer_fold_ids=("fold-0",),
        required_nested_fold_ids=(), expected_outer_folds=1, expected_nested_folds=0,
        minimum_training_sessions=504, minimum_evaluation_sessions=63,
        minimum_purge_minutes=40, minimum_embargo_sessions=1,
        evidence_class=ROW_CERTIFIED, historical_rows_opened=True,
    )
    relative = f"state/readiness/{name}.json"
    return relative, _write_json(root, relative, certificate)


def _pilot_manifest(contract_id: str, mechanism: str) -> dict[str, object]:
    training = [f"session-{index:04d}" for index in range(504)]
    evaluation = [f"session-{index:04d}" for index in range(504, 567)]
    core = {
        "schema_version": SESSION_MANIFEST_SCHEMA,
        "contract_id": contract_id,
        "mechanism_sha256": mechanism,
        "stage": "pilot",
        "markets": ["ES"],
        "fold_ordinal": 0,
        "selection_rule": "FIRST_ROW_CERTIFIED_EXECUTABLE_OUTER_FOLD",
        "training_session_ids": training,
        "evaluation_session_ids": evaluation,
        "purge_applied": True,
        "embargo_applied": True,
    }
    return _identified(core, "manifest_id")


def _stage_manifest(
    *, contract_id: str, mechanism: str, stage: str, markets: tuple[str, ...],
    pilot_sessions: list[str], reuse_pilot: bool = False,
) -> dict[str, object]:
    confirmation = [f"confirm-{index:04d}" for index in range(504)]
    if reuse_pilot:
        confirmation.append(pilot_sessions[0])
    core = {
        "schema_version": SESSION_MANIFEST_SCHEMA,
        "contract_id": contract_id,
        "mechanism_sha256": mechanism,
        "stage": stage,
        "excluded_pilot_evaluation_session_ids": pilot_sessions,
        "evaluation_session_ids_by_market": {
            market: confirmation for market in markets
        },
    }
    return _identified(core, "manifest_id")


def _registration_context(
    root: Path, *, stage: str, predecessor_stage: str, predecessor_decision: str = "PASS",
    mechanism: str = "a" * 64, reuse_pilot: bool = False,
) -> tuple[dict[str, object], dict[str, object]]:
    contract, profile = load_active_ladder(root)
    mechanism_path, mechanism, mechanism_id = _frozen_mechanism(root, contract, profile)
    contract_id = str(contract["contract_id"])
    stage_markets = {
        "pilot": ("ES",), "tier_1": CORE, "tier_2": BALANCED,
        "tier_3": ALL_APPROVED, "holdout": ALL_APPROVED, "forward": ALL_APPROVED,
    }[stage]
    if predecessor_stage == "tier_0":
        tier0 = build_tier0_certificate(
            contract_id=contract_id, profile_id=str(profile["profile_id"]),
            mechanism_id=mechanism_id, mechanism_sha256=mechanism,
            test_node_ids=("synthetic-suite",), passed_test_count=1,
        )
        tier0_path = "state/tier0/certificate.json"
        tier0_sha = _write_json(root, tier0_path, tier0)
        extras = {"synthetic_certificate_path": tier0_path,
                  "synthetic_certificate_sha256": tier0_sha}
    else:
        extras = {"promotion_evidence": _promotion_evidence(
            predecessor_stage,
            {"pilot": ("ES",), "tier_1": CORE, "tier_2": BALANCED,
             "tier_3": TRADITIONAL}[predecessor_stage],
        )} if predecessor_stage in {"pilot", "tier_1", "tier_2", "tier_3"} else {}
    predecessor = _decision(
        contract_id=contract_id, mechanism=mechanism,
        stage=predecessor_stage, decision=predecessor_decision,
        subgroup=(
            extras["promotion_evidence"]["subgroup_decisions"]
            if predecessor_stage == "tier_3" and predecessor_decision == "PASS"
            else None
        ),
        extras=extras,
    )
    predecessor_path = f"state/decisions/{stage}.json"
    predecessor_sha = _write_json(root, predecessor_path, predecessor)
    pilot = _pilot_manifest(contract_id, mechanism)
    pilot_sessions = list(pilot["evaluation_session_ids"])
    pilot_sha = sha256_json(pilot_sessions)
    if stage == "pilot":
        manifest = pilot
    else:
        manifest = _stage_manifest(
            contract_id=contract_id, mechanism=mechanism, stage=stage,
            markets=stage_markets, pilot_sessions=pilot_sessions,
            reuse_pilot=reuse_pilot,
        )
    manifest_path = f"state/session_manifests/{stage}.json"
    manifest_sha = _write_json(root, manifest_path, manifest)
    certificate: dict[str, object] = {
        "protocol_id": mechanism_id,
        "requirements": {
            "required_markets": list(stage_markets),
            "minimum_training_sessions": 504,
            "minimum_evaluation_sessions": 63,
            "minimum_purge_minutes": 40,
            "minimum_embargo_sessions": 1,
        },
        "source_bindings": {manifest_path: manifest_sha},
    }
    binding = {
        "contract_id": contract_id,
        "profile_id": profile["profile_id"],
        "stage": stage,
        "mechanism_sha256": mechanism,
        "mechanism_path": mechanism_path,
        "mechanism_id": mechanism_id,
        "predecessor_decision_path": predecessor_path,
        "predecessor_decision_sha256": predecessor_sha,
        "session_manifest_path": manifest_path,
        "session_manifest_sha256": manifest_sha,
        "pilot_evaluation_session_ids_sha256": pilot_sha,
    }
    if stage == "pilot":
        next_manifest = _stage_manifest(
            contract_id=contract_id, mechanism=mechanism, stage="tier_1",
            markets=CORE, pilot_sessions=pilot_sessions,
        )
        next_manifest_path = "state/session_manifests/tier_1_prechecked.json"
        next_manifest_sha = _write_json(root, next_manifest_path, next_manifest)
        next_path, next_sha = _row_certificate(
            root, markets=CORE, protocol_id=mechanism_id, name="tier1",
            additional_sources={next_manifest_path: next_manifest_sha},
        )
        binding["tier_1_readiness_evidence_path"] = next_path
        binding["tier_1_readiness_evidence_sha256"] = next_sha
        binding["tier_1_session_manifest_path"] = next_manifest_path
        binding["tier_1_session_manifest_sha256"] = next_manifest_sha
    registration: dict[str, object] = {
        "protocol_id": mechanism_id, "alpha_ladder_binding": binding,
    }
    if stage == "tier_2":
        registration["reporting"] = {
            "core_markets": list(CORE),
            "addition_markets": [market for market in BALANCED if market not in CORE],
            "report_separately": True,
        }
    if stage == "tier_3":
        registration["reporting"] = {
            "traditional_markets": list(TRADITIONAL),
            "satellite_markets": list(SATELLITE),
            "combined_markets": list(ALL_APPROVED),
            "traditional_must_pass_independently": True,
            "satellite_can_rescue_traditional_failure": False,
        }
    return registration, certificate


def test_successor_contract_is_exactly_nested_and_non_authorizing(tmp_path: Path) -> None:
    contract, _profile = _active_ladder(tmp_path)
    validated = validate_contract(contract)
    stages = validated["stages"]
    assert isinstance(stages, dict)
    assert stages["tier_0"]["historical_years"] == []
    assert stages["pilot"]["training_sessions"] == 504
    assert stages["pilot"]["evaluation_sessions"] == 63
    assert stages["tier_1"]["markets"] == list(CORE)
    assert stages["tier_2"]["markets"] == list(BALANCED)
    assert stages["tier_3"]["markets"] == list(ALL_APPROVED)
    assert set(CORE) < set(BALANCED) < set(ALL_APPROVED)


def test_active_pointer_fails_closed_after_bound_contract_change(tmp_path: Path) -> None:
    _active_ladder(tmp_path)
    contract_path = tmp_path / "state/alpha_ladder_registry/contract.json"
    contract_path.write_bytes(contract_path.read_bytes() + b" ")
    with pytest.raises(IntegrityError, match="bound artifact changed"):
        load_active_ladder(tmp_path)


def test_tier0_cannot_be_a_historical_registration(tmp_path: Path) -> None:
    _active_ladder(tmp_path)
    with pytest.raises(UnauthorizedOperation, match="no current Alpha ladder stage"):
        validate_stage_registration(
            {"alpha_ladder_binding": {"stage": "tier_0"}},
            certificate={}, root=tmp_path,
        )


def test_pilot_scope_and_locked_fold_fail_closed(tmp_path: Path) -> None:
    _active_ladder(tmp_path)
    registration, certificate = _registration_context(
        tmp_path, stage="pilot", predecessor_stage="tier_0",
    )
    changed_market = copy.deepcopy(certificate)
    changed_market["requirements"]["required_markets"] = ["CL"]
    with pytest.raises(UnauthorizedOperation, match="exact ladder market set"):
        validate_stage_registration(registration, certificate=changed_market, root=tmp_path)

    binding = registration["alpha_ladder_binding"]
    manifest_path = tmp_path / binding["session_manifest_path"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    core = dict(manifest)
    core.pop("manifest_id")
    core["training_session_ids"] = core["training_session_ids"][:-1]
    changed = _identified(core, "manifest_id")
    binding["session_manifest_sha256"] = _write_json(
        tmp_path, str(binding["session_manifest_path"]), changed,
    )
    certificate["source_bindings"][str(binding["session_manifest_path"])] = binding[
        "session_manifest_sha256"
    ]
    with pytest.raises(UnauthorizedOperation, match="504/63"):
        validate_stage_registration(registration, certificate=certificate, root=tmp_path)


def test_pilot_requires_purge_and_embargo(tmp_path: Path) -> None:
    contract, _profile = _active_ladder(tmp_path)
    manifest = _pilot_manifest(str(contract["contract_id"]), "a" * 64)
    core = dict(manifest)
    core.pop("manifest_id")
    core["embargo_applied"] = False
    with pytest.raises(UnauthorizedOperation, match="504/63"):
        validate_session_manifest(
            _identified(core, "manifest_id"), contract_id=str(contract["contract_id"]),
            mechanism_sha256="a" * 64, stage="pilot", markets=("ES",),
        )


def test_pilot_registration_requires_four_market_readiness(tmp_path: Path) -> None:
    _active_ladder(tmp_path)
    registration, certificate = _registration_context(
        tmp_path, stage="pilot", predecessor_stage="tier_0",
    )
    binding = registration["alpha_ladder_binding"]
    binding.pop("tier_1_readiness_evidence_path")
    with pytest.raises((IntegrityError, UnauthorizedOperation)):
        validate_stage_registration(registration, certificate=certificate, root=tmp_path)

    registration, certificate = _registration_context(
        tmp_path, stage="pilot", predecessor_stage="tier_0",
    )
    wrong_path, wrong_sha = _row_certificate(
        tmp_path, markets=("ES",), protocol_id=str(registration["protocol_id"]),
        name="wrong-tier1",
    )
    binding = registration["alpha_ladder_binding"]
    binding["tier_1_readiness_evidence_path"] = wrong_path
    binding["tier_1_readiness_evidence_sha256"] = wrong_sha
    with pytest.raises(UnauthorizedOperation, match="four-market Tier 1"):
        validate_stage_registration(registration, certificate=certificate, root=tmp_path)


def test_frozen_mechanism_file_drift_blocks_registration(tmp_path: Path) -> None:
    _active_ladder(tmp_path)
    registration, certificate = _registration_context(
        tmp_path, stage="tier_1", predecessor_stage="pilot",
    )
    binding = registration["alpha_ladder_binding"]
    path = tmp_path / binding["mechanism_path"]
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(IntegrityError, match="bound artifact changed"):
        validate_stage_registration(registration, certificate=certificate, root=tmp_path)


def test_pilot_sessions_cannot_reappear_in_confirmation(tmp_path: Path) -> None:
    _active_ladder(tmp_path)
    registration, certificate = _registration_context(
        tmp_path, stage="tier_1", predecessor_stage="pilot", reuse_pilot=True,
    )
    with pytest.raises(UnauthorizedOperation, match="reused"):
        validate_stage_registration(registration, certificate=certificate, root=tmp_path)


def test_mechanism_change_or_failed_predecessor_blocks_progression(tmp_path: Path) -> None:
    contract, _profile = _active_ladder(tmp_path)
    with pytest.raises(UnauthorizedOperation, match="did not pass"):
        validate_stage_decision(
            _decision(
                contract_id=str(contract["contract_id"]), mechanism="b" * 64,
                stage="tier_1",
            ),
            contract_id=str(contract["contract_id"]), mechanism_sha256="a" * 64,
            expected_stage="tier_1",
        )
    registration, certificate = _registration_context(
        tmp_path, stage="tier_2", predecessor_stage="tier_1",
        predecessor_decision="FAIL",
    )
    with pytest.raises(UnauthorizedOperation, match="did not pass"):
        validate_stage_registration(registration, certificate=certificate, root=tmp_path)


def test_satellites_cannot_rescue_a_failed_traditional_tier3(tmp_path: Path) -> None:
    contract, _profile = _active_ladder(tmp_path)
    decision = _decision(
        contract_id=str(contract["contract_id"]), mechanism="a" * 64,
        stage="tier_3", subgroup={
            "traditional": "FAIL", "satellite": "PASS", "combined": "PASS",
            "satellite_can_rescue_traditional_failure": False,
        },
        extras={"promotion_evidence": _promotion_evidence("tier_3", TRADITIONAL)},
    )
    with pytest.raises(UnauthorizedOperation, match="traditional subgroup"):
        validate_stage_decision(
            decision, contract_id=str(contract["contract_id"]),
            mechanism_sha256="a" * 64, expected_stage="tier_3",
        )


def test_tier3_registration_requires_complete_38_3_reporting(tmp_path: Path) -> None:
    _active_ladder(tmp_path)
    registration, certificate = _registration_context(
        tmp_path, stage="tier_3", predecessor_stage="tier_2",
    )
    registration.pop("reporting")
    with pytest.raises((IntegrityError, UnauthorizedOperation), match="reporting"):
        validate_stage_registration(registration, certificate=certificate, root=tmp_path)


def test_tier2_registration_requires_separate_core_and_addition_reporting(
    tmp_path: Path,
) -> None:
    _active_ladder(tmp_path)
    registration, certificate = _registration_context(
        tmp_path, stage="tier_2", predecessor_stage="tier_1",
    )
    registration.pop("reporting")
    with pytest.raises((IntegrityError, UnauthorizedOperation), match="reporting"):
        validate_stage_registration(registration, certificate=certificate, root=tmp_path)


def test_project_level_holdout_claim_is_single_use(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    gateway = CertifiedResearchGateway(tmp_path, RepoBoundary(tmp_path))
    expected = {
        "alpha_ladder_stage": "holdout",
        "trial_id": "1" * 64,
        "alpha_ladder_contract_id": "2" * 64,
        "mechanism_sha256": "3" * 64,
    }
    monkeypatch.setattr(
        CertifiedResearchGateway, "execution_scope", lambda self, **kwargs: expected,
    )

    class Receipt:
        scope = tuple(sorted(expected.items()))
        receipt_id = "4" * 64

        def consume(self, *args, **kwargs):
            path = tmp_path / "state/authorization_uses" / f"{self.receipt_id}.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("used", encoding="utf-8")
            return path

    receipt = Receipt()
    result = gateway.claim_historical_execution(
        registration_path=tmp_path / "unused.json",
        expected_registration_sha256="5" * 64,
        receipt=receipt,  # type: ignore[arg-type]
    )
    assert result.exists()
    assert (tmp_path / "state/alpha_ladder_holdout_claims/2025.json").exists()
    with pytest.raises(UnauthorizedOperation, match="already claimed"):
        gateway.claim_historical_execution(
            registration_path=tmp_path / "unused.json",
            expected_registration_sha256="5" * 64,
            receipt=receipt,  # type: ignore[arg-type]
        )


def test_every_stage_accepts_only_the_exact_successful_progression(tmp_path: Path) -> None:
    _active_ladder(tmp_path)
    for stage, predecessor in (
        ("pilot", "tier_0"), ("tier_1", "pilot"), ("tier_2", "tier_1"),
        ("tier_3", "tier_2"), ("holdout", "tier_3"), ("forward", "holdout"),
    ):
        registration, certificate = _registration_context(
            tmp_path, stage=stage, predecessor_stage=predecessor,
        )
        result = validate_stage_registration(
            registration, certificate=certificate, root=tmp_path,
        )
        assert result["alpha_ladder_stage"] == stage
        assert len(result["mechanism_sha256"]) == 64
