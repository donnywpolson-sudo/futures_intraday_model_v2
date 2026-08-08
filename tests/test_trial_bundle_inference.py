import inspect
import json
import math
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from futures_rebuild.boundary import (
    OperationClassification,
    OperationReceipt,
)
from futures_rebuild.bundle import (
    BundleClassification,
    BundleMetadata,
    _authorization_scope,
    seal_bundle,
    verify_bundle,
)
from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.clock import SyntheticClock, issue_production_clock
from futures_rebuild.economics import VerifiedEconomicsRegistry
from futures_rebuild.errors import ContractError, IntegrityError, UnauthorizedOperation
from futures_rebuild.identity import ActualContractIdentity
from futures_rebuild.legacy_trial_census import publish_legacy_trial_census
from futures_rebuild.inference import (
    InferenceAdapter,
    InferencePolicy,
    VerifiedIdentityRegistry,
)
from futures_rebuild.predictor import (
    TrustedPredictorLoader,
    trusted_loader_code_hash,
    trusted_dependency_lock_hash,
    trusted_runtime_code_hash,
    trusted_runtime_config_hash,
)
from futures_rebuild.schemas import FeatureLineage, FeatureRow
from futures_rebuild.time_contracts import AvailabilityBasis
from futures_rebuild.trial import (
    EvaluationClassification,
    EvaluationFirewall,
    EvaluationReleaseRole,
    ExperimentCharter,
    LegacyCensusReceipt,
    TrialEventLedger,
    TrialRegistry,
)
from tests.test_legacy_trial_census import (
    _archive_receipt as _legacy_archive_receipt,
    _publisher as _legacy_publisher,
    _snapshot as _legacy_census_snapshot,
)


def inference_policy():
    return InferencePolicy(300, 0.0001, 0.1, 0.5)


def _definition_payload(decision):
    return {
        "schema_version": "1.0.0",
        "records": [
            {
                "available_at": (decision - timedelta(days=1)).isoformat(),
                "currency": "USD",
                "dataset": "GLBX.MDP3",
                "effective_at": (decision - timedelta(days=2)).isoformat(),
                "exchange": "XCME",
                "instrument_id": 12345,
                "min_tick": "0.25",
                "multiplier": "50",
                "publisher_id": 1,
                "raw_symbol": "ESZ6",
                "source_received_at": (
                    decision - timedelta(days=1, seconds=1)
                ).isoformat(),
            }
        ],
    }


def _economics_payload(actual, decision):
    return {
        "schema_version": "1.0.0",
        "records": [
            {
                "actual_identity_hash": actual.identity_hash,
                "ambiguity_reasons": [],
                "asset_class": "EQUITY_INDEX",
                "available_at": (decision - timedelta(hours=1)).isoformat(),
                "currency": "USD",
                "effective_at": (decision - timedelta(days=1)).isoformat(),
                "point_value": "50",
                "quote_convention_id": "INDEX_POINTS",
                "source_fields_used": ["min_price_increment", "unit_of_measure_qty"],
                "source_received_at": (decision - timedelta(hours=2)).isoformat(),
                "tick_size": "0.25",
                "tick_value": "12.50",
                "verification_source_ids": ["cme_contract_spec"],
            }
        ],
    }


def _artifact(*, parity_shift=0.0):
    score = 1.0
    up = 1.0 / (1.0 + math.exp(-score))
    return {
        "artifact_format": "FUTURES_TRUSTED_LINEAR_FORECAST_V1",
        "expected_return_scale": 0.002,
        "feature_names": ["momentum"],
        "intercept": 0.0,
        "parity_input": [1.0],
        "parity_output": {
            "expected_return": 0.002 + parity_shift,
            "probability_down": 1.0 - up,
            "probability_neutral": 0.0,
            "probability_up": up,
            "uncertainty": 0.01,
        },
        "uncertainty": 0.01,
        "weights": [1.0],
    }


def build_inference_system(
    boundary,
    operation_factory,
    release_factory,
    decision,
    *,
    parity_shift=0.0,
    classification=BundleClassification.SYNTHETIC_MECHANICS_ONLY,
    infer_scope=None,
):
    environment = boundary.active_root / "configs" / "environment.lock.json"
    runtime_config = boundary.active_root / "configs" / "inference_runtime.json"
    dependency_receipt = boundary.active_root / "configs" / "dependency_lock_receipt.json"
    dependencies = boundary.active_root / "requirements.lock"
    environment.write_text('{"synthetic":true}', encoding="utf-8")
    runtime_config.write_text('{"synthetic":true}', encoding="utf-8")
    dependencies.write_text("synthetic==1\n", encoding="utf-8")
    dependency_receipt.write_text(
        json.dumps(
            {"requirements_sha256": sha256_file(dependencies), "synthetic": True},
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    _, definition_receipt = release_factory(
        release_kind="actual_contract_definitions",
        filename="identities.json",
        content=_definition_payload(decision),
    )
    identities = VerifiedIdentityRegistry.from_release(definition_receipt, boundary)
    definition = next(iter(identities.definitions.values())).definition
    actual = ActualContractIdentity.from_definition(
        definition,
        instrument_id_date_utc=date(2026, 7, 14),
        exchange_session_date=date(2026, 7, 14),
    )
    _, economics_receipt = release_factory(
        release_kind="actual_contract_economics",
        filename="contract_economics.json",
        content=_economics_payload(actual, decision),
    )
    economics = VerifiedEconomicsRegistry.from_release(economics_receipt, boundary)
    _, training_receipt = release_factory(
        release_kind="synthetic_training",
        filename="rows.bin",
        content=b"training",
    )
    _, raw_receipt = release_factory(
        release_kind="futures_phase2_causal_interval",
        filename="rows.bin",
        content=b"raw",
    )
    feature_release, feature_receipt = release_factory(
        release_kind="feature_release",
        filename="rows.bin",
        content=b"features",
    )
    policy = inference_policy()
    names = ("momentum",)
    metadata = BundleMetadata(
        bundle_classification=classification,
        feature_names=names,
        feature_schema_hash=sha256_json({"feature_names": list(names)}),
        preprocessing_hash="1" * 64,
        calibration_hash="2" * 64,
        decision_policy_hash=policy.policy_hash,
        monitoring_policy_hash="8" * 64,
        monitoring_reference_hash="9" * 64,
        training_release_receipts=(training_receipt,),
        inference_source_release_receipts=(feature_receipt,),
        definition_release_receipts=(definition_receipt,),
        economics_release_receipts=(economics_receipt,),
        training_cutoff=decision,
        loader_code_hash=trusted_loader_code_hash(),
        code_hash=trusted_runtime_code_hash(),
        config_hash=trusted_runtime_config_hash(boundary),
        environment_hash=sha256_file(environment),
        dependency_lock_hash=trusted_dependency_lock_hash(boundary),
    )
    artifact = boundary.active_root / "state" / "artifacts" / "model.json"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text(
        json.dumps(_artifact(parity_shift=parity_shift), sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    artifact_hash = sha256_file(artifact)
    scope = _authorization_scope(artifact_hash, metadata)
    seal_receipt = operation_factory("SEAL_SYNTHETIC_BUNDLE", scope=scope)
    if classification is BundleClassification.CANDIDATE:
        # Intentionally retain a local synthetic receipt so candidate sealing must fail.
        return {
            "actual": actual,
            "artifact": artifact,
            "economics": economics,
            "feature_receipt": feature_receipt,
            "identities": identities,
            "metadata": metadata,
            "raw_receipt": raw_receipt,
            "seal_receipt": seal_receipt,
        }
    bundle = seal_bundle(
        artifact,
        boundary.active_root / "bundles" / "sealed",
        boundary.active_root / "state" / "locks" / "bundle.lock",
        metadata,
        boundary=boundary,
        operation_receipt=seal_receipt,
    )
    predictor = TrustedPredictorLoader.load(bundle, boundary=boundary)
    infer_receipt = operation_factory("INFER", scope=infer_scope)
    clock = SyntheticClock(boundary, infer_receipt, decision)
    adapter = InferenceAdapter(
        bundle_path=bundle,
        policy=policy,
        identity_registry=identities,
        economics_registry=economics,
        predictor=predictor,
        clock=clock,
        boundary=boundary,
        operation_receipt=infer_receipt,
    )
    row = FeatureRow(
        actual=actual,
        bar_event_at=decision - timedelta(minutes=1),
        decision_at=decision,
        available_at_max=decision,
        source_release_id=feature_receipt.release_id,
        allowed_upstream_release_ids=(raw_receipt.release_id,),
        verified_release_receipts=tuple(
            sorted((feature_receipt, raw_receipt), key=lambda item: item.release_id)
        ),
        boundary=boundary,
        values={"momentum": 1.0},
        lineage={
            "momentum": FeatureLineage(
                raw_receipt.release_id,
                decision,
                "5" * 64,
                AvailabilityBasis.DERIVED_FROM_VERIFIED_UPSTREAM,
                "6" * 64,
                decision,
                actual.contract_segment_hash,
            )
        },
        inputs_complete=True,
        planned_entry_at=decision + timedelta(minutes=10),
        label_unlock_at=decision + timedelta(days=1),
    )
    return {
        "actual": actual,
        "adapter": adapter,
        "artifact": artifact,
        "bundle": bundle,
        "clock": clock,
        "economics": economics,
        "feature_release": feature_release,
        "identities": identities,
        "metadata": metadata,
        "predictor": predictor,
        "row": row,
    }


def test_production_inference_has_no_timestamp_override_and_late_row_abstains(
    boundary, operation_factory, release_factory, decision
) -> None:
    system = build_inference_system(boundary, operation_factory, release_factory, decision)
    assert "observed_at" not in inspect.signature(system["adapter"].infer).parameters
    with pytest.raises(TypeError):
        system["adapter"].infer(system["row"], observed_at=decision)  # type: ignore[call-arg]
    system["clock"].set(decision + timedelta(minutes=11))
    result = system["adapter"].infer(system["row"])
    assert result.abstained and "ENTRY_WINDOW_CLOSED" in result.abstention_reasons
    assert result.recorded_at == decision + timedelta(minutes=11)


def test_arbitrary_predictor_is_rejected_even_with_matching_strings(
    boundary, operation_factory, release_factory, decision
) -> None:
    system = build_inference_system(boundary, operation_factory, release_factory, decision)

    class Liar:
        artifact_sha256 = sha256_file(system["bundle"] / "model.artifact")
        bundle_id = system["bundle"].name
        environment_hash = system["metadata"].environment_hash

        def predict_one(self, features):
            raise AssertionError("arbitrary predictor must not run")

    receipt = operation_factory("INFER")
    with pytest.raises(ContractError, match="arbitrary Python"):
        InferenceAdapter(
            bundle_path=system["bundle"],
            policy=inference_policy(),
            identity_registry=system["identities"],
            economics_registry=system["economics"],
            predictor=Liar(),  # type: ignore[arg-type]
            clock=SyntheticClock(boundary, receipt, decision),
            boundary=boundary,
            operation_receipt=receipt,
        )


def test_synthetic_inference_rejects_noncanonical_authorization_scope(
    boundary, operation_factory, release_factory, decision
) -> None:
    with pytest.raises(UnauthorizedOperation, match="exact required scope"):
        build_inference_system(
            boundary,
            operation_factory,
            release_factory,
            decision,
            infer_scope={"unexpected": "scope"},
        )


def test_trusted_loader_parity_and_release_tamper_fail_before_prediction(
    boundary, operation_factory, release_factory, decision
) -> None:
    system = build_inference_system(boundary, operation_factory, release_factory, decision)
    result = system["adapter"].infer(system["row"])
    assert not result.abstained and result.expected_return == pytest.approx(0.002)
    assert result.bundle_classification == "SYNTHETIC_MECHANICS_ONLY"
    assert result.candidate_provenance_id is None
    assert result.production_eligible is False
    (system["feature_release"] / "rows.bin").write_bytes(b"tampered")
    with pytest.raises(IntegrityError):
        system["adapter"].infer(system["row"])


def test_reload_parity_mismatch_and_artifact_tamper_fail(
    boundary, operation_factory, release_factory, decision
) -> None:
    with pytest.raises(IntegrityError, match="parity"):
        build_inference_system(
            boundary, operation_factory, release_factory, decision, parity_shift=0.01
        )
    # Use a separate test root fixture invocation state by rebuilding valid releases.
    system = build_inference_system(boundary, operation_factory, release_factory, decision)
    (system["bundle"] / "model.artifact").write_text("{}", encoding="utf-8")
    with pytest.raises(IntegrityError):
        verify_bundle(system["bundle"], boundary=boundary)


def test_loaded_predictor_is_immutable_and_runtime_config_is_reverified(
    boundary, operation_factory, release_factory, decision
) -> None:
    system = build_inference_system(boundary, operation_factory, release_factory, decision)
    with pytest.raises(AttributeError, match="immutable"):
        system["predictor"]._weights = (999.0,)
    (boundary.active_root / "configs" / "inference_runtime.json").write_text(
        '{"tampered":true}', encoding="utf-8"
    )
    with pytest.raises(IntegrityError, match="config hash"):
        TrustedPredictorLoader.load(system["bundle"], boundary=boundary)


def test_candidate_sealing_requires_exact_external_authorization(
    boundary, operation_factory, release_factory, decision
) -> None:
    system = build_inference_system(
        boundary,
        operation_factory,
        release_factory,
        decision,
        classification=BundleClassification.CANDIDATE,
    )
    with pytest.raises(UnauthorizedOperation):
        seal_bundle(
            system["artifact"],
            boundary.active_root / "bundles" / "candidate",
            boundary.active_root / "state" / "locks" / "candidate.lock",
            system["metadata"],
            boundary=boundary,
            operation_receipt=system["seal_receipt"],
        )


def _census(boundary, operation_factory, monkeypatch):
    snapshot, _ = _legacy_census_snapshot(
        boundary=boundary, monkeypatch=monkeypatch
    )
    archive_receipt = _legacy_archive_receipt(
        boundary, operation_factory, snapshot
    )
    receipt = publish_legacy_trial_census(
        snapshot=snapshot,
        source_archive_receipt=archive_receipt,
        boundary=boundary,
        publisher=_legacy_publisher(boundary, operation_factory),
    )
    return LegacyCensusReceipt.from_release(receipt, boundary)


def test_legacy_census_is_explicitly_unresolved_and_non_authorizing(
    boundary, operation_factory, monkeypatch
) -> None:
    census = _census(boundary, operation_factory, monkeypatch)
    assert census.status == "INVALID_TRIAL_CENSUS_UNRESOLVED"
    assert census.exact_count_state == "INDETERMINATE"
    assert census.preregistered_penalty_count == 0
    assert census.trusted_gate is False


def _charter(
    receipts,
    *,
    feature_hash="1" * 64,
    real=False,
    economics=(),
    outcome_unlock_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
):
    ordered = tuple(sorted(receipts, key=lambda item: item.release_id))
    economics_ids = set(economics)
    data_receipts = [
        receipt for receipt in ordered if receipt.receipt_id not in economics_ids
    ]
    real_roles = (
        EvaluationReleaseRole.TRAINING,
        EvaluationReleaseRole.INNER_VALIDATION,
        EvaluationReleaseRole.OUTER_SCREEN,
        EvaluationReleaseRole.FINAL_HOLDOUT,
    )
    role_by_release = {}
    for index, receipt in enumerate(data_receipts):
        role_by_release[receipt.release_id] = (
            real_roles[index]
            if real and len(data_receipts) == len(real_roles)
            else (
                EvaluationReleaseRole.OUTER_SCREEN
                if real
                else EvaluationReleaseRole.SYNTHETIC_MECHANICS
            )
        )
    for receipt in ordered:
        if receipt.receipt_id in economics_ids:
            role_by_release[receipt.release_id] = EvaluationReleaseRole.ECONOMICS
    return ExperimentCharter(
        hypothesis_id="synthetic_mechanics" if not real else "historical_discovery",
        data_release_receipts=ordered,
        release_role_bindings=tuple(
            (receipt.release_id, role_by_release[receipt.release_id])
            for receipt in ordered
        ),
        economics_release_receipt_ids=tuple(sorted(economics)),
        feature_policy_hash=feature_hash,
        target_policy_hash="2" * 64,
        decision_rule_hash="3" * 64,
        fold_policy_hash="4" * 64,
        cost_policy_hash="5" * 64,
        primary_metric="synthetic_invariant" if not real else "net_return",
        benchmark_id="mechanical_zero",
        minimum_effect=0.001,
        minimum_effect_unit="NET_RETURN_PER_SESSION",
        multiplicity_family_id="family_a",
        multiplicity_family_rule_hash="6" * 64,
        holdout_policy_hash="7" * 64,
        robustness_policy_hash="8" * 64,
        outcome_unlock_at=outcome_unlock_at,
        evaluation_classification=(
            EvaluationClassification.REAL_HISTORY_DISCOVERY
            if real
            else EvaluationClassification.SYNTHETIC_MECHANICS_ONLY
        ),
    )


def _registry(boundary, operation_factory, census):
    receipt = operation_factory(
        "REGISTER_TRIAL",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
    )
    clock = issue_production_clock(boundary, receipt)
    events = TrialEventLedger(
        boundary.active_root / "state" / "trial_events",
        boundary.active_root / "state" / "locks" / "trial_events.lock",
        boundary=boundary,
        operation_receipt=receipt,
        clock=clock,
    )
    registry = TrialRegistry(
        boundary.active_root / "state" / "trial_registry",
        boundary.active_root / "manifests" / "data_releases",
        event_ledger=events,
        census=census,
        boundary=boundary,
        operation_receipt=receipt,
    )
    return registry, events


def _trial_economics_payload():
    return {
        "schema_version": "1.0.0",
        "records": [
            {
                "actual_identity_hash": "a" * 64,
                "ambiguity_reasons": [],
                "asset_class": "EQUITY_INDEX",
                "available_at": "2026-01-03T00:00:00+00:00",
                "currency": "USD",
                "effective_at": "2026-01-01T00:00:00+00:00",
                "point_value": "50",
                "quote_convention_id": "INDEX_POINTS",
                "source_fields_used": ["contract_multiplier", "min_price_increment"],
                "source_received_at": "2026-01-02T00:00:00+00:00",
                "tick_size": "0.25",
                "tick_value": "12.50",
                "verification_source_ids": ["cme_contract_spec"],
            }
        ],
    }


def test_global_trial_ledger_assigns_counts_and_semantic_changes_increment(
    boundary, operation_factory, release_factory, monkeypatch
) -> None:
    _, evaluation = release_factory(
        release_kind="historical_evaluation", filename="rows.bin", content=b"history"
    )
    _, economics = release_factory(
        release_kind="actual_contract_economics",
        filename="contract_economics.json",
        content=_trial_economics_payload(),
    )
    census = _census(boundary, operation_factory, monkeypatch)
    registry, events = _registry(boundary, operation_factory, census)
    first = _charter(
        (evaluation, economics), real=True, economics=(economics.receipt_id,)
    )
    second = _charter(
        (evaluation, economics), feature_hash="8" * 64, real=True,
        economics=(economics.receipt_id,),
    )
    first_path = registry.register(first)
    second_path = registry.register(second)
    first_record = json.loads(first_path.read_text(encoding="utf-8"))
    second_record = json.loads(second_path.read_text(encoding="utf-8"))
    assert first_record["counted_trial_number"] == 1
    assert second_record["counted_trial_number"] == 2
    declarations = [item for item in events.events() if item["event_type"] == "DECLARED"]
    assert [item["counted_trial_number"] for item in declarations] == [1, 2]
    assert "trial_number" not in first.core() and "trusted" not in first.core()


def test_unresolved_census_and_missing_preoutcome_anchor_block_real_history(
    boundary, operation_factory, release_factory, monkeypatch
) -> None:
    _, evaluation = release_factory(
        release_kind="historical_evaluation", filename="rows.bin", content=b"history"
    )
    _, economics = release_factory(
        release_kind="actual_contract_economics",
        filename="contract_economics.json",
        content=_trial_economics_payload(),
    )
    real = _charter(
        (evaluation, economics), real=True, economics=(economics.receipt_id,)
    )
    unresolved = _census(boundary, operation_factory, monkeypatch)
    registry, _ = _registry(boundary, operation_factory, unresolved)
    registry.register(real)
    with pytest.raises(UnauthorizedOperation, match="UNRESOLVED"):
        registry.permit(real.charter_id)


def test_real_history_declaration_after_outcome_unlock_is_rejected(
    boundary, operation_factory, release_factory, monkeypatch
) -> None:
    _, evaluation = release_factory(
        release_kind="historical_evaluation", filename="rows.bin", content=b"history"
    )
    _, economics = release_factory(
        release_kind="actual_contract_economics",
        filename="contract_economics.json",
        content=_trial_economics_payload(),
    )
    census = _census(boundary, operation_factory, monkeypatch)
    registry, events = _registry(boundary, operation_factory, census)
    real = _charter(
        (evaluation, economics),
        real=True,
        economics=(economics.receipt_id,),
        outcome_unlock_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    with pytest.raises(UnauthorizedOperation, match="pre-outcome window"):
        registry.register(real)
    assert events.events() == ()


def test_unresolved_census_blocks_before_external_authority_can_be_considered(
    boundary, operation_factory, release_factory, monkeypatch
) -> None:
    evaluations = []
    for role_index in range(4):
        _, evaluation = release_factory(
            release_kind="historical_evaluation",
            filename="rows.bin",
            content=f"history-{role_index}".encode("ascii"),
        )
        evaluations.append(evaluation)
    _, economics = release_factory(
        release_kind="actual_contract_economics",
        filename="contract_economics.json",
        content=_trial_economics_payload(),
    )
    census = _census(boundary, operation_factory, monkeypatch)
    registry, events = _registry(boundary, operation_factory, census)
    real = _charter(
        (*evaluations, economics), real=True, economics=(economics.receipt_id,)
    )
    registry.register(real)
    with pytest.raises(UnauthorizedOperation, match="INVALID_TRIAL_CENSUS_UNRESOLVED"):
        registry.permit(real.charter_id)
    assert all(event["event_type"] != "PRE_OUTCOME_ANCHORED" for event in events.events())


def test_real_history_registration_rejects_economics_evidence_before_counting(
    boundary, operation_factory, release_factory, monkeypatch
) -> None:
    _, evaluation = release_factory(
        release_kind="historical_evaluation", filename="rows.bin", content=b"history"
    )
    _, evidence = release_factory(
        release_kind="migration_evidence",
        filename="costs.yaml",
        content="ES:\n  tick_value: 12.5\n",
    )
    census = _census(boundary, operation_factory, monkeypatch)
    registry, events = _registry(boundary, operation_factory, census)
    real = _charter(
        (evaluation, evidence), real=True, economics=(evidence.receipt_id,)
    )
    with pytest.raises(IntegrityError, match="wrong release kind"):
        registry.register(real)
    assert events.events() == ()


def test_synthetic_permit_uses_verified_release_and_firewall(
    boundary, operation_factory, release_factory, monkeypatch
) -> None:
    release, evaluation = release_factory(
        release_kind="synthetic_evaluation", filename="rows.bin", content=b"synthetic"
    )
    census = _census(boundary, operation_factory, monkeypatch)
    registry, _ = _registry(boundary, operation_factory, census)
    charter = _charter((evaluation,))
    registry.register(charter)
    permit = registry.permit(charter.charter_id)
    assert EvaluationFirewall.assert_input(
        permit,
        evaluation.release_id,
        "rows.bin",
        boundary=boundary,
        registry=registry,
        required_role=EvaluationReleaseRole.SYNTHETIC_MECHANICS,
    ) == release / "rows.bin"
    with pytest.raises(IntegrityError, match="forged|stale"):
        EvaluationFirewall.assert_input(
            replace(permit, issuer_mac="0" * 64),
            evaluation.release_id,
            "rows.bin",
            boundary=boundary,
            registry=registry,
            required_role=EvaluationReleaseRole.SYNTHETIC_MECHANICS,
        )
    changed = _charter((evaluation,), feature_hash="8" * 64)
    registry.register(changed)
    with pytest.raises(IntegrityError, match="forged|stale"):
        EvaluationFirewall.assert_input(
            permit,
            evaluation.release_id,
            "rows.bin",
            boundary=boundary,
            registry=registry,
            required_role=EvaluationReleaseRole.SYNTHETIC_MECHANICS,
        )
    permit = registry.permit(charter.charter_id)
    (release / "rows.bin").write_bytes(b"tampered")
    with pytest.raises(IntegrityError):
        EvaluationFirewall.assert_input(
            permit,
            evaluation.release_id,
            "rows.bin",
            boundary=boundary,
            registry=registry,
            required_role=EvaluationReleaseRole.SYNTHETIC_MECHANICS,
        )


def test_trial_tail_and_canonical_path_substitution_fail_closed(
    boundary, operation_factory, release_factory, monkeypatch
) -> None:
    _, evaluation = release_factory(
        release_kind="synthetic_evaluation", filename="rows.bin", content=b"synthetic"
    )
    census = _census(boundary, operation_factory, monkeypatch)
    registry, events = _registry(boundary, operation_factory, census)
    charter = _charter((evaluation,))
    registry.register(charter)
    event_path = next(events.root.glob("*.json"))
    event_path.unlink()
    with pytest.raises(IntegrityError, match="persistent head"):
        registry.permit(charter.charter_id)

    receipt = operation_factory(
        "REGISTER_TRIAL",
        classification=OperationClassification.CONTROLLED_REBUILD_NON_ALPHA,
    )
    with pytest.raises(UnauthorizedOperation, match="canonical"):
        TrialEventLedger(
            boundary.active_root / "state" / "alternate_trial_events",
            boundary.active_root / "state" / "locks" / "trial_events.lock",
            boundary=boundary,
            operation_receipt=receipt,
            clock=issue_production_clock(boundary, receipt),
        )
