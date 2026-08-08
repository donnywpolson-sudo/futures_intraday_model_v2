from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from futures_rebuild import tier1_bracket_v5 as v5
from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.tier1_bracket_post_audit import CausalBar
from futures_rebuild.tier1_bracket_v4 import ExpectedCheckpoint, FrozenPrediction, MarketSpec
from futures_rebuild.tier1_final_decision_validity import (
    derive_final_decision, plan_final_strategy,
)
from futures_rebuild.tier1_final_execution import (
    OPERATION, OUTPUT_ROOT, _required_scope, claim_final_execution,
    load_final_execution_plan, load_final_registered_context,
)
from futures_rebuild.tier1_final_lifecycle import (
    PREDECESSOR_POINTER_SHA256, prepare_final_lifecycle,
)
from futures_rebuild.tier1_final_unpublished_evidence import (
    REQUIRED_PAYLOADS, stage_unpublished_evidence, verify_unpublished_evidence,
)
from futures_rebuild.tier1_final_pipeline import run_final_trial_pipeline
from futures_rebuild.tier1_final_protocol import (
    load_final_effective_contract, load_final_trial_protocol,
    load_invalid_closure_preparation,
)
from futures_rebuild.tier1_frozen_trial_pipeline import materialize_reported_bar_rows
from tests.test_tier1_frozen_source_adequacy import _checkpoint
from tests.test_tier1_frozen_source_semantics import _rows


ROOT = Path(__file__).resolve().parents[1]


def _contract() -> dict[str, object]:
    from futures_rebuild.tier1_standard_only_protocol import load_standard_only_protocol

    return deepcopy(load_standard_only_protocol(root=ROOT))


def _prediction(opportunity_id: str) -> FrozenPrediction:
    return FrozenPrediction(
        opportunity_id, "ES", 2020, "2020-01-02", "08:30", 0,
        0.5, -0.1, "long", 0.5, "long", 0.2, 0.01,
    )


def _path(*, ending: str, drawdown: str, complete: bool = True) -> v5.AccountPathV5:
    return v5.AccountPathV5(
        "candidate", (), {}, (), {}, Decimal(ending), Decimal(drawdown), complete,
    )


def test_extreme_cost_risk_cap_is_policy_abstention_not_missing_path() -> None:
    contract = _contract()
    contract["costs"]["round_trip_adverse_execution_ticks"]["extreme"]["ES"] = 100  # type: ignore[index]
    offsets = [
        *[offset for offset in range(-64, -1) if offset not in {-30, -20}],
        1, 2, 5, 20, 40, 61,
    ]
    source = _rows(offsets)
    rows, resolutions = materialize_reported_bar_rows(
        source_rows=source,
        census=(_checkpoint(),),
        market_specs={"ES": source[0].market_spec},
        contract=contract,
        prediction_scope_sessions=("2020-01-02",),
    )
    prediction = _prediction(rows[0].expected.opportunity_id)
    plan = plan_final_strategy(
        strategy="candidate", predictions=(prediction,), rows=rows,
        scenario="extreme", resolutions=resolutions, contract=contract,
    )
    assert plan.trades == ()
    assert plan.preliminary_terminals[prediction.opportunity_id] == (
        "RISK_CAP_REJECTION"
    )
    assert "MISSING_PRICE_PATH" not in plan.preliminary_terminals.values()


def test_final_protocol_preserves_parameters_and_closes_predecessor_only() -> None:
    closure = load_invalid_closure_preparation(root=ROOT)
    protocol = load_final_trial_protocol(root=ROOT)
    effective = load_final_effective_contract(root=ROOT)
    assert closure["state"] == "PREPARED_NOT_PUBLISHED"
    assert protocol["state"] == "PREPARED_NOT_REGISTERED"
    assert protocol["lineage"]["new_numbered_version_created"] is False
    assert protocol["inherited_research_specification"]["parameter_changes"] == []
    assert effective["protocol_id"] == protocol["lineage"]["predecessor_protocol_id"]


def test_final_lifecycle_is_prepared_without_publication_or_pointer_mutation() -> None:
    prepared = prepare_final_lifecycle(root=ROOT)
    assert prepared.trial["supersedes_invalid_trial_id"] == load_invalid_closure_preparation(root=ROOT)["trial_id"]
    assert prepared.certificate["overall_decision"] == "PASS"
    assert prepared.certificate["durable_unpublished_evidence_required_before_terminal_report"] is True
    assert prepared.pointer["trial_id"] == prepared.trial_id
    assert prepared.trial["source_row_access"] is False
    assert not (ROOT / "state/trial_registry/tier1_final_trial" / f"{prepared.trial_id}.json").exists()
    assert sha256_file(ROOT / "configs/active_tier1_trial.json") == PREDECESSOR_POINTER_SHA256


def test_final_execution_plan_requires_registration_and_durable_staging() -> None:
    plan = load_final_execution_plan(root=ROOT)
    assert plan["success_requires_verified_unpublished_bundle"] is True
    assert plan["execution_mode"] == (
        "CREATE_ONLY_SEALED_UNPUBLISHED_EVIDENCE_AND_TERMINAL_SUMMARY"
    )
    with pytest.raises(IntegrityError, match="final execution artifact"):
        load_final_registered_context(root=Path("C:/path-that-does-not-exist"), plan=plan)


def test_final_execution_authorization_is_exact_and_single_use(tmp_path: Path) -> None:
    from futures_rebuild.boundary import OperationClassification, OperationReceipt

    boundary = RepoBoundary(tmp_path)
    trial_id = "a" * 64
    plan = {
        "plan_id": "b" * 64,
        "plan_sha256": "c" * 64,
        "selected_sources_id": "d" * 64,
    }
    output_root = tmp_path / OUTPUT_ROOT
    required = _required_scope(
        trial_id=trial_id, plan=plan, output_root=output_root,
    )
    receipt = OperationReceipt.issue_user_approved(
        boundary, operation=OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope={
            key: value for key, value in required.items()
            if key not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}
        },
        approval_command=OPERATION,
        approval_plan_id=str(plan["plan_id"]),
        approval_plan_sha256=str(plan["plan_sha256"]),
        approval_line=(
            f"APPROVE {OPERATION} PLAN {plan['plan_id']} "
            f"SHA256 {plan['plan_sha256']}"
        ),
    )
    claim = claim_final_execution(
        root=tmp_path, boundary=boundary, receipt=receipt,
        trial_id=trial_id, plan=plan, output_root=output_root,
    )
    assert claim.exists()
    with pytest.raises(UnauthorizedOperation, match="already consumed"):
        claim_final_execution(
            root=tmp_path, boundary=boundary, receipt=receipt,
            trial_id=trial_id, plan=plan, output_root=output_root,
        )


def test_fully_observed_failed_candidate_gate_is_conclusive_rejection() -> None:
    evaluation = {"stress": {"candidate": _path(
        ending="98566.25", drawdown="1552.50",
    )}}
    coverage = {
        "stress": {
            "passed": False,
            "by_strategy": {"candidate": 0, "risk_matched_always_long_intraday": 1},
        }
    }
    decision = derive_final_decision(
        evaluation=evaluation, selected_path_coverage=coverage,
    )
    assert decision["classification"] == "REJECT_HISTORICAL_SCREEN_MANDATORY_GATE"
    assert set(decision["failed_mandatory_gates"]) == {
        "STRESS_NET_PNL_NOT_POSITIVE",
        "CONTINUOUS_DRAWDOWN_EXCEEDS_1500_USD",
    }
    assert decision["missing_data_helped_decision"] is False


def test_missing_candidate_path_can_never_create_rejection_or_promotion() -> None:
    decision = derive_final_decision(
        evaluation={"stress": {"candidate": _path(
            ending="98000", drawdown="2000",
        )}},
        selected_path_coverage={
            "stress": {"passed": False, "by_strategy": {"candidate": 1}},
        },
    )
    assert decision["classification"] == "INCONCLUSIVE_DATA_OR_COVERAGE"
    assert decision["inference_executed"] is False
    assert decision["promotion_possible"] is False


def test_unpublished_evidence_is_complete_create_only_and_verifiable(
    tmp_path: Path,
) -> None:
    boundary = RepoBoundary(tmp_path)
    payloads = {name: {"name": name} for name in REQUIRED_PAYLOADS}
    staged = stage_unpublished_evidence(
        root=tmp_path, boundary=boundary,
        output_root=tmp_path / "state/unpublished",
        trial_id="a" * 64, authorization_receipt_id="b" * 64,
        payloads=payloads,
    )
    assert staged["state"] == "SEALED_UNPUBLISHED"
    assert staged["publication"] == "false"
    manifest = verify_unpublished_evidence(
        root=tmp_path, bundle_path=Path(staged["bundle_path"]),
    )
    assert manifest["publication"] is False
    assert set(manifest["files"]) == {
        f"{name}.json" for name in REQUIRED_PAYLOADS
    }
    with pytest.raises(IntegrityError, match="create-only"):
        stage_unpublished_evidence(
            root=tmp_path, boundary=boundary,
            output_root=tmp_path / "state/unpublished",
            trial_id="a" * 64, authorization_receipt_id="b" * 64,
            payloads=payloads,
        )


def test_complete_final_pipeline_reaches_one_terminal_decision_synthetically() -> None:
    chicago = ZoneInfo("America/Chicago")
    training_dates = [
        date(2018, 1, 2) + timedelta(days=index) for index in range(30)
    ] + [
        date(2019, 1, 2) + timedelta(days=index) for index in range(30)
    ]
    evaluation_dates = [
        date(year, 1, 2) + timedelta(days=index)
        for year in (2020, 2021, 2022) for index in range(10)
    ]
    sessions = training_dates + evaluation_dates
    census: list[v5.CensusCheckpoint] = []
    streams: dict[tuple[str, int], list[v5.V5SourceRecord]] = {
        (market, year): []
        for market in v5.MARKETS for year in range(2018, 2023)
    }
    spec = MarketSpec(Decimal("0.25"), Decimal("12.50"), Decimal("50"))
    ordinal = 0
    for session_index, session_date in enumerate(sessions):
        session = session_date.isoformat()
        decisions = {
            checkpoint: int(datetime.combine(
                session_date,
                time(*[int(value) for value in checkpoint.split(":")]),
                chicago,
            ).timestamp() * 1_000_000_000)
            for checkpoint in v5.CHECKPOINTS
        }
        for market_index, market in enumerate(v5.MARKETS):
            for checkpoint in v5.CHECKPOINTS:
                decision = decisions[checkpoint]
                core = {
                    "market": market, "session": session,
                    "checkpoint": checkpoint, "decision": decision,
                }
                census.append(v5.CensusCheckpoint(
                    ExpectedCheckpoint(
                        sha256_json(core), market, session_date.year,
                        session, checkpoint, decision,
                    ),
                    True, "c" * 64,
                ))
            first = decisions["08:30"] - 70 * v5.NS_PER_MINUTE
            last = decisions["13:30"] + 61 * v5.NS_PER_MINUTE
            event = first
            while event <= last:
                ordinal += 1
                minute_index = (event - first) // v5.NS_PER_MINUTE
                center = (
                    Decimal("100") + Decimal(market_index * 5)
                    + Decimal(session_index % 7) / Decimal("10")
                    + Decimal(minute_index % 23) / Decimal("100")
                )
                close = center + Decimal((minute_index % 3) - 1) / Decimal("100")
                streams[(market, session_date.year)].append(v5.V5SourceRecord(
                    market, session, "ELIGIBLE",
                    CausalBar(
                        event, event + v5.NS_PER_MINUTE,
                        event + v5.NS_PER_MINUTE + 5_000_000_000,
                        center, max(center, close) + Decimal("0.25"),
                        min(center, close) - Decimal("0.25"), close, True,
                    ),
                    float(100 + ordinal % 37), "d" * 64,
                    f"{ordinal:064x}", spec,
                ))
                event += v5.NS_PER_MINUTE
    result = run_final_trial_pipeline(
        streams={key: tuple(value) for key, value in streams.items()},
        census=tuple(census), contract=_contract(), trial_id="e" * 64,
        runtime_receipt={"runtime_receipt_id": "f" * 64},
    )
    assert result.decision["classification"] in {
        "PASS_HISTORICAL_SCREEN", "FAIL_PROMOTION_GATE",
        "FAIL_MULTIPLICITY_OR_CONTROL", "FAIL_NO_EDGE",
        "REJECT_HISTORICAL_SCREEN_MANDATORY_GATE",
        "INCONCLUSIVE_DATA_OR_COVERAGE",
    }
    assert result.evidence.predictions
    assert result.evidence.opportunity_ledger
    assert result.evidence.runtime_receipt["runtime_receipt_id"] == "f" * 64
