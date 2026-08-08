from decimal import Decimal
from pathlib import Path

import pytest

from futures_rebuild.canonical import canonical_bytes, sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.tier1_bracket_post_audit import (
    BaselineRun,
    CausalBar,
    GateEvidence,
    OpportunityRecord,
    ResearchRiskAccount,
    SessionObservation,
    account_metrics,
    assert_allowed_research_year,
    causality_certificate,
    classify_historical_screen,
    cost_ticks,
    first_causal_entry_bar,
    latest_causal_feature_bar,
    load_invalid_closure_preparation,
    load_post_audit_contract,
    persist_post_audit_registration,
    planned_initial_loss_usd,
    prepare_post_audit_registration,
    reconcile_opportunity_ledger,
    simulate_bracket_fill,
    validate_independent_baselines,
    verify_post_audit_registration,
)
import futures_rebuild.tier1_bracket_post_audit as post_audit_module


ROOT = Path(__file__).parents[1]
D = Decimal


def _bar(event: int, *, available: int | None = None, price: str = "100", high: str | None = None,
         low: str | None = None, executable: bool = True) -> CausalBar:
    close = D(price)
    return CausalBar(
        event_at_ns=event,
        bar_end_at_ns=event + 60,
        available_at_ns=event + 65 if available is None else available,
        open_price=close,
        high_price=D(high) if high is not None else close,
        low_price=D(low) if low is not None else close,
        close_price=close,
        executable=executable,
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(payload) + b"\n")


def _synthetic_registration_root(tmp_path: Path) -> Path:
    root = tmp_path / "synthetic-repo"
    contract = {
        "schema_version": "tier1_bracket_post_audit_successor/3.0.0",
        "state": "PREPARED_NOT_REGISTERED",
        "supersedes_invalid_trial_id": post_audit_module.INVALID_TRIAL_ID,
        "classification": "POST_AUDIT_NON_PRISTINE_HISTORICAL_SCREEN_ONLY",
        "risk": {
            "profile_id": "RESEARCH_ACCOUNT_100K_V1",
            "starting_capital_usd": "100000",
            "maximum_planned_initial_loss_usd": "1000",
            "daily_loss_threshold_usd": "1000",
            "continuous_drawdown_threshold_usd": "5000",
        },
        "costs": {
            "label": "PROVIDER_NEUTRAL_PROVISIONAL_RESEARCH_COSTS",
            "fee_per_side_usd": "5.00",
        },
        "inference": {
            "portfolio_mees_usd_per_complete_session": "20",
            "stationary_bootstrap_resamples": 10000,
            "minimum_complete_clusters": 30,
        },
        "ordered_outcomes": [
            "INVALID",
            "INCONCLUSIVE_DATA_OR_POWER",
            "FAIL_NO_EDGE",
            "FAIL_NOT_ECONOMIC",
            "INCONCLUSIVE_EFFECT",
            "FAIL_MULTIPLICITY_OR_CONTROL",
            "PASS_HISTORICAL_SCREEN",
        ],
    }
    _write_json(root / post_audit_module.CONTRACT_PATH, contract)
    preserved = {
        post_audit_module.INVALID_TRIAL_REGISTRY: {
            "trial_id": post_audit_module.INVALID_TRIAL_ID,
            "source_pairs": [
                {
                    "market": market,
                    "year": year,
                    "source_parquet_sha256": f"{index:064x}",
                }
                for index, (market, year) in enumerate(
                    (
                        (market, year)
                        for market in post_audit_module.MARKETS
                        for year in range(2018, 2023)
                    ),
                    start=1,
                )
            ],
        },
        post_audit_module.INVALID_EVALUATION_EVENT: {"event": "synthetic"},
        post_audit_module.INVALID_EVALUATION_MANIFEST: {"manifest": "synthetic"},
    }
    for relative, payload in preserved.items():
        _write_json(root / relative, payload)
    for relative in (
        Path("src/futures_rebuild/tier1_bracket_post_audit.py"),
        Path("src/futures_rebuild/historical_capability.py"),
    ):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# synthetic implementation binding\n", encoding="utf-8")
    closure = {
        "schema_version": "tier1_bracket_invalid_trial_closure_preparation/2.0.0",
        "state": "PREPARED_NOT_PUBLISHED_NOT_ACTIVE",
        "disposition": "INVALID_VOID_CAUSAL_TIMING_FILL_AND_COVERAGE_DEFECTS",
        "trial_id": post_audit_module.INVALID_TRIAL_ID,
        "preserve_existing_artifacts_byte_for_byte": True,
        "publication_authorized": False,
        "activation_authorized": False,
        "preserved_bindings": {
            "trial_registry_path": post_audit_module.INVALID_TRIAL_REGISTRY.as_posix(),
            "trial_registry_sha256": post_audit_module.sha256_file(
                root / post_audit_module.INVALID_TRIAL_REGISTRY
            ),
            "evaluation_event_path": post_audit_module.INVALID_EVALUATION_EVENT.as_posix(),
            "evaluation_event_sha256": post_audit_module.sha256_file(
                root / post_audit_module.INVALID_EVALUATION_EVENT
            ),
            "evaluation_manifest_path": post_audit_module.INVALID_EVALUATION_MANIFEST.as_posix(),
            "evaluation_manifest_sha256": post_audit_module.sha256_file(
                root / post_audit_module.INVALID_EVALUATION_MANIFEST
            ),
        },
    }
    _write_json(root / post_audit_module.CLOSURE_PATH, closure)
    return root


def test_contract_is_post_audit_provider_neutral_and_closure_is_void(
    local_evidence_root: Path,
) -> None:
    contract = load_post_audit_contract(root=local_evidence_root)
    closure = load_invalid_closure_preparation(root=local_evidence_root)

    assert "apex" not in canonical_bytes(contract).decode("utf-8").lower()
    assert contract["costs"]["fee_per_side_usd"] == "5.00"
    assert contract["risk"]["profile_id"] == "RESEARCH_ACCOUNT_100K_V1"
    assert closure["disposition"] == "INVALID_VOID_CAUSAL_TIMING_FILL_AND_COVERAGE_DEFECTS"
    assert closure["strategy_conclusion"].startswith("INCONCLUSIVE_INVALID_TRIAL")


def test_registration_is_deterministic_prepare_only_and_has_no_row_authority(
    tmp_path: Path,
) -> None:
    source_root = _synthetic_registration_root(tmp_path)
    first = prepare_post_audit_registration(root=source_root)
    second = prepare_post_audit_registration(root=source_root)

    assert first.trial_id == second.trial_id == sha256_json(first.canonical_payload)
    assert canonical_bytes(first.canonical_payload) == canonical_bytes(second.canonical_payload)
    assert first.canonical_payload["source_row_access"] is False
    assert first.canonical_payload["holdout_or_forward_access"] is False
    output_root = tmp_path / "output"
    written = persist_post_audit_registration(root=output_root, prepared=first)
    verified = verify_post_audit_registration(root=output_root, prepared=first)
    assert written["trial_id"] == verified["trial_id"] == first.trial_id
    with pytest.raises(IntegrityError, match="already exists"):
        persist_post_audit_registration(root=output_root, prepared=first)


def test_0830_decision_rejects_bar_that_is_not_available_until_083105() -> None:
    prior = _bar(0, available=65)
    unavailable_checkpoint_bar = _bar(60, available=125)

    selected = latest_causal_feature_bar(
        bars=(prior, unavailable_checkpoint_bar), decision_at_ns=120,
    )

    assert selected is prior
    with pytest.raises(IntegrityError, match="no completed feature bar"):
        latest_causal_feature_bar(bars=(unavailable_checkpoint_bar,), decision_at_ns=120)


def test_availability_equality_is_allowed_but_entry_must_be_strictly_later() -> None:
    feature = _bar(0, available=60)
    assert latest_causal_feature_bar(bars=(feature,), decision_at_ns=60) is feature

    same_time = _bar(60, available=125)
    next_bar = _bar(120, available=185)
    selected = first_causal_entry_bar(
        bars=(same_time, next_bar), decision_at_ns=60, one_bar_delay_ns=60,
    )
    assert selected is next_bar


def test_holdout_is_rejected_before_path_construction() -> None:
    assert_allowed_research_year(2022)
    with pytest.raises(UnauthorizedOperation, match="2025 holdout"):
        assert_allowed_research_year(2025)


def test_opportunity_ledger_retains_abstentions_and_reconciles_exactly() -> None:
    rows = (
        OpportunityRecord(
            "a", "ES", "2022-01-03", "08:30", 1,
            "INSUFFICIENT_CAUSAL_HISTORY", False,
        ),
        OpportunityRecord(
            "b", "CL", "2022-01-03", "08:30", 1, "HURDLE_FAILURE", True,
            feature_event_at_ns=0, feature_available_at_ns=1,
        ),
        OpportunityRecord(
            "c", "ZN", "2022-01-03", "08:30", 1, "ADMITTED_TRADE", True,
            feature_event_at_ns=0, feature_available_at_ns=1,
            order_submitted_at_ns=2, fill_at_ns=2, outcome_coverage="COMPLETE",
        ),
    )
    census = reconcile_opportunity_ledger(expected_ids=("a", "b", "c"), records=rows)
    assert census == {
        "expected_opportunities": 3,
        "predictions": 2,
        "pre_prediction_abstentions": 1,
        "post_prediction_terminal_rows": 2,
        "predictions_awaiting_terminal_decision": 0,
    }
    with pytest.raises(IntegrityError, match="exactly match"):
        reconcile_opportunity_ledger(expected_ids=("a", "b", "c", "d"), records=rows)

    certificate = causality_certificate(rows)
    assert certificate["prediction_count"] == 2
    assert certificate["admitted_trade_count"] == 1
    assert len(certificate["certificate_id"]) == 64


def test_prediction_with_future_feature_or_predecision_fill_fails_closed() -> None:
    future_feature = OpportunityRecord(
        "x", "ES", "2022-01-03", "08:30", 100, "HURDLE_FAILURE", True,
        feature_event_at_ns=60, feature_available_at_ns=101,
    )
    with pytest.raises(IntegrityError, match="causal feature"):
        future_feature.validate()
    early_fill = OpportunityRecord(
        "y", "ES", "2022-01-03", "08:30", 100, "ADMITTED_TRADE", True,
        feature_event_at_ns=60, feature_available_at_ns=99,
        order_submitted_at_ns=100, fill_at_ns=101, outcome_coverage="COMPLETE",
    )
    with pytest.raises(IntegrityError, match="causal order"):
        early_fill.validate()


def test_costs_are_provider_neutral_monotonic_and_planned_risk_includes_stress() -> None:
    contract = load_post_audit_contract(root=ROOT)
    for market in ("ES", "CL", "ZN", "6E"):
        assert cost_ticks(contract=contract, scenario="base", market=market) \
            < cost_ticks(contract=contract, scenario="stress", market=market) \
            < cost_ticks(contract=contract, scenario="extreme", market=market)
    risk = planned_initial_loss_usd(
        atr=D("2"), tick_size=D("0.25"), tick_value=D("12.50"),
        round_trip_cost_ticks=4, fee_per_side_usd=D("5"),
    )
    assert risk == D("210")


def test_gap_through_stop_uses_adverse_open_and_costs_reconcile() -> None:
    entry = _bar(120, available=185, price="100")
    gap = _bar(180, available=245, price="97", high="99", low="96")
    fill = simulate_bracket_fill(
        direction="long", decision_at_ns=60, entry_bar=entry, path_bars=(gap,),
        atr=D("1"), tick_size=D("1"), tick_value=D("1"), point_value=D("1"),
        fee_per_side_usd=D("5"), round_trip_cost_ticks=4,
    )
    assert fill.reason == "STOP_GAP"
    assert fill.exit_price == D("95")
    assert fill.exit_price < fill.stop_price
    assert fill.gross_pnl_usd - fill.costs_usd == fill.net_pnl_usd


def test_stop_target_collision_is_stop_first() -> None:
    entry = _bar(120, price="100")
    collision = _bar(180, price="100", high="120", low="90")
    fill = simulate_bracket_fill(
        direction="long", decision_at_ns=60, entry_bar=entry, path_bars=(collision,),
        atr=D("1"), tick_size=D("1"), tick_value=D("1"), point_value=D("1"),
        fee_per_side_usd=D("5"), round_trip_cost_ticks=4,
    )
    assert fill.reason in {"STOP", "STOP_GAP"}


def test_timeout_uses_first_causal_executable_bar_and_missing_exit_fails() -> None:
    entry = _bar(120, price="100")
    before = _bar(180, price="100", executable=False)
    timeout = _bar(240, price="100")
    fill = simulate_bracket_fill(
        direction="long", decision_at_ns=60, entry_bar=entry,
        path_bars=(before, timeout), atr=D("1"), tick_size=D("1"),
        tick_value=D("1"), point_value=D("1"), fee_per_side_usd=D("5"),
        round_trip_cost_ticks=0, maximum_hold_ns=120,
    )
    assert fill.reason == "TIMEOUT"
    assert fill.exit_at_ns == 240

    with pytest.raises(IntegrityError, match="ends before"):
        simulate_bracket_fill(
            direction="long", decision_at_ns=60, entry_bar=entry,
            path_bars=(before,), atr=D("1"), tick_size=D("1"),
            tick_value=D("1"), point_value=D("1"), fee_per_side_usd=D("5"),
            round_trip_cost_ticks=0, maximum_hold_ns=120,
        )


def test_risk_account_blocks_after_daily_loss_and_halts_after_drawdown() -> None:
    account = ResearchRiskAccount()
    daily = account.mark(unrealized_pnl_usd=D("-1000"))
    assert daily.entry_blocked_for_session
    assert not daily.permanently_halted
    assert daily.liquidation_required

    halted = account.mark(unrealized_pnl_usd=D("-5000"))
    assert halted.entry_blocked_for_session
    assert halted.permanently_halted

    overshot = account.close(net_pnl_usd=D("-5200"))
    assert overshot.permanently_halted
    assert overshot.equity_usd == D("94800")


def test_baselines_require_independent_signal_scheduler_and_complete_universe() -> None:
    names = (
        "flat_no_trade",
        "fold_local_unconditional_return_by_market_session",
        "previous_bar_sign_momentum",
        "previous_bar_sign_reversal",
        "risk_matched_always_long_intraday",
        "equal_risk_version_of_candidate_signal",
    )
    runs = tuple(
        BaselineRun(name, f"signal-{index}", f"schedule-{index}", ("a", "b"))
        for index, name in enumerate(names)
    )
    validate_independent_baselines(expected_opportunity_ids=("a", "b"), runs=runs)

    reused = tuple(
        BaselineRun(name, "shared", f"schedule-{index}", ("a", "b"))
        for index, name in enumerate(names)
    )
    with pytest.raises(IntegrityError, match="reused"):
        validate_independent_baselines(expected_opportunity_ids=("a", "b"), runs=reused)


def test_metrics_use_complete_daily_account_returns_and_two_sided_turnover() -> None:
    metrics = account_metrics(sessions=(
        SessionObservation("s1", D("100"), True, D("2")),
        SessionObservation("s2", D("0"), True, D("0")),
        SessionObservation("s3", D("-50"), True, D("2")),
        SessionObservation("s4", None, False),
    ))
    assert metrics["complete_sessions"] == 3
    assert metrics["incomplete_sessions"] == 1
    assert metrics["net_pnl_usd"] == "50"
    assert metrics["turnover_contract_equivalents"] == "4"
    assert metrics["annualized_daily_sharpe"] is not None
    assert metrics["annualized_daily_sortino"] is not None

    flat = account_metrics(sessions=(SessionObservation("s1", D("0"), True),))
    assert flat["net_pnl_usd"] == "0"
    assert flat["turnover_contract_equivalents"] == "0"
    assert flat["annualized_daily_sharpe"] is None
    assert flat["annualized_daily_sortino"] is None


@pytest.mark.parametrize(
    ("evidence", "expected"),
    (
        (GateEvidence(invalid=True), "INVALID"),
        (GateEvidence(), "INCONCLUSIVE_DATA_OR_POWER"),
        (GateEvidence(complete_clusters=30, power=D("0.8"), every_required_sleeve_powered=True,
                      confidence_upper_usd=D("0")), "FAIL_NO_EDGE"),
        (GateEvidence(complete_clusters=30, power=D("0.8"), every_required_sleeve_powered=True,
                      confidence_upper_usd=D("20")), "FAIL_NOT_ECONOMIC"),
        (GateEvidence(complete_clusters=30, power=D("0.8"),
                      every_required_sleeve_powered=True,
                      confidence_lower_usd=D("10"), confidence_upper_usd=D("30")),
         "INCONCLUSIVE_EFFECT"),
        (GateEvidence(complete_clusters=30, power=D("0.8"),
                      every_required_sleeve_powered=True,
                      confidence_lower_usd=D("21"), confidence_upper_usd=D("30")),
         "FAIL_MULTIPLICITY_OR_CONTROL"),
        (GateEvidence(complete_clusters=30, power=D("0.8"), every_required_sleeve_powered=True,
                      confidence_lower_usd=D("21"), confidence_upper_usd=D("30"),
                      dsr_probability=D("0.95"), romano_wolf_passed=True, controls_passed=True,
                      stress_and_baselines_passed=True, distribution_gate_passed=True,
                      drawdown_gate_passed=True), "PASS_HISTORICAL_SCREEN"),
    ),
)
def test_ordered_outcome_classification(evidence: GateEvidence, expected: str) -> None:
    assert classify_historical_screen(evidence) == expected
