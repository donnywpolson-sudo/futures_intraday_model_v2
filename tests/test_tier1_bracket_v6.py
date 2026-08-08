from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from futures_rebuild.canonical import sha256_file
from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_bracket_v4 import MarketSpec
from futures_rebuild.tier1_bracket_v5 import (
    NS_PER_MINUTE,
    CalendarSessionSpec,
    EvidenceArtifactsV5,
    build_expected_census_from_calendar,
    load_v5_contract,
    materialize_v5_rows,
)
from futures_rebuild.tier1_bracket_v6 import (
    V5_EVENT,
    V5_REGISTRY,
    V5_TRIAL_ID,
    SourceIntegrityAuditV6,
    V6PipelineResult,
    build_evidence_manifest_v6,
    load_v6_contract,
    normalize_source_mappings_v6,
    prepare_v5_retirement_v6,
    prepare_v6_registration,
    persist_evidence_bundle_v6,
)


ROOT = Path(__file__).resolve().parents[1]


def _row(
    event: int, *, session: str | None = "2020-01-02",
    disposition: str = "ELIGIBLE",
) -> dict[str, object]:
    return {
        "event_at_ns": event,
        "exchange_session_date": session,
        "source_row_sha256": f"{event // NS_PER_MINUTE:064x}"[-64:],
        "disposition": disposition,
        "prediction_in_coverage_denominator": True,
        "failure_code": "NONE" if disposition == "ELIGIBLE" else "DEFINITION_INTRABAR_CHANGE",
        "failure_detail_sha256": "a" * 64,
        "actual_identity_hash": "b" * 64 if disposition == "ELIGIBLE" else None,
        "open_nano": 100_000_000_000,
        "high_nano": 101_000_000_000,
        "low_nano": 99_000_000_000,
        "close_nano": 100_000_000_000,
        "volume": 10,
        "tick_size": "0.25",
        "tick_value": "12.50",
        "point_value": "50",
    }


def test_sessionless_nontradable_row_becomes_explicit_ambiguous_session() -> None:
    start = 1_600_000_000_000_000_000
    rows = iter(
        [
            _row(start),
            _row(start + NS_PER_MINUTE, session=None, disposition="UNRESOLVED_FAIL_CLOSED"),
            _row(start + 2 * NS_PER_MINUTE),
        ]
    )
    audit = SourceIntegrityAuditV6("ES")
    output = list(normalize_source_mappings_v6(market="ES", rows=rows, audit=audit))
    events = [item.bar.event_at_ns for item in output if item.bar is not None]
    assert events == [start, start + NS_PER_MINUTE, start + NS_PER_MINUTE, start + 2 * NS_PER_MINUTE]
    assert not output[1].executable and not output[2].executable
    assert audit.sessionless_nontradable_rows == 1
    assert audit.ambiguous_sessions == {"2020-01-02"}


def test_same_session_missing_minute_is_not_silently_shortened() -> None:
    start = 1_600_000_000_000_000_000
    audit = SourceIntegrityAuditV6("CL")
    output = list(normalize_source_mappings_v6(
        market="CL",
        rows=iter([_row(start), _row(start + 2 * NS_PER_MINUTE)]),
        audit=audit,
    ))
    events = [item.bar.event_at_ns for item in output if item.bar is not None]
    assert events == [start, start + 2 * NS_PER_MINUTE, start + 2 * NS_PER_MINUTE]
    assert audit.same_session_gap_count == 1
    assert audit.ambiguous_sessions == {"2020-01-02"}


def test_ambiguity_marker_becomes_three_explicit_checkpoint_abstentions() -> None:
    start = 1_600_000_000_000_000_000
    audit = SourceIntegrityAuditV6("ES")
    normalized = tuple(normalize_source_mappings_v6(
        market="ES",
        rows=iter([_row(start), _row(start + 2 * NS_PER_MINUTE)]),
        audit=audit,
    ))
    census = build_expected_census_from_calendar(sessions=(
        CalendarSessionSpec(
            "ES", "2020-01-02", 0, 10**20, "c" * 64,
            {checkpoint: True for checkpoint in ("08:30", "10:30", "13:30")},
        ),
    ))
    materialized = materialize_v5_rows(
        source_rows=normalized,
        census=census,
        market_specs={"ES": MarketSpec(Decimal("0.25"), Decimal("12.5"), Decimal("50"))},
        contract=load_v5_contract(root=Path(__file__).resolve().parents[1]),
        prediction_scope_sessions=("2020-01-02",),
    )
    assert len(materialized) == 3
    assert {row.ledger.terminal_disposition for row in materialized} == {
        "MISSING_OR_AMBIGUOUS_MARKET_IDENTITY"
    }
    assert all(not row.ledger.prediction_produced for row in materialized)


def test_labeled_nontradable_row_is_never_executable_and_marks_session() -> None:
    start = 1_600_000_000_000_000_000
    audit = SourceIntegrityAuditV6("ZN")
    output = list(normalize_source_mappings_v6(
        market="ZN",
        rows=iter([_row(start, disposition="UNRESOLVED_FAIL_CLOSED")]),
        audit=audit,
    ))
    assert len(output) == 2
    assert all(not item.executable for item in output)
    assert audit.nontradable_rows == 1
    assert audit.ambiguous_sessions == {"2020-01-02"}


def test_tradable_row_without_session_fails_closed() -> None:
    audit = SourceIntegrityAuditV6("6E")
    with pytest.raises(IntegrityError, match="tradable.*session"):
        list(normalize_source_mappings_v6(
            market="6E", rows=iter([_row(1_600_000_000_000_000_000, session=None)]),
            audit=audit,
        ))


def test_orphan_without_matching_neighbors_fails_closed() -> None:
    start = 1_600_000_000_000_000_000
    audit = SourceIntegrityAuditV6("ES")
    with pytest.raises(IntegrityError, match="minute-contiguous"):
        list(normalize_source_mappings_v6(
            market="ES",
            rows=iter([
                _row(start),
                _row(start + 2 * NS_PER_MINUTE, session=None, disposition="UNRESOLVED_FAIL_CLOSED"),
                _row(start + 3 * NS_PER_MINUTE),
            ]),
            audit=audit,
        ))


def test_v6_inherits_v5_strategy_contract_without_parameter_changes() -> None:
    inherited, delta = load_v6_contract(root=ROOT)
    assert delta["inherited_v5_contract_sha256"] == sha256_file(
        ROOT / "configs/tier1_bracket_successor_v5.json"
    )
    assert inherited["strategy"]["minimum_predicted_net_r_after_stress_costs"] == "0.25"
    assert inherited["risk"]["continuous_drawdown_threshold_usd"] == "1500"
    assert delta["source_integrity_successor"]["silent_drop_or_shortened_feature_window"] == "FORBIDDEN"


def test_v5_is_preserved_and_v6_prepares_without_publication() -> None:
    preserved = {
        path: (ROOT / path).read_bytes()
        for path in (
            Path("configs/tier1_bracket_successor_v5.json"),
            Path("src/futures_rebuild/tier1_bracket_v5.py"),
            Path("tests/test_tier1_bracket_v5.py"),
            V5_REGISTRY,
            V5_EVENT,
        )
    }
    retirement = prepare_v5_retirement_v6(root=ROOT)
    registration = prepare_v6_registration(root=ROOT)
    assert retirement.canonical_payload["trial_id"] == V5_TRIAL_ID
    assert registration.canonical_payload["supersedes_v5_trial_id"] == V5_TRIAL_ID
    assert registration.canonical_payload["change_scope"] == "SOURCE_INTEGRITY_REPRESENTATION_ONLY"
    assert len(registration.canonical_payload["source_bindings"]) == 20
    assert not (
        ROOT / "state/trial_registry/tier1_bracket_v5_retirement" / f"{retirement.record_id}.json"
    ).exists()
    assert not (
        ROOT / "state/trial_registry/tier1_bracket_successor_v6" / f"{registration.trial_id}.json"
    ).exists()
    assert preserved == {path: (ROOT / path).read_bytes() for path in preserved}


def test_v6_evidence_is_decimal_safe_complete_and_create_only(tmp_path: Path) -> None:
    evidence = EvidenceArtifactsV5(
        model={"coefficient": Decimal("1.25")},
        predictions=({"opportunity_id": "p", "score": Decimal("0.3")},),
        opportunity_ledger=({"opportunity_id": "p", "terminal": "ADMITTED_TRADE"},),
        fills=({"opportunity_id": "p", "net": Decimal("2.50")},),
        continuous_equity_marks=({"equity": Decimal("100002.50")},),
        segmented_metrics={"folds": []},
        inference={"status": "OK"},
        decision={"classification": "FAIL_NO_EDGE"},
        runtime_receipt={"runtime_receipt_id": "a" * 64},
    )
    result = V6PipelineResult(
        base=SimpleNamespace(evidence=evidence),
        source_integrity_audit={"ES/2020": {"nontradable_rows": 1}},
    )
    trial_id = "f" * 64
    manifest = build_evidence_manifest_v6(trial_id=trial_id, result=result)
    assert set(manifest["files"]) == {
        "model.json", "predictions.json", "opportunity_ledger.json",
        "fills.json", "continuous_equity_marks.json",
        "segmented_metrics.json", "inference.json", "decision.json",
        "runtime_receipt.json", "source_integrity_audit.json",
    }
    output = tmp_path / "evidence"
    published = persist_evidence_bundle_v6(
        boundary=RepoBoundary(tmp_path), output_root=output,
        trial_id=trial_id, result=result,
    )
    assert Path(published["manifest_path"]).exists()
    with pytest.raises(IntegrityError, match="create-only"):
        persist_evidence_bundle_v6(
            boundary=RepoBoundary(tmp_path), output_root=output,
            trial_id=trial_id, result=result,
        )
