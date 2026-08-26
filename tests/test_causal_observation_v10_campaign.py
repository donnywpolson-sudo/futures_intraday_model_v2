from __future__ import annotations

import json
from pathlib import Path

import pytest

from futures_rebuild.boundary import RepoBoundary
from futures_rebuild.canonical import io_path, sha256_json
from futures_rebuild.causal_observation_canary import (
    DecodedMarket,
    build_market_candidate,
)
from futures_rebuild.causal_observation_foundation import (
    issue_synthetic_observation_context,
    prepared_inventory,
)
from futures_rebuild.causal_observation_full_build import _V10StageCreator
from futures_rebuild.causal_observation_market_checkpoint import MARKET_ORDER
from futures_rebuild.causal_observation_v10_campaign import (
    CampaignState,
    append_journal_event,
    simulate_complete_campaign,
    transition,
    validate_journal,
)
from futures_rebuild.causal_observation_verifier import verify_observation_candidate
from futures_rebuild.data_layout import DataReleaseManifest
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from tests.test_causal_observation_canary import RULEBOOK, _bar, _definition


def _canary_result() -> dict[str, object]:
    core = {
        "schema_version": "development_causal_observation_v10_es_2025_canary_result/1.0.0",
        "status": "PASS_V10_ES_2025_CANARY_VERIFIED_INACTIVE",
        "target_market": "ES",
        "target_year": 2025,
        "complete_market_checkpoint": False,
        "reusable_in_same_checkpoint_set": False,
        "can_seed_complete_market_checkpoint": False,
        "campaign_advancement_eligible": True,
        "publication_authorized": False,
        "activation_authorized": False,
    }
    return {**core, "result_id": sha256_json(core)}


def _deep_candidate(
    root: Path, attempt: str
) -> tuple[dict[str, object], Path, DataReleaseManifest]:
    relative = (
        Path("data/causally_gated_normalized/v10")
        / ("c" * 64)
        / "ES"
        / attempt
        / "work/2025/2025-07-01_2025-07-13T220000Z"
    )
    stage = root / relative
    assert len(str(stage / "candidate/observations.parquet")) > 260
    context = issue_synthetic_observation_context(
        boundary=RepoBoundary(root), fixture_id="3" * 64
    )
    decoded = DecodedMarket(
        definitions=(_definition(),),
        primary_1m=(_bar(0, row_digit="2"), _bar(2, row_digit="3")),
        reference_1s={},
        reference_1h={},
        reference_1d={},
        support_rows=(),
        decoded_record_count=3,
    )
    prepared = build_market_candidate(
        publisher=_V10StageCreator(
            boundary=RepoBoundary(root), relative=relative.as_posix()
        ),  # type: ignore[arg-type]
        context=context,
        market="ES",
        window={"start": "2024-03-04T00:00:00Z", "end": "2024-03-05T00:00:00Z"},
        decoded=decoded,
        economics_rulebook=RULEBOOK,
    )
    verify_observation_candidate(
        stage=prepared.stage,
        manifest=prepared.manifest,
        economics_rulebook=RULEBOOK,
    )
    return prepared_inventory(prepared), prepared.stage, prepared.manifest


def test_all_41_markets_advance_only_after_certificate_finalization() -> None:
    final = simulate_complete_campaign()
    assert final.phase == "INACTIVE_COMPLETE"
    assert final.certified_markets == MARKET_ORDER


@pytest.mark.parametrize(
    "phase",
    (
        "NORMALIZATION",
        "CERTIFICATION_PASS_1",
        "CERTIFICATION_PASS_2",
        "CERTIFICATE_FINALIZATION",
    ),
)
def test_every_later_market_failure_preserves_exact_certified_prefix(phase: str) -> None:
    for index, market in enumerate(MARKET_ORDER):
        stopped = simulate_complete_campaign(fault=(market, phase))
        assert stopped.phase == "TERMINAL_STOP"
        assert stopped.stop_class == "UNEXPECTED_FAILURE"
        assert stopped.certified_markets == MARKET_ORDER[:index]


def test_one_infrastructure_recovery_resumes_exact_stage_then_repetition_stops() -> None:
    state = transition(CampaignState(), "PASS")
    state = transition(state, "CANARY_VERIFIED", evidence=_canary_result())
    assert state.phase == "NORMALIZATION" and state.market == "ES"
    stopped = transition(state, "INFRASTRUCTURE_FAILURE")
    assert stopped.phase == "RECOVERABLE_STOP"
    resumed = transition(stopped, "RESUME")
    assert resumed.phase == "NORMALIZATION"
    repeated = transition(resumed, "INFRASTRUCTURE_FAILURE")
    assert repeated.phase == "TERMINAL_STOP"
    assert repeated.stop_class == "REPEATED_INFRASTRUCTURE_FAILURE"


def test_unknown_event_never_advances() -> None:
    with pytest.raises(UnauthorizedOperation, match="invalid"):
        transition(CampaignState(), "SOMETHING_UNMODELED")


def test_hash_chained_journal_reconstructs_and_rejects_mutation(tmp_path: Path) -> None:
    first_state = transition(CampaignState(), "PASS")
    first_path = tmp_path / "journal/0001.json"
    first = append_journal_event(
        first_path,
        sequence=1,
        previous_event_id=None,
        event="PREFLIGHT_PASS",
        state=first_state,
    )
    second_state = transition(
        first_state, "CANARY_VERIFIED", evidence=_canary_result()
    )
    second_path = tmp_path / "journal/0002.json"
    append_journal_event(
        second_path,
        sequence=2,
        previous_event_id=str(first["event_id"]),
        event="CANARY_PASS",
        state=second_state,
    )
    assert validate_journal((first_path, second_path)) == second_state
    payload = json.loads(second_path.read_text())
    payload["event"] = "CHANGED"
    second_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(IntegrityError, match="chain"):
        validate_journal((first_path, second_path))


def test_deepest_v10_terminal_candidate_lifecycle_is_deterministic(tmp_path: Path) -> None:
    first, first_stage, first_manifest = _deep_candidate(
        tmp_path / "first", "a" * 64
    )
    second, _, _ = _deep_candidate(tmp_path / "second", "b" * 64)
    assert first["stage_file_sha256"] == second["stage_file_sha256"]
    extra = first_stage / "candidate/unmanifested.bin"
    io_path(extra).write_bytes(b"unexpected")
    with pytest.raises(IntegrityError, match="unmanifested"):
        verify_observation_candidate(
            stage=first_stage,
            manifest=first_manifest,
            economics_rulebook=RULEBOOK,
        )
