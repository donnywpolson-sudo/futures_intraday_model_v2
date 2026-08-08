from __future__ import annotations

from pathlib import Path

import pytest

from futures_rebuild.boundary import OperationClassification, OperationReceipt
from futures_rebuild.causal_market_year_materialization import FOUNDATION_RELEASE_ID
from futures_rebuild.data_layout import PhasePublisher
from futures_rebuild.errors import IntegrityError
from futures_rebuild.phase8_economics_index import (
    build_phase8_interval_selection,
    load_phase8_interval_selection,
    prepare_phase8_economics_publication,
    publish_phase8_actual_contract_economics_index,
    publish_phase8_interval_selection,
    selection_fingerprint,
)


def _root() -> Path:
    return Path(__file__).resolve().parents[1]


def _publisher(boundary) -> PhasePublisher:
    return PhasePublisher(
        boundary=boundary,
        operation_receipt=OperationReceipt.issue_local(
            boundary,
            operation="PUBLISH_RELEASE",
            classification=OperationClassification.SYNTHETIC_MECHANICS_ONLY,
            scope={"release_kind": "phase8_test"},
        ),
        lock_path=boundary.active_root / "state" / "locks" / "phase8-test.lock",
    )


def _synthetic_selection() -> dict[str, object]:
    intervals: list[dict[str, object]] = []
    market_years: list[dict[str, object]] = []
    release_index = 0
    for group in range(644):
        market = f"M{group:03d}"
        year = 2000
        group_releases: list[str] = []
        interval_count = 2 if group < 33 else 1
        for part in range(interval_count):
            release_index += 1
            start = f"{year}-01-{part + 1:02d}T00:00:00Z"
            end = f"{year}-01-{part + 1:02d}T23:59:00Z"
            release_id = f"{release_index:064x}"
            intervals.append(
                {
                    "end": end,
                    "interval_key": f"{market}/{year}/{start}_{end}",
                    "market": market,
                    "release_id": release_id,
                    "start": start,
                    "year": year,
                }
            )
            group_releases.append(release_id)
        market_years.append(
            {
                "coverage_end": intervals[-1]["end"],
                "coverage_start": intervals[-interval_count]["start"],
                "market": market,
                "source_release_ids": group_releases,
                "year": year,
            }
        )
    return {
        "foundation_manifest_sha256": "a" * 64,
        "foundation_release_id": "b" * 64,
        "intervals": intervals,
        "market_years": market_years,
        "schema_version": "1.0.0",
    }


def test_live_foundation_selection_is_explicit_and_complete(
    local_evidence_root: Path,
) -> None:
    payload = build_phase8_interval_selection(
        repository_root=local_evidence_root,
        foundation_release_id=FOUNDATION_RELEASE_ID,
    )

    assert len(payload["intervals"]) == 677
    assert len(payload["market_years"]) == 644
    assert len({item["release_id"] for item in payload["intervals"]}) == 677
    assert len({item["interval_key"] for item in payload["intervals"]}) == 677
    assert selection_fingerprint(payload) == selection_fingerprint(payload)


def test_selection_is_published_as_a_readable_immutable_reference(boundary) -> None:
    payload = _synthetic_selection()
    receipt = publish_phase8_interval_selection(
        payload=payload, boundary=boundary, publisher=_publisher(boundary)
    )

    assert load_phase8_interval_selection(receipt, boundary=boundary) == payload


def test_selection_rejects_duplicate_causal_receipt() -> None:
    payload = _synthetic_selection()
    changed = {**payload, "intervals": [dict(item) for item in payload["intervals"]]}
    changed["intervals"][1]["release_id"] = changed["intervals"][0]["release_id"]

    with pytest.raises(IntegrityError, match="repeats an interval or causal receipt"):
        selection_fingerprint(changed)


def test_prepare_only_publication_has_no_approval_token_surface() -> None:
    prepared = prepare_phase8_economics_publication(
        audit_receipt_id="a" * 64,
        foundation_release_id="b" * 64,
        rulebook_hash="c" * 64,
    )

    assert prepared["status"] == "CONFIRMATION_REQUIRED"
    assert "approval_to_paste" not in str(prepared)
    assert "677" in str(prepared)


def test_index_rejects_incomplete_economics_mapping(boundary) -> None:
    payload = _synthetic_selection()
    selection = publish_phase8_interval_selection(
        payload=payload, boundary=boundary, publisher=_publisher(boundary)
    )
    rulebook_path = _root() / "configs" / "contract_economics_rules.json"
    from futures_rebuild.foundation.economics import EconomicsRuleBook

    rulebook = EconomicsRuleBook.from_file(rulebook_path)
    # The exact mapping check happens before the audit receipt is dereferenced.
    with pytest.raises(IntegrityError, match="exactly one selected causal receipt"):
        publish_phase8_actual_contract_economics_index(
            selection_receipt=selection,
            audit_receipt=selection,
            economics_by_causal_release={},
            rulebook=rulebook,
            boundary=boundary,
            publisher=_publisher(boundary),
        )
