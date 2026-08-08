from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from futures_rebuild.contract_economics_audit import (
    ContractEconomicsAudit,
    SignatureException,
    VerifiedContractEconomicsAudit,
    _definition_at,
    _publish_contract_economics_signature_audit,
    audit_contract_economics,
    prepare_contract_economics_signature_audit,
    require_phase8_passing_contract_economics_audit,
)
from futures_rebuild.data_layout import PhasePublisher
from futures_rebuild.foundation.economics import EconomicsRuleBook
from futures_rebuild.foundation.records import INT64_NULL, ProviderBar, ProviderDefinition


UTC = timezone.utc
RELEASE = "a" * 64
MANIFEST = "b" * 64
FILE = "c" * 64


def _rulebook(point="50", tick="0.25"):
    return EconomicsRuleBook.from_embedded_payload(
        {
            "authority_policy": {"eligible_contract_requires_provider_unit_qty_match": True, "mutable_public_urls_authorize_economics": False, "provider_sentinel_allowed": False, "rulebook_hash_bound_into_every_economics_record": True},
            "currency": "USD", "dataset": "GLBX.MDP3", "forbidden_authorities": ["min_price_increment_amount", "contract_multiplier", "legacy_phase1b_multiplier"],
            "point_value_definition": "synthetic", "rules_version": "1.3.0", "valid_from": "2020-01-01T00:00:00+00:00",
            "verification_sources": {"DATABENTO_DEFINITION_GLBX_MDP3": {"authoritative": True, "binding": "EXACT_LAYOUT_V2_DBN_RELEASE_LOGICAL_DEFINITION_PATH_AND_PROVIDER_EVENT_TIME", "locator": "manifests/data_releases/dbn/" + RELEASE + ".json#data/dbn/definition/{market}/{year}/{filename}", "role": "definition"}, "REFERENCE": {"authoritative": False, "binding": "MUTABLE_PUBLIC_REFERENCE_NOT_TRUST_EVIDENCE", "locator": "reference", "role": "crosscheck"}},
            "rules": [{"market": "ES", "point_value": point, "expected_unit_qty": point, "quote_convention": "USD_PER_INDEX_POINT", "source_ids": ["DATABENTO_DEFINITION_GLBX_MDP3", "REFERENCE"]}],
        }, required_market="ES"
    )


def _definition(instrument_id, *, tick="0.25", unit="50", recv=1, expiration=0):
    return ProviderDefinition(
        dataset="GLBX.MDP3", market="ES", publisher_id=1, instrument_id=instrument_id,
        instrument_id_date_utc="1970-01-01", ts_event_ns=recv, ts_recv_ns=recv,
        activation_ns=0, expiration_ns=expiration, security_update_action="ADD",
        instrument_class="FUTURE", security_type="FUT", raw_symbol=f"ES{instrument_id}",
        exchange="XCME", currency="USD", min_price_increment_nano=int(Decimal(tick) * 1_000_000_000),
        unit_of_measure_qty_nano=(INT64_NULL if unit is None else int(Decimal(unit) * 1_000_000_000)), unit_of_measure="USD",
        source_release_id=RELEASE, source_manifest_sha256=MANIFEST,
        source_file_path="dbn/definition/ES/2020/source.dbn.zst", source_file_sha256=FILE,
        row_ordinal=instrument_id, row_sha256=f"{instrument_id:064x}",
    )


def _bar(instrument_id, at):
    return ProviderBar(
        dataset="GLBX.MDP3", market="ES", publisher_id=1, instrument_id=instrument_id,
        event_at_ns=at, open_nano=100_000_000_000, high_nano=101_000_000_000,
        low_nano=99_000_000_000, close_nano=100_500_000_000, volume=1,
        source_release_id=RELEASE, source_manifest_sha256=MANIFEST,
        source_file_path="dbn/ohlcv_1m/ES/2020/source.dbn.zst", source_file_sha256=FILE,
        row_sha256=f"{instrument_id + at:064x}",
    )


def test_groups_identical_contract_economics_and_marks_roll_boundary():
    report = audit_contract_economics(
        [_definition(1), _definition(2)], [_bar(1, 2), _bar(2, 62)], rulebook=_rulebook()
    )
    assert report.passed
    assert len(report.signatures) == 1
    assert next(iter(report.contracts_by_signature.values())) == (1, 2)
    assert [item.elapsed_ns for item in report.roll_boundaries] == [60]
    assert "return" not in str(report.as_dict()).lower()


def test_new_signature_requires_explicit_exception():
    report = audit_contract_economics(
        [_definition(1), _definition(2, tick="0.5")], [_bar(1, 2), _bar(2, 62)], rulebook=_rulebook(tick="0.5")
    )
    assert not report.passed
    signature_id = report.unapproved_signature_ids[0]
    approved = audit_contract_economics(
        [_definition(1), _definition(2, tick="0.5")], [_bar(1, 2), _bar(2, 62)], rulebook=_rulebook(tick="0.5"),
        exceptions=[SignatureException("ES", signature_id, "Documented contract specification change")],
    )
    assert approved.passed


def test_missing_definition_and_bad_tick_math_fail_closed():
    missing = audit_contract_economics([], [_bar(1, 2)], rulebook=_rulebook())
    assert not missing.passed
    assert "no point-in-time" in missing.unresolved_contracts[0].reason
    mismatch = audit_contract_economics(
        [_definition(1, unit="51")], [_bar(1, 2)], rulebook=_rulebook()
    )
    assert not mismatch.passed
    assert "unit quantity" in mismatch.unresolved_contracts[0].reason


def test_missing_provider_unit_uses_protected_rulebook_but_mismatch_still_blocks():
    report = audit_contract_economics(
        [_definition(1, unit=None)], [_bar(1, 2)], rulebook=_rulebook()
    )
    assert report.passed
    signature = next(iter(report.signatures.values()))
    assert signature.contract_unit_quantity == Decimal("50")


def test_equal_timestamp_equivalent_definitions_use_canonical_row_order():
    first = _definition(1, recv=1)
    second = _definition(1, recv=1)
    second = ProviderDefinition(**{**second.__dict__, "row_sha256": "0" * 64})
    selected = _definition_at([first, second], _bar(1, 2), _rulebook())
    assert selected.row_sha256 == "0" * 64


def test_tied_definitions_with_different_economics_fail_closed():
    first = _definition(1, recv=1)
    second = _definition(1, tick="0.5", recv=1)
    try:
        _definition_at([first, second], _bar(1, 2), _rulebook(tick="0.5"))
    except Exception as exc:
        assert "ambiguous" in str(exc)
    else:
        raise AssertionError("different tied economics must not resolve")


def test_blocked_audit_cannot_publish_acceptance_receipt(boundary, operation_factory):
    blocked = audit_contract_economics([], [_bar(1, 2)], rulebook=_rulebook())
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / "audit.lock",
    )
    try:
        _publish_contract_economics_signature_audit(
            blocked, source_release_id=RELEASE, boundary=boundary, publisher=publisher
        )
    except Exception as exc:
        assert "cannot publish" in str(exc)
    else:
        raise AssertionError("blocked audit publication must fail")


def test_prepare_output_is_plain_language_and_never_executes():
    result = prepare_contract_economics_signature_audit(
        markets=("ES", "CL"), years=(2018, 2022), dbn_release_id=RELEASE
    )
    assert result["status"] == "CONFIRMATION_REQUIRED"
    assert "approval_to_paste" not in result


def test_audit_report_is_immutable_and_non_authorizing(boundary, operation_factory):
    audit = audit_contract_economics(
        [_definition(1)], [_bar(1, 2)], rulebook=_rulebook()
    )
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / "audit.lock",
    )
    receipt = _publish_contract_economics_signature_audit(
        audit, source_release_id=RELEASE, boundary=boundary, publisher=publisher
    )
    verified = VerifiedContractEconomicsAudit.from_release(receipt, boundary)
    assert verified.payload["status"] == "PASSED"
    assert receipt.verify(boundary).metadata["authoritative_economics"] is False


def test_phase8_gate_requires_all_rulebook_markets(boundary, operation_factory):
    root = Path(__file__).parents[1]
    rulebook = EconomicsRuleBook.from_file(root / "configs" / "contract_economics_rules.json")
    audit = audit_contract_economics([_definition(1)], [_bar(1, 2)], rulebook=_rulebook())
    audit = ContractEconomicsAudit(
        signatures=audit.signatures,
        contracts_by_signature=audit.contracts_by_signature,
        roll_boundaries=audit.roll_boundaries,
        unapproved_signature_ids=(),
        unresolved_contracts=(),
        bar_count=audit.bar_count,
        mapping_resolution_by_market={market: "ohlcv-1d" for market in rulebook.rules},
    )
    publisher = PhasePublisher(
        boundary=boundary,
        operation_receipt=operation_factory("PUBLISH_RELEASE"),
        lock_path=boundary.active_root / "state" / "locks" / "audit.lock",
    )
    receipt = _publish_contract_economics_signature_audit(
        audit, source_release_id=RELEASE, boundary=boundary, publisher=publisher
    )
    require_phase8_passing_contract_economics_audit(
        receipt, boundary=boundary, rulebook=rulebook
    )
