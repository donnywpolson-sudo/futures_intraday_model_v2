from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild.tier1_standard_only_protocol import (
    PREDECESSOR_SHA256,
    PROTOCOL_PATH,
    SOURCE_ADEQUACY_SHA256,
    build_source_policy_correction_record,
    load_standard_only_protocol,
    publish_source_policy_correction,
)
from futures_rebuild.canonical import sha256_file


ROOT = Path(__file__).resolve().parents[1]


def test_standard_only_protocol_preserves_fail_closed_selected_path_rule():
    protocol = load_standard_only_protocol(root=ROOT)
    correction = protocol["source_policy_correction"]
    coverage = protocol["coverage"]

    assert protocol["lineage"]["prior_protocol_preserved"] is True
    assert protocol["source"]["historical_l1_bbo_dependency"] is False
    assert correction["feature_complete_execution_unavailable"] == 34
    assert correction["selected_missing_execution_path"] == "INCONCLUSIVE_DATA_OR_COVERAGE"
    assert correction["runner_up_substitution"] is False
    assert correction["zero_return_imputation_for_missing_execution_path"] is False
    assert coverage["selected_execution_path_rate_required_for_valid_evaluation"] == "1.0"


def test_correction_record_binds_preserved_evidence_and_forbids_expansion():
    record = build_source_policy_correction_record(root=ROOT)

    assert sha256_file(ROOT / "configs/tier1_frozen_trial_protocol.json") == PREDECESSOR_SHA256
    assert sha256_file(ROOT / record["evidence"]["source_adequacy_record_path"]) == SOURCE_ADEQUACY_SHA256
    assert record["replacement_protocol"]["path"] == PROTOCOL_PATH.as_posix()
    assert record["replacement_protocol"]["trial_registered"] is False
    assert record["correction"]["promotion_possible_with_selected_missing_path"] is False
    assert not any(record["publication_scope"].values())


def test_correction_publication_is_create_only(monkeypatch, tmp_path):
    import futures_rebuild.tier1_standard_only_protocol as module

    publication_root = tmp_path / "published"
    monkeypatch.setattr(module, "PUBLICATION_ROOT", publication_root)
    first = publish_source_policy_correction(root=ROOT)
    assert first.exists()
    with pytest.raises(IntegrityError, match="create-only"):
        publish_source_policy_correction(root=ROOT)
