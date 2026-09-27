from copy import deepcopy
import json
from pathlib import Path

import pytest

from futures_rebuild.errors import IntegrityError
from futures_rebuild import fixed_hour_search_accounting as accounting

pytestmark = pytest.mark.current
ROOT = Path(__file__).resolve().parents[1]


def record(status="OBSERVED_COMPLETE", **changes):
    return {"trial_id": "observed-trial", "status": status,
        "source_sha256": "a" * 64,
        "comparison_scope_id": "b" * 64 if status == "OBSERVED_COMPLETE" else None,
        "sharpe": .25 if status == "OBSERVED_COMPLETE" else None, **changes}


def test_observations_are_not_charges_or_completeness():
    rows = [record(), record(), record("ABORTED_NO_PERFORMANCE", trial_id="abort"),
        record("OBSERVED_PARTIAL", trial_id="partial"), record("NOT_EVALUATED", trial_id="planned"),
        record("UNRESOLVED", trial_id="unknown")]
    result = accounting.performance_inventory(rows)
    assert len(result["records"]) == 5 and result["observed_sharpe_count"] == 1
    assert result["search_charge_reduction"] == 0
    assert result["historical_population_completeness"] == "NOT_ESTABLISHED"
    assert result["global_dsr_status"].startswith("UNAVAILABLE")
    assert not any(result["authority"].values())
    mixed = accounting.performance_inventory([record(), record(trial_id="other", comparison_scope_id="c" * 64)])
    assert len(mixed["observed_comparison_scopes"]) == 2
    assert mixed["global_dsr_status"].startswith("UNAVAILABLE")


@pytest.mark.parametrize("bad", [
    record("ABORTED_NO_PERFORMANCE", sharpe=0.),
    record("NOT_EVALUATED", sharpe=.3), record("OBSERVED_PARTIAL", sharpe=.2),
    record(sharpe=float("nan")), record(sharpe=True), record(source_sha256=""),
    record(comparison_scope_id=""), record("INVENTED"),
])
def test_reject_imputation_and_unbound_or_nonfinite_observations(bad):
    with pytest.raises(IntegrityError): accounting.performance_inventory([bad])


def test_conflicting_evidence_fails():
    with pytest.raises(IntegrityError):
        accounting.performance_inventory([record(), record(sharpe=.4)])


def test_adjustment_counts_complete_family_and_resolution():
    result = accounting.adjustment_mechanics({"ES:ridge_only": 0.}, search_upper_bound=469, charged_floor=469)
    assert result["family_size"] == 13132
    assert result["raw_p_threshold"] == .05 / 13132
    assert result["minimum_plus_one_bootstrap_resamples_for_resolution"] == 262639
    assert len(result["adjusted_p_values"]) == 28
    assert result["adjusted_p_values"]["NQ:ridge_only"] == 1.
    assert result["promotion_authorized"] is False
    assert result["count_bound_verified"] is False
    assert result["individual_p_values_validated"] is False
    assert 1 / 1000 > result["raw_p_threshold"]
    # Even an extreme synthetic p-value cannot turn arithmetic into authority.
    assert not any(result["authority"].values())
    threshold = result["raw_p_threshold"]
    boundary = accounting.adjustment_mechanics({"ES:ridge_only": threshold}, search_upper_bound=469, charged_floor=469)
    assert boundary["adjusted_p_values"]["ES:ridge_only"] == pytest.approx(.05)
    larger = accounting.adjustment_mechanics({"ES:ridge_only": threshold}, search_upper_bound=938, charged_floor=469)
    assert larger["adjusted_p_values"]["ES:ridge_only"] == pytest.approx(.1)


@pytest.mark.parametrize("values,upper,floor", [
    ({}, 468, 469), ({}, 469, 468), ({}, True, 469),
    ({"ES:unknown": .1}, 469, 469), ({"ES:ridge_only": -1}, 469, 469),
    ({"ES:ridge_only": float("nan")}, 469, 469), ({"ES:ridge_only": True}, 469, 469),
])
def test_invalid_adjustment_inputs(values, upper, floor):
    with pytest.raises(IntegrityError):
        accounting.adjustment_mechanics(values, search_upper_bound=upper, charged_floor=floor)


def test_policy_preserves_legacy_floor_without_pseudo_sharpes():
    # Inspect preserved accounting metadata, not historical result source files.
    policy = json.loads((ROOT / accounting.CONFIG).read_bytes())
    assert policy["charged_search_floor"] == 469
    assert sum(policy["charge_buckets"].values()) == 469
    assert policy["charge_buckets"]["legacy_safety_margin_charge"] == 1
    assert policy["charge_buckets"]["legacy_unresolved_reference_charges"] == 65
    assert policy["observed_performance_inventory"]["observed_sharpe_count"] == 0
    assert policy["observed_performance_inventory"]["status_counts"] == {
        "OBSERVED_PARTIAL": 1, "ABORTED_NO_PERFORMANCE": 1}
    assert policy["search_count_upper_bound"] is None
    assert policy["proposed_replacement"]["ready_for_qualification"] is False
    assert not any(policy["authority"].values())
    broken = deepcopy(policy); broken["charged_search_floor"] = 468
    with pytest.raises(IntegrityError): accounting.validate_policy(broken, ROOT)


def test_prepared_artifact(monkeypatch):
    policy = json.loads((ROOT / accounting.CONFIG).read_bytes())
    def forbidden_recovery(_):
        pytest.fail("stale accounting certificate must not reopen historical dispositions")
    monkeypatch.setattr(accounting, 'build_policy', forbidden_recovery)
    with pytest.raises(IntegrityError, match='predecessor drifted'):
        accounting.validate_policy(policy, ROOT)
    assert policy["state"] == "IMPLEMENTED_INACTIVE_STATISTICAL_VALIDATION_PENDING"
