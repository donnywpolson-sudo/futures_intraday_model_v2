"""Inactive search-accounting successor; never grants qualification or data access.

Charges are not observations. The alternative below validates adjustment
arithmetic only, not the validity of financial time-series p-values or a bound
on previously unrecorded research. Existing model/qualification artifacts stay
unchanged until a separately validated statistical-policy cutover.
"""
from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from pathlib import Path

from .canonical import sha256_file, sha256_json, write_new_canonical_authority
from .errors import IntegrityError
from .fixed_hour_model_comparison_preparation import CONFIG as MODEL_CONFIG, ledger_snapshot, validate_bundle
from .fixed_hour_model_comparison_evaluation import BASELINES
from .fixed_hour_terminal_successor import AUTHORITY, MARKETS

CONFIG = Path("configs/fixed_hour_search_accounting_v1.json")
STATUSES = {"OBSERVED_COMPLETE", "OBSERVED_PARTIAL", "ABORTED_NO_PERFORMANCE",
            "NOT_EVALUATED", "UNRESOLVED"}
CLAIMS = tuple(f"{market}:{baseline}" for market in MARKETS for baseline in BASELINES)


def performance_inventory(records):
    """Validate supplied evidence metadata, not inferred or reconstructed returns.

    Even a complete supplied subset is not a complete historical population.
    No zero-fill, surrogate observation, or reduced search charge is permitted.
    """
    seen = {}
    required = {"trial_id", "status", "source_sha256", "comparison_scope_id", "sharpe"}
    for r in records:
        if set(r) != required or not isinstance(r["trial_id"], str) or not r["trial_id"]:
            raise IntegrityError("invalid performance record fields or identity")
        if r["status"] not in STATUSES:
            raise IntegrityError("unknown performance evidence status")
        source = r["source_sha256"]
        if not isinstance(source, str) or len(source) != 64 or any(c not in "0123456789abcdef" for c in source):
            raise IntegrityError("performance disposition needs a source identity")
        if r["status"] == "OBSERVED_COMPLETE":
            scope, sharpe = r["comparison_scope_id"], r["sharpe"]
            if (not isinstance(scope, str) or len(scope) != 64 or any(c not in "0123456789abcdef" for c in scope)
                or type(sharpe) not in (int, float) or not math.isfinite(sharpe)):
                raise IntegrityError("observed Sharpe needs finite value and comparable scope")
        elif r["sharpe"] is not None or r["comparison_scope_id"] is not None:
            raise IntegrityError("missing/partial/unevaluated performance cannot become a Sharpe observation")
        previous = seen.get(r["trial_id"])
        if previous is not None and previous != r:
            raise IntegrityError("conflicting duplicate evidence")
        seen[r["trial_id"]] = dict(r)
    ordered = sorted(seen.values(), key=lambda r: r["trial_id"])
    complete = [r for r in ordered if r["status"] == "OBSERVED_COMPLETE"]
    core = {"records": ordered, "status_counts": dict(Counter(r["status"] for r in ordered)),
        "observed_sharpe_count": len(complete),
        "observed_comparison_scopes": sorted({r["comparison_scope_id"] for r in complete}),
        "historical_population_completeness": "NOT_ESTABLISHED",
        "global_dsr_status": "UNAVAILABLE_NO_COMPARABLE_COMPLETE_POPULATION",
        "search_charge_reduction": 0, "authority": AUTHORITY}
    return {**core, "inventory_id": sha256_json(core)}


def adjustment_mechanics(p_values, *, search_upper_bound, charged_floor):
    """Bonferroni arithmetic on a declared bound, never a validation certificate.

    Valid individual p-values and a bound covering the full claim-generating
    search are mathematical preconditions. An integer supplied here proves
    neither; this function deliberately cannot qualify a market.
    """
    if (type(charged_floor) is not int or charged_floor < 469
        or type(search_upper_bound) is not int or not charged_floor <= search_upper_bound <= 10_000_000):
        raise IntegrityError("require explicit bounded search count at least the retained charge floor")
    if not set(p_values) <= set(CLAIMS):
        raise IntegrityError("unknown market/baseline claim")
    values = {claim: 1.0 for claim in CLAIMS}
    for claim, value in p_values.items():
        if type(value) not in (float, int) or not math.isfinite(value) or not 0 <= value <= 1:
            raise IntegrityError("invalid p-value; missing is omitted, never zero-filled")
        values[claim] = float(value)
    comparisons = len(CLAIMS) * search_upper_bound
    return {"method": "BONFERRONI_ARITHMETIC_ONLY", "declared_claims": list(CLAIMS),
        "charged_floor": charged_floor, "assumed_search_upper_bound": search_upper_bound,
        "count_bound_verified": False, "individual_p_values_validated": False,
        "family_size": comparisons, "raw_p_threshold": .05 / comparisons,
        "adjusted_p_values": {k: min(1., v * comparisons) for k, v in values.items()},
        "missing_claims": [k for k in CLAIMS if k not in p_values],
        "minimum_plus_one_bootstrap_resamples_for_resolution": 20 * comparisons - 1,
        "promotion_authorized": False, "authority": AUTHORITY}


def build_policy(root: Path):
    predecessor = json.loads((root / MODEL_CONFIG).read_bytes())
    validate_bundle(predecessor, root)
    census = ledger_snapshot(root)
    floor = census["burden"]["combined_burden"]
    legacy = json.loads((root / "configs/legacy_trial_penalty_decision.json").read_bytes())
    if legacy["preregistered_penalty_count"] != sum(legacy[k] for k in
            ("observed_attempt_floor", "unresolved_reference_count", "safety_margin")):
        raise IntegrityError("legacy penalty components do not reconcile")
    buckets = {"legacy_observed_attempt_floor_not_sharpe_count": legacy["observed_attempt_floor"],
        "legacy_unresolved_reference_charges": legacy["unresolved_reference_count"],
        "legacy_safety_margin_charge": legacy["safety_margin"],
        "other_predecessor_charges_not_assumed_observed": floor - legacy["preregistered_penalty_count"] - 108 - 180,
        "prepared_ridge_successor_configurations": 108, "current_comparison_configurations": 180}
    if any(v < 0 for v in buckets.values()) or sum(buckets.values()) != floor:
        raise IntegrityError("search charges do not reconcile")
    # Import only the two exact disposition artifacts already inspected in the
    # authorized recovery audit. These are not a full historical-result census.
    disposition_paths = (
        "state/trial_registry/fixed_hour_ridge_results/8762645d7b0243451c3b216bcf0acbf1a49ce41a79b0353703d74af616f33c09.json",
        "state/fixed_hour_multifamily_governed/b5dd8cbc3322d445257bdf1dc5bd13e8407e7f7226614929e23db1bca1f2f24d/mechanical_abort.json",
    )
    dispositions = [json.loads((root / p).read_bytes()) for p in disposition_paths]
    partial, aborted = dispositions
    if (partial.get("outer_folds_executed") != 1 or partial.get("remaining_outer_folds_authorized_or_executed") != 0
        or partial.get("evaluation_state") != "EVALUATED_FAIL"
        or aborted.get("state") != "ABORTED_BEFORE_PARQUET_ROW_DECODING_NO_MODEL_FIT_NO_RESULT"
        or aborted.get("performance_result_observed") is not False):
        raise IntegrityError("audited result disposition changed")
    inventory = performance_inventory([
        {"trial_id": r["trial_id"], "status": status, "source_sha256": sha256_file(root / p),
         "comparison_scope_id": None, "sharpe": None}
        for r, p, status in zip(dispositions, disposition_paths, ("OBSERVED_PARTIAL", "ABORTED_NO_PERFORMANCE"), strict=True)])
    core = {"schema_version": "fixed_hour_search_accounting/1.0.0",
        "state": "IMPLEMENTED_INACTIVE_STATISTICAL_VALIDATION_PENDING",
        "predecessor_bundle_id": predecessor["bundle_id"], "mechanism_id": predecessor["mechanism_id"],
        "predecessor_config": MODEL_CONFIG.as_posix(), "predecessor_sha256": sha256_file(root / MODEL_CONFIG),
        "ledger_head": census["ledger_head"], "charged_search_floor": floor,
        "charge_buckets": buckets, "search_count_upper_bound": None,
        "observed_performance_inventory": inventory,
        "inventory_scope": "TWO_AUDITED_DISPOSITIONS_ONLY;NOT_A_COMPLETE_POPULATION",
        "disposition_sources": {p: sha256_file(root / p) for p in disposition_paths},
        "required_evidence_statuses": sorted(STATUSES),
        "comparison_scope_definition": ["market", "ordered_evaluation_dates", "sampling_unit",
            "cost_basis", "return_units", "selection_procedure", "result_source_identity"],
        "dsr_policy": "DO_NOT_MAP_CHARGES_TO_SHARPES;NO_IMPUTATION;SUBSET_DSR_CANNOT_QUALIFY_GLOBAL_SEARCH",
        "proposed_replacement": {
            "method": "BONFERRONI_ACROSS_COMPLETE_SEARCH_AND_28_CLAIMS",
            "alpha": .05, "fixed_claims": list(CLAIMS),
            "diagnostic_at_charge_floor_not_verified_upper_bound": adjustment_mechanics({}, search_upper_bound=floor, charged_floor=floor),
            "validation_required": ["DEFENSIBLE_COMPLETE_SEARCH_UPPER_BOUND_BEFORE_NEW_RESULTS",
                "VALID_P_VALUES_FOR_THE_ENTIRE_NESTED_SELECTION_PROCEDURE",
                "DEPENDENT_DATE_NULL_CALIBRATION_AND_MONTE_CARLO_PRECISION",
                "PREDECLARED_POWER_AND_SAMPLE_ADEQUACY_WITHOUT_MARKET_RESULT_TUNING",
                "INACTIVE_SUCCESSORS_FOR_GLOBAL_READINESS_PHASE_3_PHASE_6_AND_QUALIFIER"],
            "existing_999_resample_test_reusable_at_new_threshold": False,
            "dsr_equivalence_claim": False, "ready_for_qualification": False},
        "unchanged": ["MODELS", "FEATURES", "180_CONFIGURATION_SEARCH", "COSTS",
            "CHRONOLOGICAL_SPLITS", "RIDGE_ADDED_VALUE_GATE", "NO_RIDGE_FALLBACK",
            "EXISTING_DSR_GATE_UNTIL_VALIDATED_CUTOVER", "HOLDOUT", "ACTIVE_POINTERS"],
        "blocking_reasons": ["STATISTICAL_REPLACEMENT_NOT_VALIDATED", "SEARCH_UPPER_BOUND_NOT_ESTABLISHED"],
        "sources": ["https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf",
            "https://katsevich-teaching.github.io/stat-9610-notes/multiple-testing-chapter.html"],
        "implementation_sha256": sha256_file(Path(__file__)), "authority": AUTHORITY}
    return {**core, "policy_id": sha256_json(core)}


def validate_policy(policy, root):
    # Reject stale preparation before opening any historical disposition source.
    if policy.get("predecessor_sha256") != sha256_file(root / MODEL_CONFIG):
        raise IntegrityError("accounting predecessor drifted; historical assessment is not a current certificate")
    if policy != build_policy(root):
        raise IntegrityError("accounting policy, source bindings, or ledger drifted")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare", action="store_true")
    args = parser.parse_args()
    if not args.prepare:
        parser.error("--prepare required; no historical evaluation or activation command exists")
    root = Path(__file__).resolve().parents[2]
    policy = build_policy(root)
    write_new_canonical_authority(root / CONFIG, policy)
    validate_policy(json.loads((root / CONFIG).read_bytes()), root)
    print(json.dumps({"config": CONFIG.as_posix(), "state": policy["state"],
        "charged_floor": policy["charged_search_floor"], "blocking_reasons": policy["blocking_reasons"]}))


if __name__ == "__main__":
    main()
