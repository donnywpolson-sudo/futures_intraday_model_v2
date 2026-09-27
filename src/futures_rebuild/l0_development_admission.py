"""Inactive, exact-scope L0 development admission; never certifies economic results.

No function here reads market rows or activates a contract. Source facts must
come from the existing trusted admission route. A separately approved activation
must pin the exact contract AND readiness certificate before registration.
"""
from collections.abc import Mapping
from pathlib import Path

from .canonical import (assert_no_linklike_ancestors, contained_path, sha256_file,
                        sha256_json)
from .errors import IntegrityError, UnauthorizedOperation
from . import l0_alpha_candidate as c

CONTRACT_SCHEMA = "l0_development_admission_contract/1"
CERTIFICATE_SCHEMA = "l0_development_readiness_certificate/1"
REGISTRATION_SCHEMA = "l0_development_registration/1"
GATEWAY_SCHEMA = "certified_research_gateway/l0-development-1"
ACTIVATION_PATH = Path("configs/active_l0_development_admission.json")
CONTRACT_PATH = Path("configs/l0_development_admission_v4.json")
CLAIM = "BOUNDED_L0_DEVELOPMENT_ADMISSION_NOT_ECONOMIC_COMPLETENESS"
FAMILY = "l0_development"
SOURCE_CHECKS = (
    "trusted_release", "row_identity", "calendar_applicability", "calendar_publication",
    "bar_availability", "revision_uncertainty", "instrument_definitions",
    "contract_mapping", "fee_evidence",
)
FOLD_CHECKS = ("chronological_order", "strict_label_maturity", "training_only_transforms",
               "purge_applied", "embargo_applied", "same_contract_history")
FOUNDATIONS = (
    "configs/active_alpha_research_ladder.json",
    "configs/active_alpha_historical_research_design.json",
    "configs/active_alpha_ladder_execution_design.json",
    "configs/historical_observability_policy.json",
)
SCOPE_KEYS = frozenset({
    "gateway_schema", "operation_kind", "trial_id", "trial_family", "protocol_id",
    "registration_path", "registration_sha256", "readiness_certificate_id",
    "readiness_evidence_sha256", "scientific_policy_sha256", "global_trial_ledger_head",
    "conservative_legacy_attempt_count", "l0_contract_id", "l0_contract_sha256",
    "l0_activation_sha256", "l0_declaration_id", "l0_bundle_id", "l0_market",
    "l0_dataset_id", "markets", "start_inclusive", "end_exclusive", "claim",
})


def _exact(value, keys, name):
    if not isinstance(value, Mapping) or set(value) != set(keys):
        raise IntegrityError(f"L0 {name} fields differ")
    return value


def _identity(value, field, schema):
    if not isinstance(value, Mapping) or value.get("schema_version") != schema:
        raise IntegrityError(f"L0 {field} schema differs")
    c.identity(value, field)


def _read(root, relative, digest=None):
    from .preexecution_fold_certification import _load_json_bytes
    if not isinstance(relative, str) or Path(relative).is_absolute() or ".." in Path(relative).parts:
        raise IntegrityError("L0 evidence path must be repository-relative")
    path = contained_path(root, relative)
    assert_no_linklike_ancestors(path)
    return _load_json_bytes(path, expected_sha256=digest)[0]


def _binding(root, binding):
    _exact(binding, ("path", "sha256"), "evidence binding")
    c.old.profit.digest(binding["sha256"])
    return _read(root, binding["path"], binding["sha256"])


def _verify_files(root, bindings):
    from .preexecution_fold_certification import _binding_map
    for relative, digest in _binding_map(bindings, name="L0 immutable bindings").items():
        path = contained_path(root, relative)
        assert_no_linklike_ancestors(path)
        if sha256_file(path) != digest:
            raise IntegrityError("L0 bound evidence changed")


def build_contract(root):
    """Return preparation metadata only; no activation or certificate issuance."""
    from . import l0_alpha_preparation as p
    bundle = _read(root, p.CONFIG)
    c.identity(bundle, "bundle_id")
    if bundle["design"] != c.design() or set(bundle["implementation_files"]) != set(p.FILES):
        raise IntegrityError("L0 preparation no longer represents the frozen experiment")
    _verify_files(root, bundle["implementation_files"])
    calendar_binding = bundle["entry_calendar"]
    if calendar_binding["path"] != p.CALENDAR_PATH or calendar_binding["research_view"] != "evidence_only":
        raise IntegrityError("L0 calendar binding leaves the explicit inactive successor")
    calendar = _binding(root, {k: calendar_binding[k] for k in ("path", "sha256")})
    c.identity(calendar, "calendar_id")
    if calendar["calendar_id"] != calendar_binding["calendar_id"]:
        raise IntegrityError("L0 calendar identity differs")
    return c.bind({
        "schema_version": CONTRACT_SCHEMA, "state": "PREPARED_INACTIVE",
        "bundle": {"path": p.CONFIG, "sha256": sha256_file(root / p.CONFIG)},
        "bundle_id": bundle["bundle_id"], "protocol_id": c.mechanism_id(),
        "scope": {"markets": list(c.MARKETS), "start_inclusive": "2018-01-01",
                  "end_exclusive": "2023-01-01", "claim": CLAIM},
        "requirements": {"source_checks": list(SOURCE_CHECKS), "fold_checks": list(FOLD_CHECKS),
                         "outer_fold_markets": 32, "inner_fold_markets": 128,
                         "minimum_inner_observed_dates": 252,
                         "minimum_outer_and_final_observed_dates": 505},
        "foundation_bindings": {name: sha256_file(root / name) for name in FOUNDATIONS},
        "authority": {"activation": False, "registration": False, "historical_execution": False,
                      "L1": False, "holdout": False, "forward": False, "trading": False},
    }, "contract_id")


def validate_contract(root, contract):
    _identity(contract, "contract_id", CONTRACT_SCHEMA)
    if contract != build_contract(root):
        raise IntegrityError("L0 contract, implementation or foundation binding changed")
    return dict(contract)


def declaration(contract):
    """The full campaign is declared even when one market remains blocked."""
    return c.bind({"schema_version": "l0_development_declaration/1",
                   "contract_id": contract["contract_id"], "protocol_id": contract["protocol_id"],
                   **contract["scope"], "configuration_charges": 40,
                   "folds": c.design()["folds"], "comparisons": c.design()["comparisons"],
                   "scenarios": list(c.SCENARIOS), "promotion_authority": False}, "declaration_id")


def build_certificate(*, contract_binding, contract, source_bindings, source_facts,
                      fold_evidence, final_support, dataset_ids, evidence_class):
    """Validate census facts, not model selections, risk capital or P&L.

    Missing source facts are UNKNOWN. Counts alone never turn them into PASS.
    A row certificate still needs independent activation of its exact bytes.
    """
    from .preexecution_fold_certification import _integer, _binding_map, ROW_CERTIFIED, SYNTHETIC_ONLY
    _identity(contract, "contract_id", CONTRACT_SCHEMA)
    _exact(contract_binding, ("path", "sha256"), "contract binding")
    if evidence_class not in (ROW_CERTIFIED, SYNTHETIC_ONLY):
        raise IntegrityError("L0 evidence class differs")
    bindings = _binding_map(source_bindings, name="L0 source metadata bindings")
    if any(not name.endswith(".json") for name in bindings):
        raise IntegrityError("L0 source bindings must reference evidence JSON, not market payloads")
    _exact(source_facts, c.MARKETS, "source market scope")
    _exact(final_support, c.MARKETS, "final-fit scope")
    _exact(dataset_ids, c.MARKETS, "dataset scope")
    design = c.design()
    dates = c.weekday_dates()
    failures = []
    for market in c.MARKETS:
        c.old.profit.digest(dataset_ids[market])
        _exact(source_facts[market], SOURCE_CHECKS, "source facts")
        for check, fact in source_facts[market].items():
            _exact(fact, ("status", "evidence_paths"), "source fact")
            if fact["status"] not in ("PASS", "FAIL", "UNKNOWN"):
                raise IntegrityError("L0 source fact status differs")
            paths = fact["evidence_paths"]
            if not isinstance(paths, list) or len(paths) != len(set(paths)) or any(p not in bindings for p in paths):
                raise IntegrityError("L0 source fact lacks its bound evidence")
            if fact["status"] != "PASS" or not paths:
                failures.append(f"{market}:{check}")
        final_count = _integer(final_support[market], name="L0 final observed dates")
        if final_count > len(dates):
            raise IntegrityError("L0 final training count exceeds the development clock")
        if final_count < 505:
            failures.append(f"{market}:final_support")
    ranges = design["folds"]
    expected = {(m, i, role): window["audit_dates"] for m in c.MARKETS
                for i, f in enumerate(ranges)
                for role, window in [("outer", f["outer"]),
                                     *((f"inner_{j}", w) for j, w in enumerate(f["inner"]))]}
    observed = set()
    for row in fold_evidence:
        _exact(row, ("market", "outer_fold", "role", "audit_dates", "observed_training_dates",
                     "training_rows", "checks", "calendar", "benchmarks"), "fold evidence")
        _integer(row["outer_fold"], name="L0 outer ordinal")
        key = (row["market"], row["outer_fold"], row["role"])
        if key in observed or key not in expected or row["audit_dates"] != expected[key]:
            raise IntegrityError("L0 exact fold-market identity differs")
        observed.add(key)
        count = _integer(row["observed_training_dates"], name="L0 observed dates")
        n = _integer(row["training_rows"], name="L0 training rows")
        if n < count or n > count * 330 or count > dates.index(row["audit_dates"][0]):
            raise IntegrityError("L0 training counts exceed their date/row denominator")
        minimum = 505 if row["role"] == "outer" else 252
        if count < minimum:
            failures.append(f"{key}:training_support")
        _exact(row["checks"], FOLD_CHECKS, "fold checks")
        if any(type(v) is not bool for v in row["checks"].values()):
            raise IntegrityError("L0 fold checks must be booleans")
        if not all(row["checks"].values()):
            failures.append(f"{key}:causal_fold_checks")
        _exact(row["calendar"], ("allowed", "blocked", "unknown"), "calendar dispositions")
        for v in row["calendar"].values():
            _integer(v, name="L0 calendar decisions")
        if sum(row["calendar"].values()) != len(expected[key]) * 330:
            raise IntegrityError("L0 calendar does not reconcile to the independent clock")
        if row["calendar"]["unknown"]:
            failures.append(f"{key}:calendar_unknown")
        _exact(row["benchmarks"], design["comparisons"], "benchmark scope")
        for name, scenarios in row["benchmarks"].items():
            _exact(scenarios, c.SCENARIOS, "benchmark scenarios")
            for scenario, counts in scenarios.items():
                _exact(counts, ("entries", "priced_entries", "exits", "priced_exits",
                                "unresolved_positions", "terminal_dates"), "benchmark counts")
                for v in counts.values():
                    _integer(v, name="L0 benchmark disposition")
                if (counts["priced_entries"] > counts["entries"] or counts["exits"] > counts["entries"]
                        or counts["priced_exits"] > counts["exits"]
                        or counts["entries"] > len(expected[key]) * 330
                        or counts["terminal_dates"] > len(expected[key])):
                    raise IntegrityError("L0 benchmark counts exceed their denominators")
                if name == "no_trade" and any(v for k, v in counts.items() if k != "terminal_dates"):
                    raise IntegrityError("L0 no-trade benchmark attempted orders")
                if (counts["entries"] != counts["priced_entries"] or counts["entries"] != counts["exits"]
                        or counts["exits"] != counts["priced_exits"] or counts["unresolved_positions"]
                        or counts["terminal_dates"] != len(expected[key])):
                    failures.append(f"{key}:{name}:{scenario}:unresolved_benchmark")
    if observed != set(expected):
        raise IntegrityError("L0 requires all 160 original fold-market identities")
    ready = not failures and evidence_class == ROW_CERTIFIED
    return c.bind({
        "schema_version": CERTIFICATE_SCHEMA, "trial_family": FAMILY,
        "protocol_id": contract["protocol_id"], "claim": CLAIM,
        "contract_binding": dict(contract_binding), "contract_id": contract["contract_id"],
        "source_bindings": bindings, "source_facts": source_facts,
        "fold_evidence": fold_evidence, "final_support": final_support, "dataset_ids": dataset_ids,
        "evidence_class": evidence_class, "historical_rows_opened": evidence_class == ROW_CERTIFIED,
        "overall_decision": "PASS" if ready else "FAIL", "failures": failures,
        "registration_allowed": ready, "historical_execution_authorization_allowed": ready,
        "economic_completeness": "NOT_EVALUATED", "promotion_authority": False,
        "synthetic_evidence_proves_alpha": False,
    }, "certificate_id")


def validate_certificate(root, certificate):
    _identity(certificate, "certificate_id", CERTIFICATE_SCHEMA)
    _exact(certificate, ("schema_version", "trial_family", "protocol_id", "claim",
        "contract_binding", "contract_id", "source_bindings", "source_facts", "fold_evidence",
        "final_support", "dataset_ids", "evidence_class", "historical_rows_opened",
        "overall_decision", "failures", "registration_allowed",
        "historical_execution_authorization_allowed", "economic_completeness",
        "promotion_authority", "synthetic_evidence_proves_alpha", "certificate_id"), "certificate")
    contract = validate_contract(root, _binding(root, certificate["contract_binding"]))
    rebuilt = build_certificate(contract_binding=certificate["contract_binding"], contract=contract,
        **{key: certificate[key] for key in ("source_bindings", "source_facts", "fold_evidence",
                                           "final_support", "dataset_ids", "evidence_class")})
    if rebuilt != certificate:
        raise IntegrityError("L0 certificate does not reproduce from its evidence")
    _verify_files(root, certificate["source_bindings"])
    # Evidence documents carry exact claim scope. Merely listing a file is not a PASS.
    for market, facts in certificate["source_facts"].items():
        for check, fact in facts.items():
            for path in fact["evidence_paths"]:
                document = _read(root, path, certificate["source_bindings"][path])
                claim = document.get("l0_source_claims", {}).get(market, {}).get(check)
                if claim != {"status": fact["status"], "contract_id": contract["contract_id"],
                             "start_inclusive": "2018-01-01", "end_exclusive": "2023-01-01",
                             "evidence_class": certificate["evidence_class"]}:
                    raise IntegrityError("L0 source evidence claim is absent or scope-mismatched")
    return dict(certificate)


def require_active(root, certificate):
    """No activation writer exists here; engineering approval cannot activate L0."""
    if not (root / ACTIVATION_PATH).is_file():
        raise UnauthorizedOperation("L0 development admission is inactive")
    active = _read(root, ACTIVATION_PATH.as_posix())
    expected = c.bind({"schema_version": "l0_development_activation/1", "state": "ACTIVE",
                      "contract_binding": certificate["contract_binding"],
                      "contract_id": certificate["contract_id"],
                      "readiness_certificate_id": certificate["certificate_id"],
                      "claim": CLAIM}, "activation_id")
    if active != expected:
        raise UnauthorizedOperation("L0 activation does not bind this exact contract and evidence")
    return sha256_file(root / ACTIVATION_PATH)


def validate_registration(root, registration, certificate):
    _identity(certificate, "certificate_id", CERTIFICATE_SCHEMA)
    contract = validate_contract(root, _binding(root, certificate["contract_binding"]))
    _identity(registration, "trial_id", REGISTRATION_SCHEMA)
    _exact(registration, ("schema_version", "trial_id", "trial_family", "protocol_id",
                          "fold_readiness_binding", "scientific_governance_binding",
                          "l0_declaration"), "registration")
    if (registration["trial_family"] != FAMILY or registration["protocol_id"] != contract["protocol_id"]
            or registration["l0_declaration"] != declaration(contract)):
        raise UnauthorizedOperation("L0 registration leaves its declared experiment")
    require_active(root, certificate)
    return contract


def execution_scope(root, registration, certificate, path, digest, extras):
    from .research_gateway_policy import scientific_governance_binding
    contract = validate_registration(root, registration, certificate)
    governance = registration["scientific_governance_binding"]
    current = scientific_governance_binding(root)
    if any(governance.get(k) != current[k] for k in ("scientific_policy", "legacy_attempts")):
        raise IntegrityError("L0 scientific governance changed")
    _exact(extras, ("l0_bundle_id", "l0_market", "l0_dataset_id"), "market execution scope")
    market = extras["l0_market"]
    if (market not in c.MARKETS or extras["l0_bundle_id"] != contract["bundle_id"]
            or extras["l0_dataset_id"] != certificate["dataset_ids"][market]):
        raise UnauthorizedOperation("L0 market/dataset execution scope differs")
    binding = registration["fold_readiness_binding"]
    scope = {
        "gateway_schema": GATEWAY_SCHEMA, "operation_kind": "TRIAL_HISTORICAL_EXECUTION",
        "trial_id": registration["trial_id"], "trial_family": FAMILY,
        "protocol_id": contract["protocol_id"], "registration_path": path.relative_to(root).as_posix(),
        "registration_sha256": digest, "readiness_certificate_id": certificate["certificate_id"],
        "readiness_evidence_sha256": binding["evidence_sha256"],
        "scientific_policy_sha256": governance["scientific_policy"]["sha256"],
        "global_trial_ledger_head": governance["global_trial_ledger"]["head"],
        "conservative_legacy_attempt_count": str(governance["legacy_attempts"]["conservative_count"]),
        "l0_contract_id": contract["contract_id"],
        "l0_contract_sha256": certificate["contract_binding"]["sha256"],
        "l0_activation_sha256": require_active(root, certificate),
        "l0_declaration_id": registration["l0_declaration"]["declaration_id"],
        "markets": "ES,NQ,CL,GC", "start_inclusive": "2018-01-01", "end_exclusive": "2023-01-01",
        "claim": CLAIM, **extras,
    }
    validate_execution_scope(scope)
    return scope


def validate_execution_scope(scope):
    _exact(scope, SCOPE_KEYS, "receipt scope")
    non_hash = {"gateway_schema", "operation_kind", "trial_family", "registration_path",
                "conservative_legacy_attempt_count", "l0_market", "markets", "start_inclusive",
                "end_exclusive", "claim"}
    for key in SCOPE_KEYS - non_hash:
        c.old.profit.digest(scope[key])
    if (scope["gateway_schema"] != GATEWAY_SCHEMA or scope["operation_kind"] != "TRIAL_HISTORICAL_EXECUTION"
            or scope["trial_family"] != FAMILY or scope["protocol_id"] != c.mechanism_id()
            or scope["l0_market"] not in c.MARKETS or scope["markets"] != "ES,NQ,CL,GC"
            or scope["start_inclusive"] != "2018-01-01" or scope["end_exclusive"] != "2023-01-01"
            or scope["claim"] != CLAIM or not scope["registration_path"].startswith("state/trial_registry/")
            or not scope["conservative_legacy_attempt_count"].isdigit()
            or int(scope["conservative_legacy_attempt_count"]) <= 0):
        raise UnauthorizedOperation("L0 receipt leaves the bounded development experiment")
