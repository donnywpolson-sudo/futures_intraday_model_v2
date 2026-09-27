"""Generated evidence only; no market readers, fitting, or real activation."""
from copy import deepcopy
from pathlib import Path

import pytest

from futures_rebuild import l0_alpha_candidate as c, l0_alpha_preparation as prep
from futures_rebuild import l0_development_admission as l0
from futures_rebuild import certified_research_gateway as gateway_module
from futures_rebuild.boundary import RepoBoundary, OperationReceipt, OperationClassification
from futures_rebuild.canonical import canonical_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError, UnauthorizedOperation
from futures_rebuild.preexecution_fold_certification import (
    ROW_CERTIFIED, SYNTHETIC_ONLY, require_registration_ready, validate_fold_readiness_certificate,
)
from futures_rebuild.research_gateway_policy import (
    scientific_governance_binding, CERTIFIED_TRIAL_EXECUTION_OPERATION,
    require_current_real_history_operation,
)

REPO = Path(__file__).resolve().parents[1]


def save(root, relative, value):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_bytes(value) + b"\n")
    return path


def rehash(value, field):
    value[field] = sha256_json({k: v for k, v in value.items() if k != field})
    return value


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    # Only the pre-existing global active-foundation check is simulated. The L0
    # contract, certificate, scope, file integrity and receipt code run unchanged.
    monkeypatch.setattr(gateway_module, "require_stage3_active_authority", lambda root: ({}, {}, {}))
    for relative in (*prep.FILES, *l0.FOUNDATIONS, "configs/research_readiness_contract.json",
                     "configs/legacy_trial_penalty_decision.json"):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((REPO / relative).read_bytes())
    # A minimal synthetic preparation supplies precisely the fields the admission
    # validator consumes. No real source catalog or historical payload is copied.
    calendar = c.bind({"schema_version": "SYNTHETIC_ONLY", "schedules": []}, "calendar_id")
    calendar_path = save(tmp_path, prep.CALENDAR_PATH, calendar)
    bundle = c.bind({"design": c.design(), "entry_calendar": {
        "path": prep.CALENDAR_PATH, "sha256": sha256_file(calendar_path),
        "calendar_id": calendar["calendar_id"], "research_view": "evidence_only"}, "implementation_files": {
        name: sha256_file(tmp_path / name) for name in prep.FILES}}, "bundle_id")
    save(tmp_path, prep.CONFIG, bundle)
    contract = l0.build_contract(tmp_path)
    path = save(tmp_path, l0.CONTRACT_PATH, contract)
    binding = {"path": l0.CONTRACT_PATH.as_posix(), "sha256": sha256_file(path)}
    rows = []
    design = c.design()
    for market in c.MARKETS:
        for i, fold in enumerate(design["folds"]):
            for role, window in [("outer", fold["outer"]),
                                 *((f"inner_{j}", w) for j, w in enumerate(fold["inner"]))]:
                rows.append({"market": market, "outer_fold": i, "role": role,
                    "audit_dates": window["audit_dates"], "observed_training_dates": 505 if role == "outer" else 252,
                    "training_rows": 505 if role == "outer" else 252,
                    "checks": dict.fromkeys(l0.FOLD_CHECKS, True),
                    "calendar": {"allowed": 20790, "blocked": 0, "unknown": 0},
                    "benchmarks": {name: {scenario: {
                        "entries": 0 if name == "no_trade" else 63,
                        "priced_entries": 0 if name == "no_trade" else 63,
                        "exits": 0 if name == "no_trade" else 63,
                        "priced_exits": 0 if name == "no_trade" else 63,
                        "unresolved_positions": 0, "terminal_dates": 63,
                    } for scenario in c.SCENARIOS} for name in design["comparisons"]}})
    facts = {market: {name: {"status": "PASS", "evidence_paths": ["state/evidence/source.json"]}
                      for name in l0.SOURCE_CHECKS} for market in c.MARKETS}
    # ROW_CERTIFIED below is a test double for an authorized census, like the
    # existing gateway tests. It is materialized only inside this isolated root.
    args = dict(contract_binding=binding, contract=contract,
                source_facts=facts, fold_evidence=rows,
                final_support=dict.fromkeys(c.MARKETS, 505),
                dataset_ids={m: sha256_json(m) for m in c.MARKETS}, evidence_class=ROW_CERTIFIED)
    document = {"l0_source_claims": {m: {name: {
        "status": "PASS", "contract_id": contract["contract_id"],
        "start_inclusive": "2018-01-01", "end_exclusive": "2023-01-01",
        "evidence_class": ROW_CERTIFIED,
    } for name in l0.SOURCE_CHECKS} for m in c.MARKETS}}
    source = save(tmp_path, "state/evidence/source.json", document)
    args["source_bindings"] = {"state/evidence/source.json": sha256_file(source)}
    certificate = l0.build_certificate(**args)
    return tmp_path, contract, certificate, args


def fixture_registration(prepared, *, active=False):
    root, contract, certificate, args = prepared
    path = save(root, "state/evidence/certificate.json", certificate)
    registration = c.bind({
        "schema_version": l0.REGISTRATION_SCHEMA, "trial_family": l0.FAMILY,
        "protocol_id": c.mechanism_id(), "scientific_governance_binding": scientific_governance_binding(root),
        "l0_declaration": l0.declaration(contract),
        "fold_readiness_binding": {"evidence_path": "state/evidence/certificate.json",
                                   "evidence_sha256": sha256_file(path),
                                   "certificate_id": certificate["certificate_id"]},
    }, "trial_id")
    if active:
        save(root, l0.ACTIVATION_PATH, c.bind({
            "schema_version": "l0_development_activation/1", "state": "ACTIVE",
            "contract_binding": certificate["contract_binding"],
            "contract_id": contract["contract_id"], "readiness_certificate_id": certificate["certificate_id"],
            "claim": l0.CLAIM}, "activation_id"))
    registry_path = root / "state/trial_registry/l0_development" / (registration["trial_id"] + ".json")
    gateway = gateway_module.CertifiedResearchGateway(root, RepoBoundary(root))
    return gateway, registration, registry_path, path


def test_inactive_and_synthetic_cannot_register(prepared):
    root, _, certificate, args = prepared
    assert len(certificate["fold_evidence"]) == 160
    assert l0.validate_certificate(root, certificate) == certificate
    gateway, registration, target, evidence = fixture_registration(prepared)
    with pytest.raises(UnauthorizedOperation, match="inactive"):
        gateway.register_trial(registration_path=target, registration=registration, readiness_evidence_path=evidence)
    assert not target.exists()
    args["evidence_class"] = SYNTHETIC_ONLY
    synthetic = l0.build_certificate(**args)
    assert synthetic["overall_decision"] == "FAIL" and not synthetic["registration_allowed"]
    assert synthetic["economic_completeness"] == "NOT_EVALUATED"


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "date", "market", "inner_min", "outer_min",
                                       "final_min", "unknown_calendar", "missing_fee", "future", "gc_exit"])
def test_scope_and_evidence_failures(prepared, mutation):
    root, _, certificate, original = prepared
    args = deepcopy(original)
    rows = args["fold_evidence"]
    if mutation == "missing": rows.pop()
    if mutation == "duplicate": rows[-1] = deepcopy(rows[0])
    if mutation == "date": rows[0]["audit_dates"][0] = "2025-07-14"
    if mutation == "market": rows[0]["market"] = "YM"
    if mutation == "inner_min": rows[1]["observed_training_dates"] = 251
    if mutation == "outer_min": rows[0]["observed_training_dates"] = 504
    if mutation == "final_min": args["final_support"]["GC"] = 504
    if mutation == "unknown_calendar": rows[0]["calendar"].update(allowed=20789, unknown=1)
    if mutation == "missing_fee": args["source_facts"]["GC"]["fee_evidence"]["status"] = "UNKNOWN"
    if mutation == "future": rows[1]["checks"]["strict_label_maturity"] = False
    if mutation == "gc_exit":
        gc = next(r for r in rows if r["market"] == "GC")
        gc["benchmarks"]["always_long"]["adverse"].update(priced_exits=62, unresolved_positions=1, terminal_dates=62)
    if mutation in ("missing", "duplicate", "date", "market"):
        with pytest.raises(IntegrityError): l0.build_certificate(**args)
    else:
        result = l0.build_certificate(**args)
        assert result["overall_decision"] == "FAIL" and result["failures"]
        forged = rehash({**result, "overall_decision": "PASS", "registration_allowed": True}, "certificate_id")
        with pytest.raises(IntegrityError): validate_fold_readiness_certificate(forged, root=root)


def test_verified_gateway_lifecycle_exact_scope_and_one_use(prepared):
    root, contract, certificate, _ = prepared
    gateway, registration, path, evidence = fixture_registration(prepared, active=True)
    registered = gateway.register_trial(registration_path=path, registration=registration, readiness_evidence_path=evidence)
    extra = {"l0_market": "ES", "l0_bundle_id": contract["bundle_id"], "l0_dataset_id": certificate["dataset_ids"]["ES"]}
    scope = gateway.execution_scope(registration_path=path, expected_registration_sha256=registered["registration_sha256"], additional_scope=extra)
    assert "alpha_ladder_stage" not in scope
    assert scope["claim"] == l0.CLAIM
    require_current_real_history_operation(CERTIFIED_TRIAL_EXECUTION_OPERATION, scope)
    receipt = OperationReceipt.issue_user_approved(gateway.boundary,
        operation=CERTIFIED_TRIAL_EXECUTION_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope, approval_command=CERTIFIED_TRIAL_EXECUTION_OPERATION,
        approval_plan_id="a" * 64, approval_plan_sha256="b" * 64,
        approval_line=f"APPROVE {CERTIFIED_TRIAL_EXECUTION_OPERATION} PLAN {'a' * 64} SHA256 {'b' * 64}")
    claim_args = dict(registration_path=path, expected_registration_sha256=registered["registration_sha256"], receipt=receipt, additional_scope=extra)
    assert gateway.claim_historical_execution(**claim_args).exists()
    with pytest.raises(UnauthorizedOperation, match="already used"):
        gateway.claim_historical_execution(**claim_args)
    for change in ({"l0_market": "GC"}, {"l0_dataset_id": "a" * 64}, {"l0_bundle_id": "a" * 64}, {"alpha_ladder_stage": "holdout"}):
        with pytest.raises((IntegrityError, UnauthorizedOperation)):
            gateway.execution_scope(registration_path=path, expected_registration_sha256=registered["registration_sha256"], additional_scope={**extra, **change})
    for change in ({"end_exclusive": "2026-01-01"}, {"markets": "ES"}, {"claim": "ALPHA"}, {"holdout": "true"}):
        with pytest.raises((IntegrityError, UnauthorizedOperation)):
            require_current_real_history_operation(CERTIFIED_TRIAL_EXECUTION_OPERATION, {**scope, **change})


@pytest.mark.parametrize("target", ["source", "implementation", "foundation", "activation", "registration"])
def test_changed_evidence_rejected_before_reader(prepared, monkeypatch, target):
    root, contract, certificate, _ = prepared
    gateway, registration, path, evidence = fixture_registration(prepared, active=True)
    registered = gateway.register_trial(registration_path=path, registration=registration, readiness_evidence_path=evidence)
    extra = {"l0_market": "ES", "l0_bundle_id": contract["bundle_id"], "l0_dataset_id": certificate["dataset_ids"]["ES"]}
    scope = gateway.execution_scope(registration_path=path, expected_registration_sha256=registered["registration_sha256"], additional_scope=extra)
    receipt = OperationReceipt.issue_user_approved(gateway.boundary,
        operation=CERTIFIED_TRIAL_EXECUTION_OPERATION,
        classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
        scope=scope, approval_command=CERTIFIED_TRIAL_EXECUTION_OPERATION,
        approval_plan_id="a" * 64, approval_plan_sha256="b" * 64,
        approval_line=f"APPROVE {CERTIFIED_TRIAL_EXECUTION_OPERATION} PLAN {'a' * 64} SHA256 {'b' * 64}")
    files = {"source": root / "state/evidence/source.json", "implementation": root / prep.FILES[0],
             "foundation": root / l0.FOUNDATIONS[0], "activation": root / l0.ACTIVATION_PATH, "registration": path}
    files[target].write_bytes(files[target].read_bytes() + b" ")
    monkeypatch.setattr(prep, "validate_bundle", lambda root: {"bundle_id": contract["bundle_id"]})
    calls = []
    with pytest.raises((IntegrityError, UnauthorizedOperation)):
        prep.execute_registered_market(gateway, registration_path=path,
            registration_sha256=registered["registration_sha256"], receipt=receipt, market="ES",
            expected_dataset_id=certificate["dataset_ids"]["ES"], source_contract_id="a" * 64,
            source_release_id="b" * 64, reader=lambda receipt: calls.append(True))
    assert not calls
    # Independently exercise the bound-evidence gate as well as the L0 entry.
    with pytest.raises((IntegrityError, UnauthorizedOperation)):
        gateway.execution_scope(registration_path=path, expected_registration_sha256=registered["registration_sha256"],
            additional_scope={"l0_market": "ES", "l0_bundle_id": contract["bundle_id"], "l0_dataset_id": certificate["dataset_ids"]["ES"]})


def test_real_workspace_preparation_stays_inactive():
    assert not (REPO / l0.ACTIVATION_PATH).exists()
    contract = l0.build_contract(REPO)
    assert contract["state"] == "PREPARED_INACTIVE" and not any(contract["authority"].values())
    assert contract["scope"]["markets"] == ["ES", "NQ", "CL", "GC"]
