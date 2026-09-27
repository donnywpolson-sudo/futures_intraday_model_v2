"""Prepared callback seam only. No certified execution or research entry point runs."""
import ast
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pytest

from futures_rebuild import l0_alpha_preparation as original
from futures_rebuild.canonical import sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError
from futures_rebuild.l0_alpha_report_demo import fixture_context, fixture_replay
from futures_rebuild.l0_alpha_reporting import load_catalog
from futures_rebuild.live_cockpit.app import CockpitApi

pytestmark = pytest.mark.current
ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("prepare_l0_reporting_successor", ROOT / "scripts/prepare_l0_reporting_successor.py")
successor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(successor)


def forbidden(*args, **kwargs):
    raise AssertionError("restricted source/provider/research/gate entry point reached")


@pytest.fixture
def proposal(tmp_path):
    request = successor.prepare(ROOT, tmp_path / "proposal")
    return tmp_path / "proposal", request


def load_store(path, monkeypatch):
    name = "futures_rebuild.l0_alpha_report_store"
    module = ModuleType(name)
    module.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), "exec"), module.__dict__)
    monkeypatch.setitem(sys.modules, name, module)
    return module


def seam(proposal, tmp_path, monkeypatch, produce, *, previous=None, assembly_error=None, reporting_error=False):
    folder, _ = proposal
    store_module = load_store(folder / "payload" / successor.STORE, monkeypatch)
    stores = []
    init = store_module.ReportingStore.__init__
    def capture(self, *args, **kwargs):
        init(self, *args, **kwargs)
        stores.append(self)
    monkeypatch.setattr(store_module.ReportingStore, "__init__", capture)
    if reporting_error:
        write_catalog = store_module.write_catalog
        def fail_report(directory, reports, comparisons):
            if reporting_error is True or any(r["execution"]["status"] == "FAILED" for r in reports):
                raise OSError("synthetic full disk")
            return write_catalog(directory, reports, comparisons)
        monkeypatch.setattr(store_module, "write_catalog", fail_report)
    bundle = json.loads((ROOT / successor.BUNDLE).read_bytes())
    window = {"audit_dates": fixture_context()["scope"]["dates"]}
    bundle["design"]["folds"] = [{"outer": window, "inner": [window]}]
    bundle["bundle_id"] = "SYNTHETIC_SEAM_NOT_REGISTERABLE"
    bundle["design"]["candidates"] = bundle["design"]["candidates"][:1]
    metadata = {"parent_candidate_id": "synthetic_parent", "hypothesis": "SYNTHETIC ACCOUNTING ONLY",
                "lineage": {"informed_by_results": [{"result_id": "synthetic_prior", "use": "fixture revision"}]}}
    source = (folder / "payload" / successor.PREP).read_text()
    function = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == "execute_registered_market")
    lease = next(n for n in function.body if isinstance(n, ast.With))
    start = next(i for i, n in enumerate(lease.body) if isinstance(n, ast.ImportFrom) and n.module == "l0_alpha_report_store")
    # Execute exactly the prepared post-authorization seam, not the research entrypoint.
    harness = ast.FunctionDef(name="synthetic_seam", args=ast.arguments(posonlyargs=[], args=[], kwonlyargs=[], kw_defaults=[], defaults=[]),
        body=deepcopy(lease.body[start:]), decorator_list=[])
    tree = ast.fix_missing_locations(ast.Module(body=[harness], type_ignores=[]))
    def assemble(*args, **kwargs):
        if assembly_error is not None:
            raise assembly_error
        return SimpleNamespace(_evidence_id="synthetic_data_id")
    namespace = {"__name__": "futures_rebuild._synthetic_reporting_seam", "__package__": "futures_rebuild",
        "scope": {"trial_id": "SYNTHETIC_NOT_A_TRIAL_REGISTRATION"}, "bundle": bundle, "market": "ES",
        "binding": {"synthetic_only": True}, "extra": {"synthetic_only": True}, "output": tmp_path / "run",
        "reporting_metadata": metadata, "previous_report_catalog": previous, "expected_dataset_id": "synthetic_data_id",
        "source_contract_id": "SYNTHETIC_NOT_REGISTERABLE", "source_release_id": "SYNTHETIC_NOT_REGISTERABLE",
        "registration_sha256": "0" * 64, "receipt": SimpleNamespace(receipt_id="SYNTHETIC_NO_AUTHORITY"),
        "reader": lambda receipt: (), "c": SimpleNamespace(assemble_dataset=assemble),
        "gateway": SimpleNamespace(root=tmp_path, claim_historical_execution=forbidden),
        "run_dataset": lambda dataset, store, **kwargs: produce(store), "read_canonical_authority": lambda *a, **k: {"synthetic_calendar": True},
        "AS_OF_NS": original.AS_OF_NS, "datetime": original.datetime, "timezone": original.timezone,
        "sha256_json": sha256_json, "IntegrityError": IntegrityError}
    for name in ("run_dataset", "execute_registered_market"):
        monkeypatch.setattr(original, name, forbidden)
    for name in ("run_market_wfa", "fit_model", "predict", "replay_window", "final_refit", "evaluate_wfa"):
        monkeypatch.setattr(original.research, name, forbidden)
    import socket
    monkeypatch.setattr(socket, "socket", forbidden)
    exec(compile(tree, str(folder / "payload" / successor.PREP), "exec"), namespace)
    return namespace["synthetic_seam"], stores, store_module


def producer(values, *, complete=True, error=None):
    outcome = {"synthetic_result": "NOT_RESEARCH_EVIDENCE"}
    def produce(store):
        def attempt():
            replay = fixture_replay(values)
            replay["complete"] = complete
            store.save("outer_0/selected/adverse", replay)
            if error is not None:
                raise error
            return outcome
        return store.checkpoint("outer_0/selected", attempt)
    return produce, outcome


@pytest.mark.parametrize("values,complete,expected", [([0]*8, True, "COMPLETE"), ([1,-2,3,0,4,-1,0,2], True, "COMPLETE"), ([0]*8, False, "INCOMPLETE")])
def test_actual_prepared_seam_routes_outcomes_to_canonical_reports(proposal, tmp_path, monkeypatch, values, complete, expected):
    produce, expected_result = producer(values, complete=complete)
    execute, stores, _ = seam(proposal, tmp_path, monkeypatch, produce)
    assert execute() is expected_result
    store = stores[0]
    report = store.reports[("outer_0/selected", "adverse")]
    assert report["execution"]["status"] == expected
    assert report["execution"]["valid_no_trade"] == (complete and not any(values))
    assert report["identity"]["parent_candidate_id"] == "synthetic_parent"
    assert report["information_use"]["independence"] == "RESULT_INFORMED_REVISION_REUSE_IS_EXPLORATORY"
    assert store.reports[("runner", "adverse")]["execution"]["status"] == "COMPLETE"
    view = load_catalog(store.latest_catalog)
    assert CockpitApi(SimpleNamespace(candidate_reports=view)).candidate_reports() == view
    assert view["history"]["observed_attempt_count"] == 1
    assert all(r["research_gate_passed"] is False for r in view["reports"])


@pytest.mark.parametrize("early", [False, True])
def test_original_exception_and_early_unknown_metadata_survive(proposal, tmp_path, monkeypatch, early):
    error = RuntimeError("ORIGINAL_SYNTHETIC_RUNNER_EXCEPTION")
    produce, _ = producer([0]*8, error=error)
    execute, stores, _ = seam(proposal, tmp_path, monkeypatch, produce, assembly_error=error if early else None)
    with pytest.raises(RuntimeError) as caught:
        execute()
    assert caught.value is error
    store = stores[0]
    report = store.reports[("runner", "adverse")]
    assert report["execution"]["status"] == "FAILED" and not report["daily"]
    assert report["identity"]["attempt_id"] is None
    if not early:
        assert store.reports[("outer_0/selected", "adverse")]["execution"]["status"] == "FAILED"
        assert "ORIGINAL_SYNTHETIC_RUNNER_EXCEPTION" in store.path("outer_0/selected/failure").read_text()
    else:
        assert len(store.reports) == 1


@pytest.mark.parametrize("runner_failure,reporting_error", [(False, True), (True, True), (True, "failure_event")])
def test_report_write_failure_does_not_change_runner_outcome(proposal, tmp_path, monkeypatch, runner_failure, reporting_error, capsys):
    error = RuntimeError("ORIGINAL_ERROR") if runner_failure else None
    produce, expected = producer([0]*8, error=error)
    execute, stores, _ = seam(proposal, tmp_path, monkeypatch, produce, reporting_error=reporting_error)
    if error:
        with pytest.raises(RuntimeError) as caught:
            execute()
        assert caught.value is error
    else:
        assert execute() is expected
    store = stores[0]
    assert store.report_failure["status"] == "UNAVAILABLE_REPORTING_FAILED"
    assert store.latest_catalog is None
    assert store.path("reporting_failure").is_file()
    assert "reporting unavailable" in capsys.readouterr().err
    terminal = "failure" if error else "completed"
    assert store.path("outer_0/selected/" + terminal).is_file()


def test_original_artifact_io_error_still_propagates(proposal, tmp_path, monkeypatch):
    produce, _ = producer([0]*8)
    execute, stores, _ = seam(proposal, tmp_path, monkeypatch, produce)
    error = OSError("ORIGINAL_ARTIFACT_FAILURE")
    def save(*args, **kwargs):
        raise error
    monkeypatch.setattr(original.ResearchStore, "save", save)
    with pytest.raises(OSError) as caught:
        execute()
    assert caught.value is error


def test_interrupted_invocation_stays_incomplete_and_cannot_repeat_attempt(proposal, tmp_path, monkeypatch):
    interruption = KeyboardInterrupt("SYNTHETIC_INTERRUPT")
    def produce(store):
        return store.checkpoint("outer_0/selected", lambda: (_ for _ in ()).throw(interruption))
    execute, stores, _ = seam(proposal, tmp_path, monkeypatch, produce)
    with pytest.raises(KeyboardInterrupt) as caught:
        execute()
    assert caught.value is interruption
    store = stores[0]
    assert store.reports[("runner", "adverse")]["execution"]["status"] == "INCOMPLETE"
    assert store.reports[("outer_0/selected", "adverse")]["execution"]["status"] == "INCOMPLETE"
    assert not store.path("outer_0/selected/failure").exists()
    with pytest.raises(IntegrityError, match="no duplicate"):
        store.checkpoint("outer_0/selected", forbidden)


def test_retry_preserves_original_checkpoint_identity_and_prior_reports(proposal, tmp_path, monkeypatch):
    produce, expected = producer([0]*8)
    execute, stores, _ = seam(proposal, tmp_path, monkeypatch, produce)
    assert execute() is expected
    first = stores[0]
    checkpoint = first.path("outer_0/selected/completed").read_bytes()
    before = {p: p.read_bytes() for p in (tmp_path / "run").rglob("*.json")}
    attempt = first.reports[("outer_0/selected", "adverse")]["identity"]["attempt_id"]
    resume, resumed_stores, _ = seam(proposal, tmp_path, monkeypatch,
        lambda store: store.checkpoint("outer_0/selected", forbidden), previous=first.latest_catalog)
    assert resume() == expected
    last = resumed_stores[0]
    assert last.reports[("outer_0/selected", "adverse")]["identity"]["attempt_id"] == attempt
    assert last.path("outer_0/selected/completed").read_bytes() == checkpoint
    assert all(p.read_bytes() == raw for p, raw in before.items())
    assert load_catalog(last.latest_catalog)["history"]["observed_attempt_count"] == 1


def test_payload_binding_and_reproduction_preserve_science(proposal):
    folder, request = proposal
    assert successor.prepare(ROOT, folder, check=True) == request
    base = json.loads((ROOT / successor.BUNDLE).read_bytes())
    proposed = json.loads((folder / "proposed_bundle.json").read_bytes())
    assert all(proposed[k] == base[k] for k in base if k not in ("bundle_id", "prior_preparation", "implementation_files", "development_admission"))
    for path, digest in proposed["implementation_files"].items():
        candidate = folder / "payload" / path
        assert sha256_file(candidate if candidate.exists() else ROOT/path) == digest
    assert set(request["payload_sha256s"]) == {successor.PREP, successor.STORE, successor.ADMISSION, successor.REPORT_TEST, successor.NEXT_BUNDLE, successor.NEXT_CONTRACT}
    assert all(v is False for v in request["authority"].values())


@pytest.mark.parametrize("path", [successor.PREP, "src/futures_rebuild/l0_alpha_reporting.py", "src/futures_rebuild/research/__init__.py"])
def test_stale_base_and_transitive_dependency_invalidate_request(proposal, monkeypatch, path):
    folder, _ = proposal
    actual = successor.sha256_file
    monkeypatch.setattr(successor, "sha256_file", lambda p: "f"*64 if Path(p) == ROOT/path else actual(p))
    with pytest.raises(IntegrityError, match="drift|stale"):
        successor.prepare(ROOT, folder, check=True)


def test_gate_prefix_and_original_store_remain_exact(proposal):
    folder, _ = proposal
    before = ast.parse((ROOT / successor.PREP).read_text())
    after = ast.parse((folder / "payload" / successor.PREP).read_text())
    for name in ("ResearchStore", "run_dataset"):
        original_node = next(n for n in before.body if getattr(n, "name", None) == name)
        candidate_node = next(n for n in after.body if getattr(n, "name", None) == name)
        assert ast.dump(original_node) == ast.dump(candidate_node)
    a, b = [next(n for n in t.body if getattr(n, "name", None) == "execute_registered_market") for t in (before, after)]
    aw, bw = [next(n for n in f.body if isinstance(n, ast.With)) for f in (a,b)]
    assert [ast.dump(n) for n in a.body[:a.body.index(aw)]] == [ast.dump(n) for n in b.body[:b.body.index(bw)]]
    assert [ast.dump(n) for n in aw.body[:3]] == [ast.dump(n) for n in bw.body[:3]]
    assert [ast.dump(n) for n in aw.items] == [ast.dump(n) for n in bw.items]
    protected_tail = next(n for n in bw.body if isinstance(n, ast.Try))
    assert [ast.dump(n) for n in aw.body[3:6]] == [ast.dump(n) for n in protected_tail.body[:3]]
    successor.verify_inputs(ROOT, json.loads((folder / "input_manifest.json").read_bytes()))


def test_prepared_existing_hook_regression_uses_new_reviewed_failure_rule(proposal, tmp_path, monkeypatch):
    folder, _ = proposal
    load_store(folder / "payload" / successor.STORE, monkeypatch)
    path = folder / "payload" / successor.REPORT_TEST
    namespace = {"__name__": "synthetic_existing_hook_test", "__file__": str(path)}
    exec(compile(path.read_bytes(), str(path), "exec"), namespace)
    namespace["test_reporting_store_uses_lifecycle_hooks_without_fit_replay_or_registration"](tmp_path / "synthetic_hook", monkeypatch)


def test_modified_payload_is_rejected_without_rebinding(proposal):
    folder, _ = proposal
    path = folder / "payload" / successor.STORE
    path.write_bytes(path.read_bytes() + b"\n# synthetic tamper\n")
    with pytest.raises(IntegrityError, match="payload differs"):
        successor.prepare(ROOT, folder, check=True)


def test_projected_bundle_and_contract_match_existing_metadata_builders(proposal, tmp_path, monkeypatch):
    folder, _ = proposal
    bundle = json.loads((folder / "proposed_bundle.json").read_bytes())
    contract = json.loads((folder / "proposed_contract.json").read_bytes())
    overlay = tmp_path / "SYNTHETIC_METADATA_ONLY_NOT_RESEARCH"
    names = set(bundle["implementation_files"]) | set(contract["foundation_bindings"])
    names.update((successor.BUNDLE, successor.NEXT_BUNDLE, successor.NEXT_CONTRACT,
                  bundle["predecessor"]["path"], bundle["entry_calendar"]["path"]))
    for name in names:
        source = folder / "payload" / name
        source = source if source.exists() else ROOT / name
        destination = overlay / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source.read_bytes())
    candidate = ModuleType("futures_rebuild._proposed_preparation")
    candidate.__file__ = str(overlay / successor.PREP)
    exec(compile((overlay / successor.PREP).read_bytes(), candidate.__file__, "exec"), candidate.__dict__)
    # Reuse the exact frozen inventory/source-plan metadata. No catalog rescan or row read.
    candidate.metadata_inventory = lambda root: deepcopy(bundle["l1_inventory"])
    from futures_rebuild import l0_alpha_source_readiness
    monkeypatch.setattr(l0_alpha_source_readiness, "prepare", lambda root: deepcopy(bundle["historical_readiness_proposal"]))
    monkeypatch.setattr(original, "execute_registered_market", forbidden)
    for name in ("run_dataset", "run_synthetic", "run_rehearsal"):
        if hasattr(candidate, name):
            setattr(candidate, name, forbidden)
    assert candidate.build_bundle(overlay) == bundle
    from futures_rebuild import l0_development_admission
    monkeypatch.setattr(original, "CONFIG", successor.NEXT_BUNDLE)
    monkeypatch.setattr(original, "FILES", candidate.FILES)
    monkeypatch.setattr(l0_development_admission, "build_certificate", forbidden)
    monkeypatch.setattr(l0_development_admission, "validate_certificate", forbidden)
    assert l0_development_admission.build_contract(overlay) == contract
