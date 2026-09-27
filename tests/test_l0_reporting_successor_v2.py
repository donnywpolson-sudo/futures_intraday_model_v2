"""Exact inactive payload seam; synthetic metadata/accounting only, no research."""
from copy import deepcopy
import importlib.util
import json
from pathlib import Path

import pytest

from futures_rebuild.canonical import canonical_authority_file_bytes, sha256_file, sha256_json
from futures_rebuild.errors import IntegrityError, LeaseBusy
from futures_rebuild.locking import FileLease
from futures_rebuild import l0_alpha_reporting as reporting

ROOT = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


legacy = load("_prior_seam_tests", ROOT / "tests/test_l0_reporting_successor.py")
successor = load("_fresh_reporting_builder", ROOT / "scripts/prepare_l0_reporting_successor_v2.py")
pytestmark = pytest.mark.current


@pytest.fixture
def proposal(tmp_path, request):
    if getattr(request, "param", None) == "rejected":
        folder = ROOT / successor.old.OUTPUT
        return folder, successor.old.prepare(ROOT, folder, check=True)
    folder = tmp_path / "proposal"
    return folder, successor.prepare(ROOT, folder)


# Reuse the existing strict assertions, exercising the corrected payload.
test_outcomes = legacy.test_actual_prepared_seam_routes_outcomes_to_canonical_reports
test_original_exceptions = legacy.test_original_exception_and_early_unknown_metadata_survive
test_report_errors = legacy.test_report_write_failure_does_not_change_runner_outcome
test_original_artifact_error = legacy.test_original_artifact_io_error_still_propagates
test_interruption = legacy.test_interrupted_invocation_stays_incomplete_and_cannot_repeat_attempt
test_retry = legacy.test_retry_preserves_original_checkpoint_identity_and_prior_reports
test_hook_regression = legacy.test_prepared_existing_hook_regression_uses_new_reviewed_failure_rule


def files(directory):
    return {p.relative_to(directory).as_posix(): p.read_bytes() for p in directory.rglob("*") if p.is_file()}


def initial(proposal, tmp_path, monkeypatch, *, error=None, complete=True):
    early = []
    produce, expected = legacy.producer([0] * 8, complete=complete, error=error)
    def capture(store):
        early.append(store.latest_catalog)
        return produce(store)
    execute, stores, module = legacy.seam(proposal, tmp_path, monkeypatch, capture)
    if error:
        with pytest.raises(type(error)) as caught:
            execute()
        assert caught.value is error
    else:
        assert execute() is expected
    return stores[0], early[0], module


def reject(proposal, tmp_path, monkeypatch, catalog, *, match=None):
    before = files(tmp_path / "run")
    reached = []
    execute, _, _ = legacy.seam(proposal, tmp_path, monkeypatch,
        lambda store: store.checkpoint("outer_0/selected", legacy.forbidden), previous=catalog)
    def reader(receipt):
        reached.append(True)
        return ()
    execute.__globals__["reader"] = reader
    with pytest.raises(IntegrityError, match=match):
        execute()
    assert not reached
    assert files(tmp_path / "run") == before


@pytest.mark.parametrize("proposal", [
    pytest.param("rejected", marks=pytest.mark.xfail(strict=True, raises=pytest.fail.Exception,
        reason="Rejected exact payload accepts stale catalog before reader")),
    "corrected",
], indirect=True)
def test_exact_stale_catalog_regression(proposal, tmp_path, monkeypatch):
    store, early, _ = initial(proposal, tmp_path, monkeypatch)
    assert early != store.latest_catalog
    assert store.path("outer_0/selected/completed").is_file()
    reject(proposal, tmp_path, monkeypatch, early)


def forged_catalog(store, reports, module):
    """Even a self-consistent catalog claim cannot replace checkpoint references."""
    path = reporting.write_catalog(store.root / "candidate_reports", reports)
    saved = store.reports
    store.reports = {(r["identity"]["operation_id"], "adverse"): r for r in reports}
    binding = store._catalog_binding(path, store._checkpoint_state())
    store.reports = saved
    binding["binding_id"] = sha256_json(binding)
    module.write_new_canonical_authority(path.with_suffix(".checkpoints.json"), binding)
    return path


@pytest.mark.parametrize("field,value", [("candidate_id", "wrong_candidate"), ("attempt_id", "f" * 64),
    ("operation_id", "outer_0/omitted_selected"), ("run_id", "foreign_run")])
def test_same_count_wrong_identities_rejected(proposal, tmp_path, monkeypatch, field, value):
    store, _, module = initial(proposal, tmp_path, monkeypatch)
    reports = deepcopy(list(store.reports.values()))
    target = next(r for r in reports if r["record_kind"] == "ATTEMPT")
    target["identity"][field] = value
    target["report_id"] = sha256_json({k: v for k, v in target.items() if k != "report_id"})
    path = forged_catalog(store, reports, module)
    assert len(reporting.load_catalog(path)["reports"]) == len(store.reports)
    reject(proposal, tmp_path, monkeypatch, path, match="references|identity")


@pytest.mark.parametrize("case", ["missing_catalog", "missing_metadata", "missing_start", "missing_checkpoint",
    "missing_catalog_binding", "foreign_path", "changed_checkpoint", "terminal_conflict", "foreign_context", "status_conflict"])
def test_missing_foreign_conflicting_references(proposal, tmp_path, monkeypatch, case):
    store, _, module = initial(proposal, tmp_path, monkeypatch)
    path = store.latest_catalog
    marker = store.path("outer_0/selected/completed")
    reference = store.path("report_checkpoint_bindings/outer_0/selected/completed")
    if case == "missing_catalog":
        path = None
    elif case == "missing_metadata":
        reference.unlink()  # Task-owned synthetic fixture only.
    elif case == "missing_start":
        store.path("outer_0/selected/started").unlink()
    elif case == "missing_checkpoint":
        marker.unlink()
    elif case == "missing_catalog_binding":
        path.with_suffix(".checkpoints.json").unlink()
    elif case == "foreign_path":
        foreign = tmp_path / "foreign" / path.name
        foreign.parent.mkdir()
        foreign.write_bytes(path.read_bytes())
        path = foreign
    elif case == "changed_checkpoint":
        marker.write_bytes(marker.read_bytes() + b" ")
    elif case == "terminal_conflict":
        store.save("outer_0/selected/failure", {"error": "SYNTHETIC_CONFLICT"})
    else:
        metadata = json.loads(reference.read_bytes())
        if case == "foreign_context":
            metadata["store_context_sha256"] = "f" * 64
        else:
            metadata["required_reports"]["adverse"] = "FAILED"
        metadata.pop("metadata_id")
        metadata["metadata_id"] = sha256_json(metadata)
        reference.write_bytes(canonical_authority_file_bytes(metadata))
        if case == "status_conflict":
            binding = store._catalog_binding(path, store._checkpoint_state())
            binding["binding_id"] = sha256_json(binding)
            path.with_suffix(".checkpoints.json").write_bytes(canonical_authority_file_bytes(binding))
    reject(proposal, tmp_path, monkeypatch, path)


@pytest.mark.parametrize("failure_at", ["metadata", "catalog", "binding"])
def test_original_save_then_report_failure_without_durable_notice(proposal, tmp_path, monkeypatch, failure_at):
    from futures_rebuild import canonical
    old_catalog = []
    execute, stores, module = legacy.seam(proposal, tmp_path, monkeypatch, legacy.forbidden)
    write = module.write_new_canonical_authority
    def no_report_storage(path, value, *args, **kwargs):
        raise OSError("SYNTHETIC_REPORT_STORAGE_UNAVAILABLE")
    def no_failure_notice(path, *args, **kwargs):
        if Path(path).name == "reporting_failure.json":
            raise OSError("SYNTHETIC_FAILURE_NOTICE_STORAGE_UNAVAILABLE")
        return write(path, *args, **kwargs)
    def no_binding(path, *args, **kwargs):
        if str(path).endswith(".checkpoints.json"):
            raise OSError("SYNTHETIC_BINDING_STORAGE_UNAVAILABLE")
        return write(path, *args, **kwargs)
    def produce(store):
        def operation():
            store.save("outer_0/selected/adverse", legacy.fixture_replay([0] * 8))
            old_catalog.append(store.latest_catalog)
            monkeypatch.setattr(canonical, "write_new_canonical_authority", no_failure_notice)
            if failure_at == "metadata":
                monkeypatch.setattr(module, "write_new_canonical_authority", no_report_storage)
            elif failure_at == "binding":
                monkeypatch.setattr(module, "write_new_canonical_authority", no_binding)
            else:
                monkeypatch.setattr(module, "write_catalog", no_report_storage)
            return {"synthetic": "COMPLETE_ORIGINAL_RESULT"}
        return store.checkpoint("outer_0/selected", operation)
    execute.__globals__["run_dataset"] = lambda data, store, **kw: produce(store)
    result = execute()
    store = stores[0]
    assert result == {"synthetic": "COMPLETE_ORIGINAL_RESULT"}
    assert store.report_failure is not None and store.latest_catalog is None
    assert not store.path("reporting_failure").exists()
    assert store.path("outer_0/selected/completed").is_file()
    monkeypatch.setattr(module, "write_new_canonical_authority", write)
    reject(proposal, tmp_path, monkeypatch, old_catalog[0], match="missing metadata|stale catalog")


def test_initial_empty_state_is_valid_and_does_not_invent_candidate(proposal, tmp_path, monkeypatch):
    result = {"SYNTHETIC": "NO_ATTEMPT"}
    execute, stores, _ = legacy.seam(proposal, tmp_path, monkeypatch, lambda store: result)
    assert execute() is result
    view = reporting.load_catalog(stores[0].latest_catalog)
    assert view["history"]["observed_attempt_count"] == 0
    assert all(r["execution"]["valid_no_trade"] is False for r in view["reports"])


def test_gate_prefix_and_scientific_store_unchanged(proposal, monkeypatch):
    def verify(root, manifest):
        assert successor.snapshot(root) == manifest
    monkeypatch.setattr(legacy.successor, "verify_inputs", verify)
    legacy.test_gate_prefix_and_original_store_remain_exact(proposal)


@pytest.mark.parametrize("state", ["failed", "incomplete", "interrupted"])
def test_current_failed_incomplete_catalog_preserves_semantics(proposal, tmp_path, monkeypatch, state):
    error = RuntimeError("SYNTHETIC_FAILURE") if state == "failed" else None
    if state == "interrupted":
        def interrupt(store):
            return store.checkpoint("outer_0/selected", lambda: (_ for _ in ()).throw(KeyboardInterrupt()))
        execute, stores, _ = legacy.seam(proposal, tmp_path, monkeypatch, interrupt)
        with pytest.raises(KeyboardInterrupt):
            execute()
        store = stores[0]
    else:
        store, _, _ = initial(proposal, tmp_path, monkeypatch, error=error, complete=state != "incomplete")
    before = files(store.root)
    module = legacy.load_store(proposal[0] / "payload" / successor.old.STORE, monkeypatch)
    resumed = module.ReportingStore(store.root, store.context, report_contexts=store.declared_report_contexts,
        runner_context=store.runner_context, previous_catalog=store.latest_catalog)
    assert resumed.reports == store.reports
    assert not reporting.load_catalog(resumed.latest_catalog)["reports"][0]["research_gate_passed"]
    if state in ("failed", "interrupted"):
        with pytest.raises(IntegrityError, match="no duplicate"):
            resumed.checkpoint("outer_0/selected", legacy.forbidden)
    assert files(store.root) == before


def test_invalid_leaf_completed_checkpoint_stays_failed(proposal, tmp_path, monkeypatch):
    result = {"status": "INVALID_LEAF_SUPPORT"}
    execute, stores, _ = legacy.seam(proposal, tmp_path, monkeypatch,
        lambda s: s.checkpoint("outer_0/selected", lambda: result))
    assert execute() is result
    store = stores[0]
    assert store.reports[("outer_0/selected", "adverse")]["execution"]["status"] == "FAILED"
    resume, _, _ = legacy.seam(proposal, tmp_path, monkeypatch,
        lambda s: s.checkpoint("outer_0/selected", legacy.forbidden), previous=store.latest_catalog)
    assert resume() == result


def test_changed_checkpoint_during_validation_is_rejected(proposal, tmp_path, monkeypatch):
    store, _, _ = initial(proposal, tmp_path, monkeypatch)
    execute, _, module = legacy.seam(proposal, tmp_path, monkeypatch, legacy.forbidden, previous=store.latest_catalog)
    state = module.ReportingStore._checkpoint_state
    count = []
    def changed(self):
        result = state(self)
        count.append(True)
        return result if len(count) == 1 else {}
    monkeypatch.setattr(module.ReportingStore, "_checkpoint_state", changed)
    execute.__globals__["reader"] = legacy.forbidden
    before = files(store.root)
    with pytest.raises(IntegrityError, match="changed during"):
        execute()
    assert files(store.root) == before


def test_existing_writer_lease_excludes_a_second_writer(proposal, tmp_path, monkeypatch):
    store, _, _ = initial(proposal, tmp_path, monkeypatch)
    produce = lambda s: s.checkpoint("outer_0/selected", legacy.forbidden)
    execute, _, _ = legacy.seam(proposal, tmp_path, monkeypatch, produce, previous=store.latest_catalog)
    def reader(receipt):
        with pytest.raises(LeaseBusy):
            with FileLease(store.root / "writer.lock"):
                pytest.fail("second writer acquired the existing lease")
        return ()
    execute.__globals__["reader"] = reader
    with FileLease(store.root / "writer.lock"):
        execute()
    # The real registered function holds this same lease around validation and reader use.
    import ast
    source = (proposal[0] / "payload" / successor.old.PREP).read_text()
    function = next(n for n in ast.parse(source).body if getattr(n, "name", None) == "execute_registered_market")
    lease = next(n for n in function.body if isinstance(n, ast.With))
    assert "FileLease" in ast.unparse(lease.items[0].context_expr)
    assert "ReportingStore" in ast.unparse(lease) and "reader(receipt)" in ast.unparse(lease)


def test_metadata_builders_match_and_drift_is_rejected(proposal, tmp_path, monkeypatch):
    legacy.test_projected_bundle_and_contract_match_existing_metadata_builders(proposal, tmp_path, monkeypatch)
    folder, request = proposal
    assert successor.prepare(ROOT, folder, check=True) == request
    digest = successor.sha256_file
    monkeypatch.setattr(successor, "sha256_file", lambda path: "f" * 64 if Path(path) == ROOT / successor.TEST else digest(path))
    with pytest.raises(IntegrityError, match="drift"):
        successor.prepare(ROOT, folder, check=True)


def test_payload_tamper_rejected(proposal):
    folder, _ = proposal
    path = folder / "payload" / successor.old.STORE
    path.write_bytes(path.read_bytes() + b"\n# SYNTHETIC_TAMPER\n")
    with pytest.raises(IntegrityError, match="payload differs"):
        successor.prepare(ROOT, folder, check=True)
