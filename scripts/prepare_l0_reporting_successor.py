"""Create/check an inactive L0 reporting patch; never admit or execute research.

Uses the existing L0 proposed_bundle/proposed_contract and immutable binding-map
format. Candidate source files live only in this task's payload directory.
"""
import argparse
import ast
from copy import deepcopy
import difflib
from importlib.metadata import version
import json
from pathlib import Path

from futures_rebuild.canonical import (canonical_authority_file_bytes, read_canonical_authority,
    sha256_bytes, sha256_file, sha256_json, write_new_canonical_authority as write)
from futures_rebuild.errors import IntegrityError

OUTPUT = Path("state/unpublished_evidence/l0_alpha_candidate_v1/reporting_successor_v1")
PREP = "src/futures_rebuild/l0_alpha_preparation.py"
STORE = "src/futures_rebuild/l0_alpha_report_store.py"
ADMISSION = "src/futures_rebuild/l0_development_admission.py"
BUNDLE = "configs/l0_alpha_candidate_v7.json"
CONTRACT = "configs/l0_development_admission_v2.json"
NEXT_BUNDLE = "configs/l0_alpha_candidate_v8.json"
NEXT_CONTRACT = "configs/l0_development_admission_v3.json"
BUILDER = "scripts/prepare_l0_reporting_successor.py"
TEST = "tests/test_l0_reporting_successor.py"
REPORT_TEST = "tests/test_l0_alpha_reporting.py"
PRIOR = "state/unpublished_evidence/l0_alpha_candidate_v1/reporting_implementation_v2/task_report.md"
VIEW_FILES = ("src/futures_rebuild/live_cockpit/app.py", "src/futures_rebuild/live_cockpit/assets/index.html",
    "src/futures_rebuild/live_cockpit/assets/styles.css", "src/futures_rebuild/live_cockpit/assets/candidate-reports.js")
ABSENT = ("configs/active_l0_development_admission.json", NEXT_BUNDLE, NEXT_CONTRACT)
FAILURE_POLICY = {
    "state": "PROPOSED_FOR_REVIEW_NOT_ADOPTED",
    "initialization": "INVALID_METADATA_OR_STALE_RESUME_FAILS_BEFORE_SOURCE_READER;NO_FALLBACK_STORE",
    "original_artifact_write": "PROPAGATE_UNCHANGED;EXISTING_CHECKPOINT_RULES_APPLY",
    "derived_report_write": "NONFATAL_AFTER_ORIGINAL_SAVE;DISABLE_FURTHER_REPORTING_FOR_THIS_INVOCATION",
    "notification": "IN_MEMORY_STATUS_AND_STDERR;CURRENT_CATALOG_UNAVAILABLE;CREATE_ONLY_FAILURE_RECORD_BEST_EFFORT",
    "research_result": "RETURN_OR_RERAISE_ORIGINAL_OBJECT_UNCHANGED;NO_REPORT_STATUS_INSIDE_RESEARCH_RESULT",
    "resume": "EXPLICIT_PREVIOUS_CATALOG_AND_ORIGINAL_CHECKPOINT_GUARDS;REPORT_FAILURE_REQUIRES_REVIEW_NO_AUTOMATIC_RETRY",
    "limitation": "IF_STORAGE_AND_STDERR_BOTH_FAIL_ONLY_IN_MEMORY_NOTICE_SURVIVES;NO_CLAIM_OF_DURABLE_REPORT_DELIVERY",
}


def bind(value, field):
    return {**value, field: sha256_json(value)}


def replace_once(text, before, after):
    if text.count(before) != 1:
        raise IntegrityError("prepared source seam changed")
    return text.replace(before, after, 1)


def reporting_dependencies(root, existing):
    """Static local import closure, including package initializers; stop at bound foundations."""
    pending = {STORE, "src/futures_rebuild/l0_alpha_reporting.py"}
    found = set()
    while pending:
        name = pending.pop()
        if name in found or name in existing:
            continue
        path = root / name
        if not path.is_file():
            raise IntegrityError("missing executable reporting dependency: " + name)
        found.add(name)
        module = name.removeprefix("src/").removesuffix(".py").replace("/", ".")
        package = module.removesuffix(".__init__") if module.endswith(".__init__") else module.rpartition(".")[0]
        parts = package.split(".")
        for i in range(1, len(parts) + 1):
            initializer = "src/" + "/".join(parts[:i]) + "/__init__.py"
            if (root / initializer).is_file():
                pending.add(initializer)
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            modules = []
            if isinstance(node, ast.Import):
                modules = [a.name for a in node.names if a.name.startswith("futures_rebuild")]
            elif isinstance(node, ast.ImportFrom):
                prefix = ".".join(parts[:len(parts) - node.level + 1]) if node.level else ""
                target = ".".join(x for x in (prefix, node.module) if x)
                if target.startswith("futures_rebuild"):
                    modules = [target, *[target + "." + a.name for a in node.names]]
            for target in modules:
                stem = "src/" + target.replace(".", "/")
                for candidate in (stem + ".py", stem + "/__init__.py"):
                    if (root / candidate).is_file():
                        pending.add(candidate)
    return sorted(found)


def patched_sources(root, dependencies):
    preparation = (root / PREP).read_text(encoding="utf-8")
    preparation = replace_once(preparation, f'CONFIG = "{BUNDLE}"', f'CONFIG = "{NEXT_BUNDLE}"')
    preparation = replace_once(preparation, 'AS_OF_NS = int(',
        'FILES += ' + repr(tuple([*dependencies, REPORT_TEST, TEST])) + '\nAS_OF_NS = int(')
    preparation = preparation.replace('"configs/l0_alpha_candidate_v6.json"', f'"{BUNDLE}"')
    preparation = preparation.replace('"APPROVED_INACTIVE_CALENDAR_AND_PRODUCER_SUCCESSOR;SCIENTIFIC_DESIGN_UNCHANGED"',
        '"REPORTING_LIFECYCLE_SUCCESSOR;SCIENTIFIC_DESIGN_UNCHANGED"')
    preparation = preparation.replace(f'"{CONTRACT}"', f'"{NEXT_CONTRACT}"')
    preparation = replace_once(preparation,
        'market, expected_dataset_id, source_contract_id, source_release_id, reader):',
        'market, expected_dataset_id, source_contract_id, source_release_id, reader,\n                              reporting_metadata=None, previous_report_catalog=None):')
    start = '        dataset = c.assemble_dataset(market, reader(receipt), as_of_ns=AS_OF_NS,'
    stop = '        return run_dataset(dataset, ResearchStore(output, extra), calendar=calendar, calendar_view=bundle["entry_calendar"]["research_view"])'
    first, last = preparation.index(start), preparation.index(stop) + len(stop)
    old_tail = preparation[first:last]
    tail = old_tail.replace('return run_dataset(dataset, ResearchStore(output, extra),', 'result = run_dataset(dataset, store,')
    integration = '''        from .l0_alpha_report_store import ReportingStore, l0_context, operation_contexts
        metadata = dict(reporting_metadata or {})
        if set(metadata) - {"parent_candidate_id", "hypothesis", "intended_change", "lineage", "intended_policy_changes"}:
            raise IntegrityError("undeclared reporting metadata fields")
        base = l0_context(bundle, market, experiment_id=scope["trial_id"], run_id=sha256_json(binding),
            provenance={"working_file_sha256s": bundle["implementation_files"],
                        "working_code_identity": sha256_json(bundle["implementation_files"]),
                        "basis": "EXACT_WORKING_BYTES_VERIFIED_BY_VALIDATE_BUNDLE;INCLUDES_UNCOMMITTED_CODE"},
            lineage=metadata.pop("lineage", None))
        base.update(metadata)
        base["provenance"].update(data_identity=expected_dataset_id, source_contract_id=source_contract_id,
            source_release_id=source_release_id, registration_sha256=registration_sha256, receipt_id=receipt.receipt_id)
        base["scope"].update(observation_cutoff_utc=datetime.fromtimestamp(AS_OF_NS / 10**9, timezone.utc).isoformat(),
                             data_kind="GOVERNED_L0_DEVELOPMENT")
        store = ReportingStore(output, extra, report_contexts=operation_contexts(bundle, base),
                               runner_context=base, previous_catalog=previous_report_catalog)
        store.observe_run("INCOMPLETE")
        try:
'''
    integration += "\n".join("    " + line for line in tail.splitlines())
    integration += '''
        except Exception as exc:
            store.observe_run("FAILED", type(exc).__name__)
            raise
        store.observe_run("COMPLETE")
        return result'''
    preparation = preparation[:first] + integration + preparation[last:]
    admission = replace_once((root / ADMISSION).read_text(encoding="utf-8"),
                             f'CONTRACT_PATH = Path("{CONTRACT}")', f'CONTRACT_PATH = Path("{NEXT_CONTRACT}")')
    store = (root / STORE).read_text(encoding="utf-8")
    store = replace_once(store, 'import subprocess\n', 'import subprocess\nimport sys\n')
    store = replace_once(store, 'clock=None, previous_catalog=None):', 'clock=None, previous_catalog=None, runner_context=None):')
    store = replace_once(store, '        self.report_contexts = deepcopy(report_contexts)',
        '''        self.runner_context = deepcopy(runner_context)
        self.report_failure = None
        self.report_contexts = deepcopy(report_contexts)''')
    store = replace_once(store, '        if (self.root / "candidate_reports").exists() and previous_catalog is None:',
        '''        if self.path("reporting_failure").exists():
            raise IntegrityError("prior reporting failure requires review; no automatic retry")
        if (self.root / "candidate_reports").exists() and previous_catalog is None:''')
    store = replace_once(store, '        super().save(name, value)\n        if name == "run":',
        '''        super().save(name, value)
        self._observe_safely(name, value)

    def observe_run(self, status, reason=None):
        self._observe_safely("__runner_status__", {"status": status, "reason": reason})

    def _observe_safely(self, name, value):
        # Proposed review rule: report I/O cannot change an original research result or exception.
        if self.report_failure is not None:
            return
        try:
            self._observe(name, value)
        except Exception as exc:
            self.latest_catalog = None
            self.report_failure = {"status": "UNAVAILABLE_REPORTING_FAILED", "event": name,
                "error_type": type(exc).__name__, "store_context_sha256": sha256_json(self.context),
                "research_outcome_unchanged": True, "automatic_retry": False}
            try:
                from .canonical import write_new_canonical_authority
                write_new_canonical_authority(self.path("reporting_failure"), self.report_failure)
            except Exception:
                pass  # Storage failure can prevent durable notice; in-memory status remains explicit.
            try:
                print("L0 reporting unavailable; research outcome unchanged: " + type(exc).__name__, file=sys.stderr)
            except Exception:
                pass

    def _observe(self, name, value):
        if name == "__runner_status__":
            if self.runner_context is None:
                raise IntegrityError("runner reporting context was not declared")
            if value["status"] == "INCOMPLETE":
                self.runner_context["started_at_utc"] = self.clock()
            ctx = deepcopy(self.runner_context)
            ctx.update(candidate_id="L0_RUN_LIFECYCLE", record_kind="RUN_LIFECYCLE", operation_id="runner",
                       attempt_id=None, finished_at_utc=self.clock() if value["status"] != "INCOMPLETE" else None)
            ctx["scope"].update(evaluation_stage="UNKNOWN", dates=None, fold_by_date=None, segment_by_date=None)
            ctx["provenance"]["store_context_sha256"] = sha256_json(self.context)
            self.reports[("runner", "adverse")] = candidate_report(ctx, execution_status=value["status"], failure_reason=value["reason"])
            self._publish()
            return
        if name == "run":''')
    report_test = replace_once((root / REPORT_TEST).read_text(encoding="utf-8"),
        '''    with pytest.raises(IntegrityError, match="ordered and unique"):
        resumed.save("run", {"synthetic": "PRECOMPUTED_FIXTURE_ONLY"})''',
        '''    resumed.save("run", {"synthetic": "PRECOMPUTED_FIXTURE_ONLY"})
    assert resumed.report_failure["status"] == "UNAVAILABLE_REPORTING_FAILED"
    assert resumed.report_failure["error_type"] == "IntegrityError"''')
    return {PREP: preparation.encode(), ADMISSION: admission.encode(), STORE: store.encode(), REPORT_TEST: report_test.encode()}


def snapshot(root):
    bundle = read_canonical_authority(root / BUNDLE, identity_field="bundle_id")
    contract = read_canonical_authority(root / CONTRACT, identity_field="contract_id")
    dependencies = reporting_dependencies(root, bundle["implementation_files"])
    files = set(bundle["implementation_files"]) | set(contract["foundation_bindings"]) | set(dependencies)
    metadata_bindings = [contract["bundle"], bundle["entry_calendar"], bundle["prior_preparation"], bundle["predecessor"],
        {"path": "data/active/catalog.json", "sha256": bundle["historical_readiness_proposal"]["catalog_sha256"]}]
    metadata_bindings += [{"path": v["catalog_path"], "sha256": v["catalog_sha256"]}
                          for v in bundle["l1_inventory"].values() if "catalog_path" in v]
    files.update(b["path"] for b in metadata_bindings)
    files.update((BUNDLE, CONTRACT, BUILDER, TEST, PRIOR, "docs/L0_REPORTING.md", "docs/L0_ALPHA_CANDIDATE.md",
                  "CURRENT_WORKFLOW.md", "AGENTS.md", "tests/test_l0_alpha_reporting.py",
                  "tests/test_research_hac_bootstrap_rw.py", "src/futures_rebuild/l0_alpha_report_demo.py",
                  bundle["entry_calendar"]["path"], bundle["prior_preparation"]["path"], bundle["predecessor"]["path"], *VIEW_FILES))
    hashes = {p: sha256_file(root / p) for p in sorted(files)}
    if any(hashes[p] != h for p, h in bundle["implementation_files"].items()):
        raise IntegrityError("V7 bound implementation already drifted")
    if any(hashes[p] != h for p, h in contract["foundation_bindings"].items()):
        raise IntegrityError("V2 admission foundation drifted")
    if any(hashes[b["path"]] != b["sha256"] for b in metadata_bindings):
        raise IntegrityError("bound preparation metadata drifted")
    runtime = {name: version(name) for name in bundle["runtime_versions"]}
    if runtime != bundle["runtime_versions"]:
        raise IntegrityError("bound runtime versions drifted")
    if any((root / p).exists() for p in ABSENT):
        raise IntegrityError("L0 activation or proposed target already exists; reassess scope")
    return bind({"schema_version": "l0_reporting_successor_inputs/1", "files": hashes, "runtime_versions": runtime,
                 "absent": list(ABSENT), "additional_reporting_dependencies": dependencies,
                 "base_bundle_id": bundle["bundle_id"], "base_contract_id": contract["contract_id"]}, "input_manifest_id")


def verify_inputs(root, manifest):
    if manifest != snapshot(root):
        raise IntegrityError("stale base or dependency drift; prepared request cannot be rebound")


def _write_bytes(path, content, check):
    if path.exists():
        if path.read_bytes() != content:
            raise IntegrityError("prepared payload differs: " + str(path))
    elif check:
        raise IntegrityError("prepared payload missing: " + str(path))
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as stream:
            stream.write(content)


def prepare(root, output=None, *, check=False):
    root = Path(root).resolve()
    output = Path(output) if output is not None else root / OUTPUT
    manifest_path = output / "input_manifest.json"
    if manifest_path.exists():
        manifest = read_canonical_authority(manifest_path, identity_field="input_manifest_id")
        verify_inputs(root, manifest)
    else:
        if check:
            raise IntegrityError("prepare input manifest missing")
        manifest = snapshot(root)
    sources = patched_sources(root, manifest["additional_reporting_dependencies"])
    bundle = json.loads((root / BUNDLE).read_bytes())
    contract = json.loads((root / CONTRACT).read_bytes())
    proposed = deepcopy(bundle)
    proposed.pop("bundle_id")
    for name in [*manifest["additional_reporting_dependencies"], REPORT_TEST, TEST]:
        proposed["implementation_files"][name] = manifest["files"][name]
    for name, content in sources.items():
        proposed["implementation_files"][name] = sha256_bytes(content)
    proposed["prior_preparation"].update(path=BUNDLE, sha256=manifest["files"][BUNDLE],
        revision="REPORTING_LIFECYCLE_SUCCESSOR;SCIENTIFIC_DESIGN_UNCHANGED")
    proposed["development_admission"]["contract_path"] = NEXT_CONTRACT
    proposed = bind(proposed, "bundle_id")
    next_contract = deepcopy(contract)
    next_contract.pop("contract_id")
    next_contract.update(bundle={"path": NEXT_BUNDLE, "sha256": sha256_bytes(canonical_authority_file_bytes(proposed))}, bundle_id=proposed["bundle_id"])
    next_contract = bind(next_contract, "contract_id")
    payloads = {**sources, NEXT_BUNDLE: canonical_authority_file_bytes(proposed), NEXT_CONTRACT: canonical_authority_file_bytes(next_contract)}
    patch = "".join("".join(difflib.unified_diff((root / p).read_text(encoding="utf-8").splitlines(True), data.decode().splitlines(True),
                         fromfile="a/" + p, tofile="b/" + p)) for p, data in sources.items()).encode()
    payload_hashes = {name: sha256_bytes(data) for name, data in sorted(payloads.items())}
    request = bind({"schema_version": "l0_reporting_successor_review_request/1", "state": "PREPARED_NOT_ADMITTED_NOT_ACTIVATED",
        "input_manifest_id": manifest["input_manifest_id"], "base_bundle_id": bundle["bundle_id"], "base_contract_id": contract["contract_id"],
        "proposed_bundle_id": proposed["bundle_id"], "proposed_contract_id": next_contract["contract_id"],
        "payload_sha256s": payload_hashes, "patch_sha256": sha256_bytes(patch), "report_write_policy": FAILURE_POLICY,
        "schemas": {"report": "l0_alpha_report/2", "view": "l0_alpha_comparison_view/2", "catalog": "l0_alpha_report_catalog/2",
                    "preparation": proposed["schema_version"], "admission_contract": next_contract["schema_version"]},
        "proposed_admission_scope": deepcopy(contract["scope"]), "requirements_unchanged": deepcopy(contract["requirements"]),
        "requested_action": "REVIEW_EXACT_INACTIVE_SOURCE_AND_PREPARATION_SUCCESSOR_AND_REPORT_FAILURE_POLICY;NO_READINESS_CERTIFICATE_OR_ACTIVATION",
        "next_readiness_entrypoint": "l0_development_admission.build_certificate/validate_certificate;NOT_RUN;COMPLETE_VALID_INPUTS_AND_SEPARATE_AUTHORITY_REQUIRED",
        "authority": {k: False for k in ("admitted", "activation", "registration", "research_execution", "provider", "holdout", "forward", "promotion", "trading", "staging", "commit", "push")},
        "scientific_design_unchanged": proposed["design"] == bundle["design"],
        "old_binding_expected": "CURRENT_V7_VALIDATOR_REJECTS_PROPOSED_V8_BINDINGS;NO_ACCEPTED_HASH_REFRESH"}, "request_id")
    output.mkdir(parents=True, exist_ok=True)
    documents = {"input_manifest.json": manifest, "proposed_bundle.json": proposed, "proposed_contract.json": next_contract, "admission_request.json": request}
    for name, doc in documents.items():
        _write_bytes(output / name, canonical_authority_file_bytes(doc), check)
    for name, content in payloads.items():
        _write_bytes(output / "payload" / name, content, check)
    _write_bytes(output / "integration.patch", patch, check)
    verify_inputs(root, manifest)
    return request


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    result = prepare(Path.cwd(), check=args.check)
    print(json.dumps({k: result[k] for k in ("state", "request_id", "proposed_bundle_id", "proposed_contract_id")}))
