"""Prepare/check the inactive freshness repair. Never adopt or run research."""
import argparse
from copy import deepcopy
import difflib
import importlib.util
import json
from pathlib import Path

from futures_rebuild.canonical import canonical_authority_file_bytes, read_canonical_authority, sha256_file, sha256_bytes, sha256_json
from futures_rebuild.errors import IntegrityError

spec = importlib.util.spec_from_file_location("_rejected_reporting_builder", Path(__file__).with_name("prepare_l0_reporting_successor.py"))
old = importlib.util.module_from_spec(spec)
spec.loader.exec_module(old)
OUTPUT = Path("state/unpublished_evidence/l0_alpha_candidate_v1/reporting_successor_v2")
BUILDER = "scripts/prepare_l0_reporting_successor_v2.py"
TEST = "tests/test_l0_reporting_successor_v2.py"
REJECTED = "f3782682ddc49326b9beea12f295cd71f46cd3fdd035a7dfa5cb7f68223579b1"

# Inserted into the inactive ReportingStore, not imported by the live runner.
FRESHNESS_METHODS = '''    def _checkpoint_metadata(self, name, value):
        operation, _, leaf = name.rpartition("/")
        if leaf not in ("started", "completed", "failure"):
            return
        if leaf in ("started", "completed") and value.get("context") != self.context:
            raise IntegrityError("foreign checkpoint context cannot acquire reporting metadata")
        if leaf == "completed" and (value.get("operation") != operation or value.get("checkpoint_id") !=
                sha256_json({k: v for k, v in value.items() if k != "checkpoint_id"})):
            raise IntegrityError("invalid completed checkpoint identity cannot acquire reporting metadata")
        required = {}
        if operation in self.report_contexts:
            if leaf == "started":
                required = {"adverse": "INCOMPLETE"}
            elif leaf == "failure" or value.get("result", {}).get("status") == "INVALID_LEAF_SUPPORT":
                required = {suffix: "FAILED" for op, suffix in self.reports if op == operation}
                required.setdefault("adverse", "FAILED")
            else:
                required = {suffix: "COMPLETE" if replay.get("complete") is True else "INCOMPLETE"
                            for (op, suffix), replay in self.replays.items() if op == operation}
                required.setdefault("adverse", "INCOMPLETE")
        payload = {"schema_version": "l0_checkpoint_reporting_metadata/1", "operation": operation,
                   "event": leaf, "artifact_sha256": sha256_file(self.path(name)),
                   "store_context_sha256": sha256_json(self.context), "required_reports": required}
        payload["metadata_id"] = sha256_json(payload)
        write_new_canonical_authority(self.path("report_checkpoint_bindings/" + name), payload)

    def _checkpoint_state(self):
        # Only checkpoint-envelope byte identities and metadata projections. No replay/ledger decoding.
        state = {}
        references = set()
        for leaf in ("started", "completed", "failure"):
            for path in self.root.rglob(leaf + ".json"):
                relative = path.relative_to(self.root)
                if relative.parts[0] in ("candidate_reports", "report_checkpoint_bindings"):
                    continue
                assert_no_linklike_ancestors(path)
                operation = relative.parent.as_posix()
                if operation not in self.report_contexts and operation != "final_refit_and_reload":
                    raise IntegrityError("checkpoint freshness: undeclared checkpoint operation")
                reference = self.path("report_checkpoint_bindings/" + operation + "/" + leaf)
                references.add(reference)
                assert_no_linklike_ancestors(reference)
                if not reference.is_file():
                    raise IntegrityError("checkpoint freshness unavailable: missing metadata binding; no legacy backfill")
                metadata = read_canonical_authority(reference, identity_field="metadata_id",
                    expected_schema="l0_checkpoint_reporting_metadata/1")
                if (metadata["operation"] != operation or metadata["event"] != leaf
                        or metadata["store_context_sha256"] != sha256_json(self.context)
                        or metadata["artifact_sha256"] != sha256_file(path)):
                    raise IntegrityError("checkpoint freshness: foreign or conflicting metadata reference")
                state.setdefault(operation, {})[leaf] = metadata
        if set((self.root / "report_checkpoint_bindings").rglob("*.json")) != references:
            raise IntegrityError("checkpoint freshness: metadata references a missing checkpoint")
        for events in state.values():
            if "started" not in events or {"completed", "failure"} <= events.keys():
                raise IntegrityError("checkpoint freshness: missing start or conflicting terminal references")
        return state

    def _check_report_references(self, state):
        expected = {}
        for operation, events in state.items():
            terminal = events.get("failure", events.get("completed", events["started"]))
            for suffix, status in terminal["required_reports"].items():
                expected[(operation, suffix)] = status
        actual = {key for key in self.reports if key[0] not in ("runner", "outer_aggregate")}
        if actual != set(expected):
            raise IntegrityError("checkpoint freshness: missing or extra candidate references")
        for (operation, suffix), status in expected.items():
            if operation not in self.report_contexts:
                raise IntegrityError("checkpoint freshness: undeclared operation")
            report = self.reports[(operation, suffix)]
            identity = report["identity"]
            declaration = self.report_contexts[operation]
            benchmark = suffix.removeprefix("comparison_") if suffix.startswith("comparison_") else None
            if (identity["operation_id"] != operation or identity["attempt_id"] != sha256_json([self.context, operation])
                    or identity["candidate_id"] != (benchmark or declaration["candidate_id"])
                    or identity["benchmark_id"] != benchmark
                    or any(identity[k] != declaration.get(k) for k in ("experiment_id", "run_id", "parent_candidate_id"))
                    or report["execution"]["status"] != status):
                raise IntegrityError("checkpoint freshness: candidate identity or status conflict")

    def _catalog_binding(self, catalog, state):
        return {"schema_version": "l0_catalog_checkpoint_binding/1", "catalog_sha256": sha256_file(catalog),
                "store_context_sha256": sha256_json(self.context), "declarations_sha256": self.declarations_sha256,
                "checkpoint_state": state,
                "report_ids": sorted(r["report_id"] for r in self.reports.values())}

    def _verify_resume(self, catalog, state):
        reference = catalog.with_suffix(".checkpoints.json")
        assert_no_linklike_ancestors(reference)
        if not reference.is_file():
            raise IntegrityError("checkpoint freshness unavailable: catalog has no binding; no legacy backfill")
        binding = read_canonical_authority(reference, identity_field="binding_id",
            expected_schema="l0_catalog_checkpoint_binding/1")
        if {k: v for k, v in binding.items() if k != "binding_id"} != self._catalog_binding(catalog, state):
            raise IntegrityError("stale catalog/checkpoint state or conflicting report references")
        self._check_report_references(state)
        if self._checkpoint_state() != state:
            raise IntegrityError("checkpoint state changed during resume validation")

'''


def sources(root, manifest):
    result = old.patched_sources(root, manifest["additional_reporting_dependencies"])
    preparation = result[old.PREP].decode()
    preparation = old.replace_once(preparation, "AS_OF_NS = int(", "FILES += (" + repr(TEST) + ",)\nAS_OF_NS = int(")
    result[old.PREP] = preparation.encode()
    store = result[old.STORE].decode()
    store = old.replace_once(store, "The hash-bound V7 runner and preparation remain unchanged.",
        "Resume requires current checkpoint bindings; legacy catalogs remain display-only.")
    store = old.replace_once(store,
        "    Declarations must be supplied before checkpoint execution. The current V7\n    historical entry constructs ResearchStore itself; binding this store there\n    requires a separately reviewed preparation/admission successor.",
        "    Declarations must be supplied before checkpoint execution. Historical callers\n    hold the existing writer lease across resume validation and source access.\n    Resume validates metadata and never backfills from original result payloads.")
    store = old.replace_once(store, "from .canonical import sha256_bytes, sha256_file, sha256_json",
        "from .canonical import (sha256_bytes, sha256_file, sha256_json, read_canonical_authority,\n                        write_new_canonical_authority, assert_no_linklike_ancestors)")
    store = old.replace_once(store, "        self.runner_context = deepcopy(runner_context)",
        "        self.declared_report_contexts = deepcopy(report_contexts)\n        self.declarations_sha256 = sha256_json(report_contexts)\n        self.runner_context = deepcopy(runner_context)")
    store = old.replace_once(store, '        if (self.root / "candidate_reports").exists() and previous_catalog is None:',
        '        checkpoint_state = self._checkpoint_state()\n        if (checkpoint_state or (self.root / "candidate_reports").exists()) and previous_catalog is None:')
    store = old.replace_once(store, "                self.reports[(operation, suffix)] = report",
        '                if (operation, suffix) in self.reports:\n                    raise IntegrityError("duplicate candidate reference in resume catalog")\n                self.reports[(operation, suffix)] = report')
    store = old.replace_once(store, "            self.latest_catalog = Path(previous_catalog)",
        "            self._verify_resume(Path(previous_catalog), checkpoint_state)\n            self.latest_catalog = Path(previous_catalog)")
    store = old.replace_once(store, "    def _emit(self, operation, replay=None,", FRESHNESS_METHODS + "    def _emit(self, operation, replay=None,")
    store = old.replace_once(store, '        self.bytes = sum(p.stat().st_size for p in self.root.rglob("*.*") if p.is_file())',
        '''        binding = self._catalog_binding(self.latest_catalog, self._checkpoint_state())
        binding["binding_id"] = sha256_json(binding)
        write_new_canonical_authority(self.latest_catalog.with_suffix(".checkpoints.json"), binding)
        self.bytes = sum(p.stat().st_size for p in self.root.rglob("*.*") if p.is_file())''')
    store = old.replace_once(store, "    def _observe(self, name, value):\n", "    def _observe(self, name, value):\n        self._checkpoint_metadata(name, value)\n")
    result[old.STORE] = store.encode()
    result[old.REPORT_TEST] = result[old.REPORT_TEST].replace(
        b"report_contexts=store.report_contexts, previous_catalog=store.latest_catalog",
        b"report_contexts=store.declared_report_contexts, previous_catalog=store.latest_catalog")
    return result


def snapshot(root):
    rejected = old.prepare(root, check=True)
    if rejected["request_id"] != REJECTED:
        raise IntegrityError("rejected request identity changed")
    manifest = old.snapshot(root)
    manifest.pop("input_manifest_id")
    names = [BUILDER, TEST, "src/futures_rebuild/locking.py"]
    names += [str(p.relative_to(root)).replace("\\", "/") for p in (root / old.OUTPUT).rglob("*") if p.is_file()]
    names += ["state/unpublished_evidence/l0_alpha_candidate_v1/reporting_adoption_v1/" + n
              for n in ("task_report.md", "synthetic_resume_check.py")]
    manifest["files"].update({n: sha256_file(root / n) for n in names})
    manifest["rejected_request_id"] = REJECTED
    return old.bind(manifest, "input_manifest_id")


def prepare(root, output=None, *, check=False):
    root = Path(root).resolve()
    output = Path(output) if output is not None else root / OUTPUT
    manifest = snapshot(root)
    if (output / "input_manifest.json").exists():
        if read_canonical_authority(output / "input_manifest.json", identity_field="input_manifest_id") != manifest:
            raise IntegrityError("stale base or dependency drift; request cannot be rebound")
    changed = sources(root, manifest)
    bundle = read_canonical_authority(root / old.BUNDLE, identity_field="bundle_id")
    contract = read_canonical_authority(root / old.CONTRACT, identity_field="contract_id")
    proposed = deepcopy(bundle)
    proposed.pop("bundle_id")
    for name in [*manifest["additional_reporting_dependencies"], old.REPORT_TEST, old.TEST, TEST]:
        proposed["implementation_files"][name] = manifest["files"][name]
    proposed["implementation_files"].update({name: sha256_bytes(data) for name, data in changed.items()})
    proposed["prior_preparation"].update(path=old.BUNDLE, sha256=manifest["files"][old.BUNDLE],
        revision="REPORTING_LIFECYCLE_SUCCESSOR;SCIENTIFIC_DESIGN_UNCHANGED")
    proposed["development_admission"]["contract_path"] = old.NEXT_CONTRACT
    proposed = old.bind(proposed, "bundle_id")
    next_contract = deepcopy(contract)
    next_contract.pop("contract_id")
    next_contract.update(bundle={"path": old.NEXT_BUNDLE, "sha256": sha256_bytes(canonical_authority_file_bytes(proposed))}, bundle_id=proposed["bundle_id"])
    next_contract = old.bind(next_contract, "contract_id")
    payloads = {**changed, old.NEXT_BUNDLE: canonical_authority_file_bytes(proposed), old.NEXT_CONTRACT: canonical_authority_file_bytes(next_contract)}
    def diff(before):
        return "".join("".join(difflib.unified_diff(before(name).splitlines(True), data.decode().splitlines(True),
            fromfile="a/" + name, tofile="b/" + name)) for name, data in changed.items()).encode()
    patch = diff(lambda name: (root / name).read_text(encoding="utf-8"))
    delta = diff(lambda name: (root / old.OUTPUT / "payload" / name).read_text(encoding="utf-8"))
    request = read_canonical_authority(root / old.OUTPUT / "admission_request.json", identity_field="request_id")
    request.pop("request_id")
    request.update(input_manifest_id=manifest["input_manifest_id"], proposed_bundle_id=proposed["bundle_id"],
        proposed_contract_id=next_contract["contract_id"], payload_sha256s={n: sha256_bytes(d) for n, d in sorted(payloads.items())},
        patch_sha256=sha256_bytes(patch), delta_from_rejected_sha256=sha256_bytes(delta), rejected_request_id=REJECTED,
        previous_approval_transferred=False,
        freshness_rule="EXACT_CURRENT_CHECKPOINT_BYTES_AND_METADATA;EXACT_REPORT_IDENTITIES_AND_STATUSES;VALIDATE_UNDER_EXISTING_WRITER_LEASE_BEFORE_READER;NO_LEGACY_BACKFILL")
    request["schemas"].update(checkpoint_metadata="l0_checkpoint_reporting_metadata/1", catalog_checkpoint_binding="l0_catalog_checkpoint_binding/1")
    request = old.bind(request, "request_id")
    docs = {"input_manifest.json": manifest, "admission_request.json": request, "proposed_bundle.json": proposed, "proposed_contract.json": next_contract}
    for name, doc in docs.items():
        old._write_bytes(output / name, canonical_authority_file_bytes(doc), check)
    for name, data in payloads.items():
        old._write_bytes(output / "payload" / name, data, check)
    old._write_bytes(output / "integration.patch", patch, check)
    old._write_bytes(output / "delta_from_rejected.patch", delta, check)
    if snapshot(root) != manifest:
        raise IntegrityError("input drift during preparation")
    return request


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    print(json.dumps(prepare(Path.cwd(), check=args.check), sort_keys=True))
