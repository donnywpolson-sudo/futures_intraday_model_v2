"""L0 report integration through existing ResearchStore save/checkpoint hooks.

This optional store is for a future authorized lifecycle, or tiny synthetic
save/checkpoint fixtures. It never invokes fitting, replay, registration or gates.
Resume requires current checkpoint bindings; legacy catalogs remain display-only.
"""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys

from .canonical import (sha256_bytes, sha256_file, sha256_json, read_canonical_authority,
                        write_new_canonical_authority, assert_no_linklike_ancestors)
from .errors import IntegrityError
from .l0_alpha_preparation import ResearchStore
from .l0_alpha_reporting import PNL_BASIS, candidate_report, check_scope, combine_folds, load_catalog, write_catalog


def code_provenance(root, paths):
    """Only explicit code/config paths; content hashes include untracked edits."""
    root = Path(root).resolve()
    names = sorted(set(paths))
    if not names:
        raise IntegrityError("explicit nonempty code/config provenance scope required")
    files = {}
    for name in names:
        path = (root / name).resolve()
        if not path.is_relative_to(root) or path.suffix not in (".py", ".js", ".html", ".css", ".json", ".toml") or Path(name).parts[0] not in ("src", "tests", "configs", "pyproject.toml"):
            raise IntegrityError("provenance accepts explicit repository code/config paths only")
        files[name] = sha256_file(path)
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True).stdout.decode().strip()
    diff = subprocess.run(["git", "diff", "--binary", "HEAD", "--", *names], cwd=root, check=True, capture_output=True).stdout
    return {"git_head": head, "scoped_tracked_diff_sha256": sha256_bytes(diff), "scoped_tracked_diff_present": bool(diff),
            "working_file_sha256s": files, "working_code_identity": sha256_json(files),
            "scope": "EXPLICIT_IMPLEMENTATION_FILES_INCLUDING_UNCOMMITTED_AND_UNTRACKED_BYTES;NOT_WHOLE_REPOSITORY"}


def l0_context(bundle, market, *, experiment_id, run_id, provenance=None, lineage=None):
    """Project known frozen design fields without inspecting source/result rows."""
    design = bundle["design"]
    if market not in design["research_markets"]:
        raise IntegrityError("market outside declared L0 family")
    return {"experiment_id": experiment_id, "run_id": run_id,
            "hypothesis": None, "intended_change": None, "parent_candidate_id": None,
            "provenance": {"bundle_id": bundle["bundle_id"], "bound_implementation_files": bundle["implementation_files"],
                           "data_identity": None, "code_at_execution": deepcopy(provenance)},
            "lineage": {"declared_family": deepcopy(design["candidates"]), "search_space": deepcopy(bundle["configuration_ids"]),
                        "selection_metric": design["selection"], "decision_rule": design["ties"],
                        "historical_search": {"upper_bound_established": False, "configuration_charges": bundle["configuration_charges"],
                                              "preserved_accounting_sha256": sha256_json(bundle.get("predecessor", {})),
                                              "charges_are_observed_attempts": False}, **deepcopy(lineage or {})},
            "declared_benchmarks": deepcopy(design["comparisons"]),
            "policy": {"model": None, "features": None, "selection": {"metric": design["selection"], "ties": design["ties"]},
                       "refitting": {"refresh": design["refresh"], "training": design["training"], "purge": design["purge"], "embargo_dates": design["embargo_dates"], "final_rule": design["final_rule"]},
                       "entry": {"rule": design["entry"], "calendar_rule": design["entry_calendar"], "position": design["position"]},
                       "exit": {"holding_minutes": design["holding_minutes"], "last_exit": design["last_exit"]},
                       "sizing": "ONE_STANDARD_CONTRACT", "opportunity_set": {"decision_window": design["decision_window"], "calendar": design["calendar"]},
                       "execution": {"target": design["target"], "availability": design["availability"], "mapping": design["contract_mapping"]},
                       "costs": deepcopy(design["costs"])},
            "scope": {"instruments": [market], "opportunity_set": {"decision_window": design["decision_window"], "calendar": design["calendar"]},
                      "calendar_id": bundle["entry_calendar"]["calendar_id"],
                      "session_boundaries": {"decision_window": design["decision_window"], "last_entry": design["last_entry"], "last_exit": design["last_exit"]},
                      "timezone": design["timezone"], "cutoff_convention": "START_INCLUSIVE_END_EXCLUSIVE;LABEL_MATURITY_BEFORE_DECISION",
                      "pnl_basis": PNL_BASIS, "observation_cutoff_utc": None,
                      "execution_assumptions": {"availability": design["availability"], "mapping": design["contract_mapping"],
                                                "calendar_view": bundle["entry_calendar"]["research_view"], "price_basis": design["target"]},
                      "cost_assumptions": {"basis": "ASSUMED_REFERENCE_ALLOWANCES", "round_trip_fee_usd": design["costs"]["round_trip_fee_usd"],
                                           "round_trip_allowances_ticks": deepcopy(design["costs"]["round_trip_allowances_ticks"][market]), "scenario": "adverse"},
                      "sizing_basis": "ONE_STANDARD_CONTRACT", "data_kind": None}}


def operation_contexts(bundle, base):
    """Use exact declared window identities; one entry per attempted operation."""
    result = {}
    for i, fold in enumerate(bundle["design"]["folds"]):
        def context(candidate, window, stage, number):
            ctx = deepcopy(base)
            ctx["candidate_id"] = candidate["candidate_id"] if candidate else "selected_procedure"
            ctx["policy"].update(model=candidate, features=candidate["feature_names"] if candidate else None)
            ctx["scope"].update(dates=list(window["audit_dates"]), evaluation_stage=stage,
                                 fold_by_date={d: number for d in window["audit_dates"]},
                                 segment_by_date={d: number for d in window["audit_dates"]})
            ctx["lineage"]["attempted_settings"] = candidate
            return ctx
        for candidate in bundle["design"]["candidates"]:
            name = candidate["candidate_id"].replace(":", "_").replace("=", "_")
            for j, inner in enumerate(fold["inner"]):
                result[f"outer_{i}/{name}/inner_{j}"] = context(candidate, inner, "INNER_DEVELOPMENT", f"outer_{i}/inner_{j}")
        result[f"outer_{i}/selected"] = context(None, fold["outer"], "OUTER_CHRONOLOGICAL", f"outer_{i}")
    return result


class ReportingStore(ResearchStore):
    """Observe save events while retaining ResearchStore's original artifacts.

    Declarations must be supplied before checkpoint execution. Historical callers
    hold the existing writer lease across resume validation and source access.
    Resume validates metadata and never backfills from original result payloads.
    """
    def __init__(self, root, context, *, report_contexts, maximum_bytes=8 * 2**30, clock=None, previous_catalog=None, runner_context=None):
        for declaration in report_contexts.values():
            check_scope(declaration["scope"], observations=True)
        self.declared_report_contexts = deepcopy(report_contexts)
        self.declarations_sha256 = sha256_json(report_contexts)
        self.runner_context = deepcopy(runner_context)
        self.report_failure = None
        self.report_contexts = deepcopy(report_contexts)
        self.reports = {}
        self.replays = {}
        self.latest_catalog = None
        self.clock = clock or (lambda: datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"))
        super().__init__(root, context, maximum_bytes=maximum_bytes)
        if self.path("reporting_failure").exists():
            raise IntegrityError("prior reporting failure requires review; no automatic retry")
        checkpoint_state = self._checkpoint_state()
        if (checkpoint_state or (self.root / "candidate_reports").exists()) and previous_catalog is None:
            raise IntegrityError("resume requires the explicit previous derived catalog; never silently omit prior attempts")
        if previous_catalog is not None:
            if Path(previous_catalog).resolve().parent != self.root / "candidate_reports":
                raise IntegrityError("previous catalog must belong to the same run output")
            for report in load_catalog(previous_catalog)["reports"]:
                if report["provenance"].get("store_context_sha256") != sha256_json(self.context):
                    raise IntegrityError("previous report belongs to another lifecycle context")
                operation = report["identity"]["operation_id"]
                suffix = "comparison_" + report["identity"]["benchmark_id"] if report["identity"]["benchmark_id"] else report["scope"]["cost_assumptions"]["scenario"]
                if (operation, suffix) in self.reports:
                    raise IntegrityError("duplicate candidate reference in resume catalog")
                self.reports[(operation, suffix)] = report
            self._verify_resume(Path(previous_catalog), checkpoint_state)
            self.latest_catalog = Path(previous_catalog)

    def _checkpoint_metadata(self, name, value):
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

    def _emit(self, operation, replay=None, *, suffix="adverse", status=None, reason=None):
        ctx = deepcopy(self.report_contexts[operation])
        ctx["operation_id"] = operation
        ctx["provenance"]["store_context_sha256"] = sha256_json(self.context)
        ctx["attempt_id"] = sha256_json([self.context, operation])
        ctx["finished_at_utc"] = self.clock() if status != "INCOMPLETE" else None
        if suffix.startswith("comparison_"):
            benchmark = suffix.removeprefix("comparison_")
            ctx["candidate_id"] = benchmark
            ctx["benchmark_id"] = benchmark
            ctx["policy"]["model"] = {"benchmark": benchmark}
            ctx["policy"]["features"] = ["DECLARED_BENCHMARK_POLICY"]
        elif suffix in ("base", "adverse", "severe"):
            ctx["scope"]["cost_assumptions"]["scenario"] = suffix
        if replay is not None:
            ctx["policy"]["entry"]["evaluated_policy"] = replay.get("policy")
            self.replays[(operation, suffix)] = replay
            ctx["provenance"]["replay_id"] = replay.get("replay_id")
            ctx["source_and_opportunity_coverage"] = {"scheduled_decision_count": replay.get("decision_count"),
                "decision_ledger_sha256": replay.get("decision_ledger_sha256"), "denominator": "DECLARED_EVALUATION_CALENDAR",
                "source_support": ctx.get("source_and_opportunity_coverage")}
        report = candidate_report(ctx, replay, execution_status=status, failure_reason=reason)
        self.reports[(operation, suffix)] = report
        self._publish()

    def _publish(self):
        comparisons = []
        for op in dict.fromkeys(op for op, _ in self.reports):
            selected = self.reports.get((op, "adverse"))
            if selected is None or selected["scope"]["evaluation_stage"] != "OUTER_CHRONOLOGICAL":
                continue
            for benchmark in selected["declared_benchmarks"] or []:
                baseline = self.reports.get((op, "comparison_" + benchmark))
                comparisons.append({"candidate_report_id": selected["report_id"],
                                    "incumbent_report_id": baseline["report_id"] if baseline else None,
                                    "role": "benchmark", "benchmark_id": benchmark})
        # Every prior catalog/report remains immutable; current catalog is explicit.
        from .canonical import canonical_bytes
        # ponytail: immutable catalog snapshots are quadratic in attempt count; bounded by the existing run budget.
        predicted = len(canonical_bytes(list(self.reports.values()))) * 8
        if self.bytes + predicted > self.maximum_bytes:
            raise IntegrityError("derived reports exceed existing output budget")
        self.latest_catalog = write_catalog(self.root / "candidate_reports", list(self.reports.values()), comparisons)
        binding = self._catalog_binding(self.latest_catalog, self._checkpoint_state())
        binding["binding_id"] = sha256_json(binding)
        write_new_canonical_authority(self.latest_catalog.with_suffix(".checkpoints.json"), binding)
        self.bytes = sum(p.stat().st_size for p in self.root.rglob("*.*") if p.is_file())

    def save(self, name, value):
        super().save(name, value)
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
        self._checkpoint_metadata(name, value)
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
        if name == "run":
            self._aggregate_outer_folds()
            return
        operation, _, leaf = name.rpartition("/")
        if operation not in self.report_contexts:
            return  # Source ledgers, forecasts, final refits and unknown operations are never inspected.
        context = self.report_contexts[operation]
        if leaf == "started":
            context["started_at_utc"] = self.clock()
            self._emit(operation, status="INCOMPLETE", reason="ATTEMPT_STARTED;NO_COMPLETE_RESULT_YET")
        elif leaf in ("model", "rejected_model"):
            context["policy"].update(model=deepcopy(value["candidate"]), features=deepcopy(value["candidate"]["feature_names"]))
            context["provenance"].update(model_id=value.get("model_id"), training_id=value.get("training_id"),
                                          latest_label_ns=value.get("latest_label_ns"), dependency_versions=value.get("dependency_versions"))
            if context["scope"]["evaluation_stage"] == "OUTER_CHRONOLOGICAL":
                context["lineage"]["selection_inputs"] = {"use": "INNER_SELECTION_WITHIN_DECLARED_PROCEDURE;NOT_INDEPENDENT_CONFIRMATION",
                    "report_ids": [r["report_id"] for (op, _), r in self.reports.items()
                                   if op.split("/")[0] == operation.split("/")[0] and r["scope"]["evaluation_stage"] == "INNER_DEVELOPMENT"]}
        elif leaf == "replay" or leaf in ("base", "adverse", "severe") or leaf.startswith("comparison_"):
            self._emit(operation, value, suffix="adverse" if leaf == "replay" else leaf,
                       status="INCOMPLETE", reason="AWAITING_CHECKPOINT_COMPLETION")
        elif leaf == "failure":
            for op, suffix in list(self.reports):
                if op == operation:
                    self._emit(op, self.replays.get((op, suffix)), suffix=suffix, status="FAILED",
                               reason="CHECKPOINT_FAILURE;ORIGINAL_FAILURE_RECORD_RETAINED;NO_AUTOMATIC_RETRY")
        elif leaf == "completed":
            result = value["result"]
            if result.get("status") == "INVALID_LEAF_SUPPORT":
                self._emit(operation, status="FAILED", reason="INVALID_LEAF_SUPPORT")
            else:
                for op, suffix in list(self.replays):
                    if op == operation:
                        replay = self.replays[(op, suffix)]
                        self._emit(op, replay, suffix=suffix, status="COMPLETE" if replay.get("complete") is True else "INCOMPLETE")

    def _aggregate_outer_folds(self):
        operations = [op for op, ctx in self.report_contexts.items() if ctx["scope"]["evaluation_stage"] == "OUTER_CHRONOLOGICAL"]
        if not operations:
            return
        ctx = deepcopy(self.report_contexts[operations[0]])
        ctx["candidate_id"] = "selected_procedure_all_outer_folds"
        ctx["operation_id"] = "outer_aggregate"
        ctx["provenance"]["store_context_sha256"] = sha256_json(self.context)
        ctx["scope"]["dates"] = sorted(day for op in operations for day in self.report_contexts[op]["scope"]["dates"])
        for mapping in ("fold_by_date", "segment_by_date"):
            ctx["scope"][mapping] = {day: fold for op in operations for day, fold in self.report_contexts[op]["scope"][mapping].items()}
        for suffix in ("base", "adverse", "severe", *["comparison_" + b for b in ctx.get("declared_benchmarks") or []]):
            sources = [self.reports[(op, suffix)] for op in operations if (op, suffix) in self.reports]
            if not sources:
                continue  # Missing benchmark comparisons remain explicitly unavailable.
            declaration = deepcopy(ctx)
            declaration["scope"]["cost_assumptions"] = deepcopy(sources[0]["scope"]["cost_assumptions"])
            if suffix.startswith("comparison_"):
                declaration["candidate_id"] = declaration["benchmark_id"] = suffix.removeprefix("comparison_")
            self.reports[("outer_aggregate", suffix)] = combine_folds(declaration, sources)
        self._publish()

    def record_abandoned(self, operation, reason):
        """Record an owner-declared abandonment; does not create or consume a trial."""
        if operation not in self.report_contexts:
            raise IntegrityError("abandonment must name a declared candidate operation")
        self.save(operation + "/reporting_abandonment", {"reason": reason, "at_utc": self.clock()})
        self._emit(operation, status="ABANDONED", reason=reason)
