"""Bounded discovery metadata and descriptive summaries; no research authority.

Existing evidence stays in place. The installed boundary must adopt the prepared
integration before these checks govern authentic operations. Confirmation remains
disabled until a separately reviewed statistical successor exists.
"""
from copy import deepcopy
from datetime import date
import json
import math
from pathlib import Path

from .canonical import (assert_no_linklike_ancestors, assert_plain_file, canonical_bytes,
                        sha256_file, sha256_json, write_new_canonical_authority)
from .errors import IntegrityError, UnauthorizedOperation
from .locking import FileLease
from .l0_alpha_reporting import measured, unavailable, validate_report, compare_candidates

SCHEMA = "discovery_study/1"
CATALOG_SCHEMA = "discovery_catalog/1"
LIMIT_KEYS = {"attempts", "fits", "seconds", "memory_bytes", "new_output_bytes", "additional_cost_usd"}
GUARDED_OPERATIONS = {
    "MATERIALIZE_ES_NQ_TRANSFER_ONCE", "EXECUTE_ES_NQ_TRANSFER_ONCE",
    "MATERIALIZE_RETROSPECTIVE_SCREEN_ONCE", "EXECUTE_RETROSPECTIVE_SCREEN_ONCE",
    "AUDIT_RETROSPECTIVE_SCREEN_ONCE",
    "EXECUTE_CERTIFIED_TRIAL_HISTORICAL_SCREEN",
}
APPROVED = Path("configs/approved_discovery_batch.json")
HISTORY = Path("state/discovery_history")


def bind(value, field):
    result = deepcopy(value)
    result[field] = sha256_json(result)
    return result


def identity(value, field):
    if not isinstance(value, dict) or value.get(field) != sha256_json({k: v for k, v in value.items() if k != field}):
        raise IntegrityError("invalid discovery identity: " + field)


def nonempty(value):
    return isinstance(value, str) and bool(value.strip())


def intervals(values):
    if not isinstance(values, list):
        raise IntegrityError("explicit exposure intervals required")
    for item in values:
        if (not isinstance(item, dict) or set(item) != {"market", "start", "end", "reason"}
                or not nonempty(item["market"]) or not nonempty(item["reason"])):
            raise IntegrityError("invalid exposure interval")
        try:
            if not date.fromisoformat(item["start"]) <= date.fromisoformat(item["end"]):
                raise ValueError()
        except (TypeError, ValueError):
            raise IntegrityError("invalid exposure dates") from None
    return sorted({sha256_json(x): x for x in values}.values(), key=sha256_json)


def validate_study(study):
    identity(study, "study_summary_id")
    required = {"schema_version", "study_summary_id", "scientific_id", "question", "kind", "parents",
                "parentage", "intended_change", "informed_by", "exposure", "history_complete", "declaration"}
    if (set(study) != required or study["schema_version"] != SCHEMA
            or study["kind"] not in {"PREDICTIVE", "ECONOMIC", "DIAGNOSTIC"}
            or study["parentage"] not in {"DECLARED", "NO_PARENT_KNOWN"}
            or type(study["history_complete"]) is not bool
            or any(not nonempty(study[k]) for k in ("scientific_id", "question", "intended_change"))
            or not isinstance(study["parents"], list) or len(set(study["parents"])) != len(study["parents"])
            or any(not nonempty(p) for p in study["parents"])
            or bool(study["parents"]) != (study["parentage"] == "DECLARED")
            or not isinstance(study["informed_by"], list) or any(not nonempty(p) for p in study["informed_by"])):
        raise IntegrityError("incomplete scientific history declaration")
    intervals(study["exposure"])
    ref = study["declaration"]
    if not isinstance(ref, dict) or set(ref) != {"path", "sha256"} or not nonempty(ref["path"]) or len(ref.get("sha256", "")) != 64:
        raise IntegrityError("exact declaration reference required")


class History:
    """Small append-only metadata journal using the existing lease and writer.

    Only this journal is enumerated, never arbitrary historical/source folders.
    A reviewed batch binds the entire journal prefix so deletion/reordering fails.
    Human disclosures are evidence claims, not proof that no unreported look occurred.
    """
    def __init__(self, directory):
        self.directory = Path(directory).resolve()
        assert_no_linklike_ancestors(self.directory)

    def records(self):
        result, previous = [], None
        for number, path in enumerate(sorted(self.directory.glob("[0-9]*.json")), 1):
            assert_plain_file(path)
            value = json.loads(path.read_bytes())
            identity(value, "event_id")
            if (set(value) != {"kind", "previous", "payload", "event_id"}
                    or value["kind"] not in {"STUDY", "EXPOSURE", "ATTEMPT", "FIT", "RESULT", "LEGACY_ATTEMPT"}
                    or not isinstance(value["payload"], dict)):
                raise IntegrityError("invalid history event")
            if value["kind"] == "STUDY":
                validate_study(value["payload"])
            if path.name != f"{number:08d}.json" or value["previous"] != previous:
                raise IntegrityError("discovery history was reset, truncated or forked")
            previous = value["event_id"]
            result.append(value)
        return result

    def _append(self, records, kind, payload):
        event = bind({"kind": kind, "previous": records[-1]["event_id"] if records else None,
                      "payload": payload}, "event_id")
        write_new_canonical_authority(self.directory / f"{len(records)+1:08d}.json", event)
        return event

    def add_study(self, study):
        validate_study(study)
        self.directory.mkdir(parents=True, exist_ok=True)
        with FileLease(self.directory / "journal.lock"):
            records = self.records()
            known = {r["payload"]["scientific_id"]: r["payload"] for r in records if r["kind"] == "STUDY"}
            old = known.get(study["scientific_id"])
            if old is not None:
                if old != study:
                    raise IntegrityError("same scientific identity cannot acquire a new history or name")
                return old
            if any(p not in known for p in study["parents"]):
                raise IntegrityError("missing parent record")
            inherited = [x for prior in known.values() for x in prior["exposure"]]
            influences = [x for p in study["parents"] for x in known[p]["informed_by"]]
            # Global observed exposure also survives parent omission or a renamed study.
            inherited += [x for r in records if r["kind"] == "EXPOSURE" for x in r["payload"]["intervals"]]
            inherited += [x for r in records if r["kind"] == "ATTEMPT" for x in r["payload"]["exposure"]]
            influences += [r["payload"]["result_reference"] for r in records if r["kind"] == "EXPOSURE"]
            if not {sha256_json(x) for x in inherited} <= {sha256_json(x) for x in study["exposure"]}:
                raise IntegrityError("descendant omitted inherited exposure")
            if not set(influences) <= set(study["informed_by"]):
                raise IntegrityError("descendant omitted prior-result influences")
            if study["history_complete"] and any(not prior["history_complete"] for prior in known.values()):
                raise IntegrityError("unknown history cannot become complete by descent")
            self._append(records, "STUDY", study)
        return study

    def expose(self, scientific_id, values, result_reference):
        values = intervals(values)
        if not nonempty(result_reference):
            raise IntegrityError("result reference required")
        with FileLease(self.directory / "journal.lock"):
            records = self.records()
            if scientific_id not in {r["payload"]["scientific_id"] for r in records if r["kind"] == "STUDY"}:
                raise IntegrityError("unknown exposed study")
            return self._append(records, "EXPOSURE", {"scientific_id": scientific_id,
                "intervals": values, "result_reference": result_reference})

    def import_attempt(self, scientific_id, operation_id, execution, reference):
        """Known metadata only; never invent historical resource consumption."""
        if execution not in {"COMPLETE", "FAILED", "INCOMPLETE", "UNKNOWN"} or not all(
                nonempty(v) for v in (scientific_id, operation_id, reference)):
            raise IntegrityError("invalid historical attempt metadata")
        payload = {"scientific_id": scientific_id, "operation_id": operation_id,
                   "execution": execution, "reference": reference, "historical_consumption": "UNKNOWN"}
        with FileLease(self.directory / "journal.lock"):
            records = self.records()
            if not any(r["kind"] == "STUDY" and r["payload"]["scientific_id"] == scientific_id for r in records):
                raise IntegrityError("historical attempt needs its study")
            for record in records:
                if record["kind"] == "LEGACY_ATTEMPT" and record["payload"]["operation_id"] == operation_id:
                    if record["payload"] != payload:
                        raise IntegrityError("historical attempt cannot be rewritten")
                    return record
            return self._append(records, "LEGACY_ATTEMPT", payload)


def validate_limits(limits):
    if not isinstance(limits, dict) or set(limits) != LIMIT_KEYS:
        raise IntegrityError("all six campaign limits required")
    for key, value in limits.items():
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise IntegrityError("invalid campaign limit")
        if key in {"attempts", "fits", "memory_bytes", "new_output_bytes"} and type(value) is not int:
            raise IntegrityError("integer campaign count/byte limits required")
    if limits["attempts"] < 1 or limits["seconds"] <= 5 or limits["memory_bytes"] <= 0 or limits["new_output_bytes"] <= 512*1024+4096:
        raise IntegrityError("campaign needs working capacity and failure reserve")
    if limits["additional_cost_usd"] != 0:
        raise UnauthorizedOperation("this implementation is local and provider-free")


def prepare_batch(history, *, studies, scope, candidates, benchmarks, primary_rule, limits,
                  stop_conditions, rationale, economics=None, _records=None):
    records = history.records() if _records is None else _records
    known = {r["payload"]["scientific_id"]: r["payload"] for r in records if r["kind"] == "STUDY"}
    validate_limits(limits)
    if (not studies or len(set(studies)) != len(studies) or any(s not in known for s in studies)
            or not candidates or not benchmarks or not stop_conditions
            or any(not nonempty(x) for x in [*candidates, *benchmarks, *stop_conditions, primary_rule, rationale])):
        raise IntegrityError("complete bounded hypothesis family required")
    if (set(scope) != {"stage", "start", "end", "markets"} or not isinstance(scope["markets"], list)
            or not scope["markets"] or any(not nonempty(m) for m in scope["markets"])):
        raise IntegrityError("explicit stage, dates and markets required")
    if scope["stage"] != "DEVELOPMENT":
        raise UnauthorizedOperation("confirmation remains blocked: qualification not validated")
    try:
        valid = date(2018, 1, 1) <= date.fromisoformat(scope["start"]) <= date.fromisoformat(scope["end"]) < date(2023, 1, 1)
    except (TypeError, ValueError):
        valid = False
    if not valid:
        raise UnauthorizedOperation("ordinary discovery is restricted to 2018-2022 development")
    if any(known[s]["kind"] == "ECONOMIC" for s in studies):
        required = {"execution", "costs", "sizing", "capital_treatment", "benchmark_policies",
                    "minimum_daily_net_usd", "minimum_benchmark_improvement_usd", "risk_limits"}
        if not isinstance(economics, dict) or set(economics) != required or any(economics[x] is None for x in required):
            raise IntegrityError("pre-result economic assumptions, hurdles and risk limits required")
        for key in ("minimum_daily_net_usd", "minimum_benchmark_improvement_usd"):
            if type(economics[key]) not in (int, float) or not math.isfinite(economics[key]) or economics[key] <= 0:
                raise IntegrityError("economic hurdles must be finite and meaningful, not invented zero defaults")
        if any(not economics[k] for k in required):
            raise IntegrityError("empty economic policies or risk limits are not declarations")
    return bind({"schema_version": "discovery_batch/1", "history_prefix": [r["event_id"] for r in records],
                 "studies": studies, "scope": scope, "candidates": candidates, "benchmarks": benchmarks,
                 "primary_rule": primary_rule, "limits": limits, "stop_conditions": stop_conditions,
                 "rationale": rationale, "economics": economics, "owner_review_required": True,
                 "promotion_authorized": False}, "batch_id")


def check_batch(history, batch):
    identity(batch, "batch_id")
    records = history.records()
    prefix = batch["history_prefix"]
    if [r["event_id"] for r in records[:len(prefix)]] != prefix:
        raise IntegrityError("reviewed discovery history prefix changed")
    if any(r["kind"] in {"STUDY", "EXPOSURE", "LEGACY_ATTEMPT"} for r in records[len(prefix):]):
        raise IntegrityError("new information requires a newly reviewed batch")
    rebuilt = prepare_batch(history, _records=records[:len(prefix)],
        **{key: batch[key] for key in ("studies", "scope", "candidates", "benchmarks", "primary_rule",
                                      "limits", "stop_conditions", "rationale", "economics")})
    if rebuilt != batch:
        raise IntegrityError("batch declaration differs")
    return records


def reserve_attempt(history, batch, operation_id, reservation):
    """Charge maximum work BEFORE any read/fit; failed attempts never refund it."""
    validate_limits(reservation)
    if reservation["attempts"] != 1 or not nonempty(operation_id) or len(operation_id) > 128:
        raise IntegrityError("one exact operation per reservation")
    with FileLease(history.directory / "journal.lock"):
        records = check_batch(history, batch)
        attempts = [r["payload"] for r in records if r["kind"] == "ATTEMPT"]
        if (any(a["operation_id"] == operation_id for a in attempts)
                or any(r["kind"] == "LEGACY_ATTEMPT" and r["payload"]["operation_id"] == operation_id for r in records)):
            raise UnauthorizedOperation("operation already attempted; no retry")
        current = [a for a in attempts if a["batch_id"] == batch["batch_id"]]
        finished = {r["payload"]["operation_id"] for r in records if r["kind"] == "RESULT"}
        if any(a["operation_id"] not in finished for a in current):
            raise UnauthorizedOperation("prior attempt still incomplete; review it before another attempt")
        for key in LIMIT_KEYS:
            charged = max([a["reservation"][key] for a in current] + [reservation[key]]) if key == "memory_bytes" else sum(a["reservation"][key] for a in current) + reservation[key]
            if charged > batch["limits"][key]:
                raise IntegrityError("campaign reservation exhausted: " + key)
        payload = {"batch_id": batch["batch_id"], "studies": batch["studies"],
            "operation_id": operation_id, "reservation": reservation,
            "exposure": [{"market": m, "start": batch["scope"]["start"], "end": batch["scope"]["end"],
                          "reason": "Conservatively reserved exposure: " + operation_id} for m in batch["scope"]["markets"]]}
        if len(canonical_bytes(payload)) > 3500:
            raise IntegrityError("attempt metadata exceeds its 4 KiB reservation")
        return history._append(records, "ATTEMPT", payload)


def finish_attempt(history, operation_id, summary):
    if (not isinstance(summary, dict) or summary.get("execution") not in {"COMPLETE", "FAILED", "INCOMPLETE", "ABANDONED"}
            or len(canonical_bytes(summary)) > 3500):
        raise IntegrityError("terminal metadata must be valid and fit its reserved allowance")
    with FileLease(history.directory / "journal.lock"):
        records = history.records()
        if not any(r["kind"] == "ATTEMPT" and r["payload"]["operation_id"] == operation_id for r in records):
            raise IntegrityError("unreserved attempt")
        if any(r["kind"] == "RESULT" and r["payload"]["operation_id"] == operation_id for r in records):
            raise IntegrityError("terminal attempt cannot be overwritten by a diagnostic")
        return history._append(records, "RESULT", {"operation_id": operation_id, "summary": summary})


def require_adopted_scope(root, operation, requested):
    """Additional check for a reviewed policy successor; never replaces receipts.

    No new operation is admitted. The original policy runs after this check.
    Once adopted, missing or unlisted execution scopes fail rather than bypass.
    """
    if operation not in GUARDED_OPERATIONS:
        return
    path = Path(root) / APPROVED
    if not path.is_file():
        raise UnauthorizedOperation("discovery successor not adopted")
    assert_plain_file(path)
    adopted = json.loads(path.read_bytes())
    identity(adopted, "adoption_id")
    if adopted.get("state") != "ADOPTED" or adopted.get("module_sha256") != sha256_file(Path(__file__)):
        raise UnauthorizedOperation("discovery integration absent or stale")
    scope = {k: v for k, v in requested.items() if k not in {"approval_command", "approval_plan_id", "approval_plan_sha256"}}
    matches = [x for x in adopted["operations"] if x["operation"] == operation and x["scope"] == scope]
    if len(matches) != 1:
        raise UnauthorizedOperation("operation differs from reviewed candidates, scope or budget binding")
    if scope.get("study_id") not in adopted["batch"]["studies"]:
        raise UnauthorizedOperation("operation study is outside reviewed batch")
    check_batch(History(Path(root) / HISTORY), adopted["batch"])
    known = {r["payload"]["scientific_id"]: r["payload"] for r in History(Path(root) / HISTORY).records() if r["kind"] == "STUDY"}
    for scientific_id in adopted["batch"]["studies"]:
        ref = known[scientific_id]["declaration"]
        target = (Path(root) / ref["path"]).resolve()
        if not target.is_relative_to(Path(root).resolve()):
            raise UnauthorizedOperation("declaration path escapes repository")
        assert_plain_file(target)
        if sha256_file(target) != ref["sha256"]:
            raise IntegrityError("scientific declaration changed")
    return matches[0]


def result_summary(study, *, execution, completed, expected, primary_pass=None, evidence_complete=False,
                   power_sufficient=False, metrics=(), uncertainty=None, reason=None, attempts=(), data_scope=None):
    validate_study(study)
    validate_result_scope(data_scope)
    if execution not in {"COMPLETE", "FAILED", "INCOMPLETE", "ABANDONED", "NOT_RUN"}:
        raise IntegrityError("unknown execution status")
    if set(completed) != set(expected) or any(type(x) is not int or x < 0 for x in [*completed.values(), *expected.values()]):
        raise IntegrityError("explicit complete/expected counts required")
    if not expected or not any(expected.values()):
        raise IntegrityError("no expected evidence cannot establish a completed scientific result")
    if any(completed[k] > expected[k] for k in expected):
        raise IntegrityError("work exceeds declared scope")
    complete = execution == "COMPLETE" and completed == expected and evidence_complete is True
    decision = "NOT_EVALUATED"
    if execution == "FAILED":
        decision = "MECHANICAL_FAILURE"
    elif execution != "NOT_RUN":
        decision = "INCONCLUSIVE"
        if complete and power_sufficient is True and type(primary_pass) is bool:
            decision = ("PREDICTIVE_FINDING" if study["kind"] == "PREDICTIVE" else
                        "DEVELOPMENT_CANDIDATE" if study["kind"] == "ECONOMIC" else
                        "DIAGNOSTIC_ONLY") if primary_pass else "UNSUPPORTED"
    next_action = {"UNSUPPORTED": "CLOSE_STUDY", "PREDICTIVE_FINDING": "REVIEW_ECONOMIC_STUDY",
                   "DEVELOPMENT_CANDIDATE": "REVIEW_CONFIRMATION_PREREQUISITES",
                   "MECHANICAL_FAILURE": "DIAGNOSE_NO_AUTOMATIC_RETRY", "INCONCLUSIVE": "RESOLVE_EVIDENCE_GAP",
                   "DIAGNOSTIC_ONLY": "RETAIN_PARENT_EXPOSURE", "NOT_EVALUATED": "REVIEW_BOUNDED_OPERATION"}[decision]
    rows = []
    for metric in metrics:
        if set(metric) != {"name", "value", "unit", "availability", "reason"} or not nonempty(metric["name"]):
            raise IntegrityError("metric needs value, units and availability reason")
        if metric["availability"] == "AVAILABLE":
            if type(metric["value"]) not in (int, float) or not math.isfinite(metric["value"]):
                raise IntegrityError("nonfinite or missing metric cannot be available")
        elif metric["availability"] != "UNAVAILABLE" or metric["value"] is not None or not nonempty(metric["reason"]):
            raise IntegrityError("unavailable metric must retain reason and null")
        rows.append(deepcopy(metric) if complete else {"name": metric["name"], **unavailable("FULL_RUN_EVIDENCE_INCOMPLETE", metric["unit"])})
    capital_reason = "PREDICTION_ONLY_NO_TRADING_POLICY" if study["kind"] != "ECONOMIC" else "REFERENCE_PNL_HAS_NO_ACCOUNT_RETURN_MODEL"
    rows.extend({"name": name, **unavailable(capital_reason)} for name in ("CAGR", "Account Sharpe", "Percentage drawdown"))
    return bind({"schema_version": "discovery_result/1", "scientific_id": study["scientific_id"],
                 "question": study["question"], "kind": study["kind"], "parents": study["parents"],
                 "declaration": study["declaration"], "informed_by": study["informed_by"],
                 "exposure": study["exposure"], "history_complete": study["history_complete"],
                 "stage": "DEVELOPMENT", "execution": execution, "completed": completed, "expected": expected,
                 "data_scope": data_scope,
                 "decision": decision, "reason": reason, "metrics": rows, "uncertainty": uncertainty,
                 "attempts": list(attempts), "next_action": next_action, "automatic_continuation": False,
                 "confirmation_authorized": False, "alpha_established": False}, "result_id")


def trade_statistics(report, trades):
    """Supplement v2 reports without changing their schema or saved bytes."""
    validate_report(report)
    names = ("Win rate", "Average win", "Average loss", "Profit factor", "Break-even trades")
    empty = {name: unavailable("COMPLETE_RECONCILED_TRADE_LEDGER_REQUIRED") for name in names}
    if not report["metric_eligibility"]["complete_reference_economics"] or not isinstance(trades, list):
        return empty
    keys = ("gross_nano_usd", "cost_nano_usd", "net_nano_usd")
    if len(trades) != report["metrics"]["trade_count"]["value"] or any(
            not isinstance(t, dict) or any(type(t.get(k)) is not int for k in keys) or t["cost_nano_usd"] < 0 or
            t["gross_nano_usd"] - t["cost_nano_usd"] != t["net_nano_usd"] for t in trades):
        return empty
    if any(not math.isclose(sum(t[k] for t in trades)/1e9, report["metrics"][k.replace("_nano", "")]["value"], abs_tol=1e-7, rel_tol=0) for k in keys):
        return empty
    wins = [t["net_nano_usd"]/1e9 for t in trades if t["net_nano_usd"] > 0]
    losses = [t["net_nano_usd"]/1e9 for t in trades if t["net_nano_usd"] < 0]
    return {"Win rate": measured(len(wins)/len(trades), "fraction of all closed trades including break-even") if trades else unavailable("ZERO_TRADES"),
            "Average win": measured(math.fsum(wins)/len(wins), "USD") if wins else unavailable("NO_WINNING_TRADES"),
            "Average loss": measured(math.fsum(losses)/len(losses), "USD") if losses else unavailable("NO_LOSING_TRADES"),
            "Profit factor": measured(math.fsum(wins)/-math.fsum(losses), "net winning dollars / absolute net losing dollars") if losses else unavailable("ZERO_LOSS_DENOMINATOR"),
            "Break-even trades": measured(len(trades)-len(wins)-len(losses), "closed trades with exactly zero net")}


def display_rows(summary):
    identity(summary, "result_id")
    if summary.get("schema_version") != "discovery_result/1" or summary.get("stage") != "DEVELOPMENT":
        raise UnauthorizedOperation("ordinary summaries exclude protected evaluation")
    validate_result_scope(summary.get("data_scope"))
    rows = [["Question", summary["question"]], ["Study", summary["scientific_id"]],
            ["Declaration", summary["declaration"]["path"]],
            ["Result influences", ", ".join(summary["informed_by"]) or "None declared"],
            ["Type / stage", summary["kind"] + " / " + summary["stage"]],
            ["Evidence", summary["data_scope"]["data_kind"] + ": " + ", ".join(summary["data_scope"]["markets"]) + " " + summary["data_scope"]["start"] + " to " + summary["data_scope"]["end"]],
            ["Parents", ", ".join(summary["parents"]) or "No parent known"],
            ["History", "Declared complete; not proof of independence" if summary["history_complete"] else "Incomplete / unknown"],
            ["Exposure", "; ".join(x["market"] + " " + x["start"] + " to " + x["end"] + ": " + x["reason"] for x in summary["exposure"]) or "Unknown / none declared"],
            ["Execution / decision", summary["execution"] + " / " + summary["decision"]],
            ["Completed / expected", json.dumps(summary["completed"], sort_keys=True) + " / " + json.dumps(summary["expected"], sort_keys=True)]]
    rows.extend([m["name"], f"{m['value']:.6g} {m['unit'] or ''}" if m["availability"] == "AVAILABLE" else "Unavailable: " + m["reason"]] for m in summary["metrics"])
    if "economics" in summary:
        rows.extend([label, json.dumps(value, sort_keys=True)] for label, value in summary["economics"].items())
    rows.extend([["Uncertainty", json.dumps(summary["uncertainty"], sort_keys=True)],
                 ["Reason", summary["reason"] or "None supplied"], ["Attempts", ", ".join(summary["attempts"]) or "None"],
                 ["Next action", summary["next_action"]], ["Authority", "No automatic continuation, confirmation, alpha certification or trading"]])
    return rows


def economic_summary(study, report, *, benchmarks, trades=None, primary_pass=None,
                     power_sufficient=False, uncertainty=None):
    """Descriptive projection of existing v2 economics, never a new qualifier."""
    validate_study(study)
    validate_report(report)
    if study["kind"] != "ECONOMIC" or report["identity"]["experiment_id"] != study["scientific_id"]:
        raise IntegrityError("economic report must belong to the declared economic study")
    scope = report["scope"]
    if not scope["dates"]:
        raise IntegrityError("economic summary needs its declared calendar")
    complete = report["metric_eligibility"]["complete_reference_economics"]
    metrics = [{"name": k, **v} for k, v in report["metrics"].items()
               if k not in {"cagr", "account_sharpe", "percentage_drawdown"}]
    metrics.append({"name": "Maximum closed-trade drawdown", **(
        measured(report["drawdown"]["maximum_depth_usd"], "USD") if complete else unavailable("INCOMPLETE_REFERENCE_SERIES"))})
    metrics.extend({"name": k, **v} for k, v in trade_statistics(report, trades).items())
    comparisons = [compare_candidates(b, report, role="benchmark") for b in benchmarks]
    declared = report["declared_benchmarks"] or []
    benchmark_complete = (bool(declared) and len(benchmarks) == len(declared)
        and {b["identity"]["benchmark_id"] for b in benchmarks} == set(declared)
        and all(c["status"] == "DESCRIPTIVE_COMPLETE" for c in comparisons))
    for other, comparison in zip(benchmarks, comparisons, strict=True):
        metrics.append({"name": "Daily improvement over " + str(other["identity"]["benchmark_id"]), **(
            measured(comparison["statistics"]["mean_daily_difference_usd"], "USD/evaluation date")
            if comparison["status"] == "DESCRIPTIVE_COMPLETE" else unavailable("INCOMPATIBLE_OR_INCOMPLETE_BENCHMARK"))})
    result = result_summary(study, execution=report["execution"]["status"],
        completed={"dates": report["coverage"]["available_dates"]}, expected={"dates": report["coverage"]["expected_dates"]},
        evidence_complete=complete, power_sufficient=power_sufficient and benchmark_complete,
        primary_pass=primary_pass, metrics=metrics, uncertainty=uncertainty,
        reason="Descriptive strategy economics. A decision requires the separately declared rule, adequate evidence and all benchmarks; qualification remains blocked.",
        attempts=[report["identity"]["attempt_id"] or "UNKNOWN_ATTEMPT"],
        data_scope={"start": min(scope["dates"]), "end": max(scope["dates"]), "markets": scope["instruments"],
                    "data_kind": "SYNTHETIC" if scope["data_kind"].startswith("SYNTHETIC") else "DEVELOPMENT_REPORTED"})
    result["economics"] = {"Source report": report["report_id"], "Execution / sizing": [scope["execution_assumptions"], scope["sizing_basis"]],
        "Cost assumptions": scope["cost_assumptions"], "Exposure and trade diagnostics": report["trade_diagnostics"],
        "Drawdown scope": report["drawdown"].get("limitation", "INCOMPLETE_REFERENCE_SERIES"),
        "Benchmark coverage": "COMPLETE" if benchmark_complete else "MISSING_OR_INCOMPATIBLE"}
    result.pop("result_id")
    return bind(result, "result_id")


def validate_result_scope(scope):
    try:
        valid = (set(scope) == {"start", "end", "markets", "data_kind"} and scope["markets"]
                 and scope["data_kind"] in {"SYNTHETIC", "DEVELOPMENT_REPORTED"}
                 and date(2018, 1, 1) <= date.fromisoformat(scope["start"]) <= date.fromisoformat(scope["end"]) < date(2023, 1, 1))
    except (TypeError, ValueError, KeyError):
        valid = False
    if not valid:
        raise UnauthorizedOperation("explicit development-only result scope required")


def markdown(summary):
    def escape(value):
        return str(value).replace("&", "&amp;").replace("<", "&lt;").replace("|", "&#124;").replace("\n", "<br>")
    return "| Field | Result |\n| --- | --- |\n" + "\n".join("| " + " | ".join(map(escape, row)) + " |" for row in display_rows(summary)) + "\n"


def load_catalog(path):
    """Explicit metadata-only snapshot; never follows result/source references."""
    path = Path(path)
    assert_plain_file(path)
    catalog = json.loads(path.read_bytes())
    identity(catalog, "catalog_id")
    if catalog.get("schema_version") != CATALOG_SCHEMA or catalog.get("payload_kind") != "METADATA_ONLY":
        raise UnauthorizedOperation("explicit discovery metadata catalog required")
    return {"reports": [], "comparisons": [], "study_summaries": [
        {"result_id": r["result_id"], "rows": display_rows(r)} for r in catalog["summaries"]],
        "historical_census_complete": False}


def transfer_summary(study, result, *, fits, operation_id, data_kind):
    """Project the frozen ES/NQ producer result; never recompute or choose scores."""
    primary = result.get("summary", {})
    inference = primary.get("intervals", {}).get("20", {})
    rows = []
    if inference.get("status") == "AVAILABLE":
        for i, benchmark in enumerate(("NQ-only", "zero change", "training mean")):
            for label, key in (("improvement", "mean"), ("lower bound", "lower")):
                rows.append({"name": benchmark + " " + label, **measured(inference[key][i], "NQ ticks squared")})
    windows = sum(f["status"] == "COMPLETE" for f in result.get("folds", []))
    return result_summary(study, execution="COMPLETE", completed={"fits": fits, "windows": windows},
        expected={"fits": 16, "windows": 8}, primary_pass=primary.get("candidate"),
        evidence_complete=windows == 8, power_sufficient=inference.get("status") == "AVAILABLE",
        metrics=rows, uncertainty=primary, attempts=[operation_id],
        reason="Frozen predictive decision using the adopted coverage/interval prerequisites, not a new power study. Sensitivities never replace the primary. No economic evaluation.",
        data_scope={"start": "2018-01-01", "end": "2022-12-31", "markets": ["ES", "NQ"], "data_kind": data_kind})
