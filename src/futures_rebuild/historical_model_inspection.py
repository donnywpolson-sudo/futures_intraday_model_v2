"""One saved ES model, one historical session. CLI preparation never reads prices.

Historical execution requires a fresh exact OperationReceipt supplied by the
approved task. No fitting, policy selection, provider, or order path exists here.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path

import numpy as np

from . import l0_alpha_candidate as candidate, l0_alpha_research as research
from .boundary import OperationClassification, OperationReceipt, RepoBoundary
from .canonical import sha256_file, sha256_json
from .causal_timeframe_storage import contained
from .errors import IntegrityError, UnauthorizedOperation
from .model_input_admission import WindowRequest, active_policy, read_windows
from .simulation_records import (HISTORICAL_LABEL, HISTORICAL_SCHEMA, MAX_BYTES,
                                 NO_POLICY, DIAGNOSTIC_SCHEMA, LIBRARY_SCHEMA, OOS_CONTEXT,
                                 encoded, validate_bundle, validate_library)
from .foundation.statistics_v2 import observation_identity

OPERATION = "INSPECT_SAVED_ES_MODEL_SESSION_ONCE"
LIBRARY_OPERATION = "PREPARE_SAVED_ES_OOS_LIBRARY_ONCE"
SPEC_SCHEMA = "historical-model-inspection/1"
BASE = "state/unpublished_evidence/l0_order_readiness_v1"
STUDY = BASE + "/retrospective_screen_adoption_v1/study.json"
ATTEMPT = BASE + "/retrospective_screen_completion_v1/attempt/f0a11575f15af79dc90ee908239a5bc3a925e7c34d73a342badc5bafb6ff67b5"
MODEL_BATCH = ATTEMPT + "/campaign/ES/outer_0/forecast_1.0.json"
SESSION = "2021-01-26"
MODEL_ID = "f1dd90159f464379ce20abd8bf5476b1a02da44ed96229d018cf91daafb0b0c6"
CATALOG = "data/causally_gated_normalized/_control/catalog.json"
MINUTE, DELAY = candidate.MINUTE, candidate.DELAY
NUMERICAL_FILES = ("src/futures_rebuild/l0_alpha_candidate.py",
                   "src/futures_rebuild/l0_alpha_research.py",
                   "src/futures_rebuild/rolling_hour_research.py")
BINDINGS = (*NUMERICAL_FILES, "src/futures_rebuild/historical_model_inspection.py",
            "src/futures_rebuild/model_input_admission.py", "src/futures_rebuild/model_input_preparation.py",
            "src/futures_rebuild/simulation_records.py", "src/futures_rebuild/research_gateway_policy.py")


def stamp(ns):
    return datetime.fromtimestamp(ns // 10**9, timezone.utc).isoformat()


def require(ok, message):
    if not ok:
        raise IntegrityError(message)


def saved_model(root):
    """Read only the embedded model prefix, not the batch's historical predictions."""
    path = contained(root, MODEL_BATCH)
    with path.open(encoding="utf-8") as stream:
        prefix = stream.read(16384)
    marker = '"component_model":'
    if marker not in prefix:
        raise IntegrityError("Saved component model is missing")
    model, _ = json.JSONDecoder().raw_decode(prefix.split(marker, 1)[1].lstrip())
    candidate.identity(model, "model_id")
    require(model["model_id"] == MODEL_ID and model["mechanism_id"] == candidate.mechanism_id()
            and model["candidate"]["candidate_id"] == "hour_context:ridge=1"
            and model["feature_names"] == list(candidate.FEATURES), "Saved model identity or feature schema differs")
    require(model["dependency_versions"] == {k: version(k) for k in model["dependency_versions"]},
            "Saved model runtime differs; do not refit")
    require(model["columns"] == list(range(7)) and all(len(model[k]) == 7 for k in ("means", "scales", "weights")),
            "Saved transformations are incomplete")
    require(np.isfinite([model[k] for k in ("means", "scales", "weights")]).all()
            and np.isfinite(model["training_mean_ticks"])
            and all(s > 0 for s in model["scales"]), "Saved transformations are invalid")
    study = json.loads(contained(root, STUDY).read_bytes())
    for name in NUMERICAL_FILES:
        require(sha256_file(contained(root, name)) == study["bindings"][name],
                "Original feature/prediction implementation changed: " + name)
    require(candidate.frozen_ranges(candidate.weekday_dates())[0]["outer"]["first_date"] == SESSION,
            "Earliest evaluation session changed")
    return model


def prepare_spec(root, output):
    """Metadata-only declaration; no source-container hashing, row access or inference."""
    root = Path(root).resolve(strict=True)
    output = Path(output).resolve()
    require(output.is_relative_to(root) and not output.exists(), "Output must be a new directory inside this repository")
    relative_output = output.relative_to(root).as_posix()
    require(relative_output.startswith("FuturesLiveCockpit/ReplayRuns/"), "Use the cockpit ReplayRuns directory")
    model = saved_model(root)
    policy = active_policy(root)
    entries = json.loads(contained(root, CATALOG).read_bytes())["entries"]
    entry = next(e for e in entries if (e["market"], e["year"]) == ("ES", 2021))
    require("FEATURE_GENERATION" in entry["permitted_uses"], "Source does not permit features")
    times = candidate.decision_times(SESSION)
    require(model["latest_label_ns"] < times[0], "Training labels overlap this session")
    spec = {"schema": SPEC_SCHEMA, "operation": OPERATION, "claim": "DIAGNOSTIC_INSPECTION_ONLY",
            "market": "ES", "session": SESSION, "model_id": model["model_id"],
            "model_batch": MODEL_BATCH, "model_batch_sha256": sha256_file(contained(root, MODEL_BATCH)),
            "training_cutoff": stamp(model["latest_label_ns"]), "feature_names": list(candidate.FEATURES),
            "source_entry": entry, "input_admission_id": policy["id"],
            "start_ns": times[0] - DELAY - 61 * MINUTE, "end_ns": times[-1] - DELAY,
            "first_decision_ns": times[0], "last_decision_ns": times[-1],
            "contract_rule": "ONE_UNCHANGED_OBSERVED_CONTRACT_OR_REFUSE",
            "trading_policy": None, "output": relative_output,
            "disposition": "CLOSED — NO SUPPORTED EDGE; inspection only",
            "physical_read_scope": "Existing admission verifies/scans the ES 2021 annual release; only this session and 61-bar lookback are retained for inference. The saved first-window forecast batch is read only to compare this session's predictions.",
            "limits": {"fits": 0, "sessions": 1, "decisions": 330, "bars": 390, "additional_cost_usd": 0,
                       "new_output_bytes": MAX_BYTES},
            "bindings": {name: sha256_file(contained(root, name)) for name in BINDINGS}}
    spec["spec_id"] = sha256_json(spec)
    return spec


def validate_spec(root, spec):
    require(isinstance(spec, dict) and isinstance(spec.get("output"), str), "Invalid run specification")
    contained(root, spec["output"])
    require(spec == prepare_spec(root, Path(root) / spec["output"]), "Historical run specification is stale or changed")


def prepare_file(root, spec_path, output):
    spec = prepare_spec(root, output)
    spec_path = Path(spec_path)
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    with spec_path.open("xb") as stream:
        stream.write(encoded(spec))
    return spec


def require_scope(root, scope):
    """Policy hook: an exact fresh task receipt still supplies personal authority."""
    base = {k: v for k, v in scope.items() if k not in ("approval_command", "approval_plan_id", "approval_plan_sha256")}
    require(set(base) == {"spec_path", "spec_id", "spec_sha256"}, "Historical inspection receipt scope differs")
    path = contained(root, base["spec_path"])
    require(sha256_file(path) == base["spec_sha256"], "Run specification bytes changed")
    spec = json.loads(path.read_bytes())
    require(spec.get("spec_id") == base["spec_id"], "Run specification identity changed")
    operation = LIBRARY_OPERATION if spec.get("schema") == "historical-model-library/1" else OPERATION
    require(scope.get("approval_command") == operation
            and scope.get("approval_plan_id") == base["spec_id"]
            and scope.get("approval_plan_sha256") == base["spec_sha256"], "Personal approval is not bound to this inspection")
    (validate_library_spec if operation == LIBRARY_OPERATION else validate_spec)(root, spec)
    return spec


def prepare_library_spec(root, output):
    spec = prepare_spec(root, output)
    require("DISCOVERY_RESEARCH" in spec["source_entry"]["permitted_uses"], "Source does not permit declared outcome research")
    root = Path(root)
    study = json.loads(contained(root, STUDY).read_bytes())
    fold = candidate.frozen_ranges(candidate.weekday_dates())[0]
    dates = fold["outer"]["audit_dates"]
    require(study["outer_windows"][0] == {"outer_fold": 0, "audit_dates": dates}
            and len(dates) == 63 and dates[0] == SESSION and dates[-1] == "2021-04-22"
            and all(w["last_date"] < dates[0] for w in fold["inner"]),
            "Saved evaluation or tuning membership changed")
    model = saved_model(root)
    require(model["fit_kind"] == "outer" and model["latest_label_ns"] < candidate.decision_times(dates[0])[0],
            "Model was not fitted before this evaluation window")
    spec.update(schema="historical-model-library/1", operation=LIBRARY_OPERATION, sessions=dates,
                evaluation_context=OOS_CONTEXT, study_sha256=sha256_file(contained(root, STUDY)),
                outcome_rule="Original next-minute open to open 60 minutes later; completed bars plus five seconds",
                physical_read_scope="At most 126 admission calls verify/scan the local ES 2021 annual release: separate feature and target requests per session. Only declared session/lookback/target observations are retained. The saved first-window forecast batch supplies parity references. No protected periods or provider access.",
                limits={"fits": 0, "sessions": 63, "decisions": 20_790, "bars_per_session": 452,
                        "additional_cost_usd": 0, "new_output_bytes": 63 * MAX_BYTES + 100_000})
    spec.pop("spec_id")
    spec["spec_id"] = sha256_json(spec)
    return spec


def validate_library_spec(root, spec):
    require(isinstance(spec, dict) and isinstance(spec.get("output"), str), "Invalid library specification")
    require(spec == prepare_library_spec(root, Path(root) / spec["output"]), "Library specification is stale or changed")


def prepare_library_file(root, path, output):
    spec = prepare_library_spec(root, output)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open("xb") as stream:
        stream.write(encoded(spec))
    return spec


class SessionExcluded(IntegrityError):
    """Expected data-quality exclusion; never catches binding or parity failures."""


def session_spec(library_spec, day):
    require(day in library_spec["sessions"], "Session is outside the declared evaluation window")
    times = candidate.decision_times(day)
    return {**library_spec, "session": day, "start_ns": times[0] - DELAY - 61 * MINUTE,
            "end_ns": times[-1] - DELAY, "first_decision_ns": times[0], "last_decision_ns": times[-1]}


def check_session_windows(results, status):
    rows = {}
    for result in results:
        if result["status"] != status or result["reasons"]:
            raise SessionExcluded("Admission refused: " + ",".join(result["reasons"]))
        request = result["request"]
        observations = result["observations"]
        if [r["bar_start_ns"] for r in observations] != list(range(request.start_ns, request.end_ns, MINUTE)):
            raise SessionExcluded("Incomplete or unordered session history")
        for row in observations:
            if (row["market"] != "ES" or (row["min_price_increment_nano"], row["multiplier_nano"], row["currency"])
                    != (250_000_000, 50 * 10**9, "USD")):
                raise SessionExcluded("Session has incompatible ES contract economics")
            if (row["bar_end_ns"] != row["bar_start_ns"] + MINUTE
                    or max(row["available_at_ns"], row["decision_eligible_at_ns"], row["definition_received_at_ns"])
                    > row["bar_end_ns"] + DELAY):
                raise SessionExcluded("Observation is delayed beyond the frozen clock")
            previous = rows.setdefault(row["bar_start_ns"], row)
            require(previous == row, "Overlapping admitted observations disagree")
    if not rows or len({observation_identity(r) for r in rows.values()}) != 1:
        raise SessionExcluded("Session crosses a contract identity")
    return rows


def diagnostic_bundle(model, spec, features, targets):
    """Targets are attached only after feature-only inference has finished."""
    bundle, predictions = prediction_bundle(model, spec, features)
    return attach_diagnostics(bundle, predictions, spec, features, targets), predictions


def attach_diagnostics(bundle, predictions, spec, features, targets):
    feature_rows = check_session_windows(features, "ELIGIBLE")
    target_rows = check_session_windows(targets, "COMPLETE")
    times = candidate.decision_times(spec["session"])
    require(len(targets) == len(times), "Missing scheduled target request")
    identity = observation_identity(next(iter(feature_rows.values())))
    for at, result in zip(times, targets, strict=True):
        request = result["request"]
        end = at - DELAY
        require((request.start_ns, request.end_ns, request.decision_at_ns, request.kind,
                 request.signal_identity, request.signal_available_at_ns, request.evaluated_at_ns)
                == (end + MINUTE, end + 62 * MINUTE, at, "target", identity, at, end + 62 * MINUTE + DELAY),
                "Target request differs from declared outcome")
    if any(observation_identity(r) != identity for r in target_rows.values()):
        raise SessionExcluded("Target path crosses session identity")
    indexed = dict(feature_rows)
    for start, row in target_rows.items():
        require(indexed.setdefault(start, row) == row, "Feature/target observations disagree")
    stop = times[-1] - DELAY + 62 * MINUTE
    if sorted(indexed) != list(range(spec["start_ns"], stop, MINUTE)):
        raise SessionExcluded("Incomplete outcome tail")
    for start in range(spec["end_ns"], stop, MINUTE):
        row = indexed[start]
        bundle["records"].append({"bar": {"contract": row["actual_contract"], "opened_at": stamp(start),
            "closed_at": stamp(start + MINUTE), **{k: str(Decimal(row[k + "_nano"]) / 10**9) for k in ("open", "high", "low", "close")}, "volume": row["volume"]},
            "available_at": stamp(start + MINUTE + DELAY), "inspection": {"status": "CONTEXT",
            "reasons": ["Outcome-only tail; no scheduled prediction"], "decision_at": None,
            "feature_window_start": None, "features": None, "prediction": None}})
    decisions = []
    for at in times:
        row = feature_rows[at - DELAY - MINUTE]
        decisions.append({"status": "ELIGIBLE", "entry_ns": at - DELAY + MINUTE,
            "exit_ns": at - DELAY + 61 * MINUTE, "actual_contract": row["actual_contract"],
            "tick_nano": row["min_price_increment_nano"],
            "tick_value_nano_usd": row["min_price_increment_nano"] * row["multiplier_nano"] // 10**9})
    outcomes = candidate.attach_outcomes(decisions, list(target_rows.values()), as_of_ns=stop + DELAY)
    for row in bundle["records"]:
        row["outcomes"] = []
    clocks = {r["available_at"]: r for r in bundle["records"]}
    for at, decision, outcome in zip(times, decisions, outcomes, strict=True):
        if outcome["status"] != "MATURED":
            raise SessionExcluded("Original outcome calculation refused target: " + outcome["status"])
        index = (at - DELAY - MINUTE - spec["start_ns"]) // MINUTE
        key = sha256_json([candidate.mechanism_id(), spec["market"], at])
        actual = Decimal(str(outcome["target_ticks"]))
        clock = stamp(outcome["known_at_ns"])
        clocks[clock]["outcomes"].append({"forecast_id": key, "decision_index": index,
            "entry_at": stamp(decision["entry_ns"]), "exit_at": stamp(decision["exit_ns"]), "available_at": clock,
            "actual_ticks": str(actual), "error_ticks": str(actual - Decimal(repr(predictions[key])))})
    bundle["schema"] = DIAGNOSTIC_SCHEMA
    bundle["metadata"].update(kind="historical_diagnostics", evaluation_context=OOS_CONTEXT,
        input_sha256=hashlib.sha256(encoded([r["bar"] for r in bundle["records"]])).hexdigest())
    bundle["summary"]["outcomes"] = len(outcomes)
    validate_bundle(bundle)
    return bundle


def execute_library_authorized(root, receipt):
    root = Path(root).resolve(strict=True)
    if type(receipt) is not OperationReceipt:
        raise UnauthorizedOperation("A fresh exact library receipt is required")
    spec = require_scope(root, dict(receipt.scope))
    require(spec["operation"] == LIBRARY_OPERATION, "Wrong operation for library execution")
    receipt.consume(RepoBoundary(root), operation=LIBRARY_OPERATION,
                    classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
                    required_scope=dict(receipt.scope))
    model = saved_model(root)
    batch_path = contained(root, MODEL_BATCH)
    require(batch_path.stat().st_size <= 32_000_000 and sha256_file(batch_path) == spec["model_batch_sha256"],
            "Saved forecast batch differs")
    batch = json.loads(batch_path.read_bytes())
    candidate.identity(batch, "forecast_batch_id")
    values = [v for v in batch["predictions"] if v is not None]
    require(len(values) == len(batch["forecast_ids"]) == len(set(batch["forecast_ids"])), "Invalid saved forecast alignment")
    retained = dict(zip(batch["forecast_ids"], values, strict=True))
    directory = contained(root, spec["output"])
    directory.mkdir(parents=True, exist_ok=False)
    library = {"schema": LIBRARY_SCHEMA, "status": "COMPLETE", "model_id": spec["model_id"],
               "spec_id": spec["spec_id"], "evaluation_context": OOS_CONTEXT, "sessions": []}
    for day in spec["sessions"]:
        current = session_spec(spec, day)
        times = candidate.decision_times(day)
        entry = {"session": day, "status": "EXCLUDED", "contract": None, "path": None, "sha256": None, "reasons": []}
        try:
            features = read_windows(root, market="ES", year=2021, purpose="FEATURE_GENERATION",
                requests=[WindowRequest(at - DELAY - 61 * MINUTE, at - DELAY, at) for at in times])
            indexed = check_session_windows(features, "ELIGIBLE")
            bundle, predictions = prediction_bundle(model, current, features)
            require(all(k in retained and np.isclose(v, retained[k], rtol=1e-10, atol=1e-9) for k, v in predictions.items()),
                    "Recomputed predictions differ from retained forecasts")
            identity = observation_identity(next(iter(indexed.values())))
            targets = read_windows(root, market="ES", year=2021, purpose="DISCOVERY_RESEARCH",
                requests=[WindowRequest(at - DELAY + MINUTE, at - DELAY + 62 * MINUTE, at, "target",
                    at - DELAY + 62 * MINUTE + DELAY, identity, at) for at in times])
            bundle = attach_diagnostics(bundle, predictions, current, features, targets)
            raw = encoded(bundle)
            require(len(raw) <= MAX_BYTES, "Session exceeds saved-result bound")
            target = directory / day
            target.mkdir(exist_ok=False)
            with (target / "result.partial").open("xb") as stream:
                stream.write(raw)
            (target / "result.partial").replace(target / "result.json")
            entry.update(status="INCLUDED", contract=bundle["metadata"]["contract"], path=f"{day}/result.json",
                         sha256=hashlib.sha256(raw).hexdigest())
        except SessionExcluded as exc:
            entry["reasons"] = [str(exc)[:300]]
        library["sessions"].append(entry)
        # Progress is diagnostic only; the viewer never opens partial indexes.
        (directory / "progress.partial").write_bytes(encoded({"processed": len(library["sessions"]), "last": entry}))
    for name, digest in spec["bindings"].items():
        require(sha256_file(contained(root, name)) == digest, "Source binding changed during library preparation")
    require(sha256_file(contained(root, STUDY)) == spec["study_sha256"]
            and sha256_file(batch_path) == spec["model_batch_sha256"]
            and active_policy(root)["id"] == spec["input_admission_id"], "Research inputs changed during preparation")
    validate_library(library)
    with (directory / "library.partial").open("xb") as stream:
        stream.write(encoded(library))
    (directory / "library.partial").replace(directory / "library.json")
    return {"library": str(directory / "library.json"), "included": sum(e["status"] == "INCLUDED" for e in library["sessions"]),
            "excluded": sum(e["status"] == "EXCLUDED" for e in library["sessions"])}


def prediction_bundle(model, spec, admitted):
    """Pure calculation over admitted windows. Never loads data or fits a model."""
    times = candidate.decision_times(spec["session"])
    require(len(admitted) == len(times), "All scheduled decisions must be retained")
    indexed, identities = {}, set()
    for at, result in zip(times, admitted, strict=True):
        request = result["request"]
        require((request.start_ns, request.end_ns, request.decision_at_ns, request.kind)
                == (at - DELAY - 61 * MINUTE, at - DELAY, at, "feature"), "Wrong feature lookback or decision time")
        require(result["status"] == "ELIGIBLE" and not result["reasons"],
                "Historical input refused: " + ",".join(result["reasons"]))
        rows = result["observations"]
        require(len(rows) == 61 and [r["bar_start_ns"] for r in rows] == list(range(request.start_ns, request.end_ns, MINUTE)),
                "Incomplete, duplicate or unordered lookback")
        for row in rows:
            require(row["market"] == spec["market"] and row["bar_end_ns"] == row["bar_start_ns"] + MINUTE
                    and max(row["decision_eligible_at_ns"], row["available_at_ns"], row["definition_received_at_ns"], row["bar_end_ns"] + DELAY) <= at,
                    "Future input or wrong market")
            require(max(row["decision_eligible_at_ns"], row["available_at_ns"])
                    <= row["bar_end_ns"] + DELAY, "Delayed bars cannot be displayed before their availability")
            identity = tuple(row[k] for k in ("actual_contract", "publisher_id", "instrument_id", "min_price_increment_nano", "multiplier_nano", "currency"))
            identities.add(identity)
            previous = indexed.setdefault(row["bar_start_ns"], row)
            require(previous == row, "Overlapping feature windows disagree")
    require(len(identities) == 1, "Session crosses contract or economics")
    contract, _, _, tick, _, _ = next(iter(identities))
    require(tick == 250_000_000 and next(iter(identities))[-2:] == (50 * 10**9, "USD"),
            "Saved ES model requires ES contract economics")
    expected = list(range(spec["start_ns"], spec["end_ns"], MINUTE))
    require(sorted(indexed) == expected, "Session does not match the declared range")
    require(model["latest_label_ns"] < times[0] and model["model_id"] == spec["model_id"], "Model cutoff or identity differs")
    records, predictions = [], {}
    scheduled = set(times)
    for start in expected:
        row = indexed[start]
        end, clock = start + MINUTE, start + MINUTE + DELAY
        bar = {"contract": contract, "opened_at": stamp(start), "closed_at": stamp(end),
               **{k: str(Decimal(row[k + "_nano"]) / 10**9) for k in ("open", "high", "low", "close")}, "volume": row["volume"]}
        info = {"status": "CONTEXT", "reasons": ["Required lookback; no scheduled decision"],
                "decision_at": None, "feature_window_start": None, "features": None, "prediction": None}
        if clock in scheduled:
            window = [indexed[t] for t in range(end - 61 * MINUTE, end, MINUTE)]
            values = candidate.feature_values(window)
            require(np.isfinite(values).all(), "Nonfinite model features")
            value = float(research.predict(model, [values])[0])
            info = {"status": "ELIGIBLE", "reasons": ["Complete causal lookback; frozen model; no trading policy"],
                    "decision_at": stamp(clock), "feature_window_start": stamp(end - 61 * MINUTE),
                    "features": [repr(float(v)) for v in values], "prediction": repr(value)}
            predictions[sha256_json([candidate.mechanism_id(), spec["market"], clock])] = value
        records.append({"bar": bar, "available_at": stamp(clock), "inspection": info})
    meta = {"kind": "historical_predictions", "label": HISTORICAL_LABEL, "synthetic": False,
            "market": spec["market"], "contract": contract, "session": spec["session"],
            "strategy": model["candidate"]["candidate_id"], "model_id": model["model_id"],
            "training_cutoff": spec["training_cutoff"], "feature_names": list(candidate.FEATURES),
            "forecast_units": "ticks", "horizon_minutes": 60,
            "target": "Next-minute open to open 60 minutes later; reference prices, not fills",
            "disposition": spec["disposition"], "input_sha256": hashlib.sha256(encoded([r["bar"] for r in records])).hexdigest(),
            "spec_id": spec["spec_id"], "source_release": Path(spec["source_entry"]["parquet_path"]).parent.name,
            "tick_size": str(Decimal(tick) / 10**9), "availability_assumption": "Bar close plus 5 seconds; assumed availability, not measured delivery latency",
            "trading_policy": None}
    bundle = {"schema": HISTORICAL_SCHEMA, "status": "COMPLETE", "metadata": meta, "records": records,
              "summary": {"predictions": len(predictions), "refused": 0, "message": NO_POLICY}}
    validate_bundle(bundle)
    return bundle, predictions


def execute_authorized(root, receipt):
    """Called by the approved task, never from the historical CLI or viewer."""
    root = Path(root).resolve(strict=True)
    if type(receipt) is not OperationReceipt:
        raise UnauthorizedOperation("A fresh exact task receipt is required")
    boundary = RepoBoundary(root)
    scope = dict(receipt.scope)
    spec = require_scope(root, scope)
    receipt.consume(boundary, operation=OPERATION,
                    classification=OperationClassification.EXTERNAL_REAL_HISTORY_AUTHORIZATION,
                    required_scope=scope)
    model = saved_model(root)
    # The unchanged admission reader verifies the annual release before returning
    # only these windows. Its physical scan is disclosed in the prepared scope.
    times = candidate.decision_times(spec["session"])
    requests = [WindowRequest(at - DELAY - 61 * MINUTE, at - DELAY, at) for at in times]
    results = read_windows(root, market="ES", year=2021, requests=requests, purpose="FEATURE_GENERATION")
    bundle, predictions = prediction_bundle(model, spec, results)
    # Compare only this session's predictions, never use outcomes or fit again.
    batch_path = contained(root, MODEL_BATCH)
    require(sha256_file(batch_path) == spec["model_batch_sha256"], "Saved forecast batch changed")
    require(batch_path.stat().st_size <= 32_000_000, "Saved forecast batch exceeds its bounded read")
    batch = json.loads(batch_path.read_bytes())
    candidate.identity(batch, "forecast_batch_id")
    eligible_predictions = [value for value in batch["predictions"] if value is not None]
    require(len(eligible_predictions) == len(batch["forecast_ids"]), "Saved forecast alignment differs")
    require(len(set(batch["forecast_ids"])) == len(batch["forecast_ids"]), "Duplicate saved forecast identity")
    retained = dict(zip(batch["forecast_ids"], eligible_predictions, strict=True))
    require(all(key in retained and np.isclose(value, retained[key], rtol=1e-10, atol=1e-9)
                for key, value in predictions.items()), "Recomputed predictions differ from the saved session")
    validate_spec(root, spec)
    raw = encoded(bundle)
    require(len(raw) <= MAX_BYTES, "Historical output exceeds its bound")
    directory = contained(root, spec["output"])
    directory.mkdir(parents=True, exist_ok=False)
    temporary = directory / "result.partial"
    with temporary.open("xb") as stream:
        stream.write(raw)
    temporary.replace(directory / "result.json")
    return {"result": str(directory / "result.json"), "predictions_verified": len(predictions),
            "model_id": model["model_id"], "contract": bundle["metadata"]["contract"]}
