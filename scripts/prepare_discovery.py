"""Prepare exact local discovery integration candidates, never install or read data."""
import argparse
import difflib
import json
from pathlib import Path

from futures_rebuild.canonical import sha256_file, sha256_json, write_new_canonical_authority
from futures_rebuild import discovery as d
from futures_rebuild.errors import IntegrityError

SOURCES = (
    "src/futures_rebuild/research_gateway_policy.py",
    "src/futures_rebuild/es_nq_transfer_access.py",
    "src/futures_rebuild/live_cockpit/app.py",
    "src/futures_rebuild/live_cockpit/assets/candidate-reports.js",
)
BASE = "state/unpublished_evidence/l0_order_readiness_v1/"


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise IntegrityError("candidate seam changed; inspect before preparing")
    return text.replace(old, new, 1)


def write_fresh(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = text.encode("utf-8")
    if path.exists():
        if path.read_bytes() != raw:
            raise IntegrityError("prepared output exists with different bytes: " + str(path))
    else:
        with path.open("xb") as stream:
            stream.write(raw)


def study(root, folder, question, parents=()):
    relative = BASE + folder + "/study.json"
    declaration = json.loads((root / relative).read_bytes())
    return d.bind({"schema_version": d.SCHEMA, "scientific_id": declaration["study_id"],
        "question": question, "kind": "PREDICTIVE", "parents": list(parents),
        "parentage": "DECLARED" if parents else "NO_PARENT_KNOWN",
        "intended_change": "Prepared transfer question" if parents else "Imported completed retrospective question",
        "informed_by": [BASE + "retrospective_screen_completion_v1/replacement/REPORT.md",
                        BASE + "screen_clock_diagnostic_v1/replacement/REPORT.md"] if parents else [],
        "exposure": [{"market": m, "start": "2018-01-01", "end": "2022-12-31",
                      "reason": "Previously exposed development history; legacy search incomplete"} for m in ("ES", "NQ", "CL", "GC")],
        "history_complete": False, "declaration": {"path": relative, "sha256": sha256_file(root / relative)}}, "study_summary_id")


def evaluation_metadata(root, op):
    """Import terminal task metadata only; never open forecasts or observations."""
    folder = (root / op["destination"]).parent / "evaluation_task_v1"
    paths = [folder / "task_result.json", folder / "final_checks.json"]
    if not all(p.is_file() for p in paths):
        return {"execution": "UNKNOWN", "decision": "UNKNOWN", "reference": op["destination"], "bindings": {}}
    result, checks = [json.loads(p.read_bytes()) for p in paths]
    if (result.get("operation_id") != op["operation_id"] or checks.get("operation_id") != op["operation_id"]
            or checks.get("study_id") != op["study_id"]
            or result.get("status") != "COMPLETE_FIXED_STUDY"
            or checks.get("status") != "PASS_COMPLETE_SAVED_RESULT_VALIDATION"
            or result.get("receipt_consumed") is not True or result.get("error") is not None
            or result.get("saved_forecast_batches") != 16 or checks.get("saved_fit_batches") != 16
            or checks.get("completed_windows") != 8
            or result.get("fold_statuses") != [{"outer_fold":i,"status":"COMPLETE"} for i in range(8)]):
        raise IntegrityError("terminal evaluation metadata is incomplete or differs from this study")
    if result.get("result_classification") != "NO_SUPPORTED_LEAD":
        raise IntegrityError("a different scientific disposition requires review before history import")
    return {"execution": "COMPLETE", "decision": "UNSUPPORTED", "reference": paths[0].relative_to(root).as_posix(),
            "completed": {"fits":16,"windows":8},
            "bindings": {p.relative_to(root).as_posix(): sha256_file(p) for p in paths}}


def prepare(root, output):
    root, output = Path(root).resolve(), Path(output).resolve()
    if not output.is_relative_to(root):
        raise IntegrityError("preparation output must remain in this workspace")
    before = {name: sha256_file(root / name) for name in SOURCES}
    access_before = sha256_file(root / "configs/approved_es_nq_transfer.json")
    history = d.History(output / "history")
    parent = study(root, "retrospective_screen_preparation_v1", "Do recent-hour features improve one-hour predictions in ES/NQ/CL/GC?")
    child = study(root, "es_nq_transfer_preparation_v1", "Does ES add information about NQ's following 15-minute move beyond NQ alone?", (parent["scientific_id"],))
    history.add_study(parent); history.add_study(child)
    from futures_rebuild.es_nq_transfer_access import PACKAGE as source_package, MATERIALIZATION, identity
    original_package = root / source_package
    op_path = original_package / "execute_operation.json"
    op = json.loads(op_path.read_bytes())
    existing_evaluation = (root / op["destination"]).exists()
    material_path = root / MATERIALIZATION / "complete_manifest.json"
    material = json.loads(material_path.read_bytes())
    identity(material, "manifest_id")
    if (material["status"] != "COMPLETE_RECONCILED" or material["study_id"] != child["scientific_id"]
            or (material_path.parent / "fatal_stop.json").exists()
            or sha256_file(material_path.parent / "reconciliation.json") != material["reconciliation_sha256"]):
        raise IntegrityError("complete preserved materialization metadata required; no automatic replacement")
    first = root / BASE / "es_nq_transfer_adoption_v1/materialize_operation.json"
    first_op = json.loads(first.read_bytes())
    failure = root / BASE / "es_nq_transfer_adoption_v1/materialization/fatal_stop.json"
    if not failure.is_file():
        raise IntegrityError("historical failure metadata missing; inspect before importing")
    history.import_attempt(child["scientific_id"], first_op["operation_id"], "FAILED", failure.relative_to(root).as_posix())
    history.import_attempt(child["scientific_id"], material["operation_id"], "COMPLETE", material_path.relative_to(root).as_posix())
    terminal = evaluation_metadata(root, op) if existing_evaluation else None
    if existing_evaluation:
        history.import_attempt(child["scientific_id"], op["operation_id"], terminal["execution"], terminal["reference"])
        write_new_canonical_authority(output / "historical_evaluation.json", terminal)
    new_package = (output / "transfer_access").relative_to(root).as_posix()
    limits = {**op["limits"], "fits": 16}
    batch = d.prepare_batch(history, studies=[child["scientific_id"]],
        scope={"stage": "DEVELOPMENT", "start": "2018-01-01", "end": "2022-12-31", "markets": ["ES", "NQ"]},
        candidates=["es_nq OLS", "nq_only OLS explanatory benchmark"],
        benchmarks=["nq_only", "zero_change", "training_mean"],
        primary_rule="Preserve original transfer study: all eight windows, 16 fits, primary block length 20 and all three frozen comparisons; no sensitivity substitution",
        limits=limits, stop_conditions=["receipt failure", "integrity failure", "resource limit", "attempt completed"],
        rationale=("Infrastructure-only reference: another task has attempted evaluation; no replacement or new execution proposed"
                   if existing_evaluation else "Use the already reconciled materialization for the existing frozen predictive evaluation; no new materialization"))
    write_new_canonical_authority(output / "batch.json", batch)
    write_new_canonical_authority(output / "source_operation_reference.json", {
        "path": str(op_path.relative_to(root)).replace("\\", "/"), "sha256": sha256_file(op_path),
        "operation_id": op["operation_id"], "authority": "REFERENCE_ONLY_NO_RECEIPT_NO_REBIND",
        "required_before_installation": "Review and rebind exact reader/policy dependencies; current operation is not compatible with candidate bytes"})
    candidates = {}
    for name in SOURCES:
        original = (root / name).read_text(encoding="utf-8")
        updated = original
        if name.endswith("research_gateway_policy.py"):
            old = '    if operation in {"MATERIALIZE_ES_NQ_TRANSFER_ONCE", "EXECUTE_ES_NQ_TRANSFER_ONCE"}:'
            updated = replace_once(updated, old,
                '    from .discovery import require_adopted_scope\n'
                '    require_adopted_scope(Path(__file__).resolve().parents[2], operation, scope)\n\n' + old)
        elif name.endswith("es_nq_transfer_access.py"):
            updated = replace_once(updated, "PACKAGE = BASE / 'es_nq_transfer_adoption_v1/source_binding_repair_v1'", f"PACKAGE = Path('{new_package}')")
            updated = replace_once(updated, "    budget = Budget(path, op['limits'], started=started)",
                "    from .discovery_attempt import CampaignBudget\n"
                "    budget = CampaignBudget(root, op, path, started=started)\n"
                "    original_fit = mechanics.fit_ols\n"
                "    def counted_fit(*args, **kwargs):\n"
                "        budget.reserve_fit()\n"
                "        return original_fit(*args, **kwargs)\n"
                "    mechanics.run_screen.__globals__['fit_ols'] = counted_fit")
            updated = replace_once(updated, "        return result\n    except BaseException as exc:",
                "        if op['kind'] == 'execute':\n"
                "            budget.publish_transfer(result)\n"
                "        budget.finish('COMPLETE')\n"
                "        return result\n    except BaseException as exc:")
        elif name.endswith("app.py"):
            updated = replace_once(updated, '    parser.add_argument("--candidate-reports", type=Path, default=None,',
                '    parser.add_argument("--study-summaries", type=Path, default=None, help="Explicit discovery metadata catalog")\n'
                '    parser.add_argument("--candidate-reports", type=Path, default=None,')
            updated = replace_once(updated, '        candidate_reports = load_catalog(args.candidate_reports)',
                '        candidate_reports = load_catalog(args.candidate_reports)\n'
                '    if args.study_summaries is not None:\n'
                '        from ..discovery import load_catalog as load_studies\n'
                '        studies = load_studies(args.study_summaries)\n'
                '        candidate_reports = {**(candidate_reports or studies), "study_summaries": studies["study_summaries"]}')
        else:
            updated = replace_once(updated, '    content.replaceChildren();',
                '    content.replaceChildren();\n'
                '    dialog.style.boxSizing = "border-box";\n'
                '    dialog.style.maxWidth = "94vw";\n'
                '    if (view.study_summaries?.length) {\n'
                '      document.getElementById("candidate-reports-title").textContent = "Research results";\n'
                '      dialog.querySelector(":scope > p").textContent = "Predictive and economic studies retain distinct measures. Development results grant no confirmation or trading authority.";\n'
                '    }')
            updated = replace_once(updated, '    if (!view.reports || !view.reports.length) {',
                '    for (const study of view.study_summaries || []) {\n'
                '      content.append(node("h3", "Study result"));\n'
                '      table(["Field", "Result"], study.rows);\n'
                '      const grid = content.lastElementChild.querySelector("table");\n'
                '      grid.style.tableLayout = "fixed";\n'
                '      grid.querySelector("th").style.width = "24%";\n'
                '      for (const cell of content.lastElementChild.querySelectorAll("td")) cell.style.overflowWrap = "anywhere";\n'
                '    }\n'
                '    if ((!view.reports || !view.reports.length) && view.study_summaries?.length) return;\n'
                '    if (!view.reports || !view.reports.length) {')
        target = output / "candidate" / name
        write_fresh(target, updated)
        write_fresh(target.with_suffix(target.suffix + ".patch"), "".join(difflib.unified_diff(
            original.splitlines(True), updated.splitlines(True), fromfile=name, tofile=name)))
        if name.endswith(".py"):
            compile(updated, name, "exec")
        candidates[name] = {"before_sha256": before[name], "candidate_sha256": sha256_file(target)}
    # Bind an exact inactive access successor, preserving the original package.
    package = root / new_package
    source_bindings = json.loads((original_package / "source_bindings.json").read_bytes())
    write_new_canonical_authority(package / "source_bindings.json", source_bindings)
    implementation = json.loads((original_package / "implementation.json").read_bytes())
    implementation.pop("implementation_id")
    for name in (SOURCES[0], SOURCES[1]):
        implementation["bindings"][name] = candidates[name]["candidate_sha256"]
    for name in ("src/futures_rebuild/discovery.py", "src/futures_rebuild/discovery_attempt.py"):
        implementation["bindings"][name] = sha256_file(root / name)
    implementation["bindings"][new_package + "/source_bindings.json"] = sha256_file(package / "source_bindings.json")
    implementation = d.bind(implementation, "implementation_id")
    write_new_canonical_authority(package / "implementation.json", implementation)
    operations = {}
    for kind in ("materialize", "execute"):
        successor = json.loads((original_package / (kind + "_operation.json")).read_bytes())
        previous_id = successor.pop("operation_id")
        successor.update(implementation_id=implementation["implementation_id"], parent_operation_id=previous_id,
                         destination=MATERIALIZATION.as_posix() if kind == "materialize" else new_package + "/evaluation")
        if kind == "execute":
            successor.update(materialization_binding={"path":material_path.relative_to(root).as_posix(),
                "sha256":sha256_file(material_path)}, read_set=material["read_set"],
                status="BOUND_PREPARED_NOT_AUTHORIZED")
        successor = d.bind(successor, "operation_id")
        write_new_canonical_authority(package / (kind + "_operation.json"), successor)
        operations[kind] = successor
    access = json.loads((root / "configs/approved_es_nq_transfer.json").read_bytes())
    access.update(implementation_sha256=sha256_file(package / "implementation.json"),
                  reader_sha256=candidates[SOURCES[1]]["candidate_sha256"],
                  adoption_authority="PROPOSED_ONLY_REQUIRES_OWNER_ADOPTION; no receipt or historical access")
    if (access["operation_ids"]["execute"] is not None and (
            access["operation_ids"]["execute"] != op["operation_id"]
            or access["operation_sha256"]["execute"] != sha256_file(op_path))):
        raise IntegrityError("current execution binding differs; inspect before preparing")
    # The successor proposes fitting only. Historical materialization operations
    # and receipts stay in their original package; none can rerun through this one.
    access["operation_ids"] = {"materialize": None, "execute": None if existing_evaluation else operations["execute"]["operation_id"]}
    access["operation_sha256"] = {"materialize": None, "execute": None if existing_evaluation else sha256_file(package / "execute_operation.json")}
    write_new_canonical_authority(output / "candidate/configs/approved_es_nq_transfer.json", access)
    from futures_rebuild.es_nq_transfer_access import scope as operation_scope
    adoption = d.bind({"state": "ADOPTED", "module_sha256": sha256_file(Path(d.__file__)), "batch": batch,
        "operations": [] if existing_evaluation else [{"operation": operations["execute"]["operation_name"],
                        "scope": operation_scope(operations["execute"]), "reservation": limits}]}, "adoption_id")
    write_new_canonical_authority(output / "candidate/configs/approved_discovery_batch.json", adoption)
    if (before != {name: sha256_file(root/name) for name in SOURCES}
            or access_before != sha256_file(root / "configs/approved_es_nq_transfer.json")):
        raise IntegrityError("live integration changed during preparation")
    if terminal and any(sha256_file(root/p) != h for p,h in terminal["bindings"].items()):
        raise IntegrityError("terminal evaluation metadata changed during preparation")
    manifest = d.bind({"schema_version": "discovery_preparation/1", "state": "PREPARED_NOT_INSTALLED",
        "candidate_files": candidates, "modules": {p: sha256_file(root/p) for p in (
            "src/futures_rebuild/discovery.py", "src/futures_rebuild/discovery_attempt.py")},
        "batch_id": batch["batch_id"], "activation": False, "historical_execution": False,
        "proposed_config_files": {p: sha256_file(output / "candidate" / p) for p in (
            "configs/approved_es_nq_transfer.json", "configs/approved_discovery_batch.json")},
        "history_import": {p.name: sha256_file(p) for p in sorted((output / "history").glob("[0-9]*.json"))},
        "history_destination": d.HISTORY.as_posix(), "access_package": new_package,
        "preserved_materialization": {"path": material_path.relative_to(root).as_posix(), "sha256": sha256_file(material_path)},
        "baseline_access_sha256": access_before,
        "baseline_execution_destination": op["destination"],
        "baseline_execution_destination_absent": not existing_evaluation,
        "infrastructure_only_no_new_evaluation": existing_evaluation,
        "historical_evaluation": terminal,
        "qualification": "BLOCKED_STATISTICAL_VALIDATION_FAILED"}, "preparation_id")
    write_new_canonical_authority(output / "manifest.json", manifest)
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(prepare(Path(__file__).resolve().parents[1], args.output), indent=2))
