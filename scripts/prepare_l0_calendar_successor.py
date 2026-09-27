"""Reference-only inactive L0 successor; no price readers or certificate issuance.

Explicit input_manifest.json pins prior evidence and the permitted roll metadata.
--check rederives outputs, including all 160 original fold-market slots.
"""
from collections import Counter
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from zoneinfo import ZoneInfo
import argparse
import json
import os

import pyarrow.compute as pc
import pyarrow.parquet as pq

import prepare_l0_calendar_footnote_candidate as prior
from futures_rebuild import l0_alpha_calendar as cal, l0_alpha_candidate as c
from futures_rebuild import l0_development_admission as admission
from futures_rebuild.canonical import sha256_file
from futures_rebuild.cme_calendar_successor import RECOVERY_RAW_SHA256
from futures_rebuild.cme_calendar_successor import FAMILY_PATTERN
from futures_rebuild.errors import IntegrityError

OUTPUT = Path(cal.OUTPUT) / "calendar_remediation_v2"
ROLL_COLUMNS = ["observation_ordinal", "actual_contract_before", "actual_contract_after", "effective_time_ns"]


def long_path(path):
    path = str(Path(path).absolute())
    return "\\\\?\\" + path if os.name == "nt" and not path.startswith("\\\\?\\") else path


def read_bound(root, binding):
    with open(long_path(root / binding["path"]), "rb") as stream:
        raw = stream.read()
    if sha256(raw).hexdigest() != binding["sha256"]:
        raise IntegrityError("candidate input binding changed: " + binding["path"])
    return raw


def build_successor(root, manifest):
    before = json.loads(read_bound(root, manifest["prior_calendar"]))
    c.identity(before, "calendar_id")
    if before["calendar_id"] != manifest["prior_calendar_id"]:
        raise IntegrityError("prior calendar identity differs")
    january = json.loads(read_bound(root, manifest["january_record"]))
    raw = read_bound(root, manifest["january_raw"])
    sidecar = json.loads(read_bound(root, manifest["january_sidecar"]))
    c.identity(january, "record_id")
    if (sha256(raw).hexdigest() != RECOVERY_RAW_SHA256 or
        sidecar["raw_sha256"] != RECOVERY_RAW_SHA256 or sidecar["raw_size"] != len(raw) or
        january["raw"]["sha256"] != RECOVERY_RAW_SHA256 or
        january["sidecar"]["sha256"] != manifest["january_sidecar"]["sha256"] or
        sidecar["archive_timestamp"] != january["archive_timestamp"]):
        raise IntegrityError("original January acquisition identity differs")
    cells = cal.read_xls_bytes(raw)
    values = {(x.row, x.column): " ".join(str(x.value).split()) for x in cells}
    if (values.get((0, 1)) != "CME Group Globex New Years Holiday Schedule: December 31, 2018 - January 2, 2019"
        or values.get((2, 2)) != "Tuesday, Jan 1"):
        raise IntegrityError("January workbook date identity differs")
    after = deepcopy(before)
    after.pop("calendar_id")
    day = "2019-01-01"
    start = int(datetime(2019, 1, 1, tzinfo=cal.CHICAGO).timestamp()) * 10**9
    stop = (c.decision_times(day)[-1] // c.MINUTE + 61) * c.MINUTE + 1
    available = int(datetime.strptime(january["archive_timestamp"], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).timestamp()) * 10**9
    for family, row, label in [("EQUITY_INDEX", 4, "Equity"), ("ENERGY", 8, "Energy, Metals & DME"), ("METALS", 8, "Energy, Metals & DME")]:
        if values.get((row, 0)) != label or values.get((row, 2)) != "Closed for New Year's":
            raise IntegrityError("January product disposition differs")
        after["schedules"].append({"family": family, "dates": [day],
            "segments": [{"start_ns": start, "end_ns": stop, "state": "CLOSED"}],
            "available_at_ns": available, "availability_basis": "ORIGINAL_ACCEPTED_ARCHIVE_ACQUISITION_AND_EXACT_RAW_SIDECAR_RECOVERED",
            "source": manifest["january_raw"]["path"], "source_sha256": RECOVERY_RAW_SHA256,
            "sheet": cells[0].sheet, "row_zero_based": row, "column_zero_based": 2,
            "source_record": manifest["january_record"], "original_sidecar": manifest["january_sidecar"],
            "limitation": "Frozen window only; evidence endpoint is not an asserted exchange reopening"})
        parsed = cal.parse_family_schedule(cells, label_pattern=FAMILY_PATTERN[family])
        opening = [e for e in parsed.events if e.event == "OPEN" and e.column == 3]
        closing = [e for e in parsed.events if e.event == "CLOSE" and e.column == 5]
        if (len(opening) != 1 or len(closing) != 1 or opening[0].at != datetime(2019, 1, 1, 17)
            or closing[0].at != datetime(2019, 1, 2, 16)):
            raise IntegrityError("January reopening/next close differs")
        next_day = "2019-01-02"
        after["schedules"][-1]["dates"].append(next_day)
        after["schedules"][-1]["segments"].append({"start_ns": c.decision_times(next_day)[0],
            "end_ns": (c.decision_times(next_day)[-1] // c.MINUTE + 61) * c.MINUTE + 1, "state": "OPEN"})
        after["schedules"][-1]["reopening_evidence"] = {"open_cell": [row, 3], "close_cell": [row, 5],
            "open_chicago": opening[0].at.isoformat(), "close_chicago": closing[0].at.isoformat()}
    publication = json.loads(read_bound(root, manifest["mourning_publication"]))
    evidence = publication["content_evidence"]
    if sha256_file(Path(long_path(evidence["path"]))) != evidence["sha256"]:
        raise IntegrityError("mourning source bytes changed")
    distribution = publication["distribution_record"]
    if (publication["products"] != ["ES", "NQ"] or distribution["provided_by"] != "CME Group"
        or distribution["precision"] != "minute"):
        raise IntegrityError("mourning publication scope differs")
    known = datetime.fromisoformat(distribution["available_by_utc"])
    if known.utcoffset() is None:
        raise IntegrityError("publication timezone required")
    published = datetime.fromisoformat(distribution["published_at_local"]).replace(tzinfo=ZoneInfo(distribution["timezone"]))
    if int(known.timestamp()) != int(published.timestamp()) + 60:
        raise IntegrityError("publication precision bound differs")
    found = [x for x in after["schedules"] if x["family"] == "EQUITY_INDEX" and x["dates"] == ["2018-12-05"]]
    if len(found) != 1 or found[0]["available_at_ns"] is not None:
        raise IntegrityError("mourning predecessor differs")
    found[0].update(available_at_ns=int(known.timestamp()) * 10**9,
        availability_basis="ISSUER_LINKED_ORIGINAL_DISTRIBUTION_RECORD;NOT_APPLICATION_RECEIPT",
        source=evidence["path"], source_sha256=evidence["sha256"], publication_evidence=manifest["mourning_publication"],
        evidence_role="OWNED_OFFICIAL_CLOSURE_CONTENT_WITH_SEPARATE_PUBLIC_DISTRIBUTION_EVIDENCE")
    after["prior_candidate_id"] = before["calendar_id"]
    after["successor_input_manifest_id"] = manifest["input_manifest_id"]
    after["builder_sha256"] = sha256_file(Path(__file__))
    return before, c.bind(after, "calendar_id")


def selected_contract_metadata(root, manifest):
    """Read certified roll-table contract columns only; do not infer missing definitions."""
    months = []
    for entry in manifest["roll_metadata"]:
        path = root / entry["path"]
        if path.name != "roll.parquet" or entry["market"] not in c.MARKETS or not 2018 <= entry["year"] <= 2022:
            raise IntegrityError("undeclared metadata read")
        read_bound(root, entry)
        parquet = pq.ParquetFile(long_path(path))
        if (parquet.schema_arrow.metadata or {}).get(b"logical_table") != b"roll":
            raise IntegrityError("expected roll metadata schema")
        table = parquet.read(columns=ROLL_COLUMNS)
        different = pc.not_equal(table["actual_contract_before"], table["actual_contract_after"])
        if pc.any(pc.not_equal(different, pc.is_valid(table["effective_time_ns"]))).as_py():
            raise IntegrityError("roll change timing missing or contradictory")
        changes = table.filter(different).to_pylist()
        contracts = sorted(set(pc.unique(table["actual_contract_before"]).to_pylist() + pc.unique(table["actual_contract_after"]).to_pylist()))
        months.append({"market": entry["market"], "year": entry["year"], "interval": entry["interval"],
            "binding": entry, "rows": table.num_rows, "contracts": contracts, "changes": changes,
            "first_observed_contract": table["actual_contract_after"][0].as_py() if table.num_rows else None,
            "instrument_identity_and_termination": "UNRESOLVED_DEFINITION_FIELDS_NOT_IN_ROLL_SCHEMA",
            "decision_mapping": "PARTIAL;NON_CHANGE_ROWS_HAVE_NO_EVENT_TIME;MONTH_START_NOT_ASSUMED"})
        read_bound(root, entry)
    return {"read_columns": ROLL_COLUMNS, "months": months, "market_rows": {m: sum(x["rows"] for x in months if x["market"] == m) for m in c.MARKETS},
        "contracts": {m: sorted({s for x in months if x["market"] == m for s in x["contracts"]}) for m in c.MARKETS},
        "complete_decision_contract_mapping": False, "termination_bound": False}


def partial_admission(root, calendar, counts, manifest):
    contract = json.loads(read_bound(root, manifest["admission_contract"]))
    admission.validate_contract(root, contract)
    old_bundle = json.loads(read_bound(root, manifest["training_bundle"]))
    bundle = json.loads(read_bound(root, manifest["frozen_bundle"]))
    for key in ("decision_window", "last_entry", "last_exit", "entry", "holding_minutes", "history_bars",
                "training", "folds", "embargo_dates", "purge", "target", "calendar", "availability", "contract_mapping"):
        if old_bundle["design"][key] != bundle["design"][key]:
            raise IntegrityError("saved training support scope differs: " + key)
    if json.loads(read_bound(root, manifest["training_plan"])) != bundle["historical_readiness_proposal"]:
        raise IntegrityError("saved support source plan differs")
    training = {m: json.loads(read_bound(root, b)) for m, b in manifest["training_reports"].items()}
    daily = {m: {d["date"]: d for d in values["daily"]} for m, values in counts["views"]["evidence_only"].items()}
    rows = []
    for market in c.MARKETS:
        for i, f in enumerate(c.design()["folds"]):
            for role, window in [("outer", f["outer"]), *[(f"inner_{j}", w) for j, w in enumerate(f["inner"])]]:
                dates = window["audit_dates"]
                tally = sum((Counter(daily[market][day]["after"]) for day in dates), Counter())
                support = [r for r in training[market]["fold_support"] if r["outer_fold"] == i and r["role"] == role]
                if len(support) != 1 or training[market]["support_status"] != "SUPPORTED":
                    raise IntegrityError("saved training support identity differs")
                rows.append({"market": market, "outer_fold": i, "role": role, "audit_dates": dates,
                    "observed_training_dates": support[0]["observed_training_dates"], "training_rows": support[0]["training_rows"],
                    "training_support_binding": manifest["training_reports"][market],
                    "calendar": {k.lower(): tally[k] for k in ("ALLOWED", "BLOCKED", "UNKNOWN")},
                    "unknown_intervals": [{"date": day, "decision_index_runs": [r[:2] for r in daily[market][day]["runs"] if r[3] == "UNKNOWN"]} for day in dates if daily[market][day]["after"].get("UNKNOWN", 0)],
                    "missing_certificate_fields": ["checks", "benchmarks"]})
    if len(rows) != 160 or any(sum(x["calendar"].values()) != len(x["audit_dates"])*330 for x in rows):
        raise IntegrityError("fold-market calendar census differs")
    return {"status": "PARTIAL_INPUTS_NOT_A_CERTIFICATE", "contract_id": contract["contract_id"],
        "candidate_calendar_id": calendar["calendar_id"], "fold_evidence_partial": rows,
        "source_requirements": list(admission.SOURCE_CHECKS), "fold_requirements": list(admission.FOLD_CHECKS),
        "candidate_matches_contract_calendar": bundle["entry_calendar"]["calendar_id"] == calendar["calendar_id"],
        "full_entrypoint": "l0_development_admission.build_certificate then validate_certificate",
        "full_admission_run": False, "reason": "UNKNOWN_CALENDAR;INCOMPLETE_SOURCE_AND_BENCHMARK_EVIDENCE;CURRENT_CONTRACT_BINDS_ORIGINAL_CALENDAR",
        "missing_inputs": ["scope-matched source_bindings/source_facts", "complete fold training/causal/benchmark evidence", "final_support", "dataset_ids", "admissible evidence_class", "validated candidate-compatible inactive bundle/contract"],
        "activation": False}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    check = parser.parse_args().check
    root = Path.cwd()
    out = root / OUTPUT
    manifest = json.loads((out / "input_manifest.json").read_text())
    c.identity(manifest, "input_manifest_id")
    for binding in manifest["required_bindings"]:
        read_bound(root, binding)
    before, after = build_successor(root, manifest)
    counts = prior.coverage(before, after)
    outputs = {"calendar_candidate.json": after, "coverage.json": counts,
        "selected_contract_metadata.json": selected_contract_metadata(root, manifest),
        "admission_partial_inputs.json": partial_admission(root, after, counts, manifest)}
    for name, value in outputs.items():
        path = out / name
        if check or path.exists():
            if not path.exists() or json.loads(path.read_text()) != value:
                raise IntegrityError("saved successor differs: " + name)
        else:
            with path.open("x", encoding="utf-8", newline="\n") as stream:
                json.dump(value, stream, sort_keys=True, separators=(",", ":"))
                stream.write("\n")
    print(json.dumps({"candidate_id": after["calendar_id"], "reproduced": check,
        "contracts_per_market": {m: len(s) for m, s in outputs["selected_contract_metadata.json"]["contracts"].items()},
        "summary": {m: d["summary"] for m, d in counts["views"]["evidence_only"].items()}}))


if __name__ == "__main__":
    main()
