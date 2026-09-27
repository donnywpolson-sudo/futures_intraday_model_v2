"""Prepare an inactive export request; validate separate metadata exports only.

Never opens original observation containers or issues research certificates.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

import prepare_l0_calendar_successor as v2
from futures_rebuild import l0_alpha_candidate as c, l0_alpha_preparation as p
from futures_rebuild import l0_development_admission as admission
from futures_rebuild.canonical import sha256_file
from futures_rebuild.causal_observation_parquet import SCHEMAS
from futures_rebuild.errors import IntegrityError

OUTPUT = Path(c.__file__).resolve().parents[2] / "state/unpublished_evidence/l0_order_readiness_v1/calendar_remediation_v3"
COLUMNS = (
    "market", "source_contract_id", "source_release_id", "source_row_sha256",
    "source_cadence", "bar_start_ns", "bar_end_ns", "decision_eligible_at_ns",
    "publisher_id", "instrument_id", "raw_symbol", "actual_contract",
    "definition_source_file_path", "definition_source_file_sha256", "definition_row_sha256",
    "definition_event_at_ns", "definition_received_at_ns", "listing_activation_ns", "expiration_ns",
    "min_price_increment_nano", "multiplier_nano", "project_trade_date",
)
EXPORT_SCHEMA = pa.schema([SCHEMAS["observations"].field(name) for name in COLUMNS])


def save_or_check(path, value, check):
    if check or path.exists():
        if not path.exists() or json.loads(path.read_text()) != value:
            raise IntegrityError("prepared output changed: " + str(path))
    else:
        with path.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(value, stream, sort_keys=True, indent=2)
            stream.write("\n")


def validate_rows(rows, source, contracts):
    """Schema/provenance consistency only; no full source-row digest reconstruction."""
    previous = -1
    definitions, seen = {}, set()
    for row in rows:
        if set(row) != set(COLUMNS):
            raise IntegrityError("metadata columns differ; prices are not permitted")
        if (row["market"] != source["market"] or row["source_cadence"] != "1m"
            or not f"{source['year']}-01-01" <= row["project_trade_date"] < f"{source['year']+1}-01-01"
            or row["actual_contract"] not in contracts or row["raw_symbol"] != row["actual_contract"]):
            raise IntegrityError("metadata selection scope differs")
        start = row["bar_start_ns"]
        if type(start) is not int or start <= previous or start % c.MINUTE or row["bar_end_ns"] != start+c.MINUTE:
            raise IntegrityError("duplicate, ambiguous or invalid observation time")
        previous = start
        for key in ("source_contract_id", "source_release_id", "source_row_sha256", "definition_row_sha256", "definition_source_file_sha256"):
            if not isinstance(row[key], str) or len(row[key]) != 64 or any(x not in '0123456789abcdef' for x in row[key]):
                raise IntegrityError("metadata provenance digest differs")
        if row["source_row_sha256"] in seen:
            raise IntegrityError("duplicate source row")
        seen.add(row["source_row_sha256"])
        for key in ("instrument_id", "publisher_id", "min_price_increment_nano", "multiplier_nano"):
            if type(row[key]) is not int or row[key] <= 0:
                raise IntegrityError("invalid instrument identity or units")
        for key in ("definition_event_at_ns", "definition_received_at_ns", "listing_activation_ns", "expiration_ns", "decision_eligible_at_ns"):
            if type(row[key]) is not int or row[key] < 0:
                raise IntegrityError("invalid temporal metadata")
        identity = (row["publisher_id"], row["instrument_id"], row["definition_event_at_ns"], row["definition_received_at_ns"])
        meaning = tuple(row[k] for k in ("actual_contract", "definition_row_sha256", "listing_activation_ns", "expiration_ns", "min_price_increment_nano", "multiplier_nano"))
        if identity in definitions and definitions[identity] != meaning:
            raise IntegrityError("ambiguous definition version")
        definitions[identity] = meaning
        # Future-known metadata is retained for a subsequent causal join, never backdated.
    return {"rows": len(seen), "definition_versions": len(definitions)}


def termination_permission(selection, rules):
    """Validate exact, separately evidenced termination; never use expiration as termination."""
    matches = [r for r in rules if all(r[k] == selection[k] for k in
               ("market", "publisher_id", "instrument_id", "actual_contract", "definition_row_sha256"))
               and r["effective_start_ns"] <= selection["decision_ns"] < r["effective_end_ns"]]
    if len(matches) > 1:
        raise IntegrityError("ambiguous termination applicability")
    if not matches:
        return "UNKNOWN"
    rule = matches[0]
    if (rule.get("available_by_ns") is None or rule["available_by_ns"] > selection["decision_ns"]
        or not rule.get("source_binding") or not rule.get("section") or not rule.get("contract_month")
        or not rule.get("venue")):
        return "UNKNOWN"
    c.old.profit.digest(rule["source_binding"].get("sha256"))
    if rule.get("fact_kind") != "LAST_TRADING_TIME":
        raise IntegrityError("expiration/settlement is not last trading")
    local = datetime.fromisoformat(rule["last_trading_local"])
    zone = ZoneInfo(rule["timezone"])
    if local.tzinfo is not None:
        raise IntegrityError("local termination must use the explicit named timezone")
    instant = local.replace(tzinfo=zone, fold=rule.get("fold", 0))
    if instant.astimezone(timezone.utc).astimezone(zone).replace(tzinfo=None) != local:
        raise IntegrityError("nonexistent local termination time")
    if local.replace(tzinfo=zone, fold=0).utcoffset() != local.replace(tzinfo=zone, fold=1).utcoffset() and "fold" not in rule:
        raise IntegrityError("ambiguous local termination time")
    ns = int(instant.timestamp()) * 10**9
    if ns != rule["last_trading_ns"]:
        raise IntegrityError("termination timezone conversion differs")
    if selection["definition_received_at_ns"] > selection["decision_ns"]:
        return "UNKNOWN"
    if selection["exit_ns"] >= rule["effective_end_ns"]:
        return "UNKNOWN"
    return "BEFORE_TERMINATION" if selection["exit_ns"] < ns else "BLOCKED_BY_TERMINATION"


def validate_export(root, spec, receipt):
    c.identity(spec, "specification_id")
    c.identity(receipt, "export_id")
    if (receipt["specification_id"] != spec["specification_id"] or receipt["calendar_id"] != spec["calendar_id"]
        or receipt["source_files"] != spec["source_files"] or receipt["evidence_class"] != "METADATA_PROJECTION_NOT_ADMISSION"):
        raise IntegrityError("stale or incompatible export binding")
    expected = {f"{s['market']}/{s['year']}.parquet": s for s in spec["source_files"]}
    if set(receipt["outputs"]) != set(expected):
        raise IntegrityError("missing or extra export partitions")
    result = {}
    for name, source in expected.items():
        # Only the dedicated export location; no arbitrary input paths or original containers.
        path = OUTPUT / "metadata_export" / name
        if path.is_symlink() or path.resolve() != path.absolute():
            raise IntegrityError("export path redirects")
        binding = receipt["outputs"][name]
        if sha256_file(path) != binding["sha256"]:
            raise IntegrityError("export bytes changed")
        parquet = pq.ParquetFile(v2.long_path(path))
        if not parquet.schema_arrow.equals(EXPORT_SCHEMA, check_metadata=False):
            raise IntegrityError("export includes undeclared fields or types")
        def rows():
            for batch in parquet.iter_batches(batch_size=8192, columns=list(COLUMNS)):
                for row in batch.to_pylist():
                    yield {k: value.hex() if isinstance(value, bytes) else value for k,value in row.items()}
        result[name] = validate_rows(rows(), source, spec["observed_symbols"][source["market"]])
        if result[name]["rows"] != source["row_count"] or binding["rows"] != source["row_count"]:
            raise IntegrityError("truncated export")
        if sha256_file(path) != binding["sha256"]:
            raise IntegrityError("export changed during validation")
    return {"metadata_consistency": result, "source_projection_independently_verified": False,
            "termination_complete": False, "admission": "NOT_EVALUATED"}


def prepare(root, check=False):
    base = root / v2.OUTPUT
    verified = json.loads((base/"verification.json").read_text())
    for name, digest in verified["files"].items():
        if sha256_file(root/name) != digest:
            raise IntegrityError("v2 evidence changed: " + name)
    bundle = p.validate_bundle(root)
    contract = admission.validate_contract(root, json.loads((root/admission.CONTRACT_PATH).read_text()))
    calendar = json.loads((base/"calendar_candidate.json").read_text())
    metadata = json.loads((base/"selected_contract_metadata.json").read_text())
    plan = bundle["historical_readiness_proposal"]
    spec = c.bind({"schema_version": "l0_metadata_projection_request/1", "state": "PREPARED_NOT_AUTHORIZED_NOT_EXECUTED",
        "calendar_id": calendar["calendar_id"], "bundle_id": bundle["bundle_id"], "contract_id": contract["contract_id"],
        "source_files": plan["exact_sources"], "columns": list(COLUMNS), "observed_symbols": metadata["contracts"],
        "scope": {"markets":list(c.MARKETS),"years":list(range(2018,2023)),"dates":list(c.weekday_dates()),"folds":c.design()["folds"]},
        "operation": "LOCAL_COLUMN_PROJECTION_FROM_20_MIXED_ANNUAL_PARQUET_CONTAINERS;EXPLICIT_APPROVAL_REQUIRED",
        "source_bytes": plan["source_bytes"], "rows":plan["row_limit"], "cost_usd":0,
        "maximum_output_bytes":2**30,"maximum_peak_working_set_bytes":2**30,"hard_stop_minutes":20,"retries":0,
        "destination":str(OUTPUT.relative_to(root)/"metadata_export"),
        "not_supplied_by_projection":["full source row rehash without prohibited columns","venue/contract-month authority","historical last trading rules","official publication/application receipt","training labels or benchmark evidence"],
        "builder_sha256":sha256_file(Path(__file__))}, "specification_id")
    # Explicit proposal only: the existing validator intentionally rejects this binding.
    proposed = dict(bundle)
    proposed.pop("bundle_id")
    proposed["entry_calendar"] = {"path":str((base/"calendar_candidate.json").relative_to(root)).replace('\\','/'),
        "sha256":sha256_file(base/"calendar_candidate.json"),"calendar_id":calendar["calendar_id"],"research_view":"evidence_only"}
    proposed = c.bind(proposed,"bundle_id")
    OUTPUT.mkdir(exist_ok=True)
    save_or_check(OUTPUT/"proposed_bundle.json", proposed, check)
    next_contract = dict(contract); next_contract.pop("contract_id")
    next_contract.update(bundle={"path":str((OUTPUT/"proposed_bundle.json").relative_to(root)).replace('\\','/'),
                                "sha256":sha256_file(OUTPUT/"proposed_bundle.json")},bundle_id=proposed["bundle_id"])
    next_contract=c.bind(next_contract,"contract_id")
    try:
        admission.validate_contract(root,next_contract)
    except IntegrityError:
        rejected=True
    else:
        raise IntegrityError("current contract unexpectedly admits calendar rebinding")
    coverage=json.loads((base/"coverage.json").read_text()); partial=json.loads((base/"admission_partial_inputs.json").read_text())
    groups=[]
    for market,info in coverage['views']['evidence_only'].items():
        causes={}
        for day in info['daily']:
            for start,stop,_,state,reason in day['runs']:
                if state=='UNKNOWN': causes.setdefault(reason,[]).append({'date':day['date'],'start_index':start,'stop_index':stop})
        groups.extend({'market':market,'reason':reason,'intervals':intervals} for reason,intervals in causes.items())
    outputs={"export_specification.json":spec,"proposed_contract.json":next_contract,
        "binding_proposal.json":{"state":"PROPOSAL_ONLY_REJECTED_BY_CURRENT_GATE","parent_contract_id":contract['contract_id'],
          "proposed_contract_id":next_contract['contract_id'],"calendar_id":calendar['calendar_id'],"current_validator_rejects":rejected,
          "bundle_changes":["entry_calendar","bundle_id"],"contract_changes":["bundle","bundle_id","contract_id"],
          "scientific_design_unchanged":proposed['design']==bundle['design'],"activation":False},
        "coverage_reuse.json":{"calendar_id":calendar['calendar_id'],"coverage_binding":{"path":str(base/'coverage.json'),"sha256":sha256_file(base/'coverage.json')},
          "development_and_audit":{m:v['summary']['after'] if 'after' in v['summary'] else {s:x['after'] for s,x in v['summary'].items()} for m,v in coverage['views']['evidence_only'].items()},
          "transitions_from_v2":0,"fold_slots":partial['fold_evidence_partial'],"unknown_groups":groups}}
    for name,value in outputs.items(): save_or_check(OUTPUT/name,value,check)
    print(json.dumps({'specification_id':spec['specification_id'],'calendar_id':calendar['calendar_id'],'proposal_rejected':rejected,'reproduced':check}))


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--check',action='store_true')
    parser.add_argument('--validate-export',action='store_true')
    args=parser.parse_args()
    if args.validate_export:
        print(json.dumps(validate_export(Path.cwd(),json.loads((OUTPUT/'export_specification.json').read_text()),
                                        json.loads((OUTPUT/'metadata_export/receipt.json').read_text()))))
    else:
        prepare(Path.cwd(),args.check)
